"""HybridConnectionListener for Azure Relay Hybrid Connections."""

import asyncio
import logging
from typing import Optional, Callable, Awaitable, Union, Any
import websockets
from websockets.exceptions import ConnectionClosed as _WSConnectionClosed

from .connection_string import RelayConnectionStringBuilder
from .token_provider import TokenProvider, SecurityToken
from .protocol import (
    ProtocolHandler,
    MESSAGE_KIND_ACCEPT,
    MESSAGE_KIND_REQUEST,
    MESSAGE_KIND_REQUEST_POINTER,
    MESSAGE_KIND_RENEW_TOKEN,
    CONTROL_CHANNEL_MAX_BODY_SIZE,
)
from .context import (
    RelayedHttpListenerContext,
    RelayedHttpListenerRequest,
    RelayedHttpListenerResponse,
)
from .stream import HybridConnectionStream


logger = logging.getLogger(__name__)


AcceptHandler = Callable[
    [RelayedHttpListenerContext],
    Union[bool, Awaitable[bool]],
]


class HybridConnectionListener:
    """
    Listener for Azure Relay Hybrid Connections.

    Supports two protocol patterns:

    1. **HTTP request/response** – incoming HTTP requests are surfaced via
       the ``request_handler`` callback. Responses up to 64 kB are sent over
       the control channel; larger responses, or requests delivered over a
       rendezvous WebSocket, automatically use the rendezvous path.

    2. **WebSocket rendezvous** – sender-initiated WebSocket connections are
       optionally screened by ``accept_handler`` and then yielded by
       ``accept_connection()`` (or the ``connections()`` async iterator)
       as ``HybridConnectionStream`` instances.
    """

    def __init__(self, address: str, token_provider: TokenProvider):
        """
        Initialize the HybridConnectionListener.
        
        Args:
            address: The hybrid connection URI (e.g., "sb://namespace.servicebus.windows.net/path")
            token_provider: TokenProvider instance for generating SAS tokens
        """
        self._address = address
        self._token_provider = token_provider
        self._websocket: Optional[Any] = None
        self._is_online = False
        self._protocol_handler = ProtocolHandler()
        self._current_token: Optional[SecurityToken] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._token_renewal_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._should_reconnect = False
        self._reconnect_attempt = 0
        self._ping_interval = 30.0  # Send ping every 30 seconds

        # Per-control-channel send lock so concurrent handler tasks don't
        # interleave JSON+binary frame pairs.
        self._control_send_lock = asyncio.Lock()

        # Background tasks spawned to handle individual control messages.
        # Tracked so they can be cancelled on close.
        self._dispatch_tasks: "set[asyncio.Task]" = set()

        # Queue of accepted rendezvous WebSocket streams produced by
        # sender 'connect' operations. Drained via ``accept_connection()``.
        self._pending_connections: "asyncio.Queue[HybridConnectionStream]" = asyncio.Queue()
        self._open_streams: "set[HybridConnectionStream]" = set()

        # Event callbacks
        self.on_connecting: Optional[Callable[[], None]] = None
        self.on_online: Optional[Callable[[], None]] = None
        self.on_offline: Optional[Callable[[], None]] = None

        # Request handler for incoming HTTP requests.
        self.request_handler: Optional[
            Callable[[RelayedHttpListenerContext], Union[None, Awaitable[None]]]
        ] = None

        # Accept handler invoked before accepting a sender's WebSocket
        # rendezvous. Returning False (or raising) rejects the connection;
        # the listener will surface the response status code/description
        # back to the sender via the WebSocket reject handshake.
        self.accept_handler: Optional[AcceptHandler] = None

    @classmethod
    def from_connection_string(cls, connection_string: str) -> "HybridConnectionListener":
        """
        Create a HybridConnectionListener from a connection string.
        
        Args:
            connection_string: Azure Relay connection string containing Endpoint,
                             SharedAccessKeyName, SharedAccessKey, and EntityPath
        
        Returns:
            A new HybridConnectionListener instance
        """
        builder = RelayConnectionStringBuilder(connection_string)
        
        # Validate required fields
        if not builder.endpoint:
            raise ValueError("Connection string missing Endpoint")
        if not builder.shared_access_key_name:
            raise ValueError("Connection string missing SharedAccessKeyName")
        if not builder.shared_access_key:
            raise ValueError("Connection string missing SharedAccessKey")
        if not builder.entity_path:
            raise ValueError("Connection string missing EntityPath")
        
        # Create token provider
        token_provider = TokenProvider(
            key_name=builder.shared_access_key_name,
            shared_access_key=builder.shared_access_key
        )
        
        # Build the address (sb:// URI format)
        address = builder.build_uri()
        
        return cls(address, token_provider)

    @property
    def is_online(self) -> bool:
        """Get whether the listener is currently online and connected."""
        return self._is_online

    async def open(self) -> None:
        """
        Open the listener and establish the control channel.
        
        This method connects to the Azure Relay service via WebSocket and
        prepares to receive HTTP requests. Automatic reconnection is enabled.
        
        Raises:
            ConnectionError: If unable to connect to the relay service
        """
        self._should_reconnect = True
        self._reconnect_attempt = 0
        await self._connect()
    
    async def _connect(self) -> None:
        """
        Internal method to establish the WebSocket connection.
        """
        # Fire connecting event
        if self.on_connecting:
            self.on_connecting()
        
        try:
            namespace, path = self._split_address()
            
            # Get a token for authentication
            self._current_token = self._token_provider.get_token(self._address)
            
            # Build the WebSocket control channel URL
            ws_url = self._protocol_handler.build_control_channel_url(
                namespace=namespace,
                path=path,
                token=self._current_token.token
            )
            
            # Connect to the WebSocket
            self._websocket = await websockets.connect(ws_url)
            
            # Connection established - we're now online
            # Note: The control channel doesn't receive an initial message;
            # it's a persistent connection that receives 'request' or 'accept'
            # messages when senders connect
            
            self._is_online = True
            self._reconnect_attempt = 0  # Reset reconnect attempts on successful connection
            
            # Fire online event
            if self.on_online:
                self.on_online()
            
            # Start receiving messages in the background
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            # Start token renewal task
            self._token_renewal_task = asyncio.create_task(self._token_renewal_loop())
            
            # Start ping/keepalive task
            self._ping_task = asyncio.create_task(self._ping_loop())
                
        except Exception as e:
            self._is_online = False
            if self.on_offline:
                self.on_offline()
            raise ConnectionError(f"Failed to open listener: {e}") from e

    async def close(self) -> None:
        """
        Close the listener and disconnect from the relay service.
        
        This method gracefully closes the WebSocket control channel,
        cancels in-flight dispatch tasks, and drains any queued or open
        rendezvous streams.
        """
        # Disable automatic reconnection
        self._should_reconnect = False
        
        # Cancel the reconnect task if it's running
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        
        # Cancel the ping task if it's running
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        
        # Cancel the token renewal task if it's running
        if self._token_renewal_task and not self._token_renewal_task.done():
            self._token_renewal_task.cancel()
            try:
                await self._token_renewal_task
            except asyncio.CancelledError:
                pass
        
        # Cancel the receive task if it's running
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        # Cancel any in-flight per-message dispatch tasks
        for task in list(self._dispatch_tasks):
            if not task.done():
                task.cancel()
        for task in list(self._dispatch_tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._dispatch_tasks.clear()

        # Drain queued accepted streams and close them
        while not self._pending_connections.empty():
            try:
                stream = self._pending_connections.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await stream.close()
            except Exception:
                pass

        # Close any streams the listener still owns (e.g. HTTP rendezvous)
        for stream in list(self._open_streams):
            try:
                await stream.close()
            except Exception:
                pass
        self._open_streams.clear()

        if self._websocket:
            try:
                await self._websocket.close()
            except Exception:
                pass  # Ignore errors during close
            finally:
                self._websocket = None
        
        self._is_online = False
        
        # Fire offline event
        if self.on_offline:
            self.on_offline()

    # ------------------------------------------------------------------ #
    # Public rendezvous accept API
    # ------------------------------------------------------------------ #

    async def accept_connection(self) -> HybridConnectionStream:
        """Wait for and return the next accepted rendezvous WebSocket.

        Blocks until a sender connects, the ``accept_handler`` accepts it,
        and the rendezvous WebSocket is established. The returned
        ``HybridConnectionStream`` is fully owned by the caller; the
        listener will not close it on shutdown.
        """
        stream = await self._pending_connections.get()
        # The stream is being handed off to the caller; remove it from the
        # set tracked for shutdown cleanup.
        self._open_streams.discard(stream)
        return stream

    async def connections(self):
        """Async iterator over accepted rendezvous WebSocket streams.

        ``async for stream in listener.connections(): ...`` yields each
        accepted ``HybridConnectionStream`` as the rendezvous succeeds.
        """
        while True:
            yield await self.accept_connection()

    # ------------------------------------------------------------------ #
    # Receive loop and per-message dispatch
    # ------------------------------------------------------------------ #

    async def _receive_loop(self) -> None:
        """Read control messages and dispatch each to its own task."""
        if not self._websocket:
            return

        try:
            while self._is_online and self._websocket:
                message = await self._websocket.recv()

                if not self._protocol_handler.is_text_message(message):
                    # Stray binary frame on the control channel – the
                    # service uses binary frames as continuation bodies of
                    # the previous request. Anything we see here without a
                    # matching request is a protocol violation; ignore.
                    logger.debug(
                        "HybridConnectionListener: ignoring unexpected binary "
                        "frame on control channel (%d bytes)",
                        len(message) if message else 0,
                    )
                    continue

                try:
                    parsed = self._protocol_handler.parse_control_message(message)
                except ValueError:
                    logger.warning(
                        "HybridConnectionListener: dropping malformed control message"
                    )
                    continue

                kind = parsed.get("kind")
                if kind == MESSAGE_KIND_REQUEST:
                    # Body, if any, arrives as the very next frame on the
                    # control channel; read it inline before dispatching so
                    # the binary frame is matched to its request.
                    body = b""
                    if parsed.get("body"):
                        body = await self._read_body_frames(self._websocket)
                    self._spawn_dispatch(
                        self._handle_control_request(parsed, body)
                    )
                elif kind == MESSAGE_KIND_REQUEST_POINTER:
                    self._spawn_dispatch(
                        self._handle_rendezvous_request(parsed.get("address"))
                    )
                elif kind == MESSAGE_KIND_ACCEPT:
                    self._spawn_dispatch(self._handle_accept(parsed))
                elif kind == MESSAGE_KIND_RENEW_TOKEN:
                    # Listener never receives renewToken from the service;
                    # ignore.
                    continue
                else:
                    logger.debug(
                        "HybridConnectionListener: ignoring control message of "
                        "unknown kind %r",
                        kind,
                    )

        except _WSConnectionClosed:
            self._is_online = False
            if self.on_offline:
                self.on_offline()
            if self._should_reconnect:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("HybridConnectionListener: receive loop error")
            self._is_online = False
            if self.on_offline:
                self.on_offline()
            if self._should_reconnect:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def _spawn_dispatch(self, coro: Awaitable[None]) -> None:
        task = asyncio.create_task(coro)
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def _read_body_frames(self, websocket: Any) -> bytes:
        """Read binary frames from ``websocket`` until one is received.

        ``websockets`` already reassembles fragmented WebSocket messages, so
        a single ``recv()`` returns the full body content the service
        delivers for a request or response.
        """
        frame = await websocket.recv()
        if not self._protocol_handler.is_binary_message(frame):
            raise ValueError(
                "Expected binary body frame after request/response message, "
                f"got {type(frame).__name__}"
            )
        return self._protocol_handler.decode_binary_body(frame)

    # ------------------------------------------------------------------ #
    # HTTP request handling
    # ------------------------------------------------------------------ #

    async def _handle_control_request(
        self, parsed: dict, body: bytes
    ) -> None:
        """Handle a request that arrived fully over the control channel."""
        request = RelayedHttpListenerRequest(
            http_method=parsed["method"],
            url=parsed["requestTarget"],
            headers=parsed.get("requestHeaders", {}),
            body=body,
        )
        rendezvous_address = parsed.get("address")
        request_id = parsed.get("id")

        await self._invoke_request_handler(
            request=request,
            request_id=request_id,
            rendezvous_address=rendezvous_address,
            rendezvous_stream=None,
        )

    async def _handle_rendezvous_request(self, address: Optional[str]) -> None:
        """Handle a request pointer requiring a rendezvous WebSocket.

        Opens the rendezvous WebSocket using ``sb-hc-action=request``,
        reads the full request and any body frames, dispatches to the
        request handler, and keeps the rendezvous socket alive to serve
        subsequent requests from the same sender (per the protocol).
        """
        if not address:
            logger.warning("Rendezvous-pointer request missing address")
            return

        try:
            url = self._protocol_handler.build_rendezvous_request_url(address)
        except ValueError as exc:
            logger.warning("Invalid rendezvous request URL: %s", exc)
            return

        try:
            websocket = await websockets.connect(url)
        except Exception:
            logger.exception("Failed to open rendezvous request WebSocket")
            return

        stream = HybridConnectionStream(websocket, address=url)
        self._open_streams.add(stream)
        try:
            await self._serve_rendezvous_requests(stream)
        finally:
            self._open_streams.discard(stream)
            await stream.close()

    async def _serve_rendezvous_requests(
        self, stream: HybridConnectionStream
    ) -> None:
        """Loop on a rendezvous WebSocket, serving requests until closure."""
        while not stream.is_closed:
            try:
                payload, message_type = await stream.receive()
            except ConnectionError:
                return

            if message_type != "text":
                logger.warning(
                    "Rendezvous request socket received unexpected %s frame",
                    message_type,
                )
                return

            try:
                parsed = self._protocol_handler.parse_control_message(payload)
            except ValueError:
                logger.warning("Malformed message on rendezvous request socket")
                return

            if parsed.get("kind") != MESSAGE_KIND_REQUEST:
                logger.warning(
                    "Unexpected message kind %r on rendezvous request socket",
                    parsed.get("kind"),
                )
                return

            body = b""
            if parsed.get("body"):
                payload, message_type = await stream.receive()
                if message_type != "binary":
                    logger.warning(
                        "Expected binary body frame on rendezvous request, got %s",
                        message_type,
                    )
                    return
                body = payload

            request = RelayedHttpListenerRequest(
                http_method=parsed["method"],
                url=parsed["requestTarget"],
                headers=parsed.get("requestHeaders", {}),
                body=body,
            )
            await self._invoke_request_handler(
                request=request,
                request_id=parsed.get("id"),
                rendezvous_address=parsed.get("address") or stream.address,
                rendezvous_stream=stream,
            )

    async def _invoke_request_handler(
        self,
        *,
        request: RelayedHttpListenerRequest,
        request_id: Optional[str],
        rendezvous_address: Optional[str],
        rendezvous_stream: Optional[HybridConnectionStream],
    ) -> None:
        """Run user request handler and send the response."""
        response_sent: dict = {"value": False}

        async def _on_close(response: RelayedHttpListenerResponse) -> None:
            if response_sent["value"]:
                return
            response_sent["value"] = True
            await self._send_response(
                request_id=request_id,
                response=response,
                rendezvous_address=rendezvous_address,
                rendezvous_stream=rendezvous_stream,
            )

        context = RelayedHttpListenerContext(
            request,
            tracking_id=request_id,
            rendezvous_address=rendezvous_address,
            response_close_callback=_on_close,
        )

        if self.request_handler is None:
            # No handler: respond with 501 Not Implemented per .NET behavior.
            context.response.status_code = 501
            context.response.status_description = "Not Implemented"
        else:
            try:
                if asyncio.iscoroutinefunction(self.request_handler):
                    await self.request_handler(context)
                else:
                    result = self.request_handler(context)
                    if asyncio.iscoroutine(result):
                        await result
            except Exception:
                logger.exception("Request handler raised an exception")
                if not response_sent["value"] and not context.response.is_closed:
                    context.response.status_code = 500
                    context.response.status_description = "Internal Server Error"

        if not response_sent["value"]:
            await _on_close(context.response)

    async def _send_response(
        self,
        *,
        request_id: Optional[str],
        response: RelayedHttpListenerResponse,
        rendezvous_address: Optional[str],
        rendezvous_stream: Optional[HybridConnectionStream],
    ) -> None:
        body = response.get_body()
        has_body = len(body) > 0

        response_message = self._protocol_handler.build_response_message(
            request_id=request_id or "",
            status_code=response.status_code,
            status_description=response.status_description,
            headers=response.headers,
            has_body=has_body,
        )

        # Decide whether the response must use a rendezvous socket.
        needs_rendezvous_upgrade = (
            rendezvous_stream is None
            and rendezvous_address is not None
            and len(body) > CONTROL_CHANNEL_MAX_BODY_SIZE
        )

        if rendezvous_stream is not None:
            # Response over an already-established rendezvous socket.
            await rendezvous_stream.send_text(response_message)
            if has_body:
                await rendezvous_stream.send_bytes(body)
            return

        if needs_rendezvous_upgrade:
            await self._upgrade_response_to_rendezvous(
                rendezvous_address=rendezvous_address,
                response_message=response_message,
                body=body,
            )
            return

        # Default: respond over the control channel.
        if self._websocket is None:
            logger.warning(
                "Dropping response %r: control channel is not connected", request_id
            )
            return
        async with self._control_send_lock:
            await self._websocket.send(response_message)
            if has_body:
                await self._websocket.send(
                    self._protocol_handler.encode_binary_body(body)
                )

    async def _upgrade_response_to_rendezvous(
        self,
        *,
        rendezvous_address: str,
        response_message: str,
        body: bytes,
    ) -> None:
        url = self._protocol_handler.build_rendezvous_request_url(rendezvous_address)
        websocket = await websockets.connect(url)
        stream = HybridConnectionStream(websocket, address=url)
        self._open_streams.add(stream)
        try:
            await stream.send_text(response_message)
            if body:
                await stream.send_bytes(body)
        finally:
            # Close after sending the response. If the sender remains connected,
            # the service will tear the socket down for us when it sees EOF.
            self._open_streams.discard(stream)
            await stream.close()

    # ------------------------------------------------------------------ #
    # WebSocket rendezvous (accept) handling
    # ------------------------------------------------------------------ #

    async def _handle_accept(self, parsed: dict) -> None:
        """Process an accept message: invoke ``accept_handler`` and connect."""
        address = parsed.get("address")
        if not address:
            logger.warning("Accept message missing address")
            return

        tracking_id = parsed.get("id")
        connect_headers = parsed.get("connect_headers", {}) or {}

        # Build a synthetic request from the connect headers for inspection.
        host = connect_headers.get("Host") or connect_headers.get("host") or ""
        synthetic_request = RelayedHttpListenerRequest(
            http_method="GET",
            url=f"wss://{host}" if host else address,
            headers=connect_headers,
        )
        context = RelayedHttpListenerContext(
            synthetic_request,
            tracking_id=tracking_id,
            is_websocket_upgrade=True,
            rendezvous_address=address,
        )

        accept = True
        if self.accept_handler is not None:
            try:
                result = self.accept_handler(context)
                if asyncio.iscoroutine(result):
                    result = await result
                if result is None:
                    accept = True
                else:
                    accept = bool(result)
            except Exception:
                logger.exception("AcceptHandler raised an exception; rejecting")
                if not context.response.is_closed:
                    context.response.status_code = 502
                    context.response.status_description = (
                        "The Listener's custom AcceptHandler threw an exception."
                    )
                accept = False

        if accept:
            await self._accept_rendezvous(address, tracking_id, connect_headers)
        else:
            # If the handler did not explicitly set a non-200 status, default
            # to 400 / "Rejected by user code" to mirror the .NET behavior.
            status_code = context.response.status_code
            status_description = context.response.status_description
            if status_code == 200:
                status_code = 400
                if not status_description or status_description == "OK":
                    status_description = "Rejected by user code"
            await self._reject_rendezvous(
                address=address,
                status_code=status_code,
                status_description=status_description or "Rejected by user code",
            )

    async def _accept_rendezvous(
        self,
        address: str,
        tracking_id: Optional[str],
        connect_headers: dict,
    ) -> None:
        try:
            url = self._protocol_handler.build_rendezvous_accept_url(address)
            websocket = await websockets.connect(url)
        except Exception:
            logger.exception("Failed to open rendezvous accept WebSocket")
            return

        stream = HybridConnectionStream(
            websocket,
            tracking_id=tracking_id,
            connect_headers=connect_headers,
            address=url,
        )
        self._open_streams.add(stream)
        await self._pending_connections.put(stream)

    async def _reject_rendezvous(
        self,
        *,
        address: str,
        status_code: int,
        status_description: str,
    ) -> None:
        try:
            url = self._protocol_handler.build_rendezvous_reject_url(
                address=address,
                status_code=status_code,
                status_description=status_description,
            )
        except ValueError:
            logger.exception("Invalid reject URL")
            return

        try:
            # Connecting to the reject URL is expected to fail: the service
            # surfaces the rejection back to the sender and closes the
            # handshake with HTTP 410 Gone, which raises an exception in
            # the WebSocket client. Any HTTP 410 / InvalidStatus is the
            # successful path; any other failure is logged at WARNING for
            # diagnosability. If for some reason the WebSocket succeeds
            # we still close it (the rejection has already taken effect).
            ws = await websockets.connect(url)
        except Exception as exc:
            message = str(exc)
            if "410" in message:
                logger.debug("Rendezvous reject handshake completed (HTTP 410)")
            else:
                logger.warning(
                    "Rendezvous reject handshake failed with unexpected error: %s",
                    exc,
                )
            return

        try:
            await ws.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Misc
    # ------------------------------------------------------------------ #

    def _split_address(self) -> "tuple[str, str]":
        address_without_scheme = self._address.replace("sb://", "").replace("https://", "")
        parts = address_without_scheme.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid address format: {self._address}")
        return parts[0], parts[1]

    async def _token_renewal_loop(self) -> None:
        """
        Background loop to monitor token expiration and renew before expiry.
        
        This method runs continuously while the listener is open, checking
        the token expiration time and renewing it when needed.
        """
        try:
            while self._is_online and self._current_token:
                # Calculate time until token expires
                expires_in = self._current_token.expires_in_seconds()
                
                # Renew when 50% of validity remains (or if already expired)
                token_validity = self._token_provider.token_validity_seconds
                renewal_threshold = token_validity * 0.5
                
                if expires_in <= renewal_threshold:
                    # Time to renew the token
                    try:
                        # Generate a new token
                        new_token = self._token_provider.get_token(self._address)
                        
                        # Send renewToken message to the relay
                        if self._websocket:
                            renew_message = self._protocol_handler.build_renew_token_message(
                                new_token.token
                            )
                            async with self._control_send_lock:
                                await self._websocket.send(renew_message)
                            
                            # Update the current token
                            self._current_token = new_token
                    
                    except Exception:
                        # Token renewal failed
                        # Log or handle renewal failure
                        # For now, we'll continue and try again on next iteration
                        pass
                
                # Wait before checking again (check every 10% of validity period, or 1 second minimum)
                check_interval = max(1, token_validity * 0.1)
                await asyncio.sleep(check_interval)
                
        except asyncio.CancelledError:
            # Task was cancelled (listener closing)
            pass
        except Exception:
            # Unexpected error in renewal loop
            pass
    
    async def _reconnect_loop(self) -> None:
        """
        Background loop to handle automatic reconnection with exponential backoff.
        
        This method attempts to reconnect to the relay service when the connection
        is lost, using exponential backoff between attempts.
        """
        try:
            while self._should_reconnect and not self._is_online:
                # Calculate backoff delay (exponential: 1s, 2s, 4s, 8s, 16s, max 60s)
                self._reconnect_attempt += 1
                delay = min(2 ** (self._reconnect_attempt - 1), 60)
                
                # Wait before attempting reconnection
                await asyncio.sleep(delay)
                
                if not self._should_reconnect:
                    break
                
                try:
                    # Attempt to reconnect
                    await self._connect()
                    
                    # If we get here, connection succeeded
                    break
                    
                except Exception:
                    # Connection failed, will retry on next iteration
                    pass
                    
        except asyncio.CancelledError:
            # Task was cancelled
            pass
        except Exception:
            # Unexpected error in reconnect loop
            pass
    
    async def _ping_loop(self) -> None:
        """
        Background loop to send periodic ping messages to keep the connection alive.
        
        This method runs continuously while the listener is online, sending ping
        messages at regular intervals to prevent NAT timeout.
        """
        try:
            while self._is_online and self._websocket:
                # Wait for the ping interval
                await asyncio.sleep(self._ping_interval)
                
                if not self._is_online or not self._websocket:
                    break
                
                try:
                    # Send a WebSocket ping
                    pong_waiter = await self._websocket.ping()
                    
                    # Wait for pong response with timeout
                    await asyncio.wait_for(pong_waiter, timeout=10.0)
                    
                except asyncio.TimeoutError:
                    # Ping timeout - connection may be dead
                    # Trigger reconnection
                    self._is_online = False
                    if self.on_offline:
                        self.on_offline()
                    
                    if self._should_reconnect:
                        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                    break
                    
                except Exception:
                    # Other ping error
                    # Could be connection closed, etc.
                    pass
                    
        except asyncio.CancelledError:
            # Task was cancelled (listener closing)
            pass
        except Exception:
            # Unexpected error in ping loop
            pass
