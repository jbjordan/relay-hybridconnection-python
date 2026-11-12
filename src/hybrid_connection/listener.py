"""HybridConnectionListener for Azure Relay Hybrid Connections."""

import asyncio
import inspect
import logging
from contextvars import ContextVar
from uuid import uuid4
from urllib.parse import quote
from typing import Optional, Callable, Awaitable, Coroutine, Union, Any
import websockets
from websockets.exceptions import ConnectionClosed as _WSConnectionClosed

from .connection_string import RelayConnectionStringBuilder
from .token_provider import TokenProvider, SecurityToken, acquire_token
from .options import TransportOptions, RetryPolicy, ListenerLimits
from .errors import RelayError, ServerBusyError, relay_error, redact
from .runtime import get_runtime_information
from .http_stream import RequestBody, ResponseWriter
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

    def __init__(self, address: str, token_provider: TokenProvider, *,
                 transport=None, retry_policy=None, limits=None, stream_requests=False,
                 subprotocols=None):
        """
        Initialize the HybridConnectionListener.
        
        Args:
            address: The hybrid connection URI (e.g., "sb://namespace.servicebus.windows.net/path")
            token_provider: TokenProvider instance for generating SAS tokens
        """
        self._address = address
        self._token_provider = token_provider
        self.transport = transport or TransportOptions()
        self.retry_policy = retry_policy or RetryPolicy()
        self.limits = limits or ListenerLimits()
        self.stream_requests = stream_requests
        self.subprotocols = list(subprotocols or [])
        self.tracking_id = str(uuid4())
        self.last_error = None
        self._terminal_failure = False
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
        self._ping_interval = self.transport.ping_interval
        self._lifecycle_lock = asyncio.Lock()
        self._closed = False
        self._close_complete = False

        # Per-control-channel send lock so concurrent handler tasks don't
        # interleave JSON+binary frame pairs.
        self._control_send_lock = asyncio.Lock()

        # Background tasks spawned to handle individual control messages.
        # Tracked so they can be cancelled on close.
        self._dispatch_tasks: "set[asyncio.Task]" = set()
        self._dispatch_owner: ContextVar[Optional[asyncio.Task]] = ContextVar(
            "hybrid_connection_dispatch_owner", default=None
        )

        # Queue of accepted rendezvous WebSocket streams produced by
        # sender 'connect' operations. Drained via ``accept_connection()``.
        self._pending_connections: "asyncio.Queue[Optional[HybridConnectionStream]]" = asyncio.Queue(
            maxsize=self.limits.max_pending_connections
        )
        self._open_streams: "set[HybridConnectionStream]" = set()

        # Event callbacks
        self.on_connecting: Optional[Callable[[], None]] = None
        self.on_online: Optional[Callable[[], None]] = None
        self.on_offline: Optional[Callable[[], None]] = None
        self.on_stopped: Optional[Callable[[], None]] = None

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
    def from_connection_string(cls, connection_string: str, *, credential=None, **options) -> "HybridConnectionListener":
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
        if not builder.entity_path:
            raise ValueError("Connection string missing EntityPath")
        
        # Create token provider
        token_provider = builder.create_token_provider(credential)
        if token_provider is None:
            raise ValueError("Listener requires authentication")
        if "transport" not in options and builder.operation_timeout is not None:
            options["transport"] = TransportOptions(
                open_timeout=builder.operation_timeout, write_timeout=builder.operation_timeout
            )
        
        # Build the address (sb:// URI format)
        address = builder.build_uri()
        
        return cls(address, token_provider, **options)

    async def get_runtime_information(self):
        return await get_runtime_information(self._address, self._token_provider, self.transport)

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
            RuntimeError: If a cancelled shutdown must be completed first
        """
        async with self._lifecycle_lock:
            if self._should_reconnect:
                return
            if self._terminal_failure and not self._close_complete:
                raise RuntimeError("Await close() before reopening a stopped listener")
            if self._closed:
                if not self._close_complete:
                    raise RuntimeError(
                        "Listener shutdown is incomplete; await close() before reopening"
                    )
                # Previous accept waiters must retain their closed queue.
                self._pending_connections = asyncio.Queue(maxsize=self.limits.max_pending_connections)
                self._closed = False
                self._close_complete = False
                self._terminal_failure = False
            self._should_reconnect = True
            self._reconnect_attempt = 0
            opened = False
            try:
                await self._connect()
                opened = True
            finally:
                if not opened:
                    self._should_reconnect = False
                    await self._disconnect_control_channel()
    
    async def _connect(self) -> None:
        try:
            async with asyncio.timeout(self.transport.open_timeout):
                await self._connect_core()
        except TimeoutError as exc:
            await self._disconnect_control_channel()
            self.last_error = relay_error(exc, tracking_id=self.tracking_id, listener=True)
            if self.last_error is exc:
                raise
            raise self.last_error from exc

    async def _connect_core(self) -> None:
        """
        Internal method to establish the WebSocket connection.
        """
        try:
            if self.on_connecting:
                self.on_connecting()

            namespace, path = self._split_address()
            
            # Get a token for authentication
            async with asyncio.timeout(self.transport.open_timeout):
                self._current_token = await acquire_token(self._token_provider, self._address)
            
            # Build the WebSocket control channel URL
            ws_url = self._protocol_handler.build_control_channel_url(
                namespace=namespace,
                path=path,
                token=self._current_token.token
            )
            ws_url += "&sb-hc-id=" + quote(self.tracking_id, safe="")
            
            # Connect to the WebSocket
            self._websocket = await self.transport.connect(
                ws_url, default_factory=websockets.connect, ping_interval=None
            )
            
            # Connection established - we're now online
            # Note: The control channel doesn't receive an initial message;
            # it's a persistent connection that receives 'request' or 'accept'
            # messages when senders connect
            
            self._is_online = True
            self._reconnect_attempt = 0  # Reset reconnect attempts on successful connection
            self.last_error = None
            logger.info("Listener online", extra={"tracking_id": self.tracking_id})
            
            # Fire online event
            if self.on_online:
                self.on_online()
            
            # Start receiving messages in the background
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            # Start token renewal task
            self._token_renewal_task = asyncio.create_task(self._token_renewal_loop())
            
            # Start ping/keepalive task
            if self._ping_interval is not None:
                self._ping_task = asyncio.create_task(self._ping_loop())
                
        except Exception as e:
            await self._disconnect_control_channel()
            self.last_error = relay_error(e, tracking_id=self.tracking_id, listener=True)
            self.last_error.args = (f"Failed to open listener: {self.last_error}",)
            if self.on_offline:
                self.on_offline()
            if self.last_error is e:
                raise
            raise self.last_error from e

    async def close(self) -> None:
        """
        Close the listener and disconnect from the relay service.
        
        This method gracefully closes the WebSocket control channel,
        cancels in-flight dispatch tasks, and drains any queued or open
        rendezvous streams. If cancelled, close() can be retried; finish
        shutdown before reopening the listener.
        """
        # A cancelled handler may call close() from its own finally block.
        # It must not wait on the shutdown that is waiting for that handler.
        if (
            self._closed
            and not self._close_complete
            and self._lifecycle_lock.locked()
            and self._dispatch_owner.get() in self._dispatch_tasks
        ):
            return
        async with self._lifecycle_lock:
            if self._close_complete:
                return
            self._closed = True
            self._should_reconnect = False
            was_online = self._is_online
            self._is_online = False

            queued_streams = set()
            while not self._pending_connections.empty():
                stream = self._pending_connections.get_nowait()
                if stream is not None:
                    queued_streams.add(stream)
            self._open_streams.update(queued_streams)
            self._pending_connections.put_nowait(None)

            await self._cancel_tasks([self._reconnect_task])
            await self._disconnect_control_channel()
            await self._cancel_tasks(list(self._dispatch_tasks))
            self._dispatch_tasks.clear()

            for stream in queued_streams | self._open_streams:
                try:
                    await stream.close()
                except Exception as exc:
                    logger.warning("Failed to close a listener-owned stream: %s", redact(exc))
                    stream.abort()
            self._open_streams.clear()
            self._close_complete = True

            if was_online and self.on_offline:
                self.on_offline()
            if self.on_stopped:
                self.on_stopped()

    async def _cancel_tasks(self, tasks: list[Optional[asyncio.Task]]) -> None:
        current = asyncio.current_task()
        owner = self._dispatch_owner.get()
        tasks = [
            task for task in tasks
            if task is not None and task is not current and task is not owner
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("Listener background task failed: %s", redact(result),
                             extra={"tracking_id": self.tracking_id})

    async def _disconnect_control_channel(self) -> None:
        self._is_online = False
        websocket = self._websocket
        self._websocket = None
        try:
            await self._cancel_tasks(
                [self._receive_task, self._ping_task, self._token_renewal_task]
            )
            if websocket is not None:
                try:
                    async with asyncio.timeout(self.transport.close_timeout):
                        await websocket.close()
                except Exception as exc:
                    logger.warning("Failed to close the control WebSocket: %s", redact(exc))
                    websocket.transport.abort()
        except asyncio.CancelledError:
            self._websocket = websocket
            raise

    def _on_connection_lost(self, websocket: Any, error=None) -> None:
        if websocket is not self._websocket or not self._is_online:
            return
        self._is_online = False
        self.last_error = relay_error(
            error or ConnectionError("Control connection lost"),
            tracking_id=self.tracking_id, listener=True,
        )
        logger.warning("Listener disconnected: %s", self.last_error,
                       extra={"tracking_id": self.tracking_id,
                              "close_code": self.last_error.close_code})
        if self._should_reconnect and (
            self._reconnect_task is None or self._reconnect_task.done()
        ):
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
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

        Raises:
            ConnectionError: If the listener is closed while waiting.
        """
        queue = self._pending_connections
        if not self._should_reconnect and self.last_error is not None and queue.empty():
            raise self.last_error
        stream = await queue.get()
        if stream is None:
            queue.put_nowait(None)
            raise ConnectionError("HybridConnectionListener is closed")
        # The stream is being handed off to the caller; remove it from the
        # set tracked for shutdown cleanup.
        self._open_streams.discard(stream)
        return stream

    async def connections(self):
        """Async iterator over accepted rendezvous WebSocket streams.

        ``async for stream in listener.connections(): ...`` yields each
        accepted ``HybridConnectionStream`` as the rendezvous succeeds.
        Closing the listener ends the iterator.
        """
        while True:
            try:
                stream = await self.accept_connection()
            except ConnectionError:
                return
            yield stream

    # ------------------------------------------------------------------ #
    # Receive loop and per-message dispatch
    # ------------------------------------------------------------------ #

    async def _receive_loop(self) -> None:
        """Read control messages and dispatch each to its own task."""
        websocket = self._websocket
        if websocket is None:
            return

        try:
            while self._is_online and self._websocket is websocket:
                message = await websocket.recv()

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
                overloaded = len(self._dispatch_tasks) >= self.limits.max_concurrent_requests
                if kind == MESSAGE_KIND_REQUEST:
                    # Body, if any, arrives as the very next frame on the
                    # control channel; read it inline before dispatching so
                    # the binary frame is matched to its request.
                    body = b""
                    if parsed.get("body"):
                        body = await self._read_body_frames(websocket)
                    if overloaded:
                        response = RelayedHttpListenerResponse()
                        response.status_code = 503
                        response.status_description = "Listener capacity exceeded"
                        await self._send_response(
                            request_id=parsed["id"], response=response,
                            rendezvous_address=parsed.get("address"), rendezvous_stream=None,
                            control_websocket=websocket,
                        )
                        continue
                    self._spawn_dispatch(
                        self._handle_control_request(
                            parsed, body, control_websocket=websocket
                        )
                    )
                elif kind == MESSAGE_KIND_REQUEST_POINTER:
                    if overloaded:
                        await self._handle_rendezvous_request(parsed.get("address"), reject=True)
                        continue
                    self._spawn_dispatch(
                        self._handle_rendezvous_request(parsed.get("address"))
                    )
                elif kind == MESSAGE_KIND_ACCEPT:
                    if overloaded or self._pending_connections.full():
                        await self._reject_rendezvous(
                            address=parsed["address"], status_code=503,
                            status_description="Listener capacity exceeded",
                        )
                        continue
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

        except _WSConnectionClosed as exc:
            self._on_connection_lost(websocket, exc)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            self._on_connection_lost(websocket, exc)

    def _spawn_dispatch(self, coro: Coroutine[Any, Any, None]) -> None:
        async def dispatch() -> None:
            token = self._dispatch_owner.set(asyncio.current_task())
            try:
                await coro
            finally:
                self._dispatch_owner.reset(token)

        def completed(task: asyncio.Task) -> None:
            # Cancellation before dispatch starts must also close its coroutine.
            coro.close()
            self._dispatch_completed(task)

        task = asyncio.create_task(dispatch())
        self._dispatch_tasks.add(task)
        task.add_done_callback(completed)

    def _dispatch_completed(self, task: asyncio.Task) -> None:
        self._dispatch_tasks.discard(task)
        if not task.cancelled():
            exception = task.exception()
            if exception is not None:
                logger.error("Failed to handle a relay message: %s", redact(exception),
                             extra={"tracking_id": self.tracking_id})

    async def _read_body_frames(self, websocket: Any) -> bytes:
        """Read binary frames from ``websocket`` until one is received.

        ``websockets`` already reassembles fragmented WebSocket messages, so
        a single ``recv()`` returns the full body content the service
        delivers for a request or response.
        """
        async with asyncio.timeout(self.limits.body_timeout):
            frame = await websocket.recv()
        if not self._protocol_handler.is_binary_message(frame):
            raise ValueError(
                "Expected binary body frame after request/response message, "
                f"got {type(frame).__name__}"
            )
        if len(frame) > CONTROL_CHANNEL_MAX_BODY_SIZE:
            raise ValueError("Control-channel body exceeds 64 KiB")
        return self._protocol_handler.decode_binary_body(frame)

    # ------------------------------------------------------------------ #
    # HTTP request handling
    # ------------------------------------------------------------------ #

    async def _handle_control_request(
        self, parsed: dict, body: bytes, *, control_websocket: Optional[Any] = None
    ) -> None:
        """Handle a request that arrived fully over the control channel."""
        if control_websocket is None:
            control_websocket = self._websocket
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
            control_websocket=control_websocket,
        )

    async def _handle_rendezvous_request(self, address: Optional[str], reject=False) -> None:
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
            logger.warning("Invalid rendezvous request URL: %s", redact(exc))
            return

        try:
            websocket = await self.transport.connect(
                url, default_factory=websockets.connect, max_size=self.limits.max_http_body_size
            )
        except Exception as exc:
            logger.warning("Failed to open rendezvous request WebSocket: %s", redact(exc))
            return

        stream = HybridConnectionStream(websocket, address=url, transport=self.transport)
        if self._closed:
            await stream.close()
            return
        self._open_streams.add(stream)
        if reject:
            try:
                async with asyncio.timeout(self.limits.body_timeout):
                    payload, _ = await stream.receive()
                parsed = self._protocol_handler.parse_control_message(payload)
                response = RelayedHttpListenerResponse()
                response.status_code = 503
                response.status_description = "Listener capacity exceeded"
                await self._send_response(
                    request_id=parsed.get("id"), response=response, rendezvous_address=url,
                    rendezvous_stream=stream, control_websocket=None,
                )
            finally:
                try:
                    await stream.close()
                except Exception:
                    stream.abort()
                    self._open_streams.discard(stream)
                    raise
                else:
                    self._open_streams.discard(stream)
            return
        await self._serve_and_close_rendezvous(stream)

    async def _serve_and_close_rendezvous(
        self, stream: HybridConnectionStream
    ) -> None:
        try:
            await self._serve_rendezvous_requests(stream)
        finally:
            await stream.close()
            self._open_streams.discard(stream)

    async def _serve_rendezvous_requests(
        self, stream: HybridConnectionStream
    ) -> None:
        """Loop on a rendezvous WebSocket, serving requests until closure."""
        async for payload, message_type in stream:
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
            body_stream = None
            if parsed.get("body"):
                if self.stream_requests:
                    body_stream = RequestBody(
                        stream.websocket, self.limits.max_http_body_size, self.limits.body_timeout
                    )
                else:
                    async with asyncio.timeout(self.limits.body_timeout):
                        payload, message_type = await stream.receive()
                    if message_type != "binary":
                        raise ValueError("Expected binary HTTP request body")
                    body = payload

            request = RelayedHttpListenerRequest(
                http_method=parsed["method"],
                url=parsed["requestTarget"],
                headers=parsed.get("requestHeaders", {}),
                body=body,
                body_stream=body_stream,
            )
            await self._invoke_request_handler(
                request=request,
                request_id=parsed.get("id"),
                rendezvous_address=parsed.get("address") or stream.address,
                rendezvous_stream=stream,
                control_websocket=None,
            )
            if body_stream is not None:
                await body_stream.drain()

    async def _invoke_request_handler(
        self,
        *,
        request: RelayedHttpListenerRequest,
        request_id: Optional[str],
        rendezvous_address: Optional[str],
        rendezvous_stream: Optional[HybridConnectionStream],
        control_websocket: Optional[Any],
    ) -> None:
        """Run user request handler and send the response."""
        response_sent: dict = {"value": False}
        writer = None
        response_stream = rendezvous_stream
        owns_stream = False
        deadline = None

        async def _flush(response):
            nonlocal writer, response_stream, owns_stream
            if self._closed:
                raise ConnectionError("Listener is closed")
            data = response.output_stream.take()
            if not data:
                return
            if writer is None:
                message = self._protocol_handler.build_response_message(
                    request_id, response.status_code, response.status_description,
                    response.headers, has_body=True,
                )
                response._started = True
                if response_stream is None:
                    if not rendezvous_address:
                        raise ConnectionError("Streaming requires a rendezvous address")
                    url = self._protocol_handler.build_rendezvous_request_url(rendezvous_address)
                    ws = await self.transport.connect(
                        url, default_factory=websockets.connect,
                        max_size=self.limits.max_http_body_size,
                    )
                    response_stream = HybridConnectionStream(ws, address=url, transport=self.transport)
                    owns_stream = True
                    self._open_streams.add(response_stream)
                await response_stream.send_text(message)
                writer = ResponseWriter(response_stream.websocket, self.transport.write_timeout)
            await writer.write(data)
            if deadline is not None and not deadline.expired():
                deadline.reschedule(asyncio.get_running_loop().time() + self.limits.body_timeout)

        async def _on_close(response: RelayedHttpListenerResponse) -> None:
            if response_sent["value"]:
                return
            if writer is not None:
                await _flush(response)
                await writer.finish()
                if owns_stream:
                    self._spawn_dispatch(self._serve_and_close_rendezvous(response_stream))
            else:
                await self._send_response(
                    request_id=request_id,
                    response=response,
                    rendezvous_address=rendezvous_address,
                    rendezvous_stream=rendezvous_stream,
                    control_websocket=control_websocket,
                )
            response_sent["value"] = True
            response._started = True
            logger.info("HTTP response sent", extra={
                "tracking_id": request_id, "status_code": response.status_code,
                "channel": "rendezvous" if writer is not None or rendezvous_stream is not None else "automatic",
            })
            if deadline is not None and not deadline.expired():
                deadline.reschedule(asyncio.get_running_loop().time() + self.limits.body_timeout)

        context = RelayedHttpListenerContext(
            request,
            tracking_id=request_id,
            rendezvous_address=rendezvous_address,
            response_close_callback=_on_close,
            response_flush_callback=_flush,
            response_buffer_limit=self.limits.max_response_buffer_size,
            response_flush_interval=self.limits.response_flush_interval,
        )

        logger.debug("HTTP request received", extra={"tracking_id": request_id,
                                                    "method": request.http_method})
        try:
            async with asyncio.timeout(self.limits.request_timeout) as deadline:
                if self.request_handler is None:
                    context.response.status_code = 501
                    context.response.status_description = "Not Implemented"
                else:
                    try:
                        result = self.request_handler(context)
                        if inspect.isawaitable(result):
                            await result
                    except Exception as exc:
                        if context.response.failure is not None or context.response.has_started:
                            raise
                        logger.warning("Request handler failed: %s", redact(exc),
                                       extra={"tracking_id": request_id})
                        if not response_sent["value"]:
                            context.response.output_stream.take()
                            context.response.headers.clear()
                            context.response.status_code = 500
                            context.response.status_description = "Internal Server Error"
                if not response_sent["value"]:
                    await context.response.close()
        except TimeoutError as exc:
            if not context.response.has_started and context.response.failure is None:
                await context.response.cancel_pending_flush()
                if not context.response.is_closed:
                    context.response.output_stream.take()
                    context.response.headers.clear()
                    context.response.status_code = 500
                    context.response.status_description = "Request deadline exceeded"
                    await context.response.close()
            raise relay_error(exc, tracking_id=request_id) from exc
        finally:
            await context.response.cancel_pending_flush()
            if not response_sent["value"]:
                if writer is not None:
                    await writer.abort()
                if response_stream is not None:
                    await response_stream.close()
                    self._open_streams.discard(response_stream)

    async def _send_response(
        self,
        *,
        request_id: Optional[str],
        response: RelayedHttpListenerResponse,
        rendezvous_address: Optional[str],
        rendezvous_stream: Optional[HybridConnectionStream],
        control_websocket: Optional[Any],
    ) -> None:
        if self._closed:
            raise ConnectionError("Cannot send response: listener is closed")
        body = response.get_body()
        has_body = len(body) > 0

        response_message = self._protocol_handler.build_response_message(
            request_id=request_id or "",
            status_code=response.status_code,
            status_description=response.status_description,
            headers=response.headers,
            has_body=has_body,
        )

        if rendezvous_stream is not None:
            # Response over an already-established rendezvous socket.
            await rendezvous_stream.send_text(response_message)
            if has_body:
                await rendezvous_stream.send_bytes(body)
            return

        if len(body) <= CONTROL_CHANNEL_MAX_BODY_SIZE:
            try:
                async with asyncio.timeout(self.transport.write_timeout), self._control_send_lock:
                    if (
                        control_websocket is not None
                        and control_websocket is self._websocket
                    ):
                        await control_websocket.send(response_message)
                        if has_body:
                            await control_websocket.send(
                                self._protocol_handler.encode_binary_body(body)
                            )
                        return
            except BaseException as exc:
                # After sending the envelope, a retry could interleave or duplicate
                # the body. Retire this channel instead of replaying the response.
                error = exc if isinstance(exc, Exception) else ConnectionError("Control response send cancelled")
                self._on_connection_lost(control_websocket, error)
                raise

        # A replacement control channel cannot respond to the old one's requests.
        if not rendezvous_address:
            raise ConnectionError(
                f"Cannot send response {request_id!r}: no usable control channel "
                "or rendezvous address"
            )
        await self._upgrade_response_to_rendezvous(
            rendezvous_address=rendezvous_address,
            response_message=response_message,
            body=body,
        )

    async def _upgrade_response_to_rendezvous(
        self,
        *,
        rendezvous_address: str,
        response_message: str,
        body: bytes,
    ) -> None:
        if self._closed:
            raise ConnectionError("Cannot upgrade response: listener is closed")
        url = self._protocol_handler.build_rendezvous_request_url(rendezvous_address)
        websocket = await self.transport.connect(
            url, default_factory=websockets.connect, max_size=self.limits.max_http_body_size
        )
        stream = HybridConnectionStream(websocket, address=url, transport=self.transport)
        if self._closed:
            await stream.close()
            raise ConnectionError("Listener closed while upgrading the response")
        self._open_streams.add(stream)
        handed_off = False
        try:
            await stream.send_text(response_message)
            if body:
                await stream.send_bytes(body)
            self._spawn_dispatch(self._serve_and_close_rendezvous(stream))
            handed_off = True
        finally:
            if not handed_off:
                await stream.close()
                self._open_streams.discard(stream)

    # ------------------------------------------------------------------ #
    # WebSocket rendezvous (accept) handling
    # ------------------------------------------------------------------ #

    async def _handle_accept(self, parsed: dict) -> None:
        try:
            async with asyncio.timeout(self.limits.accept_timeout):
                await self._handle_accept_core(parsed)
        except TimeoutError:
            logger.warning("Accept deadline exceeded", extra={"tracking_id": parsed.get("id")})
            if parsed.get("address"):
                async with asyncio.timeout(self.transport.open_timeout):
                    await self._reject_rendezvous(
                        address=parsed["address"], status_code=408,
                        status_description="Listener accept deadline exceeded",
                    )

    async def _handle_accept_core(self, parsed: dict) -> None:
        """Process an accept message: invoke ``accept_handler`` and connect."""
        address = parsed.get("address")
        if not address:
            logger.warning("Accept message missing address")
            return

        tracking_id = parsed.get("id")
        connect_headers = parsed.get("connect_headers", {}) or {}

        # Build a synthetic request from the connect headers for inspection.
        synthetic_request = RelayedHttpListenerRequest(
            http_method="GET",
            url=self._protocol_handler.logical_accept_url(self._address, address),
            headers=connect_headers,
        )
        context = RelayedHttpListenerContext(
            synthetic_request,
            tracking_id=tracking_id,
            is_websocket_upgrade=True,
            rendezvous_address=address,
        )

        offered = [
            item.strip() for item in
            (synthetic_request.get_header("Sec-WebSocket-Protocol") or "").split(",")
            if item.strip()
        ]
        selected = next((p for p in self.subprotocols if p in offered), None)
        if selected is not None:
            context.response.headers["Sec-WebSocket-Protocol"] = selected
        accept = True
        if self.accept_handler is not None:
            try:
                result = self.accept_handler(context)
                if inspect.isawaitable(result):
                    async with asyncio.timeout(self.limits.accept_timeout):
                        result = await result
                if result is None:
                    accept = True
                else:
                    accept = bool(result)
            except Exception as exc:
                logger.warning("AcceptHandler raised an exception; rejecting: %s", redact(exc),
                               extra={"tracking_id": tracking_id})
                if not context.response.is_closed:
                    context.response.status_code = 502
                    context.response.status_description = (
                        "The Listener's custom AcceptHandler threw an exception."
                    )
                accept = False

        selected = next((value for key, value in context.response.headers.items()
                         if key.lower() == "sec-websocket-protocol"), None)
        if accept and offered and selected not in offered:
            accept = False
            context.response.status_code = 400
            context.response.status_description = "No supported WebSocket subprotocol"
        if accept and selected and selected not in offered:
            accept = False
            context.response.status_code = 400
            context.response.status_description = "Selected subprotocol was not offered"
        if accept:
            await self._accept_rendezvous(address, tracking_id, connect_headers, selected)
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
        subprotocol: Optional[str] = None,
    ) -> None:
        try:
            url = self._protocol_handler.build_rendezvous_accept_url(address)
            # Relay forwards these extension headers as the sender's server response.
            # A client compression offer isn't a valid server negotiation response.
            websocket = await self.transport.connect(
                url, default_factory=websockets.connect, compression=None,
                subprotocols=[subprotocol] if subprotocol else None,
            )
        except Exception as exc:
            logger.warning("Failed to open rendezvous accept WebSocket: %s", redact(exc),
                           extra={"tracking_id": tracking_id})
            return

        stream = HybridConnectionStream(
            websocket,
            tracking_id=tracking_id,
            connect_headers=connect_headers,
            address=url,
            transport=self.transport,
        )
        if self._closed:
            await stream.close()
            return
        self._open_streams.add(stream)
        if self._pending_connections.full():
            logger.warning("Accepted stream queue is full", extra={"tracking_id": tracking_id})
            await stream.close(code=1013, reason="Listener capacity exceeded")
            self._open_streams.discard(stream)
        else:
            self._pending_connections.put_nowait(stream)
            logger.info("Rendezvous accepted", extra={"tracking_id": tracking_id,
                                                     "subprotocol": subprotocol})

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
        except ValueError as exc:
            logger.warning("Invalid reject URL: %s", redact(exc))
            return

        try:
            # Connecting to the reject URL is expected to fail: the service
            # surfaces the rejection back to the sender and closes the
            # handshake with HTTP 410 Gone, which raises an exception in
            # the WebSocket client. Any HTTP 410 / InvalidStatus is the
            # successful path; any other failure is logged at WARNING for
            # diagnosability. If for some reason the WebSocket succeeds
            # we still close it (the rejection has already taken effect).
            ws = await self.transport.connect(url, default_factory=websockets.connect)
        except Exception as exc:
            if getattr(getattr(exc, "response", None), "status_code", None) == 410:
                logger.debug("Rendezvous reject handshake completed (HTTP 410)")
            else:
                logger.warning(
                    "Rendezvous reject handshake failed with unexpected error: %s",
                    redact(exc),
                )
            return

        try:
            await ws.close()
        except Exception as exc:
            logger.warning("Failed to close rejection socket: %s", redact(exc))
            ws.transport.abort()

    # ------------------------------------------------------------------ #
    # Misc
    # ------------------------------------------------------------------ #

    def _split_address(self) -> "tuple[str, str]":
        return self._protocol_handler.split_relay_address(self._address)

    async def _token_renewal_loop(self) -> None:
        """
        Background loop to monitor token expiration and renew before expiry.
        
        This method runs continuously while the listener is open, checking
        the token expiration time and renewing it when needed.
        """
        websocket = self._websocket
        if websocket is None:
            return
        try:
            while (
                self._is_online
                and self._websocket is websocket
                and self._current_token
            ):
                # Calculate time until token expires
                expires_in = self._current_token.expires_in_seconds()
                
                # Renew when 50% of validity remains (or if already expired)
                token_validity = getattr(self._token_provider, "token_validity_seconds", 600)
                renewal_threshold = token_validity * 0.5
                
                if expires_in <= renewal_threshold:
                    # Time to renew the token
                    try:
                        # Generate a new token
                        async with asyncio.timeout(self.transport.open_timeout):
                            new_token = await acquire_token(self._token_provider, self._address)
                        
                        # Send renewToken message to the relay
                        renew_message = self._protocol_handler.build_renew_token_message(
                            new_token.token
                        )
                        async with asyncio.timeout(self.transport.write_timeout), self._control_send_lock:
                            if not self._is_online or self._websocket is not websocket:
                                break
                            await websocket.send(renew_message)
                            self._current_token = new_token
                        logger.info("Listener token renewed", extra={
                            "tracking_id": self.tracking_id,
                            "expires_at": new_token.expires_at_utc,
                        })

                    except (_WSConnectionClosed, OSError) as exc:
                        logger.warning("Token renewal lost the control channel: %s", redact(exc))
                        self._on_connection_lost(websocket, exc)
                        return
                    except Exception as exc:
                        logger.warning("Failed to renew the listener token; will retry: %s", redact(exc))
                
                # Wait before checking again (check every 10% of validity period, or 1 second minimum)
                check_interval = max(0.1, min(token_validity * 0.1, max(0.1, expires_in / 2)))
                await asyncio.sleep(check_interval)
                
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._on_connection_lost(websocket, exc)
    
    async def _reconnect_loop(self) -> None:
        """
        Background loop to handle automatic reconnection with exponential backoff.
        
        This method attempts to reconnect to the relay service when the connection
        is lost, using exponential backoff between attempts.
        """
        try:
            started = asyncio.get_running_loop().time()
            async with self._lifecycle_lock:
                await self._disconnect_control_channel()
            while self._should_reconnect and not self._is_online:
                # Calculate backoff delay (exponential: 1s, 2s, 4s, 8s, 16s, max 60s)
                self._reconnect_attempt += 1
                policy = self.retry_policy
                delay = policy.delay(self._reconnect_attempt)
                elapsed = asyncio.get_running_loop().time() - started
                if (
                    (self.last_error is not None and not self.last_error.is_transient)
                    or (policy.max_attempts is not None and self._reconnect_attempt > policy.max_attempts)
                    or (policy.max_elapsed is not None and elapsed + delay >= policy.max_elapsed)
                ):
                    self._should_reconnect = False
                    self._terminal_failure = True
                    if not self._pending_connections.full():
                        self._pending_connections.put_nowait(None)
                    logger.error("Listener reconnection stopped: %s", self.last_error,
                                 extra={"tracking_id": self.tracking_id})
                    if self.on_stopped:
                        self.on_stopped()
                    return
                logger.info("Listener reconnect scheduled", extra={
                    "tracking_id": self.tracking_id,
                    "attempt": self._reconnect_attempt, "delay": delay,
                })
                
                # Wait before attempting reconnection
                await asyncio.sleep(delay)
                
                try:
                    async with self._lifecycle_lock:
                        if not self._should_reconnect or self._is_online:
                            return
                        remaining = (
                            max(0, policy.max_elapsed - (asyncio.get_running_loop().time() - started))
                            if policy.max_elapsed is not None else None
                        )
                        async with asyncio.timeout(remaining):
                            await self._connect()
                except (ConnectionError, TimeoutError) as exc:
                    await self._disconnect_control_channel()
                    self.last_error = relay_error(exc, tracking_id=self.tracking_id, listener=True)
                    logger.warning("Listener reconnection failed: %s", self.last_error,
                                   extra={"tracking_id": self.tracking_id})
                else:
                    return
                    
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Listener reconnection loop failed: %s", redact(exc))
            self.last_error = relay_error(exc, tracking_id=self.tracking_id, listener=True)
            self._should_reconnect = False
            self._terminal_failure = True
            if not self._pending_connections.full():
                self._pending_connections.put_nowait(None)
            if self.on_stopped:
                self.on_stopped()
    
    async def _ping_loop(self) -> None:
        """
        Background loop to send periodic ping messages to keep the connection alive.
        
        This method runs continuously while the listener is online, sending ping
        messages at regular intervals to prevent NAT timeout.
        """
        websocket = self._websocket
        if websocket is None:
            return
        try:
            while self._is_online and self._websocket is websocket:
                # Wait for the ping interval
                await asyncio.sleep(self._ping_interval)
                
                if not self._is_online or self._websocket is not websocket:
                    break
                
                try:
                    # Send a WebSocket ping
                    async with asyncio.timeout(self.transport.write_timeout):
                        pong_waiter = await websocket.ping()
                    
                    # Wait for pong response with timeout
                    await asyncio.wait_for(pong_waiter, timeout=self.transport.ping_timeout)
                    
                except (asyncio.TimeoutError, _WSConnectionClosed, OSError) as exc:
                    logger.warning("Listener keepalive failed: %s", redact(exc))
                    self._on_connection_lost(websocket, exc)
                    break
                    
                except Exception as exc:
                    self._on_connection_lost(websocket, exc)
                    break
                    
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._on_connection_lost(websocket, exc)
