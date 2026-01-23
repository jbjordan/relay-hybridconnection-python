"""HybridConnectionListener for Azure Relay Hybrid Connections."""

import asyncio
from typing import Optional, Callable, TYPE_CHECKING
import websockets

if TYPE_CHECKING:
    from websockets.client import WebSocketClientProtocol

from .connection_string import RelayConnectionStringBuilder
from .token_provider import TokenProvider, SecurityToken
from .protocol import ProtocolHandler
from .context import RelayedHttpListenerContext, RelayedHttpListenerRequest


class HybridConnectionListener:
    """
    Listener for Azure Relay Hybrid Connections using HTTP request/response pattern.
    
    This class establishes a WebSocket control channel to the Azure Relay service
    and receives HTTP requests through that channel.
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
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
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
        
        # Event callbacks
        self.on_connecting: Optional[Callable[[], None]] = None
        self.on_online: Optional[Callable[[], None]] = None
        self.on_offline: Optional[Callable[[], None]] = None
        
        # Request handler
        self.request_handler: Optional[Callable] = None

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
            # Parse the address to extract namespace and path
            # Expected format: sb://namespace/path
            address_without_scheme = self._address.replace("sb://", "").replace("https://", "")
            parts = address_without_scheme.split("/", 1)
            
            if len(parts) != 2:
                raise ValueError(f"Invalid address format: {self._address}")
            
            namespace = parts[0]
            path = parts[1]
            
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
            # it's a persistent connection that receives 'request' messages
            # when senders connect
            
            self._is_online = True
            self._reconnect_attempt = 0  # Reset reconnect attempts on successful connection
            
            # Fire online event
            if self.on_online:
                self.on_online()
            
            # Start receiving requests in the background
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
        
        This method gracefully closes the WebSocket control channel.
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

    async def _receive_loop(self) -> None:
        """
        Background loop to receive and process messages from the control channel.
        
        This method runs continuously while the listener is open, receiving
        request messages and dispatching them to the request_handler.
        """
        if not self._websocket:
            return
        
        try:
            while self._is_online and self._websocket:
                # Receive a message from the WebSocket
                message = await self._websocket.recv()
                
                # Check if it's a text message (JSON request)
                if self._protocol_handler.is_text_message(message):
                    try:
                        # Parse the request message
                        request_data = self._protocol_handler.parse_request_message(message)
                        
                        # Check for body flag
                        has_body = request_data.get('body', False)
                        body_data = b""
                        
                        # If there's a body, read the next binary frame
                        if has_body:
                            body_frame = await self._websocket.recv()
                            if self._protocol_handler.is_binary_message(body_frame):
                                body_data = self._protocol_handler.decode_binary_body(body_frame)
                        
                        # Create the request object
                        request = RelayedHttpListenerRequest(
                            http_method=request_data['method'],
                            url=request_data['requestTarget'],
                            headers=request_data.get('requestHeaders', {}),
                            body=body_data
                        )
                        
                        # Create the context
                        context = RelayedHttpListenerContext(request)
                        
                        # Invoke the request handler if set
                        if self.request_handler:
                            # Handle the request (could be sync or async)
                            if asyncio.iscoroutinefunction(self.request_handler):
                                await self.request_handler(context)
                            else:
                                self.request_handler(context)
                        
                        # Send the response back
                        await self._send_response(request_data['id'], context)
                        
                    except Exception as e:
                        # Log or handle request processing errors
                        # For now, continue to next message
                        pass
                        
        except websockets.exceptions.ConnectionClosed:
            # WebSocket was closed
            self._is_online = False
            if self.on_offline:
                self.on_offline()
            
            # Trigger reconnection if enabled
            if self._should_reconnect:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                
        except Exception as e:
            # Other errors during receive loop
            self._is_online = False
            if self.on_offline:
                self.on_offline()
            
            # Trigger reconnection if enabled
            if self._should_reconnect:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _send_response(self, request_id: str, context: RelayedHttpListenerContext) -> None:
        """
        Send an HTTP response back to the relay via the WebSocket.
        
        Args:
            request_id: The ID of the request being responded to
            context: The context containing the response to send
        """
        if not self._websocket:
            return
        
        response = context.response
        
        # Get the response body
        body = response.get_body()
        has_body = len(body) > 0
        
        # Build the response message
        response_message = self._protocol_handler.build_response_message(
            request_id=request_id,
            status_code=response.status_code,
            status_description=response.status_description,
            headers=response.headers,
            has_body=has_body
        )
        
        # Send the JSON response message
        await self._websocket.send(response_message)
        
        # If there's a body, send it as a binary frame
        if has_body:
            body_frame = self._protocol_handler.encode_binary_body(body)
            await self._websocket.send(body_frame)

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
                            await self._websocket.send(renew_message)
                            
                            # Update the current token
                            self._current_token = new_token
                    
                    except Exception as e:
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
        except Exception as e:
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
                    
                except Exception as e:
                    # Connection failed, will retry on next iteration
                    pass
                    
        except asyncio.CancelledError:
            # Task was cancelled
            pass
        except Exception as e:
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
                    
                except Exception as e:
                    # Other ping error
                    # Could be connection closed, etc.
                    pass
                    
        except asyncio.CancelledError:
            # Task was cancelled (listener closing)
            pass
        except Exception as e:
            # Unexpected error in ping loop
            pass
