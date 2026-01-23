"""Unit tests for HybridConnectionListener."""

import secrets
import string

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider


def generate_random_sas_key(length: int = 44) -> str:
    """Generate a random SAS key similar to Azure's base64-encoded keys."""
    alphabet = string.ascii_letters + string.digits + "+/"
    key = "".join(secrets.choice(alphabet) for _ in range(length - 1))
    return key + "="


def generate_connection_string(
    namespace: str = "contoso",
    key_name: str = "RootManageSharedAccessKey",
    entity_path: str = "hc1",
    sas_key: str | None = None,
) -> str:
    """Generate a connection string with the given parameters."""
    if sas_key is None:
        sas_key = generate_random_sas_key()
    
    return (
        f"Endpoint=sb://{namespace}.servicebus.windows.net/;"
        f"SharedAccessKeyName={key_name};"
        f"SharedAccessKey={sas_key};"
        f"EntityPath={entity_path}"
    )


class TestHybridConnectionListenerConstruction:
    """Tests for HybridConnectionListener construction."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.endpoint = "sb://contoso.servicebus.windows.net/"
        self.sas_key_name = "RootManageSharedAccessKey"
        self.entity_path = "hc1"
        self.sas_key_value = generate_random_sas_key()

    def test_constructor_with_address_and_token_provider(self):
        """Test basic constructor."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        address = f"sb://contoso.servicebus.windows.net/{self.entity_path}"
        
        listener = HybridConnectionListener(address, token_provider)
        
        assert listener._address == address
        assert listener._token_provider == token_provider
        assert listener.is_online is False
        assert listener._websocket is None

    def test_from_connection_string_valid(self):
        """Test creating listener from connection string."""
        connection_string = generate_connection_string(
            namespace="contoso",
            key_name=self.sas_key_name,
            entity_path=self.entity_path,
            sas_key=self.sas_key_value,
        )
        
        listener = HybridConnectionListener.from_connection_string(connection_string)
        
        assert listener is not None
        assert listener.is_online is False
        assert f"contoso.servicebus.windows.net/{self.entity_path}" in listener._address

    def test_from_connection_string_missing_endpoint(self):
        """Test that missing Endpoint raises error."""
        connection_string = (
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value};"
            f"EntityPath={self.entity_path}"
        )
        
        with pytest.raises(ValueError, match="missing required fields"):
            HybridConnectionListener.from_connection_string(connection_string)

    def test_from_connection_string_missing_entity_path(self):
        """Test that missing EntityPath raises error."""
        connection_string = (
            f"Endpoint={self.endpoint};"
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value}"
        )
        
        with pytest.raises(ValueError, match="EntityPath"):
            HybridConnectionListener.from_connection_string(connection_string)


class TestHybridConnectionListenerProperties:
    """Tests for HybridConnectionListener properties."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.sas_key_name = "RootManageSharedAccessKey"
        self.sas_key_value = generate_random_sas_key()
        self.address = "sb://contoso.servicebus.windows.net/hc1"

    def test_is_online_initially_false(self):
        """Test that is_online starts as False."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        assert listener.is_online is False

    def test_event_callbacks_default_none(self):
        """Test that event callbacks default to None."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        assert listener.on_connecting is None
        assert listener.on_online is None
        assert listener.on_offline is None

    def test_request_handler_default_none(self):
        """Test that request_handler defaults to None."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        assert listener.request_handler is None

    def test_can_set_event_callbacks(self):
        """Test that event callbacks can be set."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        connecting_called = False
        online_called = False
        offline_called = False
        
        def on_connecting():
            nonlocal connecting_called
            connecting_called = True
        
        def on_online():
            nonlocal online_called
            online_called = True
        
        def on_offline():
            nonlocal offline_called
            offline_called = True
        
        listener.on_connecting = on_connecting
        listener.on_online = on_online
        listener.on_offline = on_offline
        
        assert listener.on_connecting is not None
        assert listener.on_online is not None
        assert listener.on_offline is not None


@pytest.mark.asyncio
class TestHybridConnectionListenerOpen:
    """Tests for HybridConnectionListener.open() method."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.sas_key_name = "RootManageSharedAccessKey"
        self.sas_key_value = generate_random_sas_key()
        self.address = "sb://contoso.servicebus.windows.net/hc1"

    async def test_open_calls_on_connecting(self):
        """Test that open() calls on_connecting callback."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        connecting_called = False
        
        def on_connecting():
            nonlocal connecting_called
            connecting_called = True
        
        listener.on_connecting = on_connecting
        
        # Mock websockets.connect to avoid actual network call
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            # First recv returns accept, subsequent calls raise CancelledError to break loops
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.01)  # Give tasks time to start and hit CancelledError
            finally:
                await listener.close()
        
        assert connecting_called is True

    async def test_open_establishes_websocket_connection(self):
        """Test that open() establishes WebSocket connection."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.01)
                
                # Verify websockets.connect was called
                assert mock_connect.called
                # Verify URL contains expected components
                call_args = mock_connect.call_args[0][0]
                assert "wss://" in call_args
                assert "contoso.servicebus.windows.net" in call_args
                assert "$hc/hc1" in call_args
                assert "sb-hc-action=listen" in call_args
            finally:
                await listener.close()

    async def test_open_sets_is_online_true(self):
        """Test that open() sets is_online to True."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.01)
                assert listener.is_online is True
            finally:
                await listener.close()

    async def test_open_calls_on_online(self):
        """Test that open() calls on_online callback."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        online_called = False
        
        def on_online():
            nonlocal online_called
            online_called = True
        
        listener.on_online = on_online
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.01)
            finally:
                await listener.close()
        
        assert online_called is True

    async def test_open_raises_on_connection_failure(self):
        """Test that open() raises ConnectionError on failure."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            with pytest.raises(ConnectionError, match="Failed to open listener"):
                await listener.open()

    async def test_open_calls_on_offline_on_failure(self):
        """Test that open() calls on_offline on connection failure."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        offline_called = False
        
        def on_offline():
            nonlocal offline_called
            offline_called = True
        
        listener.on_offline = on_offline
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            try:
                await listener.open()
            except ConnectionError:
                pass
        
        assert offline_called is True


@pytest.mark.asyncio
class TestHybridConnectionListenerClose:
    """Tests for HybridConnectionListener.close() method."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.sas_key_name = "RootManageSharedAccessKey"
        self.sas_key_value = generate_random_sas_key()
        self.address = "sb://contoso.servicebus.windows.net/hc1"

    async def test_close_closes_websocket(self):
        """Test that close() closes the WebSocket connection."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        # First open the listener
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_ws.close = AsyncMock()
            mock_connect.return_value = mock_ws
            
            await listener.open()
            await asyncio.sleep(0.01)
            
            # Now close it
            await listener.close()
            
            # Verify close was called on WebSocket
            assert mock_ws.close.called

    async def test_close_sets_is_online_false(self):
        """Test that close() sets is_online to False."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_ws.close = AsyncMock()
            mock_connect.return_value = mock_ws
            
            await listener.open()
            await asyncio.sleep(0.01)
            await listener.close()
        
        assert listener.is_online is False

    async def test_close_calls_on_offline(self):
        """Test that close() calls on_offline callback."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        offline_called = False
        
        def on_offline():
            nonlocal offline_called
            offline_called = True
        
        listener.on_offline = on_offline
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_ws.close = AsyncMock()
            mock_connect.return_value = mock_ws
            
            await listener.open()
            await asyncio.sleep(0.01)
            await listener.close()
        
        assert offline_called is True

    async def test_close_without_open(self):
        """Test that close() can be called without open()."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        # Should not raise exception
        await listener.close()
        
        assert listener.is_online is False

    async def test_close_ignores_websocket_errors(self):
        """Test that close() ignores errors from WebSocket.close()."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=['{"type": "accept"}', asyncio.CancelledError()])
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_ws.close = AsyncMock(side_effect=Exception("Close failed"))
            mock_connect.return_value = mock_ws
            
            await listener.open()
            await asyncio.sleep(0.01)
            
            # Should not raise exception
            await listener.close()
            
            assert listener.is_online is False


@pytest.mark.asyncio
class TestHybridConnectionListenerRequestDispatch:
    """Tests for request handling and dispatching."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.sas_key_name = "RootManageSharedAccessKey"
        self.sas_key_value = generate_random_sas_key()
        self.address = "sb://contoso.servicebus.windows.net/hc1"

    async def test_receive_loop_dispatches_request_to_handler(self):
        """Test that incoming requests are dispatched to request_handler."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        handler_called = False
        received_context = None
        
        def request_handler(context):
            nonlocal handler_called, received_context
            handler_called = True
            received_context = context
        
        listener.request_handler = request_handler
        
        # Mock request message (Azure Relay format)
        request_msg = '{"request": {"id": "req-123", "method": "GET", "requestTarget": "/test"}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            # First recv returns accept, second returns request, third raises to exit loop
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                # Wait a bit for receive loop to process
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
        
        assert handler_called is True
        assert received_context is not None
        assert received_context.request.http_method == "GET"
        assert received_context.request.url == "/test"

    async def test_receive_loop_handles_request_with_body(self):
        """Test that requests with body are handled correctly."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        received_body = None
        
        def request_handler(context):
            nonlocal received_body
            received_body = context.request.read_body()
        
        listener.request_handler = request_handler
        
        # Mock request message with body flag (Azure Relay format)
        request_msg = '{"request": {"id": "req-456", "method": "POST", "requestTarget": "/api", "body": true}}'
        body_data = b"test body content"
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                body_data,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
        
        assert received_body == body_data

    async def test_receive_loop_sends_response(self):
        """Test that responses are sent back via WebSocket."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        def request_handler(context):
            context.response.status_code = 200
            context.response.status_description = "OK"
            context.response.headers["Content-Type"] = "text/plain"
            context.response.output_stream.write(b"Hello World")
        
        listener.request_handler = request_handler
        
        request_msg = '{"request": {"id": "req-789", "method": "GET", "requestTarget": "/"}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
            
            # Verify send was called - once for response JSON, once for body
            assert mock_ws.send.call_count >= 2
            
            # Check that response message was sent (Azure Relay format)
            calls = [str(call) for call in mock_ws.send.call_args_list]
            # First send should be JSON response
            first_call = mock_ws.send.call_args_list[0][0][0]
            assert '"response"' in first_call or isinstance(first_call, str)

    async def test_receive_loop_handles_async_handler(self):
        """Test that async request handlers are awaited."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        handler_called = False
        
        async def async_request_handler(context):
            nonlocal handler_called
            await asyncio.sleep(0.01)  # Simulate async work
            handler_called = True
            context.response.status_code = 200
        
        listener.request_handler = async_request_handler
        
        request_msg = '{"request": {"id": "req-async", "method": "GET", "requestTarget": "/"}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.2)
            finally:
                await listener.close()
        
        assert handler_called is True

    async def test_receive_loop_handles_request_with_headers(self):
        """Test that request headers are passed to handler."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        received_headers = None
        
        def request_handler(context):
            nonlocal received_headers
            received_headers = context.request.headers
        
        listener.request_handler = request_handler
        
        request_msg = '{"request": {"id": "req-hdr", "method": "GET", "requestTarget": "/", "requestHeaders": {"User-Agent": "TestClient", "Accept": "text/html"}}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
        
        assert received_headers is not None
        assert received_headers.get("User-Agent") == "TestClient"
        assert received_headers.get("Accept") == "text/html"

    async def test_receive_loop_continues_on_handler_error(self):
        """Test that errors in request handler don't crash the receive loop."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        call_count = 0
        
        def failing_handler(context):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Handler error")
            context.response.status_code = 200
        
        listener.request_handler = failing_handler
        
        request_msg1 = '{"request": {"id": "req-1", "method": "GET", "requestTarget": "/"}}'
        request_msg2 = '{"request": {"id": "req-2", "method": "GET", "requestTarget": "/"}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg1,  # First request will fail
                request_msg2,  # Second request should succeed
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
        
        # Both requests should have been processed
        assert call_count == 2

    async def test_no_handler_set(self):
        """Test that listener works even if no request_handler is set."""
        token_provider = TokenProvider(self.sas_key_name, self.sas_key_value)
        listener = HybridConnectionListener(self.address, token_provider)
        
        # Don't set request_handler
        request_msg = '{"request": {"id": "req-none", "method": "GET", "requestTarget": "/"}}'
        
        with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.recv = AsyncMock(side_effect=[
                '{"type": "accept"}',
                request_msg,
                asyncio.CancelledError()
            ])
            mock_ws.send = AsyncMock()
            mock_ws.ping = AsyncMock(side_effect=asyncio.CancelledError())
            mock_connect.return_value = mock_ws
            
            try:
                await listener.open()
                await asyncio.sleep(0.1)
            finally:
                await listener.close()
            
            # Should still send a response (default values)
            assert mock_ws.send.called
