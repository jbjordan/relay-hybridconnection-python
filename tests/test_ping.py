"""Unit tests for ping/keepalive functionality in HybridConnectionListener."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider


@pytest.fixture
def listener():
    """Create a listener for testing."""
    token_provider = TokenProvider("test-key", "dGVzdC1rZXk=")
    return HybridConnectionListener("sb://test.servicebus.windows.net/test", token_provider)


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_ping_task_starts_on_open(listener):
    """Test that ping task is started when listener opens."""
    with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        
        # Return accept message once, then block on recv() forever
        accept_received = [False]
        async def mock_recv():
            if not accept_received[0]:
                accept_received[0] = True
                return '{"type": "accept", "address": "test"}'
            # Block forever (simulating waiting for more messages)
            await asyncio.Event().wait()
        
        mock_ws.recv = mock_recv
        mock_ws.close = AsyncMock()
        
        # Mock ping to return a completed future
        async def mock_ping():
            future = asyncio.Future()
            future.set_result(None)
            return future
        
        mock_ws.ping = mock_ping
        mock_connect.return_value = mock_ws
        
        try:
            await listener.open()
            
            # Give it a brief moment to start tasks
            await asyncio.sleep(0.01)
            
            # Verify ping task was created
            assert listener._ping_task is not None
            assert not listener._ping_task.done()
        finally:
            await listener.close()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_ping_task_cancelled_on_close(listener):
    """Test that ping task is cancelled when listener closes."""
    with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        
        # Return accept message once, then block on recv() forever
        accept_received = [False]
        async def mock_recv():
            if not accept_received[0]:
                accept_received[0] = True
                return '{"type": "accept", "address": "test"}'
            await asyncio.Event().wait()
        
        mock_ws.recv = mock_recv
        mock_ws.close = AsyncMock()
        
        # Mock ping to return a completed future
        async def mock_ping():
            future = asyncio.Future()
            future.set_result(None)
            return future
        
        mock_ws.ping = mock_ping
        mock_connect.return_value = mock_ws
        
        try:
            await listener.open()
            
            # Give it a brief moment to start tasks
            await asyncio.sleep(0.01)
            
            ping_task = listener._ping_task
            assert ping_task is not None
        finally:
            await listener.close()
        
        # Verify ping task was cancelled
        assert ping_task.done()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_ping_sent_periodically(listener):
    """Test that ping messages are sent at regular intervals."""
    # Set a shorter ping interval for testing
    listener._ping_interval = 0.1
    
    ping_calls = []
    
    async def mock_ping():
        ping_calls.append(asyncio.get_event_loop().time())
        future = asyncio.Future()
        future.set_result(None)
        return future
    
    with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        
        # Return accept message once, then block on recv() forever
        accept_received = [False]
        async def mock_recv():
            if not accept_received[0]:
                accept_received[0] = True
                return '{"type": "accept", "address": "test"}'
            await asyncio.Event().wait()
        
        mock_ws.recv = mock_recv
        mock_ws.close = AsyncMock()
        mock_ws.ping = mock_ping
        mock_connect.return_value = mock_ws
        
        try:
            await listener.open()
            
            # Wait for multiple ping intervals
            await asyncio.sleep(0.35)
        finally:
            await listener.close()
        
        # Verify multiple pings were sent
        assert len(ping_calls) >= 2


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_ping_handles_connection_closed(listener):
    """Test that ping handles closed connections gracefully."""
    listener._ping_interval = 0.1
    
    # Mock ping to raise exception
    async def mock_ping_error():
        raise Exception("Connection closed")
    
    with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        
        # Return accept message once, then block on recv() forever
        accept_received = [False]
        async def mock_recv():
            if not accept_received[0]:
                accept_received[0] = True
                return '{"type": "accept", "address": "test"}'
            await asyncio.Event().wait()
        
        mock_ws.recv = mock_recv
        mock_ws.close = AsyncMock()
        mock_ws.ping = mock_ping_error
        mock_connect.return_value = mock_ws
        
        try:
            await listener.open()
            
            # Wait for ping to be attempted
            await asyncio.sleep(0.15)
        finally:
            # Verify listener didn't crash
            await listener.close()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_ping_stops_when_offline(listener):
    """Test that ping loop stops when listener goes offline."""
    listener._ping_interval = 0.1
    
    ping_count = [0]
    
    async def mock_ping():
        ping_count[0] += 1
        future = asyncio.Future()
        future.set_result(None)
        return future
    
    with patch('src.hybrid_connection.listener.websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        
        # Return accept message once, then block on recv() forever
        accept_received = [False]
        async def mock_recv():
            if not accept_received[0]:
                accept_received[0] = True
                return '{"type": "accept", "address": "test"}'
            await asyncio.Event().wait()
        
        mock_ws.recv = mock_recv
        mock_ws.close = AsyncMock()
        mock_ws.ping = mock_ping
        mock_connect.return_value = mock_ws
        
        try:
            await listener.open()
            
            # Wait for a ping
            await asyncio.sleep(0.15)
            
            initial_count = ping_count[0]
            
            # Set offline
            listener._is_online = False
            
            # Wait some more
            await asyncio.sleep(0.2)
            
            # Verify no more pings were sent
            # (may be +1 if one was in flight)
            assert ping_count[0] <= initial_count + 1
        finally:
            await listener.close()
