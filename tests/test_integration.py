"""
Integration tests for HybridConnectionListener with live Azure Relay.

These tests connect to a real Azure Relay instance to validate
the full request/response cycle. They will be skipped if the
SKIP_INTEGRATION environment variable is set.
"""
import pytest
import asyncio
import os
import aiohttp
from hybrid_connection import HybridConnectionListener

# Skip all tests in this module if SKIP_INTEGRATION is set
pytestmark = [
    pytest.mark.skipif(
        os.getenv("SKIP_INTEGRATION", "false").lower() == "true",
        reason="Integration tests disabled via SKIP_INTEGRATION env var"
    ),
    pytest.mark.integration,
    pytest.mark.timeout(120)  # 2 minute timeout for integration tests
]


@pytest.fixture
def relay_url(connection_string):
    """Build the relay URL from connection string."""
    # Parse connection string to extract namespace and path
    parts = {}
    for part in connection_string.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            parts[key] = value
    
    endpoint = parts.get("Endpoint", "").replace("sb://", "")
    entity_path = parts.get("EntityPath", "")
    
    return f"https://{endpoint}/{entity_path}"


@pytest.mark.asyncio
async def test_connection_to_relay(connection_string):
    """Test that listener can connect to Azure Relay."""
    listener = HybridConnectionListener.from_connection_string(connection_string)
    
    # Track connection events
    events = []
    listener.on_connecting = lambda: events.append("connecting")
    listener.on_online = lambda: events.append("online")
    listener.on_offline = lambda: events.append("offline")
    
    # Open the listener
    await listener.open()
    
    # Wait a moment for events to fire
    await asyncio.sleep(1)
    
    # Verify we got the expected events and are online
    assert "connecting" in events
    assert "online" in events
    assert listener.is_online
    
    # Close the listener
    await listener.close()
    
    # Wait for close to complete
    await asyncio.sleep(0.5)
    
    # Verify offline event and state
    assert "offline" in events
    assert not listener.is_online


@pytest.mark.asyncio
async def test_simple_get_request_response(connection_string, relay_url):
    """Test sending a GET request and receiving a response."""
    listener = HybridConnectionListener.from_connection_string(connection_string)
    
    # Track received requests
    received_requests = []
    
    async def request_handler(context):
        """Handle incoming requests."""
        received_requests.append({
            "method": context.request.http_method,
            "url": context.request.url,
            "headers": dict(context.request.headers)
        })
        
        # Send a simple response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/plain"
        context.response.output_stream.write(b"Hello from integration test!")
        await context.response.close()
    
    listener.request_handler = request_handler
    
    # Open the listener
    await listener.open()
    
    # Wait for listener to be fully online
    await asyncio.sleep(2)
    
    try:
        # Send a request via HTTP
        async with aiohttp.ClientSession() as session:
            async with session.get(relay_url) as response:
                status = response.status
                body = await response.text()
        
        # Verify the response
        assert status == 200
        assert body == "Hello from integration test!"
        
        # Verify the listener received the request
        assert len(received_requests) == 1
        assert received_requests[0]["method"] == "GET"
    
    finally:
        await listener.close()


@pytest.mark.asyncio
async def test_post_request_with_body(connection_string, relay_url):
    """Test sending a POST request with body and receiving response."""
    listener = HybridConnectionListener.from_connection_string(connection_string)
    
    # Track received requests
    received_requests = []
    
    async def request_handler(context):
        """Handle incoming requests."""
        # Read the request body
        body = context.request.input_stream.read() if context.request.has_entity_body else b""
        
        received_requests.append({
            "method": context.request.http_method,
            "url": context.request.url,
            "body": body.decode("utf-8") if body else "",
            "has_body": context.request.has_entity_body
        })
        
        # Echo the body back in the response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/plain"
        context.response.output_stream.write(b"Received: " + body)
        await context.response.close()
    
    listener.request_handler = request_handler
    
    # Open the listener
    await listener.open()
    
    # Wait for listener to be fully online
    await asyncio.sleep(2)
    
    try:
        # Send a POST request with body
        test_body = "Test payload from integration test"
        async with aiohttp.ClientSession() as session:
            async with session.post(relay_url, data=test_body) as response:
                status = response.status
                body = await response.text()
        
        # Verify the response
        assert status == 200
        assert body == f"Received: {test_body}"
        
        # Verify the listener received the request with body
        assert len(received_requests) == 1
        assert received_requests[0]["method"] == "POST"
        assert received_requests[0]["has_body"] is True
        assert received_requests[0]["body"] == test_body
    
    finally:
        await listener.close()


@pytest.mark.asyncio
async def test_multiple_sequential_requests(connection_string, relay_url):
    """Test handling multiple sequential requests."""
    listener = HybridConnectionListener.from_connection_string(connection_string)
    
    # Track received requests
    request_count = 0
    
    async def request_handler(context):
        """Handle incoming requests."""
        nonlocal request_count
        request_count += 1
        
        # Send a simple response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/plain"
        context.response.output_stream.write(f"Response {request_count}".encode())
        await context.response.close()
    
    listener.request_handler = request_handler
    
    # Open the listener
    await listener.open()
    
    # Wait for listener to be fully online
    await asyncio.sleep(2)
    
    try:
        # Send multiple requests
        async with aiohttp.ClientSession() as session:
            for i in range(3):
                async with session.get(relay_url) as response:
                    status = response.status
                    body = await response.text()
                    
                    # Verify each response
                    assert status == 200
                    assert body == f"Response {i + 1}"
        
        # Verify all requests were received
        assert request_count == 3
    
    finally:
        await listener.close()


@pytest.mark.asyncio
async def test_request_with_custom_headers(connection_string, relay_url):
    """Test that custom headers are passed through correctly."""
    listener = HybridConnectionListener.from_connection_string(connection_string)
    
    # Track received requests
    received_headers = {}
    
    async def request_handler(context):
        """Handle incoming requests."""
        nonlocal received_headers
        received_headers = dict(context.request.headers)
        
        # Send a simple response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["X-Custom-Response"] = "test-value"
        context.response.output_stream.write(b"OK")
        await context.response.close()
    
    listener.request_handler = request_handler
    
    # Open the listener
    await listener.open()
    
    # Wait for listener to be fully online
    await asyncio.sleep(2)
    
    try:
        # Send a request with custom headers
        custom_headers = {
            "X-Custom-Header": "test-header-value",
            "X-Request-ID": "12345"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(relay_url, headers=custom_headers) as response:
                status = response.status
                response_headers = dict(response.headers)
        
        # Verify the response
        assert status == 200
        
        # Verify custom headers were received by listener
        assert "X-Custom-Header" in received_headers
        assert received_headers["X-Custom-Header"] == "test-header-value"
        assert "X-Request-ID" in received_headers
        assert received_headers["X-Request-ID"] == "12345"
        
        # Verify custom response header was returned
        assert "X-Custom-Response" in response_headers
        assert response_headers["X-Custom-Response"] == "test-value"
    
    finally:
        await listener.close()
