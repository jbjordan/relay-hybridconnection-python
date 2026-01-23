"""
Unit tests for RelayedHttpListenerRequest and RelayedHttpListenerResponse classes.
"""

import pytest
from io import BytesIO
from src.hybrid_connection.context import (
    RelayedHttpListenerRequest, 
    RelayedHttpListenerResponse,
    RelayedHttpListenerContext
)


class TestRelayedHttpListenerRequest:
    """Tests for RelayedHttpListenerRequest class."""
    
    def test_create_basic_request(self):
        """Test creating a basic request with method and URL."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        
        assert request.http_method == "GET"
        assert request.url == "/api/test"
        assert request.headers == {}
        assert not request.has_entity_body
    
    def test_http_method_uppercase(self):
        """Test that HTTP method is converted to uppercase."""
        request = RelayedHttpListenerRequest("post", "/api/test")
        assert request.http_method == "POST"
        
        request2 = RelayedHttpListenerRequest("Put", "/api/test")
        assert request2.http_method == "PUT"
    
    def test_request_with_headers(self):
        """Test request with headers."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Custom-Header": "custom-value"
        }
        request = RelayedHttpListenerRequest("GET", "/api/test", headers=headers)
        
        assert request.headers == headers
        assert len(request.headers) == 3
    
    def test_get_header_case_insensitive(self):
        """Test that get_header is case-insensitive."""
        headers = {
            "Content-Type": "application/json",
            "X-Custom-Header": "value"
        }
        request = RelayedHttpListenerRequest("GET", "/api/test", headers=headers)
        
        assert request.get_header("content-type") == "application/json"
        assert request.get_header("CONTENT-TYPE") == "application/json"
        assert request.get_header("Content-Type") == "application/json"
        assert request.get_header("x-custom-header") == "value"
    
    def test_get_header_default(self):
        """Test get_header returns default when header not found."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        
        assert request.get_header("Non-Existent") is None
        assert request.get_header("Non-Existent", "default") == "default"
    
    def test_request_with_body(self):
        """Test request with body."""
        body = b"Hello, World!"
        request = RelayedHttpListenerRequest("POST", "/api/test", body=body)
        
        assert request.has_entity_body
        assert request.content_length == 13
        assert request.read_body() == body
    
    def test_request_without_body(self):
        """Test request without body."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        
        assert not request.has_entity_body
        assert request.content_length == 0
        assert request.read_body() == b""
    
    def test_read_body_as_string(self):
        """Test reading body as string."""
        body = "Hello, World!".encode("utf-8")
        request = RelayedHttpListenerRequest("POST", "/api/test", body=body)
        
        assert request.read_body_as_string() == "Hello, World!"
    
    def test_read_body_as_string_with_encoding(self):
        """Test reading body with specific encoding."""
        body = "Héllo, Wörld!".encode("utf-8")
        request = RelayedHttpListenerRequest("POST", "/api/test", body=body)
        
        assert request.read_body_as_string("utf-8") == "Héllo, Wörld!"
    
    def test_input_stream(self):
        """Test input_stream provides BytesIO access to body."""
        body = b"Stream content"
        request = RelayedHttpListenerRequest("POST", "/api/test", body=body)
        
        stream = request.input_stream
        assert isinstance(stream, BytesIO)
        assert stream.read() == body
        
        # Test stream can be reset and re-read
        stream.seek(0)
        assert stream.read(6) == b"Stream"
    
    def test_content_type_property(self):
        """Test content_type property."""
        headers = {"Content-Type": "application/json"}
        request = RelayedHttpListenerRequest("POST", "/api/test", headers=headers)
        
        assert request.content_type == "application/json"
    
    def test_content_type_none_when_missing(self):
        """Test content_type returns None when header is missing."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        assert request.content_type is None
    
    def test_full_request(self):
        """Test creating a full request with all properties."""
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer token123",
            "Accept": "application/json"
        }
        body = b'{"key": "value"}'
        
        request = RelayedHttpListenerRequest(
            http_method="POST",
            url="https://relay.example.com/api/data",
            headers=headers,
            body=body
        )
        
        assert request.http_method == "POST"
        assert request.url == "https://relay.example.com/api/data"
        assert request.headers == headers
        assert request.has_entity_body
        assert request.content_length == 16
        assert request.content_type == "application/json"
        assert request.read_body() == body
    
    def test_empty_body_bytes(self):
        """Test request with explicit empty body."""
        request = RelayedHttpListenerRequest("POST", "/api/test", body=b"")
        
        assert not request.has_entity_body
        assert request.content_length == 0
    
    def test_various_http_methods(self):
        """Test various HTTP methods."""
        methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
        
        for method in methods:
            request = RelayedHttpListenerRequest(method, "/api/test")
            assert request.http_method == method
    
    def test_url_with_query_string(self):
        """Test URL with query string is preserved."""
        url = "/api/test?param1=value1&param2=value2"
        request = RelayedHttpListenerRequest("GET", url)
        
        assert request.url == url
    
    def test_binary_body(self):
        """Test request with binary body data."""
        # Create binary data with non-UTF8 bytes
        body = bytes([0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD])
        request = RelayedHttpListenerRequest("POST", "/api/upload", body=body)
        
        assert request.has_entity_body
        assert request.content_length == 6
        assert request.read_body() == body
    
    def test_large_body(self):
        """Test request with large body."""
        body = b"x" * 1000000  # 1MB body
        request = RelayedHttpListenerRequest("POST", "/api/upload", body=body)
        
        assert request.has_entity_body
        assert request.content_length == 1000000
        assert request.read_body() == body


class TestRelayedHttpListenerResponse:
    """Tests for RelayedHttpListenerResponse class."""
    
    def test_create_default_response(self):
        """Test creating a response with default values."""
        response = RelayedHttpListenerResponse()
        
        assert response.status_code == 200
        assert response.status_description == "OK"
        assert response.headers == {}
        assert not response.is_closed
    
    def test_set_status_code(self):
        """Test setting status code."""
        response = RelayedHttpListenerResponse()
        response.status_code = 404
        
        assert response.status_code == 404
    
    def test_set_status_description(self):
        """Test setting status description."""
        response = RelayedHttpListenerResponse()
        response.status_code = 404
        response.status_description = "Not Found"
        
        assert response.status_description == "Not Found"
    
    def test_set_headers(self):
        """Test setting response headers."""
        response = RelayedHttpListenerResponse()
        response.headers["Content-Type"] = "application/json"
        response.headers["X-Custom-Header"] = "value"
        
        assert len(response.headers) == 2
        assert response.headers["Content-Type"] == "application/json"
        assert response.headers["X-Custom-Header"] == "value"
    
    def test_set_header_method(self):
        """Test set_header method."""
        response = RelayedHttpListenerResponse()
        response.set_header("Content-Type", "text/html")
        response.set_header("Cache-Control", "no-cache")
        
        assert response.headers["Content-Type"] == "text/html"
        assert response.headers["Cache-Control"] == "no-cache"
    
    def test_output_stream(self):
        """Test output_stream for writing response body."""
        response = RelayedHttpListenerResponse()
        response.output_stream.write(b"Hello, World!")
        
        body = response.get_body()
        assert body == b"Hello, World!"
    
    def test_write_multiple_chunks(self):
        """Test writing multiple chunks to output stream."""
        response = RelayedHttpListenerResponse()
        response.output_stream.write(b"Hello, ")
        response.output_stream.write(b"World!")
        
        body = response.get_body()
        assert body == b"Hello, World!"
    
    @pytest.mark.asyncio
    async def test_close_response(self):
        """Test closing a response."""
        response = RelayedHttpListenerResponse()
        response.status_code = 200
        response.output_stream.write(b"Response body")
        
        assert not response.is_closed
        await response.close()
        assert response.is_closed
    
    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """Test that closing multiple times is safe."""
        response = RelayedHttpListenerResponse()
        await response.close()
        await response.close()  # Should not raise
        assert response.is_closed
    
    @pytest.mark.asyncio
    async def test_cannot_modify_after_close(self):
        """Test that response cannot be modified after closing."""
        response = RelayedHttpListenerResponse()
        await response.close()
        
        with pytest.raises(RuntimeError, match="Cannot modify response after it has been closed"):
            response.status_code = 404
    
    @pytest.mark.asyncio
    async def test_cannot_set_status_description_after_close(self):
        """Test that status description cannot be set after close."""
        response = RelayedHttpListenerResponse()
        await response.close()
        
        with pytest.raises(RuntimeError, match="Cannot modify response after it has been closed"):
            response.status_description = "Not Found"
    
    @pytest.mark.asyncio
    async def test_cannot_set_header_after_close(self):
        """Test that headers cannot be set after close."""
        response = RelayedHttpListenerResponse()
        await response.close()
        
        with pytest.raises(RuntimeError, match="Cannot modify response after it has been closed"):
            response.set_header("Content-Type", "text/html")
    
    @pytest.mark.asyncio
    async def test_cannot_write_to_stream_after_close(self):
        """Test that output stream cannot be accessed after close."""
        response = RelayedHttpListenerResponse()
        await response.close()
        
        with pytest.raises(RuntimeError, match="Cannot write to response after it has been closed"):
            _ = response.output_stream
    
    def test_various_status_codes(self):
        """Test setting various HTTP status codes."""
        status_codes = [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503]
        
        for code in status_codes:
            response = RelayedHttpListenerResponse()
            response.status_code = code
            assert response.status_code == code
    
    def test_empty_response(self):
        """Test creating response with no body."""
        response = RelayedHttpListenerResponse()
        response.status_code = 204
        response.status_description = "No Content"
        
        body = response.get_body()
        assert body == b""
    
    def test_json_response(self):
        """Test creating a JSON response."""
        response = RelayedHttpListenerResponse()
        response.status_code = 200
        response.set_header("Content-Type", "application/json")
        
        json_data = b'{"message": "Success", "code": 200}'
        response.output_stream.write(json_data)
        
        assert response.get_body() == json_data
        assert response.headers["Content-Type"] == "application/json"
    
    def test_html_response(self):
        """Test creating an HTML response."""
        response = RelayedHttpListenerResponse()
        response.status_code = 200
        response.set_header("Content-Type", "text/html")
        
        html = b"<html><body><h1>Hello World</h1></body></html>"
        response.output_stream.write(html)
        
        assert response.get_body() == html
    
    def test_binary_response(self):
        """Test creating response with binary data."""
        response = RelayedHttpListenerResponse()
        response.status_code = 200
        response.set_header("Content-Type", "application/octet-stream")
        
        binary_data = bytes([0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD])
        response.output_stream.write(binary_data)
        
        assert response.get_body() == binary_data
    
    def test_large_response_body(self):
        """Test response with large body."""
        response = RelayedHttpListenerResponse()
        large_data = b"x" * 100000  # 100KB
        response.output_stream.write(large_data)
        
        assert response.get_body() == large_data
    
    def test_error_response(self):
        """Test creating an error response."""
        response = RelayedHttpListenerResponse()
        response.status_code = 500
        response.status_description = "Internal Server Error"
        response.set_header("Content-Type", "application/json")
        
        error_body = b'{"error": "Internal server error occurred"}'
        response.output_stream.write(error_body)
        
        assert response.status_code == 500
        assert response.status_description == "Internal Server Error"
        assert response.get_body() == error_body


class TestRelayedHttpListenerContext:
    """Tests for RelayedHttpListenerContext class."""
    
    def test_create_context(self):
        """Test creating a context with a request."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        context = RelayedHttpListenerContext(request)
        
        assert context.request is request
        assert context.response is not None
        assert isinstance(context.response, RelayedHttpListenerResponse)
    
    def test_context_request_properties(self):
        """Test accessing request properties via context."""
        headers = {"Content-Type": "application/json"}
        body = b'{"key": "value"}'
        request = RelayedHttpListenerRequest("POST", "/api/data", headers=headers, body=body)
        context = RelayedHttpListenerContext(request)
        
        assert context.request.http_method == "POST"
        assert context.request.url == "/api/data"
        assert context.request.headers == headers
        assert context.request.read_body() == body
    
    def test_context_response_properties(self):
        """Test accessing response properties via context."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        context = RelayedHttpListenerContext(request)
        
        # Modify response
        context.response.status_code = 404
        context.response.status_description = "Not Found"
        context.response.set_header("Content-Type", "text/plain")
        context.response.output_stream.write(b"Resource not found")
        
        assert context.response.status_code == 404
        assert context.response.status_description == "Not Found"
        assert context.response.headers["Content-Type"] == "text/plain"
        assert context.response.get_body() == b"Resource not found"
    
    def test_context_lifecycle(self):
        """Test typical request/response lifecycle using context."""
        # Simulate incoming request
        headers = {"Accept": "application/json"}
        request = RelayedHttpListenerRequest("GET", "/api/status", headers=headers)
        context = RelayedHttpListenerContext(request)
        
        # Handler processes request and builds response
        assert context.request.http_method == "GET"
        assert context.request.url == "/api/status"
        
        context.response.status_code = 200
        context.response.set_header("Content-Type", "application/json")
        context.response.output_stream.write(b'{"status": "ok"}')
        
        # Verify response is ready
        assert context.response.status_code == 200
        assert context.response.get_body() == b'{"status": "ok"}'
    
    @pytest.mark.asyncio
    async def test_context_response_close(self):
        """Test closing response via context."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        context = RelayedHttpListenerContext(request)
        
        context.response.status_code = 200
        context.response.output_stream.write(b"Test response")
        
        assert not context.response.is_closed
        await context.response.close()
        assert context.response.is_closed
    
    def test_context_with_get_request(self):
        """Test context with GET request (no body)."""
        request = RelayedHttpListenerRequest("GET", "/api/users?page=1")
        context = RelayedHttpListenerContext(request)
        
        assert not context.request.has_entity_body
        assert context.request.url == "/api/users?page=1"
        
        # Build response
        context.response.status_code = 200
        context.response.set_header("Content-Type", "application/json")
        context.response.output_stream.write(b'[{"id": 1, "name": "User"}]')
        
        assert context.response.get_body() == b'[{"id": 1, "name": "User"}]'
    
    def test_context_with_post_request(self):
        """Test context with POST request (with body)."""
        body = b'{"name": "New User", "email": "user@example.com"}'
        headers = {"Content-Type": "application/json"}
        request = RelayedHttpListenerRequest("POST", "/api/users", headers=headers, body=body)
        context = RelayedHttpListenerContext(request)
        
        assert context.request.has_entity_body
        assert context.request.content_type == "application/json"
        assert context.request.read_body() == body
        
        # Build success response
        context.response.status_code = 201
        context.response.status_description = "Created"
        context.response.set_header("Content-Type", "application/json")
        context.response.output_stream.write(b'{"id": 123, "created": true}')
        
        assert context.response.status_code == 201
    
    def test_context_separate_request_response(self):
        """Test that request and response are independent objects."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        context = RelayedHttpListenerContext(request)
        
        # Request should be the same object
        assert context.request is request
        
        # Response should be a new object
        assert context.response is not request
        assert isinstance(context.response, RelayedHttpListenerResponse)
    
    def test_context_default_response_values(self):
        """Test that context creates response with default values."""
        request = RelayedHttpListenerRequest("GET", "/api/test")
        context = RelayedHttpListenerContext(request)
        
        # Response should have default values
        assert context.response.status_code == 200
        assert context.response.status_description == "OK"
        assert context.response.headers == {}
        assert not context.response.is_closed
    
    def test_context_request_immutability(self):
        """Test that request in context maintains its properties."""
        headers = {"Authorization": "Bearer token"}
        body = b"test data"
        request = RelayedHttpListenerRequest("PUT", "/api/resource", headers=headers, body=body)
        context = RelayedHttpListenerContext(request)
        
        # Request properties should remain unchanged
        assert context.request.http_method == "PUT"
        assert context.request.url == "/api/resource"
        assert context.request.headers == headers
        assert context.request.read_body() == body
