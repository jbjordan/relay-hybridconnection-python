"""
Unit tests for the protocol module.

Tests the WebSocket protocol handling including:
- URL building for control channel
- Request message parsing
- Response message building
- Binary body frame handling
"""

import pytest
import json
from src.hybrid_connection.protocol import ProtocolHandler


class TestBuildControlChannelUrl:
    """Tests for building the control channel WebSocket URL."""

    def test_basic_url_construction(self):
        """Test basic URL construction with simple parameters."""
        namespace = "myrelay.servicebus.windows.net"
        path = "test-hc"
        token = "simple_token"

        url = ProtocolHandler.build_control_channel_url(namespace, path, token)

        assert url.startswith("wss://")
        assert namespace in url
        assert f"/$hc/{path}" in url
        assert "sb-hc-action=listen" in url
        assert "sb-hc-token=" in url

    def test_token_url_encoding(self):
        """Test that tokens with special characters are properly URL-encoded."""
        namespace = "myrelay.servicebus.windows.net"
        path = "test-hc"
        token = "token+with/special=chars&more"

        url = ProtocolHandler.build_control_channel_url(namespace, path, token)

        # Token should be URL-encoded (spaces, +, /, =, & should be encoded)
        assert "+" not in url.split("sb-hc-token=")[1]
        assert "token%2B" in url or "token+" not in url

    def test_url_format(self):
        """Test that the URL has the correct format."""
        namespace = "test.servicebus.windows.net"
        path = "my-connection"
        token = "abc123"

        url = ProtocolHandler.build_control_channel_url(namespace, path, token)

        expected_base = f"wss://{namespace}/$hc/{path}?sb-hc-action=listen&sb-hc-token="
        assert url.startswith(expected_base)


class TestParseRequestMessage:
    """Tests for parsing request JSON messages from the control channel."""

    def test_parse_valid_get_request(self):
        """Test parsing a valid GET request message."""
        message = json.dumps({
            "request": {
                "id": "request-123",
                "method": "GET",
                "requestTarget": "/api/test",
                "requestHeaders": {
                    "Host": "example.com",
                    "User-Agent": "TestClient/1.0"
                }
            }
        })

        result = ProtocolHandler.parse_request_message(message)

        assert result['id'] == 'request-123'
        assert result['method'] == 'GET'
        assert result['requestTarget'] == '/api/test'
        assert 'requestHeaders' in result
        assert result['requestHeaders']['Host'] == 'example.com'

    def test_parse_post_request_with_body(self):
        """Test parsing a POST request with a body flag."""
        message = json.dumps({
            "request": {
                "id": "request-456",
                "method": "POST",
                "requestTarget": "/api/data",
                "requestHeaders": {
                    "Content-Type": "application/json",
                    "Content-Length": "100"
                },
                "body": True
            }
        })

        result = ProtocolHandler.parse_request_message(message)

        assert result['method'] == 'POST'
        assert result['body'] is True
        assert result['requestHeaders']['Content-Type'] == 'application/json'

    def test_parse_request_without_headers(self):
        """Test parsing a request without headers (optional field)."""
        message = json.dumps({
            "request": {
                "id": "request-789",
                "method": "DELETE",
                "requestTarget": "/api/resource/1"
            }
        })

        result = ProtocolHandler.parse_request_message(message)

        assert result['method'] == 'DELETE'
        assert result['id'] == 'request-789'

    def test_parse_invalid_json(self):
        """Test that invalid JSON raises ValueError."""
        message = "not valid json {{"

        with pytest.raises(ValueError, match="Invalid JSON"):
            ProtocolHandler.parse_request_message(message)

    def test_parse_missing_request_field(self):
        """Test that missing request field raises ValueError."""
        message = json.dumps({
            "id": "request-123",
            "method": "GET",
            "requestTarget": "/test"
        })

        with pytest.raises(ValueError, match="missing 'request' field"):
            ProtocolHandler.parse_request_message(message)

    def test_parse_wrong_message_type(self):
        """Test that wrong message structure raises ValueError."""
        message = json.dumps({
            "response": {
                "requestId": "request-123"
            }
        })

        with pytest.raises(ValueError, match="missing 'request' field"):
            ProtocolHandler.parse_request_message(message)

    def test_parse_missing_required_fields(self):
        """Test that missing required fields raise ValueError."""
        # Missing id
        message1 = json.dumps({
            "request": {
                "method": "GET",
                "requestTarget": "/test"
            }
        })

        with pytest.raises(ValueError, match="missing required field"):
            ProtocolHandler.parse_request_message(message1)

        # Missing method
        message2 = json.dumps({
            "request": {
                "id": "request-123",
                "requestTarget": "/test"
            }
        })

        with pytest.raises(ValueError, match="missing required field"):
            ProtocolHandler.parse_request_message(message2)

        # Missing requestTarget
        message3 = json.dumps({
            "request": {
                "id": "request-123",
                "method": "GET"
            }
        })

        with pytest.raises(ValueError, match="missing required field"):
            ProtocolHandler.parse_request_message(message3)


class TestBuildResponseMessage:
    """Tests for building response JSON messages."""

    def test_build_simple_success_response(self):
        """Test building a simple 200 OK response."""
        response_json = ProtocolHandler.build_response_message(
            request_id="request-123",
            status_code=200,
            status_description="OK"
        )

        data = json.loads(response_json)

        assert 'response' in data
        response = data['response']
        assert response['requestId'] == 'request-123'
        assert response['statusCode'] == 200
        assert response['statusDescription'] == 'OK'
        assert 'responseHeaders' not in response
        assert 'body' not in response

    def test_build_response_with_headers(self):
        """Test building a response with headers."""
        headers = {
            "Content-Type": "text/html",
            "Content-Length": "1234",
            "X-Custom-Header": "value"
        }

        response_json = ProtocolHandler.build_response_message(
            request_id="request-456",
            status_code=200,
            status_description="OK",
            headers=headers
        )

        data = json.loads(response_json)
        response = data['response']

        assert response['responseHeaders'] == headers
        assert response['responseHeaders']['Content-Type'] == 'text/html'

    def test_build_response_with_body(self):
        """Test building a response with body flag."""
        response_json = ProtocolHandler.build_response_message(
            request_id="request-789",
            status_code=200,
            status_description="OK",
            has_body=True
        )

        data = json.loads(response_json)
        response = data['response']

        assert response['body'] is True

    def test_build_response_with_headers_and_body(self):
        """Test building a response with both headers and body."""
        headers = {"Content-Type": "application/json"}

        response_json = ProtocolHandler.build_response_message(
            request_id="request-999",
            status_code=201,
            status_description="Created",
            headers=headers,
            has_body=True
        )

        data = json.loads(response_json)
        response = data['response']

        assert response['statusCode'] == 201
        assert response['responseHeaders']['Content-Type'] == 'application/json'
        assert response['body'] is True

    def test_build_error_responses(self):
        """Test building various error response codes."""
        # 404 Not Found
        response_json = ProtocolHandler.build_response_message(
            request_id="req-1",
            status_code=404,
            status_description="Not Found"
        )
        data = json.loads(response_json)
        assert data['response']['statusCode'] == 404

        # 500 Internal Server Error
        response_json = ProtocolHandler.build_response_message(
            request_id="req-2",
            status_code=500,
            status_description="Internal Server Error"
        )
        data = json.loads(response_json)
        assert data['response']['statusCode'] == 500

    def test_build_response_is_valid_json(self):
        """Test that the built response is valid JSON."""
        response_json = ProtocolHandler.build_response_message(
            request_id="test-id",
            status_code=200,
            status_description="OK"
        )

        # Should not raise an exception
        parsed = json.loads(response_json)
        assert isinstance(parsed, dict)


class TestBuildRenewTokenMessage:
    """Tests for building renewToken messages."""

    def test_build_renew_token_message(self):
        """Test building a renewToken message."""
        token = "new_token_value"

        message_json = ProtocolHandler.build_renew_token_message(token)

        message = json.loads(message_json)

        assert message['type'] == 'renewToken'
        assert message['token'] == token

    def test_renew_token_is_valid_json(self):
        """Test that the renewToken message is valid JSON."""
        message_json = ProtocolHandler.build_renew_token_message("test_token")

        # Should not raise an exception
        parsed = json.loads(message_json)
        assert isinstance(parsed, dict)


class TestParseAcceptMessage:
    """Tests for parsing accept messages."""

    def test_parse_valid_accept_message(self):
        """Test parsing a valid accept message."""
        message = json.dumps({
            "type": "accept"
        })

        result = ProtocolHandler.parse_accept_message(message)

        assert result['type'] == 'accept'

    def test_parse_accept_with_additional_fields(self):
        """Test parsing accept message with additional fields."""
        message = json.dumps({
            "type": "accept",
            "extra": "data"
        })

        result = ProtocolHandler.parse_accept_message(message)

        assert result['type'] == 'accept'
        assert result['extra'] == 'data'

    def test_parse_invalid_accept_json(self):
        """Test that invalid JSON raises ValueError."""
        message = "not json"

        with pytest.raises(ValueError, match="Invalid JSON"):
            ProtocolHandler.parse_accept_message(message)

    def test_parse_wrong_type(self):
        """Test that wrong message type raises ValueError."""
        message = json.dumps({
            "type": "request"
        })

        with pytest.raises(ValueError, match="Expected message type 'accept'"):
            ProtocolHandler.parse_accept_message(message)

    def test_parse_missing_type(self):
        """Test that missing type field raises ValueError."""
        message = json.dumps({
            "data": "value"
        })

        with pytest.raises(ValueError, match="missing 'type' field"):
            ProtocolHandler.parse_accept_message(message)


class TestBinaryBodyHandling:
    """Tests for binary body frame handling."""

    def test_encode_binary_body(self):
        """Test encoding body as binary frame."""
        body = b"Hello, World!"

        result = ProtocolHandler.encode_binary_body(body)

        assert result == body
        assert isinstance(result, bytes)

    def test_decode_binary_body(self):
        """Test decoding binary frame."""
        frame = b"Response body content"

        result = ProtocolHandler.decode_binary_body(frame)

        assert result == frame
        assert isinstance(result, bytes)

    def test_encode_empty_body(self):
        """Test encoding an empty body."""
        body = b""

        result = ProtocolHandler.encode_binary_body(body)

        assert result == b""

    def test_decode_empty_frame(self):
        """Test decoding an empty frame."""
        frame = b""

        result = ProtocolHandler.decode_binary_body(frame)

        assert result == b""

    def test_encode_large_body(self):
        """Test encoding a large body."""
        body = b"X" * 10000

        result = ProtocolHandler.encode_binary_body(body)

        assert result == body
        assert len(result) == 10000


class TestMessageTypeChecking:
    """Tests for message type checking utilities."""

    def test_is_text_message_with_string(self):
        """Test that strings are recognized as text messages."""
        message = "text message"

        assert ProtocolHandler.is_text_message(message) is True

    def test_is_text_message_with_bytes(self):
        """Test that bytes are not recognized as text messages."""
        message = b"binary message"

        assert ProtocolHandler.is_text_message(message) is False

    def test_is_binary_message_with_bytes(self):
        """Test that bytes are recognized as binary messages."""
        message = b"binary message"

        assert ProtocolHandler.is_binary_message(message) is True

    def test_is_binary_message_with_bytearray(self):
        """Test that bytearray is recognized as binary message."""
        message = bytearray(b"binary message")

        assert ProtocolHandler.is_binary_message(message) is True

    def test_is_binary_message_with_string(self):
        """Test that strings are not recognized as binary messages."""
        message = "text message"

        assert ProtocolHandler.is_binary_message(message) is False

    def test_is_text_message_with_other_types(self):
        """Test that other types are not recognized as text messages."""
        assert ProtocolHandler.is_text_message(123) is False
        assert ProtocolHandler.is_text_message(None) is False
        assert ProtocolHandler.is_text_message([]) is False

    def test_is_binary_message_with_other_types(self):
        """Test that other types are not recognized as binary messages."""
        assert ProtocolHandler.is_binary_message(123) is False
        assert ProtocolHandler.is_binary_message(None) is False
        assert ProtocolHandler.is_binary_message("string") is False
