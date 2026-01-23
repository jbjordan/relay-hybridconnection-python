"""
WebSocket protocol handling for Azure Relay Hybrid Connections.

This module handles the low-level protocol operations including:
- Building control channel WebSocket URLs
- Parsing incoming request messages
- Building outgoing response messages
- Handling binary body frames
"""

import json
import base64
from typing import Dict, Optional, Any
from urllib.parse import quote


class ProtocolHandler:
    """Handles protocol-level operations for Azure Relay Hybrid Connections."""

    @staticmethod
    def build_control_channel_url(namespace: str, path: str, token: str) -> str:
        """
        Build the WebSocket URL for the control channel.

        Args:
            namespace: The relay namespace (e.g., "myrelay.servicebus.windows.net")
            path: The hybrid connection path (entity name)
            token: The SAS token for authentication

        Returns:
            The complete WebSocket URL for listening
        """
        # URL-encode the token to handle special characters
        encoded_token = quote(token, safe='')
        
        # Build the WebSocket URL with the listen action
        url = f"wss://{namespace}/$hc/{path}?sb-hc-action=listen&sb-hc-token={encoded_token}"
        
        return url

    @staticmethod
    def parse_request_message(message: str) -> Dict[str, Any]:
        """
        Parse a 'request' JSON message from the control channel.

        Args:
            message: The JSON message string received from the WebSocket

        Returns:
            A dictionary containing the parsed request data with keys:
            - id: Request ID
            - method: HTTP method (GET, POST, etc.)
            - requestTarget: The request URL/path
            - requestHeaders: Dictionary of HTTP headers
            - body: Boolean indicating if there's a body to follow
            - address: WebSocket URL for rendezvous (if present)

        Raises:
            ValueError: If the message is not valid JSON or missing required fields
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON message: {e}")

        # Azure Relay wraps the request in a "request" object
        if 'request' not in data:
            raise ValueError("Message missing 'request' field")

        request_data = data['request']

        required_fields = ['id', 'method', 'requestTarget']
        for field in required_fields:
            if field not in request_data:
                raise ValueError(f"Request message missing required field: {field}")

        return request_data

    @staticmethod
    def build_response_message(
        request_id: str,
        status_code: int,
        status_description: str,
        headers: Optional[Dict[str, str]] = None,
        has_body: bool = False
    ) -> str:
        """
        Build a 'response' JSON message for the control channel.

        Args:
            request_id: The ID of the request being responded to
            status_code: HTTP status code (e.g., 200, 404)
            status_description: HTTP status description (e.g., "OK", "Not Found")
            headers: Optional dictionary of response headers
            has_body: Whether a binary body frame will follow

        Returns:
            A JSON string containing the response message
        """
        response_data = {
            'requestId': request_id,
            'statusCode': status_code,
            'statusDescription': status_description
        }

        if headers:
            response_data['responseHeaders'] = headers

        if has_body:
            response_data['body'] = True

        # Wrap in response object (matching Azure Relay protocol)
        return json.dumps({'response': response_data})

    @staticmethod
    def build_renew_token_message(token: str) -> str:
        """
        Build a 'renewToken' JSON message for token renewal.

        Args:
            token: The new SAS token

        Returns:
            A JSON string containing the renewToken message
        """
        message = {
            'type': 'renewToken',
            'token': token
        }
        return json.dumps(message)

    @staticmethod
    def parse_accept_message(message: str) -> Dict[str, Any]:
        """
        Parse an 'accept' JSON message from the control channel.

        The 'accept' message is sent by the relay when the listener
        successfully connects.

        Args:
            message: The JSON message string received from the WebSocket

        Returns:
            A dictionary containing the parsed accept data

        Raises:
            ValueError: If the message is not valid JSON or wrong type
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON message: {e}")

        if 'type' not in data:
            raise ValueError("Message missing 'type' field")

        if data['type'] != 'accept':
            raise ValueError(f"Expected message type 'accept', got '{data['type']}'")

        return data

    @staticmethod
    def encode_binary_body(body: bytes) -> bytes:
        """
        Encode a body as a binary WebSocket frame.

        For the HTTP request/response pattern over the control channel,
        the body is sent as-is in a binary WebSocket frame.

        Args:
            body: The body content as bytes

        Returns:
            The body bytes (no additional encoding needed for WebSocket binary frames)
        """
        return body

    @staticmethod
    def decode_binary_body(frame: bytes) -> bytes:
        """
        Decode a binary WebSocket frame to get the body content.

        Args:
            frame: The binary frame received from the WebSocket

        Returns:
            The body content as bytes
        """
        return frame

    @staticmethod
    def is_text_message(message: Any) -> bool:
        """
        Check if a message is a text message (JSON).

        Args:
            message: The message to check

        Returns:
            True if the message is a string (text/JSON), False otherwise
        """
        return isinstance(message, str)

    @staticmethod
    def is_binary_message(message: Any) -> bool:
        """
        Check if a message is a binary message (body frame).

        Args:
            message: The message to check

        Returns:
            True if the message is bytes (binary), False otherwise
        """
        return isinstance(message, (bytes, bytearray))
