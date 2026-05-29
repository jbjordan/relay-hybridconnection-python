"""
WebSocket protocol handling for Azure Relay Hybrid Connections.

This module handles the low-level protocol operations including:
- Building control channel WebSocket URLs
- Parsing incoming control messages (accept / request / request-pointer / renewToken)
- Building outgoing response messages
- Building rendezvous accept and reject URLs
- Building sender connect URLs
- Handling binary body frames
"""

import json
from typing import Dict, Optional, Any, Tuple
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode


# Per-message size cap that triggers a rendezvous upgrade for HTTP messages
# (matches the Azure Relay service limit for control-channel messages).
CONTROL_CHANNEL_MAX_BODY_SIZE = 64 * 1024

# Control-message kinds returned from parse_control_message.
MESSAGE_KIND_ACCEPT = "accept"
MESSAGE_KIND_REQUEST = "request"
MESSAGE_KIND_REQUEST_POINTER = "request_pointer"
MESSAGE_KIND_RENEW_TOKEN = "renew_token"
MESSAGE_KIND_UNKNOWN = "unknown"


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

        The message may be a "full" request (with method/requestTarget/headers,
        and optional `body` flag) or a rendezvous-pointer request (only carrying
        the `address` of a rendezvous WebSocket where the full request will be
        delivered, used when the request exceeds 64 kB).

        Args:
            message: The JSON message string received from the WebSocket

        Returns:
            A dictionary containing the parsed request data. For a full request:
            - id, method, requestTarget, requestHeaders, body (bool), address (optional)
            For a rendezvous-pointer request, only `address` is guaranteed.

        Raises:
            ValueError: If the message is not valid JSON or missing the
                'request' field, or has neither full-request fields nor an
                `address`.
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON message: {e}")

        # Azure Relay wraps the request in a "request" object
        if 'request' not in data:
            raise ValueError("Message missing 'request' field")

        request_data = data['request']

        # A rendezvous-pointer request has only `address`; a full request has
        # at minimum id, method, and requestTarget.
        has_full = all(k in request_data for k in ('id', 'method', 'requestTarget'))
        has_address_only = 'address' in request_data and not has_full

        if not has_full and not has_address_only:
            raise ValueError(
                "Request message missing required fields: needs either "
                "(id, method, requestTarget) or rendezvous 'address'"
            )

        return request_data

    @staticmethod
    def is_rendezvous_pointer_request(request_data: Dict[str, Any]) -> bool:
        """
        Check whether a parsed request message is a rendezvous-pointer
        (i.e. carries only an `address` and the full request must be read
        from the rendezvous WebSocket).
        """
        return 'address' in request_data and not all(
            k in request_data for k in ('id', 'method', 'requestTarget')
        )

    @staticmethod
    def parse_control_message(message: str) -> Dict[str, Any]:
        """
        Parse a JSON message from the control channel and return a typed
        result tagged with `kind`.

        Args:
            message: The JSON message string received from the WebSocket.

        Returns:
            A dict with a `kind` key. Possible kinds:

            - 'accept':   { kind, address, id, connect_headers }
            - 'request':  { kind, id, method, requestTarget, requestHeaders,
                            body (bool), address (optional rendezvous URL) }
            - 'request_pointer': { kind, address }
            - 'renew_token': { kind, token }
            - 'unknown':  { kind, raw }

        Raises:
            ValueError: If the message is not valid JSON.
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON message: {e}")

        if not isinstance(data, dict):
            return {'kind': MESSAGE_KIND_UNKNOWN, 'raw': data}

        if 'accept' in data:
            accept = data['accept'] or {}
            return {
                'kind': MESSAGE_KIND_ACCEPT,
                'address': accept.get('address'),
                'id': accept.get('id'),
                'connect_headers': accept.get('connectHeaders', {}) or {},
            }

        if 'request' in data:
            req = data['request'] or {}
            if ProtocolHandler.is_rendezvous_pointer_request(req):
                return {
                    'kind': MESSAGE_KIND_REQUEST_POINTER,
                    'address': req.get('address'),
                }
            return {
                'kind': MESSAGE_KIND_REQUEST,
                'id': req.get('id'),
                'method': req.get('method'),
                'requestTarget': req.get('requestTarget'),
                'requestHeaders': req.get('requestHeaders', {}) or {},
                'body': bool(req.get('body', False)),
                'address': req.get('address'),
            }

        if 'renewToken' in data:
            renew = data['renewToken'] or {}
            return {
                'kind': MESSAGE_KIND_RENEW_TOKEN,
                'token': renew.get('token'),
            }

        return {'kind': MESSAGE_KIND_UNKNOWN, 'raw': data}

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
        Build a ``renewToken`` JSON message for token renewal.

        The wire shape required by the Azure Relay service is:

            {
              "renewToken": {
                "token": "SharedAccessSignature sr=...&sig=...&se=...&skn=..."
              }
            }

        Args:
            token: The new SAS token.

        Returns:
            A JSON string containing the renewToken message.
        """
        return json.dumps({"renewToken": {"token": token}})

    @staticmethod
    def parse_accept_message(message: str) -> Dict[str, Any]:
        """
        Parse an 'accept' JSON message from the control channel.

        The Azure Relay service sends an `accept` notification on the control
        channel when a sender opens a new WebSocket on the Hybrid Connection.
        The real wire format is:

            {
              "accept": {
                "address": "wss://...?sb-hc-action=accept&...",
                "id": "...",
                "connectHeaders": { "Host": "...", ... }
              }
            }

        For backward compatibility this method also accepts the legacy
        ``{"type": "accept"}`` shape used in early test fixtures.

        Args:
            message: The JSON message string received from the WebSocket

        Returns:
            A dict with `address`, `id`, and `connect_headers` (plus any extra
            top-level keys for the legacy shape). When the legacy shape is
            present, `type` will be set to ``'accept'``.

        Raises:
            ValueError: If the message is not valid JSON or is not an
                accept notification.
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON message: {e}")

        if not isinstance(data, dict):
            raise ValueError("Accept message must be a JSON object")

        if 'accept' in data:
            accept = data['accept'] or {}
            return {
                'address': accept.get('address'),
                'id': accept.get('id'),
                'connect_headers': accept.get('connectHeaders', {}) or {},
            }

        if 'type' not in data:
            raise ValueError("Message missing 'type' field")

        if data['type'] != 'accept':
            raise ValueError(f"Expected message type 'accept', got '{data['type']}'")

        return data

    @staticmethod
    def build_rendezvous_accept_url(address: str) -> str:
        """
        Return the URL the listener should connect to in order to accept a
        rendezvous WebSocket. The service-provided `address` is already a
        complete URL with `sb-hc-action=accept`; this helper exists for
        symmetry with the reject path and to validate the input.

        Args:
            address: The rendezvous WebSocket URL from the accept message.

        Returns:
            The URL to connect to for accepting the rendezvous WebSocket.

        Raises:
            ValueError: If the address is empty or not a valid WebSocket URL.
        """
        if not address:
            raise ValueError("address cannot be empty")
        parsed = urlsplit(address)
        if parsed.scheme not in ("ws", "wss"):
            raise ValueError(
                f"Rendezvous address must use ws:// or wss://, got {parsed.scheme!r}"
            )
        return address

    @staticmethod
    def build_rendezvous_reject_url(
        address: str,
        status_code: int,
        status_description: str,
    ) -> str:
        """
        Build the URL the listener should connect to in order to reject a
        rendezvous WebSocket. The listener appends `sb-hc-statusCode` and
        `sb-hc-statusDescription` query parameters to the original address;
        attempting the WebSocket handshake against the resulting URL causes
        the service to surface the rejection (HTTP 410) back to the sender.

        Args:
            address: The rendezvous WebSocket URL from the accept message.
            status_code: Numeric HTTP status code for the rejection.
            status_description: Human readable reason for the rejection.

        Returns:
            The URL to connect to for rejecting the rendezvous WebSocket.

        Raises:
            ValueError: If the address is empty or invalid, or the status
                code is not a positive integer.
        """
        if not address:
            raise ValueError("address cannot be empty")
        if not isinstance(status_code, int) or status_code <= 0:
            raise ValueError("status_code must be a positive integer")

        parsed = urlsplit(address)
        if parsed.scheme not in ("ws", "wss"):
            raise ValueError(
                f"Rendezvous address must use ws:// or wss://, got {parsed.scheme!r}"
            )

        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_pairs.append(("sb-hc-statusCode", str(status_code)))
        query_pairs.append(("sb-hc-statusDescription", status_description or ""))
        new_query = urlencode(query_pairs)
        return urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment)
        )

    @staticmethod
    def build_sender_connect_url(
        namespace: str,
        path: str,
        token: Optional[str] = None,
        hc_id: Optional[str] = None,
    ) -> str:
        """
        Build the URL the sender opens a WebSocket to in order to initiate
        a Hybrid Connection rendezvous with a listener.

        Args:
            namespace: The relay namespace (e.g., "myrelay.servicebus.windows.net")
            path: The hybrid connection path (entity name and optional suffix)
            token: Optional SAS token for authentication. If omitted, the
                Hybrid Connection must be configured to allow anonymous
                senders.
            hc_id: Optional client-supplied ID for end-to-end diagnostic
                tracing; flows to the listener via the accept message.

        Returns:
            The complete WebSocket URL for the sender to connect to.
        """
        if not namespace:
            raise ValueError("namespace cannot be empty")
        if not path:
            raise ValueError("path cannot be empty")

        query_pairs = [("sb-hc-action", "connect")]
        if hc_id:
            query_pairs.append(("sb-hc-id", hc_id))
        if token:
            query_pairs.append(("sb-hc-token", token))

        query = urlencode(query_pairs, quote_via=quote)
        return f"wss://{namespace}/$hc/{path}?{query}"

    @staticmethod
    def build_rendezvous_request_url(address: str) -> str:
        """
        Return the URL the listener uses to open a rendezvous WebSocket for
        a large HTTP request. The service-provided `address` already includes
        `sb-hc-action=request`; this helper validates the URL.

        Args:
            address: The rendezvous URL from the request message.

        Returns:
            The URL to open the rendezvous WebSocket.

        Raises:
            ValueError: If the address is empty or not a WebSocket URL.
        """
        if not address:
            raise ValueError("address cannot be empty")
        parsed = urlsplit(address)
        if parsed.scheme not in ("ws", "wss"):
            raise ValueError(
                f"Rendezvous address must use ws:// or wss://, got {parsed.scheme!r}"
            )
        return address

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
