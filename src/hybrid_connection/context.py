"""
RelayedHttpListenerRequest and RelayedHttpListenerResponse classes 
for handling HTTP requests and responses over Azure Relay Hybrid Connections.
"""

from typing import Dict, Optional, Awaitable, Callable
from io import BytesIO


class RelayedHttpListenerRequest:
    """
    Represents an incoming HTTP request received over a Hybrid Connection.
    
    This class provides access to the HTTP method, URL, headers, and body
    of requests relayed through Azure Relay.
    """
    
    def __init__(
        self,
        http_method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None
    ):
        """
        Initialize a RelayedHttpListenerRequest.
        
        Args:
            http_method: The HTTP method (GET, POST, PUT, DELETE, etc.)
            url: The request URL
            headers: Dictionary of HTTP headers
            body: The request body as bytes
        """
        self._http_method = http_method.upper()
        self._url = url
        self._headers = headers if headers is not None else {}
        self._body = body if body is not None else b""
        self._input_stream = BytesIO(self._body)
    
    @property
    def http_method(self) -> str:
        """Get the HTTP method of the request."""
        return self._http_method
    
    @property
    def url(self) -> str:
        """Get the URL of the request."""
        return self._url
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get the HTTP headers as a dictionary."""
        return self._headers
    
    @property
    def input_stream(self) -> BytesIO:
        """Get a stream for reading the request body."""
        return self._input_stream
    
    @property
    def has_entity_body(self) -> bool:
        """Check if the request has a body."""
        return len(self._body) > 0
    
    def get_header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a header value by name (case-insensitive).
        
        Args:
            name: The header name
            default: Default value if header not found
            
        Returns:
            The header value or default
        """
        # Headers are typically case-insensitive
        name_lower = name.lower()
        for key, value in self._headers.items():
            if key.lower() == name_lower:
                return value
        return default
    
    @property
    def content_length(self) -> int:
        """Get the content length of the request body."""
        return len(self._body)
    
    @property
    def content_type(self) -> Optional[str]:
        """Get the Content-Type header value."""
        return self.get_header("Content-Type")
    
    def read_body(self) -> bytes:
        """Read the entire request body."""
        return self._body
    
    def read_body_as_string(self, encoding: str = "utf-8") -> str:
        """
        Read the request body as a string.
        
        Args:
            encoding: The character encoding to use
            
        Returns:
            The body as a string
        """
        return self._body.decode(encoding)


class RelayedHttpListenerResponse:
    """
    Represents an HTTP response to be sent over a Hybrid Connection.
    
    This class provides properties for setting the status code, headers,
    and response body for requests received via Azure Relay.
    """
    
    def __init__(
        self,
        close_callback: Optional[Callable[["RelayedHttpListenerResponse"], Awaitable[None]]] = None,
    ):
        """Initialize a RelayedHttpListenerResponse with default values.

        Args:
            close_callback: Optional async callback invoked on the first call
                to ``close()``. The listener uses this so handlers that call
                ``await context.response.close()`` cause the response to be
                sent immediately.
        """
        self._status_code = 200
        self._status_description = "OK"
        self._headers: Dict[str, str] = {}
        self._output_stream = BytesIO()
        self._is_closed = False
        self._close_callback = close_callback
    
    @property
    def status_code(self) -> int:
        """Get or set the HTTP status code."""
        return self._status_code
    
    @status_code.setter
    def status_code(self, value: int):
        """Set the HTTP status code."""
        if self._is_closed:
            raise RuntimeError("Cannot modify response after it has been closed")
        self._status_code = value
    
    @property
    def status_description(self) -> str:
        """Get or set the HTTP status description (reason phrase)."""
        return self._status_description
    
    @status_description.setter
    def status_description(self, value: Optional[str]):
        """Set the HTTP status description. ``None`` clears any value."""
        if self._is_closed:
            raise RuntimeError("Cannot modify response after it has been closed")
        # Per the .NET behavior, allow clearing the status description.
        self._status_description = value if value is not None else ""
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get the HTTP headers dictionary."""
        return self._headers
    
    @property
    def output_stream(self) -> BytesIO:
        """Get the output stream for writing the response body."""
        if self._is_closed:
            raise RuntimeError("Cannot write to response after it has been closed")
        return self._output_stream
    
    @property
    def is_closed(self) -> bool:
        """Check if the response has been closed."""
        return self._is_closed
    
    def set_header(self, name: str, value: str):
        """
        Set a response header.
        
        Args:
            name: The header name
            value: The header value
        """
        if self._is_closed:
            raise RuntimeError("Cannot modify response after it has been closed")
        self._headers[name] = value
    
    def get_body(self) -> bytes:
        """
        Get the response body as bytes.
        
        Returns:
            The response body
        """
        return self._output_stream.getvalue()
    
    async def close(self):
        """
        Close the response, marking it ready to be sent.

        If a close callback was supplied (typical when the response came from
        a listener), invoking ``close()`` triggers the response to be sent
        immediately. Subsequent calls are no-ops.
        """
        if self._is_closed:
            return
        self._is_closed = True
        if self._close_callback is not None:
            cb = self._close_callback
            self._close_callback = None
            await cb(self)


class RelayedHttpListenerContext:
    """
    Represents the context for an HTTP request/response pair over a Hybrid Connection.
    
    This class wraps both the incoming request and the outgoing response,
    providing a unified interface for handling relayed HTTP traffic.
    """
    
    def __init__(
        self,
        request: RelayedHttpListenerRequest,
        *,
        tracking_id: Optional[str] = None,
        is_websocket_upgrade: bool = False,
        rendezvous_address: Optional[str] = None,
        response_close_callback: Optional[
            Callable[["RelayedHttpListenerResponse"], Awaitable[None]]
        ] = None,
    ):
        """
        Initialize a RelayedHttpListenerContext.

        Args:
            request: The incoming HTTP request.
            tracking_id: Optional tracking id for this exchange.
            is_websocket_upgrade: True when the context represents a sender's
                WebSocket connect request being inspected by an accept
                handler (the "request" is synthetic, derived from the
                ``connectHeaders`` of the accept message).
            rendezvous_address: Optional rendezvous URL associated with this
                exchange (the listener may need it to upgrade the response).
            response_close_callback: Optional async callback passed through to
                the response; triggered on the first call to
                ``response.close()``.
        """
        self._request = request
        self._response = RelayedHttpListenerResponse(close_callback=response_close_callback)
        self._tracking_id = tracking_id
        self._is_websocket_upgrade = is_websocket_upgrade
        self._rendezvous_address = rendezvous_address
    
    @property
    def request(self) -> RelayedHttpListenerRequest:
        """Get the HTTP request."""
        return self._request
    
    @property
    def response(self) -> RelayedHttpListenerResponse:
        """Get the HTTP response."""
        return self._response

    @property
    def tracking_id(self) -> Optional[str]:
        """Get the tracking id for this exchange (may be ``None``)."""
        return self._tracking_id

    @property
    def is_websocket_upgrade(self) -> bool:
        """Whether the context represents a pending WebSocket rendezvous.

        Inside ``HybridConnectionListener.accept_handler`` this is True, and
        any status set on ``response`` is used as the WebSocket reject
        reason when the handler returns ``False``.
        """
        return self._is_websocket_upgrade

    @property
    def rendezvous_address(self) -> Optional[str]:
        """The rendezvous WebSocket address associated with this exchange."""
        return self._rendezvous_address

