"""
RelayedHttpListenerRequest and RelayedHttpListenerResponse classes 
for handling HTTP requests and responses over Azure Relay Hybrid Connections.
"""

from typing import Dict, Optional
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
    
    def __init__(self):
        """Initialize a RelayedHttpListenerResponse with default values."""
        self._status_code = 200
        self._status_description = "OK"
        self._headers: Dict[str, str] = {}
        self._output_stream = BytesIO()
        self._is_closed = False
    
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
    def status_description(self, value: str):
        """Set the HTTP status description."""
        if self._is_closed:
            raise RuntimeError("Cannot modify response after it has been closed")
        self._status_description = value
    
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
        Close the response and finalize it for sending.
        
        This method marks the response as closed and prevents further modifications.
        In the full implementation, this would trigger sending the response via WebSocket.
        """
        if self._is_closed:
            return
        self._is_closed = True


class RelayedHttpListenerContext:
    """
    Represents the context for an HTTP request/response pair over a Hybrid Connection.
    
    This class wraps both the incoming request and the outgoing response,
    providing a unified interface for handling relayed HTTP traffic.
    """
    
    def __init__(self, request: RelayedHttpListenerRequest):
        """
        Initialize a RelayedHttpListenerContext.
        
        Args:
            request: The incoming HTTP request
        """
        self._request = request
        self._response = RelayedHttpListenerResponse()
    
    @property
    def request(self) -> RelayedHttpListenerRequest:
        """Get the HTTP request."""
        return self._request
    
    @property
    def response(self) -> RelayedHttpListenerResponse:
        """Get the HTTP response."""
        return self._response
