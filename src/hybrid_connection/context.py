"""
RelayedHttpListenerRequest and RelayedHttpListenerResponse classes 
for handling HTTP requests and responses over Azure Relay Hybrid Connections.
"""

from typing import Dict, Optional, Awaitable, Callable
from io import BytesIO
from collections.abc import MutableMapping
from http import HTTPStatus
import asyncio
import logging
from .http_stream import ResponseBuffer
from .protocol import validate_status, validate_headers
from .errors import redact

logger = logging.getLogger(__name__)


class ResponseHeaders(MutableMapping):
    """Case-insensitive headers that become immutable once transmission starts."""

    def __init__(self, response):
        self._response = response
        self._values = {}

    def _check_writable(self):
        if self._response.is_closed or self._response.has_started or self._response._closing:
            raise RuntimeError("Cannot modify response headers after transmission")

    def __getitem__(self, key):
        return self._values[key.lower()][1]

    def __setitem__(self, key, value):
        self._check_writable()
        validate_headers({key: value})
        self._values[key.lower()] = (key, value)

    def __delitem__(self, key):
        self._check_writable()
        del self._values[key.lower()]

    def __iter__(self):
        return (name for name, value in self._values.values())

    def __len__(self):
        return len(self._values)

    def copy(self):
        return dict(self)


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
        body: Optional[bytes] = None,
        body_stream=None,
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
        self._body_stream = body_stream
    
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
        return self._body_stream if self._body_stream is not None else self._input_stream
    
    @property
    def has_entity_body(self) -> bool:
        """Check if the request has a body."""
        return self._body_stream is not None or len(self._body) > 0
    
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
    def content_length(self) -> Optional[int]:
        """Get the content length of the request body."""
        return None if self._body_stream is not None else len(self._body)
    
    @property
    def content_type(self) -> Optional[str]:
        """Get the Content-Type header value."""
        return self.get_header("Content-Type")
    
    def read_body(self) -> bytes:
        """Read the entire request body."""
        if self._body_stream is not None:
            raise RuntimeError("Streaming request: use await read_body_async() or iter_body()")
        return self._body

    async def read_body_async(self) -> bytes:
        if self._body_stream is not None:
            self._body = await self._body_stream.read(-1)
            self._body_stream = None
            self._input_stream = BytesIO(self._body)
        return self._body

    async def iter_body(self, chunk_size=65536):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self._body_stream is not None:
            while chunk := await self._body_stream.read(chunk_size):
                yield chunk
        else:
            for offset in range(0, len(self._body), chunk_size):
                yield self._body[offset:offset + chunk_size]
    
    def read_body_as_string(self, encoding: str = "utf-8") -> str:
        """
        Read the request body as a string.
        
        Args:
            encoding: The character encoding to use
            
        Returns:
            The body as a string
        """
        return self.read_body().decode(encoding)


class RelayedHttpListenerResponse:
    """
    Represents an HTTP response to be sent over a Hybrid Connection.
    
    This class provides properties for setting the status code, headers,
    and response body for requests received via Azure Relay.
    """
    
    def __init__(
        self,
        close_callback: Optional[Callable[["RelayedHttpListenerResponse"], Awaitable[None]]] = None,
        *,
        flush_callback=None,
        buffer_limit=4 * 1024 * 1024,
        flush_interval=2,
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
        self._description_explicit = False
        self._headers = ResponseHeaders(self)
        self._output_stream = ResponseBuffer(self, buffer_limit)
        self._is_closed = False
        self._close_callback = close_callback
        self._flush_callback = flush_callback
        self._flush_interval = flush_interval
        self._flush_timer = None
        self._flush_task = None
        self._lock = asyncio.Lock()
        self._failure = None
        self._started = False
        self._closing = False

    @property
    def failure(self):
        return self._failure

    @property
    def has_started(self):
        return self._started

    def _schedule_flush(self):
        if self._flush_callback is None or self._flush_timer is not None:
            return
        loop = asyncio.get_running_loop()
        delay = 0 if self._output_stream.tell() > 65536 else self._flush_interval
        self._flush_timer = loop.call_later(delay, self._start_flush)

    def _start_flush(self):
        self._flush_timer = None
        if self._flush_task is not None and not self._flush_task.done():
            return
        self._flush_task = asyncio.create_task(self.flush())
        self._flush_task.add_done_callback(self._flush_completed)

    def _flush_completed(self, task):
        if not task.cancelled() and task.exception() is not None:
            logger.warning("HTTP response flush failed: %s", redact(task.exception()))

    def _cancel_timer(self):
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None

    async def flush(self):
        async with self._lock:
            if self._failure is not None:
                raise self._failure
            if self._is_closed:
                raise RuntimeError("Response is closed")
            self._cancel_timer()
            if self._flush_callback is not None and self._output_stream.tell():
                try:
                    await self._flush_callback(self)
                    self._started = True
                except BaseException as exc:
                    self._failure = exc
                    raise

    async def cancel_pending_flush(self):
        self._cancel_timer()
        if self._flush_task is not None and not self._flush_task.done():
            self._flush_task.cancel()
            await asyncio.gather(self._flush_task, return_exceptions=True)
    
    @property
    def status_code(self) -> int:
        """Get or set the HTTP status code."""
        return self._status_code
    
    @status_code.setter
    def status_code(self, value: int):
        """Set the HTTP status code."""
        if self._is_closed or self._started or self._closing:
            raise RuntimeError("Cannot modify response after it has been closed")
        validate_status(value, self._status_description)
        self._status_code = value
        if not self._description_explicit:
            self._status_description = HTTPStatus(value).phrase if value in HTTPStatus._value2member_map_ else ""
    
    @property
    def status_description(self) -> str:
        """Get or set the HTTP status description (reason phrase)."""
        return self._status_description
    
    @status_description.setter
    def status_description(self, value: Optional[str]):
        """Set the HTTP status description. ``None`` clears any value."""
        if self._is_closed or self._started or self._closing:
            raise RuntimeError("Cannot modify response after it has been closed")
        validate_status(self._status_code, value or "")
        # Per the .NET behavior, allow clearing the status description.
        self._status_description = value if value is not None else ""
        self._description_explicit = True
    
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
        if self._is_closed or self._started or self._closing:
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
        immediately. Subsequent calls are no-ops after success, or re-raise
        the failure without replaying a potentially partial response.
        """
        async with self._lock:
            if self._failure is not None:
                raise self._failure
            if self._is_closed:
                return
            self._cancel_timer()
            self._closing = True
            try:
                if self._close_callback is not None:
                    await self._close_callback(self)
                self._is_closed = True
                self._close_callback = None
            except BaseException as exc:
                self._failure = exc
                raise
            finally:
                self._closing = False


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
        response_flush_callback=None,
        response_buffer_limit=4 * 1024 * 1024,
        response_flush_interval=2,
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
        self._response = RelayedHttpListenerResponse(
            close_callback=response_close_callback, flush_callback=response_flush_callback,
            buffer_limit=response_buffer_limit, flush_interval=response_flush_interval,
        )
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

