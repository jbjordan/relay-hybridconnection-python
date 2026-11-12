"""
HybridConnectionStream: a duplex WebSocket stream resulting from a successful
rendezvous between a sender and a listener over Azure Relay.

The stream is a thin abstraction over an open `websockets` client connection
that exposes a small, opinionated send/receive surface so callers don't need
to depend on the underlying library directly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, Union

from websockets.exceptions import ConnectionClosed as _WSConnectionClosed
from websockets.exceptions import ConnectionClosedOK as _WSConnectionClosedOK
from .errors import relay_error
from .options import TransportOptions

logger = logging.getLogger(__name__)


# Message type constants returned alongside payloads from `receive()`.
MESSAGE_TYPE_TEXT = "text"
MESSAGE_TYPE_BINARY = "binary"


class HybridConnectionStream:
    """
    Duplex WebSocket stream produced by a Hybrid Connection rendezvous.

    For the listener side, an instance is created when the listener accepts a
    sender's rendezvous WebSocket. For the sender side, an instance is created
    by ``HybridConnectionClient.create_connection``.

    The class is intentionally minimal: it provides send/receive of text and
    binary messages, an async close, and metadata about the rendezvous
    (tracking id, connect headers, address).
    """

    def __init__(
        self,
        websocket: Any,
        *,
        tracking_id: Optional[str] = None,
        connect_headers: Optional[Dict[str, str]] = None,
        address: Optional[str] = None,
        transport: Optional[TransportOptions] = None,
    ) -> None:
        self._websocket = websocket
        self._tracking_id = tracking_id
        self._connect_headers: Dict[str, str] = dict(connect_headers or {})
        self._address = address
        self._closed = False
        self._close_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._read_fragments = None
        self._read_buffer = b""
        self._write_closed = False
        self._shutdown_timer = None
        self._transport = transport or TransportOptions()

    @property
    def websocket(self) -> Any:
        """The underlying websockets connection object."""
        return self._websocket

    @property
    def tracking_id(self) -> Optional[str]:
        """The unique id for this connection (from the accept message)."""
        return self._tracking_id

    @property
    def connect_headers(self) -> Dict[str, str]:
        """The HTTP headers the sender supplied when opening the connection."""
        return self._connect_headers

    @property
    def address(self) -> Optional[str]:
        """The rendezvous WebSocket URL this stream is connected to."""
        return self._address

    @property
    def is_closed(self) -> bool:
        """Whether the stream has been closed."""
        return self._closed

    async def send(self, data: Union[bytes, bytearray, memoryview, str]) -> None:
        """
        Send a message. ``bytes``/``bytearray``/``memoryview`` are sent as a
        binary WebSocket frame; ``str`` is sent as a text frame.
        """
        if self._closed or self._write_closed:
            raise RuntimeError("Cannot send on a closed HybridConnectionStream")
        try:
            async with self._send_lock:
                async with asyncio.timeout(self._transport.write_timeout):
                    await self._websocket.send(data)
            logger.debug("Stream message sent", extra={"tracking_id": self.tracking_id,
                                                       "size": len(data)})
        except _WSConnectionClosed as exc:
            self._closed = True
            raise relay_error(exc, tracking_id=self.tracking_id) from exc
        except asyncio.CancelledError:
            self.abort()
            raise
        except (OSError, TimeoutError) as exc:
            self.abort()
            raise relay_error(exc, tracking_id=self.tracking_id) from exc

    async def send_text(self, text: str) -> None:
        """Send a text WebSocket frame."""
        if not isinstance(text, str):
            raise TypeError("send_text expects a str payload")
        await self.send(text)

    async def send_bytes(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """Send a binary WebSocket frame."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("send_bytes expects a bytes-like payload")
        await self.send(bytes(data))

    async def receive(self) -> Tuple[Union[bytes, str], str]:
        """
        Receive a single WebSocket message.

        Returns:
            A tuple ``(payload, message_type)`` where ``message_type`` is
            ``"text"`` or ``"binary"``.

        Raises:
            ConnectionError: If the stream has been closed.
        """
        if self._closed:
            raise ConnectionError("HybridConnectionStream is closed")
        if self._read_fragments is not None or self._read_buffer:
            raise RuntimeError("Finish byte-stream reads before switching to receive()")
        try:
            async with self._read_lock:
                async with asyncio.timeout(self._transport.read_timeout):
                    message = await self._websocket.recv()
            logger.debug("Stream message received", extra={"tracking_id": self.tracking_id,
                                                           "size": len(message)})
        except _WSConnectionClosed as e:
            self._closed = True
            raise relay_error(e, tracking_id=self.tracking_id) from e
        except (OSError, TimeoutError) as exc:
            raise relay_error(exc, tracking_id=self.tracking_id) from exc

        if isinstance(message, (bytes, bytearray)):
            return bytes(message), MESSAGE_TYPE_BINARY
        return message, MESSAGE_TYPE_TEXT

    async def read(self, size: int = 65536) -> bytes:
        """Read up to size bytes across message boundaries; normal closure is EOF.

        Byte-stream and message-oriented reads must not be interleaved.
        A cancelled partial read closes the socket because WebSocket fragments
        cannot safely be abandoned.
        """
        if size < 0:
            raise ValueError("Use a bounded positive read size")
        if size == 0:
            return b""
        async with self._read_lock:
            try:
                async with asyncio.timeout(self._transport.read_timeout):
                    while not self._read_buffer:
                        if self._closed:
                            return b""
                        if self._read_fragments is None:
                            self._read_fragments = self._websocket.recv_streaming(decode=False).__aiter__()
                        try:
                            self._read_buffer = await anext(self._read_fragments)
                        except StopAsyncIteration:
                            self._read_fragments = None
                    result, self._read_buffer = self._read_buffer[:size], self._read_buffer[size:]
                    return result
            except _WSConnectionClosedOK:
                self._closed = True
                return b""
            except asyncio.CancelledError:
                await self.close()
                raise
            except (_WSConnectionClosed, OSError, TimeoutError) as exc:
                await self.close()
                raise relay_error(exc, tracking_id=self.tracking_id) from exc

    async def write(self, data: bytes) -> None:
        """Write bytes with transport backpressure."""
        await self.send_bytes(data)

    async def shutdown(self) -> None:
        """Send the WebSocket close-output gesture without waiting for the peer.

        This isn't TCP half-close: the peer is expected to reply with a close
        frame. Buffered incoming data remains readable until then.
        """
        async with self._send_lock:
            if self._closed or self._write_closed:
                return
            self._websocket.protocol.send_close(1000, "Shutdown")
            self._websocket.send_data()
            self._write_closed = True
            self._shutdown_timer = asyncio.get_running_loop().call_later(
                self._transport.close_timeout, self._websocket.transport.abort
            )
            try:
                async with asyncio.timeout(self._transport.write_timeout):
                    await self._websocket.drain()
            except (OSError, TimeoutError) as exc:
                self._websocket.transport.abort()
                raise relay_error(exc, tracking_id=self.tracking_id) from exc

    def abort(self) -> None:
        """Immediately release the transport without a graceful handshake."""
        self._websocket.transport.abort()
        self._closed = True
        if self._shutdown_timer:
            self._shutdown_timer.cancel()
        logger.warning("Stream aborted", extra={"tracking_id": self.tracking_id})

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Close the underlying WebSocket. Safe to call multiple times.

        Close failures propagate so the caller can correct the parameters
        or retry; a failed or cancelled close does not mark the stream closed.
        """
        async with self._close_lock:
            if self._shutdown_timer:
                self._shutdown_timer.cancel()
            if self._closed:
                return
            async with asyncio.timeout(self._transport.close_timeout):
                await self._websocket.close(code=code, reason=reason)
            self._closed = True
            logger.debug("Stream closed", extra={"tracking_id": self.tracking_id})

    def __aiter__(self) -> "HybridConnectionStream":
        return self

    async def __anext__(self) -> Tuple[Union[bytes, str], str]:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self.receive()
        except ConnectionError as exc:
            if isinstance(exc.__cause__, _WSConnectionClosedOK):
                raise StopAsyncIteration from exc
            raise
