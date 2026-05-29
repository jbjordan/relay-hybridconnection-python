"""
HybridConnectionStream: a duplex WebSocket stream resulting from a successful
rendezvous between a sender and a listener over Azure Relay.

The stream is a thin abstraction over an open `websockets` client connection
that exposes a small, opinionated send/receive surface so callers don't need
to depend on the underlying library directly.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Tuple, Union

import websockets
from websockets.exceptions import ConnectionClosed as _WSConnectionClosed


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
    ) -> None:
        self._websocket = websocket
        self._tracking_id = tracking_id
        self._connect_headers: Dict[str, str] = dict(connect_headers or {})
        self._address = address
        self._closed = False
        self._close_lock = asyncio.Lock()

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
        if self._closed:
            raise RuntimeError("Cannot send on a closed HybridConnectionStream")
        await self._websocket.send(data)

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
        try:
            message = await self._websocket.recv()
        except _WSConnectionClosed as e:
            self._closed = True
            raise ConnectionError(f"HybridConnectionStream closed: {e}") from e

        if isinstance(message, (bytes, bytearray)):
            return bytes(message), MESSAGE_TYPE_BINARY
        return message, MESSAGE_TYPE_TEXT

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Close the underlying WebSocket. Safe to call multiple times.
        """
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            try:
                await self._websocket.close(code=code, reason=reason)
            except Exception:
                # Ignore close errors – the connection may already be gone.
                pass

    def __aiter__(self) -> "HybridConnectionStream":
        return self

    async def __anext__(self) -> Tuple[Union[bytes, str], str]:
        try:
            return await self.receive()
        except ConnectionError as exc:
            raise StopAsyncIteration from exc
