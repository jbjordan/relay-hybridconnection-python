"""Unit tests for HybridConnectionStream."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from src.hybrid_connection.stream import (
    HybridConnectionStream,
    MESSAGE_TYPE_BINARY,
    MESSAGE_TYPE_TEXT,
)


def _fake_ws() -> MagicMock:
    ws = MagicMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    return ws


class TestStreamMetadata:
    def test_properties_are_exposed(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(
            ws,
            tracking_id="tid-123",
            connect_headers={"Host": "h", "X-Custom": "v"},
            address="wss://ns/$hc/p?sb-hc-action=accept",
        )

        assert stream.websocket is ws
        assert stream.tracking_id == "tid-123"
        assert stream.connect_headers == {"Host": "h", "X-Custom": "v"}
        assert stream.address == "wss://ns/$hc/p?sb-hc-action=accept"
        assert stream.is_closed is False


class TestStreamSend:
    @pytest.mark.asyncio
    async def test_send_text(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.send_text("hello")

        ws.send.assert_awaited_once_with("hello")

    @pytest.mark.asyncio
    async def test_send_bytes(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.send_bytes(b"\x00\x01\x02")

        ws.send.assert_awaited_once_with(b"\x00\x01\x02")

    @pytest.mark.asyncio
    async def test_send_auto_text(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.send("auto")
        ws.send.assert_awaited_once_with("auto")

    @pytest.mark.asyncio
    async def test_send_auto_bytes(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.send(b"bin")
        ws.send.assert_awaited_once_with(b"bin")

    @pytest.mark.asyncio
    async def test_send_after_close_raises(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)
        await stream.close()

        with pytest.raises(RuntimeError):
            await stream.send_text("x")

    @pytest.mark.asyncio
    async def test_send_text_validates_type(self):
        stream = HybridConnectionStream(_fake_ws())
        with pytest.raises(TypeError):
            await stream.send_text(b"not a str")

    @pytest.mark.asyncio
    async def test_send_bytes_validates_type(self):
        stream = HybridConnectionStream(_fake_ws())
        with pytest.raises(TypeError):
            await stream.send_bytes("not bytes")


class TestStreamReceive:
    @pytest.mark.asyncio
    async def test_receive_text(self):
        ws = _fake_ws()
        ws.recv.return_value = "hello"
        stream = HybridConnectionStream(ws)

        payload, kind = await stream.receive()
        assert payload == "hello"
        assert kind == MESSAGE_TYPE_TEXT

    @pytest.mark.asyncio
    async def test_receive_binary(self):
        ws = _fake_ws()
        ws.recv.return_value = b"\xff\xfe"
        stream = HybridConnectionStream(ws)

        payload, kind = await stream.receive()
        assert payload == b"\xff\xfe"
        assert kind == MESSAGE_TYPE_BINARY

    @pytest.mark.asyncio
    async def test_receive_bytearray_becomes_bytes(self):
        ws = _fake_ws()
        ws.recv.return_value = bytearray(b"abc")
        stream = HybridConnectionStream(ws)

        payload, kind = await stream.receive()
        assert isinstance(payload, bytes)
        assert payload == b"abc"
        assert kind == MESSAGE_TYPE_BINARY

    @pytest.mark.asyncio
    async def test_receive_raises_on_closed_connection(self):
        ws = _fake_ws()
        ws.recv.side_effect = ConnectionClosed(None, None)
        stream = HybridConnectionStream(ws)

        with pytest.raises(ConnectionError):
            await stream.receive()
        assert stream.is_closed is True

    @pytest.mark.asyncio
    async def test_receive_after_close_raises(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)
        await stream.close()

        with pytest.raises(ConnectionError):
            await stream.receive()


class TestStreamClose:
    @pytest.mark.asyncio
    async def test_close_invokes_websocket_close(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.close(code=1001, reason="bye")
        ws.close.assert_awaited_once_with(code=1001, reason="bye")
        assert stream.is_closed is True

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        ws = _fake_ws()
        stream = HybridConnectionStream(ws)

        await stream.close()
        await stream.close()
        ws.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_swallows_inner_exceptions(self):
        ws = _fake_ws()
        ws.close.side_effect = RuntimeError("network broke")
        stream = HybridConnectionStream(ws)

        await stream.close()
        assert stream.is_closed is True


class TestStreamAsyncIteration:
    @pytest.mark.asyncio
    async def test_async_iteration_yields_messages(self):
        ws = _fake_ws()
        messages = ["one", b"two", "three"]
        ws.recv.side_effect = messages + [
            ConnectionClosed(None, None)
        ]
        stream = HybridConnectionStream(ws)

        collected = []
        async for payload, kind in stream:
            collected.append((payload, kind))

        assert collected == [
            ("one", MESSAGE_TYPE_TEXT),
            (b"two", MESSAGE_TYPE_BINARY),
            ("three", MESSAGE_TYPE_TEXT),
        ]
