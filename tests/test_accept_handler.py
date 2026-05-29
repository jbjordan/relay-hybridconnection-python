"""
Unit tests for HybridConnectionListener accept handler and rendezvous flow.

These tests are modeled on the .NET ``WebSocketTests.AcceptHandlerTest`` and
``RawWebSocketSenderTest`` from
https://github.com/Azure/azure-relay-dotnet/blob/dev/test/Microsoft.Azure.Relay.UnitTests/WebSocketTests.cs

They use mocked WebSocket connections rather than a live relay; an in-process
``websockets.serve`` based integration smoke test lives in
``test_integration_rendezvous.py``.
"""

import asyncio
import json
import secrets
import string
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest

from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider


def _random_sas_key(length: int = 44) -> str:
    alphabet = string.ascii_letters + string.digits + "+/"
    return "".join(secrets.choice(alphabet) for _ in range(length - 1)) + "="


def _build_listener() -> HybridConnectionListener:
    provider = TokenProvider("RootManageSharedAccessKey", _random_sas_key())
    return HybridConnectionListener(
        "sb://contoso.servicebus.windows.net/hc1", provider
    )


async def _drain_dispatch_tasks(listener: HybridConnectionListener) -> None:
    """Wait until every spawned dispatch task has finished."""
    # Pump the loop a few times so newly created tasks have a chance to start.
    for _ in range(50):
        if not listener._dispatch_tasks:
            return
        await asyncio.sleep(0)
    pending = [t for t in listener._dispatch_tasks if not t.done()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


ACCEPT_MESSAGE = json.dumps({
    "accept": {
        "address": "wss://dc-node.servicebus.windows.net:443/$hc/hc1?sb-hc-action=accept&sb-hc-id=abc",
        "id": "abc",
        "connectHeaders": {
            "Host": "dc-node.servicebus.windows.net",
            "X-MyApp": "test-client",
        },
    }
})


class TestAcceptHandlerDefault:
    @pytest.mark.asyncio
    async def test_default_accept_handler_accepts_connection(self):
        listener = _build_listener()
        accepted_ws = MagicMock()
        accepted_ws.close = AsyncMock()

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=accepted_ws),
        ) as connect:
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {"Host": "h"},
                }
            )

        connect.assert_awaited_once()
        # Default behavior is to accept – the URL we connect to is the
        # rendezvous accept URL (unmodified).
        url = connect.call_args.args[0]
        assert "sb-hc-action=accept" in url
        assert "sb-hc-statusCode" not in url

        # Stream should be queued for accept_connection().
        assert listener._pending_connections.qsize() == 1


class TestAcceptHandlerCustom:
    @pytest.mark.asyncio
    async def test_handler_accepts_and_inspects_headers(self):
        listener = _build_listener()
        captured_headers = {}

        def handler(context):
            captured_headers.update(context.request.headers)
            assert context.is_websocket_upgrade is True
            assert context.tracking_id == "id-1"
            return True

        listener.accept_handler = handler

        accepted_ws = MagicMock()
        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=accepted_ws),
        ):
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {"Host": "h", "X-MyApp": "test"},
                }
            )

        assert captured_headers == {"Host": "h", "X-MyApp": "test"}
        assert listener._pending_connections.qsize() == 1

    @pytest.mark.asyncio
    async def test_async_handler_supported(self):
        listener = _build_listener()

        async def handler(_context):
            await asyncio.sleep(0)
            return True

        listener.accept_handler = handler

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=MagicMock()),
        ):
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {},
                }
            )

        assert listener._pending_connections.qsize() == 1


class TestAcceptHandlerReject:
    @pytest.mark.asyncio
    async def test_reject_with_default_status(self):
        listener = _build_listener()
        listener.accept_handler = lambda _ctx: False

        # The reject connect attempt is expected to fail (HTTP 410 is the
        # successful rejection signal); ensure we tolerate it.
        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(side_effect=Exception("HTTP 410 Gone (expected reject)")),
        ) as connect:
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {},
                }
            )

        url = connect.call_args.args[0]
        qs = parse_qs(urlsplit(url).query)
        assert qs.get("sb-hc-statusCode") == ["400"]
        assert qs.get("sb-hc-statusDescription") == ["Rejected by user code"]
        # Nothing should end up in the queue.
        assert listener._pending_connections.empty()

    @pytest.mark.asyncio
    async def test_reject_with_custom_status(self):
        listener = _build_listener()

        def handler(context):
            context.response.status_code = 401
            context.response.status_description = "Unauthorized client"
            return False

        listener.accept_handler = handler

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(side_effect=Exception("expected reject")),
        ) as connect:
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {},
                }
            )

        url = connect.call_args.args[0]
        qs = parse_qs(urlsplit(url).query)
        assert qs.get("sb-hc-statusCode") == ["401"]
        assert qs.get("sb-hc-statusDescription") == ["Unauthorized client"]

    @pytest.mark.asyncio
    async def test_handler_exception_rejects_with_502(self):
        listener = _build_listener()

        def handler(_ctx):
            raise RuntimeError("boom")

        listener.accept_handler = handler

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(side_effect=Exception("expected reject")),
        ) as connect:
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {},
                }
            )

        url = connect.call_args.args[0]
        qs = parse_qs(urlsplit(url).query)
        assert qs.get("sb-hc-statusCode") == ["502"]
        assert "AcceptHandler" in qs.get("sb-hc-statusDescription", [""])[0]


class TestAcceptConnectionApi:
    @pytest.mark.asyncio
    async def test_accept_connection_returns_queued_stream(self):
        listener = _build_listener()

        accepted_ws = MagicMock()
        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=accepted_ws),
        ):
            await listener._handle_accept(
                {
                    "address": "wss://dc-node/$hc/hc1?sb-hc-action=accept",
                    "id": "id-1",
                    "connect_headers": {"X-A": "B"},
                }
            )

        stream = await asyncio.wait_for(listener.accept_connection(), timeout=1)
        assert stream.websocket is accepted_ws
        assert stream.tracking_id == "id-1"
        assert stream.connect_headers["X-A"] == "B"
        # Once handed off, the listener no longer tracks it.
        assert stream not in listener._open_streams

    @pytest.mark.asyncio
    async def test_async_iter_connections(self):
        listener = _build_listener()

        ws_a = MagicMock()
        ws_b = MagicMock()
        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(side_effect=[ws_a, ws_b]),
        ):
            await listener._handle_accept(
                {"address": "wss://dc/p?sb-hc-action=accept", "id": "1", "connect_headers": {}}
            )
            await listener._handle_accept(
                {"address": "wss://dc/p?sb-hc-action=accept", "id": "2", "connect_headers": {}}
            )

        collected = []
        async for stream in listener.connections():
            collected.append(stream)
            if len(collected) == 2:
                break

        assert {s.tracking_id for s in collected} == {"1", "2"}


class TestReceiveLoopDispatch:
    """Verify the receive loop dispatches accept messages to background tasks."""

    @pytest.mark.asyncio
    async def test_accept_message_dispatched(self):
        listener = _build_listener()
        listener._is_online = True

        recv_calls = [ACCEPT_MESSAGE]

        async def fake_recv():
            if recv_calls:
                return recv_calls.pop(0)
            # Block forever after the single message
            await asyncio.sleep(60)

        ws = MagicMock()
        ws.recv = fake_recv
        listener._websocket = ws

        accepted_ws = MagicMock()
        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=accepted_ws),
        ):
            task = asyncio.create_task(listener._receive_loop())
            # Give the dispatcher a chance to spawn and run handler.
            await asyncio.sleep(0.05)
            await _drain_dispatch_tasks(listener)

            assert listener._pending_connections.qsize() == 1

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
