"""
Unit tests for HTTP request/response handling, including rendezvous upgrades.

These mirror the .NET ``HybridRequestTests`` (``SmallRequestSmallResponse``,
``SmallRequestLargeResponse``, ``EmptyRequestEmptyResponse``,
``LargeRequestEmptyResponse``, ``AllowNullStatusDescription``). Where the
.NET tests stand up a real listener and HTTP client, ours wire the code under
test to mocked WebSockets so we can drive the protocol deterministically.
"""

import asyncio
import json
import secrets
import string
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider
from src.hybrid_connection.protocol import CONTROL_CHANNEL_MAX_BODY_SIZE


def _random_sas_key(length: int = 44) -> str:
    alphabet = string.ascii_letters + string.digits + "+/"
    return "".join(secrets.choice(alphabet) for _ in range(length - 1)) + "="


def _make_listener() -> HybridConnectionListener:
    provider = TokenProvider("RootManageSharedAccessKey", _random_sas_key())
    return HybridConnectionListener(
        "sb://contoso.servicebus.windows.net/hc1", provider
    )


def _capture_control_websocket(listener: HybridConnectionListener):
    """Attach a mock control-channel WebSocket and return it."""
    ws = MagicMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    listener._websocket = ws
    return ws


def _parse_sent_messages(ws_mock: MagicMock):
    """Return the list of payloads sent on the control channel mock."""
    return [c.args[0] for c in ws_mock.send.await_args_list]


class TestSmallRequestSmallResponse:
    """Mirror of .NET ``SmallRequestSmallResponse``."""

    @pytest.mark.asyncio
    async def test_get_request_returns_expected_response(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        expected_body = b'{ "a" : true }'

        def handler(context):
            assert context.request.http_method == "GET"
            assert context.request.url == "/api/test"
            context.response.status_code = 200
            context.response.status_description = "OK"
            context.response.output_stream.write(expected_body)

        listener.request_handler = handler

        await listener._handle_control_request(
            {
                "id": "req-1",
                "method": "GET",
                "requestTarget": "/api/test",
                "requestHeaders": {"User-Agent": "Test"},
                "body": False,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=b"",
        )

        messages = _parse_sent_messages(ws)
        # Expect a JSON response header followed by a binary body frame.
        assert len(messages) == 2
        header = json.loads(messages[0])["response"]
        assert header["statusCode"] == 200
        assert header["statusDescription"] == "OK"
        assert header["requestId"] == "req-1"
        assert header["body"] is True
        assert messages[1] == expected_body

    @pytest.mark.asyncio
    async def test_post_request_handler_receives_body(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)
        captured = {}

        def handler(context):
            captured["method"] = context.request.http_method
            captured["body"] = context.request.read_body()
            context.response.status_code = 200

        listener.request_handler = handler

        body = b'{"a": 11, "b": 22}'
        await listener._handle_control_request(
            {
                "id": "req-1",
                "method": "POST",
                "requestTarget": "/api/data",
                "requestHeaders": {"Content-Type": "application/json"},
                "body": True,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=body,
        )

        assert captured["method"] == "POST"
        assert captured["body"] == body


class TestSmallRequestLargeResponse:
    """Mirror of .NET ``SmallRequestLargeResponse`` – response larger than
    the control-channel limit must be upgraded to rendezvous."""

    @pytest.mark.asyncio
    async def test_large_response_upgrades_to_rendezvous(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        big_body = b"y" * (65 * 1024)  # >64KB

        def handler(context):
            context.response.status_code = 200
            context.response.status_description = "TestStatusDescription"
            context.response.output_stream.write(big_body)

        listener.request_handler = handler

        rendezvous_ws = MagicMock()
        rendezvous_ws.send = AsyncMock()
        rendezvous_ws.close = AsyncMock()

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=rendezvous_ws),
        ) as connect:
            await listener._handle_control_request(
                {
                    "id": "req-large",
                    "method": "GET",
                    "requestTarget": "/big",
                    "requestHeaders": {},
                    "body": False,
                    "address": "wss://dc/$hc/p?sb-hc-action=request&sb-hc-id=req-large",
                },
                body=b"",
            )

        # The control-channel WebSocket should NOT have been used for response.
        assert not ws.send.await_args_list

        # Should have connected once to the rendezvous URL.
        connect.assert_awaited_once()
        url = connect.call_args.args[0]
        assert "sb-hc-action=request" in url

        sent = [c.args[0] for c in rendezvous_ws.send.await_args_list]
        assert len(sent) == 2
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 200
        assert header["statusDescription"] == "TestStatusDescription"
        assert header["body"] is True
        assert sent[1] == big_body
        rendezvous_ws.close.assert_awaited()


class TestEmptyRequestEmptyResponse:
    """Mirror of .NET ``EmptyRequestEmptyResponse``."""

    @pytest.mark.asyncio
    async def test_handler_that_only_closes_response(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        async def handler(context):
            await context.response.close()

        listener.request_handler = handler

        await listener._handle_control_request(
            {
                "id": "req-empty",
                "method": "GET",
                "requestTarget": "/",
                "requestHeaders": {},
                "body": False,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=b"",
        )

        sent = _parse_sent_messages(ws)
        assert len(sent) == 1  # only JSON header, no body frame
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 200
        assert "body" not in header


class TestLargeRequestEmptyResponse:
    """Mirror of .NET ``LargeRequestEmptyResponse`` – large request body."""

    @pytest.mark.asyncio
    async def test_large_request_body_is_delivered(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)
        captured = {}

        def handler(context):
            captured["body"] = context.request.read_body()

        listener.request_handler = handler

        body = b"y" * (65 * 1024)
        await listener._handle_control_request(
            {
                "id": "req-large-body",
                "method": "POST",
                "requestTarget": "/upload",
                "requestHeaders": {},
                "body": True,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=body,
        )

        assert captured["body"] == body
        sent = _parse_sent_messages(ws)
        assert len(sent) == 1
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 200


class TestAllowNullStatusDescription:
    """Mirror of .NET ``AllowNullStatusDescription``."""

    @pytest.mark.asyncio
    async def test_setting_status_description_to_none_clears_it(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        def handler(context):
            context.response.status_description = "TestStatusDescription"
            context.response.status_code = 201
            context.response.status_description = None  # explicit clear

        listener.request_handler = handler

        await listener._handle_control_request(
            {
                "id": "req-1",
                "method": "POST",
                "requestTarget": "/create",
                "requestHeaders": {},
                "body": False,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=b"",
        )

        sent = _parse_sent_messages(ws)
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 201
        assert header["statusDescription"] == ""


class TestNoHandlerReturns501:
    """If no request handler is installed, listener should respond 501."""

    @pytest.mark.asyncio
    async def test_no_handler_returns_not_implemented(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        await listener._handle_control_request(
            {
                "id": "req-1",
                "method": "GET",
                "requestTarget": "/",
                "requestHeaders": {},
                "body": False,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=b"",
        )

        sent = _parse_sent_messages(ws)
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 501


class TestHandlerExceptionReturns500:
    """Handler raising an exception should produce a 500 response, not crash."""

    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self):
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        def handler(_context):
            raise RuntimeError("oops")

        listener.request_handler = handler

        await listener._handle_control_request(
            {
                "id": "req-1",
                "method": "GET",
                "requestTarget": "/",
                "requestHeaders": {},
                "body": False,
                "address": "wss://dc/$hc/p?sb-hc-action=request",
            },
            body=b"",
        )

        sent = _parse_sent_messages(ws)
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 500


class TestRendezvousRequestPointer:
    """A request whose payload is a rendezvous pointer should be served by
    opening the rendezvous WebSocket and replaying the full request from it."""

    @pytest.mark.asyncio
    async def test_rendezvous_pointer_serves_request_over_rendezvous(self):
        listener = _make_listener()
        captured = {}

        async def handler(context):
            captured["method"] = context.request.http_method
            captured["body"] = context.request.read_body()
            context.response.status_code = 200
            context.response.output_stream.write(b"reply")
            await context.response.close()

        listener.request_handler = handler

        rendezvous_ws = MagicMock()
        rendezvous_ws.send = AsyncMock()
        rendezvous_ws.close = AsyncMock()

        # Sequence of frames the service delivers on the rendezvous socket:
        # 1) JSON request envelope
        # 2) binary request body
        # 3) ConnectionClosed (to terminate the rendezvous serve loop)
        from websockets.exceptions import ConnectionClosed

        rendezvous_ws.recv = AsyncMock(
            side_effect=[
                json.dumps(
                    {
                        "request": {
                            "id": "req-1",
                            "method": "POST",
                            "requestTarget": "/upload",
                            "requestHeaders": {},
                            "body": True,
                        }
                    }
                ),
                b"X" * 100_000,  # >64KB
                ConnectionClosed(None, None),
            ]
        )

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(return_value=rendezvous_ws),
        ):
            await listener._handle_rendezvous_request(
                "wss://dc/$hc/p?sb-hc-action=request&sb-hc-id=req-1"
            )

        assert captured["method"] == "POST"
        assert len(captured["body"]) == 100_000

        sent = [c.args[0] for c in rendezvous_ws.send.await_args_list]
        assert len(sent) == 2
        header = json.loads(sent[0])["response"]
        assert header["statusCode"] == 200
        assert sent[1] == b"reply"
        # The stream marks itself closed when ConnectionClosed is raised
        # in receive; the rendezvous handler then ends without explicitly
        # closing the underlying websocket a second time. That is fine –
        # the connection has already been torn down by the time we get
        # here, so the assertion is that the stream did not crash and the
        # response was delivered.


class TestControlChannelLimitBoundary:
    @pytest.mark.asyncio
    async def test_response_at_limit_uses_control_channel(self):
        """A response exactly at the limit should still travel over control."""
        listener = _make_listener()
        ws = _capture_control_websocket(listener)

        body = b"x" * CONTROL_CHANNEL_MAX_BODY_SIZE

        def handler(context):
            context.response.output_stream.write(body)

        listener.request_handler = handler

        with patch(
            "src.hybrid_connection.listener.websockets.connect",
            new=AsyncMock(),
        ) as connect:
            await listener._handle_control_request(
                {
                    "id": "req-1",
                    "method": "GET",
                    "requestTarget": "/",
                    "requestHeaders": {},
                    "body": False,
                    "address": "wss://dc/$hc/p?sb-hc-action=request",
                },
                body=b"",
            )

        # No rendezvous needed at exactly the limit.
        connect.assert_not_called()
        assert len(_parse_sent_messages(ws)) == 2
