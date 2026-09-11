"""Listener API and rendezvous call-site coverage for address selection."""

import asyncio
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.exceptions import ConnectionClosedOK

from src.hybrid_connection import listener as listener_module
from src.hybrid_connection.client import HybridConnectionClient
from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider


ADDRESS = "sb://namespace.example/path"
RENDEZVOUS = (
    "wss://namespace.example:10443/$hc/path%2Fpart?"
    "sb-hc-action=accept&sb-hc-id=id%2B1&empty="
)


def _listener(**kwargs):
    return HybridConnectionListener(
        ADDRESS, TokenProvider("test-policy", "dGVzdC1rZXk="), **kwargs
    )


def _websocket():
    websocket = Mock()
    websocket.close = AsyncMock()
    websocket.send = AsyncMock()
    websocket.recv = AsyncMock(side_effect=ConnectionClosedOK(None, None))
    return websocket


@pytest.mark.parametrize("option", [None, True, False])
def test_constructor_and_factory_ipv6_preference(connection_string, option):
    kwargs = {} if option is None else {"prefer_ipv6": option}
    expected = True if option is None else option
    assert _listener(**kwargs)._prefer_ipv6 is expected
    assert HybridConnectionListener.from_connection_string(
        connection_string, **kwargs
    )._prefer_ipv6 is expected


def test_preference_is_keyword_only(connection_string):
    with pytest.raises(TypeError):
        HybridConnectionListener(ADDRESS, TokenProvider("key", "dGVzdA=="), False)
    with pytest.raises(TypeError):
        HybridConnectionListener.from_connection_string(connection_string, False)


async def _invoke_rendezvous(listener, kind):
    if kind in {"accept", "reject"}:
        if kind == "reject":
            listener.accept_handler = lambda context: False
        await listener._handle_accept({"address": RENDEZVOUS, "id": "id+1"})
    elif kind == "request":
        await listener._handle_rendezvous_request(RENDEZVOUS)
    else:
        await listener._upgrade_response_to_rendezvous(
            rendezvous_address=RENDEZVOUS,
            response_message='{"response": {"requestId": "id+1"}}',
            body=b"x" * (65 * 1024),
        )
        await asyncio.gather(*listener._dispatch_tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefer_ipv6", [True, False])
@pytest.mark.parametrize("kind", ["accept", "reject", "request", "response"])
async def test_all_rendezvous_paths_forward_policy_and_options(
    monkeypatch, kind, prefer_ipv6,
):
    listener = _listener(prefer_ipv6=prefer_ipv6)
    connect = AsyncMock(return_value=_websocket())
    monkeypatch.setattr(listener_module, "connect_rendezvous", connect)
    try:
        await _invoke_rendezvous(listener, kind)
        connect.assert_awaited_once()
        uri = connect.await_args.args[0]
        parsed = urlsplit(uri)
        original = urlsplit(RENDEZVOUS)
        assert (parsed.scheme, parsed.netloc, parsed.path) == (
            original.scheme, original.netloc, original.path
        )
        query = parse_qs(parsed.query, keep_blank_values=True)
        assert query["sb-hc-id"] == ["id+1"]
        assert query["empty"] == [""]
        if kind == "reject":
            assert query["sb-hc-statusCode"] == ["400"]
        if kind == "accept":
            assert uri == RENDEZVOUS
        options = {"prefer_ipv6": prefer_ipv6}
        if kind in {"request", "response"}:
            options["max_size"] = None
        assert connect.await_args.kwargs == options
    finally:
        await listener.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["accept", "reject", "request", "response"])
async def test_listener_close_cancels_each_inflight_rendezvous(monkeypatch, kind):
    listener = _listener()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(listener_module, "connect_rendezvous", connect)
    listener._spawn_dispatch(_invoke_rendezvous(listener, kind))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
    finally:
        await listener.close()
    assert cancelled.is_set()
    assert not listener._dispatch_tasks
    assert not listener._open_streams


@pytest.mark.asyncio
@pytest.mark.parametrize("prefer_ipv6", [True, False])
async def test_control_and_sender_connections_keep_legacy_options(
    monkeypatch, prefer_ipv6,
):
    listener = _listener(prefer_ipv6=prefer_ipv6)
    control, sender = _websocket(), _websocket()
    control.recv.side_effect = asyncio.Event().wait
    connect = AsyncMock(side_effect=[control, sender])
    rendezvous = AsyncMock(side_effect=AssertionError("Not a rendezvous connection"))
    monkeypatch.setattr(listener_module.websockets, "connect", connect)
    monkeypatch.setattr(listener_module, "connect_rendezvous", rendezvous)
    try:
        await listener.open()
        stream = await HybridConnectionClient(ADDRESS).create_connection()
        await stream.close()
        assert len(connect.await_args_list) == 2
        listen_call, send_call = connect.await_args_list
        assert listen_call.kwargs == {"ping_interval": None}
        assert send_call.kwargs == {"additional_headers": None}
        assert "sb-hc-action=listen" in listen_call.args[0]
        assert "sb-hc-action=connect" in send_call.args[0]
        assert urlsplit(listen_call.args[0]).netloc == "namespace.example"
        assert urlsplit(send_call.args[0]).netloc == "namespace.example"
        rendezvous.assert_not_awaited()
    finally:
        await listener.close()
