"""Regression coverage for protocol parity and operational controls."""

import asyncio
import json
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import websockets
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus, ConnectionClosedError
from websockets.frames import Close
from websockets.http11 import Response

from hybrid_connection import (
    HybridConnectionClient, HybridConnectionListener, HybridConnectionStream,
    TokenProvider, TransportOptions, ListenerLimits, RetryPolicy,
    RelayError, AuthorizationError, EndpointNotFoundError,
)
from hybrid_connection.context import RelayedHttpListenerResponse
from hybrid_connection.errors import relay_error, redact
from hybrid_connection.protocol import ProtocolHandler
from hybrid_connection.runtime import parse_runtime_information
from hybrid_connection.http_stream import RequestBody


def listener(**kwargs):
    return HybridConnectionListener("sb://example/hc", TokenProvider("key", "dummy"), **kwargs)


def request(address="wss://example/request"):
    return {"id": "request-1", "method": "GET", "requestTarget": "/hc/route",
            "requestHeaders": {}, "address": address, "body": False}


@pytest.mark.parametrize("status,kind,transient", [
    (401, AuthorizationError, False), (403, AuthorizationError, False),
    (404, EndpointNotFoundError, False), (503, RelayError, True),
])
def test_handshake_errors_are_structured(status, kind, transient):
    original = InvalidStatus(Response(status, "Failure", Headers()))
    error = relay_error(original, listener=True, tracking_id="probe")
    assert isinstance(error, kind)
    assert error.status_code == status
    assert error.is_transient is transient
    assert error.tracking_id == "probe"


async def test_terminal_reconnect_releases_accept_waiters(caplog):
    receiver = listener(retry_policy=RetryPolicy(jitter=0))
    receiver._should_reconnect = True
    receiver.last_error = relay_error(
        InvalidStatus(Response(404, "Not Found", Headers())), listener=True
    )
    stopped = Mock()
    receiver.on_stopped = stopped
    waiter = asyncio.create_task(receiver.accept_connection())
    with patch.object(receiver, "_connect", new=AsyncMock()) as connect:
        await receiver._reconnect_loop()
        connect.assert_not_called()
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(waiter, 1)
    assert not receiver._should_reconnect
    stopped.assert_called_once()


async def test_retry_budget_and_diagnostics(caplog):
    receiver = listener(retry_policy=RetryPolicy(
        initial_delay=0.001, max_delay=0.001, jitter=0, max_attempts=2
    ))
    receiver._should_reconnect = True
    caplog.set_level(logging.INFO)
    with patch.object(receiver, "_connect", new=AsyncMock(side_effect=ConnectionError("offline"))) as connect:
        await receiver._reconnect_loop()
    assert connect.await_count == 2
    scheduled = [r for r in caplog.records if r.message == "Listener reconnect scheduled"]
    assert [r.attempt for r in scheduled] == [1, 2]
    assert all(r.tracking_id == receiver.tracking_id for r in scheduled)


async def test_closure_reason_is_retained_and_logged(caplog):
    receiver = listener()
    receiver._websocket = AsyncMock()
    receiver._is_online = True
    receiver._websocket.recv.side_effect = ConnectionClosedError(
        Close(1008, "expired TrackingId:service-id"), None
    )
    await receiver._receive_loop()
    assert receiver.last_error.close_code == 1008
    assert receiver.last_error.tracking_id == "service-id"
    assert "expired" in caplog.text


def test_credentials_are_redacted():
    assert "secret" not in redact("url?sb-hc-token=secret&sb-hc-id=id")
    assert "secret" not in redact("Authorization: Bearer secret")
    assert "secret" not in redact("SharedAccessSignature sr=x&sig=secret")


def test_service_handshake_tracking_id_is_preserved():
    original = InvalidStatus(Response(401, "Denied. TrackingId:service-id", Headers()))
    error = relay_error(original, listener=True, tracking_id="local-id")
    assert error.tracking_id == "service-id"
    assert "Denied" in str(error)


async def test_response_failure_is_terminal_and_visible():
    callback = AsyncMock(side_effect=OSError("send failed"))
    response = RelayedHttpListenerResponse(callback)
    for _ in range(2):
        with pytest.raises(OSError, match="send failed"):
            await response.close()
    assert not response.is_closed
    assert response.failure is not None
    callback.assert_awaited_once()


async def test_handler_close_does_not_swallow_transport_error():
    receiver = listener()
    receiver._websocket = AsyncMock()
    receiver._websocket.send.side_effect = OSError("send failed")
    async def handler(context):
        await context.response.close()
    receiver.request_handler = handler
    with pytest.raises(OSError, match="send failed"):
        await receiver._handle_control_request(request(), b"")


@pytest.mark.parametrize("headers,status", [
    ({"Transfer-Encoding": "chunked"}, 200),
    ({"Content-Length": "3"}, 200),
    ({"X-Large": "x" * 32769}, 200),
    ({"X-Test": "bad\r\nInjected: true"}, 200),
    ({}, 502), ({}, 504), ({}, 999), ({}, "200"),
])
def test_invalid_http_responses_fail_before_sending(headers, status):
    with pytest.raises(ValueError):
        ProtocolHandler.build_response_message("id", status, "OK", headers)


async def test_async_credential_and_preissued_sas():
    credential = SimpleNamespace(get_token=AsyncMock(
        return_value=SimpleNamespace(token="dummy-access-token", expires_on=time.time() + 600)
    ))
    client = HybridConnectionClient("sb://example/hc", TokenProvider.from_credential(credential))
    with patch("websockets.connect", new=AsyncMock()) as connect:
        stream = await client.create_connection()
    credential.get_token.assert_awaited_once_with("https://relay.azure.net/.default")
    assert "dummy-access-token" in connect.call_args.args[0]
    assert "dummy-access-token" not in stream.address
    sas = TokenProvider("key", "dummy").get_token("sb://example/hc").token
    client = HybridConnectionClient.from_connection_string(
        f"endpoint=sb://example/;entitypath=hc;sharedaccesssignature={sas}"
    )
    assert client.token_provider.get_token("sb://example/hc").token == sas


@pytest.mark.parametrize("fields", [
    "SharedAccessKeyName=;SharedAccessKey=",
    "SharedAccessKeyName=key",
    "SharedAccessSignature=invalid",
    "Authentication=Managed Identity",
    "Unknown=value",
    "EntityPath=duplicate",
])
def test_invalid_connection_settings_do_not_become_anonymous(fields):
    with pytest.raises(ValueError):
        HybridConnectionClient.from_connection_string("Endpoint=sb://example/;EntityPath=hc;" + fields)


def test_runtime_information_atom_parsing():
    data = b'''<entry xmlns="http://www.w3.org/2005/Atom"><content>
      <HybridConnectionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect">
        <CreatedAt>2026-01-01T00:00:00Z</CreatedAt><ListenerCount>3</ListenerCount>
        <RequiresClientAuthorization>false</RequiresClientAuthorization>
        <UserMetadata>example</UserMetadata>
      </HybridConnectionDescription></content></entry>'''
    info = parse_runtime_information(data)
    assert info.listener_count == 3
    assert info.created_at.year == 2026
    assert info.requires_client_authorization is False
    assert info.user_metadata == "example"
    with pytest.raises(EndpointNotFoundError):
        parse_runtime_information(b'<feed xmlns="http://www.w3.org/2005/Atom"/>')


async def test_subprotocol_and_logical_accept_url():
    seen = []
    async def connected(ws):
        seen.append(ws.subprotocol)
        await ws.wait_closed()
    async with websockets.serve(connected, "127.0.0.1", 0, subprotocols=["chat"]) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/$hc/hc/route?app=value&sb-hc-action=accept"
        receiver = listener(subprotocols=["chat"])
        contexts = []
        receiver.accept_handler = lambda context: contexts.append(context) or True
        await receiver._handle_accept({
            "address": url, "id": "id",
            "connect_headers": {"Host": "example", "Sec-WebSocket-Protocol": "chat"},
        })
        stream = await asyncio.wait_for(receiver.accept_connection(), 1)
        assert stream.websocket.subprotocol == "chat"
        assert contexts[0].request.url == "sb://example/hc/route?app=value"
        await stream.close()
        await receiver.close()
        client = HybridConnectionClient("sb://example/hc")
        client._build_connect_url = lambda **kwargs: url
        stream = await client.create_connection(subprotocols=["chat"])
        assert stream.websocket.subprotocol == "chat"
        await stream.close()


async def test_configurable_message_limit_and_incremental_byte_reads():
    payload = b"x" * (1024 * 1024 + 17)
    async def send(ws):
        await ws.send([payload[:500000], payload[500000:]])
        await ws.close()
    async with websockets.serve(send, "127.0.0.1", 0) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        client = HybridConnectionClient("sb://example/hc",
                                       transport=TransportOptions(max_message_size=2 * 1024 * 1024))
        client._build_connect_url = lambda **kwargs: url
        stream = await client.create_connection()
        chunks = []
        while chunk := await stream.read(16384):
            assert len(chunk) <= 16384
            chunks.append(chunk)
        assert b"".join(chunks) == payload


async def test_shutdown_sends_close_output_and_rejects_writes():
    closed = asyncio.Event()
    async def peer(ws):
        await ws.wait_closed()
        closed.set()
    async with websockets.serve(peer, "127.0.0.1", 0) as server:
        ws = await websockets.connect(f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}")
        stream = HybridConnectionStream(ws)
        await stream.shutdown()
        with pytest.raises(RuntimeError):
            await stream.send_bytes(b"late")
        await asyncio.wait_for(closed.wait(), 1)
        await stream.close()


async def test_incremental_http_response_arrives_before_handler_finishes():
    first_chunk = asyncio.Event()
    finish_handler = asyncio.Event()
    received = []
    async def relay(ws):
        header = json.loads(await ws.recv())
        assert header["response"]["body"] is True
        async for fragment in ws.recv_streaming():
            received.append(fragment)
            first_chunk.set()
        await ws.close()
    async with websockets.serve(relay, "127.0.0.1", 0) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        receiver = listener(limits=ListenerLimits(max_response_buffer_size=65536))
        async def handler(context):
            await context.response.output_stream.write_async(b"a" * 65536)
            await finish_handler.wait()
            await context.response.output_stream.write_async(b"tail")
        receiver.request_handler = handler
        task = asyncio.create_task(receiver._handle_control_request(request(url), b""))
        await asyncio.wait_for(first_chunk.wait(), 2)
        assert not task.done()
        finish_handler.set()
        await asyncio.wait_for(task, 2)
        if receiver._dispatch_tasks:
            await asyncio.wait_for(asyncio.gather(*receiver._dispatch_tasks), 2)
        assert b"".join(received) == b"a" * 65536 + b"tail"
        await receiver.close()


async def test_http_request_streaming_starts_before_final_fragment():
    started = asyncio.Event()
    release = asyncio.Event()
    captured = []
    async def relay(ws):
        await ws.send(json.dumps({"request": {**request(), "body": True}}))
        async def fragments():
            yield b"first"
            await release.wait()
            yield b"last"
        await ws.send(fragments())
        await ws.recv()
        await ws.close()
    async with websockets.serve(relay, "127.0.0.1", 0) as server:
        receiver = listener(stream_requests=True)
        async def handler(context):
            captured.append(await context.request.input_stream.read(5))
            started.set()
            captured.append(await context.request.read_body_async())
        receiver.request_handler = handler
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        task = asyncio.create_task(receiver._handle_rendezvous_request(url))
        await asyncio.wait_for(started.wait(), 2)
        assert captured == [b"first"]
        release.set()
        await asyncio.wait_for(task, 2)
        assert captured == [b"first", b"last"]
        await receiver.close()


async def test_slow_http_handler_gets_deadline_response():
    receiver = listener(limits=ListenerLimits(request_timeout=0.01))
    receiver._websocket = AsyncMock()
    async def handler(context):
        await asyncio.Event().wait()
    receiver.request_handler = handler
    with pytest.raises(TimeoutError):
        await receiver._handle_control_request(request(), b"")
    response = json.loads(receiver._websocket.send.call_args.args[0])
    assert response["response"]["statusCode"] == 500


async def test_small_response_flush_timer_and_metadata_freeze():
    first_chunk = asyncio.Event()
    received = []
    async def relay(ws):
        await ws.recv()
        async for fragment in ws.recv_streaming():
            received.append(fragment)
            first_chunk.set()
        await ws.close()
    async with websockets.serve(relay, "127.0.0.1", 0) as server:
        receiver = listener(limits=ListenerLimits(response_flush_interval=0.01))
        async def handler(context):
            context.response.output_stream.write(b"first")
            await asyncio.wait_for(first_chunk.wait(), 1)
            with pytest.raises(RuntimeError):
                context.response.headers["X-Late"] = "not sent"
            context.response.output_stream.write(b"last")
        receiver.request_handler = handler
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        await receiver._handle_control_request(request(url), b"")
        await asyncio.gather(*receiver._dispatch_tasks)
        assert b"".join(received) == b"firstlast"
        await receiver.close()


async def test_cancelled_streaming_response_closes_writer_and_socket():
    first_chunk = asyncio.Event()
    closed = asyncio.Event()
    async def relay(ws):
        await ws.recv()
        try:
            async for _ in ws.recv_streaming():
                first_chunk.set()
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            closed.set()
    async with websockets.serve(relay, "127.0.0.1", 0) as server:
        receiver = listener()
        async def handler(context):
            await context.response.output_stream.write_async(b"x" * 65536)
            await asyncio.Event().wait()
        receiver.request_handler = handler
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        receiver._spawn_dispatch(receiver._handle_control_request(request(url), b""))
        await asyncio.wait_for(first_chunk.wait(), 1)
        await asyncio.wait_for(receiver.close(), 2)
        await asyncio.wait_for(closed.wait(), 1)
        assert not receiver._dispatch_tasks
        assert not receiver._open_streams


async def test_overloaded_control_request_receives_503():
    receiver = listener(limits=ListenerLimits(max_concurrent_requests=1))
    busy = asyncio.create_task(asyncio.Event().wait())
    receiver._dispatch_tasks.add(busy)
    receiver._is_online = True
    receiver._websocket = AsyncMock()
    receiver._websocket.recv.side_effect = [
        json.dumps({"request": request()}), asyncio.CancelledError(),
    ]
    try:
        with pytest.raises(asyncio.CancelledError):
            await receiver._receive_loop()
        response = json.loads(receiver._websocket.send.call_args.args[0])
        assert response["response"]["statusCode"] == 503
        assert receiver._dispatch_tasks == {busy}
    finally:
        await receiver.close()


async def test_pending_connection_capacity_closes_excess_stream():
    receiver = listener(limits=ListenerLimits(max_pending_connections=1))
    sockets = [AsyncMock(), AsyncMock()]
    with patch("websockets.connect", new=AsyncMock(side_effect=sockets)):
        await receiver._accept_rendezvous("ws://example/one", "one", {})
        await receiver._accept_rendezvous("ws://example/two", "two", {})
    assert receiver._pending_connections.qsize() == 1
    sockets[1].close.assert_awaited_once_with(code=1013, reason="Listener capacity exceeded")
    await receiver.close()


async def test_streaming_body_and_sync_response_buffer_limits():
    async def fragments():
        yield b"aa"
        yield b"bb"
    body = RequestBody(SimpleNamespace(recv_streaming=fragments), 3, 1)
    assert await body.read(2) == b"aa"
    with pytest.raises(RelayError, match="exceeds configured limit"):
        await body.read(2)
    response = RelayedHttpListenerResponse(buffer_limit=3)
    with pytest.raises(BufferError):
        response.output_stream.write(b"four")
    assert response.get_body() == b""


async def test_runtime_http_request_is_authenticated_and_reads_all_chunks():
    from hybrid_connection.runtime import get_runtime_information
    xml = b'''<entry xmlns="http://www.w3.org/2005/Atom"><content>
    <HybridConnectionDescription xmlns="urn:test"><ListenerCount>2</ListenerCount>
    </HybridConnectionDescription></content></entry>'''
    async def chunks(size):
        for offset in range(0, len(xml), 7):
            yield xml[offset:offset + 7]
    response = AsyncMock()
    response.status = 200
    response.content.iter_chunked = chunks
    response.__aenter__.return_value = response
    session = AsyncMock()
    session.get = Mock(return_value=response)
    session.__aenter__.return_value = session
    with patch("hybrid_connection.runtime.aiohttp.ClientSession", return_value=session):
        result = await get_runtime_information(
            "sb://example/hc", TokenProvider("key", "dummy"), TransportOptions()
        )
    assert result.listener_count == 2
    args, kwargs = session.get.call_args
    assert args[0] == "https://example/hc?api-version=2016-07"
    assert kwargs["headers"]["Authorization"].startswith("SharedAccessSignature ")
    assert kwargs["headers"]["X-PROCESS-AT"] == "ServiceBus"
    assert kwargs["allow_redirects"] is False


async def test_transport_timeout_and_factory_options():
    factory = AsyncMock()
    transport = TransportOptions(
        open_timeout=2, read_timeout=3, write_timeout=4,
        max_message_size=12345, proxy=None, websocket_factory=factory,
    )
    await transport.connect("wss://example/hc", default_factory=Mock())
    assert factory.call_args.kwargs["max_size"] == 12345
    assert factory.call_args.kwargs["proxy"] is None
    async def blocked():
        await asyncio.Event().wait()
    stream = HybridConnectionStream(
        SimpleNamespace(recv=blocked), transport=TransportOptions(read_timeout=0.01)
    )
    with pytest.raises(TimeoutError):
        await stream.receive()


async def test_elapsed_retry_budget_bounds_inflight_connection():
    receiver = listener(retry_policy=RetryPolicy(
        initial_delay=0.001, max_delay=0.001, jitter=0, max_elapsed=0.02
    ))
    receiver._should_reconnect = True
    async def blocked():
        await asyncio.Event().wait()
    with patch.object(receiver, "_connect", new=blocked):
        await asyncio.wait_for(receiver._reconnect_loop(), 0.5)
    assert receiver._terminal_failure
    assert isinstance(receiver.last_error, TimeoutError)
    with pytest.raises(RuntimeError, match="close"):
        await receiver.open()
    await receiver.close()


async def test_accept_handler_deadline_rejects_instead_of_waiting_forever():
    receiver = listener(limits=ListenerLimits(accept_timeout=0.01))
    async def blocked(context):
        await asyncio.Event().wait()
    receiver.accept_handler = blocked
    with patch.object(receiver, "_reject_rendezvous", new=AsyncMock()) as reject:
        await asyncio.wait_for(receiver._handle_accept({
            "address": "wss://example/accept", "id": "id", "connect_headers": {},
        }), 0.5)
    assert reject.await_count == 1
    assert reject.call_args.kwargs["status_code"] == 408


async def test_failed_listener_owned_close_aborts_transport():
    receiver = listener()
    ws = AsyncMock()
    ws.close.side_effect = OSError("close failed")
    ws.transport.abort = Mock()
    stream = HybridConnectionStream(ws)
    receiver._open_streams.add(stream)
    await receiver.close()
    ws.transport.abort.assert_called_once()
    assert stream.is_closed
    assert not receiver._open_streams


async def test_timed_out_write_aborts_ambiguous_stream():
    async def blocked(data):
        await asyncio.Event().wait()
    transport = SimpleNamespace(abort=Mock())
    stream = HybridConnectionStream(
        SimpleNamespace(send=blocked, transport=transport),
        transport=TransportOptions(write_timeout=0.01),
    )
    with pytest.raises(TimeoutError):
        await stream.send_bytes(b"ambiguous")
    assert stream.is_closed
    transport.abort.assert_called_once()
    with pytest.raises(RuntimeError):
        await stream.send_bytes(b"must not replay")
