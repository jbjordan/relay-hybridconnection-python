"""Offline transport tests, including real IPv4 and IPv6 loopback sockets."""

import asyncio
import errno
import logging
import socket
import ssl
from contextlib import asynccontextmanager
from http import HTTPStatus
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import websockets
from websockets.datastructures import Headers
from websockets.exceptions import InvalidHandshake, InvalidStatus
from websockets.http11 import Response

from src.hybrid_connection import _rendezvous


URI = (
    "wss://rendezvous.example:9443/$hc/path%2Fname?"
    "sb-hc-action=accept&sb-hc-token=test%2Btoken&empty="
)


def _address(family, host, port=9443, flowinfo=0, scope_id=0):
    sockaddr = (host, port)
    if family == socket.AF_INET6:
        sockaddr += (flowinfo, scope_id)
    return family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr


IPV4 = _address(socket.AF_INET, "192.0.2.1")
IPV6 = _address(socket.AF_INET6, "2001:db8::1")


class FakeSocket:
    def __init__(self, address):
        self.family = address[0]
        self.address = address[4]
        self.closed = False

    def setblocking(self, blocking):
        assert blocking is False

    def getpeername(self):
        assert not self.closed
        return self.address

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def proxy_settings(monkeypatch):
    settings = SimpleNamespace(
        getproxies=Mock(return_value={}), bypass=Mock(return_value=False)
    )
    monkeypatch.setattr(_rendezvous.urllib.request, "getproxies", settings.getproxies)
    monkeypatch.setattr(_rendezvous.urllib.request, "proxy_bypass", settings.bypass)
    return settings


@pytest.fixture
async def transport(monkeypatch):
    loop = asyncio.get_running_loop()
    sockets = []

    def make_socket(address):
        sock = FakeSocket(address)
        sockets.append(sock)
        return sock

    factory = Mock(side_effect=make_socket)
    start_connection = _rendezvous.aiohappyeyeballs.start_connection

    async def race(addresses, **kwargs):
        return await start_connection(addresses, socket_factory=factory, **kwargs)

    async def handshake(uri, **kwargs):
        websocket = Mock()
        sock = kwargs.get("sock")
        websocket.close = AsyncMock(side_effect=sock.close if sock else None)
        return websocket

    lookup = AsyncMock(return_value=[IPV4, IPV6])
    dial = AsyncMock()
    start = AsyncMock(side_effect=race)
    connect = AsyncMock(side_effect=handshake)
    monkeypatch.setattr(loop, "getaddrinfo", lookup)
    monkeypatch.setattr(loop, "sock_connect", dial)
    monkeypatch.setattr(_rendezvous.aiohappyeyeballs, "start_connection", start)
    monkeypatch.setattr(_rendezvous.websockets, "connect", connect)
    return SimpleNamespace(
        lookup=lookup, dial=dial, start=start, connect=connect,
        factory=factory, sockets=sockets,
    )


@pytest.mark.asyncio
async def test_ipv4_first_dns_starts_ipv6_and_keeps_uri_and_tls(transport, caplog):
    with caplog.at_level(logging.DEBUG, logger=_rendezvous.__name__):
        websocket = await _rendezvous.connect_rendezvous(URI, max_size=None)

    transport.lookup.assert_awaited_once_with(
        "rendezvous.example", 9443, family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP,
    )
    assert transport.dial.await_args.args[1] == IPV6[4]
    assert len(transport.sockets) == 1
    winner = transport.sockets[0]
    assert winner.family == socket.AF_INET6
    assert not winner.closed
    transport.start.assert_awaited_once_with(
        [IPV6, IPV4], happy_eyeballs_delay=0.25, interleave=1
    )
    transport.connect.assert_awaited_once_with(
        URI, sock=winner, open_timeout=None, max_size=None
    )
    assert "IPv6" in caplog.text
    assert "2001:db8::1" in caplog.text
    assert "test%2Btoken" not in caplog.text
    assert "sb-hc-" not in caplog.text
    await websocket.close()
    assert winner.closed


@pytest.mark.asyncio
async def test_opt_out_uses_legacy_transport_without_preconnecting(transport):
    websocket = await _rendezvous.connect_rendezvous(
        URI, prefer_ipv6=False, open_timeout=3, max_size=None
    )
    transport.connect.assert_awaited_once_with(URI, open_timeout=3, max_size=None)
    transport.lookup.assert_not_awaited()
    transport.start.assert_not_awaited()
    await websocket.close()


@pytest.mark.asyncio
async def test_missing_aaaa_uses_ipv4_without_delay(transport):
    transport.lookup.return_value = [IPV4]
    websocket = await _rendezvous.connect_rendezvous(URI, open_timeout=0.1)
    transport.dial.assert_awaited_once_with(transport.sockets[0], IPV4[4])
    assert transport.sockets[0].family == socket.AF_INET
    await websocket.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("socket_creation_fails", [False, True])
async def test_ipv6_failure_falls_back_immediately(transport, socket_creation_fails):
    if socket_creation_fails:
        make_socket = transport.factory.side_effect

        def factory(address):
            if address[0] == socket.AF_INET6:
                raise OSError(errno.EAFNOSUPPORT, "IPv6 is unsupported")
            return make_socket(address)

        transport.factory.side_effect = factory
    else:
        async def dial(sock, address):
            if sock.family == socket.AF_INET6:
                raise OSError(errno.ENETUNREACH, "IPv6 route unavailable")

        transport.dial.side_effect = dial

    websocket = await _rendezvous.connect_rendezvous(URI, open_timeout=0.1)
    assert transport.sockets[-1].family == socket.AF_INET
    assert all(sock.closed for sock in transport.sockets[:-1])
    assert not transport.sockets[-1].closed
    await websocket.close()


@pytest.mark.asyncio
async def test_stalled_ipv6_does_not_block_ipv4(transport):
    cancelled = asyncio.Event()

    async def dial(sock, address):
        if sock.family == socket.AF_INET6:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    transport.dial.side_effect = dial
    websocket = await _rendezvous.connect_rendezvous(URI, open_timeout=1)
    assert [sock.family for sock in transport.sockets] == [
        socket.AF_INET6, socket.AF_INET
    ]
    assert cancelled.is_set()
    assert transport.sockets[0].closed
    assert not transport.sockets[1].closed
    await websocket.close()


@pytest.mark.asyncio
async def test_addresses_interleave_without_losing_ipv6_scope(transport):
    ipv6_scoped = _address(socket.AF_INET6, "fe80::1", flowinfo=42, scope_id=7)
    ipv6_second = _address(socket.AF_INET6, "2001:db8::2")
    ipv4_second = _address(socket.AF_INET, "192.0.2.2")
    transport.lookup.return_value = [IPV4, ipv4_second, ipv6_scoped, ipv6_second]

    async def dial(sock, address):
        if address != ipv4_second[4]:
            raise OSError(errno.ECONNREFUSED, "Connection refused")

    transport.dial.side_effect = dial
    websocket = await _rendezvous.connect_rendezvous(URI)
    assert [call.args[1] for call in transport.dial.await_args_list] == [
        ipv6_scoped[4], IPV4[4], ipv6_second[4], ipv4_second[4]
    ]
    assert all(sock.closed for sock in transport.sockets[:-1])
    await websocket.close()


@pytest.mark.asyncio
async def test_all_addresses_fail_without_a_websocket_handshake(transport):
    transport.dial.side_effect = OSError(errno.ECONNREFUSED, "Connection refused")
    with pytest.raises(OSError, match="Connection refused"):
        await _rendezvous.connect_rendezvous(URI)
    assert len(transport.sockets) == 2
    assert all(sock.closed for sock in transport.sockets)
    transport.connect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup_error", [None, socket.gaierror("DNS failed")])
async def test_empty_or_failed_dns_does_not_open_sockets(transport, lookup_error):
    transport.lookup.return_value = []
    transport.lookup.side_effect = lookup_error
    with pytest.raises(OSError):
        await _rendezvous.connect_rendezvous(URI)
    transport.start.assert_not_awaited()
    transport.connect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["dns", "tcp", "handshake"])
async def test_cancellation_closes_pending_and_selected_sockets(transport, phase):
    started = asyncio.Event()
    cancelled = []

    async def blocked(*args, **kwargs):
        if phase != "tcp" or len(transport.sockets) == 2:
            started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(args)

    target = {"dns": transport.lookup, "tcp": transport.dial,
              "handshake": transport.connect}[phase]
    target.side_effect = blocked
    before = asyncio.all_tasks()
    task = asyncio.create_task(_rendezvous.connect_rendezvous(URI))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert cancelled
    assert all(sock.closed for sock in transport.sockets)
    assert not (asyncio.all_tasks() - before)
    if phase != "handshake":
        transport.connect.assert_not_awaited()
    else:
        transport.connect.assert_awaited_once()


@pytest.mark.asyncio
async def test_dns_tcp_and_handshake_share_one_opening_budget(transport):
    async def lookup(*args, **kwargs):
        await asyncio.sleep(0.04)
        return [IPV6, IPV4]

    async def dial(*args):
        await asyncio.sleep(0.04)

    async def handshake(*args, **kwargs):
        await asyncio.sleep(0.04)
        return Mock()

    transport.lookup.side_effect = lookup
    transport.dial.side_effect = dial
    transport.connect.side_effect = handshake
    with pytest.raises(asyncio.TimeoutError):
        await _rendezvous.connect_rendezvous(URI, open_timeout=0.1)
    assert all(sock.closed for sock in transport.sockets)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["dns", "tcp", "handshake"])
async def test_opening_timeout_cleans_up_each_phase(transport, phase):
    async def blocked(*args, **kwargs):
        await asyncio.Event().wait()

    target = {"dns": transport.lookup, "tcp": transport.dial,
              "handshake": transport.connect}[phase]
    target.side_effect = blocked
    with pytest.raises(asyncio.TimeoutError):
        await _rendezvous.connect_rendezvous(URI, open_timeout=0.05)
    assert all(sock.closed for sock in transport.sockets)
    if phase == "dns":
        transport.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_simultaneous_tcp_winners_close_the_runner_up(transport, monkeypatch):
    monkeypatch.setattr(_rendezvous, "_HAPPY_EYEBALLS_DELAY", 0.01)
    ready = asyncio.Event()

    async def dial(sock, address):
        if sock.family == socket.AF_INET:
            ready.set()
        await ready.wait()

    transport.dial.side_effect = dial
    websocket = await _rendezvous.connect_rendezvous(URI)
    assert len(transport.sockets) == 2
    winner = transport.connect.await_args.kwargs["sock"]
    assert not winner.closed
    assert all(sock.closed for sock in transport.sockets if sock is not winner)
    transport.connect.assert_awaited_once()
    await websocket.close()
    assert all(sock.closed for sock in transport.sockets)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    InvalidStatus(Response(410, "Gone", Headers())),
    InvalidStatus(Response(401, "Unauthorized", Headers())),
    InvalidHandshake("Invalid WebSocket upgrade"),
    ssl.SSLCertVerificationError(1, "Certificate hostname mismatch"),
])
async def test_handshake_failures_never_retry_on_ipv4(transport, error):
    transport.connect.side_effect = error
    with pytest.raises(type(error)) as raised:
        await _rendezvous.connect_rendezvous(URI)
    assert raised.value is error
    transport.connect.assert_awaited_once()
    transport.start.assert_awaited_once()
    assert len(transport.sockets) == 1
    assert transport.sockets[0].family == socket.AF_INET6
    assert transport.sockets[0].closed


@pytest.mark.asyncio
@pytest.mark.parametrize("scheme,proxies,bypass,options,supports_proxy,delegated", [
    ("wss", {"wss": "http://proxy"}, False, {}, True, True),
    ("wss", {"https": "http://proxy"}, False, {}, True, True),
    ("wss", {"https": "socks5h://proxy"}, False, {}, True, True),
    ("wss", {"socks": "http://proxy"}, False, {}, True, True),
    ("wss", {"http": "http://proxy"}, False, {}, True, False),
    ("wss", {"ws": "http://proxy"}, False, {}, True, False),
    ("ws", {"ws": "http://proxy"}, False, {}, True, True),
    ("ws", {"https": "http://proxy"}, False, {}, True, True),
    ("ws", {"http": "http://proxy"}, False, {}, True, True),
    ("ws", {"socks": "socks5h://proxy"}, False, {}, True, True),
    ("wss", {"wss": "http://proxy"}, True, {}, True, False),
    ("ws", {"http": "http://proxy"}, True, {}, True, False),
    ("wss", {}, False, {"proxy": "http://explicit-proxy"}, True, True),
    ("wss", {"https": "http://proxy"}, False, {"proxy": None}, True, False),
    ("wss", {"https": "http://proxy"}, False, {}, False, False),
])
async def test_proxy_selection_precedes_destination_dns(
    transport, proxy_settings, monkeypatch, scheme, proxies, bypass, options,
    supports_proxy, delegated,
):
    monkeypatch.setattr(_rendezvous, "_SUPPORTS_PROXY", supports_proxy)
    proxy_settings.getproxies.return_value = proxies
    proxy_settings.bypass.return_value = bypass
    uri = URI.replace("wss://", f"{scheme}://", 1)
    websocket = await _rendezvous.connect_rendezvous(uri, **options)
    if delegated:
        transport.lookup.assert_not_awaited()
        transport.start.assert_not_awaited()
        transport.connect.assert_awaited_once_with(uri, open_timeout=10, **options)
    else:
        transport.start.assert_awaited_once()
    if supports_proxy and "proxy" not in options:
        proxy_settings.bypass.assert_called_once_with("rendezvous.example:9443")
    await websocket.close()


def test_no_proxy_matches_the_original_host_and_custom_port(monkeypatch):
    monkeypatch.setattr(_rendezvous, "_SUPPORTS_PROXY", True)
    monkeypatch.setattr(
        _rendezvous.urllib.request, "proxy_bypass",
        _rendezvous.urllib.request.proxy_bypass_environment,
    )
    monkeypatch.setattr(
        _rendezvous.urllib.request, "getproxies_environment",
        lambda: {"no": "rendezvous.example:9443", "https": "http://proxy"},
    )
    monkeypatch.setattr(
        _rendezvous.urllib.request, "getproxies",
        lambda: {"https": "http://proxy"},
    )
    assert not _rendezvous._uses_proxy(_rendezvous.parse_uri(URI), {})
    other_port = URI.replace(":9443/", ":9444/")
    assert _rendezvous._uses_proxy(_rendezvous.parse_uri(other_port), {})


async def _echo(websocket):
    async for message in websocket:
        await websocket.send(message)


@asynccontextmanager
async def _dual_stack_server(process_request=None):
    ipv6_socket = None
    try:
        ipv6_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        ipv6_socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        ipv6_socket.bind(("::1", 0, 0, 0))
    except OSError as exc:
        if ipv6_socket is not None:
            ipv6_socket.close()
        if exc.errno in {
            errno.EAFNOSUPPORT, errno.EPROTONOSUPPORT, errno.EADDRNOTAVAIL,
            errno.ENOPROTOOPT,
        }:
            pytest.skip(f"OS IPv6 loopback is unavailable: {exc}")
        raise

    try:
        port = ipv6_socket.getsockname()[1]
        async with websockets.serve(
            _echo, sock=ipv6_socket, process_request=process_request
        ), websockets.serve(
            _echo, "127.0.0.1", port, process_request=process_request
        ):
            yield port
    finally:
        ipv6_socket.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("prefer_ipv6", [True, False])
async def test_real_dual_stack_peer_family_and_original_http_target(
    monkeypatch, prefer_ipv6,
):
    requests = []

    def record_request(connection, request):
        requests.append((
            connection.transport.get_extra_info("socket").family,
            request.headers["Host"], request.path,
        ))

    async with _dual_stack_server(record_request) as port:
        lookup = AsyncMock(return_value=[
            _address(socket.AF_INET, "127.0.0.1", port),
            _address(socket.AF_INET6, "::1", port),
        ])
        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
        uri = f"ws://rendezvous.example:{port}/path%2Fpart?token=a%2Bb&empty="
        websocket = await _rendezvous.connect_rendezvous(
            uri, prefer_ipv6=prefer_ipv6
        )
        try:
            selected = socket.AF_INET6 if prefer_ipv6 else socket.AF_INET
            sock = websocket.transport.get_extra_info("socket")
            assert sock.family == selected
            assert sock.getsockname()[0] == ("::1" if prefer_ipv6 else "127.0.0.1")
            assert websocket.remote_address[0] == sock.getsockname()[0]
            assert requests == [(
                selected, f"rendezvous.example:{port}", "/path%2Fpart?token=a%2Bb&empty="
            )]
            await websocket.send(b"actual loopback peer")
            assert await websocket.recv() == b"actual loopback peer"
        finally:
            await websocket.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [HTTPStatus.UNAUTHORIZED, HTTPStatus.GONE])
async def test_real_http_failure_performs_only_one_upgrade(monkeypatch, status):
    families = []

    def reject(connection, request):
        families.append(connection.transport.get_extra_info("socket").family)
        return connection.respond(status, status.phrase)

    async with _dual_stack_server(reject) as port:
        monkeypatch.setattr(
            asyncio.get_running_loop(), "getaddrinfo", AsyncMock(return_value=[
                _address(socket.AF_INET, "127.0.0.1", port),
                _address(socket.AF_INET6, "::1", port),
            ]),
        )
        with pytest.raises(InvalidStatus) as raised:
            await _rendezvous.connect_rendezvous(f"ws://rendezvous.example:{port}/")
        assert raised.value.response.status_code == status
        assert families == [socket.AF_INET6]


@pytest.mark.asyncio
@pytest.mark.parametrize("ipv6_mode", ["missing", "unreachable", "stalled"])
async def test_real_ipv4_echo_after_fallback(monkeypatch, ipv6_mode):
    loop = asyncio.get_running_loop()
    real_connect = loop.sock_connect
    sockets = []

    async def dial(sock, address):
        sockets.append(sock)
        if sock.family == socket.AF_INET6:
            if ipv6_mode == "stalled":
                await asyncio.Event().wait()
            raise OSError(errno.ENETUNREACH, "IPv6 route unavailable")
        await real_connect(sock, address)

    async with websockets.serve(_echo, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        addresses = [_address(socket.AF_INET, "127.0.0.1", port)]
        if ipv6_mode != "missing":
            addresses.append(_address(socket.AF_INET6, "::1", port))
        monkeypatch.setattr(loop, "getaddrinfo", AsyncMock(return_value=addresses))
        monkeypatch.setattr(loop, "sock_connect", dial)
        websocket = await _rendezvous.connect_rendezvous(
            f"ws://rendezvous.example:{port}/", open_timeout=1
        )
        try:
            assert websocket.transport.get_extra_info("socket").family == socket.AF_INET
            assert websocket.remote_address == ("127.0.0.1", port)
            await websocket.send("IPv4 fallback")
            assert await websocket.recv() == "IPv4 fallback"
        finally:
            await websocket.close()
        assert all(sock.fileno() == -1 for sock in sockets)
