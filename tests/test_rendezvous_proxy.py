"""Exercise proxy preservation through a real offline HTTP CONNECT tunnel."""

import asyncio
import logging
import urllib.request
from unittest.mock import AsyncMock

import pytest
import websockets

from src.hybrid_connection import _rendezvous


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _rendezvous._SUPPORTS_PROXY,
    reason="Automatic/configured proxies require websockets 15 or newer",
)
@pytest.mark.parametrize("configuration", ["system", "explicit"])
async def test_proxy_owns_target_dns_and_routing(monkeypatch, caplog, configuration):
    handshakes = []
    tunnels = []
    tasks = []
    lookups = []

    async def echo(websocket):
        handshakes.append((
            websocket.request.headers["Host"], websocket.request.path
        ))
        async for payload in websocket:
            await websocket.send(payload)

    async def copy(reader, writer):
        try:
            while data := await reader.read(4096):
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()

    async with websockets.serve(echo, "127.0.0.1", 0) as target:
        port = target.sockets[0].getsockname()[1]

        async def tunnel(reader, writer):
            upstream_writer = None
            try:
                request = await reader.readuntil(b"\r\n\r\n")
                tunnels.append(request.split(b"\r\n", 1)[0].decode("ascii"))
                upstream_reader, upstream_writer = await asyncio.open_connection(
                    "127.0.0.1", port
                )
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                await asyncio.gather(
                    copy(reader, upstream_writer), copy(upstream_reader, writer)
                )
            finally:
                if upstream_writer is not None:
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                writer.close()
                await writer.wait_closed()

        def accept(reader, writer):
            tasks.append(asyncio.create_task(tunnel(reader, writer)))

        proxy_server = await asyncio.start_server(accept, "127.0.0.1", 0)
        proxy_port = proxy_server.sockets[0].getsockname()[1]
        proxy = f"http://127.0.0.1:{proxy_port}"
        monkeypatch.setattr(
            urllib.request, "getproxies",
            lambda: {"ws": proxy} if configuration == "system" else {},
        )
        monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
        loop = asyncio.get_running_loop()
        getaddrinfo = loop.getaddrinfo

        async def resolve(host, *args, **kwargs):
            lookups.append(host)
            assert host != "rendezvous.example", "Target DNS belongs to the proxy"
            return await getaddrinfo(host, *args, **kwargs)

        monkeypatch.setattr(loop, "getaddrinfo", resolve)
        direct = AsyncMock(side_effect=AssertionError("Proxy must not be bypassed"))
        monkeypatch.setattr(_rendezvous.aiohappyeyeballs, "start_connection", direct)
        uri = f"ws://rendezvous.example:{port}/request%2Fpath?value=a%2Bb&empty="
        options = {"proxy": proxy} if configuration == "explicit" else {}
        try:
            with caplog.at_level(logging.DEBUG, logger=_rendezvous.__name__):
                websocket = await _rendezvous.connect_rendezvous(
                    uri, open_timeout=2, **options
                )
            try:
                assert websocket.remote_address == ("127.0.0.1", proxy_port)
                await websocket.send(b"proxied echo")
                assert await websocket.recv() == b"proxied echo"
            finally:
                await websocket.close()
        finally:
            proxy_server.close()
            await proxy_server.wait_closed()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)

    assert tunnels == [f"CONNECT rendezvous.example:{port} HTTP/1.1"]
    assert handshakes == [(
        f"rendezvous.example:{port}", "/request%2Fpath?value=a%2Bb&empty="
    )]
    assert "rendezvous.example" not in lookups
    direct.assert_not_awaited()
    assert "address selection delegated to proxy" in caplog.text
    assert uri not in caplog.text
