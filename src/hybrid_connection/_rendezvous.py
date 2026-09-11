"""IPv6-first transport selection for listener rendezvous connections."""

import asyncio
import inspect
import logging
import socket
import urllib.request
from typing import Any, Optional

import aiohappyeyeballs
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.uri import WebSocketURI, parse_uri


logger = logging.getLogger(__name__)
_SUPPORTS_PROXY = "proxy" in inspect.signature(websockets.connect).parameters
_HAPPY_EYEBALLS_DELAY = 0.25


async def connect_rendezvous(
    uri: str,
    *,
    prefer_ipv6: bool = True,
    open_timeout: Optional[float] = 10,
    **kwargs: Any,
) -> ClientConnection:
    """Open one WebSocket handshake after selecting a reachable TCP address.

    Proxy routes and the opt-out retain the installed websockets transport.
    Direct connections share one timeout across DNS, TCP, TLS, and the upgrade.
    """
    if not prefer_ipv6:
        return await websockets.connect(uri, open_timeout=open_timeout, **kwargs)

    parsed_uri = parse_uri(uri)
    if _uses_proxy(parsed_uri, kwargs):
        logger.debug("Rendezvous address selection delegated to proxy")
        return await websockets.connect(uri, open_timeout=open_timeout, **kwargs)

    return await asyncio.wait_for(
        _connect_direct(uri, parsed_uri, **kwargs), timeout=open_timeout
    )


def _uses_proxy(uri: WebSocketURI, kwargs: dict[str, Any]) -> bool:
    if not _SUPPORTS_PROXY:
        return False

    proxy = kwargs.get("proxy", True)
    if proxy is not True:
        return proxy is not None
    if urllib.request.proxy_bypass(f"{uri.host}:{uri.port}"):
        return False

    # Only detect whether a route is proxied. Let websockets choose the proxy,
    # including its priority, authentication, and remote-DNS behavior.
    proxies = urllib.request.getproxies()
    schemes = ("wss", "socks", "https") if uri.secure else (
        "ws", "socks", "https", "http"
    )
    return any(proxies.get(scheme) is not None for scheme in schemes)


async def _connect_direct(
    uri: str, parsed_uri: WebSocketURI, **kwargs: Any
) -> ClientConnection:
    loop = asyncio.get_running_loop()
    addresses = await loop.getaddrinfo(
        parsed_uri.host,
        parsed_uri.port,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    if not addresses:
        raise OSError("getaddrinfo returned no rendezvous addresses")

    # Stable sorting keeps resolver preference within each address family.
    addresses.sort(key=lambda address: address[0] != socket.AF_INET6)
    sock = await aiohappyeyeballs.start_connection(
        addresses, happy_eyeballs_delay=_HAPPY_EYEBALLS_DELAY, interleave=1
    )
    handed_off = False
    try:
        logger.debug(
            "Rendezvous TCP connected over %s to %s",
            "IPv6" if sock.family == socket.AF_INET6 else "IPv4",
            sock.getpeername(),
        )
        # The original URI retains Host, TLS SNI, and certificate validation.
        # Only TCP attempts are raced; a single-use upgrade is never retried here.
        websocket = await websockets.connect(
            uri, sock=sock, open_timeout=None, **kwargs
        )
        handed_off = True
        return websocket
    finally:
        if not handed_off:
            sock.close()
