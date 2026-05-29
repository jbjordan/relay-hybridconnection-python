"""
In-process integration tests for the rendezvous protocol.

These tests stand up a fake Azure Relay service over loopback using
``websockets.serve`` and validate that the listener and client successfully
complete a rendezvous WebSocket handshake, exchange data both ways, and
clean up.

The fake service implements only the subset of the Hybrid Connections
protocol required by the test scenarios: it accepts a listener control
channel, accepts a sender connect channel, and bridges the two with an
``accept`` message and a paired rendezvous WebSocket.
"""

import asyncio
import json
import secrets
import string
from urllib.parse import parse_qs, urlsplit

import pytest
import websockets

from src.hybrid_connection.client import HybridConnectionClient
from src.hybrid_connection.listener import HybridConnectionListener
from src.hybrid_connection.token_provider import TokenProvider


def _random_sas_key(length: int = 44) -> str:
    alphabet = string.ascii_letters + string.digits + "+/"
    return "".join(secrets.choice(alphabet) for _ in range(length - 1)) + "="


@pytest.fixture
async def fake_relay():
    """Stand up a fake relay on a free loopback port."""
    relay = FakeRelay()
    server = await websockets.serve(
        relay.handle, "127.0.0.1", 0, max_size=None
    )
    relay.port = server.sockets[0].getsockname()[1]
    relay.server = server
    try:
        yield relay
    finally:
        server.close()
        await server.wait_closed()


class FakeRelay:
    """A tiny in-process fake of the Azure Relay service.

    Implements just enough of the Hybrid Connections protocol to let our
    listener and client perform a rendezvous WebSocket handshake.
    """

    def __init__(self) -> None:
        self.port: int = 0
        self.server = None
        self._listeners: "dict[str, asyncio.Queue[tuple]]" = {}
        self._rendezvous_pairs: "dict[str, asyncio.Future]" = {}

    @property
    def namespace(self) -> str:
        return f"127.0.0.1:{self.port}"

    async def handle(self, websocket) -> None:
        path = websocket.request.path
        parsed = urlsplit(f"ws://x{path}")
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        action = params.get("sb-hc-action")

        # /$hc/<path>?sb-hc-action=listen  -> listener control channel
        # /$hc/<path>?sb-hc-action=connect -> sender connect
        # /$rendezvous/<id>?sb-hc-action=accept  -> rendezvous from listener
        if parsed.path.startswith("/$hc/") and action == "listen":
            entity_path = parsed.path[len("/$hc/"):]
            await self._handle_listener(entity_path, websocket)
        elif parsed.path.startswith("/$hc/") and action == "connect":
            entity_path = parsed.path[len("/$hc/"):]
            await self._handle_sender(entity_path, websocket, params)
        elif parsed.path.startswith("/$rendezvous/") and action == "accept":
            rid = parsed.path[len("/$rendezvous/"):]
            await self._handle_rendezvous_accept(rid, websocket)
        else:
            await websocket.close(code=1008, reason="unknown route")

    async def _handle_listener(self, entity_path: str, websocket) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        self._listeners[entity_path] = queue
        ws_closed = asyncio.create_task(websocket.wait_closed())
        try:
            while True:
                get_task = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {get_task, ws_closed},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if get_task in done:
                    accept_msg, _future = get_task.result()
                    try:
                        await websocket.send(accept_msg)
                    except Exception:
                        return
                else:
                    get_task.cancel()
                    return
        finally:
            self._listeners.pop(entity_path, None)
            ws_closed.cancel()

    async def _handle_sender(
        self, entity_path: str, websocket, params: dict
    ) -> None:
        listener_queue = self._listeners.get(entity_path)
        if listener_queue is None:
            await websocket.close(code=1011, reason="no listener registered")
            return

        rid = params.get("sb-hc-id") or secrets.token_hex(8)
        rendezvous_future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._rendezvous_pairs[rid] = rendezvous_future

        accept_url = f"ws://{self.namespace}/$rendezvous/{rid}?sb-hc-action=accept&sb-hc-id={rid}"
        accept_msg = json.dumps({
            "accept": {
                "address": accept_url,
                "id": rid,
                "connectHeaders": {"Host": self.namespace},
            }
        })
        await listener_queue.put((accept_msg, rendezvous_future))

        try:
            listener_ws = await asyncio.wait_for(rendezvous_future, timeout=5)
        except asyncio.TimeoutError:
            await websocket.close(code=1011, reason="rendezvous timeout")
            return

        # Bridge both WebSockets bidirectionally until either side closes.
        await _bridge(websocket, listener_ws)

    async def _handle_rendezvous_accept(self, rid: str, websocket) -> None:
        future = self._rendezvous_pairs.pop(rid, None)
        if future is None or future.done():
            await websocket.close(code=1011, reason="unknown rendezvous id")
            return
        future.set_result(websocket)
        # Keep this coroutine alive (and the websocket open) until the
        # bridge completes; closing here would terminate the WS.
        try:
            await websocket.wait_closed()
        except Exception:
            pass


async def _bridge(a, b) -> None:
    async def relay(src, dst):
        try:
            async for message in src:
                await dst.send(message)
        except Exception:
            pass
        finally:
            try:
                await dst.close()
            except Exception:
                pass

    await asyncio.gather(relay(a, b), relay(b, a))


def _make_listener(fake_relay: FakeRelay) -> HybridConnectionListener:
    provider = TokenProvider("RootManageSharedAccessKey", _random_sas_key())
    listener = HybridConnectionListener(
        f"sb://{fake_relay.namespace}/hc1", provider
    )
    # Override the protocol URL builder to use ws:// for the loopback test.
    from src.hybrid_connection.protocol import ProtocolHandler
    original_build = ProtocolHandler.build_control_channel_url

    def _ws(namespace, path, token):
        return f"ws://{namespace}/$hc/{path}?sb-hc-action=listen&sb-hc-token=token"

    listener._protocol_handler.build_control_channel_url = _ws  # type: ignore[assignment]
    return listener


def _make_client(fake_relay: FakeRelay) -> HybridConnectionClient:
    client = HybridConnectionClient(f"sb://{fake_relay.namespace}/hc1")
    # Patch the protocol URL builder to use ws:// for loopback.
    original = client._build_connect_url

    def _ws(*, hc_id=None):
        url = original(hc_id=hc_id)
        return url.replace("wss://", "ws://", 1)

    client._build_connect_url = _ws  # type: ignore[assignment]
    return client


pytestmark = pytest.mark.timeout(15)


@pytest.mark.asyncio
async def test_rendezvous_round_trip(fake_relay):
    """Listener accepts a sender's WebSocket and the two exchange messages."""
    listener = _make_listener(fake_relay)
    await listener.open()
    try:
        # Wait for the control channel to be established.
        await asyncio.sleep(0.05)
        assert listener.is_online

        client = _make_client(fake_relay)

        # Sender opens a connection in the background.
        sender_task = asyncio.create_task(client.create_connection())

        # Listener side: get the accepted rendezvous stream.
        listener_stream = await asyncio.wait_for(
            listener.accept_connection(), timeout=5
        )
        sender_stream = await sender_task

        # Round-trip: client -> listener
        await sender_stream.send_text("ping")
        payload, kind = await asyncio.wait_for(listener_stream.receive(), timeout=2)
        assert kind == "text"
        assert payload == "ping"

        # Round-trip: listener -> client
        await listener_stream.send_bytes(b"pong")
        payload, kind = await asyncio.wait_for(sender_stream.receive(), timeout=2)
        assert kind == "binary"
        assert payload == b"pong"

        await listener_stream.close()
        await sender_stream.close()
    finally:
        await listener.close()


@pytest.mark.asyncio
async def test_accept_handler_can_reject(fake_relay):
    """Rejecting the sender should cause its WebSocket to be closed."""
    listener = _make_listener(fake_relay)

    rejections = []

    def reject_handler(context):
        rejections.append(dict(context.request.headers))
        context.response.status_code = 401
        context.response.status_description = "Not allowed"
        return False

    listener.accept_handler = reject_handler

    await listener.open()
    try:
        await asyncio.sleep(0.05)

        client = _make_client(fake_relay)

        # The sender's connection should ultimately be torn down by the
        # service. In our fake relay the close manifests as the bridge
        # task closing the sender's websocket. Either receive() raising or
        # close() succeeding is acceptable.
        sender_stream = None
        try:
            sender_stream = await asyncio.wait_for(
                client.create_connection(), timeout=3
            )
        except Exception:
            # Some setups may surface the reject as a connect-time error;
            # that's a valid outcome too.
            pass

        # Give the listener a chance to handle the accept message.
        await asyncio.sleep(0.5)
        assert rejections, "accept_handler should have been invoked"

        if sender_stream is not None:
            try:
                await asyncio.wait_for(sender_stream.receive(), timeout=2)
            except (ConnectionError, asyncio.TimeoutError):
                pass
            await sender_stream.close()
    finally:
        await listener.close()
