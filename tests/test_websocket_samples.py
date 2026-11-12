"""Tests for the WebSocket rendezvous samples."""

import os
import sys
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'samples'))


def test_websocket_listener_imports():
    """The websocket_listener sample should be importable."""
    try:
        import websocket_listener  # noqa: F401
    except ImportError as e:
        pytest.fail(f"Failed to import websocket_listener: {e}")


def test_websocket_listener_has_main():
    import websocket_listener
    assert hasattr(websocket_listener, "main")
    assert hasattr(websocket_listener, "echo")
    assert hasattr(websocket_listener, "accept_handler")


def test_websocket_sender_imports():
    """The websocket_sender sample should be importable."""
    try:
        import websocket_sender  # noqa: F401
    except ImportError as e:
        pytest.fail(f"Failed to import websocket_sender: {e}")


def test_websocket_sender_has_main():
    import websocket_sender
    assert hasattr(websocket_sender, "main")


@pytest.mark.asyncio
async def test_websocket_sender_uses_a_fresh_uuid_for_each_connection(monkeypatch):
    import websocket_sender

    streams = []
    for _ in range(2):
        stream = MagicMock()
        stream.send_text = AsyncMock()
        stream.send_bytes = AsyncMock()
        stream.receive = AsyncMock(
            side_effect=[
                ("hello 0", "text"),
                ("hello 1", "text"),
                ("hello 2", "text"),
                (b"\x00\x01\x02\x03", "binary"),
            ]
        )
        stream.close = AsyncMock()
        streams.append(stream)

    client = MagicMock()
    client.create_connection = AsyncMock(side_effect=streams)
    monkeypatch.setattr(
        websocket_sender.HybridConnectionClient,
        "from_connection_string",
        MagicMock(return_value=client),
    )

    await websocket_sender.main()
    await websocket_sender.main()

    tracking_ids = [
        call.kwargs["hc_id"] for call in client.create_connection.await_args_list
    ]
    assert len(tracking_ids) == 2
    assert all(UUID(tracking_id).version == 4 for tracking_id in tracking_ids)
    assert tracking_ids[0] != tracking_ids[1]
    for stream in streams:
        stream.close.assert_awaited_once()
