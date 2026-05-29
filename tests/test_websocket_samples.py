"""Tests for the WebSocket rendezvous samples."""

import os
import sys

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
