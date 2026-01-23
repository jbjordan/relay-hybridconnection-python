"""Tests for pytest configuration and fixtures."""

import pytest
from unittest.mock import AsyncMock


def test_connection_string_fixture(connection_string):
    """Test that connection_string fixture returns valid string."""
    assert isinstance(connection_string, str)
    assert "Endpoint=sb://" in connection_string
    assert "SharedAccessKeyName=" in connection_string
    assert "SharedAccessKey=" in connection_string
    assert "EntityPath=" in connection_string


def test_namespace_fixture(namespace):
    """Test that namespace fixture returns expected value."""
    assert isinstance(namespace, str)
    assert ".servicebus.windows.net" in namespace


def test_path_fixture(path):
    """Test that path fixture returns expected value."""
    assert isinstance(path, str)
    assert len(path) > 0


def test_key_name_fixture(key_name):
    """Test that key_name fixture returns expected value."""
    assert isinstance(key_name, str)
    assert len(key_name) > 0


def test_key_fixture(key):
    """Test that key fixture returns expected value."""
    assert isinstance(key, str)
    assert len(key) > 0


def test_mock_websocket_fixture(mock_websocket):
    """Test that mock_websocket fixture returns properly configured mock."""
    assert mock_websocket is not None
    assert hasattr(mock_websocket, 'send')
    assert hasattr(mock_websocket, 'recv')
    assert hasattr(mock_websocket, 'close')
    assert hasattr(mock_websocket, 'ping')
    assert mock_websocket.open is True
    
    # Check that methods are async mocks
    assert isinstance(mock_websocket.send, AsyncMock)
    assert isinstance(mock_websocket.recv, AsyncMock)
    assert isinstance(mock_websocket.close, AsyncMock)
    assert isinstance(mock_websocket.ping, AsyncMock)


@pytest.mark.asyncio
async def test_mock_websocket_send(mock_websocket):
    """Test that mock_websocket.send() works."""
    await mock_websocket.send("test message")
    mock_websocket.send.assert_called_once_with("test message")


@pytest.mark.asyncio
async def test_mock_websocket_recv(mock_websocket):
    """Test that mock_websocket.recv() returns accept message."""
    message = await mock_websocket.recv()
    assert message == '{"type": "accept"}'


@pytest.mark.asyncio
async def test_mock_websocket_close(mock_websocket):
    """Test that mock_websocket.close() works."""
    await mock_websocket.close()
    mock_websocket.close.assert_called_once()


@pytest.mark.asyncio
async def test_mock_websocket_ping(mock_websocket):
    """Test that mock_websocket.ping() works."""
    await mock_websocket.ping()
    mock_websocket.ping.assert_called_once()
