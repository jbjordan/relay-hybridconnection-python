"""Pytest configuration and fixtures for hybrid connection tests."""

import os
import pytest
from unittest.mock import AsyncMock, Mock


def get_relay_connection_string() -> str:
    """Get the Azure Relay connection string from environment variable.
    
    Returns:
        The connection string from the 'relay-python' environment variable.
    
    Raises:
        ValueError: If the environment variable is not set.
    """
    conn_str = os.environ.get("relay-python")
    if not conn_str:
        raise ValueError(
            "Environment variable 'relay-python' is not set. "
            "Please set it to your Azure Relay connection string."
        )
    return conn_str


@pytest.fixture
def connection_string():
    """Provides the Azure Relay connection string for testing."""
    return get_relay_connection_string()


@pytest.fixture
def mock_websocket():
    """Returns a mock WebSocket for unit testing."""
    ws = AsyncMock()
    ws.open = True
    ws.close_code = None
    ws.close_reason = None
    
    # Mock send method
    ws.send = AsyncMock()
    
    # Mock recv method (returns accept message by default)
    ws.recv = AsyncMock(return_value='{"type": "accept"}')
    
    # Mock close method
    ws.close = AsyncMock()
    
    # Mock ping method
    ws.ping = AsyncMock()
    
    # Mock pong waiter
    ws.pong_waiter = AsyncMock()
    
    return ws


def _parse_connection_string(conn_str: str) -> dict:
    """Parse connection string into its components.
    
    Returns:
        Dictionary with 'endpoint', 'namespace', 'key_name', 'key', and 'entity_path'.
    """
    parts = {}
    for part in conn_str.split(";"):
        if part.startswith("Endpoint=sb://"):
            parts["endpoint"] = part.replace("Endpoint=", "")
            parts["namespace"] = part.replace("Endpoint=sb://", "").rstrip("/")
        elif part.startswith("SharedAccessKeyName="):
            parts["key_name"] = part.replace("SharedAccessKeyName=", "")
        elif part.startswith("SharedAccessKey="):
            parts["key"] = part.replace("SharedAccessKey=", "")
        elif part.startswith("EntityPath="):
            parts["entity_path"] = part.replace("EntityPath=", "")
    return parts


@pytest.fixture
def namespace():
    """Provides the relay namespace for testing."""
    conn_str = get_relay_connection_string()
    return _parse_connection_string(conn_str)["namespace"]


@pytest.fixture
def path():
    """Provides the hybrid connection path for testing."""
    conn_str = get_relay_connection_string()
    return _parse_connection_string(conn_str)["entity_path"]


@pytest.fixture
def key_name():
    """Provides the shared access key name for testing."""
    conn_str = get_relay_connection_string()
    return _parse_connection_string(conn_str)["key_name"]


@pytest.fixture
def key():
    """Provides the shared access key for testing."""
    conn_str = get_relay_connection_string()
    return _parse_connection_string(conn_str)["key"]
