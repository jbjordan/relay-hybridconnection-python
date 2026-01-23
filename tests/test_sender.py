"""
Unit tests for the sender sample application.
"""

import pytest
import sys
from pathlib import Path


# Add samples directory to path
samples_dir = Path(__file__).parent.parent / "samples"
sys.path.insert(0, str(samples_dir))


def test_sender_imports():
    """Test that sender module can be imported."""
    import sender
    assert sender is not None


def test_sender_relay_url():
    """Test that RELAY_URL is correctly configured."""
    import sender
    assert sender.RELAY_URL.startswith("https://")
    assert ".servicebus.windows.net/" in sender.RELAY_URL


def test_sender_main_function_exists():
    """Test that main() function is defined."""
    import sender
    assert hasattr(sender, "main")
    assert callable(sender.main)


@pytest.mark.asyncio
async def test_sender_main_with_mock(mocker):
    """Test sender main() with mocked aiohttp session."""
    import sender
    
    # Mock response
    mock_response = mocker.MagicMock()
    mock_response.status = 200
    mock_response.reason = "OK"
    mock_response.headers = {"Content-Type": "text/html"}
    mock_response.text = mocker.AsyncMock(return_value="<html>Hello World!</html>")
    mock_response.__aenter__ = mocker.AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = mocker.AsyncMock(return_value=None)
    
    # Mock session
    mock_session = mocker.MagicMock()
    mock_session.get = mocker.MagicMock(return_value=mock_response)
    mock_session.__aenter__ = mocker.AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = mocker.AsyncMock(return_value=None)
    
    # Patch ClientSession
    mocker.patch("aiohttp.ClientSession", return_value=mock_session)
    
    # Run main
    await sender.main()
    
    # Verify session.get was called with correct URL
    mock_session.get.assert_called_once()
    call_args = mock_session.get.call_args
    assert sender.RELAY_URL in str(call_args)


def test_sender_configuration():
    """Test that sender configuration matches PRD requirements."""
    import sender
    
    # Verify configuration uses expected URL format
    assert sender.RELAY_URL.startswith("https://")
    assert ".servicebus.windows.net/" in sender.RELAY_URL
    
    # Verify no authentication headers are required (anonymous sender mode)
    # This is demonstrated by the simple aiohttp.ClientSession().get() call
    import inspect
    source = inspect.getsource(sender.main)
    assert "session.get" in source
    assert "ServiceBusAuthorization" not in source  # No auth header required
