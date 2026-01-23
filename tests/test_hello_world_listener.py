"""
Tests for the Hello World listener sample.

These tests verify that the sample application is correctly structured
and can be imported without errors.
"""

import pytest
import sys
import os

# Add samples directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'samples'))


def test_hello_world_listener_imports():
    """Test that the hello_world_listener module can be imported."""
    try:
        import hello_world_listener
        assert hello_world_listener is not None
    except ImportError as e:
        pytest.fail(f"Failed to import hello_world_listener: {e}")


def test_connection_string_defined():
    """Test that CONNECTION_STRING is defined in the module."""
    import hello_world_listener
    assert hasattr(hello_world_listener, 'CONNECTION_STRING')
    assert hello_world_listener.CONNECTION_STRING.startswith('Endpoint=sb://')


def test_html_response_defined():
    """Test that HTML_RESPONSE is defined and contains expected content."""
    import hello_world_listener
    assert hasattr(hello_world_listener, 'HTML_RESPONSE')
    assert 'Hello World!' in hello_world_listener.HTML_RESPONSE
    assert 'ralph loop' in hello_world_listener.HTML_RESPONSE


def test_html_response_is_valid_html():
    """Test that the HTML response is valid HTML."""
    import hello_world_listener
    html = hello_world_listener.HTML_RESPONSE
    assert '<!DOCTYPE html>' in html
    assert '<html>' in html
    assert '</html>' in html
    assert '<body>' in html
    assert '</body>' in html


def test_main_function_exists():
    """Test that the main() function is defined."""
    import hello_world_listener
    assert hasattr(hello_world_listener, 'main')
    assert callable(hello_world_listener.main)


@pytest.mark.asyncio
async def test_listener_creation():
    """Test that a listener can be created from the connection string."""
    from hybrid_connection import HybridConnectionListener
    import hello_world_listener
    
    # This should not raise an error
    listener = HybridConnectionListener.from_connection_string(
        hello_world_listener.CONNECTION_STRING
    )
    assert listener is not None
    assert not listener.is_online
