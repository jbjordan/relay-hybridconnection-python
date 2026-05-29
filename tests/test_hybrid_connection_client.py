"""Unit tests for HybridConnectionClient (sender side)."""

import secrets
import string
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.hybrid_connection.client import HybridConnectionClient
from src.hybrid_connection.token_provider import TokenProvider


def _random_sas_key(length: int = 44) -> str:
    alphabet = string.ascii_letters + string.digits + "+/"
    return "".join(secrets.choice(alphabet) for _ in range(length - 1)) + "="


CONNECTION_STRING = (
    "Endpoint=sb://contoso.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    f"SharedAccessKey={_random_sas_key()};"
    "EntityPath=hc1"
)


class TestConstruction:
    def test_construct_with_address_only_anonymous(self):
        client = HybridConnectionClient("sb://contoso.servicebus.windows.net/hc1")
        assert client.address == "sb://contoso.servicebus.windows.net/hc1"
        assert client.token_provider is None

    def test_construct_with_token_provider(self):
        provider = TokenProvider("name", "key")
        client = HybridConnectionClient(
            "sb://contoso.servicebus.windows.net/hc1", provider
        )
        assert client.token_provider is provider

    def test_construct_rejects_empty_address(self):
        with pytest.raises(ValueError):
            HybridConnectionClient("")


class TestFromConnectionString:
    def test_authenticated_from_connection_string(self):
        client = HybridConnectionClient.from_connection_string(CONNECTION_STRING)
        assert "contoso.servicebus.windows.net/hc1" in client.address
        assert client.token_provider is not None

    def test_anonymous_from_connection_string(self):
        """Anonymous senders may omit SharedAccessKeyName/SharedAccessKey."""
        conn = "Endpoint=sb://contoso.servicebus.windows.net/;EntityPath=hc1"
        client = HybridConnectionClient.from_connection_string(conn)
        assert client.token_provider is None
        assert "contoso.servicebus.windows.net/hc1" in client.address

    def test_missing_entity_path_raises(self):
        conn = (
            "Endpoint=sb://contoso.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            f"SharedAccessKey={_random_sas_key()}"
        )
        with pytest.raises(ValueError):
            HybridConnectionClient.from_connection_string(conn)

    def test_empty_connection_string_raises(self):
        with pytest.raises(ValueError):
            HybridConnectionClient.from_connection_string("")


class TestCreateConnection:
    @pytest.mark.asyncio
    async def test_create_connection_anonymous_builds_correct_url(self):
        client = HybridConnectionClient("sb://contoso.servicebus.windows.net/hc1")

        fake_ws = MagicMock()
        fake_ws.send = AsyncMock()
        fake_ws.recv = AsyncMock()
        fake_ws.close = AsyncMock()

        with patch(
            "src.hybrid_connection.client.websockets.connect",
            new=AsyncMock(return_value=fake_ws),
        ) as connect:
            stream = await client.create_connection()

        connect.assert_awaited_once()
        url = connect.call_args.args[0]
        assert url.startswith("wss://contoso.servicebus.windows.net/$hc/hc1?")
        assert "sb-hc-action=connect" in url
        assert "sb-hc-token" not in url

        assert stream.websocket is fake_ws

    @pytest.mark.asyncio
    async def test_create_connection_authenticated_includes_token(self):
        provider = TokenProvider("RootManageSharedAccessKey", _random_sas_key())
        client = HybridConnectionClient(
            "sb://contoso.servicebus.windows.net/hc1", provider
        )

        fake_ws = MagicMock()
        with patch(
            "src.hybrid_connection.client.websockets.connect",
            new=AsyncMock(return_value=fake_ws),
        ) as connect:
            await client.create_connection()

        url = connect.call_args.args[0]
        assert "sb-hc-action=connect" in url
        assert "sb-hc-token=" in url

    @pytest.mark.asyncio
    async def test_create_connection_passes_additional_headers(self):
        client = HybridConnectionClient("sb://contoso.servicebus.windows.net/hc1")

        with patch(
            "src.hybrid_connection.client.websockets.connect",
            new=AsyncMock(return_value=MagicMock()),
        ) as connect:
            await client.create_connection(
                request_headers={"X-MyApp": "v1", "Sec-WebSocket-Protocol": "echo"}
            )

        kwargs = connect.call_args.kwargs
        assert kwargs["additional_headers"] == {
            "X-MyApp": "v1",
            "Sec-WebSocket-Protocol": "echo",
        }

    @pytest.mark.asyncio
    async def test_create_connection_includes_hc_id(self):
        client = HybridConnectionClient("sb://contoso.servicebus.windows.net/hc1")

        with patch(
            "src.hybrid_connection.client.websockets.connect",
            new=AsyncMock(return_value=MagicMock()),
        ) as connect:
            stream = await client.create_connection(hc_id="diag-42")

        url = connect.call_args.args[0]
        assert "sb-hc-id=diag-42" in url
        assert stream.tracking_id == "diag-42"
