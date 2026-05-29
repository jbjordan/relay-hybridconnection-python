"""
Unit tests for rendezvous URL building and accept message parsing.

These tests mirror the protocol-level expectations from the .NET unit tests
(Microsoft.Azure.Relay.UnitTests), specifically the URL handshake and the
accept/request envelope handling described in the Hybrid Connections protocol.
"""

import json

import pytest

from src.hybrid_connection.protocol import (
    CONTROL_CHANNEL_MAX_BODY_SIZE,
    MESSAGE_KIND_ACCEPT,
    MESSAGE_KIND_REQUEST,
    MESSAGE_KIND_REQUEST_POINTER,
    MESSAGE_KIND_RENEW_TOKEN,
    MESSAGE_KIND_UNKNOWN,
    ProtocolHandler,
)


class TestBuildRendezvousAcceptUrl:
    def test_accepts_ws_url_unchanged(self):
        address = "wss://ns.servicebus.windows.net:443/$hc/path?sb-hc-action=accept&sb-hc-id=abc"
        assert ProtocolHandler.build_rendezvous_accept_url(address) == address

    def test_rejects_empty_address(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_accept_url("")

    def test_rejects_non_ws_scheme(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_accept_url(
                "https://ns.servicebus.windows.net/path"
            )


class TestBuildRendezvousRejectUrl:
    def test_appends_status_code_and_description(self):
        address = "wss://ns.servicebus.windows.net/$hc/path?sb-hc-action=accept"
        url = ProtocolHandler.build_rendezvous_reject_url(
            address=address,
            status_code=401,
            status_description="Unauthorized",
        )

        assert "sb-hc-statusCode=401" in url
        assert "sb-hc-statusDescription=Unauthorized" in url
        # Original parameters preserved
        assert "sb-hc-action=accept" in url

    def test_url_encodes_status_description(self):
        address = "wss://ns/$hc/p?sb-hc-action=accept"
        url = ProtocolHandler.build_rendezvous_reject_url(
            address=address,
            status_code=400,
            status_description="Bad Request With Spaces",
        )
        # Spaces become + or %20 via urlencode
        assert "Bad+Request+With+Spaces" in url or "Bad%20Request%20With%20Spaces" in url

    def test_rejects_empty_address(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_reject_url("", 400, "Bad")

    def test_rejects_invalid_status_code(self):
        address = "wss://ns/$hc/p?sb-hc-action=accept"
        for invalid in [0, -1, "400", None]:
            with pytest.raises((ValueError, TypeError)):
                ProtocolHandler.build_rendezvous_reject_url(address, invalid, "Bad")

    def test_rejects_non_ws_scheme(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_reject_url(
                "https://ns/p", 400, "Bad"
            )


class TestBuildSenderConnectUrl:
    def test_anonymous_sender_url(self):
        url = ProtocolHandler.build_sender_connect_url(
            namespace="contoso.servicebus.windows.net",
            path="hc1",
        )
        assert url.startswith("wss://contoso.servicebus.windows.net/$hc/hc1?")
        assert "sb-hc-action=connect" in url
        assert "sb-hc-token" not in url

    def test_authenticated_sender_url_encodes_token(self):
        token = "SharedAccessSignature sr=http%3a%2f%2fns%2fp&sig=ABC%3D&se=1000&skn=root"
        url = ProtocolHandler.build_sender_connect_url(
            namespace="contoso.servicebus.windows.net",
            path="hc1",
            token=token,
        )
        assert "sb-hc-action=connect" in url
        assert "sb-hc-token=" in url
        # Token value MUST be URL-encoded so the space after
        # "SharedAccessSignature" is encoded as %20 (not left raw).
        assert "SharedAccessSignature sr=" not in url
        assert "SharedAccessSignature%20sr" in url

    def test_sender_url_includes_hc_id(self):
        url = ProtocolHandler.build_sender_connect_url(
            namespace="contoso.servicebus.windows.net",
            path="hc1",
            hc_id="diag-1234",
        )
        assert "sb-hc-id=diag-1234" in url

    def test_rejects_empty_namespace_or_path(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_sender_connect_url("", "p")
        with pytest.raises(ValueError):
            ProtocolHandler.build_sender_connect_url("ns", "")


class TestBuildRendezvousRequestUrl:
    def test_passes_through_valid_url(self):
        url = "wss://ns/$hc/p?sb-hc-action=request&sb-hc-id=abc"
        assert ProtocolHandler.build_rendezvous_request_url(url) == url

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_request_url("")

    def test_rejects_non_ws(self):
        with pytest.raises(ValueError):
            ProtocolHandler.build_rendezvous_request_url("https://ns/p")


class TestParseAcceptMessageRealFormat:
    """The wire format the Azure Relay service actually uses is
    ``{"accept": {...}}``; ensure we parse it.
    """

    def test_parses_full_accept_message(self):
        message = json.dumps({
            "accept": {
                "address": "wss://dc-node.servicebus.windows.net:443/$hc/hc1?sb-hc-action=accept&sb-hc-id=42",
                "id": "4cb542c3-047a-4d40-a19f-bdc66441e736",
                "connectHeaders": {
                    "Host": "dc-node.servicebus.windows.net:443",
                    "Sec-WebSocket-Protocol": "myproto",
                },
            }
        })

        result = ProtocolHandler.parse_accept_message(message)
        assert result["id"] == "4cb542c3-047a-4d40-a19f-bdc66441e736"
        assert result["address"].startswith("wss://")
        assert result["connect_headers"]["Sec-WebSocket-Protocol"] == "myproto"

    def test_legacy_type_accept_still_parses(self):
        message = json.dumps({"type": "accept"})
        result = ProtocolHandler.parse_accept_message(message)
        assert result["type"] == "accept"

    def test_rejects_invalid_json(self):
        with pytest.raises(ValueError):
            ProtocolHandler.parse_accept_message("not-json{")

    def test_rejects_non_object_payload(self):
        with pytest.raises(ValueError):
            ProtocolHandler.parse_accept_message(json.dumps([1, 2, 3]))


class TestParseRequestMessageRendezvousPointer:
    def test_rendezvous_pointer_request_parses(self):
        message = json.dumps({
            "request": {
                "address": "wss://ns/$hc/p?sb-hc-action=request&sb-hc-id=abc"
            }
        })
        parsed = ProtocolHandler.parse_request_message(message)
        assert "address" in parsed
        assert ProtocolHandler.is_rendezvous_pointer_request(parsed)

    def test_full_request_is_not_pointer(self):
        parsed = {"id": "1", "method": "GET", "requestTarget": "/"}
        assert not ProtocolHandler.is_rendezvous_pointer_request(parsed)

    def test_pointer_only_request_is_pointer(self):
        parsed = {"address": "wss://..."}
        assert ProtocolHandler.is_rendezvous_pointer_request(parsed)

    def test_request_missing_fields_and_address_raises(self):
        message = json.dumps({"request": {"id": "1"}})  # missing method/requestTarget and address
        with pytest.raises(ValueError):
            ProtocolHandler.parse_request_message(message)


class TestParseControlMessage:
    def test_parses_accept_message(self):
        message = json.dumps({
            "accept": {
                "address": "wss://ns/$hc/p?sb-hc-action=accept",
                "id": "id-1",
                "connectHeaders": {"Host": "ns"},
            }
        })
        result = ProtocolHandler.parse_control_message(message)
        assert result["kind"] == MESSAGE_KIND_ACCEPT
        assert result["address"].startswith("wss://")
        assert result["id"] == "id-1"
        assert result["connect_headers"] == {"Host": "ns"}

    def test_parses_full_request(self):
        message = json.dumps({
            "request": {
                "id": "req-1",
                "method": "GET",
                "requestTarget": "/api",
                "requestHeaders": {"User-Agent": "Test"},
                "body": False,
                "address": "wss://ns/$hc/p?sb-hc-action=request",
            }
        })
        result = ProtocolHandler.parse_control_message(message)
        assert result["kind"] == MESSAGE_KIND_REQUEST
        assert result["id"] == "req-1"
        assert result["method"] == "GET"
        assert result["address"].startswith("wss://")
        assert result["body"] is False

    def test_parses_request_pointer(self):
        message = json.dumps({
            "request": {"address": "wss://ns/$hc/p?sb-hc-action=request"}
        })
        result = ProtocolHandler.parse_control_message(message)
        assert result["kind"] == MESSAGE_KIND_REQUEST_POINTER
        assert result["address"].startswith("wss://")

    def test_parses_renew_token(self):
        message = json.dumps({"renewToken": {"token": "SharedAccessSignature ..."}})
        result = ProtocolHandler.parse_control_message(message)
        assert result["kind"] == MESSAGE_KIND_RENEW_TOKEN
        assert result["token"].startswith("SharedAccessSignature")

    def test_unknown_envelope(self):
        message = json.dumps({"someOtherEnvelope": {"x": 1}})
        result = ProtocolHandler.parse_control_message(message)
        assert result["kind"] == MESSAGE_KIND_UNKNOWN

    def test_invalid_json(self):
        with pytest.raises(ValueError):
            ProtocolHandler.parse_control_message("{not json")


class TestControlChannelLimit:
    def test_limit_constant_is_64k(self):
        assert CONTROL_CHANNEL_MAX_BODY_SIZE == 64 * 1024


class TestRenewTokenRoundTrip:
    """``build_renew_token_message`` must produce a JSON payload that
    ``parse_control_message`` correctly recognises as a renew_token message."""

    def test_build_then_parse_round_trip(self):
        token = "SharedAccessSignature sr=http%3a%2f%2fns%2fp&sig=ABC%3D&se=1000&skn=root"
        wire = ProtocolHandler.build_renew_token_message(token)

        # Wire shape must be {"renewToken": {"token": ...}}
        envelope = json.loads(wire)
        assert "renewToken" in envelope
        assert envelope["renewToken"]["token"] == token

        # parse_control_message must classify it correctly.
        parsed = ProtocolHandler.parse_control_message(wire)
        assert parsed["kind"] == MESSAGE_KIND_RENEW_TOKEN
        assert parsed["token"] == token
