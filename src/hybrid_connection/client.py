"""
HybridConnectionClient: sender-side counterpart to ``HybridConnectionListener``.

A ``HybridConnectionClient`` connects to the Azure Relay service over a
WebSocket using ``sb-hc-action=connect``. The service then performs the
rendezvous handshake with one of the active listeners; once accepted, the
WebSocket is joined end-to-end between sender and listener.
"""

from __future__ import annotations

from typing import Dict, Optional, Mapping

import websockets

from .connection_string import RelayConnectionStringBuilder
from .protocol import ProtocolHandler
from .stream import HybridConnectionStream
from .token_provider import TokenProvider


class HybridConnectionClient:
    """Sender-side client that opens rendezvous WebSocket connections.

    Use ``create_connection`` to open a duplex WebSocket bridged to one of the
    listeners currently registered for the Hybrid Connection.
    """

    def __init__(
        self,
        address: str,
        token_provider: Optional[TokenProvider] = None,
    ) -> None:
        """Construct a new client.

        Args:
            address: The Hybrid Connection address, of the form
                ``sb://namespace.servicebus.windows.net/path``.
            token_provider: Optional ``TokenProvider`` providing the
                ``Send`` permission. Required unless the Hybrid Connection
                is configured to allow anonymous senders.
        """
        if not address:
            raise ValueError("address cannot be empty")
        self._address = address
        self._token_provider = token_provider

    @classmethod
    def from_connection_string(cls, connection_string: str) -> "HybridConnectionClient":
        """Create a client from an Azure Relay connection string.

        The connection string must include ``EntityPath`` and (if anonymous
        sending is not enabled) the ``SharedAccessKeyName`` and
        ``SharedAccessKey`` pair.
        """
        if not connection_string:
            raise ValueError("connection_string cannot be empty")

        builder = RelayConnectionStringBuilder(connection_string)

        if not builder.endpoint:
            raise ValueError("Connection string missing Endpoint")
        if not builder.entity_path:
            raise ValueError("Connection string missing EntityPath")

        token_provider: Optional[TokenProvider] = None
        if builder.shared_access_key_name and builder.shared_access_key:
            token_provider = TokenProvider(
                key_name=builder.shared_access_key_name,
                shared_access_key=builder.shared_access_key,
            )

        address = builder.build_uri()
        return cls(address, token_provider)

    @property
    def address(self) -> str:
        return self._address

    @property
    def token_provider(self) -> Optional[TokenProvider]:
        return self._token_provider

    async def create_connection(
        self,
        request_headers: Optional[Mapping[str, str]] = None,
        *,
        hc_id: Optional[str] = None,
    ) -> HybridConnectionStream:
        """Open a rendezvous WebSocket to a listener.

        Args:
            request_headers: Optional HTTP headers to send with the
                WebSocket upgrade. These are surfaced to the listener via
                the ``connectHeaders`` field of the accept message.
            hc_id: Optional client-supplied id for end-to-end diagnostics.

        Returns:
            A ``HybridConnectionStream`` whose underlying WebSocket is
            connected end-to-end to a listener that accepted the
            connection.
        """
        url = self._build_connect_url(hc_id=hc_id)
        additional_headers: Optional[Dict[str, str]] = (
            dict(request_headers) if request_headers else None
        )
        websocket = await websockets.connect(
            url,
            additional_headers=additional_headers,
        )
        return HybridConnectionStream(
            websocket,
            tracking_id=hc_id,
            connect_headers=additional_headers,
            address=url,
        )

    def _build_connect_url(self, *, hc_id: Optional[str] = None) -> str:
        namespace, path = _split_address(self._address)
        token: Optional[str] = None
        if self._token_provider is not None:
            token = self._token_provider.get_token(self._address).token
        return ProtocolHandler.build_sender_connect_url(
            namespace=namespace, path=path, token=token, hc_id=hc_id
        )


def _split_address(address: str) -> tuple[str, str]:
    """Return (namespace, path) for a relay address."""
    stripped = address.replace("sb://", "").replace("https://", "")
    parts = stripped.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid relay address: {address!r}")
    return parts[0], parts[1]
