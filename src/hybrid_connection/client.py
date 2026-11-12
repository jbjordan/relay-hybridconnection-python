"""
HybridConnectionClient: sender-side counterpart to ``HybridConnectionListener``.

A ``HybridConnectionClient`` connects to the Azure Relay service over a
WebSocket using ``sb-hc-action=connect``. The service then performs the
rendezvous handshake with one of the active listeners; once accepted, the
WebSocket is joined end-to-end between sender and listener.
"""

from __future__ import annotations

from typing import Dict, Optional, Mapping
import asyncio
import logging
from uuid import uuid4

import websockets

from .connection_string import RelayConnectionStringBuilder
from .protocol import ProtocolHandler, validate_headers
from .stream import HybridConnectionStream
from .token_provider import TokenProvider, acquire_token
from .options import TransportOptions
from .errors import relay_error
from .runtime import get_runtime_information

logger = logging.getLogger(__name__)


class HybridConnectionClient:
    """Sender-side client that opens rendezvous WebSocket connections.

    Use ``create_connection`` to open a duplex WebSocket bridged to one of the
    listeners currently registered for the Hybrid Connection.
    """

    def __init__(
        self,
        address: str,
        token_provider: Optional[TokenProvider] = None,
        *,
        transport: Optional[TransportOptions] = None,
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
        self.transport = transport or TransportOptions()

    @classmethod
    def from_connection_string(cls, connection_string: str, *, transport=None, credential=None) -> "HybridConnectionClient":
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

        token_provider = builder.create_token_provider(credential)
        if transport is None and builder.operation_timeout is not None:
            transport = TransportOptions(open_timeout=builder.operation_timeout,
                                         write_timeout=builder.operation_timeout)

        address = builder.build_uri()
        return cls(address, token_provider, transport=transport)

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
        subprotocols: Optional[list[str]] = None,
    ) -> HybridConnectionStream:
        """Open a rendezvous WebSocket to a listener.

        Args:
            request_headers: Optional HTTP headers to send with the
                WebSocket upgrade. These are surfaced to the listener via
                the ``connectHeaders`` field of the accept message.
            hc_id: Optional client-supplied UUID for end-to-end diagnostics.
                Use a fresh UUID string for each connection, or omit it to
                let the SDK generate a fresh UUID.

        Returns:
            A ``HybridConnectionStream`` whose underlying WebSocket is
            connected end-to-end to a listener that accepted the
            connection.
        """
        hc_id = hc_id or str(uuid4())
        additional_headers: Optional[Dict[str, str]] = (
            dict(request_headers) if request_headers else None
        )
        if additional_headers:
            validate_headers(additional_headers)
            for name in list(additional_headers):
                if name.lower() == "sec-websocket-protocol":
                    if subprotocols is not None:
                        raise ValueError("Specify subprotocols only once")
                    subprotocols = [p.strip() for p in additional_headers.pop(name).split(",")]
        logger.info("Connecting sender", extra={"tracking_id": hc_id})
        try:
            async with asyncio.timeout(self.transport.open_timeout):
                token = (
                    await acquire_token(self._token_provider, self._address)
                    if self._token_provider is not None else None
                )
                url = self._build_connect_url(hc_id=hc_id, token=token.token if token else None)
                websocket = await self.transport.connect(
                    url, default_factory=websockets.connect,
                    additional_headers=additional_headers, subprotocols=subprotocols,
                )
        except Exception as exc:
            error = relay_error(exc, tracking_id=hc_id)
            logger.warning("Sender connection failed: %s", error, extra={"tracking_id": hc_id})
            if error is exc:
                raise
            raise error from exc
        logger.info("Sender connected", extra={"tracking_id": hc_id})
        return HybridConnectionStream(
            websocket,
            tracking_id=hc_id,
            connect_headers=additional_headers,
            address=self._address,
            transport=self.transport,
        )

    async def get_runtime_information(self):
        return await get_runtime_information(self._address, self._token_provider, self.transport)

    def _build_connect_url(self, *, hc_id: Optional[str] = None, token: Optional[str] = None) -> str:
        namespace, path = _split_address(self._address)
        if token is None and self._token_provider is not None:
            token = self._token_provider.get_token(self._address).token
        return ProtocolHandler.build_sender_connect_url(
            namespace=namespace, path=path, token=token, hc_id=hc_id
        )


def _split_address(address: str) -> tuple[str, str]:
    """Return (namespace, path) for a relay address."""
    return ProtocolHandler.split_relay_address(address)
