#!/usr/bin/env python3
"""
WebSocket rendezvous listener sample.

This sample shows how to use ``HybridConnectionListener.accept_connection``
to accept full-duplex WebSocket connections from a sender via the Azure
Relay Hybrid Connections rendezvous protocol. The listener echoes every
message it receives back to the sender.

Pair it with ``websocket_sender.py`` (in this directory) to see a complete
end-to-end exchange.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add the src directory to the path so we can import hybrid_connection.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hybrid_connection import HybridConnectionListener  # noqa: E402


def get_relay_connection_string() -> str:
    conn_str = os.environ.get("relay-python")
    if not conn_str:
        raise ValueError(
            "Environment variable 'relay-python' is not set. "
            "Please set it to your Azure Relay connection string."
        )
    return conn_str


def accept_handler(context) -> bool:
    """Optional accept handler. Inspect headers; reject unwanted senders."""
    headers = context.request.headers
    print(f"Incoming WebSocket from {headers.get('Host', '?')} (id={context.tracking_id})")
    # Example: reject senders that don't supply a custom token header.
    # if headers.get("X-MyApp-Token") != "expected":
    #     context.response.status_code = 401
    #     context.response.status_description = "Missing X-MyApp-Token"
    #     return False
    return True


async def echo(stream) -> None:
    """Echo every message the sender sends back to it, until they close."""
    print(f"Connection {stream.tracking_id} established")
    try:
        async for payload, kind in stream:
            label = "text" if kind == "text" else "binary"
            preview = payload[:60] if isinstance(payload, (bytes, str)) else payload
            print(f"<- ({label}) {preview!r}")
            await stream.send(payload)
    except Exception as e:
        print(f"Connection {stream.tracking_id} error: {e}")
    finally:
        await stream.close()
        print(f"Connection {stream.tracking_id} closed")


async def main() -> None:
    listener = HybridConnectionListener.from_connection_string(
        get_relay_connection_string()
    )
    listener.on_connecting = lambda: print("Connecting to Azure Relay...")
    listener.on_online = lambda: print("Listener online; awaiting WebSocket senders")
    listener.on_offline = lambda: print("Listener offline")
    listener.accept_handler = accept_handler

    await listener.open()
    try:
        async for stream in listener.connections():
            # Run each session in its own task so we can accept new ones.
            asyncio.create_task(echo(stream))
    except KeyboardInterrupt:
        pass
    finally:
        await listener.close()
        print("Listener closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
