#!/usr/bin/env python3
"""
WebSocket rendezvous sender sample.

This sample opens a full-duplex WebSocket against an Azure Relay Hybrid
Connection using ``HybridConnectionClient``. It sends a few messages,
prints what the listener echoes back, and exits.

Pair it with ``websocket_listener.py`` for an end-to-end demo.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add the src directory to the path so we can import hybrid_connection.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hybrid_connection import HybridConnectionClient  # noqa: E402


def get_relay_connection_string() -> str:
    conn_str = os.environ.get("relay-python")
    if not conn_str:
        raise ValueError(
            "Environment variable 'relay-python' is not set. "
            "Please set it to your Azure Relay connection string."
        )
    return conn_str


async def main() -> None:
    client = HybridConnectionClient.from_connection_string(
        get_relay_connection_string()
    )
    print(f"Connecting to {client.address} ...")

    stream = await client.create_connection(
        request_headers={"X-MyApp": "websocket-sender-sample"},
        hc_id="sender-1",
    )
    print(f"Connected (id={stream.tracking_id})")

    try:
        for i in range(3):
            text = f"hello {i}"
            await stream.send_text(text)
            payload, kind = await asyncio.wait_for(stream.receive(), timeout=10)
            print(f"-> {text!r}    <- ({kind}) {payload!r}")

        await stream.send_bytes(b"\x00\x01\x02\x03")
        payload, kind = await asyncio.wait_for(stream.receive(), timeout=10)
        print(f"-> b'\\x00\\x01\\x02\\x03'    <- ({kind}) {payload!r}")
    finally:
        await stream.close()
        print("Sender closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
