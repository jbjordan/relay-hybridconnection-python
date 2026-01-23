#!/usr/bin/env python3
"""
Hello World Listener Sample

This sample demonstrates how to use the HybridConnectionListener to receive
HTTP requests through Azure Relay Hybrid Connections.

The listener serves an HTML response with a "Hello World" message and information
about the Python implementation created using the ralph loop.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add the src directory to the path so we can import hybrid_connection
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hybrid_connection import HybridConnectionListener


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


CONNECTION_STRING = get_relay_connection_string()

HTML_RESPONSE = """<!DOCTYPE html>
<html>
<head>
    <title>Hybrid Connection Demo</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 50px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #0078d4;
        }
        p {
            font-size: 18px;
            line-height: 1.6;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Hello World!</h1>
        <p>This python implementation of the Hybrid Connection Protocol was created using the ralph loop.</p>
    </div>
</body>
</html>
"""


async def main():
    """Main entry point for the Hello World listener."""
    listener = HybridConnectionListener.from_connection_string(CONNECTION_STRING)
    
    # Set up event callbacks
    listener.on_connecting = lambda: print("Connecting to Azure Relay...")
    listener.on_online = lambda: print("Online - Listening for requests")
    listener.on_offline = lambda: print("Offline - Disconnected from relay")
    
    # Define the request handler
    def request_handler(context):
        """Handle incoming HTTP requests."""
        print(f"Received {context.request.http_method} request to {context.request.url}")
        
        # Set response properties
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/html; charset=utf-8"
        
        # Write the HTML response
        context.response.output_stream.write(HTML_RESPONSE.encode('utf-8'))
        
        # Note: In the real implementation, close() would be async
        # For now, we'll handle it as if it were sync for simplicity
        asyncio.create_task(context.response.close())
    
    listener.request_handler = request_handler
    
    # Open the listener
    await listener.open()
    print("Server listening. Press Ctrl+C to exit.")
    
    try:
        # Wait forever (until Ctrl+C)
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await listener.close()
        print("Listener closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        sys.exit(0)
