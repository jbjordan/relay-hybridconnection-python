# Azure Relay Hybrid Connection - Python Implementation

A Python implementation of the Azure Relay Hybrid Connection Protocol using the HTTP request/response pattern. This library enables Python applications to act as listeners behind NATs or firewalls, receiving HTTP requests routed through Azure Relay.

## Features

- **HTTP Request/Response Pattern**: Receive HTTP requests through a WebSocket control channel
- **Automatic Reconnection**: Handles disconnections with exponential backoff
- **Token Management**: Automatic SAS token renewal before expiration
- **Keepalive/Ping**: Prevents NAT timeout with periodic ping messages
- **Event Callbacks**: Monitor connection status with `on_connecting`, `on_online`, and `on_offline` events
- **Async/Await Support**: Built on Python's asyncio for efficient concurrent operations

## Installation

1. Clone or download this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

### Dependencies

- `websockets>=12.0` - WebSocket client for control channel
- `aiohttp>=3.9.0` - HTTP client for sender
- `pytest>=8.0.0` - Testing framework
- `pytest-asyncio>=0.23.0` - Async test support
- `pytest-mock>=3.12.0` - Mocking support for tests

## Quick Start

### Basic Listener Example

```python
import asyncio
from hybrid_connection import HybridConnectionListener

# Connection string from Azure Portal
CONNECTION_STRING = "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyname>;SharedAccessKey=<key>;EntityPath=<path>"

async def main():
    # Create listener from connection string
    listener = HybridConnectionListener.from_connection_string(CONNECTION_STRING)
    
    # Set up event callbacks (optional)
    listener.on_connecting = lambda: print("Connecting...")
    listener.on_online = lambda: print("Online - Ready to receive requests")
    listener.on_offline = lambda: print("Offline")
    
    # Define request handler
    def request_handler(context):
        print(f"Received {context.request.http_method} request to {context.request.url}")
        
        # Set response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/plain"
        
        # Write response body
        context.response.output_stream.write(b"Hello from Python!")
        
        # Close response (sends to client)
        asyncio.create_task(context.response.close())
    
    listener.request_handler = request_handler
    
    # Open listener and wait
    await listener.open()
    print("Listener is online. Press Ctrl+C to exit.")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await listener.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### HybridConnectionListener

Main class for receiving HTTP requests through Azure Relay.

#### Constructor

```python
# Option 1: From connection string (recommended)
listener = HybridConnectionListener.from_connection_string(connection_string)

# Option 2: From components
listener = HybridConnectionListener(
    address="sb://<namespace>.servicebus.windows.net/<path>",
    token_provider=TokenProvider.create_shared_access_signature_token_provider(
        key_name="<keyname>",
        key="<key>"
    )
)
```

#### Properties

- **`is_online`** (bool): Returns `True` if listener is connected to relay
- **`request_handler`** (Callable): Handler function for incoming requests

#### Event Callbacks

- **`on_connecting`** (Callable): Called when attempting to connect
- **`on_online`** (Callable): Called when successfully connected
- **`on_offline`** (Callable): Called when disconnected

#### Methods

- **`async open()`**: Opens the listener and establishes control channel
- **`async close()`**: Closes the listener gracefully

### RelayedHttpListenerContext

Represents an HTTP request/response pair.

#### Properties

- **`request`** (RelayedHttpListenerRequest): The incoming HTTP request
- **`response`** (RelayedHttpListenerResponse): The outgoing HTTP response

### RelayedHttpListenerRequest

Represents an incoming HTTP request.

#### Properties

- **`http_method`** (str): HTTP method (GET, POST, PUT, DELETE, etc.)
- **`url`** (str): Request URL/path
- **`headers`** (dict): HTTP request headers
- **`input_stream`** (BinaryIO): Request body stream
- **`has_entity_body`** (bool): Whether request has a body
- **`content_length`** (int): Length of request body
- **`content_type`** (str): Content-Type header value

#### Methods

- **`get_header(name)`**: Get header value (case-insensitive)
- **`read_body()`**: Read entire request body as bytes

### RelayedHttpListenerResponse

Represents an outgoing HTTP response.

#### Properties

- **`status_code`** (int): HTTP status code (e.g., 200, 404)
- **`status_description`** (str): Status reason phrase (e.g., "OK")
- **`headers`** (dict): HTTP response headers
- **`output_stream`** (BinaryIO): Response body stream

#### Methods

- **`async close()`**: Finalizes and sends the response

### TokenProvider

Generates SAS tokens for authentication.

```python
token_provider = TokenProvider.create_shared_access_signature_token_provider(
    key_name="RootKey",
    key="<your-key>"
)

# Get token for specific audience
token = await token_provider.get_token(
    audience="sb://namespace.servicebus.windows.net/path",
    validity=timedelta(hours=1)
)
```

## Running the Samples

### Hello World Listener

Starts a listener that serves an HTML "Hello World" page:

```bash
python samples/hello_world_listener.py
```

The listener will connect to Azure Relay and wait for requests. Leave it running.

### Sender

Sends an HTTP GET request to the listener via Azure Relay:

```bash
# In a separate terminal
python samples/sender.py
```

You should see:
- The sender displays the HTML response
- The listener logs the incoming request

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test Categories

```bash
# Unit tests only (fast, no network required)
pytest tests/ -k "not integration"

# Integration tests (requires Azure Relay connection)
pytest tests/test_integration.py

# Skip integration tests
SKIP_INTEGRATION=1 pytest
```

### Run Tests with Coverage

```bash
pytest --cov=hybrid_connection --cov-report=html
```

### Run Tests with Verbose Output

```bash
pytest -v
```

## Project Structure

```
hybrid-connection-python/
├── src/
│   └── hybrid_connection/
│       ├── __init__.py              # Public API exports
│       ├── listener.py              # HybridConnectionListener class
│       ├── token_provider.py        # SAS token generation
│       ├── context.py               # Request/Response classes
│       ├── connection_string.py     # Connection string parser
│       └── protocol.py              # WebSocket protocol handling
├── tests/
│   ├── conftest.py                  # Pytest fixtures
│   ├── test_connection_string.py    # Connection string tests
│   ├── test_token_provider.py       # Token provider tests
│   ├── test_context.py              # Request/Response tests
│   ├── test_protocol.py             # Protocol handling tests
│   ├── test_listener.py             # Listener unit tests
│   ├── test_token_renewal.py        # Token renewal tests
│   ├── test_reconnection.py         # Reconnection tests
│   ├── test_ping.py                 # Keepalive tests
│   ├── test_integration.py          # Live integration tests
│   ├── test_hello_world_listener.py # Sample tests
│   └── test_sender.py               # Sender tests
├── samples/
│   ├── hello_world_listener.py      # Hello World listener demo
│   └── sender.py                    # HTTP sender demo
├── requirements.txt                 # Python dependencies
├── pytest.ini                       # Pytest configuration
└── README.md                        # This file
```

## Protocol Details

This implementation follows the Azure Relay Hybrid Connections Protocol for HTTP request/response mode:

1. **Control Channel**: Establishes a WebSocket connection to `wss://<namespace>/$hc/<path>?sb-hc-action=listen&sb-hc-token=<token>`

2. **Request Reception**: Receives JSON `request` messages with HTTP method, URL, headers, and optional binary body frames

3. **Response Transmission**: Sends JSON `response` messages with status code, headers, and optional binary body frames

4. **Token Renewal**: Automatically renews SAS tokens before expiration by sending `renewToken` messages

5. **Reconnection**: Automatically reconnects with exponential backoff on disconnection

6. **Keepalive**: Sends periodic WebSocket ping messages to prevent NAT timeout

### Limitations

This implementation supports:
- ✅ HTTP request/response pattern over control channel
- ✅ Requests up to 64KB on control channel
- ✅ Automatic reconnection and token renewal
- ✅ Anonymous sender mode

This implementation does NOT support:
- ❌ WebSocket rendezvous pattern (full-duplex WebSocket)
- ❌ Requests > 64KB (rendezvous upgrade)
- ❌ Bidirectional streaming
- ❌ Multiple simultaneous listeners (load balancing)

## Configuration

### Azure Relay Setup

1. Create an Azure Relay namespace in the Azure Portal
2. Create a Hybrid Connection within the namespace
3. Configure the Hybrid Connection:
   - Enable "Requires Client Authorization" for authenticated senders
   - Disable for anonymous senders (simpler for testing)
4. Copy the connection string from the Hybrid Connection's "Shared access policies"

### Connection String Format

```
Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyname>;SharedAccessKey=<key>;EntityPath=<hc-name>
```

Components:
- **Endpoint**: Azure Service Bus namespace URL
- **SharedAccessKeyName**: Name of the shared access key
- **SharedAccessKey**: The access key for authentication
- **EntityPath**: Name of the Hybrid Connection

## Troubleshooting

### Connection Fails

- Verify the connection string is correct
- Check that the Hybrid Connection exists in Azure Portal
- Ensure the SharedAccessKey has "Listen" permissions
- Check network connectivity to Azure

### Token Errors

- Verify SharedAccessKeyName and SharedAccessKey are correct
- Check that the key has not been regenerated in Azure Portal
- Ensure clock synchronization (token expiration is time-based)

### No Requests Received

- Ensure listener is online (check `on_online` callback)
- Verify sender is using the correct relay URL
- Check if "Requires Client Authorization" is enabled (sender needs SAS token)
- Test with the provided `sender.py` sample

## References

- [Azure Relay Hybrid Connections Protocol](https://learn.microsoft.com/en-us/azure/azure-relay/relay-hybrid-connections-protocol)
- [Hybrid Connections - HTTP requests in .NET](https://learn.microsoft.com/en-us/azure/azure-relay/relay-hybrid-connections-http-requests-dotnet-get-started)
- [Azure Relay Documentation](https://learn.microsoft.com/en-us/azure/azure-relay/)

## License

This implementation was created using the ralph loop for demonstration and testing purposes.
