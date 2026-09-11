# Azure Relay Hybrid Connection - Python Implementation

A Python implementation of the Azure Relay Hybrid Connection Protocol covering both the **HTTP request/response** pattern and the **WebSocket rendezvous** pattern. Python applications can act as listeners behind NATs or firewalls, accepting HTTP requests **and** full-duplex WebSocket connections relayed through Azure Relay.

## Features

- **HTTP request/response pattern**: Receive HTTP requests through a WebSocket control channel
- **WebSocket rendezvous pattern**: Accept full-duplex WebSocket connections from senders, with an optional accept handler for inspecting / rejecting senders
- **Large request/response handling**: Automatically upgrades large HTTP requests and responses (>64 kB) to a dedicated rendezvous WebSocket
- **IPv6-first rendezvous**: Prefers reachable IPv6 addresses for direct listener rendezvous connections, with bounded IPv4 fallback
- **Sender API (``HybridConnectionClient``)**: Open authenticated or anonymous WebSocket connections to a listener
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

- `websockets>=14.0` - WebSocket client (uses the modern asyncio API)
- `aiohappyeyeballs>=2.6.2` - IPv6-first TCP connection racing and socket cleanup
- `aiohttp>=3.9.0` - HTTP client for sender
- `pytest>=8.0.0` - Testing framework
- `pytest-asyncio>=0.23.0` - Async test support
- `pytest-mock>=3.12.0` - Mocking support for tests

## Quick Start

### Basic HTTP Listener Example

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
    async def request_handler(context):
        print(f"Received {context.request.http_method} request to {context.request.url}")

        # Set response
        context.response.status_code = 200
        context.response.status_description = "OK"
        context.response.headers["Content-Type"] = "text/plain"

        # Write response body
        context.response.output_stream.write(b"Hello from Python!")

        # Close response (sends to client)
        await context.response.close()
    
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

### WebSocket Rendezvous Listener Example

Listeners can also accept full-duplex WebSocket connections opened by a sender via the Hybrid Connections rendezvous protocol. Each accepted connection is surfaced as a `HybridConnectionStream` that supports text and binary messages in both directions.

```python
import asyncio
from hybrid_connection import HybridConnectionListener

async def echo(stream):
    async for payload, kind in stream:   # kind is "text" or "binary"
        await stream.send(payload)
    await stream.close()

def accept_handler(context):
    # Optionally inspect connectHeaders before accepting.
    if context.request.get_header("X-MyApp") != "ok":
        context.response.status_code = 401
        context.response.status_description = "Missing X-MyApp header"
        return False
    return True

async def main():
    listener = HybridConnectionListener.from_connection_string(CONNECTION_STRING)
    listener.accept_handler = accept_handler

    await listener.open()
    try:
        async for stream in listener.connections():
            asyncio.create_task(echo(stream))
    finally:
        await listener.close()

asyncio.run(main())
```

### WebSocket Rendezvous Sender Example

```python
import asyncio
from hybrid_connection import HybridConnectionClient

async def main():
    client = HybridConnectionClient.from_connection_string(CONNECTION_STRING)
    stream = await client.create_connection(
        request_headers={"X-MyApp": "ok"},
        hc_id="diagnostic-id-1",
    )
    try:
        await stream.send_text("hello")
        payload, kind = await stream.receive()
        print(f"<- ({kind}) {payload!r}")
    finally:
        await stream.close()

asyncio.run(main())
```

## API Reference

### HybridConnectionListener

Main class for receiving HTTP requests **and** WebSocket connections through Azure Relay.

#### Constructor

```python
# Option 1: From connection string (recommended)
listener = HybridConnectionListener.from_connection_string(connection_string)

# Option 2: From components
listener = HybridConnectionListener(
    address="sb://<namespace>.servicebus.windows.net/<path>",
    token_provider=TokenProvider(key_name="<keyname>", shared_access_key="<key>"),
)

# Optional: retain legacy transport address selection for rendezvous.
listener = HybridConnectionListener.from_connection_string(
    connection_string, prefer_ipv6=False
)
```

Both constructors accept the keyword-only `prefer_ipv6` option, which defaults
to `True`. Existing positional arguments are unchanged.

#### IPv6-first rendezvous

For direct connections, the listener tries IPv6 first even when the system
resolver lists IPv4 first. If that TCP attempt fails, the next address is tried
immediately; if it stalls, an IPv4 attempt starts after 250 ms when an IPv4
address is available. Further attempts alternate address families. The first
successful TCP connection performs one WebSocket handshake. DNS lookup, TCP
selection, TLS, and the handshake share a 10-second opening timeout. Missing
AAAA records, unsupported IPv6, and unreachable IPv6 routes can fall back to
IPv4. TLS certificate errors and WebSocket handshake failures (including the
expected HTTP 410 rejection response) do not trigger an IPv4 handshake retry.

This policy applies to listener WebSocket accepts and rejects, HTTP
request-pointer connections, and large-response rendezvous upgrades. Namespace
listen/control connections, small HTTP exchanges on that channel, and
`HybridConnectionClient` sender connections keep their existing behavior. The
sender connects to the namespace; it does not reconnect to a gateway instance.

The SDK uses the service-provided rendezvous hostname, path, query, and port;
it does not synthesize an IPv6 hostname or enablement parameter. WSS retains the
original hostname for TLS SNI and certificate validation. IPv6 requires both an
AAAA record for that hostname and a working IPv6 route to it. An SDK upgrade
cannot provision either prerequisite.

System and configured proxy routes retain the installed `websockets` behavior:
the SDK does not resolve the destination or open a direct socket before
delegating a proxied connection. Proxy-side DNS/address-family selection is
outside this preference, so IPv6 is not guaranteed through a proxy. Automatic
system/environment proxy support starts with `websockets` 15; 14 retains its
existing direct transport behavior. `prefer_ipv6=False` restores the installed
transport's address selection, rather than forcing IPv4.

To inspect the actual selected direct rendezvous TCP peer, enable just the
transport logger at DEBUG (configure a logging handler as appropriate):

```python
import logging

logging.getLogger("hybrid_connection._rendezvous").setLevel(logging.DEBUG)
```

It reports `IPv6` or `IPv4` and the peer address/port, not SAS tokens or the
rendezvous URL. A proxied connection instead reports that address selection was
delegated; the namespace control connection is not evidence of the rendezvous
address family.

#### Properties

- **`is_online`** (bool): Returns `True` if listener is connected to relay
- **`request_handler`** (Callable): Handler function for incoming HTTP requests
- **`accept_handler`** (Callable): Optional handler invoked before accepting a sender WebSocket rendezvous. Returning `False` rejects the connection; the listener will send back the `response.status_code` / `response.status_description` as the rejection (defaults to `400 Rejected by user code`).

#### Event Callbacks

- **`on_connecting`** (Callable): Called when attempting to connect
- **`on_online`** (Callable): Called when successfully connected
- **`on_offline`** (Callable): Called when disconnected

#### Methods

- **`async open()`**: Opens the listener and establishes the control channel; repeated calls do not create duplicate connections
- **`async close()`**: Closes the listener gracefully (cancels in-flight tasks and drains accepted streams)
- **`async accept_connection() -> HybridConnectionStream`**: Wait for and return the next accepted rendezvous WebSocket; raises `ConnectionError` when the listener is closed
- **`async for stream in listener.connections(): ...`**: Async iterator over accepted rendezvous WebSockets; ends when the listener is closed

If `listener.close()` is cancelled, await it again to finish shutdown before reopening the listener.

### HybridConnectionClient

Sender-side counterpart that opens rendezvous WebSocket connections to a listener.

```python
client = HybridConnectionClient.from_connection_string(CONNECTION_STRING)
# or anonymous:
client = HybridConnectionClient("sb://<namespace>.servicebus.windows.net/<path>")

stream = await client.create_connection(
    request_headers={"X-MyApp": "..."},
    hc_id="optional-diagnostic-id",
)
```

#### Methods

- **`from_connection_string(conn_str)`** (classmethod): Build a client from a connection string (no `SharedAccessKey*` ⇒ anonymous sender)
- **`async create_connection(request_headers=None, *, hc_id=None) -> HybridConnectionStream`**: Open a rendezvous WebSocket to a listener; `request_headers` are surfaced to the listener via the accept message's `connectHeaders`

### HybridConnectionStream

The duplex WebSocket produced by a successful rendezvous. Returned by both `HybridConnectionListener.accept_connection()` and `HybridConnectionClient.create_connection()`.

#### Properties

- **`tracking_id`** (str | None): The id supplied by the sender (or assigned by the service) for end-to-end diagnostics
- **`connect_headers`** (dict): On the listener side, the headers the sender supplied with the WebSocket upgrade
- **`address`** (str): The rendezvous URL the stream is connected to
- **`is_closed`** (bool): Whether the stream has been closed

#### Methods

- **`async send(data)`**: Send `str` as text frame or bytes-like as binary frame
- **`async send_text(text)`** / **`async send_bytes(data)`**: Strict variants
- **`async receive() -> (payload, "text" | "binary")`**
- **`async for payload, kind in stream: ...`**: Ends on normal closure; raises `ConnectionError` on abnormal disconnection instead of silently truncating the transfer
- **`async close(code=1000, reason="")`**: Close the stream; errors propagate, and failed or cancelled closes can be retried

### RelayedHttpListenerContext

Represents an HTTP request/response pair (HTTP listener) **or** an inspectable WebSocket upgrade (accept handler).

#### Properties

- **`request`** (RelayedHttpListenerRequest): The incoming request (or a synthetic request carrying `connectHeaders` for WebSocket upgrades)
- **`response`** (RelayedHttpListenerResponse): The outgoing response (or the reject status/description for WebSocket upgrades)
- **`tracking_id`** (str | None): End-to-end diagnostic id
- **`is_websocket_upgrade`** (bool): `True` when the context represents a pending WebSocket rendezvous being inspected by `accept_handler`
- **`rendezvous_address`** (str | None): Rendezvous URL associated with this exchange

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
- **`status_description`** (str): Status reason phrase (e.g., "OK"). Setting to `None` clears it.
- **`headers`** (dict): HTTP response headers
- **`output_stream`** (BinaryIO): Response body stream

#### Methods

- **`async close()`**: Finalizes and sends the response once. The listener also closes the response automatically when the request handler returns.

### TokenProvider

Generates SAS tokens for authentication.

```python
token_provider = TokenProvider(key_name="RootKey", shared_access_key="<your-key>")

# Get token for specific audience
token = token_provider.get_token(audience="sb://namespace.servicebus.windows.net/path")
```

## Running the Samples

All samples read the Azure Relay connection string from the `relay-python` environment variable.

### HTTP – Hello World listener and sender

Starts a listener that serves an HTML "Hello World" page:

```bash
python samples/hello_world_listener.py
```

The listener will connect to Azure Relay and wait for requests. Leave it running.

Sends an HTTP GET request to the listener via Azure Relay:

```bash
# In a separate terminal
python samples/sender.py
```

You should see:
- The sender displays the HTML response
- The listener logs the incoming request

### WebSocket rendezvous – echo listener and sender

The WebSocket rendezvous sample establishes a full-duplex WebSocket from the sender to the listener via Azure Relay. The listener echoes every message back to the sender.

```bash
# Terminal 1 – start the listener
python samples/websocket_listener.py
```

```bash
# Terminal 2 – run the sender
python samples/websocket_sender.py
```

You should see the sender print three round-trip text messages and one binary round-trip, while the listener logs each session.

## Running Tests

Unit tests use dummy credentials and do not require an Azure connection string.
Live Azure tests run only when `relay-python` is set and `SKIP_INTEGRATION` is not
enabled. Set `SKIP_INTEGRATION=1` (or `true`) to disable them explicitly.

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
│       ├── _rendezvous.py            # IPv6-first direct rendezvous transport
│       ├── client.py                # HybridConnectionClient (sender)
│       ├── stream.py                # HybridConnectionStream (rendezvous WebSocket)
│       ├── token_provider.py        # SAS token generation
│       ├── context.py               # Request/Response classes
│       ├── connection_string.py     # Connection string parser
│       └── protocol.py              # WebSocket protocol handling
├── tests/
│   ├── conftest.py                          # Pytest fixtures
│   ├── test_connection_string.py            # Connection string tests
│   ├── test_token_provider.py               # Token provider tests
│   ├── test_context.py                      # Request/Response tests
│   ├── test_protocol.py                     # Protocol handling tests
│   ├── test_listener.py                     # Listener unit tests
│   ├── test_token_renewal.py                # Token renewal tests
│   ├── test_reconnection.py                 # Reconnection tests
│   ├── test_ping.py                         # Keepalive tests
│   ├── test_rendezvous_protocol.py          # Rendezvous URL/parsing tests
│   ├── test_rendezvous_transport.py         # Address selection and loopback socket tests
│   ├── test_rendezvous_proxy.py             # Offline HTTP CONNECT proxy tests
│   ├── test_listener_rendezvous.py          # Listener transport policy wiring tests
│   ├── test_hybrid_connection_stream.py     # HybridConnectionStream tests
│   ├── test_hybrid_connection_client.py     # HybridConnectionClient tests
│   ├── test_accept_handler.py               # Accept-handler / reject tests
│   ├── test_hybrid_request.py               # HTTP request/response tests (incl. rendezvous upgrade)
│   ├── test_integration_rendezvous.py       # In-process fake-relay tests
│   ├── test_integration.py                  # Live integration tests (requires Azure)
│   ├── test_hello_world_listener.py         # Sample tests
│   └── test_sender.py                       # Sender tests
├── samples/
│   ├── hello_world_listener.py      # HTTP listener demo
│   ├── sender.py                    # HTTP sender demo
│   ├── websocket_listener.py        # WebSocket rendezvous listener (echo)
│   └── websocket_sender.py          # WebSocket rendezvous sender
├── requirements.txt                 # Python dependencies
├── pytest.ini                       # Pytest configuration
└── README.md                        # This file
```

## Protocol Details

This implementation follows the Azure Relay Hybrid Connections Protocol:

1. **Control Channel**: Establishes a WebSocket connection to `wss://<namespace>/$hc/<path>?sb-hc-action=listen&sb-hc-token=<token>`

2. **HTTP Request Reception**: Receives JSON `request` messages with HTTP method, URL, headers, and optional binary body frames. For requests larger than 64 kB the service sends only a rendezvous pointer; the listener opens that rendezvous WebSocket (with `sb-hc-action=request`) and reads the full request from it.

3. **HTTP Response Transmission**: Sends JSON `response` messages with status code, headers, and optional binary body frames. Responses up to 64 kB are sent on the control channel; larger responses are automatically upgraded to a rendezvous WebSocket.

   Upgraded HTTP rendezvous sockets remain open for subsequent requests from the same sender until the sender disconnects or the listener closes.

4. **WebSocket Rendezvous (Accept)**: Receives JSON `accept` messages from the service when a sender opens a WebSocket. The `accept_handler` callback can inspect the `connectHeaders` and accept (return `True`) or reject (return `False` after optionally setting `response.status_code`/`response.status_description`). Accepted streams are queued for `accept_connection()` / `connections()`.

5. **Sender Connect**: `HybridConnectionClient.create_connection()` opens `wss://<namespace>/$hc/<path>?sb-hc-action=connect[&sb-hc-id=…][&sb-hc-token=…]`. Custom headers are forwarded to the listener via the accept message.

6. **Token Renewal**: Automatically renews SAS tokens before expiration by sending `renewToken` messages on the control channel.

7. **Reconnection**: Automatically reconnects with exponential backoff on disconnection.

8. **Keepalive**: Sends periodic WebSocket ping messages to prevent NAT timeout.

### Coverage

This implementation supports:
- ✅ HTTP request/response pattern over the control channel
- ✅ Requests and responses >64 kB via the rendezvous WebSocket
- ✅ Full-duplex WebSocket rendezvous via `accept_handler` + `accept_connection()` / `connections()`
- ✅ Sender-side WebSocket connections via `HybridConnectionClient`
- ✅ Anonymous and authenticated senders
- ✅ Automatic reconnection, token renewal, and keep-alive

### Known Limitations

- Multiple listeners on the same Hybrid Connection (load balancing) is supported by the service but this client does not implement coordinated draining/leasing across listener instances; each listener simply registers and the service distributes work.
- HTTP bodies are buffered in memory rather than streamed. Very large payloads (multi-MB) should be transferred via the WebSocket rendezvous if streaming is desired.
- Duplex WebSocket streams retain the `websockets` default 1 MiB incoming-message limit; split larger transfers into multiple messages. HTTP rendezvous bodies are not subject to that limit.

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
