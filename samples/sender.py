"""
Sample sender application that sends HTTP requests to the Azure Relay Hybrid Connection.

This sender uses the anonymous sender mode (no authentication required) to send
HTTP GET requests to the listener through the Azure Relay.
"""

import asyncio
import os
import aiohttp


def get_relay_url() -> str:
    """Build the relay URL from the connection string environment variable.
    
    Returns:
        The HTTPS URL for the Azure Relay hybrid connection.
    
    Raises:
        ValueError: If the environment variable is not set or missing required parts.
    """
    conn_str = os.environ.get("relay-python")
    if not conn_str:
        raise ValueError(
            "Environment variable 'relay-python' is not set. "
            "Please set it to your Azure Relay connection string."
        )
    
    endpoint = None
    entity_path = None
    
    for part in conn_str.split(";"):
        if part.startswith("Endpoint=sb://"):
            # Extract hostname from sb://hostname/
            endpoint = part.replace("Endpoint=sb://", "").rstrip("/")
        elif part.startswith("EntityPath="):
            entity_path = part.replace("EntityPath=", "")
    
    if not endpoint or not entity_path:
        raise ValueError(
            "Connection string must contain 'Endpoint' and 'EntityPath' parts."
        )
    
    return f"https://{endpoint}/{entity_path}"


RELAY_URL = get_relay_url()


async def main():
    """Send HTTP GET request to the hybrid connection endpoint and display response."""
    print(f"Sending request to: {RELAY_URL}")
    print("-" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(RELAY_URL, timeout=aiohttp.ClientTimeout(total=30)) as response:
                print(f"Status: {response.status} {response.reason}")
                print(f"\nResponse Headers:")
                for header, value in response.headers.items():
                    print(f"  {header}: {value}")
                
                print("\n" + "=" * 60)
                print("Response Body:")
                print("=" * 60)
                body = await response.text()
                print(body)
                
    except aiohttp.ClientError as e:
        print(f"Error sending request: {e}")
    except asyncio.TimeoutError:
        print("Request timed out. Is the listener running?")


if __name__ == "__main__":
    asyncio.run(main())
