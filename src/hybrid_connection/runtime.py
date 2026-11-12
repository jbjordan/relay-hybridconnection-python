"""Read-only Relay data-plane runtime information."""

from dataclasses import dataclass
import asyncio
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from xml.etree import ElementTree

import aiohttp

from .errors import RelayError, EndpointNotFoundError, ProtocolError, relay_error
from .token_provider import acquire_token


@dataclass(frozen=True)
class HybridConnectionRuntimeInformation:
    created_at: datetime | None
    updated_at: datetime | None
    requires_client_authorization: bool
    user_metadata: str | None
    listener_count: int


def parse_runtime_information(data: bytes):
    try:
        root = ElementTree.fromstring(data)
        if root.tag == "{http://www.w3.org/2005/Atom}feed":
            raise EndpointNotFoundError("Hybrid Connection not found", is_transient=False)
        description = root.find(".//{*}HybridConnectionDescription")
        if description is None:
            raise ValueError("Missing HybridConnectionDescription")
        fields = {child.tag.rsplit("}", 1)[-1]: child.text for child in description}
        def date(name):
            value = fields.get(name)
            return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
        auth = fields.get("RequiresClientAuthorization", "true")
        if auth not in ("true", "false"):
            raise ValueError("Invalid RequiresClientAuthorization")
        count = int(fields.get("ListenerCount") or 0)
        if count < 0:
            raise ValueError("Negative ListenerCount")
        return HybridConnectionRuntimeInformation(
            date("CreatedAt"), date("UpdatedAt"), auth == "true",
            fields.get("UserMetadata"), count,
        )
    except (ElementTree.ParseError, ValueError) as exc:
        raise ProtocolError("Invalid Relay runtime information response") from exc


async def get_runtime_information(address, token_provider, transport):
    if token_provider is None:
        raise ValueError("Runtime information requires an authenticated token provider")
    parsed = urlsplit(address)
    query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "api-version"]
    query.append(("api-version", "2016-07"))
    url = urlunsplit(("https", parsed.netloc, parsed.path, urlencode(query), ""))
    try:
        async with asyncio.timeout(transport.open_timeout):
            token = await acquire_token(token_provider, address)
        timeout = aiohttp.ClientTimeout(total=transport.open_timeout)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=transport.proxy is True) as session:
            async with session.get(
                url, headers={"Authorization": token.token, "X-PROCESS-AT": "ServiceBus"},
                proxy=transport.proxy if isinstance(transport.proxy, str) else None,
                ssl=transport.ssl if transport.ssl is not None else True,
                allow_redirects=False,
            ) as response:
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > 1024 * 1024:
                        raise ProtocolError("Runtime information exceeds 1 MiB")
                if response.status != 200:
                    raise relay_error(
                        RuntimeError(f"Runtime information request failed: HTTP {response.status}"),
                        status_code=response.status, listener=True,
                    )
                return parse_runtime_information(data)
    except RelayError:
        raise
    except (aiohttp.ClientError, OSError, TimeoutError) as exc:
        raise relay_error(exc, listener=True) from exc
