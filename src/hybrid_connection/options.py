"""Transport, retry and listener resource configuration."""

import asyncio
import random
import math
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class TransportOptions:
    open_timeout: float = 30
    close_timeout: float = 10
    read_timeout: Optional[float] = None
    write_timeout: float = 60
    ping_interval: Optional[float] = 30
    ping_timeout: float = 10
    max_message_size: Optional[int] = 1024 * 1024
    max_queue: int = 16
    proxy: Any = True
    ssl: Any = None
    websocket_factory: Optional[Callable] = None

    def __post_init__(self):
        for name in ("open_timeout", "close_timeout", "read_timeout",
                     "write_timeout", "ping_interval", "ping_timeout"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive")
        if self.max_message_size is not None and (
            not isinstance(self.max_message_size, int) or isinstance(self.max_message_size, bool)
            or self.max_message_size <= 0
        ):
            raise ValueError("max_message_size must be positive or None")
        if not isinstance(self.max_queue, int) or isinstance(self.max_queue, bool) or self.max_queue <= 0:
            raise ValueError("max_queue must be positive")
        if self.proxy is not True and self.proxy is not None and not isinstance(self.proxy, str):
            raise ValueError("proxy must be True, None, or a proxy URL")

    async def connect(self, url, *, default_factory, **kwargs):
        options = dict(
            open_timeout=self.open_timeout, close_timeout=self.close_timeout,
            ping_interval=self.ping_interval, ping_timeout=self.ping_timeout,
            max_size=self.max_message_size, max_queue=self.max_queue,
        )
        if self.proxy is not True:
            options["proxy"] = self.proxy
        if self.ssl is not None and url.startswith("wss:"):
            options["ssl"] = self.ssl
        options.update(kwargs)
        factory = self.websocket_factory or default_factory
        async with asyncio.timeout(self.open_timeout):
            return await factory(url, **options)


@dataclass(frozen=True)
class RetryPolicy:
    initial_delay: float = 1
    max_delay: float = 60
    multiplier: float = 2
    jitter: float = 0.2
    max_attempts: Optional[int] = None
    max_elapsed: Optional[float] = None

    def __post_init__(self):
        if not all(math.isfinite(value) for value in (
            self.initial_delay, self.max_delay, self.multiplier, self.jitter
        )):
            raise ValueError("Retry values must be finite")
        if self.initial_delay <= 0 or self.max_delay < self.initial_delay:
            raise ValueError("Invalid retry delay range")
        if self.multiplier < 1 or not 0 <= self.jitter <= 1:
            raise ValueError("Invalid retry multiplier or jitter")
        if self.max_attempts is not None and (
            not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool)
            or self.max_attempts < 0
        ):
            raise ValueError("max_attempts must be nonnegative or None")
        if self.max_elapsed is not None and (not math.isfinite(self.max_elapsed) or self.max_elapsed <= 0):
            raise ValueError("max_elapsed must be positive or None")

    def delay(self, attempt):
        delay = self.initial_delay
        for _ in range(min(max(0, attempt - 1), 1024)):
            delay = min(self.max_delay, delay * self.multiplier)
            if delay >= self.max_delay:
                break
        return min(self.max_delay, delay * random.uniform(1 - self.jitter, 1 + self.jitter))


@dataclass(frozen=True)
class ListenerLimits:
    max_concurrent_requests: int = 100
    max_pending_connections: int = 100
    max_http_body_size: int = 64 * 1024 * 1024
    max_response_buffer_size: int = 4 * 1024 * 1024
    request_timeout: float = 55
    body_timeout: float = 60
    accept_timeout: float = 20
    response_flush_interval: float = 2

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive")
            if name.startswith("max_") and (isinstance(value, bool) or not isinstance(value, int)):
                raise ValueError(f"{name} must be an integer")
