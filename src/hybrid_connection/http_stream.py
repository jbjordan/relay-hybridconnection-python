"""Bounded incremental HTTP bodies over Relay rendezvous WebSockets."""

import asyncio
from io import BytesIO

from .errors import ProtocolError
from .protocol import CONTROL_CHANNEL_MAX_BODY_SIZE


class RequestBody:
    def __init__(self, websocket, limit, timeout):
        self._fragments = websocket.recv_streaming().__aiter__()
        self._buffer = b""
        self._total = 0
        self._limit = limit
        self._timeout = timeout
        self._done = False
        self._lock = asyncio.Lock()

    async def read(self, size=65536):
        if size < 0:
            chunks = [chunk async for chunk in self]
            return b"".join(chunks)
        if size == 0:
            return b""
        async with self._lock:
            while not self._buffer and not self._done:
                try:
                    async with asyncio.timeout(self._timeout):
                        chunk = await anext(self._fragments)
                except StopAsyncIteration:
                    self._done = True
                    break
                if not isinstance(chunk, bytes):
                    raise ProtocolError("Expected a binary HTTP body")
                self._total += len(chunk)
                if self._total > self._limit:
                    raise ProtocolError("HTTP request body exceeds configured limit")
                self._buffer = chunk
            result, self._buffer = self._buffer[:size], self._buffer[size:]
            return result

    def __aiter__(self):
        return self

    async def __anext__(self):
        chunk = await self.read()
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def drain(self):
        async for _ in self:
            pass


class ResponseBuffer(BytesIO):
    def __init__(self, response, limit):
        super().__init__()
        self.response = response
        self.limit = limit

    def write(self, data):
        if self.response.is_closed or self.response.failure is not None or self.response._closing:
            raise RuntimeError("Response is closed or failed")
        if self.tell() + len(data) > self.limit:
            raise BufferError("Response buffer limit reached; use await output_stream.write_async()")
        written = super().write(data)
        self.response._schedule_flush()
        return written

    async def write_async(self, data):
        view = memoryview(data)
        for offset in range(0, len(view), min(self.limit, CONTROL_CHANNEL_MAX_BODY_SIZE)):
            chunk = view[offset:offset + min(self.limit, CONTROL_CHANNEL_MAX_BODY_SIZE)]
            if self.tell() + len(chunk) > self.limit:
                await self.response.flush()
            self.write(chunk)
            if self.tell() >= min(self.limit, CONTROL_CHANNEL_MAX_BODY_SIZE):
                await self.response.flush()

    async def flush_async(self):
        await self.response.flush()

    def take(self):
        data = self.getvalue()
        self.seek(0)
        self.truncate(0)
        return data


class ResponseWriter:
    """Send one fragmented binary message with per-chunk backpressure."""

    def __init__(self, websocket, timeout):
        self.websocket = websocket
        self.timeout = timeout
        self.queue = asyncio.Queue(maxsize=1)
        self.task = None

    async def _chunks(self):
        while True:
            item = await self.queue.get()
            if item is None:
                return
            data, acknowledged = item
            yield data
            if not acknowledged.done():
                acknowledged.set_result(None)

    async def write(self, data):
        if self.task is None:
            self.task = asyncio.create_task(self.websocket.send(self._chunks()))
        acknowledged = asyncio.get_running_loop().create_future()
        async with asyncio.timeout(self.timeout):
            await self.queue.put((data, acknowledged))
            done, _ = await asyncio.wait(
                (acknowledged, self.task), return_when=asyncio.FIRST_COMPLETED
            )
            if self.task in done:
                await self.task
                raise ConnectionError("HTTP response body sender stopped unexpectedly")
            await acknowledged

    async def finish(self):
        if self.task is not None:
            async with asyncio.timeout(self.timeout):
                await self.queue.put(None)
                await self.task

    async def abort(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
