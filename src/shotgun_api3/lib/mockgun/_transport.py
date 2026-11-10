"""Connect real client drivers directly to an in-memory fake site."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace

from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._transport import (
    StreamingResponse,
    TransportError,
    TransportRequest,
    TransportResponse,
)

from ._server import FakeServer


class AsyncInMemoryTransport:
    """An async transport whose RPC handlers execute without yielding.

    Logical leases and reserved children retain their lifetime bookkeeping;
    no sockets or asynchronous resources are allocated.
    """

    def __init__(self, server: FakeServer) -> None:
        """Bind transport requests to the supplied in-memory site."""
        self._server = server
        self._leases = 0
        self._reserved: set[asyncio.Task[object]] = set()

    def checkin(self) -> None:
        """Release one logical request lease."""
        self._leases -= 1

    def checkout(self) -> None:
        """Acquire one logical request lease."""
        self._leases += 1

    async def close(self) -> None:
        """Release transport resources; the fake site remains available."""

    def reserve(self, task: asyncio.Task[object]) -> None:
        """Retain a child task until its completion callback runs.

        Raises:
            TransportError: If the child is already reserved.
        """
        if task in self._reserved:
            raise TransportError('task is already reserved')
        self._reserved.add(task)
        task.add_done_callback(self._reserved.discard)

    async def send(self, request: TransportRequest) -> TransportResponse:
        """Dispatch one buffered request without yielding to the event loop.

        Raises:
            MockgunError: If the request or required fake-site behavior is
                unsupported or unsettled.
            Exception: If consuming a supplied request-body iterator fails.
        """
        body = request.body
        if body is not None and not isinstance(body, bytes):
            request = replace(request, body=b''.join(body))
        return self._server.handle(request)

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        """Wrap a buffered site response as a chunked async response.

        The request and complete response are processed before the streaming
        wrapper is returned.

        Raises:
            MockgunError: If the request or required fake-site behavior is
                unsupported or unsettled.
            Exception: If consuming a supplied request-body iterator fails.
        """
        response = await self.send(request)
        return AsyncStreamingResponse(
            response.status,
            response.reason,
            response.headers,
            lambda size: _async_chunks(response.content, size),
            lambda: _close_stream(response),
        )


class InMemoryTransport:
    """A synchronous transport that allocates no network resources."""

    def __init__(self, server: FakeServer) -> None:
        """Bind transport requests to the supplied in-memory site."""
        self._server = server

    def close(self) -> None:
        """Release transport resources; the fake site remains available."""

    def send(self, request: TransportRequest) -> TransportResponse:
        """Dispatch one request directly to the fake site.

        Raises:
            MockgunError: If the request or required fake-site behavior is
                unsupported or unsettled.
            Exception: If consuming a supplied request-body iterator fails.
        """
        body = request.body
        if body is not None and not isinstance(body, bytes):
            request = replace(request, body=b''.join(body))
        return self._server.handle(request)

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        """Wrap a buffered site response as a chunked synchronous response.

        The request and complete response are processed before the streaming
        wrapper is returned.

        Raises:
            MockgunError: If the request or required fake-site behavior is
                unsupported or unsettled.
            Exception: If consuming a supplied request-body iterator fails.
        """
        response = self.send(request)
        return StreamingResponse(
            response.status,
            response.reason,
            response.headers,
            lambda size: _chunks(response.content, size),
            response.close,
        )


def _chunks(content: bytes, size: int) -> Iterator[bytes]:
    """Yield buffered response bytes in the requested chunk size."""
    for start in range(0, len(content), size):
        yield content[start : start + size]


async def _async_chunks(content: bytes, size: int) -> AsyncIterator[bytes]:
    """Yield buffered response bytes through an async iterator."""
    for chunk in _chunks(content, size):
        yield chunk


async def _close_stream(response: TransportResponse) -> None:
    """Release the buffered response after an async stream closes."""
    response.close()
