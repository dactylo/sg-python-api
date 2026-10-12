from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._transport import (
    StreamingResponse,
    TransportError,
    TransportRequest,
    TransportResponse,
)

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator, Iterator


class _AsyncFakeTransport:  # pyright: ignore[reportUnusedClass]
    def __init__(self, responses: list[TransportResponse | TransportError]) -> None:
        self._responses = list(responses)
        self._reserved_tasks: set[asyncio.Task[object]] = set()
        self.requests: list[TransportRequest] = []
        self.leases = 0
        self.checkins = 0
        self.reservation_releases = 0
        self.reservations = 0

    def _next_response(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError('_AsyncFakeTransport responses exhausted')
        response = self._responses.pop(0)
        if isinstance(response, TransportError):
            raise response
        return response

    def _release_reservation(self, task: asyncio.Task[object]) -> None:
        if task not in self._reserved_tasks:
            return
        self._reserved_tasks.remove(task)
        self.reservation_releases += 1

    def checkin(self) -> None:
        self.checkins += 1

    def checkout(self) -> None:
        self.leases += 1

    async def close(self) -> None:
        pass

    def reserve(self, task: asyncio.Task[object]) -> None:
        if task in self._reserved_tasks:
            raise TransportError('async transport task is already reserved')
        self._reserved_tasks.add(task)
        self.reservations += 1
        task.add_done_callback(self._release_reservation)

    async def send(self, request: TransportRequest) -> TransportResponse:
        return self._next_response(request)

    async def send_streaming(
        self,
        request: TransportRequest,
    ) -> AsyncStreamingResponse:
        response = self._next_response(request)

        async def stream(size: int) -> AsyncIterator[bytes]:
            for offset in range(0, len(response.content), size):
                yield response.content[offset : offset + size]

        async def close_fn() -> None:
            response.close()

        return AsyncStreamingResponse(
            status=response.status,
            reason=response.reason,
            headers=response.headers,
            stream=stream,
            close_fn=close_fn,
        )


@dataclass
class _FakeTransport:
    _responses: list[TransportResponse | TransportError] = field(init=False, repr=False)
    requests: list[TransportRequest] = field(init=False)
    _lock: threading.Lock = field(init=False, repr=False)

    def __init__(self, responses: list[TransportResponse | TransportError]) -> None:
        self._responses = list(responses)
        self.requests = []
        self._lock = threading.Lock()

    def _next_response(self, request: TransportRequest) -> TransportResponse:
        with self._lock:
            self.requests.append(request)
            if not self._responses:
                raise AssertionError('_FakeTransport responses exhausted')
            response = self._responses.pop(0)
        if isinstance(response, TransportError):
            raise response
        return response

    def abort(self, token: str) -> None:
        del token

    def close(self) -> None:
        pass

    def send(self, request: TransportRequest) -> TransportResponse:
        return self._next_response(request)

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        response = self._next_response(request)

        def stream(size: int) -> Iterator[bytes]:
            return (
                response.content[offset : offset + size]
                for offset in range(0, len(response.content), size)
            )

        return StreamingResponse(
            status=response.status,
            reason=response.reason,
            headers=response.headers,
            stream=stream,
            release=response.close,
        )
