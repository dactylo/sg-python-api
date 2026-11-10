from __future__ import annotations

import asyncio
import dataclasses
from typing import TYPE_CHECKING, TypeAlias

from shotgun_api3 import AsyncShotgun
from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import AsyncFakeTransport
from tests.conftest import make_response, set_fake_async_transport

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping

DIRECT_INFO: dict[str, object] = {
    'version': [9, 0, 0],
    'api_max_entities_per_page': 500,
    's3_direct_uploads_enabled': True,
    's3_enabled_upload_types': {'*': ['*']},
}


ScriptItem: TypeAlias = 'TransportResponse | TransportError | PartialUploadFailure | StreamScript'


StreamItem: TypeAlias = bytes | TransportError


@dataclasses.dataclass(frozen=True)
class PartialUploadFailure:
    error: TransportError


@dataclasses.dataclass(frozen=True)
class StreamScript:
    chunks: tuple[StreamItem, ...]
    label: str
    releases: list[str]
    requested_sizes: list[int] = dataclasses.field(default_factory=list[int])
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict[str, str])
    status: int = 200
    blocker: asyncio.Event | None = None
    started: asyncio.Event | None = None


class AsyncFileTransport(AsyncFakeTransport):
    def __init__(
        self,
        responses: list[ScriptItem],
        on_send: Callable[[TransportRequest], None] | None = None,
    ) -> None:
        super().__init__([])
        self._on_send = on_send
        self._file_responses = list(responses)
        self.bodies: list[bytes | None] = []
        self.body_ids: list[int | None] = []

    @staticmethod
    def _consume_body(body: bytes | Iterator[bytes] | None) -> bytes | None:
        if body is None or isinstance(body, bytes):
            return body
        return b''.join(body)

    def _next(self, request: TransportRequest) -> ScriptItem:
        self.requests.append(request)
        if self._on_send is not None:
            self._on_send(request)
        if not self._file_responses:
            raise AssertionError('async file transport responses exhausted')
        return self._file_responses.pop(0)

    async def send(self, request: TransportRequest) -> TransportResponse:
        outcome = self._next(request)
        body = request.body
        self.body_ids.append(None if body is None else id(body))
        if isinstance(outcome, PartialUploadFailure):
            assert body is not None
            assert not isinstance(body, bytes)
            self.bodies.append(await asyncio.to_thread(next, body))
            raise outcome.error
        self.bodies.append(await asyncio.to_thread(self._consume_body, body))
        if isinstance(outcome, TransportError):
            raise outcome
        assert isinstance(outcome, TransportResponse)
        return outcome

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        outcome = self._next(request)
        self.body_ids.append(None)
        self.bodies.append(None)
        if isinstance(outcome, TransportError):
            raise outcome
        assert isinstance(outcome, StreamScript)

        async def stream(size: int) -> AsyncIterator[bytes]:
            outcome.requested_sizes.append(size)
            for item in outcome.chunks:
                if isinstance(item, bytes):
                    for offset in range(0, len(item), size):
                        yield item[offset : offset + size]
                else:
                    raise item
            if outcome.started is not None:
                outcome.started.set()
            if outcome.blocker is not None:
                await outcome.blocker.wait()

        async def close_fn() -> None:
            outcome.releases.append(outcome.label)

        return AsyncStreamingResponse(
            status=outcome.status,
            reason='OK' if outcome.status == 200 else 'Request failed',
            headers=CaseInsensitiveHeaders(outcome.headers),
            stream=stream,
            close_fn=close_fn,
        )


def async_file_client(
    responses: list[ScriptItem],
    *,
    base_url: str = 'https://x.example',
    on_send: Callable[[TransportRequest], None] | None = None,
    server_info: dict[str, object] | None = DIRECT_INFO,
    session_token: str | None = None,
) -> tuple[AsyncShotgun, AsyncFileTransport]:
    sg = AsyncShotgun(
        base_url,
        script_name=None if session_token is not None else 'script',
        api_key=None if session_token is not None else 'key',
        session_token=session_token,
        connect=False,
    )
    scripted = responses if server_info is None else [make_response(server_info), *responses]
    transport = AsyncFileTransport(scripted, on_send)
    set_fake_async_transport(sg, transport)
    sg.config.rpc_attempt_interval = 0
    return sg, transport
