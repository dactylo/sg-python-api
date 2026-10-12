import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, cast, get_type_hints

import pytest

from shotgun_api3._async_transport import (
    AsyncStreamingResponse,
    AsyncTransport,
)
from shotgun_api3._transport import (
    ConnectError,
    TimeoutConfig,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _AsyncFakeTransport

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

pytestmark = pytest.mark.async_required

_OK = TransportResponse(status=200, reason='OK', headers={}, content=b'PAYLOAD')
_REQUEST_ID = 't-1'
_REQ = TransportRequest(
    method='POST',
    url='https://x.example/api3/json',
    headers={},
    body=b'{}',
    timeout=TimeoutConfig(connect=1.0, read=1.0),
    token=_REQUEST_ID,
)


def test_async_streaming_response_type_hints_resolve_at_runtime() -> None:
    hints = get_type_hints(AsyncStreamingResponse.__init__)

    assert hints['stream'] == Callable[[int], AsyncIterator[bytes]]
    assert hints['close_fn'] == Callable[[], Awaitable[None]]


def test_async_transport_protocol_has_only_task_cancellation() -> None:
    assert not hasattr(AsyncTransport, 'abort')
    assert not iscoroutinefunction(AsyncTransport.checkout)
    assert not iscoroutinefunction(AsyncTransport.checkin)
    assert not iscoroutinefunction(AsyncTransport.reserve)
    assert iscoroutinefunction(AsyncTransport.close)
    assert iscoroutinefunction(AsyncTransport.send)
    assert iscoroutinefunction(AsyncTransport.send_streaming)


@pytest.mark.anyio
async def test_fake_counts_logical_rpc_leases() -> None:
    fake = _AsyncFakeTransport([_OK])

    fake.checkout()
    await fake.send(_REQ)
    fake.checkin()

    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_fake_raises_queued_errors_and_asserts_on_exhaustion() -> None:
    queued_error = ConnectError('refused')
    fake = _AsyncFakeTransport([queued_error])

    with pytest.raises(ConnectError, match='refused') as exc_info:
        await fake.send(_REQ)
    with pytest.raises(AssertionError, match='_AsyncFakeTransport responses exhausted'):
        await fake.send(_REQ)

    assert exc_info.value is queued_error
    assert fake.requests == [_REQ, _REQ]


@pytest.mark.anyio
async def test_fake_releases_child_reservation_exactly_once() -> None:
    fake = _AsyncFakeTransport([])
    task = asyncio.create_task(asyncio.sleep(0))

    fake.reserve(task)
    with pytest.raises(TransportError, match='already reserved'):
        fake.reserve(task)
    await task
    await asyncio.sleep(0)

    assert fake.reservations == 1
    assert fake.reservation_releases == 1


@pytest.mark.anyio
async def test_fake_replays_fifo_and_records_requests() -> None:
    second = TransportResponse(status=201, reason='Created', headers={}, content=b'NEXT')
    fake = _AsyncFakeTransport([_OK, second])

    assert await fake.send(_REQ) is _OK
    assert await fake.send(_REQ) is second
    assert fake.requests == [_REQ, _REQ]


@pytest.mark.anyio
async def test_fake_streams_scripted_content_in_bounded_chunks() -> None:
    fake = _AsyncFakeTransport([_OK])

    response = await fake.send_streaming(_REQ)
    chunks = [chunk async for chunk in response.aiter_chunks(3)]

    assert b''.join(chunks) == b'PAYLOAD'
    assert all(0 < len(chunk) <= 3 for chunk in chunks)
    assert fake.requests == [_REQ]


@pytest.mark.anyio
async def test_streaming_response_body_error_survives_close_failure() -> None:
    body_error = ValueError('body failed')
    close_calls = 0

    async def close_fn() -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError('close failed')

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'PARTIAL'
        raise body_error

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)

    with pytest.raises(ValueError, match='body failed') as exc_info:
        _chunks = [chunk async for chunk in response.aiter_chunks()]
    await response.close()

    assert exc_info.value is body_error
    assert close_calls == 1


@pytest.mark.anyio
async def test_streaming_response_cancellation_survives_close_failure() -> None:
    blocker = asyncio.Event()
    close_calls = 0
    original_cancellation: asyncio.CancelledError | None = None
    started = asyncio.Event()

    async def close_fn() -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError('close failed')

    async def consume(response: AsyncStreamingResponse) -> None:
        async for _chunk in response.aiter_chunks():
            pass

    async def stream(size: int) -> AsyncIterator[bytes]:
        nonlocal original_cancellation
        del size
        started.set()
        try:
            await blocker.wait()
        except asyncio.CancelledError as exc:
            original_cancellation = exc
            raise
        yield b'UNREACHABLE'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)
    task = asyncio.create_task(consume(response))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await task
    await response.close()

    assert exc_info.value is original_cancellation
    assert close_calls == 1


@pytest.mark.anyio
async def test_streaming_response_close_consumes_callback_before_exception() -> None:
    close_calls = 0

    async def close_fn() -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError('close failed')

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'PAYLOAD'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)

    with pytest.raises(RuntimeError, match='close failed'):
        await response.close()
    await response.close()

    assert close_calls == 1


@pytest.mark.anyio
async def test_streaming_response_closes_after_terminal_iteration() -> None:
    close_calls = 0

    async def close_fn() -> None:
        nonlocal close_calls
        close_calls += 1

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'PAYLOAD'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)

    assert [chunk async for chunk in response.aiter_chunks()] == [b'PAYLOAD']
    await response.close()

    assert close_calls == 1


@pytest.mark.anyio
async def test_streaming_response_early_close_survives_close_failure() -> None:
    close_calls = 0

    async def close_fn() -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError('close failed')

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'FIRST'
        yield b'SECOND'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)
    iterator = cast('AsyncGenerator[bytes, None]', response.aiter_chunks())
    assert await anext(iterator) == b'FIRST'

    await iterator.aclose()
    await response.close()

    assert close_calls == 1


@pytest.mark.anyio
async def test_streaming_response_rejects_nonpositive_chunk_size() -> None:
    async def close_fn() -> None:
        return None

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'PAYLOAD'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)

    with pytest.raises(TransportError, match='streaming chunk size must be positive'):
        response.aiter_chunks(0)


@pytest.mark.anyio
async def test_streaming_response_rejects_second_body_claim() -> None:
    async def close_fn() -> None:
        return None

    async def stream(size: int) -> AsyncIterator[bytes]:
        del size
        yield b'PAYLOAD'

    response = AsyncStreamingResponse(200, 'OK', _OK.headers, stream, close_fn)
    iterator = cast('AsyncGenerator[bytes, None]', response.aiter_chunks())

    with pytest.raises(TransportError, match='streaming response was already consumed'):
        response.aiter_chunks()
    await iterator.aclose()
    await response.close()
