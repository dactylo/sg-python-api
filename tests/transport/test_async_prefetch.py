import asyncio
import gc
import json
from collections.abc import AsyncGenerator
from typing import cast

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3._async_driver import AsyncDriver, _AsyncDriverTiming
from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._config import Config
from shotgun_api3._transport import (
    ConnectError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _AsyncFakeTransport

pytestmark = pytest.mark.async_required


class _PagedAsyncTransport(_AsyncFakeTransport):
    def __init__(self, *, per_page: int = 1, total: int = 10) -> None:
        super().__init__([])
        self.active_attempts: set[tuple[int, int]] = set()
        self.attempts: dict[int, int] = {}
        self.blocked_pages: set[int] = set()
        self.cancelled_pages: set[int] = set()
        self.failures: dict[tuple[int, int], BaseException] = {}
        self.finished_attempts: set[tuple[int, int]] = set()
        self.pages_requested: list[int] = []
        self.per_page = per_page
        self.releases: dict[int, asyncio.Event] = {}
        self.started: dict[int, asyncio.Event] = {}
        self.total = total
        self.wait_for_started: dict[int, int] = {}

    def release(self, page: int) -> None:
        self.releases.setdefault(page, asyncio.Event()).set()

    async def send(self, request: TransportRequest) -> TransportResponse:
        page = _request_page(request)
        attempt = self.attempts.get(page, 0) + 1
        self.attempts[page] = attempt
        self.active_attempts.add((page, attempt))
        self.pages_requested.append(page)
        self.requests.append(request)
        self.started.setdefault(page, asyncio.Event()).set()
        try:
            failure = self.failures.get((page, attempt))
            if failure is not None:
                raise failure
            prerequisite = self.wait_for_started.get(page)
            if prerequisite is not None:
                await self.started.setdefault(prerequisite, asyncio.Event()).wait()
            if page in self.blocked_pages:
                await self.releases.setdefault(page, asyncio.Event()).wait()
            await asyncio.sleep(0)
            return TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=_page_payload(page, self.total, self.per_page),
            )
        except asyncio.CancelledError:
            self.cancelled_pages.add(page)
            raise
        finally:
            self.active_attempts.discard((page, attempt))
            self.finished_attempts.add((page, attempt))

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        del request
        raise AssertionError('page RPCs must use buffered send')


def _build(page: int) -> dict[str, object]:
    return {
        'type': 'Shot',
        'paging': {'current_page': page, 'entities_per_page': 1},
    }


def _config(prefetch: int) -> Config:
    config = Config()
    config.update_fields(
        api_key='key',
        max_rpc_attempts=2,
        page_prefetch=prefetch,
        rpc_attempt_interval=0,
        script_name='script',
    )
    config.set_server_params('https://x.example')
    return config


def _entities(result: object) -> list[dict[str, object]]:
    mapping = cast('dict[str, object]', result)
    return cast('list[dict[str, object]]', mapping['entities'])


def _page_payload(page: int, total: int, per_page: int) -> bytes:
    remaining = max(0, total - (page - 1) * per_page)
    count = min(per_page, remaining)
    entities = [
        {'type': 'Shot', 'id': (page - 1) * per_page + offset + 1} for offset in range(count)
    ]
    return json.dumps(
        {
            'results': {
                'entities': entities,
                'paging_info': {'entity_count': total},
            }
        }
    ).encode()


def _request_page(request: TransportRequest) -> int:
    assert isinstance(request.body, bytes)
    payload = cast('dict[str, object]', json.loads(request.body))
    params = cast('list[object]', payload['params'])
    read_params = cast('dict[str, object]', params[1])
    paging = cast('dict[str, int]', read_params['paging'])
    return paging['current_page']


async def _sleepless(_delay: float) -> None:
    return None


async def _wait_started(transport: _PagedAsyncTransport, page: int) -> None:
    await transport.started.setdefault(page, asyncio.Event()).wait()


async def _raise_consumer_failure(
    pages: AsyncGenerator[object, None],
    transport: _PagedAsyncTransport,
) -> None:
    try:
        await anext(pages)
        await anext(pages)
        await _wait_started(transport, 3)
        raise RuntimeError('consumer failed')
    finally:
        await pages.aclose()


@pytest.mark.anyio
async def test_call_pages_bounds_speculation_past_final_page() -> None:
    transport = _PagedAsyncTransport(total=3)
    transport.wait_for_started[3] = 4
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )

    results = [await anext(pages), await anext(pages), await anext(pages)]
    await pages.aclose()

    assert [_entities(result)[0]['id'] for result in results] == [1, 2, 3]
    assert sorted(transport.pages_requested) == [1, 2, 3, 4]
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_call_pages_cancellation_cancels_and_awaits_all_page_tasks() -> None:
    transport = _PagedAsyncTransport()
    transport.blocked_pages.update({2, 3})
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )
    await anext(pages)
    advance = asyncio.create_task(anext(pages))
    await _wait_started(transport, 2)
    await _wait_started(transport, 3)

    advance.cancel()
    with pytest.raises(asyncio.CancelledError):
        await advance

    assert transport.cancelled_pages == {2, 3}
    assert transport.active_attempts == set()
    assert transport.leases == transport.checkins == 3
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_call_pages_forwards_first_page_without_driver_revalidation() -> None:
    transport = _PagedAsyncTransport(total=1)
    pages = AsyncDriver(transport).call_pages(
        _build, 0, 'read', config_module.snapshot(_config(0))
    )

    await anext(pages)
    await pages.aclose()

    assert transport.pages_requested == [0]


@pytest.mark.anyio
async def test_call_pages_prefetch_one_does_not_request_past_final_page() -> None:
    transport = _PagedAsyncTransport(total=2)
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(1))
    )

    await anext(pages)
    await anext(pages)
    await pages.aclose()

    assert transport.pages_requested == [1, 2]


@pytest.mark.anyio
async def test_call_pages_starts_lookahead_only_after_consumer_resumes() -> None:
    transport = _PagedAsyncTransport(total=3)
    transport.wait_for_started[2] = 3
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(3))
    )

    first = await anext(pages)
    assert _entities(first)[0]['id'] == 1
    assert transport.pages_requested == [1]

    second = await anext(pages)
    assert _entities(second)[0]['id'] == 2
    assert set(transport.pages_requested) == {1, 2, 3, 4}
    await pages.aclose()


@pytest.mark.anyio
async def test_call_pages_uses_coherent_context_during_live_config_mutation() -> None:
    transport = _PagedAsyncTransport()
    transport.blocked_pages.add(1)
    transport.failures[(2, 1)] = ConnectError('retry page two')
    transport.wait_for_started[2] = 3
    config = _config(2)
    context = config_module.snapshot(config)
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    pages = driver.call_pages(_build, 1, 'read', context)
    first_result = asyncio.create_task(anext(pages))
    await _wait_started(transport, 1)

    config.set_server_params('https://changed.example')
    config.update_fields(
        max_rpc_attempts=1,
        page_prefetch=0,
        rpc_attempt_interval=99,
    )
    transport.release(1)

    await first_result
    await anext(pages)
    await pages.aclose()

    assert transport.attempts[2] == 2
    assert set(transport.pages_requested) == {1, 2, 3}
    assert all(request.url == context.api_url for request in transport.requests)
    assert all(request.timeout == context.timeout for request in transport.requests)
    assert context.canonical_origin == 'https://x.example:443'
    assert config_module.snapshot(config).canonical_origin == 'https://changed.example:443'


@pytest.mark.anyio
async def test_call_pages_yields_in_order_after_out_of_order_completion() -> None:
    transport = _PagedAsyncTransport(total=3)
    transport.wait_for_started[2] = 3
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )

    first = await anext(pages)
    second = await anext(pages)
    third = await anext(pages)
    await pages.aclose()

    assert [_entities(result)[0]['id'] for result in (first, second, third)] == [1, 2, 3]
    assert transport.finished_attempts.issuperset({(2, 1), (3, 1)})


@pytest.mark.anyio
async def test_call_pages_zero_prefetch_is_strictly_serial() -> None:
    transport = _PagedAsyncTransport(total=2)
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(0))
    )

    first = await anext(pages)
    assert _entities(first)[0]['id'] == 1
    assert transport.pages_requested == [1]
    assert transport.leases == transport.checkins == 1

    second = await anext(pages)
    assert _entities(second)[0]['id'] == 2
    assert transport.pages_requested == [1, 2]
    assert transport.leases == transport.checkins == 2
    await pages.aclose()


@pytest.mark.anyio
async def test_consumer_failure_close_waits_for_speculative_page_cleanup() -> None:
    transport = _PagedAsyncTransport()
    transport.blocked_pages.add(3)
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )

    with pytest.raises(RuntimeError, match=r'^consumer failed$'):
        await _raise_consumer_failure(pages, transport)

    assert transport.cancelled_pages == {3}
    assert transport.active_attempts == set()
    assert transport.leases == transport.checkins == 3
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_early_aclose_cancels_and_awaits_speculative_page() -> None:
    transport = _PagedAsyncTransport()
    transport.blocked_pages.add(3)
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )
    await anext(pages)
    await anext(pages)
    await _wait_started(transport, 3)

    await pages.aclose()

    assert transport.cancelled_pages == {3}
    assert transport.active_attempts == set()
    assert transport.leases == transport.checkins == 3
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_initial_page_builder_failure_creates_no_page_task() -> None:
    transport = _PagedAsyncTransport()

    def fail_builder(_page: int) -> object:
        raise RuntimeError('builder failed')

    pages = AsyncDriver(transport).call_pages(
        fail_builder,
        1,
        'read',
        config_module.snapshot(_config(2)),
    )

    with pytest.raises(RuntimeError, match=r'^builder failed$'):
        await anext(pages)

    assert transport.pages_requested == []
    assert transport.active_attempts == set()
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_later_page_builder_failure_cancels_and_awaits_created_tasks() -> None:
    transport = _PagedAsyncTransport()

    def fail_fourth(page: int) -> object:
        if page == 4:
            raise RuntimeError('later builder failed')
        return _build(page)

    pages = AsyncDriver(transport).call_pages(
        fail_fourth,
        1,
        'read',
        config_module.snapshot(_config(3)),
    )
    await anext(pages)

    with pytest.raises(RuntimeError, match=r'^later builder failed$'):
        await anext(pages)

    assert transport.active_attempts == set()
    assert transport.leases == transport.checkins == 1
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_page_task_exception_cancels_and_awaits_speculative_tasks() -> None:
    transport = _PagedAsyncTransport()
    transport.blocked_pages.add(3)
    transport.failures[(2, 1)] = RuntimeError('page failed')
    transport.wait_for_started[2] = 3
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )
    await anext(pages)

    with pytest.raises(RuntimeError, match=r'^page failed$'):
        await anext(pages)

    assert transport.cancelled_pages == {3}
    assert transport.active_attempts == set()
    assert transport.leases == transport.checkins == 3
    assert transport.reservations == transport.reservation_releases


@pytest.mark.anyio
async def test_speculative_exception_is_retrieved_on_generator_close() -> None:
    transport = _PagedAsyncTransport()
    transport.failures[(3, 1)] = RuntimeError('speculative failure')
    transport.wait_for_started[2] = 3
    pages = AsyncDriver(transport).call_pages(
        _build, 1, 'read', config_module.snapshot(_config(2))
    )
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    unhandled: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    try:
        await anext(pages)
        await anext(pages)
        await pages.aclose()
        del pages
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert unhandled == []
    assert transport.active_attempts == set()
    assert transport.reservations == transport.reservation_releases
