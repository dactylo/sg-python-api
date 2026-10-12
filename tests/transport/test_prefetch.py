import json
import threading
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future
from typing import cast

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3 import _driver as driver_module
from shotgun_api3 import _errors, _protocol
from shotgun_api3._client import Shotgun
from shotgun_api3._config import Config, RequestContext, ResponseEvent
from shotgun_api3._driver import Driver
from shotgun_api3._transport import (
    StreamingResponse,
    TimeoutConfig,
    Transport,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _FakeTransport
from tests.conftest import _make_response


class _CountingResponse(TransportResponse):
    close_calls: int
    _count_lock: threading.Lock

    def __init__(self, payload: object) -> None:
        super().__init__(
            status=200,
            reason='OK',
            headers={'content-type': 'application/json'},
            content=json.dumps({'results': payload}).encode(),
        )
        object.__setattr__(self, 'close_calls', 0)
        object.__setattr__(self, '_count_lock', threading.Lock())

    def close(self) -> None:
        with self._count_lock:
            object.__setattr__(self, 'close_calls', self.close_calls + 1)
        super().close()


class _PageTransport:
    def __init__(
        self,
        pages: Mapping[int, TransportResponse],
        on_page: Callable[[int], None] | None = None,
        *,
        page_limit: int = 500,
    ) -> None:
        self.closed = False
        self.page_limit = page_limit
        self.pages = dict(pages)
        self.read_pages: list[int] = []
        self.requests: list[TransportRequest] = []
        self.returned_responses: list[TransportResponse] = []
        self._lock = threading.Lock()
        self._on_page = on_page

    def abort(self, token: str) -> None:
        del token

    def close(self) -> None:
        self.closed = True

    def send(self, request: TransportRequest) -> TransportResponse:
        assert isinstance(request.body, bytes)
        payload = cast('dict[str, object]', json.loads(request.body))
        if payload['method_name'] == 'info':
            response = _CountingResponse(
                {
                    'version': [9, 0, 0],
                    'api_max_entities_per_page': self.page_limit,
                }
            )
            with self._lock:
                self.returned_responses.append(response)
            return response
        params = cast('list[object]', payload['params'])
        read_params = cast('dict[str, object]', params[1])
        paging = cast('dict[str, int]', read_params['paging'])
        page = paging['current_page']
        response = self.pages.get(page)
        if response is None:
            response = _counting_page([], 0)
        with self._lock:
            self.read_pages.append(page)
            self.requests.append(request)
        if self._on_page is not None:
            self._on_page(page)
        with self._lock:
            self.returned_responses.append(response)
        return response

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        response = self.send(request)

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


class _TrackingLock:
    def __init__(self) -> None:
        self.acquisitions: list[str] = []
        self._lock = threading.Lock()

    def __enter__(self) -> None:
        self._lock.acquire()
        self.acquisitions.append(threading.current_thread().name)

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> None:
        self._lock.release()


class _TransportFactoryClient(Shotgun):
    def __init__(self, transports: list[Transport]) -> None:
        self._test_transports: Iterator[Transport] = iter(transports)
        super().__init__('https://x.example', script_name='s', api_key='k', connect=False)

    def _new_transport(self, context: RequestContext) -> Transport:
        del context
        return next(self._test_transports)


def _client(transport: _PageTransport, prefetch: int) -> Shotgun:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg._transport = transport
    sg.config.page_prefetch = prefetch
    return sg


def _context(prefetch: int) -> RequestContext:
    config = Config()
    config.update_fields(
        api_key='key',
        max_rpc_attempts=1,
        page_prefetch=prefetch,
        script_name='script',
    )
    config.set_server_params('https://x.example')
    return config_module.snapshot(config)


def _counting_page(ids: list[int], count: int) -> _CountingResponse:
    return _CountingResponse(
        {
            'entities': [{'type': 'Shot', 'id': entity_id} for entity_id in ids],
            'paging_info': {'entity_count': count},
        }
    )


def _page(ids: list[int], count: int) -> TransportResponse:
    return _make_response(
        {
            'entities': [{'type': 'Shot', 'id': entity_id} for entity_id in ids],
            'paging_info': {'entity_count': count},
        }
    )


def _request(token: str) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url='https://x.example/api',
        headers={},
        body=b'{}',
        timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0),
        token=token,
    )


def _request_bodies_by_page(transport: _PageTransport) -> dict[int, list[bytes]]:
    bodies: dict[int, list[bytes]] = {}
    for request in transport.requests:
        assert isinstance(request.body, bytes)
        payload = cast('dict[str, object]', json.loads(request.body))
        params = cast('list[object]', payload['params'])
        read_params = cast('dict[str, object]', params[1])
        paging = cast('dict[str, int]', read_params['paging'])
        bodies.setdefault(paging['current_page'], []).append(request.body)
    return bodies


def test_call_pages_shuts_down_executor_when_initial_builder_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shutdown_calls: list[tuple[bool, bool]] = []
    executor_type = driver_module.concurrent.futures.ThreadPoolExecutor

    class TrackingExecutor(executor_type):
        def shutdown(
            self,
            wait: bool = True,
            *,
            cancel_futures: bool = False,
        ) -> None:
            shutdown_calls.append((wait, cancel_futures))
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    def fail_builder(_page: int) -> object:
        raise RuntimeError('builder-failed')

    monkeypatch.setattr(
        driver_module.concurrent.futures,
        'ThreadPoolExecutor',
        TrackingExecutor,
    )
    pages = Driver(_PageTransport({})).call_pages(
        fail_builder,
        1,
        'read',
        _context(2),
    )

    with pytest.raises(RuntimeError, match=r'^builder-failed$'):
        next(pages)

    assert shutdown_calls == [(True, True)]


def test_call_pages_starts_lookahead_only_after_consumer_resumes() -> None:
    transport = _PageTransport({1: _page([1], 3)})
    pages = Driver(transport).call_pages(
        lambda page: {
            'type': 'Shot',
            'paging': {'current_page': page, 'entities_per_page': 1},
        },
        1,
        'read',
        _context(3),
    )

    assert next(pages) == {
        'entities': [{'type': 'Shot', 'id': 1}],
        'paging_info': {'entity_count': 3},
    }
    assert transport.read_pages == [1]

    pages.close()


@pytest.mark.parametrize('failure_mode', ['validation', 'verdict'])
def test_client_consumer_failure_waits_for_speculative_cleanup(
    failure_mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    third_started = threading.Event()
    release_third = threading.Event()

    def coordinate(page: int) -> None:
        if page == 2:
            assert third_started.wait(5), 'speculative page did not start'
        elif page == 3:
            third_started.set()
            assert release_third.wait(5), 'speculative page was not released'

    second_response = (
        _CountingResponse(
            {
                'entities': 'invalid',
                'paging_info': {'entity_count': 1001},
            }
        )
        if failure_mode == 'validation'
        else _counting_page(list(range(501, 1001)), 1001)
    )
    transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 1001),
            2: second_response,
            3: _counting_page([1001], 1001),
        },
        coordinate,
    )
    sg = _client(transport, prefetch=2)
    if failure_mode == 'verdict':
        original_page_verdict = _protocol.page_verdict

        def fail_second_verdict(
            paging_info: dict[str, object],
            entities_count: int,
            records_count: int,
            per_page: int,
            limit: int,
        ) -> str:
            if records_count == 1000:
                raise RuntimeError('verdict-failed')
            return original_page_verdict(
                paging_info,
                entities_count,
                records_count,
                per_page,
                limit,
            )

        monkeypatch.setattr(_protocol, 'page_verdict', fail_second_verdict)
    errors: list[Exception] = []
    find_done = threading.Event()

    def run_find() -> None:
        try:
            sg.find('Shot', [])
        except (AssertionError, RuntimeError, _errors.ShotgunError) as exc:
            errors.append(exc)
        finally:
            find_done.set()

    worker = threading.Thread(target=run_find)
    worker.start()
    assert third_started.wait(5), 'speculative page did not start'
    assert not find_done.is_set()
    release_third.set()
    worker.join(5)

    assert not worker.is_alive()
    assert len(errors) == 1
    if failure_mode == 'validation':
        assert isinstance(errors[0], _errors.ShotgunError)
        assert str(errors[0]) == 'read did not return an entities list'
    else:
        assert isinstance(errors[0], RuntimeError)
        assert str(errors[0]) == 'verdict-failed'
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert sg._lease_counts == {}
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


def test_executor_submit_failure_waits_for_submitted_page_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_started = threading.Event()
    release_second = threading.Event()
    shutdown_calls: list[tuple[bool, bool]] = []
    executor_type = driver_module.concurrent.futures.ThreadPoolExecutor

    class FailingSubmitExecutor:
        def __init__(self, max_workers: int, thread_name_prefix: str) -> None:
            self._executor = executor_type(
                max_workers=max_workers,
                thread_name_prefix=thread_name_prefix,
            )
            self.submit_calls = 0

        def shutdown(
            self,
            wait: bool = True,
            *,
            cancel_futures: bool = False,
        ) -> None:
            shutdown_calls.append((wait, cancel_futures))
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

        def submit(
            self,
            fn: Callable[..., object],
            /,
            *args: object,
            **kwargs: object,
        ) -> driver_module.concurrent.futures.Future[object]:
            self.submit_calls += 1
            if self.submit_calls == 3:
                assert second_started.wait(5), 'submitted page did not start'
                raise RuntimeError('submit-failed')
            return self._executor.submit(fn, *args, **kwargs)

    def coordinate(page: int) -> None:
        if page == 2:
            second_started.set()
            assert release_second.wait(5), 'submitted page was not released'

    transport = _PageTransport(
        {
            1: _counting_page([1], 2),
            2: _counting_page([2], 2),
            3: _counting_page([], 0),
        },
        coordinate,
    )
    monkeypatch.setattr(
        driver_module.concurrent.futures,
        'ThreadPoolExecutor',
        FailingSubmitExecutor,
    )
    pages = Driver(transport).call_pages(
        lambda page: {
            'type': 'Shot',
            'paging': {'current_page': page, 'entities_per_page': 1},
        },
        1,
        'read',
        _context(3),
    )
    next(pages)
    errors: list[Exception] = []
    advance_done = threading.Event()

    def advance() -> None:
        try:
            next(pages)
        except (AssertionError, RuntimeError) as exc:
            errors.append(exc)
        finally:
            advance_done.set()

    worker = threading.Thread(target=advance)
    worker.start()
    assert second_started.wait(5), 'submitted page did not start'
    assert not advance_done.is_set()
    release_second.set()
    worker.join(5)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert str(errors[0]) == 'submit-failed'
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert cast('_CountingResponse', transport.pages[3]).close_calls == 0
    assert shutdown_calls == [(True, True)]


def test_fake_transport_atomically_pairs_concurrent_requests_and_responses() -> None:
    responses: list[TransportResponse | TransportError] = [
        _make_response({'slot': 1}),
        _make_response({'slot': 2}),
    ]
    transport = _FakeTransport(responses)
    tracking_lock = _TrackingLock()
    object.__setattr__(transport, '_lock', tracking_lock)
    barrier = threading.Barrier(3)
    results: dict[str, TransportResponse] = {}
    errors: list[Exception] = []
    result_lock = threading.Lock()

    def send(token: str) -> None:
        try:
            barrier.wait(5)
            result = transport.send(_request(token))
            with result_lock:
                results[token] = result
        except (AssertionError, threading.BrokenBarrierError) as exc:
            with result_lock:
                errors.append(exc)

    workers = [
        threading.Thread(target=send, args=(token,), name=token)
        for token in ('request-a', 'request-b')
    ]
    for worker in workers:
        worker.start()
    barrier.wait(5)
    for worker in workers:
        worker.join(5)

    assert not errors
    assert all(not worker.is_alive() for worker in workers)
    assert [request.token for request in transport.requests] == tracking_lock.acquisitions
    assert len(tracking_lock.acquisitions) == 2
    assert [results[request.token] for request in transport.requests] == responses


def test_first_find_prefetch_uses_readiness_page_limit() -> None:
    caller = threading.current_thread()
    request_threads: list[tuple[int, threading.Thread]] = []
    transport = _PageTransport(
        {
            1: _counting_page([1, 2], 3),
            2: _counting_page([3], 3),
        },
        lambda page: request_threads.append((page, threading.current_thread())),
        page_limit=2,
    )
    sg = _client(transport, prefetch=2)

    assert [entity['id'] for entity in sg.find('Shot', [], limit=3)] == [1, 2, 3]
    assert {1, 2}.issubset(transport.read_pages)
    assert set(transport.read_pages).issubset({1, 2, 3})
    assert all(thread is not caller for _page, thread in request_threads)
    page_sizes: list[tuple[int, int]] = []
    for request in transport.requests:
        assert isinstance(request.body, bytes)
        payload = cast('dict[str, object]', json.loads(request.body))
        params = cast('list[object]', payload['params'])
        read_params = cast('dict[str, object]', params[1])
        paging = cast('dict[str, int]', read_params['paging'])
        page_sizes.append((paging['current_page'], paging['entities_per_page']))
    assert {page for page, _size in page_sizes} == set(transport.read_pages)
    assert all(size == 2 for _page, size in page_sizes)


def test_generator_close_cancels_queued_page_before_waiting_for_running_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shutdown_calls: list[tuple[bool, bool]] = []
    executor_type = driver_module.concurrent.futures.ThreadPoolExecutor

    queued_cancelled = threading.Event()
    cancel_future = Future[object].cancel

    def track_cancellation(future: Future[object]) -> bool:
        cancelled = cancel_future(future)
        if cancelled:
            queued_cancelled.set()
        return cancelled

    class SingleWorkerTrackingExecutor(executor_type):
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args
            thread_name_prefix = cast('str', kwargs['thread_name_prefix'])
            super().__init__(max_workers=1, thread_name_prefix=thread_name_prefix)

        def shutdown(
            self,
            wait: bool = True,
            *,
            cancel_futures: bool = False,
        ) -> None:
            shutdown_calls.append((wait, cancel_futures))
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    third_started = threading.Event()
    release_third = threading.Event()

    def coordinate(page: int) -> None:
        if page == 3:
            third_started.set()
            assert release_third.wait(5), 'running page was not released'

    responses = {
        1: _counting_page([1], 4),
        2: _counting_page([2], 4),
        3: _counting_page([3], 4),
        4: _counting_page([4], 4),
    }
    transport = _PageTransport(
        responses,
        coordinate,
    )
    monkeypatch.setattr(
        driver_module.concurrent.futures,
        'ThreadPoolExecutor',
        SingleWorkerTrackingExecutor,
    )
    monkeypatch.setattr(Future, 'cancel', track_cancellation)
    pages = Driver(transport).call_pages(
        lambda page: {
            'type': 'Shot',
            'paging': {'current_page': page, 'entities_per_page': 1},
        },
        1,
        'read',
        _context(3),
    )

    close_done = threading.Event()

    def close_pages() -> None:
        pages.close()
        close_done.set()

    closer = threading.Thread(target=close_pages)
    try:
        next(pages)
        next(pages)
        assert third_started.wait(5), 'running page did not start'
        closer.start()
        assert queued_cancelled.wait(5), 'queued page was not cancelled'
        assert not close_done.is_set()
    finally:
        release_third.set()
        if closer.ident is not None:
            closer.join(5)
        else:
            pages.close()

    assert not closer.is_alive()
    assert transport.read_pages == [1, 2, 3]
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert responses[4].close_calls == 0
    assert shutdown_calls == [(True, True)]


def test_generator_close_waits_for_running_page_cleanup_and_executor_shutdown() -> None:
    third_started = threading.Event()
    release_third = threading.Event()

    def coordinate(page: int) -> None:
        if page == 2:
            assert third_started.wait(5), 'page 3 did not start concurrently'
        elif page == 3:
            third_started.set()
            assert release_third.wait(5), 'page 3 was not released'

    responses = {
        1: _counting_page([1], 3),
        2: _counting_page([2], 3),
        3: _counting_page([3], 3),
    }
    transport = _PageTransport(
        responses,
        coordinate,
    )
    driver = Driver(transport)
    pages = driver.call_pages(
        lambda page: {
            'type': 'Shot',
            'paging': {'current_page': page, 'entities_per_page': 1},
        },
        1,
        'read',
        _context(2),
    )

    assert next(pages) == {
        'entities': [{'type': 'Shot', 'id': 1}],
        'paging_info': {'entity_count': 3},
    }
    assert next(pages) == {
        'entities': [{'type': 'Shot', 'id': 2}],
        'paging_info': {'entity_count': 3},
    }
    close_called = threading.Event()
    close_done = threading.Event()

    def close_pages() -> None:
        close_called.set()
        pages.close()
        close_done.set()

    closer = threading.Thread(target=close_pages)
    closer.start()
    assert close_called.wait(5), 'generator close did not start'
    assert not close_done.is_set()
    assert responses[3].close_calls == 0
    release_third.set()
    closer.join(5)

    assert not closer.is_alive()
    assert close_done.is_set()
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


@pytest.mark.parametrize(
    ('limit', 'page', 'expected_page'),
    [
        pytest.param(500, 0, 1, id='limit-at-page-size'),
        pytest.param(0, 3, 3, id='explicit-page'),
    ],
)
def test_known_single_page_find_uses_serial_call_pages(
    limit: int,
    page: int,
    expected_page: int,
) -> None:
    caller_thread = threading.current_thread()
    request_threads: list[threading.Thread] = []
    transport = _PageTransport(
        {expected_page: _counting_page([expected_page], 900 if page else 1)},
        lambda _page_number: request_threads.append(threading.current_thread()),
    )
    sg = _client(transport, prefetch=3)

    assert sg.find('Shot', [], limit=limit, page=page) == [{'type': 'Shot', 'id': expected_page}]

    assert request_threads == [caller_thread]
    assert transport.read_pages == [expected_page]


def test_later_page_builder_failure_waits_for_pending_cleanup() -> None:
    pending_started = {page: threading.Event() for page in (2, 3)}
    release_pending = threading.Event()

    def coordinate(page: int) -> None:
        if page in pending_started:
            pending_started[page].set()
            assert release_pending.wait(5), 'pending page was not released'

    def build_page(page: int) -> object:
        if page == 4:
            assert pending_started[2].wait(5), 'page 2 did not start'
            assert pending_started[3].wait(5), 'page 3 did not start'
            raise RuntimeError('later-builder-failed')
        return {
            'type': 'Shot',
            'paging': {'current_page': page, 'entities_per_page': 1},
        }

    transport = _PageTransport(
        {
            1: _counting_page([1], 3),
            2: _counting_page([2], 3),
            3: _counting_page([3], 3),
        },
        coordinate,
    )
    pages = Driver(transport).call_pages(build_page, 1, 'read', _context(3))
    first_result = cast('dict[str, object]', next(pages))
    assert first_result['entities'] == [{'type': 'Shot', 'id': 1}]
    errors: list[Exception] = []
    advance_done = threading.Event()

    def advance() -> None:
        try:
            next(pages)
        except (AssertionError, RuntimeError) as exc:
            errors.append(exc)
        finally:
            advance_done.set()

    worker = threading.Thread(target=advance)
    worker.start()
    assert pending_started[2].wait(5), 'page 2 did not start'
    assert pending_started[3].wait(5), 'page 3 did not start'
    assert not advance_done.is_set()
    release_pending.set()
    worker.join(5)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert str(errors[0]) == 'later-builder-failed'
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


def test_origin_change_retires_transport_only_after_prefetched_lease_finishes() -> None:
    old_started = {page: threading.Event() for page in (2, 3)}
    release_old = threading.Event()

    def coordinate_old(page: int) -> None:
        if page in old_started:
            old_started[page].set()
            assert release_old.wait(5), 'old request was not released'

    old_transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 1001),
            2: _counting_page(list(range(501, 1001)), 1001),
            3: _counting_page([1001], 1001),
        },
        coordinate_old,
    )
    new_transport = _PageTransport({1: _counting_page([99], 1)})
    sg = _TransportFactoryClient([old_transport, new_transport])
    sg.config.page_prefetch = 2
    first_result: list[dict[str, object]] = []
    errors: list[Exception] = []

    def run_first_find() -> None:
        try:
            first_result.extend(sg.find('Shot', []))
        except (AssertionError, RuntimeError, _errors.ShotgunError) as exc:
            errors.append(exc)

    first_worker = threading.Thread(target=run_first_find)
    first_worker.start()
    assert old_started[2].wait(5), 'old page 2 did not start'
    assert old_started[3].wait(5), 'old page 3 did not start'
    sg.config.set_server_params('https://changed.example')

    assert sg.find('Shot', [], limit=1) == [{'type': 'Shot', 'id': 99}]
    assert not old_transport.closed
    assert all(
        request.url == 'https://x.example:443/api3/json' for request in old_transport.requests
    )
    assert all(
        request.url == 'https://changed.example:443/api3/json'
        for request in new_transport.requests
    )

    release_old.set()
    first_worker.join(5)

    assert not first_worker.is_alive()
    assert not errors
    assert len(first_result) == 1001
    assert old_transport.closed
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in old_transport.returned_responses
    )
    sg.close()
    assert new_transport.closed


@pytest.mark.parametrize(
    ('limit', 'page', 'expected_page'),
    [
        (0, 0, 1),
        (1, 0, 1),
        (0, 3, 3),
    ],
)
def test_prefetch_does_not_speculate_beyond_a_first_page_stop(
    limit: int,
    page: int,
    expected_page: int,
) -> None:
    transport = _PageTransport({1: _page([1], 1), 3: _page([1], 900)})
    sg = _client(transport, prefetch=3)

    assert sg.find('Shot', [], limit=limit, page=page) == [{'type': 'Shot', 'id': 1}]

    assert transport.read_pages == [expected_page]


def test_prefetch_finishes_observers_and_response_cleanup_before_return() -> None:
    observed_pages = 0
    response_events = 0
    observer_lock = threading.Lock()

    def observe(event: object) -> None:
        nonlocal response_events
        if isinstance(event, ResponseEvent):
            with observer_lock:
                response_events += 1

    transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 501),
            2: _counting_page([501], 501),
            3: _counting_page([], 0),
        }
    )
    sg = _client(transport, prefetch=2)
    sg.config.on_response = observe

    result = sg.find('Shot', [])
    with observer_lock:
        observed_pages = response_events

    assert len(result) == 501
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert observed_pages == len(transport.read_pages) + 1
    assert sg._lease_counts == {}
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


def test_prefetch_never_requests_more_than_prefetch_minus_one_pages_past_final() -> None:
    release_second = threading.Event()
    later_started = {page: threading.Event() for page in (3, 4)}

    def coordinate(page: int) -> None:
        if page == 2:
            assert later_started[3].wait(5), 'page 3 did not start'
            assert later_started[4].wait(5), 'page 4 did not start'
            release_second.set()
        elif page in later_started:
            later_started[page].set()
            if page == 3:
                assert release_second.wait(5), 'page 2 did not finish coordination'

    transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 1002),
            2: _page(list(range(501, 1001)), 1002),
            3: _page([1001, 1002], 1002),
        },
        coordinate,
    )
    sg = _client(transport, prefetch=3)

    result = sg.find('Shot', [])

    assert len(result) == 1002
    assert set(transport.read_pages).issuperset({1, 2, 3, 4})
    assert max(transport.read_pages) <= 5


def test_prefetch_one_has_no_request_beyond_final_page() -> None:
    transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 501),
            2: _counting_page([501], 501),
        }
    )
    sg = _client(transport, prefetch=1)

    assert len(sg.find('Shot', [])) == 501

    assert transport.read_pages == [1, 2]
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )


def test_prefetch_propagates_worker_failure_after_prior_page_is_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    third_failed = threading.Event()
    verdict_record_counts: list[int] = []
    original_page_verdict = _protocol.page_verdict

    def record_page_verdict(
        paging_info: dict[str, object],
        entities_count: int,
        records_count: int,
        per_page: int,
        limit: int,
    ) -> str:
        if records_count == 1000:
            assert third_failed.is_set()
        verdict_record_counts.append(records_count)
        return original_page_verdict(
            paging_info,
            entities_count,
            records_count,
            per_page,
            limit,
        )

    def fail_third_first(page: int) -> None:
        if page == 2:
            assert third_failed.wait(5), 'page 3 did not fail concurrently'
        elif page == 3:
            third_failed.set()
            raise RuntimeError('page-three-failed')

    transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 1001),
            2: _counting_page(list(range(501, 1001)), 1001),
        },
        fail_third_first,
    )
    sg = _client(transport, prefetch=2)
    monkeypatch.setattr(_protocol, 'page_verdict', record_page_verdict)

    with pytest.raises(RuntimeError, match=r'^page-three-failed$'):
        sg.find('Shot', [])

    assert set(transport.read_pages[:3]) == {1, 2, 3}
    assert verdict_record_counts == [500, 1000]
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )
    assert sg._lease_counts == {}
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


def test_prefetch_returns_pages_in_order_after_out_of_order_completion() -> None:
    third_completed = threading.Event()
    completion_order: list[int] = []
    completion_lock = threading.Lock()

    def complete_out_of_order(page: int) -> None:
        if page == 2:
            assert third_completed.wait(5), 'page 3 did not run concurrently'
        with completion_lock:
            completion_order.append(page)
        if page == 3:
            third_completed.set()

    transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 1002),
            2: _page(list(range(501, 1001)), 1002),
            3: _page([1001, 1002], 1002),
            4: _page([], 0),
        },
        complete_out_of_order,
    )
    sg = _client(transport, prefetch=2)

    result = sg.find('Shot', [])

    assert [record['id'] for record in result] == list(range(1, 1003))
    assert completion_order.index(3) < completion_order.index(2)


def test_prefetch_two_has_exactly_one_request_beyond_final_page() -> None:
    third_started = threading.Event()

    def coordinate(page: int) -> None:
        if page == 2:
            assert third_started.wait(5), 'speculative page did not start'
        elif page == 3:
            third_started.set()

    transport = _PageTransport(
        {
            1: _counting_page(list(range(1, 501)), 501),
            2: _counting_page([501], 501),
            3: _counting_page([], 0),
        },
        coordinate,
    )
    sg = _client(transport, prefetch=2)

    assert len(sg.find('Shot', [])) == 501

    assert sorted(transport.read_pages) == [1, 2, 3]
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for response in transport.returned_responses
    )


def test_prefetch_uses_one_snapshot_during_concurrent_config_write() -> None:
    first_started = threading.Event()
    config_updated = threading.Event()

    def coordinate(page: int) -> None:
        if page == 1:
            first_started.set()
            assert config_updated.wait(5), 'configuration writer did not finish'

    transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 501),
            2: _page([501], 501),
        },
        coordinate,
    )
    sg = _client(transport, prefetch=2)

    def update_config() -> None:
        assert first_started.wait(5), 'first page did not start'
        sg.config.set_server_params('https://changed.example')
        sg.config.update_fields(page_prefetch=0, rpc_attempt_interval=99)
        config_updated.set()

    writer = threading.Thread(target=update_config)
    writer.start()
    result = sg.find('Shot', [])
    writer.join(5)

    assert not writer.is_alive()
    assert len(result) == 501
    assert {1, 2}.issubset(transport.read_pages)
    assert all(request.url == 'https://x.example:443/api3/json' for request in transport.requests)


def test_prefetch_zero_preserves_serial_page_order() -> None:
    transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 501),
            2: _page([501], 501),
        }
    )
    sg = _client(transport, prefetch=0)

    result = sg.find('Shot', [])

    assert [record['id'] for record in result] == list(range(1, 502))
    assert transport.read_pages == [1, 2]


@pytest.mark.parametrize(
    'paging_info',
    [
        pytest.param(
            [{'has_next_page': True}, {'has_next_page': False}],
            id='has-next-page',
        ),
        pytest.param(
            [dict[str, object](), dict[str, object]()],
            id='short-page',
        ),
    ],
)
def test_serial_and_prefetch_match_no_count_paging_modes(
    paging_info: list[dict[str, object]],
) -> None:
    def transport() -> _PageTransport:
        return _PageTransport(
            {
                1: _CountingResponse(
                    {
                        'entities': [
                            {'type': 'Shot', 'id': entity_id} for entity_id in range(1, 501)
                        ],
                        'paging_info': paging_info[0],
                    }
                ),
                2: _CountingResponse(
                    {
                        'entities': [{'type': 'Shot', 'id': 501}],
                        'paging_info': paging_info[1],
                    }
                ),
                3: _CountingResponse({'entities': [], 'paging_info': paging_info[1]}),
            }
        )

    serial_transport = transport()
    prefetch_transport = transport()
    serial_sg = _client(serial_transport, prefetch=0)
    prefetch_sg = _client(prefetch_transport, prefetch=2)

    serial_result = serial_sg.find('Shot', [], fields=['code'])
    prefetch_result = prefetch_sg.find('Shot', [], fields=['code'])

    assert prefetch_result == serial_result
    assert [record['id'] for record in prefetch_result] == list(range(1, 502))
    assert serial_transport.read_pages == [1, 2]
    assert {1, 2}.issubset(prefetch_transport.read_pages)
    assert set(prefetch_transport.read_pages).issubset({1, 2, 3})
    assert all(prefetch_transport.read_pages.count(page) == 1 for page in (1, 2))
    serial_bodies = _request_bodies_by_page(serial_transport)
    prefetch_bodies = _request_bodies_by_page(prefetch_transport)
    assert {page: prefetch_bodies[page] for page in serial_bodies} == serial_bodies
    assert all(len(bodies) == 1 for bodies in prefetch_bodies.values())
    assert all(
        cast('_CountingResponse', response).close_calls == 1
        for page_transport in (serial_transport, prefetch_transport)
        for response in page_transport.returned_responses
    )
    assert serial_sg._lease_counts == {}
    assert prefetch_sg._lease_counts == {}
    assert not any(
        thread.name.startswith('shotgun-api3-pages') for thread in threading.enumerate()
    )


def test_serial_and_prefetch_use_exact_per_page_request_transcripts() -> None:
    third_started = threading.Event()
    fourth_started = threading.Event()

    def coordinate_prefetch(page: int) -> None:
        if page == 2:
            assert third_started.wait(5), 'page 3 did not start concurrently'
        elif page == 3:
            third_started.set()
            assert fourth_started.wait(5), 'page 4 did not start speculatively'
        elif page == 4:
            fourth_started.set()

    serial_transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 1001),
            2: _page(list(range(501, 1001)), 1001),
            3: _page([1001], 1001),
        }
    )
    prefetch_transport = _PageTransport(
        {
            1: _page(list(range(1, 501)), 1001),
            2: _page(list(range(501, 1001)), 1001),
            3: _page([1001], 1001),
            4: _page([], 0),
        },
        coordinate_prefetch,
    )

    serial_result = _client(serial_transport, prefetch=0).find(
        'Shot',
        [],
        fields=['code'],
        include_archived_projects=False,
    )
    prefetch_result = _client(prefetch_transport, prefetch=2).find(
        'Shot',
        [],
        fields=['code'],
        include_archived_projects=False,
    )

    assert prefetch_result == serial_result
    serial_bodies = _request_bodies_by_page(serial_transport)
    prefetch_bodies = _request_bodies_by_page(prefetch_transport)
    assert sorted(serial_bodies) == [1, 2, 3]
    assert sorted(prefetch_bodies) == [1, 2, 3, 4]
    assert {page: prefetch_bodies[page] for page in serial_bodies} == serial_bodies
    assert all(len(bodies) == 1 for bodies in prefetch_bodies.values())
