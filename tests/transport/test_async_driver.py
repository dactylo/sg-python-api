import asyncio
import dataclasses
import gc
import json
import threading
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator, Mapping
from typing import Never, cast, get_type_hints

import pytest

from shotgun_api3 import _async_driver, _errors, _protocol
from shotgun_api3 import _config as config_module
from shotgun_api3._async_driver import (
    _DETACHED_CLEANUP_TASKS,
    AsyncAttemptControl,
    AsyncDriver,
    AsyncExecutionOptions,
    AsyncLiveResponse,
    _AsyncDriverTiming,
    _close_request_body_after_send,
)
from shotgun_api3._async_httpx2 import _ThreadedBody
from shotgun_api3._async_transport import (
    AsyncStreamingResponse,
    AsyncTransport,
)
from shotgun_api3._config import (
    CompatibilityWarningEvent,
    Config,
    ObserverEvent,
    ProgressEvent,
    RequestEvent,
    ResponseEvent,
    RetryEvent,
)
from shotgun_api3._driver import Driver, _DriverTiming
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    ConnectError,
    ReadTimeoutError,
    SendError,
    TimeoutConfig,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _AsyncFakeTransport, _FakeTransport

pytestmark = pytest.mark.async_required

_BLOCKED_UPLOAD_REQUEST_ID = 'blocked-upload-token'
_CANCELLED_UPLOAD_REQUEST_ID = 'cancelled-upload-token'
_GATEWAY = TransportResponse(status=502, reason='Bad Gateway', headers={}, content=b'')

_LOOP_DEADLINE_REQUEST_ID = 'loop-deadline'
_LOOP_OBSERVER_FAILURE_REQUEST_ID = 'loop-observer-failure'
_LOOP_PROGRESS_REQUEST_ID = 'loop-progress'
_OK = TransportResponse(
    status=200,
    reason='OK',
    headers={'content-type': 'application/json'},
    content=b'{"results": {"id": 7}}',
)

_PENDING_BODY_CLEANUP_CANCELLATION = 'pending body cleanup cancellation'
_PENDING_BODY_CLEANUP_REQUEST_ID = 'cancel-before-body-cleanup'
_PENDING_RESPONSE_CLEANUP_CANCELLATION = 'pending response cleanup cancellation'
_POST_RESPONSE_CLEANUP_REQUEST_ID = 'post-response-cleanup'
_POST_RESPONSE_CLOSE_FAILURE_REQUEST_ID = 'post-response-close-failure'


class _AsyncCloseResponse:
    def __init__(
        self,
        close_error: BaseException | None = None,
        status: int = 200,
        *,
        close_headers: dict[str, str] | None = None,
        close_status: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.close_calls = 0
        self.content = b'data'
        self.headers = CaseInsensitiveHeaders(headers or {})
        self.reason = 'OK'
        self.status = status
        self._close_error = close_error
        self._close_headers = close_headers
        self._close_status = close_status

    async def close(self) -> None:
        self.close_calls += 1
        if self._close_headers is not None:
            self.headers = CaseInsensitiveHeaders(self._close_headers)
        if self._close_status is not None:
            self.status = self._close_status
        if self._close_error is not None:
            raise self._close_error


class _BlockingAsyncCloseResponse(_AsyncCloseResponse):
    def __init__(self) -> None:
        super().__init__()
        self.close_finished = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_started = asyncio.Event()

    async def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        await self.close_release.wait()
        self.close_finished.set()


class _BlockingBody:
    def __init__(self) -> None:
        self.close_calls = 0
        self.close_raced = False
        self.closed = threading.Event()
        self.finished = threading.Event()
        self.release = threading.Event()
        self.started = threading.Event()

    def __iter__(self) -> '_BlockingBody':
        return self

    def __next__(self) -> bytes:
        self.started.set()
        self.release.wait(timeout=5.0)
        self.finished.set()
        return b'body'

    def close(self) -> None:
        self.close_calls += 1
        self.close_raced = not self.finished.is_set()
        self.closed.set()


class _Body:
    def __init__(self) -> None:
        self.close_calls = 0
        self.closed = threading.Event()
        self._chunks = iter((b'body',))

    def __iter__(self) -> '_Body':
        return self

    def __next__(self) -> bytes:
        return next(self._chunks)

    def close(self) -> None:
        self.close_calls += 1
        self.closed.set()


class _BlockingCloseBody(_Body):
    def __init__(self) -> None:
        super().__init__()
        self.close_finished = threading.Event()
        self.close_release = threading.Event()
        self.close_started = threading.Event()

    def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        self.close_release.wait(timeout=5.0)
        self.close_finished.set()


class _DescriptorCloseBody(Iterator[bytes]):
    def __init__(self, lookup_error: BaseException | None = None) -> None:
        self.callback_threads: list[int] = []
        self.lookup_threads: list[int] = []
        self._lookup_error = lookup_error

    def __iter__(self) -> '_DescriptorCloseBody':
        return self

    def __next__(self) -> bytes:
        raise StopIteration

    @property
    def close(self) -> Callable[[], None]:
        self.lookup_threads.append(threading.get_ident())
        if self._lookup_error is not None:
            raise self._lookup_error

        def callback() -> None:
            self.callback_threads.append(threading.get_ident())

        return callback


class _FailingCloseBody(_Body):
    def close(self) -> None:
        self.close_calls += 1
        raise RuntimeError('body close failed')


class _HangingTransport(_AsyncFakeTransport):
    def __init__(self) -> None:
        super().__init__([])
        self.started = asyncio.Event()

    async def send(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError('hanging send resumed')

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        del request
        raise AssertionError('unexpected streaming send')


class _LoopPullingTransport(_AsyncFakeTransport):
    def __init__(self) -> None:
        super().__init__([_OK])
        self.returned_chunks: list[bytes] = []

    async def send(self, request: TransportRequest) -> TransportResponse:
        body = request.body
        assert body is not None
        assert not isinstance(body, bytes)
        adapted = _ThreadedBody(body)
        try:
            async for chunk in adapted:
                self.returned_chunks.append(chunk)
        finally:
            await adapted.aclose()
        return await super().send(request)


class _PullingHangingTransport(_HangingTransport):
    async def send(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        body = request.body
        assert body is not None
        assert not isinstance(body, bytes)
        pull = asyncio.create_task(asyncio.to_thread(next, body))
        pull.add_done_callback(lambda task: task.exception())
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError('hanging send resumed')


class _RedirectBody:
    def __init__(self, closes: list[str], label: str) -> None:
        self._closes = closes
        self._label = label

    def __iter__(self) -> '_RedirectBody':
        return self

    def __next__(self) -> bytes:
        raise StopIteration

    def close(self) -> None:
        self._closes.append(self._label)


def _config() -> Config:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(script_name='script', api_key='key')
    return config


def _raise_ambient_exception() -> Never:
    raise RuntimeError('ambient exception state')


def _request(token: str) -> TransportRequest:
    context = config_module.snapshot(_config())
    return TransportRequest(
        method='GET',
        url='https://x.example/file',
        headers={},
        body=None,
        timeout=context.timeout,
        token=token,
    )


async def _sleepless(_delay: float) -> None:
    return None


@pytest.mark.anyio
async def test_already_active_cancellation_joins_response_cleanup_once() -> None:
    response = _BlockingAsyncCloseResponse()
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def cancel_before_cleanup(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel(_PENDING_RESPONSE_CLEANUP_CANCELLATION)
        await asyncio.sleep(0)

    task = asyncio.create_task(
        driver.execute(
            operation,
            context,
            lambda _attempt: _request('cancel-before-response-cleanup'),
            cancel_before_cleanup,
        )
    )

    try:
        await asyncio.wait_for(response.close_started.wait(), timeout=1.0)
        assert not response.close_finished.is_set()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        response.close_release.set()
        await asyncio.wait_for(response.close_finished.wait(), timeout=1.0)

    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert caught.value.args == (_PENDING_RESPONSE_CLEANUP_CANCELLATION,)
    assert response.close_calls == 1
    assert (fake.leases, fake.checkins) == (1, 1)
    assert set() == _DETACHED_CLEANUP_TASKS


@pytest.mark.anyio
@pytest.mark.parametrize('chunks', [(), (b'', b'a', b'', b'bc')])
async def test_attempt_control_collects_empty_and_multiple_chunks(
    chunks: tuple[bytes, ...],
) -> None:
    clock_calls = 0
    events: list[ObserverEvent] = []

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 1.0

    control = AsyncAttemptControl(2, clock, 2.0, events.append, 'read', 'rpc')

    async def stream() -> AsyncIterator[bytes]:
        for chunk in chunks:
            yield chunk

    consumed = await control.consume(stream(), total_bytes=3)

    assert type(consumed) is bytes
    assert consumed == b''.join(chunks)
    assert clock_calls == len(chunks)
    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == ([1, 3] if chunks else [])
    assert [event.total_bytes for event in progress] == ([3, 3] if chunks else [])


@pytest.mark.anyio
@pytest.mark.parametrize('body', [b'data', b''])
async def test_attempt_control_consumes_buffered_body_without_copy(body: bytes) -> None:
    clock_calls = 0
    events: list[ObserverEvent] = []

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 1.0

    control = AsyncAttemptControl(
        _attempt=2,
        _clock=clock,
        _deadline=2.0,
        _observer=events.append,
        _operation='read',
        _route='rpc',
    )

    consumed = await control._consume_buffered(body, total_bytes=len(body))

    assert consumed is body
    assert clock_calls == 1
    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == ([len(body)] if body else [])
    assert [event.total_bytes for event in progress] == ([len(body)] if body else [])


@pytest.mark.anyio
async def test_async_request_factory_failure_releases_lease_without_sending() -> None:
    fake = _AsyncFakeTransport([])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def request_factory(_attempt: int) -> TransportRequest:
        raise ValueError('factory failed')

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    with pytest.raises(ValueError, match='factory failed'):
        await driver.execute(operation, context, request_factory, consume)

    assert fake.requests == []
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_async_rpc_nested_extra_auth_matches_sync_payload() -> None:
    config = _config()
    config.extra_auth_params = {
        'nested': {
            'roles': ['artist', {'name': 'lead'}],
        },
    }
    context = config_module.snapshot(config)
    sync_fake = _FakeTransport([_OK])
    async_fake = _AsyncFakeTransport([_OK])

    assert Driver(sync_fake, _DriverTiming(sleep=lambda _delay: None)).call(
        'read',
        {},
        context,
    ) == {'id': 7}
    assert await AsyncDriver(async_fake, _AsyncDriverTiming(sleep=_sleepless)).call(
        'read',
        {},
        context,
    ) == {'id': 7}

    sync_body = sync_fake.requests[0].body
    async_body = async_fake.requests[0].body
    assert isinstance(sync_body, bytes)
    assert isinstance(async_body, bytes)
    assert json.loads(async_body) == json.loads(sync_body)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('method_name', 'params', 'auth_fields', 'category', 'retries'),
    [
        (
            'authenticate_human_user',
            {},
            {'script_name': 'script', 'api_key': 'key'},
            'human_authentication',
            True,
        ),
        (
            'get_session_token',
            {},
            {'user_login': 'artist', 'user_password': 'secret'},
            'session_authentication',
            True,
        ),
        (
            'get_session_token',
            {},
            {
                'user_login': 'artist',
                'user_password': 'secret',
                'auth_token': 'one-use',
            },
            'one_use_auth_exchange',
            False,
        ),
        (
            'future_method',
            {},
            {'script_name': 'script', 'api_key': 'key'},
            'unknown',
            False,
        ),
    ],
)
async def test_auth_and_unknown_categories_drive_retry_and_observer_choreography(
    method_name: str,
    params: object,
    auth_fields: dict[str, object],
    category: _protocol.OperationCategory,
    retries: bool,
) -> None:
    events: list[ObserverEvent] = []
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        **auth_fields,
        max_rpc_attempts=3,
        on_progress=events.append,
        on_request=events.append,
        on_response=events.append,
        on_retry=events.append,
    )
    fake = _AsyncFakeTransport([SendError('ambiguous'), _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    if retries:
        assert await driver.call(method_name, params, config_module.snapshot(config)) == {'id': 7}
    else:
        with pytest.raises(SendError, match='ambiguous'):
            await driver.call(method_name, params, config_module.snapshot(config))

    expected_types = (
        [RequestEvent, ResponseEvent, RetryEvent, RequestEvent, ProgressEvent, ResponseEvent]
        if retries
        else [RequestEvent, ResponseEvent]
    )
    assert [type(event) for event in events] == expected_types
    assert all(
        isinstance(event, (ProgressEvent, RequestEvent, ResponseEvent, RetryEvent))
        and event.operation == category
        for event in events
    )
    assert len(fake.requests) == (2 if retries else 1)


@pytest.mark.anyio
async def test_batch_retries_only_when_every_member_is_replay_safe() -> None:
    safe = {'requests': [{'request_type': 'read'}, {'request_type': 'read'}]}
    unsafe = {'requests': [{'request_type': 'read'}, {'request_type': 'create'}]}

    safe_fake = _AsyncFakeTransport([SendError('ambiguous'), _OK])
    safe_driver = AsyncDriver(safe_fake, _AsyncDriverTiming(sleep=_sleepless))
    assert await safe_driver.call('batch', safe, config_module.snapshot(_config())) == {'id': 7}
    assert len(safe_fake.requests) == 2

    unsafe_fake = _AsyncFakeTransport([SendError('ambiguous'), _OK])
    unsafe_driver = AsyncDriver(unsafe_fake, _AsyncDriverTiming(sleep=_sleepless))
    with pytest.raises(SendError, match='ambiguous'):
        await unsafe_driver.call('batch', unsafe, config_module.snapshot(_config()))
    assert len(unsafe_fake.requests) == 1


@pytest.mark.anyio
async def test_call_routes_original_body_through_one_response_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UndecodableBytes(bytes):
        def decode(self, encoding: str = 'utf-8', errors: str = 'strict') -> str:
            del encoding, errors
            raise AssertionError('RPC status checking decoded the response body')

    body = UndecodableBytes(b'{"results": {"id": 7}}')
    decoded_bodies: list[bytes] = []

    def decode_response(_headers: Mapping[str, str], received: bytes) -> object:
        decoded_bodies.append(received)
        return {'results': {'id': 7}}

    monkeypatch.setattr(_protocol, 'decode_response', decode_response)
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=body,
    )

    assert await AsyncDriver(
        _AsyncFakeTransport([response]),
        _AsyncDriverTiming(sleep=_sleepless),
    ).call('info', {}, config_module.snapshot(_config())) == {'id': 7}
    assert decoded_bodies == [body]
    assert decoded_bodies[0] is body


@pytest.mark.anyio
async def test_call_detaches_nested_input_before_request_and_retry_observers() -> None:
    values = ['original']
    params = {'type': 'Shot', 'nested': {'values': values}}
    config = _config()
    config.on_request = lambda _event: values.append('request mutation')
    config.on_retry = lambda _event: values.append('retry mutation')
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    assert await driver.call('read', params, config_module.snapshot(config)) == {'id': 7}

    attempts = fake.requests
    assert len(attempts) == 2
    assert 'request mutation' in values
    assert 'retry mutation' in values
    for request in attempts:
        assert isinstance(request.body, bytes)
        assert json.loads(request.body)['params'][1] == {
            'type': 'Shot',
            'nested': {'values': ['original']},
        }
    assert attempts[0].body == attempts[1].body
    assert attempts[0].body is not attempts[1].body
    assert attempts[0].token != attempts[1].token


@pytest.mark.anyio
async def test_call_publishes_shared_compatibility_warning_sequence() -> None:
    events: list[ObserverEvent] = []
    config = _config()
    config.update_fields(timeout_secs=None, on_compatibility_warning=events.append)
    driver = AsyncDriver(_AsyncFakeTransport([_OK]), _AsyncDriverTiming(sleep=_sleepless))

    assert await driver.call(
        'info',
        {},
        config_module.snapshot(config),
        compatibility_warning_codes=('deprecated-schema-alias',),
    ) == {'id': 7}

    warnings = [cast('CompatibilityWarningEvent', event) for event in events]
    assert [event.code for event in warnings] == [
        'deprecated-schema-alias',
        'legacy-infinite-timeout',
    ]


@pytest.mark.anyio
async def test_cancellation_closes_request_body_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _HangingTransport()
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)
    body = _Body()
    cleanup_worker_release = threading.Event()
    cleanup_worker_started = threading.Event()
    to_thread = asyncio.to_thread

    async def gated_to_thread(
        function: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        def gated_call() -> object:
            cleanup_worker_started.set()
            assert cleanup_worker_release.wait(timeout=5.0)
            return function(*args, **kwargs)

        return await to_thread(gated_call)

    monkeypatch.setattr(_async_driver.asyncio, 'to_thread', gated_to_thread)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    task = asyncio.create_task(
        driver.execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body,
                timeout=context.timeout,
                token=_CANCELLED_UPLOAD_REQUEST_ID,
            ),
            consume,
        )
    )
    await transport.started.wait()
    task.cancel()

    try:
        done, _pending = await asyncio.wait({task}, timeout=1.0)
        assert done == {task}
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.wait_for(
            to_thread(cleanup_worker_started.wait),
            timeout=1.0,
        )
        assert not body.closed.is_set()
        assert len(transport.requests) == 1
        assert (transport.leases, transport.checkins) == (1, 1)
        assert len(_DETACHED_CLEANUP_TASKS) == 1
    finally:
        cleanup_worker_release.set()
        cleanup_tasks = _DETACHED_CLEANUP_TASKS
        if cleanup_tasks:
            await asyncio.wait_for(
                asyncio.gather(*cleanup_tasks, return_exceptions=True),
                timeout=1.0,
            )

    assert await asyncio.wait_for(to_thread(body.closed.wait), timeout=1.0)
    assert body.close_calls == 1
    assert set() == _DETACHED_CLEANUP_TASKS


@pytest.mark.anyio
async def test_cancellation_during_backoff_releases_lease_without_another_attempt() -> None:
    started = asyncio.Event()
    response_closed: list[str] = []
    retry = TransportResponse(
        status=502,
        reason='Bad Gateway',
        headers={},
        content=b'',
        on_close=lambda: response_closed.append('close'),
    )
    fake = _AsyncFakeTransport([retry, _OK])

    async def sleep(_delay: float) -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        AsyncDriver(fake, _AsyncDriverTiming(sleep=sleep)).call(
            'info',
            {},
            config_module.snapshot(_config()),
        )
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert response_closed == ['close']
    assert len(fake.requests) == 1
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_cancellation_during_post_response_body_cleanup_closes_response_once(
    recwarn: pytest.WarningsRecorder,
) -> None:
    body = _BlockingCloseBody()
    response = _AsyncCloseResponse()
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    task = asyncio.create_task(
        AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless)).execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body,
                timeout=context.timeout,
                token=_POST_RESPONSE_CLEANUP_REQUEST_ID,
            ),
            consume,
        )
    )
    assert await asyncio.to_thread(body.close_started.wait, 1.0)

    try:
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.1)
        assert done == {task}
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        gc.collect()
        cleanup_tasks = _DETACHED_CLEANUP_TASKS
        assert len(cleanup_tasks) == 1
        cleanup_done = asyncio.Event()
        next(iter(cleanup_tasks)).add_done_callback(lambda _task: cleanup_done.set())
        assert response.close_calls == 1
        assert not body.close_finished.is_set()
        assert (fake.leases, fake.checkins) == (1, 1)
    finally:
        body.close_release.set()
        assert await asyncio.to_thread(body.close_finished.wait, 1.0)

    assert body.close_calls == 1
    assert response.close_calls == 1
    await asyncio.wait_for(cleanup_done.wait(), timeout=1.0)
    assert set() == _DETACHED_CLEANUP_TASKS
    assert not recwarn.list


@pytest.mark.anyio
@pytest.mark.parametrize('selected_outcome', ['success', 'retry', 'consumer-exception'])
async def test_cancellation_retains_blocked_response_cleanup(
    selected_outcome: str,
) -> None:
    response = _BlockingAsyncCloseResponse()
    if selected_outcome == 'retry':
        response.status = 502
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def select_outcome(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> dict[str, int]:
        if selected_outcome == 'consumer-exception':
            raise ValueError('consumer failed')
        return {'id': 7}

    task = asyncio.create_task(
        driver.execute(
            operation,
            context,
            lambda _attempt: _request('blocked-response-cleanup'),
            select_outcome,
        )
    )
    await response.close_started.wait()

    task.cancel('first cancellation')
    await asyncio.sleep(0)
    task.cancel('second cancellation')
    await asyncio.sleep(0)
    assert not task.done()
    response.close_release.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert caught.value.args == ('first cancellation',)
    assert response.close_calls == 1
    assert response.close_finished.is_set()
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'
    assert (fake.leases, fake.checkins) == (1, 1)
    assert set() == _DETACHED_CLEANUP_TASKS


@pytest.mark.anyio
async def test_cancellation_survives_failing_async_response_cleanup() -> None:
    response = _AsyncCloseResponse(RuntimeError('close failed'))
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def cancel(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await driver.execute(
            operation,
            context,
            lambda _attempt: _request('cancel-response'),
            cancel,
        )

    await asyncio.sleep(0)
    assert response.close_calls == 1
    assert (fake.leases, fake.checkins) == (1, 1)
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'


@pytest.mark.anyio
async def test_cancelled_blocked_body_pull_cleans_up_without_racing_or_delaying() -> None:
    transport = _PullingHangingTransport()
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)
    body = _BlockingBody()

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    task = asyncio.create_task(
        driver.execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body,
                timeout=context.timeout,
                token=_BLOCKED_UPLOAD_REQUEST_ID,
            ),
            consume,
        )
    )
    await transport.started.wait()
    assert await asyncio.to_thread(body.started.wait, 1.0)
    task.cancel()

    done, _pending = await asyncio.wait({task}, timeout=0.1)
    assert done == {task}
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not body.closed.is_set()

    body.release.set()
    assert await asyncio.to_thread(body.closed.wait, 1.0)
    assert body.finished.is_set()
    assert body.close_calls == 1
    assert body.close_raced is False
    assert (transport.leases, transport.checkins) == (1, 1)


@pytest.mark.anyio
async def test_close_failure_preserves_success_metadata_and_event() -> None:
    response = _AsyncCloseResponse(
        RuntimeError('secret backend close text'),
        close_headers={'x-sg-correlation-id': 'mutated-correlation'},
        close_status=599,
        headers={'x-sg-correlation-id': 'selected-correlation'},
    )
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'complete'

    try:
        _raise_ambient_exception()
    except RuntimeError:
        result = await driver.execute(
            operation,
            context,
            lambda _attempt: _request('close-failure'),
            consume,
        )

    assert result == 'complete'
    assert response.close_calls == 1
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'success'
    assert response_events[0].status == 200
    assert response_events[0].correlation_id == 'selected-correlation'
    assert 'secret backend close text' not in repr(events)


@pytest.mark.anyio
async def test_close_failures_preserve_retries_and_last_result() -> None:
    responses = [
        _AsyncCloseResponse(RuntimeError('secret backend close text'), status=status)
        for status in (503, 503, 200)
    ]
    fake = _AsyncFakeTransport([cast('TransportResponse', response) for response in responses])
    events: list[ObserverEvent] = []
    config = _config()
    config.update_fields(
        max_rpc_attempts=3,
        on_response=events.append,
        on_retry=events.append,
        rpc_attempt_interval=0,
    )
    context = config_module.snapshot(config)
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> dict[str, int]:
        return {'id': 7}

    try:
        _raise_ambient_exception()
    except RuntimeError:
        result = await driver.execute(
            operation,
            context,
            lambda attempt: _request(f'close-failure-retry-{attempt}'),
            consume,
        )

    assert result == {'id': 7}
    assert len(fake.requests) == 3
    assert [response.close_calls for response in responses] == [1, 1, 1]
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert [event.outcome for event in response_events] == ['retry', 'retry', 'success']
    assert len([event for event in events if isinstance(event, RetryEvent)]) == 2
    assert 'secret backend close text' not in repr(events)


@pytest.mark.anyio
async def test_common_executor_streams_download_and_closes_response_once() -> None:
    closed: list[str] = []
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-length': '4'},
        content=b'data',
        on_close=lambda: closed.append('close'),
    )
    fake = _AsyncFakeTransport([response])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def consume(
        live_response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> bytes:
        streaming = cast('AsyncStreamingResponse', live_response)
        return await control.consume(streaming.aiter_chunks(2), total_bytes=4)

    result = await driver.execute(
        operation,
        context,
        lambda attempt: _request(f'download-{attempt}'),
        consume,
        options=AsyncExecutionOptions(streaming=True),
    )

    assert result == b'data'
    assert closed == ['close']
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_consumer_failure_survives_close_failure_with_one_error_event() -> None:
    response = _AsyncCloseResponse(RuntimeError('close failed'))
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise ValueError('consumer failed')

    with pytest.raises(ValueError, match='consumer failed'):
        await driver.execute(
            operation,
            context,
            lambda _attempt: _request('consumer-failure'),
            consume,
        )

    assert response.close_calls == 1
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'


@pytest.mark.anyio
async def test_control_consumes_stream_into_async_sink_with_progress() -> None:
    events: list[ObserverEvent] = []
    config = _config()
    config.on_progress = events.append
    context = config_module.snapshot(config)
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-length': '4'},
        content=b'data',
    )
    fake = _AsyncFakeTransport([response])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('download', body_restartable=True)
    chunks: list[bytes] = []

    async def sink(chunk: bytes) -> None:
        await asyncio.sleep(0)
        chunks.append(chunk)

    async def consume(
        live_response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> str:
        streaming = cast('AsyncStreamingResponse', live_response)
        await control.consume_into(
            streaming.aiter_chunks(2),
            sink,
            total_bytes=4,
        )
        return 'written'

    assert (
        await driver.execute(
            operation,
            context,
            lambda _attempt: _request('async-sink'),
            consume,
            options=AsyncExecutionOptions(streaming=True),
        )
        == 'written'
    )

    assert chunks == [b'da', b'ta']
    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == [2, 4]


@pytest.mark.anyio
async def test_download_retries_body_read_transport_error() -> None:
    fake = _AsyncFakeTransport([_OK, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('download', body_restartable=True)
    attempts = 0

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> bytes:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ReadTimeoutError('body timed out')
        return b'data'

    assert (
        await driver.execute(
            operation,
            context,
            lambda attempt: _request(f'download-{attempt}'),
            consume,
        )
        == b'data'
    )
    assert len(fake.requests) == 2


def test_driver_init_runtime_hints_resolve() -> None:
    hints = get_type_hints(AsyncDriver.__init__)

    assert hints['transport'] is AsyncTransport


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('endpoint_role', 'retries'),
    [
        ('read', True),
        ('download', True),
        ('object_put', True),
        ('part_put', True),
        ('allocation', False),
        ('link', False),
        ('finalize', False),
    ],
)
async def test_endpoint_role_controls_ambiguous_send_replay(
    endpoint_role: _protocol._EndpointRole,
    retries: bool,
) -> None:
    fake = _AsyncFakeTransport([SendError('ambiguous'), _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint(endpoint_role, body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'ok'

    if retries:
        assert (
            await driver.execute(
                operation,
                context,
                lambda attempt: _request(f'endpoint-{attempt}'),
                consume,
            )
            == 'ok'
        )
        assert len(fake.requests) == 2
    else:
        with pytest.raises(SendError, match='ambiguous'):
            await driver.execute(
                operation,
                context,
                lambda attempt: _request(f'endpoint-{attempt}'),
                consume,
            )
        assert len(fake.requests) == 1


@pytest.mark.anyio
async def test_executor_builds_fresh_request_per_attempt() -> None:
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('download', body_restartable=True)
    built: list[int] = []

    def request_factory(attempt: int) -> TransportRequest:
        built.append(attempt)
        return _request(f'async-{attempt}')

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'downloaded'

    assert (
        await driver.execute(
            operation,
            context,
            request_factory,
            consume,
        )
        == 'downloaded'
    )

    assert built == [1, 2]
    assert [request.token for request in fake.requests] == ['async-1', 'async-2']


@pytest.mark.anyio
async def test_executor_rebuilds_and_closes_upload_body_per_attempt() -> None:
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)
    bodies: list[_Body] = []

    def request_factory(attempt: int) -> TransportRequest:
        body = _Body()
        bodies.append(body)
        return TransportRequest(
            method='PUT',
            url='https://storage.example/object',
            headers={},
            body=body,
            timeout=context.timeout,
            token=f'upload-{attempt}',
        )

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'uploaded'

    assert await driver.execute(operation, context, request_factory, consume) == 'uploaded'

    assert len(bodies) == 2
    assert bodies[0] is not bodies[1]
    assert [body.close_calls for body in bodies] == [1, 1]


@pytest.mark.anyio
async def test_form_post_retries_through_common_executor() -> None:
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('read', body_restartable=True)

    def request_factory(attempt: int) -> TransportRequest:
        return TransportRequest(
            method='POST',
            url='https://x.example/upload/form',
            headers={'content-type': 'application/x-www-form-urlencoded'},
            body=f'attempt={attempt}'.encode(),
            timeout=context.timeout,
            token=f'form-{attempt}',
        )

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'form-result'

    assert (
        await driver.execute(
            operation,
            context,
            request_factory,
            consume,
        )
        == 'form-result'
    )
    assert [request.method for request in fake.requests] == ['POST', 'POST']
    assert [request.token for request in fake.requests] == ['form-1', 'form-2']


@pytest.mark.anyio
async def test_logical_method_controls_retry_without_changing_wire_method() -> None:
    fake = _AsyncFakeTransport([SendError('ambiguous')])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    with pytest.raises(SendError, match='ambiguous'):
        await driver.call(
            'read',
            {'type': 'Shot'},
            config_module.snapshot(_config()),
            _logical_method_name='create',
        )

    body = fake.requests[0].body
    assert isinstance(body, bytes)
    assert b'"method_name": "read"' in body
    assert len(fake.requests) == 1


@pytest.mark.anyio
async def test_nonrestartable_body_never_retries_presend_failure() -> None:
    fake = _AsyncFakeTransport([ConnectError('refused'), _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=False)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'ok'

    with pytest.raises(ConnectError, match='refused'):
        await driver.execute(
            operation,
            context,
            lambda attempt: _request(f'upload-{attempt}'),
            consume,
        )

    assert len(fake.requests) == 1


@pytest.mark.anyio
async def test_observer_failures_are_sanitized_and_noninvasive(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(_event: ObserverEvent) -> None:
        raise RuntimeError('observer-secret')

    config = _config()
    config.update_fields(
        on_progress=fail,
        on_request=fail,
        on_response=fail,
        on_retry=fail,
    )
    fake = _AsyncFakeTransport([_GATEWAY, _OK])

    with caplog.at_level('WARNING', logger='shotgun_api3'):
        assert await AsyncDriver(
            fake,
            _AsyncDriverTiming(sleep=_sleepless),
        ).call('info', {}, config_module.snapshot(config)) == {'id': 7}

    assert len(fake.requests) == 2
    assert 'observer-secret' not in caplog.text


@pytest.mark.anyio
async def test_observers_record_retry_progress_and_fresh_rpc_attempts() -> None:
    events: list[ObserverEvent] = []
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    config = _config()
    config.update_fields(
        on_progress=events.append,
        on_request=events.append,
        on_response=events.append,
        on_retry=events.append,
    )
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    timing = _AsyncDriverTiming(jitter_seed=lambda: 1000.25, sleep=sleep)

    assert await AsyncDriver(fake, timing).call('info', {}, config_module.snapshot(config)) == {
        'id': 7
    }

    assert [type(event) for event in events] == [
        RequestEvent,
        ResponseEvent,
        RetryEvent,
        RequestEvent,
        ProgressEvent,
        ResponseEvent,
    ]
    assert len(sleeps) == 1
    assert len({request.token for request in fake.requests}) == 2
    assert fake.requests[0].body == fake.requests[1].body
    assert fake.requests[0].body is not fake.requests[1].body


@pytest.mark.anyio
async def test_pending_caller_cancellation_replaces_send_error_before_body_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _HangingTransport()
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)
    body = _BlockingCloseBody()

    async def cancel_then_fail(request: TransportRequest) -> TransportResponse:
        transport.requests.append(request)
        current = asyncio.current_task()
        assert current is not None
        current.cancel(_PENDING_BODY_CLEANUP_CANCELLATION)
        raise SendError('send failed')

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    monkeypatch.setattr(transport, 'send', cancel_then_fail)
    task = asyncio.create_task(
        driver.execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body,
                timeout=context.timeout,
                token=_PENDING_BODY_CLEANUP_REQUEST_ID,
            ),
            consume,
        )
    )

    try:
        with pytest.raises(asyncio.CancelledError) as exc_info:
            await task
        assert exc_info.value.args == (_PENDING_BODY_CLEANUP_CANCELLATION,)
        cleanup_tasks = _DETACHED_CLEANUP_TASKS
        assert len(cleanup_tasks) == 1
        cleanup_done = asyncio.Event()
        next(iter(cleanup_tasks)).add_done_callback(lambda _task: cleanup_done.set())
        assert not body.close_finished.is_set()
    finally:
        assert await asyncio.to_thread(body.close_started.wait, 1.0)
        body.close_release.set()
        assert await asyncio.to_thread(body.close_finished.wait, 1.0)

    assert body.close_calls == 1
    assert len(transport.requests) == 1
    assert (transport.leases, transport.checkins) == (1, 1)
    await asyncio.wait_for(cleanup_done.wait(), timeout=1.0)
    assert set() == _DETACHED_CLEANUP_TASKS


@pytest.mark.anyio
async def test_post_response_body_close_failure_preserves_primary_and_closes_response() -> None:
    body = _FailingCloseBody()
    response = _AsyncCloseResponse(RuntimeError('response close failed'))
    fake = _AsyncFakeTransport([cast('TransportResponse', response)])
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('consumer should not run')

    with pytest.raises(RuntimeError, match='body close failed'):
        await AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless)).execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body,
                timeout=context.timeout,
                token=_POST_RESPONSE_CLOSE_FAILURE_REQUEST_ID,
            ),
            consume,
        )

    assert body.close_calls == 1
    assert response.close_calls == 1
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_public_info_extracts_results() -> None:
    fake = _AsyncFakeTransport([_OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    result = await driver.call('info', {}, config_module.snapshot(_config()))

    assert result == {'id': 7}
    assert fake.requests[0].method == 'POST'


@pytest.mark.anyio
async def test_read_retries_connect_error() -> None:
    fake = _AsyncFakeTransport([ConnectError('refused'), _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    assert await driver.call('read', {'type': 'Shot'}, config_module.snapshot(_config())) == {
        'id': 7
    }

    assert len(fake.requests) == 2


@pytest.mark.anyio
async def test_redirect_close_failure_preserves_redirect_and_observer() -> None:
    redirect = _AsyncCloseResponse(
        RuntimeError('secret backend close text'),
        status=302,
        close_headers={'x-sg-correlation-id': 'mutated-correlation'},
        close_status=599,
        headers={
            'location': 'https://storage.example/signed',
            'x-sg-correlation-id': 'redirect-correlation',
        },
    )
    final = _AsyncCloseResponse(headers={'x-sg-correlation-id': 'final-correlation'})
    fake = _AsyncFakeTransport(
        [cast('TransportResponse', redirect), cast('TransportResponse', final)]
    )
    events: list[ObserverEvent] = []
    config = _config()
    config.on_response = events.append
    context = dataclasses.replace(config_module.snapshot(config), max_rpc_attempts=1)
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def redirect_factory(response: AsyncLiveResponse) -> TransportRequest:
        return dataclasses.replace(_request('redirect'), url=response.headers['location'])

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> bytes:
        return b'data'

    try:
        _raise_ambient_exception()
    except RuntimeError:
        result = await AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless)).execute(
            operation,
            context,
            lambda _attempt: _request('original'),
            consume,
            options=AsyncExecutionOptions(redirect_factory=redirect_factory),
        )

    assert result == b'data'
    assert [request.url for request in fake.requests] == [
        'https://x.example/file',
        'https://storage.example/signed',
    ]
    assert (redirect.close_calls, final.close_calls) == (1, 1)
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert [event.outcome for event in response_events] == ['success', 'success']
    assert [event.status for event in response_events] == [302, 200]
    assert [event.correlation_id for event in response_events] == [
        'redirect-correlation',
        'final-correlation',
    ]
    assert 'secret backend close text' not in repr(events)


@pytest.mark.anyio
async def test_request_body_cleanup_iterator_creates_one_task_and_closes_off_loop_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _Body()
    close_threads: list[int] = []
    create_task_calls = 0
    loop_thread = threading.get_ident()
    original_close = body.close
    original_create_task = asyncio.create_task

    def close() -> None:
        close_threads.append(threading.get_ident())
        original_close()

    def create_task(coroutine: Coroutine[object, object, None]) -> asyncio.Task[None]:
        nonlocal create_task_calls
        create_task_calls += 1
        return original_create_task(coroutine)

    monkeypatch.setattr(body, 'close', close)
    monkeypatch.setattr(_async_driver.asyncio, 'create_task', create_task)
    request = dataclasses.replace(_request('iterator-body-cleanup'), body=body)

    await _close_request_body_after_send(request)

    assert create_task_calls == 1
    assert body.close_calls == 1
    assert len(close_threads) == 1
    assert close_threads[0] != loop_thread


@pytest.mark.anyio
async def test_request_body_cleanup_looks_up_and_invokes_close_off_loop() -> None:
    body = _DescriptorCloseBody()
    loop_thread = threading.get_ident()
    request = dataclasses.replace(_request('shared-body-close'), body=body)

    await _close_request_body_after_send(request)

    assert len(body.lookup_threads) == 1
    assert len(body.callback_threads) == 1
    assert body.lookup_threads[0] == body.callback_threads[0]
    assert body.lookup_threads[0] != loop_thread


@pytest.mark.anyio
async def test_request_body_cleanup_lookup_failure_preserves_active_primary_exception() -> None:
    lookup_error = RuntimeError('descriptor lookup failed')
    primary_error = RuntimeError('active primary')
    body = _DescriptorCloseBody(lookup_error)
    request = dataclasses.replace(_request('failing-body-close-lookup'), body=body)

    async def fail_with_cleanup() -> None:
        try:
            raise primary_error
        finally:
            await _close_request_body_after_send(request)

    with pytest.raises(RuntimeError) as caught:
        await fail_with_cleanup()

    assert caught.value is primary_error
    assert len(body.lookup_threads) == 1
    assert body.callback_threads == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    'body',
    [
        pytest.param(None, id='none'),
        pytest.param(b'buffered', id='bytes'),
    ],
)
async def test_request_body_cleanup_none_or_bytes_schedules_no_work(
    body: bytes | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_work(*_args: object, **_kwargs: object) -> Never:
        pytest.fail('no-op request body cleanup scheduled work')

    monkeypatch.setattr(_async_driver.asyncio, 'create_task', unexpected_work)
    monkeypatch.setattr(_async_driver.asyncio, 'to_thread', unexpected_work)
    request = dataclasses.replace(_request('no-op-body-cleanup'), body=body)

    await _close_request_body_after_send(request)


@pytest.mark.anyio
async def test_response_closes_before_retry_sleep() -> None:
    order: list[str] = []
    gateway = TransportResponse(
        status=502,
        reason='Bad Gateway',
        headers={},
        content=b'',
        on_close=lambda: order.append('close'),
    )

    async def sleep(_delay: float) -> None:
        order.append('sleep')

    fake = _AsyncFakeTransport([gateway, _OK])

    assert await AsyncDriver(fake, _AsyncDriverTiming(sleep=sleep)).call(
        'info',
        {},
        config_module.snapshot(_config()),
    ) == {'id': 7}

    assert order == ['close', 'sleep']


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('retry_after', 'expected_delay', 'should_retry'),
    [
        pytest.param('0', 0.0, True, id='delta-zero'),
        pytest.param('30', 30.0, True, id='delta-cap'),
        pytest.param('31', None, False, id='delta-oversized'),
        pytest.param('9' * 5_000, None, False, id='delta-very-long-oversized'),
        pytest.param('Wed, 01 Jan 2025 00:00:00 GMT', 0.0, True, id='date-zero'),
        pytest.param('Wed, 01 Jan 2025 00:00:30 GMT', 30.0, True, id='date-cap'),
        pytest.param('Wed, 01 Jan 2025 00:00:31 GMT', None, False, id='date-oversized'),
        pytest.param('tomorrow', None, True, id='malformed'),
        pytest.param('-1', None, True, id='negative'),
        pytest.param('NaN', None, True, id='nan'),
        pytest.param('Infinity', None, True, id='positive-infinity'),
        pytest.param('-Infinity', None, True, id='negative-infinity'),
    ],
)
async def test_retry_after_never_sleeps_above_backoff_cap(
    retry_after: str,
    expected_delay: float | None,
    should_retry: bool,
) -> None:
    retries: list[ObserverEvent] = []
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    config = _config()
    config.update_fields(rpc_attempt_interval=4000)
    config.on_retry = retries.append
    retry = TransportResponse(
        status=429,
        reason='Retry Later',
        headers={'retry-after': retry_after},
        content=b'',
    )
    transport = _AsyncFakeTransport([retry, _OK])
    driver = AsyncDriver(
        transport,
        _AsyncDriverTiming(
            jitter_seed=lambda: 1000.25,
            sleep=sleep,
            wall_clock=lambda: 1_735_689_600.0,
        ),
    )

    if not should_retry:
        with pytest.raises(_errors.ProtocolError):
            await driver.call('info', {}, config_module.snapshot(config))
        assert sleeps == []
        assert retries == []
        assert len(transport.requests) == 1
        return

    assert await driver.call('info', {}, config_module.snapshot(config)) == {'id': 7}
    if expected_delay is None:
        fallback_delay = _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'rate_limit', 429, None),
            max_attempts=config.max_rpc_attempts,
            base_interval=config.rpc_attempt_interval / 1000.0,
            now=1000.25,
        )
        assert fallback_delay is not None
        assert sleeps == [fallback_delay]
    else:
        assert sleeps == [expected_delay]
    assert all(delay <= 30.0 for delay in sleeps)
    assert [type(event) for event in retries] == [RetryEvent]
    assert len(transport.requests) == 2


@pytest.mark.anyio
async def test_retry_holds_one_lease_for_the_whole_logical_rpc() -> None:
    fake = _AsyncFakeTransport([_GATEWAY, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    assert await driver.call('info', {}, config_module.snapshot(_config())) == {'id': 7}

    assert len(fake.requests) == 2
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_retry_sleep_runs_inside_the_logical_rpc_lease() -> None:
    fake = _AsyncFakeTransport([_GATEWAY, _OK])

    async def sleep(_delay: float) -> None:
        assert (fake.leases, fake.checkins) == (1, 0)

    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=sleep))

    assert await driver.call('info', {}, config_module.snapshot(_config())) == {'id': 7}
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_rpc_fault_and_terminal_status_close_before_propagating() -> None:
    closed: list[str] = []
    fault = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=b'{"exception": true, "error_code": 102, "message": "no"}',
        on_close=lambda: closed.append('fault'),
    )
    driver = AsyncDriver(
        _AsyncFakeTransport([fault]),
        _AsyncDriverTiming(sleep=_sleepless),
    )

    with pytest.raises(_errors.AuthenticationFault):
        await driver.call('read', {'type': 'Shot'}, config_module.snapshot(_config()))

    forbidden = TransportResponse(
        status=403,
        reason='Forbidden',
        headers={},
        content=b'denied',
        on_close=lambda: closed.append('status'),
    )
    driver = AsyncDriver(
        _AsyncFakeTransport([forbidden]),
        _AsyncDriverTiming(sleep=_sleepless),
    )

    with pytest.raises(_errors.ProtocolError):
        await driver.call('read', {'type': 'Shot'}, config_module.snapshot(_config()))

    assert closed == ['fault', 'status']


@pytest.mark.anyio
async def test_rpc_non_json_response_uses_shared_normalization() -> None:
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'text/plain'},
        content=b'plain response',
    )
    driver = AsyncDriver(
        _AsyncFakeTransport([response]),
        _AsyncDriverTiming(sleep=_sleepless),
    )

    assert await driver.call('read', {}, config_module.snapshot(_config())) == 'plain response'


@pytest.mark.anyio
async def test_streaming_deadline_allows_equality_then_rejects_overrun() -> None:
    ticks = iter((0.0, 0.2, 0.4, 0.8, 1.0, 1.2, 1.4))
    events: list[ObserverEvent] = []
    config = _config()
    config.on_progress = events.append
    context = dataclasses.replace(
        config_module.snapshot(config),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0),
    )
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-length': '4'},
        content=b'abcd',
    )
    driver = AsyncDriver(
        _AsyncFakeTransport([response]),
        _AsyncDriverTiming(clock=lambda: next(ticks), sleep=_sleepless),
    )
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    async def consume(
        live_response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> bytes:
        streaming = cast('AsyncStreamingResponse', live_response)
        return await control.consume(streaming.aiter_chunks(1), total_bytes=4)

    with pytest.raises(ReadTimeoutError, match='total attempt timeout exceeded'):
        await driver.execute(
            operation,
            context,
            lambda _attempt: _request('deadline'),
            consume,
            options=AsyncExecutionOptions(streaming=True),
        )

    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == [1, 2, 3]


@pytest.mark.anyio
async def test_streaming_redirect_stays_inside_one_leased_attempt() -> None:
    releases: list[str] = []
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers={'location': 'https://storage.example/signed'},
        content=b'',
        on_close=lambda: releases.append('redirect'),
    )
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-length': '4'},
        content=b'data',
        on_close=lambda: releases.append('final'),
    )
    fake = _AsyncFakeTransport([redirect, final])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = dataclasses.replace(config_module.snapshot(_config()), max_rpc_attempts=1)
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def redirect_factory(response: AsyncLiveResponse) -> TransportRequest:
        return dataclasses.replace(
            _request('redirect'),
            url=response.headers['location'],
        )

    async def consume(
        response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> bytes:
        streaming = cast('AsyncStreamingResponse', response)
        return await control.consume(streaming.aiter_chunks(2), total_bytes=4)

    assert (
        await driver.execute(
            operation,
            context,
            lambda _attempt: _request('original'),
            consume,
            options=AsyncExecutionOptions(
                redirect_factory=redirect_factory,
                streaming=True,
            ),
        )
        == b'data'
    )

    assert [request.url for request in fake.requests] == [
        'https://x.example/file',
        'https://storage.example/signed',
    ]
    assert releases == ['redirect', 'final']
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
async def test_streaming_redirect_uses_only_remaining_total_timeout() -> None:
    ticks = iter((0.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers={'location': 'https://storage.example/signed'},
        content=b'',
    )
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'data',
    )
    fake = _AsyncFakeTransport([redirect, final])
    context = dataclasses.replace(
        config_module.snapshot(_config()),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )
    driver = AsyncDriver(
        fake,
        _AsyncDriverTiming(clock=lambda: next(ticks), sleep=_sleepless),
    )
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def redirect_factory(response: AsyncLiveResponse) -> TransportRequest:
        return dataclasses.replace(
            _request('redirect-timeout'),
            timeout=context.timeout,
            url=response.headers['location'],
        )

    async def consume(
        response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> bytes:
        streaming = cast('AsyncStreamingResponse', response)
        return await control.consume(streaming.aiter_chunks(), total_bytes=4)

    assert (
        await driver.execute(
            operation,
            context,
            lambda _attempt: dataclasses.replace(
                _request('original-timeout'),
                timeout=context.timeout,
            ),
            consume,
            options=AsyncExecutionOptions(
                redirect_factory=redirect_factory,
                streaming=True,
            ),
        )
        == b'data'
    )

    assert fake.requests[0].timeout.total == 10.0
    assert fake.requests[1].timeout.total == 7.0


@pytest.mark.anyio
@pytest.mark.parametrize('streaming', [False, True])
async def test_redirect_budget_includes_request_observer_time(streaming: bool) -> None:
    now = 0.0
    requests: list[RequestEvent] = []
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers={'location': 'https://storage.example/signed?secret=value'},
        content=b'',
        on_close=lambda: advance(3.0),
    )
    final = TransportResponse(status=200, reason='OK', headers={}, content=b'data')
    fake = _AsyncFakeTransport([redirect, final])
    config = _config()

    def advance(value: float) -> None:
        nonlocal now
        now = value

    def observe(event: ObserverEvent) -> None:
        assert isinstance(event, RequestEvent)
        requests.append(event)
        if len(requests) == 2:
            advance(5.0)

    config.on_request = observe
    context = dataclasses.replace(
        config_module.snapshot(config),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )

    async def consume(response: AsyncLiveResponse, control: AsyncAttemptControl) -> bytes:
        if streaming:
            return await control.consume(
                cast('AsyncStreamingResponse', response).aiter_chunks(), total_bytes=4
            )
        return cast('TransportResponse', response).content

    result = await AsyncDriver(fake, _AsyncDriverTiming(clock=lambda: now)).execute(
        _protocol.classify_endpoint('download'),
        context,
        lambda _attempt: dataclasses.replace(_request('original'), timeout=context.timeout),
        consume,
        options=AsyncExecutionOptions(
            redirect_factory=lambda response: dataclasses.replace(
                _request('redirected'),
                timeout=context.timeout,
                url=response.headers['location'],
            ),
            streaming=streaming,
        ),
    )

    assert result == b'data'
    assert len(requests) == 2
    assert [request.timeout.total for request in fake.requests] == [10.0, 5.0]
    assert (fake.leases, fake.checkins) == (1, 1)


@pytest.mark.anyio
@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('expiry', ['cleanup', 'observer'])
@pytest.mark.parametrize('policy', ['one_attempt', 'replay_safe', 'unsafe'])
async def test_redirect_deadline_expiry_uses_attempt_retry_policy(
    streaming: bool,
    expiry: str,
    policy: str,
) -> None:
    now = 0.0
    events: list[ObserverEvent] = []
    sleeps: list[float] = []
    body_closes: list[str] = []
    response_closes: list[str] = []
    consumer_calls: list[str] = []
    retry = policy == 'replay_safe'
    redirects = [
        TransportResponse(
            status=302,
            reason='Found',
            headers={'location': 'https://storage.example/signed?secret=value'},
            content=b'',
            on_close=lambda attempt=attempt: close_redirect(attempt),
        )
        for attempt in (1, 2)
    ]
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'data',
        on_close=lambda: response_closes.append('final'),
    )
    fake = _AsyncFakeTransport([redirects[0], redirects[1], final])
    config = _config()

    def close_redirect(attempt: int) -> None:
        nonlocal now
        response_closes.append(f'redirect-{attempt}')
        now = (11.0 if expiry == 'cleanup' else 3.0) if attempt == 1 else 23.0

    def observe_request(event: ObserverEvent) -> None:
        nonlocal now
        assert isinstance(event, RequestEvent)
        events.append(event)
        request_count = len([item for item in events if isinstance(item, RequestEvent)])
        if request_count == 2:
            now = 11.0 if expiry == 'observer' else now
        elif request_count == 4:
            now = 25.0

    async def sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now = 20.0

    config.update_fields(
        on_request=observe_request,
        on_response=events.append,
        on_retry=events.append,
        max_rpc_attempts=2 if policy != 'one_attempt' else 1,
    )
    context = dataclasses.replace(
        config_module.snapshot(config),
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )

    def redirect_factory(response: AsyncLiveResponse) -> TransportRequest:
        attempt = len([item for item in events if isinstance(item, RequestEvent)])
        return dataclasses.replace(
            _request(f'redirect-{attempt}'),
            timeout=context.timeout,
            url=response.headers['location'],
            headers={'authorization': 'secret-request-header'},
            body=_RedirectBody(body_closes, f'body-{attempt}'),
        )

    async def consume(response: AsyncLiveResponse, _control: AsyncAttemptControl) -> bytes:
        consumer_calls.append('consume')
        return cast('TransportResponse', response).content if not streaming else b'data'

    driver = AsyncDriver(fake, _AsyncDriverTiming(clock=lambda: now, sleep=sleep))
    operation = _protocol.classify_endpoint('allocation' if policy == 'unsafe' else 'download')

    async def call() -> bytes:
        return await driver.execute(
            operation,
            context,
            lambda attempt: dataclasses.replace(
                _request(f'original-{attempt}'), timeout=context.timeout
            ),
            consume,
            options=AsyncExecutionOptions(redirect_factory=redirect_factory, streaming=streaming),
        )

    if retry:
        assert await call() == b'data'
    else:
        with pytest.raises(ReadTimeoutError):
            await call()

    assert (
        [request.timeout.total for request in fake.requests],
        len([event for event in events if isinstance(event, RequestEvent)]),
    ) == (([10.0, 10.0, 5.0], 4) if retry else ([10.0], 2))
    assert [
        (event.attempt, event.status, event.outcome)
        for event in events
        if isinstance(event, ResponseEvent)
    ] == (
        [(1, 302, 'success'), (1, None, 'retry'), (2, 302, 'success'), (2, 200, 'success')]
        if retry
        else [(1, 302, 'success'), (1, None, 'error')]
    )
    assert (
        len([event for event in events if isinstance(event, RetryEvent)]),
        len(sleeps),
    ) == (int(retry), int(retry))
    assert (body_closes, response_closes) == (
        (['body-1', 'body-3'], ['redirect-1', 'redirect-2', 'final'])
        if retry
        else (['body-1'], ['redirect-1'])
    )
    assert consumer_calls == (['consume'] if retry else [])
    assert (fake.leases, fake.checkins) == (1, 1)
    assert all(secret not in repr(events) for secret in ('secret=value', 'secret-request-header'))


@pytest.mark.anyio
async def test_unsafe_allocation_does_not_retry_body_read_transport_error() -> None:
    fake = _AsyncFakeTransport([_OK, _OK])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))
    context = config_module.snapshot(_config())
    operation = _protocol.classify_endpoint('allocation', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> bytes:
        raise ReadTimeoutError('body timed out')

    with pytest.raises(ReadTimeoutError, match='body timed out'):
        await driver.execute(
            operation,
            context,
            lambda attempt: _request(f'allocation-{attempt}'),
            consume,
        )

    assert len(fake.requests) == 1


@pytest.mark.anyio
async def test_upload_progress_deadline_raises_on_loop_before_return() -> None:
    loop_thread = threading.get_ident()
    clock_threads: list[int] = []
    ticks = iter((0.0, 2.0, 2.0))

    def clock() -> float:
        clock_threads.append(threading.get_ident())
        return next(ticks)

    context = dataclasses.replace(
        config_module.snapshot(_config()),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0),
    )
    transport = _LoopPullingTransport()
    driver = AsyncDriver(
        transport,
        _AsyncDriverTiming(clock=clock, sleep=_sleepless),
    )
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> None:
        raise AssertionError('deadline chunk reached the response consumer')

    with pytest.raises(ReadTimeoutError, match='total attempt timeout exceeded'):
        await driver.execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={'content-length': '1'},
                body=iter((b'a',)),
                timeout=context.timeout,
                token=_LOOP_DEADLINE_REQUEST_ID,
            ),
            consume,
        )

    assert transport.returned_chunks == []
    assert clock_threads == [loop_thread, loop_thread, loop_thread]


@pytest.mark.anyio
async def test_upload_progress_observer_failure_isolated_on_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop_thread = threading.get_ident()
    progress_counts: list[int] = []
    progress_threads: list[int] = []

    def fail(event: ObserverEvent) -> None:
        if isinstance(event, ProgressEvent):
            progress_threads.append(threading.get_ident())
            progress_counts.append(event.bytes_transferred)
            raise TypeError('observer-secret')

    config = _config()
    config.on_progress = fail
    context = config_module.snapshot(config)
    transport = _LoopPullingTransport()
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'uploaded'

    with caplog.at_level('WARNING', logger='shotgun_api3'):
        result = await driver.execute(
            operation,
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={'content-length': '3'},
                body=iter((b'a', b'bc')),
                timeout=context.timeout,
                token=_LOOP_OBSERVER_FAILURE_REQUEST_ID,
            ),
            consume,
        )

    assert result == 'uploaded'
    assert transport.returned_chunks == [b'a', b'bc']
    assert progress_threads == [loop_thread, loop_thread]
    assert progress_counts == [1, 3]
    assert 'observer callback failed' in caplog.text
    assert 'observer-secret' not in caplog.text


@pytest.mark.anyio
async def test_upload_progress_runs_on_loop_with_increasing_counts() -> None:
    loop_thread = threading.get_ident()
    progress_threads: list[int] = []
    progress_counts: list[int] = []

    def observe(event: ObserverEvent) -> None:
        if isinstance(event, ProgressEvent):
            progress_threads.append(threading.get_ident())
            progress_counts.append(event.bytes_transferred)

    config = _config()
    config.on_progress = observe
    context = config_module.snapshot(config)
    transport = _LoopPullingTransport()
    driver = AsyncDriver(transport, _AsyncDriverTiming(sleep=_sleepless))
    operation = _protocol.classify_endpoint('object_put', body_restartable=True)

    async def consume(
        _response: AsyncLiveResponse,
        _control: AsyncAttemptControl,
    ) -> str:
        return 'uploaded'

    def request_factory(_attempt: int) -> TransportRequest:
        return TransportRequest(
            method='PUT',
            url='https://storage.example/object',
            headers={'content-length': '3'},
            body=iter((b'a', b'bc')),
            timeout=context.timeout,
            token=_LOOP_PROGRESS_REQUEST_ID,
        )

    assert await driver.execute(operation, context, request_factory, consume) == 'uploaded'
    assert transport.returned_chunks == [b'a', b'bc']
    assert progress_threads == [loop_thread, loop_thread]
    assert progress_counts == [1, 3]


@pytest.mark.anyio
async def test_write_does_not_retry_ambiguous_send_error() -> None:
    fake = _AsyncFakeTransport([SendError('connection lost mid-body')])
    driver = AsyncDriver(fake, _AsyncDriverTiming(sleep=_sleepless))

    with pytest.raises(SendError, match='connection lost mid-body'):
        await driver.call('create', {'type': 'Shot'}, config_module.snapshot(_config()))

    assert len(fake.requests) == 1
