import asyncio
import inspect
import json
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import suppress
from pathlib import Path
from typing import Never, Protocol, cast, get_type_hints
from unittest.mock import patch

import httpx2
import pytest

from shotgun_api3 import _async_client as async_client_module
from shotgun_api3 import _config as config_module
from shotgun_api3 import _errors
from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._async_driver import AsyncDriver
from shotgun_api3._async_httpx2 import Httpx2Transport
from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._client import Shotgun
from shotgun_api3._config import Config, RequestContext, ResponseEvent, readiness_key
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    ConnectError,
    ReadTimeoutError,
    SendError,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._pyright import run_pyright
from tests._transports import _AsyncFakeTransport
from tests.conftest import _fake_async_transport, _make_response, _set_fake_async_transport

pytestmark = pytest.mark.async_required

FakeAsyncClient = Callable[..., AsyncShotgun]

_AUTH_VALUE = 'auth-value'
_CREDENTIAL = 'credential-value'
_NEW_AUTH_VALUE = 'new-auth-value'
_NEW_CREDENTIAL = 'new-credential-value'
_NEW_SESSION_ID = 'new-session'
_ONE_USE_VALUE = 'one-use-value'
_ORIGIN_AUTH_VALUE = 'old-auth-token'
_ORIGIN_CREDENTIAL = 'old-password'
_ORIGIN_SESSION_VALUE = 'old-session'
_SESSION_ID = 'session-value'

_INSECURE_RPC_ERROR = 'authenticated RPCs require HTTPS or a direct literal loopback HTTP origin'


class _ControlledAsyncTransport:
    def __init__(
        self,
        responses: list[TransportResponse | TransportError],
        *,
        block_close: bool = False,
        blocked_requests: set[int] | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self._blocked_requests = blocked_requests or set()
        self._close_error = close_error
        self._closing = False
        self._idle = asyncio.Event()
        self._idle.set()
        self._lease_holders: dict[asyncio.Task[object], int] = {}
        self._releases = {index: asyncio.Event() for index in self._blocked_requests}
        self._reserved_tasks: set[asyncio.Task[object]] = set()
        self._responses = list(responses)
        self._started = {index: asyncio.Event() for index in self._blocked_requests}
        self.active = 0
        self.cancelled_requests: set[int] = set()
        self.checkins = 0
        self.checkouts = 0
        self.close_calls = 0
        self.close_attempts = 0
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()
        if not block_close:
            self.allow_close.set()
        self.reservation_releases = 0
        self.reservations = 0
        self.requests: list[TransportRequest] = []

    def _acquire(self, task: asyncio.Task[object]) -> None:
        self.active += 1
        self._idle.clear()
        self._lease_holders[task] = self._lease_holders.get(task, 0) + 1

    def _release(self, task: asyncio.Task[object]) -> None:
        count = self._lease_holders.get(task, 0)
        if count <= 0:
            raise AssertionError('controlled async transport lease underflow')
        if count == 1:
            del self._lease_holders[task]
        else:
            self._lease_holders[task] = count - 1
        self.active -= 1
        if self.active == 0:
            self._idle.set()

    def _release_reservation(self, task: asyncio.Task[object]) -> None:
        if task not in self._reserved_tasks:
            return
        self._reserved_tasks.remove(task)
        self.reservation_releases += 1
        self._release(task)

    def _require_available(self, task: asyncio.Task[object]) -> None:
        if self._closing and self._lease_holders.get(task, 0) <= 0:
            raise TransportError('async transport is closing')

    @staticmethod
    def _task() -> asyncio.Task[object]:
        task = asyncio.current_task()
        if task is None:
            raise AssertionError('controlled async transport requires an active task')
        return cast('asyncio.Task[object]', task)

    def checkin(self) -> None:
        self.checkins += 1
        self._release(self._task())

    def checkout(self) -> None:
        task = self._task()
        self._require_available(task)
        self.checkouts += 1
        self._acquire(task)

    async def close(self) -> None:
        self.close_attempts += 1
        self._closing = True
        self.close_started.set()
        await self._idle.wait()
        await self.allow_close.wait()
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error

    def release(self, index: int) -> None:
        self._releases[index].set()

    def reserve(self, task: asyncio.Task[object]) -> None:
        owner = self._task()
        self._require_available(owner)
        if task in self._reserved_tasks:
            raise TransportError('async transport task is already reserved')
        self._reserved_tasks.add(task)
        self.reservations += 1
        self._acquire(task)
        task.add_done_callback(self._release_reservation)

    async def send(self, request: TransportRequest) -> TransportResponse:
        self._require_available(self._task())
        self.requests.append(request)
        index = len(self.requests)
        if not self._responses:
            raise AssertionError('_ControlledAsyncTransport responses exhausted')
        response = self._responses.pop(0)
        try:
            if index in self._blocked_requests:
                self._started[index].set()
                await self._releases[index].wait()
        except asyncio.CancelledError:
            self.cancelled_requests.add(index)
            raise
        if isinstance(response, TransportError):
            raise response
        return response

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        del request
        raise AssertionError('streaming is outside this test helper')

    async def wait_started(self, index: int) -> None:
        await self._started[index].wait()


class _RecordingTransport(Protocol):
    requests: list[TransportRequest]


def _assert_acquisition_rollback_released(
    sg: AsyncShotgun, replacement: _ControlledAsyncTransport
) -> None:
    assert replacement.close_attempts == replacement.close_calls == 1
    assert replacement.checkouts == replacement.checkins == 1
    # The double enforces task ownership in checkin, as the real transport does.
    assert not replacement._lease_holders
    assert replacement.active == 0
    assert id(replacement) not in sg._lease_counts
    assert id(replacement) not in sg._retired_transports
    assert not sg._active_auth_contexts
    assert not replacement.requests


def _async_call(
    sg: AsyncShotgun,
    method_name: str,
    params: object,
) -> Coroutine[object, object, object]:
    callback = cast(
        'Callable[[str, object], Coroutine[object, object, object]]',
        sg._call,
    )
    return callback(method_name, params)


async def _capture_failed_acquisition(sg: AsyncShotgun, context: RequestContext) -> BaseException:
    try:
        async with sg._driver_lease(context):
            raise AssertionError('failed acquisition unexpectedly yielded')
    except (RuntimeError, asyncio.CancelledError) as error:
        return error
    raise AssertionError('failed acquisition unexpectedly succeeded')


async def _deliver_cancellation(operation: asyncio.Task[BaseException], message: str) -> None:
    operation.cancel(message)
    # The operation is queued before this acknowledgement, so cancellation
    # reaches its suspended await before the test checks joined cleanup.
    delivered = asyncio.Event()
    asyncio.get_running_loop().call_soon(delivered.set)
    await asyncio.wait_for(delivered.wait(), timeout=1)


def _method_names(transport: _RecordingTransport) -> list[str]:
    names: list[str] = []
    for request in transport.requests:
        assert isinstance(request.body, bytes)
        payload = json.loads(request.body)
        names.append(cast('str', payload['method_name']))
    return names


def _payload(transport: _RecordingTransport, index: int) -> dict[str, object]:
    request = transport.requests[index]
    assert isinstance(request.body, bytes)
    return cast('dict[str, object]', json.loads(request.body))


def _record_page_prefetch(
    monkeypatch: pytest.MonkeyPatch,
    observed: list[int],
) -> None:
    original = AsyncDriver.call_pages

    def call_pages(
        driver: AsyncDriver,
        build_page_params: Callable[[int], object],
        first_page: int,
        method_name: str,
        context: RequestContext,
    ) -> AsyncGenerator[object, None]:
        observed.append(context.page_prefetch)
        return original(
            driver,
            build_page_params,
            first_page,
            method_name,
            context,
        )

    monkeypatch.setattr(AsyncDriver, 'call_pages', call_pages)


@pytest.mark.anyio
@pytest.mark.parametrize('redirect', [False, True])
async def test_attachment_requests_do_not_replay_response_cookies(
    monkeypatch: pytest.MonkeyPatch, redirect: bool
) -> None:
    observed: list[httpx2.Request] = []
    attachment_url = 'https://example.test:8443/attachment'

    def handle(request: httpx2.Request) -> httpx2.Response:
        observed.append(request)
        headers = {'Set-Cookie': '_session_id=response-session; Path=/; Secure'}
        if request.url.path == '/api3/json':
            response = _make_response({'version': [9, 0, 0]})
            return httpx2.Response(200, headers=headers, content=response.content)
        if request.url.path == '/redirect':
            return httpx2.Response(302, headers={**headers, 'Location': attachment_url})
        return httpx2.Response(200, headers=headers, content=b'DATA')

    def init_mock_transport(*_args: object, **_kwargs: object) -> httpx2.MockTransport:
        return httpx2.MockTransport(handle)

    monkeypatch.setattr(httpx2.AsyncClient, '_init_transport', init_mock_transport)
    sg = AsyncShotgun('https://example.test', session_token=_SESSION_ID, connect=False)
    try:
        assert await sg.info() == {'version': [9, 0, 0]}
        url = 'https://example.test/redirect' if redirect else attachment_url
        assert await sg.download_attachment({'url': url}) == b'DATA'
    finally:
        await sg.close()

    assert str(observed[-1].url) == attachment_url
    assert observed[-1].headers.get('cookie') is None
    if redirect:
        initial = next(request for request in observed if request.url.path == '/redirect')
        assert initial.headers['cookie'] == f'_session_id={_SESSION_ID}'


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('http://remote.example', None),
        ('http://localhost', 'proxy.example:3128'),
        ('http://127.0.0.1', 'proxy.example:3128'),
        ('http://[::1]', 'proxy.example:3128'),
        ('http://localhost', 'localhost:3128'),
        ('http://127.0.0.1', '127.0.0.1:3128'),
        ('http://[::1]', '[::1]:3128'),
    ],
)
async def test_authenticated_http_rpc_rejects_before_lease_request_and_observer(
    base_url: str, http_proxy: str | None
) -> None:
    sg = AsyncShotgun(base_url, script_name='s', api_key='k', http_proxy=http_proxy)
    transport = _ControlledAsyncTransport([])
    sg._transport = transport
    events: list[object] = []
    sg.config.on_request = events.append

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        await sg.delete('Asset', 1)

    assert transport.checkouts == 0
    assert transport.requests == []
    assert events == []


@pytest.mark.anyio
async def test_authenticated_http_find_rejects_before_driver_lease() -> None:
    sg = AsyncShotgun('http://remote.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport([])
    sg._transport = transport

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        await sg.find('Asset', [])

    assert transport.checkouts == 0
    assert transport.requests == []


@pytest.mark.anyio
async def test_authenticated_http_session_token_rejects_before_driver_lease() -> None:
    sg = AsyncShotgun('http://remote.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport([])
    sg._transport = transport

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        await sg.get_session_token()

    assert transport.checkouts == 0
    assert transport.requests == []


@pytest.mark.anyio
async def test_authenticated_http_invoke_rejects_before_driver_call() -> None:

    class RejectingDriver:
        async def call(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError('driver call preceded the security guard')

    sg = AsyncShotgun('http://remote.example', script_name='s', api_key='k')
    invoke = cast(
        'Callable[..., Coroutine[object, object, object]]',
        sg._invoke,
    )

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        await invoke(
            RejectingDriver(),
            async_client_module._RpcInvocation(
                context=config_module.snapshot(sg.config),
                method_name='read',
                options=async_client_module._InvocationOptions(),
                params={},
            ),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('https://remote.example', None),
        ('https://remote.example', 'proxy.example:3128'),
        ('https://remote.example', 'localhost:3128'),
        ('http://localhost', None),
        ('http://127.0.0.1', None),
        ('http://[::1]', None),
    ],
)
async def test_authenticated_http_guard_allows_secure_and_loopback_rpc(
    base_url: str,
    http_proxy: str | None,
) -> None:
    sg = AsyncShotgun(base_url, http_proxy=http_proxy, script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response(obj=True)]
    )
    sg._transport = transport

    assert await sg.delete('Asset', 1) is True
    assert _method_names(transport) == ['info', 'delete']


@pytest.mark.anyio
@pytest.mark.parametrize(
    'operation',
    [
        'share-entity',
        'share-path',
        'upload-direct',
        'upload-form',
        'download-uncached',
        'download-cached',
    ],
)
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('http://remote.example', None),
        ('http://localhost', 'proxy.example:3128'),
        ('http://127.0.0.1', 'proxy.example:3128'),
        ('http://[::1]', 'proxy.example:3128'),
        ('http://localhost', 'localhost:3128'),
        ('http://127.0.0.1', '127.0.0.1:3128'),
        ('http://[::1]', '[::1]:3128'),
    ],
)
async def test_authenticated_file_http_rejects_before_lease(
    tmp_path: Path,
    operation: str,
    base_url: str,
    http_proxy: str | None,
) -> None:
    path = tmp_path / 'image.jpg'
    path.write_bytes(b'image')
    cached = operation == 'download-cached'
    sg = AsyncShotgun(
        base_url,
        script_name=None if cached else 'dummy-script',
        api_key=None if cached else 'dummy-key',
        session_token=_SESSION_ID if cached else None,
        http_proxy=http_proxy,
        connect=False,
    )
    info = {
        'version': [9, 0, 0],
        's3_direct_uploads_enabled': operation != 'upload-form',
        's3_enabled_upload_types': {'*': ['*']} if operation != 'upload-form' else {},
    }
    transport = _AsyncFakeTransport(
        [_make_response(info), TransportResponse(200, 'OK', {}, b'1:42')]
    )
    sg._transport = transport
    events: list[object] = []
    sg.config.on_request = events.append
    if operation == 'share-entity':
        call = sg.share_thumbnail(
            [{'type': 'Asset', 'id': 1}], source_entity={'type': 'Asset', 'id': 2}
        )
    elif operation == 'share-path':
        call = sg.share_thumbnail([{'type': 'Asset', 'id': 1}], thumbnail_path=str(path))
    elif operation.startswith('upload-'):
        call = sg.upload('Asset', 1, str(path))
    else:
        call = sg.download_attachment(41)
    with patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease:
        with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
            await call
        lease.assert_not_called()
    assert transport.leases == 0
    assert transport.requests == []
    assert events == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('https://remote.example', None),
        ('https://remote.example', 'proxy.example:3128'),
        ('https://remote.example', 'localhost:3128'),
        ('http://localhost', None),
        ('http://LOCALHOST', None),
        ('http://127.0.0.1', None),
        ('http://127.42.0.1', None),
        ('http://[::1]', None),
    ],
)
@pytest.mark.parametrize(
    'operation', ['share', 'share-path', 'upload', 'download-cached', 'download-uncached']
)
async def test_authenticated_file_allows_https_and_literal_loopback(
    tmp_path: Path, base_url: str, operation: str, http_proxy: str | None
) -> None:
    cached = operation == 'download-cached'
    sg = AsyncShotgun(
        base_url,
        http_proxy=http_proxy,
        script_name=None if cached else 'dummy-script',
        api_key=None if cached else 'dummy-key',
        session_token=_SESSION_ID if cached else None,
        connect=False,
    )
    responses: list[TransportResponse | TransportError] = [
        _make_response(
            {
                'version': [9, 0, 0],
                's3_direct_uploads_enabled': True,
                's3_enabled_upload_types': {'*': ['*']},
            }
        )
    ]
    if operation == 'download-uncached':
        responses.append(_make_response({'session_id': _SESSION_ID}))
    if operation in {'upload', 'share-path'}:
        upload_type = 'Thumbnail' if operation == 'share-path' else 'Attachment'
        allocation = f'1 https://storage.example/image timestamp {upload_type} upload-1'
        responses.extend(
            [
                TransportResponse(200, 'OK', {}, allocation.encode()),
                TransportResponse(200, 'OK', {}, b''),
            ]
        )
    responses.append(
        TransportResponse(200, 'OK', {}, b'DATA' if operation.startswith('download') else b'1:42')
    )
    transport = _AsyncFakeTransport(responses)
    sg._transport = transport

    path = tmp_path / 'image.jpg'
    path.write_bytes(b'image')
    if operation == 'upload':
        assert await sg.upload('Asset', 1, str(path)) == 42
    elif operation == 'share-path':
        assert (
            await sg.share_thumbnail([{'type': 'Asset', 'id': 1}], thumbnail_path=str(path)) == 42
        )
    elif operation == 'share':
        assert (
            await sg.share_thumbnail(
                [{'type': 'Asset', 'id': 1}], source_entity={'type': 'Asset', 'id': 2}
            )
            == 42
        )
        assert b'dummy-key' in cast('bytes', transport.requests[-1].body)
    else:
        assert await sg.download_attachment(41) == b'DATA'
        assert transport.requests[-1].headers['cookie'] == f'_session_id={_SESSION_ID}'


@pytest.mark.anyio
@pytest.mark.parametrize('url', ['http://storage.example/file', 'http://remote.example:8080/file'])
async def test_authenticated_file_external_http_download_omits_credentials(url: str) -> None:
    sg = AsyncShotgun('http://remote.example', session_token=_SESSION_ID, connect=False)
    transport = _AsyncFakeTransport([TransportResponse(200, 'OK', {}, b'DATA')])
    sg._transport = transport

    assert await sg.download_attachment({'url': url}) == b'DATA'
    assert len(transport.requests) == 1
    assert transport.requests[0].url == url
    assert set(transport.requests[0].headers) == {'user-agent'}
    assert transport.requests[0].body is None


@pytest.mark.anyio
async def test_authenticated_http_guard_allows_public_unauthenticated_info() -> None:
    sg = AsyncShotgun('http://remote.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport([_make_response({'version': [9, 0, 0]})])
    sg._transport = transport

    assert await sg.info() == {'version': [9, 0, 0]}
    assert _method_names(transport) == ['info']


def test_cached_server_properties_require_population() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')

    with pytest.raises(_errors.ShotgunError, match=r'await connect\(\) or any RPC first'):
        _ = sg.server_caps
    with pytest.raises(_errors.ShotgunError, match=r'await connect\(\) or any RPC first'):
        _ = sg.server_info


@pytest.mark.anyio
@pytest.mark.parametrize('invalid_config', [False, True])
async def test_cached_session_token_validates_config_without_acquiring_driver(
    monkeypatch: pytest.MonkeyPatch,
    invalid_config: bool,
) -> None:
    sg = AsyncShotgun('https://x.example', session_token=_SESSION_ID)

    def reject_driver(_context: RequestContext) -> Never:
        raise AssertionError('cached token acquired a driver')

    monkeypatch.setattr(sg, '_driver_lease', reject_driver)
    if invalid_config:
        sg.config.update_fields(proxy_server='proxy.local', proxy_user='unpaired-user')
        with pytest.raises(ValueError, match='proxy'):
            await sg.get_session_token()
    else:
        assert await sg.get_session_token() == _SESSION_ID
    assert sg._transport is None


@pytest.mark.anyio
async def test_cancelled_failed_context_entry_waits_for_close() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]})],
        block_close=True,
        blocked_requests={1},
    )
    sg._transport = transport

    entry_task = asyncio.create_task(sg.__aenter__())
    await transport.wait_started(1)
    entry_task.cancel('entry cancellation')
    await asyncio.wait_for(transport.close_started.wait(), timeout=1)
    assert not entry_task.done()
    transport.allow_close.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await entry_task
    assert caught.value.args == ('entry cancellation',)
    assert transport.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
async def test_cancelled_readiness_finalizes_after_contended_lock() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'version': [9, 0, 0]}),
            _make_response(8),
        ],
        blocked_requests={1},
    )
    sg._transport = transport
    owner = asyncio.create_task(_async_call(sg, 'read', None))
    await transport.wait_started(1)
    waiter = asyncio.create_task(_async_call(sg, 'read', None))
    await asyncio.sleep(0)
    assert len(sg._readiness_in_progress) == 1
    readiness_lock = sg._readiness_lock
    await readiness_lock.acquire()
    try:
        owner.cancel('original')
        await asyncio.sleep(0)
        owner.cancel('repeated')
        await asyncio.sleep(0)
    finally:
        readiness_lock.release()

    with pytest.raises(asyncio.CancelledError) as raised:
        await asyncio.wait_for(owner, timeout=1)
    assert raised.value.args == ('original',)
    assert await asyncio.wait_for(waiter, timeout=1) == 8
    assert sg._readiness_in_progress == {}
    assert sg._ready_caps


@pytest.mark.anyio
async def test_cleanup_cancellation_propagates_after_outer_handled_cancellation() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    readiness_finished = asyncio.Event()
    transport = _ControlledAsyncTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=b'{"results": {"version": [9, 0, 0]}}',
                on_close=readiness_finished.set,
            ),
            _make_response(8),
        ],
        blocked_requests={1},
    )
    sg._transport = transport
    outer_waiting = asyncio.Event()

    async def call_after_handled_cancellation() -> object:
        current = asyncio.current_task()
        assert current is not None
        try:
            outer_waiting.set()
            await asyncio.Future()
        except asyncio.CancelledError:
            current.uncancel()
            assert current.cancelling() == 0
            return await _async_call(sg, 'read', None)

    owner = asyncio.create_task(call_after_handled_cancellation())
    await outer_waiting.wait()
    owner.cancel('handled')
    await transport.wait_started(1)
    readiness_lock = sg._readiness_lock
    await readiness_lock.acquire()
    try:
        transport.release(1)
        await readiness_finished.wait()
        owner.cancel('cleanup')
        await asyncio.sleep(0)
    finally:
        readiness_lock.release()

    with pytest.raises(asyncio.CancelledError) as raised:
        await asyncio.wait_for(owner, timeout=1)
    assert raised.value.args == ('cleanup',)
    assert _method_names(transport) == ['info']


@pytest.mark.anyio
@pytest.mark.parametrize('error_type', [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize('later_error_type', [RuntimeError, asyncio.CancelledError])
async def test_close_attempts_both_replaced_transports_and_preserves_first_failure(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
    later_error_type: type[BaseException],
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    old = _ControlledAsyncTransport([_make_response({'version': [9, 0, 0]})])
    replacement = _ControlledAsyncTransport([])
    sg._transport = old
    await sg.connect()
    first_error = error_type('old close failed')
    close_calls: list[str] = []

    async def close_old() -> None:
        close_calls.append('old')
        raise first_error

    async def close_replacement() -> None:
        close_calls.append('replacement')
        raise later_error_type('replacement close failed')

    monkeypatch.setattr(old, 'close', close_old)
    monkeypatch.setattr(replacement, 'close', close_replacement)
    sg._transport = replacement

    with pytest.raises(error_type) as raised:
        await sg.close()

    assert raised.value is first_error
    assert close_calls == ['old', 'replacement']
    assert sg._transport is None
    await sg.close()
    assert close_calls == ['old', 'replacement']


@pytest.mark.anyio
async def test_close_cancellation_still_closes_replaced_transport() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    old = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]})],
        block_close=True,
    )
    replacement = _ControlledAsyncTransport([])
    sg._transport = old
    await sg.connect()
    sg._transport = replacement

    closing = asyncio.create_task(sg.close())
    await old.close_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing

    assert old.close_calls == 0
    assert replacement.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
@pytest.mark.parametrize('replace_transport', [False, True])
async def test_close_closes_each_distinct_known_and_current_transport_once(
    replace_transport: bool,
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    old = _ControlledAsyncTransport([_make_response({'version': [9, 0, 0]})])
    replacement = _ControlledAsyncTransport([]) if replace_transport else old
    sg._transport = old
    await sg.connect()
    sg._transport = replacement

    await sg.close()
    await sg.close()

    assert old.close_calls == 1
    assert replacement.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
async def test_close_drains_inflight_and_next_operation_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    first_transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response(1)],
        blocked_requests={2},
    )
    second_transport = _AsyncFakeTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response(2)]
    )
    sg._transport = first_transport

    def new_transport(_context: RequestContext) -> _AsyncFakeTransport:
        return second_transport

    monkeypatch.setattr(sg, '_new_transport', new_transport)

    operation = asyncio.create_task(_async_call(sg, 'read', None))
    await first_transport.wait_started(2)
    closing = asyncio.create_task(sg.close())
    await first_transport.close_started.wait()
    await asyncio.sleep(0)
    assert not closing.done()
    first_transport.release(2)

    assert await operation == 1
    await closing
    assert first_transport.close_calls == 1
    assert sg._transport is None
    assert await _async_call(sg, 'read', None) == 2
    assert _method_names(second_transport) == ['info', 'read']


@pytest.mark.anyio
async def test_close_preserves_async_readiness_from_concurrent_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    old_transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response(1)],
        blocked_requests={2},
    )
    new_transport = _AsyncFakeTransport([_make_response({'version': [10, 0, 0]})])
    sg._transport = old_transport

    def replace_transport(_context: RequestContext) -> _AsyncFakeTransport:
        return new_transport

    monkeypatch.setattr(sg, '_new_transport', replace_transport)

    async with asyncio.timeout(5):
        async with asyncio.TaskGroup() as tasks:
            operation = tasks.create_task(_async_call(sg, 'read', None))
            try:
                await old_transport.wait_started(2)
                tasks.create_task(sg.close())
                await old_transport.close_started.wait()
                await sg.connect()
            finally:
                old_transport.release(2)

    assert operation.result() == 1
    current_key = readiness_key(config_module.snapshot(sg.config))
    ready_caps = sg._ready_caps
    assert set(ready_caps) == {current_key}
    assert ready_caps[current_key].version == (10, 0, 0)
    assert sg.server_caps.version == (10, 0, 0)


@pytest.mark.anyio
async def test_close_race_preserves_floor_auth_and_requested_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
    )
    transport = Httpx2Transport()
    sg._transport = transport
    closing: asyncio.Task[None] | None = None
    method_names: list[str] = []

    async def send(request: TransportRequest) -> TransportResponse:
        nonlocal closing
        assert isinstance(request.body, bytes)
        method_name = cast('str', json.loads(request.body)['method_name'])
        method_names.append(method_name)
        if method_name == 'info':
            if closing is None:
                closing = asyncio.create_task(sg.close())
                await asyncio.sleep(0)
                assert not closing.done()
            return _make_response({'version': [9, 0, 0]})
        if method_name == 'get_session_token':
            return _make_response({'session_id': _SESSION_ID})
        return _make_response({'id': 7})

    monkeypatch.setattr(transport, 'send', send)

    assert await _async_call(sg, 'read', None) == {'id': 7}
    assert closing is not None
    await closing
    assert method_names == ['info', 'info', 'get_session_token', 'read']


@pytest.mark.anyio
async def test_close_waits_for_prefetch_scheduled_before_first_send(
    fake_async_client: FakeAsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = fake_async_client(
        [],
        preseed_readiness=True,
        server_info={'version': [9, 0, 0], 'api_max_entities_per_page': 1},
    )
    sg.config.page_prefetch = 1
    transport = _ControlledAsyncTransport(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': {'entity_count': 2},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 2}],
                    'paging_info': {'entity_count': 2},
                }
            ),
        ]
    )
    _set_fake_async_transport(sg, transport)
    page_two_scheduled = asyncio.Event()
    release_page_two = asyncio.Event()
    original_call = AsyncDriver.call

    async def pause_page_two(
        driver: AsyncDriver,
        method_name: str,
        params: object,
        context: RequestContext,
    ) -> object:
        mapping = cast('dict[str, object]', params)
        paging = cast('dict[str, int]', mapping['paging'])
        if paging['current_page'] == 2:
            page_two_scheduled.set()
            await release_page_two.wait()
        return await original_call(
            driver,
            method_name,
            params,
            context,
        )

    monkeypatch.setattr(AsyncDriver, 'call', pause_page_two)
    find_task = asyncio.create_task(sg.find('Shot', []))
    await page_two_scheduled.wait()
    close_task = asyncio.create_task(sg.close())
    await transport.close_started.wait()
    release_page_two.set()

    assert await find_task == [
        {'type': 'Shot', 'id': 1},
        {'type': 'Shot', 'id': 2},
    ]
    await close_task
    assert transport.close_calls == 1
    assert transport.reservations == transport.reservation_releases
    assert transport.active == 0


@pytest.mark.anyio
@pytest.mark.parametrize('error_type', [SendError, ReadTimeoutError])
async def test_concurrent_ambiguous_token_exchange_failure_is_shared(
    error_type: type[SendError | ReadTimeoutError],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_ONE_USE_VALUE,
    )
    sg.config.max_rpc_attempts = 1
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            error_type('ambiguous exchange'),
            _make_response({'session_id': 'replayed-session'}),
            _make_response({'entities': [], 'paging_info': {'entity_count': 0}}),
        ],
        blocked_requests={2},
    )
    sg._transport = transport

    owner = asyncio.create_task(sg.find('Shot', []))
    await transport.wait_started(2)
    progress = next(iter(sg._token_exchanges.values()))
    waiter_entered = asyncio.Event()
    original_wait = progress.event.wait

    async def wait_for_exchange() -> bool:
        waiter_entered.set()
        return await original_wait()

    monkeypatch.setattr(progress.event, 'wait', wait_for_exchange)
    waiter = asyncio.create_task(sg.find('Shot', []))
    try:
        await asyncio.wait_for(waiter_entered.wait(), timeout=1.0)
        transport.release(2)
        results = await asyncio.wait_for(
            asyncio.gather(owner, waiter, return_exceptions=True),
            timeout=1.0,
        )
    finally:
        owner.cancel()
        waiter.cancel()
        await asyncio.gather(owner, waiter, return_exceptions=True)

    assert _method_names(transport).count('get_session_token') == 1
    assert len(results) == 2
    assert all(isinstance(result, error_type) for result in results)
    assert all(str(result) == 'ambiguous exchange' for result in results)
    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}


@pytest.mark.anyio
async def test_concurrent_cancelled_token_exchange_is_shared() -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_ONE_USE_VALUE,
    )
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'session_id': 'cancelled-session'}),
            _make_response({'session_id': 'replayed-session'}),
            _make_response(4),
        ],
        blocked_requests={2},
    )
    sg._transport = transport

    owner = asyncio.create_task(_async_call(sg, 'read', None))
    await transport.wait_started(2)
    waiter = asyncio.create_task(_async_call(sg, 'read', None))
    await asyncio.sleep(0)
    owner.cancel()

    results = await asyncio.gather(owner, waiter, return_exceptions=True)
    assert len(results) == 2
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert _method_names(transport).count('get_session_token') == 1
    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}


@pytest.mark.anyio
async def test_concurrent_connect_and_close_drain_without_stale_publication() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]})],
        blocked_requests={1},
    )
    sg._transport = transport

    connecting = asyncio.create_task(sg.connect())
    await transport.wait_started(1)
    first_close = asyncio.create_task(sg.close())
    second_close = asyncio.create_task(sg.close())
    await transport.close_started.wait()
    transport.release(1)

    await asyncio.gather(connecting, first_close, second_close)
    assert transport.close_calls == 1
    assert sg._transport is None
    assert sg.config.server_info == {}
    with pytest.raises(_errors.ShotgunError, match='await connect'):
        _ = sg.server_caps


@pytest.mark.anyio
async def test_concurrent_human_authentication_snapshots_never_leak(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client([], preseed_readiness=True)
    original_context = config_module.snapshot(sg.config)
    transport = _ControlledAsyncTransport(
        [
            _make_response(
                {
                    'entities': [{'type': 'HumanUser', 'id': 1, 'login': 'alice'}],
                    'paging_info': {'entity_count': 1},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'HumanUser', 'id': 2, 'login': 'bob'}],
                    'paging_info': {'entity_count': 1},
                }
            ),
        ],
        blocked_requests={1, 2},
    )
    _set_fake_async_transport(sg, transport)
    authentication_tasks = (
        asyncio.create_task(sg.authenticate_human_user('alice', _CREDENTIAL)),
        asyncio.create_task(sg.authenticate_human_user('bob', _NEW_CREDENTIAL)),
    )
    authentication = asyncio.gather(*authentication_tasks)
    try:
        await asyncio.wait_for(transport.wait_started(1), timeout=1.0)
        await asyncio.wait_for(transport.wait_started(2), timeout=1.0)

        assert not authentication.done()
        auths = [
            cast('list[dict[str, object]]', _payload(transport, index)['params'])[0]
            for index in range(2)
        ]
        transport.release(1)
        transport.release(2)
        results = await asyncio.wait_for(authentication, timeout=1.0)
    finally:
        transport.release(1)
        transport.release(2)
        for authentication_task in authentication_tasks:
            if not authentication_task.done():
                authentication_task.cancel()
        with suppress(BaseException):
            await authentication
        await asyncio.gather(*authentication_tasks, return_exceptions=True)

    assert all(authentication_task.done() for authentication_task in authentication_tasks)
    assert results == [
        {'type': 'HumanUser', 'id': 1, 'login': 'alice'},
        {'type': 'HumanUser', 'id': 2, 'login': 'bob'},
    ]
    assert auths == [
        {'user_login': 'alice', 'user_password': _CREDENTIAL},
        {'user_login': 'bob', 'user_password': _NEW_CREDENTIAL},
    ]
    assert config_module.snapshot(sg.config) == original_context
    assert sg.config.script_name == 's'
    assert sg.config.api_key == 'k'
    assert sg.config.user_login is None
    assert sg.config.user_password is None
    assert sg.config.auth_token is None
    assert (transport.checkouts, transport.checkins) == (4, 4)
    assert sg._lease_counts == {}


@pytest.mark.anyio
async def test_concurrent_pending_auth_coalesces_one_exchange() -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_ONE_USE_VALUE,
    )
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'session_id': 'shared-session'}),
            _make_response(1),
            _make_response(2),
        ],
        blocked_requests={2},
    )
    sg._transport = transport

    first = asyncio.create_task(_async_call(sg, 'read', {'index': 1}))
    await transport.wait_started(2)
    second = asyncio.create_task(_async_call(sg, 'read', {'index': 2}))
    await asyncio.sleep(0)
    transport.release(2)

    results = cast('tuple[int, int]', await asyncio.gather(first, second))
    assert sorted(results) == [1, 2]
    assert _method_names(transport).count('info') == 1
    assert _method_names(transport).count('get_session_token') == 1
    assert _method_names(transport).count('read') == 2
    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}


@pytest.mark.anyio
async def test_concurrent_readiness_coalesces_one_probe() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response(1), _make_response(2)],
        blocked_requests={1},
    )
    sg._transport = transport

    first = asyncio.create_task(_async_call(sg, 'read', {'index': 1}))
    await transport.wait_started(1)
    second = asyncio.create_task(_async_call(sg, 'read', {'index': 2}))
    await asyncio.sleep(0)
    transport.release(1)

    results = cast('tuple[int, int]', await asyncio.gather(first, second))
    assert sorted(results) == [1, 2]
    assert _method_names(transport).count('info') == 1
    assert _method_names(transport).count('read') == 2


@pytest.mark.anyio
async def test_connect_and_async_context_populate_then_close(
    fake_async_client: FakeAsyncClient,
) -> None:
    connected = fake_async_client([])
    await connected.connect()
    assert connected.server_caps.version == (9, 0, 0)

    managed = fake_async_client([])
    async with managed as entered:
        assert entered is managed
        assert managed.server_caps.version == (9, 0, 0)
    assert managed._transport is None


@pytest.mark.anyio
async def test_connect_false_context_entry_performs_no_io_then_stays_lazy() -> None:
    sg = AsyncShotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        connect=False,
    )
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response(
                {
                    'entities': [{'type': 'Asset', 'id': 7}],
                    'paging_info': {'entity_count': 1},
                }
            ),
        ]
    )
    sg._transport = transport

    async with sg as entered:
        assert entered is sg
        assert transport.requests == []
        assert await sg.find_one('Asset', []) == {'type': 'Asset', 'id': 7}

    assert _method_names(transport) == ['info', 'read']


@pytest.mark.anyio
async def test_connect_true_context_entry_performs_readiness_before_body() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k', connect=True)
    transport = _ControlledAsyncTransport([_make_response({'version': [9, 0, 0]})])
    sg._transport = transport

    async with sg as entered:
        assert entered is sg
        assert _method_names(transport) == ['info']


def test_constructor_matches_sync_signature_and_resolvable_hints() -> None:
    assert inspect.signature(AsyncShotgun) == inspect.signature(Shotgun)
    hints = get_type_hints(AsyncShotgun.__init__)
    assert hints['base_url'] is str
    assert hints['return'] is type(None)


def test_constructor_preserves_empty_proxy_without_proxy_settings() -> None:
    sg = AsyncShotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='',
    )

    assert sg.config.raw_http_proxy == ''
    assert sg.config.proxy_server is None
    assert sg.config.proxy_user is None
    assert sg.config.proxy_pass is None
    assert sg.config.proxy_port == 8080
    assert sg._transport is None


def test_constructor_static_contract_rejects_invalid_arguments(tmp_path: Path) -> None:
    probe = tmp_path / 'async_constructor_probe.py'
    probe.write_text(
        """from shotgun_api3 import AsyncShotgun

AsyncShotgun('https://x.example', unexpected=True)
AsyncShotgun('https://x.example', script_name=7)
""",
        encoding='utf-8',
    )
    repository = Path(__file__).parents[2]

    result = run_pyright(probe, cwd=repository)

    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    assert report['summary']['errorCount'] == 2
    diagnostics = report['generalDiagnostics']
    assert {diagnostic['rule'] for diagnostic in diagnostics} == {
        'reportArgumentType',
        'reportCallIssue',
    }


@pytest.mark.parametrize(
    ('args', 'kwargs', 'message'),
    [
        ((), {}, "AsyncShotgun.__init__() missing a required argument: 'base_url'"),
        (
            ('https://x.example',) * 13,
            {},
            'AsyncShotgun.__init__() too many positional arguments',
        ),
        (
            ('https://x.example',),
            {'unexpected': True},
            "AsyncShotgun.__init__() got an unexpected keyword argument 'unexpected'",
        ),
        (
            ('https://x.example', 's'),
            {'script_name': 'other'},
            "AsyncShotgun.__init__() multiple values for argument 'script_name'",
        ),
    ],
)
def test_constructor_type_errors_identify_async_shotgun(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    constructor = cast('Callable[..., AsyncShotgun]', AsyncShotgun)
    with pytest.raises(TypeError) as caught:
        constructor(*args, **kwargs)

    assert str(caught.value) == message


def test_constructor_validates_synchronously_without_creating_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match='script_name provided without api_key'):
        AsyncShotgun('https://x.example', script_name='s')
    with pytest.raises(ValueError, match='cannot use both'):
        AsyncShotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            login='me',
            password=_CREDENTIAL,
        )
    with pytest.raises(ValueError, match='base_url must be http or https'):
        AsyncShotgun('x.example', script_name='s', api_key='k')
    with pytest.raises(ValueError, match='http_proxy'):
        AsyncShotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            http_proxy='http://proxy.example/path',
        )

    def fail_transport(_self: AsyncShotgun, _context: object) -> object:
        raise AssertionError('constructor performed transport work')

    monkeypatch.setattr(AsyncShotgun, '_new_transport', fail_transport)
    sg = AsyncShotgun(
        'https://x.example',
        login='me',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
        connect=True,
    )
    assert sg._transport is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    'fields',
    ['code', '', b'code', b'', 0, 1, False, ['code', 1], ('code', None)],
)
async def test_create_rejects_invalid_return_fields_before_io(
    fake_async_client: FakeAsyncClient, fields: object
) -> None:
    sg = fake_async_client([])
    create = cast(
        'Callable[[str, dict[str, object], object], Coroutine[object, object, object]]', sg.create
    )

    with pytest.raises(_errors.ShotgunError, match=r'^create return_fields must be .*strings.*$'):
        await create('Shot', {}, fields)

    assert _fake_async_transport(sg).requests == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('fields_factory', 'expected'),
    [
        (lambda: None, ['id']),
        (list, ['id']),
        (tuple, ['id']),
        (lambda: ['code', ''], ['code', '']),
        (lambda: ('code', ''), ['code', '']),
        (lambda: (field for field in ['code', '']), ['code', '']),
        (lambda: (field for field in ''), []),
    ],
    ids=['none', 'empty-list', 'empty-tuple', 'list', 'tuple', 'iterator', 'empty-iterator'],
)
async def test_create_return_fields_preserve_defaults_and_detach_before_readiness(
    fake_async_client: FakeAsyncClient,
    fields_factory: Callable[[], object],
    expected: list[str],
) -> None:
    fields = fields_factory()
    sg = fake_async_client([_make_response({'type': 'Shot', 'id': 1})])
    create = cast(
        'Callable[[str, dict[str, object], object], Coroutine[object, object, object]]', sg.create
    )

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent) and isinstance(fields, list):
            fields[:] = ['changed']

    sg.config.on_response = mutate_after_readiness

    await create('Shot', {}, fields)

    request = _fake_async_transport(sg).requests[1]
    assert isinstance(request.body, bytes)
    payload = json.loads(request.body)
    assert payload['params'][1]['return_fields'] == expected


@pytest.mark.anyio
async def test_summarize_captures_nested_inputs_before_readiness_and_retry() -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    sg.config.max_rpc_attempts = 2
    sg.config.rpc_attempt_interval = 0
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            SendError('temporary send failure'),
            _make_response({'summaries': {'id': 3}}),
        ],
        blocked_requests={1},
    )
    sg._transport = transport
    nested_filter = {'names': ['original-filter']}
    summary_fields = [{'field': 'id', 'type': 'count', 'options': {'names': ['original-summary']}}]
    grouping = [{'field': 'sg_status_list', 'type': 'exact', 'options': ['original-group']}]
    summarize = cast('Callable[..., Coroutine[object, object, dict[str, object]]]', sg.summarize)

    operation = asyncio.create_task(
        summarize(
            'Shot',
            [['metadata', 'is', nested_filter]],
            summary_fields,
            grouping=grouping,
        )
    )
    await transport.wait_started(1)
    nested_filter['names'][0] = 'changed-filter'
    cast('dict[str, list[str]]', summary_fields[0]['options'])['names'][0] = 'changed-summary'
    cast('list[str]', grouping[0]['options'])[0] = 'changed-group'
    transport.release(1)

    assert await operation == {'summaries': {'id': 3}}
    assert _method_names(transport) == ['info', 'summarize', 'summarize']
    for index in (1, 2):
        params = cast('list[dict[str, object]]', _payload(transport, index)['params'])
        assert params[1] == {
            'type': 'Shot',
            'filters': {
                'logical_operator': 'and',
                'conditions': [
                    {
                        'path': 'metadata',
                        'relation': 'is',
                        'values': [{'names': ['original-filter']}],
                    }
                ],
            },
            'summaries': [
                {'field': 'id', 'type': 'count', 'options': {'names': ['original-summary']}}
            ],
            'grouping': [
                {'field': 'sg_status_list', 'type': 'exact', 'options': ['original-group']}
            ],
        }


@pytest.mark.anyio
async def test_deferred_readiness_precedes_auth_exchange_and_requested_rpc(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client(
        [_make_response({'session_id': _SESSION_ID}), _make_response({'id': 7})],
        login='me',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
        connect=True,
    )

    assert await _async_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}

    transport = _fake_async_transport(sg)
    assert _method_names(transport) == ['info', 'get_session_token', 'read']
    read_params = cast('list[dict[str, object]]', _payload(transport, 2)['params'])
    assert read_params[0] == {'session_token': _SESSION_ID}
    assert sg.config.session_token == _SESSION_ID
    assert sg.config.auth_token is None
    assert sg.config.user_login is None
    assert sg.config.user_password is None


@pytest.mark.anyio
async def test_failed_and_cancelled_readiness_attempts_are_retryable() -> None:
    failed = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    failed_transport = _AsyncFakeTransport(
        [
            ConnectError('down'),
            _make_response({'version': [9, 0, 0]}),
            _make_response(7),
        ]
    )
    failed._transport = failed_transport
    failed.config.max_rpc_attempts = 1
    with pytest.raises(ConnectError, match='down'):
        await _async_call(failed, 'read', None)
    assert await _async_call(failed, 'read', None) == 7
    assert _method_names(failed_transport) == ['info', 'info', 'read']

    cancelled = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'version': [9, 0, 0]}),
            _make_response(8),
        ],
        blocked_requests={1},
    )
    cancelled._transport = transport
    attempt = asyncio.create_task(_async_call(cancelled, 'read', None))
    await transport.wait_started(1)
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    assert await _async_call(cancelled, 'read', None) == 8
    assert _method_names(transport) == ['info', 'info', 'read']


@pytest.mark.anyio
async def test_failed_context_entry_allows_lazy_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readiness_error = ConnectError('readiness failed')
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    sg.config.max_rpc_attempts = 1
    failed_transport = _ControlledAsyncTransport([readiness_error])
    replacement_transport = _ControlledAsyncTransport([_make_response({'version': [9, 0, 0]})])
    sg._transport = failed_transport

    def new_transport(_context: RequestContext) -> _ControlledAsyncTransport:
        return replacement_transport

    monkeypatch.setattr(sg, '_new_transport', new_transport)

    with pytest.raises(ConnectError) as caught:
        await sg.__aenter__()
    assert caught.value is readiness_error

    await sg.connect()

    assert failed_transport.close_calls == 1
    assert _method_names(replacement_transport) == ['info']


@pytest.mark.anyio
async def test_failed_context_entry_closes_and_preserves_readiness_error() -> None:
    readiness_error = ConnectError('readiness failed')
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    sg.config.max_rpc_attempts = 1
    transport = _ControlledAsyncTransport([readiness_error])
    sg._transport = transport

    with pytest.raises(ConnectError) as caught:
        await sg.__aenter__()

    assert caught.value is readiness_error
    assert transport.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
async def test_failed_context_entry_first_late_cancellation_wins_after_close() -> None:
    readiness_error = ConnectError('readiness failed')
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    sg.config.max_rpc_attempts = 1
    transport = _ControlledAsyncTransport([readiness_error], block_close=True)
    sg._transport = transport

    entry_task = asyncio.create_task(sg.__aenter__())
    await asyncio.wait_for(transport.close_started.wait(), timeout=1)
    entry_task.cancel('first cancellation')
    await asyncio.sleep(0)
    entry_task.cancel('second cancellation')
    await asyncio.sleep(0)
    assert not entry_task.done()
    transport.allow_close.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await entry_task
    assert caught.value.args == ('first cancellation',)
    assert transport.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
@pytest.mark.parametrize('rollback_error_type', [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize('cancellation_at', ['none', 'old_close', 'rollback', 'repeated'])
async def test_retired_close_failure_preserves_primary_when_rollback_close_fails(
    rollback_error_type: type[BaseException],
    cancellation_at: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    diagnostics: list[dict[str, object]] = []
    monkeypatch.setattr(asyncio.get_running_loop(), 'call_exception_handler', diagnostics.append)
    primary = RuntimeError('retired close failed')
    sg = AsyncShotgun(
        'https://x.example', login='artist', password=_CREDENTIAL, auth_token=_ONE_USE_VALUE
    )
    context = config_module.snapshot(sg.config)
    exchange_key = (
        context.canonical_origin,
        context.transport_generation,
        context.auth_generation,
    )
    old = _ControlledAsyncTransport([], block_close=True, close_error=primary)
    replacement = _ControlledAsyncTransport(
        [], block_close=True, close_error=rollback_error_type('rollback close failed')
    )
    current = _ControlledAsyncTransport([])
    sg._transport = old
    async with sg._driver_lease(context):
        pass
    sg._transport = replacement
    operation = asyncio.create_task(_capture_failed_acquisition(sg, context))
    try:
        await asyncio.wait_for(old.close_started.wait(), timeout=1)
        assert sg._active_auth_contexts == {exchange_key: 1}
        sg._transport = current
        async with sg._driver_lease(context):
            assert sg._active_auth_contexts == {exchange_key: 2}
        assert id(replacement) in sg._retired_transports
        assert sg._active_auth_contexts == {exchange_key: 1}

        if cancellation_at == 'old_close':
            await _deliver_cancellation(operation, 'first cancellation')
        else:
            old.allow_close.set()
        await asyncio.wait_for(replacement.close_started.wait(), timeout=1)
        assert not sg._active_auth_contexts
        assert not operation.done()

        cancellations = {
            'none': (),
            'old_close': ('second cancellation', 'third cancellation'),
            'rollback': ('first cancellation',),
            'repeated': ('first cancellation', 'second cancellation', 'third cancellation'),
        }[cancellation_at]
        for message in cancellations:
            await _deliver_cancellation(operation, message)
            assert not operation.done()

        replacement.allow_close.set()
        outcome = await asyncio.wait_for(operation, timeout=1)
        if cancellation_at == 'none':
            assert outcome is primary
        else:
            assert isinstance(outcome, asyncio.CancelledError)
            assert outcome.args == ('first cancellation',)
        assert old.close_attempts == 1
        assert old.close_calls == (0 if cancellation_at == 'old_close' else 1)
        _assert_acquisition_rollback_released(sg, replacement)
        assert not diagnostics
    finally:
        old.allow_close.set()
        replacement.allow_close.set()
        await asyncio.wait_for(operation, timeout=1)
        await sg.close()


@pytest.mark.anyio
async def test_failed_context_entry_suppresses_close_error() -> None:
    readiness_error = ConnectError('readiness failed')
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    sg.config.max_rpc_attempts = 1
    transport = _ControlledAsyncTransport(
        [readiness_error],
        close_error=RuntimeError('close failed'),
    )
    sg._transport = transport

    with pytest.raises(ConnectError) as caught:
        await sg.__aenter__()

    assert caught.value is readiness_error
    assert transport.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
async def test_find_cancellation_closes_and_awaits_prefetched_pages(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client(
        [],
        preseed_readiness=True,
        server_info={'version': [9, 0, 0], 'api_max_entities_per_page': 1},
    )
    sg.config.page_prefetch = 2
    transport = _ControlledAsyncTransport(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': {'entity_count': 3},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 2}],
                    'paging_info': {'entity_count': 3},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': {'entity_count': 3},
                }
            ),
        ],
        blocked_requests={2, 3},
    )
    _set_fake_async_transport(sg, transport)
    operation = asyncio.create_task(sg.find('Shot', []))
    await transport.wait_started(2)
    await transport.wait_started(3)

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert transport.cancelled_requests == {2, 3}
    assert transport.checkouts == transport.checkins == 4
    assert transport.reservations == transport.reservation_releases
    assert transport.active == 0
    assert _method_names(transport) == ['read', 'read', 'read']


@pytest.mark.anyio
async def test_find_one_use_auth_exchanges_once_before_prefetched_pages(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client(
        [
            _make_response({'session_id': _SESSION_ID}),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': {'entity_count': 2},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 2}],
                    'paging_info': {'entity_count': 2},
                }
            ),
            _make_response({'entities': [], 'paging_info': {'entity_count': 2}}),
        ],
        auth_token=_ONE_USE_VALUE,
        login='artist',
        password=_CREDENTIAL,
        preseed_readiness=True,
        server_info={'version': [9, 0, 0], 'api_max_entities_per_page': 1},
    )
    sg.config.page_prefetch = 2

    assert await sg.find('Shot', [], limit=2) == [
        {'type': 'Shot', 'id': 1},
        {'type': 'Shot', 'id': 2},
    ]

    transport = _fake_async_transport(sg)
    assert _method_names(transport) == [
        'get_session_token',
        'read',
        'read',
        'read',
    ]
    for index in (1, 2, 3):
        params = cast('list[dict[str, object]]', _payload(transport, index)['params'])
        assert params[0] == {'session_token': _SESSION_ID}
    assert sg.config.session_token == _SESSION_ID
    assert sg.config.auth_token is None
    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}


@pytest.mark.anyio
async def test_first_async_find_explicit_page_is_serial(
    fake_async_client: FakeAsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_prefetch: list[int] = []
    _record_page_prefetch(monkeypatch, observed_prefetch)
    sg = fake_async_client(
        [
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 5},
                        {'type': 'Shot', 'id': 6},
                    ],
                    'paging_info': {'entity_count': 10},
                }
            )
        ],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )
    sg.config.page_prefetch = 2

    assert [entity['id'] for entity in await sg.find('Shot', [], page=3)] == [5, 6]
    transport = _fake_async_transport(sg)
    assert observed_prefetch == [0]
    assert cast(
        'dict[str, object]',
        cast('list[object]', _payload(transport, 1)['params'])[1],
    )['paging'] == {'entities_per_page': 2, 'current_page': 3}


@pytest.mark.anyio
async def test_first_async_find_keeps_auth_snapshot_when_readiness_changes_authentication() -> (
    None
):
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
    )

    def replace_authentication() -> None:
        sg.config.update_fields(
            auth_token=_NEW_AUTH_VALUE,
            user_login='new-user',
            user_password=_NEW_CREDENTIAL,
        )

    transport = _AsyncFakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
                content=(b'{"results":{"version":[9,0,0],"api_max_entities_per_page":2}}'),
                on_close=replace_authentication,
            ),
            _make_response({'session_id': 'stale-session'}),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': {'entity_count': 1},
                }
            ),
            _make_response({'session_id': _NEW_SESSION_ID}),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 2}],
                    'paging_info': {'entity_count': 1},
                }
            ),
        ]
    )
    sg._transport = transport

    assert await sg.find('Shot', [], page=1) == [{'type': 'Shot', 'id': 1}]
    first_params = cast('list[dict[str, object]]', _payload(transport, 2)['params'])
    assert first_params[0] == {'session_token': 'stale-session'}
    assert first_params[1]['paging'] == {
        'entities_per_page': 2,
        'current_page': 1,
    }
    assert sg.config.session_token is None
    assert sg.config.auth_token == _NEW_AUTH_VALUE

    assert await sg.find('Shot', [], page=1) == [{'type': 'Shot', 'id': 2}]
    second_params = cast('list[dict[str, object]]', _payload(transport, 4)['params'])
    assert second_params[0] == {'session_token': _NEW_SESSION_ID}
    assert second_params[1]['paging'] == {
        'entities_per_page': 2,
        'current_page': 1,
    }
    assert sg.config.session_token == _NEW_SESSION_ID
    assert sg.config.auth_token is None


@pytest.mark.anyio
async def test_first_async_find_keeps_leased_snapshot_when_readiness_publication_is_rejected() -> (
    None
):
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _AsyncFakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
                content=(b'{"results":{"version":[9,0,0],"api_max_entities_per_page":2}}'),
                on_close=lambda: sg.config.set_server_params('https://other.example'),
            ),
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': {'has_next_page': True},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': {'has_next_page': False},
                }
            ),
        ]
    )
    sg._transport = transport

    assert [entity['id'] for entity in await sg.find('Shot', [])] == [1, 2, 3]
    assert all(request.url == 'https://x.example:443/api3/json' for request in transport.requests)
    assert sg.config.server_info == {}


@pytest.mark.anyio
async def test_first_async_find_limit_at_page_limit_is_serial(
    fake_async_client: FakeAsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_prefetch: list[int] = []
    _record_page_prefetch(monkeypatch, observed_prefetch)
    sg = fake_async_client(
        [
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': {'entity_count': 3},
                }
            )
        ],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )
    sg.config.page_prefetch = 2

    assert [entity['id'] for entity in await sg.find('Shot', [], limit=2)] == [1, 2]
    transport = _fake_async_transport(sg)
    assert observed_prefetch == [0]
    assert cast(
        'dict[str, object]',
        cast('list[object]', _payload(transport, 1)['params'])[1],
    )['paging'] == {'entities_per_page': 2, 'current_page': 1}


@pytest.mark.anyio
async def test_first_async_find_multi_page_uses_prefetch(
    fake_async_client: FakeAsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_prefetch: list[int] = []
    _record_page_prefetch(monkeypatch, observed_prefetch)
    sg = fake_async_client(
        [
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': {'entity_count': 3},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': {'entity_count': 3},
                }
            ),
        ],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )
    sg.config.page_prefetch = 1

    assert [entity['id'] for entity in await sg.find('Shot', [], limit=3)] == [1, 2, 3]
    transport = _fake_async_transport(sg)
    assert observed_prefetch == [1]
    assert [
        cast(
            'dict[str, object]',
            cast('list[object]', _payload(transport, index)['params'])[1],
        )['paging']
        for index in (1, 2)
    ] == [
        {'entities_per_page': 2, 'current_page': 1},
        {'entities_per_page': 2, 'current_page': 2},
    ]


@pytest.mark.anyio
async def test_first_async_find_uses_readiness_page_limit(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client(
        [
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': {'has_next_page': True},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': {'has_next_page': False},
                }
            ),
        ],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )

    assert [entity['id'] for entity in await sg.find('Shot', [])] == [1, 2, 3]
    transport = _fake_async_transport(sg)
    observed: list[dict[str, object]] = []
    for index in (1, 2):
        params = cast('list[object]', _payload(transport, index)['params'])
        read_params = cast('dict[str, object]', params[1])
        observed.append(cast('dict[str, object]', read_params['paging']))
    assert observed == [
        {'entities_per_page': 2, 'current_page': 1},
        {'entities_per_page': 2, 'current_page': 2},
    ]


@pytest.mark.anyio
async def test_get_session_token_caches_and_uses_common_executor(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client(
        [_make_response({'session_id': _SESSION_ID})],
        login='me',
        password=_CREDENTIAL,
    )

    assert await sg.get_session_token() == _SESSION_ID
    assert await sg.get_session_token() == _SESSION_ID

    transport = _fake_async_transport(sg)
    assert _method_names(transport) == ['info', 'get_session_token']
    assert transport.leases == transport.checkins == 3


@pytest.mark.anyio
@pytest.mark.parametrize('invalid_page_limit', ['1.5', '9' * 5000])
async def test_invalid_page_limit_releases_readiness_waiters_and_allows_retry(
    invalid_page_limit: str,
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    invalid_metadata = {
        'version': [9, 0, 0],
        'api_max_entities_per_page': invalid_page_limit,
    }
    transport = _ControlledAsyncTransport(
        [
            _make_response(invalid_metadata),
            _make_response(invalid_metadata),
            _make_response(
                {
                    'version': [9, 0, 0],
                    'api_max_entities_per_page': '250',
                }
            ),
            _make_response({'id': 7}),
        ],
        blocked_requests={1},
    )
    sg._transport = transport

    first = asyncio.create_task(sg.connect())
    await transport.wait_started(1)
    second = asyncio.create_task(sg.connect())
    await asyncio.sleep(0)
    transport.release(1)

    results = await asyncio.wait_for(
        asyncio.gather(first, second, return_exceptions=True),
        timeout=1,
    )
    assert all(task.done() for task in (first, second))
    assert len(results) == 2
    assert all(isinstance(result, _errors.ShotgunError) for result in results)
    assert all(str(result) == 'The server returned an invalid page limit.' for result in results)
    assert sg.config.server_info == {}
    assert sg._ready_caps == {}

    assert await _async_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}
    assert sg.config.records_per_page == 250
    assert config_module.snapshot(sg.config).records_per_page == 250
    assert sg.config.server_info == {
        'version': [9, 0, 0],
        'api_max_entities_per_page': '250',
    }
    assert sg.server_info == {
        'version': [9, 0, 0],
        'api_max_entities_per_page': '250',
    }
    assert _method_names(transport) == ['info', 'info', 'info', 'read']


@pytest.mark.anyio
async def test_late_auth_context_user_reuses_completed_token_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_ONE_USE_VALUE,
    )
    transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'session_id': _SESSION_ID}),
            _make_response({'id': 1}),
            _make_response({'session_id': 'duplicate-session', 'id': 2}),
            _make_response({'id': 2}),
        ],
        blocked_requests={2},
    )
    sg._transport = transport
    original = cast(
        'Callable[[RequestContext, AsyncDriver], Coroutine[object, object, str]]',
        sg._get_session_token_for,
    )
    late_arrival_ready = asyncio.Event()
    release_late_arrival = asyncio.Event()
    late_arrival: asyncio.Task[object] | None = None

    async def delay_late_arrival(context: RequestContext, driver: AsyncDriver) -> str:
        if asyncio.current_task() is late_arrival:
            late_arrival_ready.set()
            await release_late_arrival.wait()
        return await original(context, driver)

    monkeypatch.setattr(sg, '_get_session_token_for', delay_late_arrival)
    owner = asyncio.create_task(_async_call(sg, 'read', None))
    await transport.wait_started(2)
    late_arrival = asyncio.create_task(_async_call(sg, 'read', None))
    await late_arrival_ready.wait()

    transport.release(2)
    assert await owner == {'id': 1}
    release_late_arrival.set()
    late_result = cast('dict[str, object]', await late_arrival)
    assert late_result['id'] == 2

    read_tokens: list[object] = []
    for index, method_name in enumerate(_method_names(transport)):
        if method_name == 'read':
            params = cast('list[dict[str, object]]', _payload(transport, index)['params'])
            read_tokens.append(params[0]['session_token'])
    assert _method_names(transport).count('get_session_token') == 1
    assert read_tokens == [_SESSION_ID, _SESSION_ID]
    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}


@pytest.mark.anyio
async def test_origin_change_rejects_stale_caps_and_preserves_operation_snapshot() -> None:
    sg = AsyncShotgun('https://x.example', session_token=_ORIGIN_SESSION_VALUE)
    sg.config.update_fields(
        extra_auth_params={'old-auth-marker': 'old'},
        session_uuid='old-uuid',
    )
    transport = _AsyncFakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
                content=b'{"results": {"version": [9, 0, 0]}}',
                on_close=lambda: sg.config.set_server_params('https://other.example'),
            ),
            _make_response({'id': 7}),
        ]
    )
    sg._transport = transport

    assert await _async_call(sg, 'read', None) == {'id': 7}
    assert transport.requests[1].url == 'https://x.example:443/api3/json'
    assert b'old-session' in cast('bytes', transport.requests[1].body)
    assert b'old-uuid' in cast('bytes', transport.requests[1].body)
    assert b'old-auth-marker' in cast('bytes', transport.requests[1].body)
    assert sg.config.server_info == {}
    assert config_module.snapshot(sg.config).auth == {}
    assert sg.config.extra_auth_params == {}
    assert sg._ready_caps == {}
    with pytest.raises(_errors.ShotgunError, match='await connect'):
        _ = sg.server_caps


@pytest.mark.anyio
async def test_pre_send_token_exchange_failure_wakes_waiter_for_retry() -> None:
    failed = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_ONE_USE_VALUE,
    )
    failed.config.max_rpc_attempts = 1
    failed_transport = _ControlledAsyncTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            ConnectError('exchange failed'),
            _make_response({'session_id': 'retried-session'}),
            _make_response(3),
        ],
        blocked_requests={2},
    )
    failed._transport = failed_transport
    failed_owner = asyncio.create_task(_async_call(failed, 'read', None))
    await failed_transport.wait_started(2)
    failed_waiter = asyncio.create_task(_async_call(failed, 'read', None))
    await asyncio.sleep(0)
    failed_transport.release(2)
    failed_results = await asyncio.gather(
        failed_owner,
        failed_waiter,
        return_exceptions=True,
    )
    assert any(isinstance(result, ConnectError) for result in failed_results)
    assert 3 in failed_results
    assert _method_names(failed_transport).count('get_session_token') == 2
    assert failed._active_auth_contexts == {}
    assert failed._token_exchanges == {}


@pytest.mark.anyio
async def test_public_info_is_raw_unauthenticated_and_reused_for_readiness(
    fake_async_client: FakeAsyncClient,
) -> None:
    sg = fake_async_client([_make_response({'id': 7})])

    assert await sg.info() == {'version': [9, 0, 0], 'api_max_entities_per_page': 500}
    assert await _async_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}

    transport = _fake_async_transport(sg)
    assert _method_names(transport) == ['info', 'read']
    assert _payload(transport, 0)['params'] == []
    assert sg.server_caps.version == (9, 0, 0)
    assert sg.server_info['api_max_entities_per_page'] == 500


@pytest.mark.anyio
async def test_public_retarget_never_sends_old_auth_to_new_origin() -> None:
    sg = AsyncShotgun('https://old.example', session_token=_ORIGIN_SESSION_VALUE)
    sg.config.update_fields(
        api_key='old-key',
        auth_token=_ORIGIN_AUTH_VALUE,
        extra_auth_params={'old-auth-marker': 'old'},
        script_name='old-script',
        session_uuid='old-uuid',
        sudo_as_login='old-sudo',
        user_login='old-user',
        user_password=_ORIGIN_CREDENTIAL,
    )
    sg.config.set_server_params('https://new.example')
    transport = _AsyncFakeTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response({'id': 7})]
    )
    sg._transport = transport

    assert await _async_call(sg, 'read', None) == {'id': 7}

    context = config_module.snapshot(sg.config)
    assert context.canonical_origin == 'https://new.example:443'
    assert context.session_token is None
    assert context.session_uuid is None
    assert context.auth == {}
    assert context.extra_auth_params == {}
    assert all(
        old_value not in cast('bytes', request.body)
        for request in transport.requests
        for old_value in (
            b'old-auth-token',
            b'old-key',
            b'old-password',
            b'old-session',
            b'old-sudo',
            b'old-user',
            b'old-uuid',
        )
    )
    assert all(
        b'old-auth-marker' not in cast('bytes', request.body) for request in transport.requests
    )


@pytest.mark.anyio
async def test_readiness_finalization_cancellation_remains_primary_during_entry_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transport = _ControlledAsyncTransport(
        [_make_response({'version': [9, 0, 0]})],
        block_close=True,
        blocked_requests={1},
    )
    sg._transport = transport
    original_join = sg._join_readiness_finalizer
    first_cancellation: asyncio.CancelledError | None = None
    join_started = asyncio.Event()

    async def capture_first_cancellation(
        finalizer: asyncio.Task[None],
        *,
        preserve_cancellation: bool,
    ) -> None:
        nonlocal first_cancellation
        join_started.set()
        try:
            await original_join(
                finalizer,
                preserve_cancellation=preserve_cancellation,
            )
        except asyncio.CancelledError as error:
            first_cancellation = error
            raise

    monkeypatch.setattr(sg, '_join_readiness_finalizer', capture_first_cancellation)
    entry_task = asyncio.create_task(sg.__aenter__())
    await transport.wait_started(1)
    readiness_lock = sg._readiness_lock
    await readiness_lock.acquire()
    try:
        transport.release(1)
        await join_started.wait()
        entry_task.cancel('first cancellation')
        await asyncio.sleep(0)
        assert entry_task.cancelling() == 0
    finally:
        readiness_lock.release()

    await asyncio.wait_for(transport.close_started.wait(), timeout=1)
    assert first_cancellation is not None
    entry_task.cancel('second cancellation')
    await asyncio.sleep(0)
    assert not entry_task.done()
    transport.allow_close.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await entry_task
    assert caught.value is first_cancellation
    assert caught.value.args == ('first cancellation',)
    assert transport.close_calls == 1
    assert sg._transport is None


@pytest.mark.anyio
async def test_repeated_auth_rotations_do_not_retain_exchanged_tokens() -> None:
    rotation_count = 5
    sg = AsyncShotgun(
        'https://x.example',
        login='artist-0',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
    )
    transport = _AsyncFakeTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            *(
                _make_response({'session_id': f'session-{index}'})
                for index in range(rotation_count)
            ),
        ]
    )
    sg._transport = transport

    for index in range(rotation_count):
        if index:
            sg.config.update_fields(
                auth_token=f'{_AUTH_VALUE}-{index}',
                session_token=None,
                user_login=f'artist-{index}',
                user_password=_CREDENTIAL,
            )
        assert await sg.get_session_token() == f'session-{index}'

    assert sg._active_auth_contexts == {}
    assert sg._token_exchanges == {}
    assert _method_names(transport).count('get_session_token') == rotation_count


@pytest.mark.anyio
async def test_repeated_close_and_connect_prune_stale_readiness_generations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycle_count = 5
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')
    transports = [
        _AsyncFakeTransport([_make_response({'version': [9, 0, 0]})]) for _ in range(cycle_count)
    ]

    def new_transport(_context: RequestContext) -> _AsyncFakeTransport:
        return transports.pop(0)

    monkeypatch.setattr(sg, '_new_transport', new_transport)

    for _ in range(cycle_count):
        await sg.connect()
        await sg.close()

    ready_caps = sg._ready_caps
    assert ready_caps == {}


@pytest.mark.anyio
async def test_stale_token_cannot_publish_but_initiating_snapshot_uses_it() -> None:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=_AUTH_VALUE,
    )

    def replace_state() -> None:
        sg.config.set_server_params('https://new.example')
        sg.config.update_fields(
            auth_token=_NEW_AUTH_VALUE,
            user_login='new-user',
            user_password=_NEW_CREDENTIAL,
        )

    transport = _AsyncFakeTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            TransportResponse(
                status=200,
                reason='OK',
                headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
                content=b'{"results": {"session_id": "stale-session"}}',
                on_close=replace_state,
            ),
            _make_response({'id': 9}),
        ]
    )
    sg._transport = transport

    assert await _async_call(sg, 'read', None) == {'id': 9}
    assert transport.requests[2].url == 'https://x.example:443/api3/json'
    params = cast('list[dict[str, object]]', _payload(transport, 2)['params'])
    assert params[0] == {'session_token': 'stale-session'}
    assert sg.config.session_token is None
    assert sg.config.auth_token == _NEW_AUTH_VALUE
    assert sg.config.user_login == 'new-user'


def test_sync_config_mutators_share_versioned_user_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_configs: list[Config] = []
    original_reset = Config.reset_user_agent

    def reset_user_agent(config: Config) -> None:
        reset_configs.append(config)
        original_reset(config)

    monkeypatch.setattr(Config, 'reset_user_agent', reset_user_agent)
    sync_sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg = AsyncShotgun('https://x.example', script_name='s', api_key='k')

    assert reset_configs == []
    assert sg.base_url == 'https://x.example'
    assert sg.config.user_agents[0] == 'shotgun-json (0.1.2)'
    sg.add_user_agent('integration/1')
    assert sg.config.user_agents[-1] == 'integration/1'
    sync_sg.reset_user_agent()
    sg.reset_user_agent()
    assert reset_configs == [sync_sg.config, sg.config]
    assert 'integration/1' not in sg.config.user_agents
    sg.set_session_uuid('session-7')
    assert sg.config.session_uuid == 'session-7'
