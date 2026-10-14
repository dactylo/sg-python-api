import base64
import concurrent.futures
import hmac
import inspect
import json
import socket
import sys
import threading
from collections.abc import Callable, Collection, Generator
from contextlib import contextmanager, suppress
from typing import Never, cast
from unittest.mock import MagicMock

import pytest

from shotgun_api3 import _capabilities as capabilities_module
from shotgun_api3 import _client as client_module
from shotgun_api3 import _config as config_module
from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import Config, RequestContext, readiness_key
from shotgun_api3._transport import (
    ConnectError,
    ProxyError,
    Transport,
    TransportRequest,
    TransportResponse,
    Urllib3Transport,
)
from tests._transports import _FakeTransport
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

_ASCII_CONTROL_CODES = (*range(32), 127)
_CREDENTIAL = 'credential-value'
_NEW_AUTH_VALUE = 'new-token'
_NEW_CREDENTIAL = 'new-password'
_OLD_AUTH_VALUE = 'old-token'
_OLD_CREDENTIAL = 'old-password'
_ORIGIN_AUTH_VALUE = 'old-auth-token'
_ORIGIN_CREDENTIAL = 'old-password'
_ORIGIN_SESSION_VALUE = 'old-session'
_PROXY_AUTH = 'p'
_PROXY_AUTH_VALUE = 'p/a?s#s'
_PROXY_USER = 'proxy-user'
_SESSION_ID = 'session-1'

_INSECURE_RPC_ERROR = 'authenticated RPCs require HTTPS or a direct literal loopback HTTP origin'


class _BlockingTransport:
    def __init__(self, responses: list[TransportResponse]) -> None:
        self.close_calls = 0
        self.entered_operation = threading.Event()
        self.release_operation = threading.Event()
        self.requests: list[TransportRequest] = []
        self._responses = responses

    def abort(self, token: str) -> None:
        del token

    def close(self) -> None:
        self.close_calls += 1

    def send(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        if len(self.requests) > 1:
            self.entered_operation.set()
            assert self.release_operation.wait(timeout=5)
        return self._responses.pop(0)


def _assert_proxy_authorization(
    observed: str | None,
    user: str,
    password: str,
) -> None:
    expected_token = base64.b64encode(f'{user}:{password}'.encode('latin-1')).decode('ascii')
    expected = f'Basic {expected_token}'
    if observed is None or not hmac.compare_digest(observed, expected):
        raise AssertionError('proxy authorization was missing or invalid')


def _client_call(sg: Shotgun, method_name: str, params: object) -> object:
    callback = cast('Callable[[str, object], object]', sg._call)
    return callback(method_name, params)


def _connection_view(sg: Shotgun) -> Transport | None:
    return sg._connection


def _finish_daemon_calls(
    calls: Collection[tuple[threading.Thread, threading.Event, list[BaseException]]],
) -> None:
    for worker, _finished, _recorded in calls:
        worker.join(timeout=5)
    assert all(not worker.is_alive() for worker, _finished, _recorded in calls)
    assert all(finished.is_set() for _worker, finished, _recorded in calls)
    errors = [error for _worker, _finished, recorded in calls for error in recorded]
    if errors:
        raise errors[0]


def _method_names(sg: Shotgun) -> list[object]:
    names: list[object] = []
    for request in _fake_transport(sg).requests:
        assert isinstance(request.body, bytes)
        names.append(json.loads(request.body)['method_name'])
    return names


def _proxy_response(body: bytes = b'') -> bytes:
    return (
        b'HTTP/1.1 200 OK\r\n'
        b'Content-Type: application/json\r\n'
        + f'Content-Length: {len(body)}\r\n'.encode()
        + b'Connection: close\r\n\r\n'
        + body
    )


def _run_in_threads(call: Callable[[], object], count: int) -> list[object]:
    barrier = threading.Barrier(count)

    def run() -> object:
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as executor:
        futures = [executor.submit(run) for _ in range(count)]
        return [future.result(timeout=5) for future in futures]


def _serve_proxy(
    listener: socket.socket,
    response: bytes,
    observed: list[str | None],
) -> None:
    connection, _address = listener.accept()
    with connection:
        connection.settimeout(5)
        request = b''
        while b'\r\n\r\n' not in request:
            chunk = connection.recv(4096)
            if not chunk:
                raise EOFError('proxy request headers ended before delimiter')
            request += chunk
        authorization: str | None = None
        header_block, _delimiter, body_prefix = request.partition(b'\r\n\r\n')
        content_length = 0
        for line in header_block.split(b'\r\n')[1:]:
            name, separator, value = line.partition(b':')
            if separator and name.lower() == b'proxy-authorization':
                authorization = value.strip().decode('latin-1')
            elif separator and name.lower() == b'content-length':
                content_length = int(value.strip())
        if header_block.partition(b' ')[0] != b'CONNECT':
            remaining = content_length - len(body_prefix)
            while remaining > 0:
                chunk = connection.recv(min(4096, remaining))
                if not chunk:
                    raise EOFError('proxy request body ended before Content-Length')
                remaining -= len(chunk)
        observed.append(authorization)
        connection.sendall(response)


@contextmanager
def _loopback_proxy(response: bytes) -> Generator[tuple[str, list[str | None]], None, None]:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        listener.settimeout(5)
        host, port = listener.getsockname()
        observed: list[str | None] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            worker = executor.submit(_serve_proxy, listener, response, observed)
            try:
                yield f'{host}:{port}', observed
            finally:
                worker.result(timeout=5)


def _start_daemon_call(
    call: Callable[[], object],
) -> tuple[threading.Thread, threading.Event, list[BaseException]]:
    errors: list[BaseException] = []
    finished = threading.Event()

    def run() -> None:
        with suppress(BaseException):
            try:
                call()
            finally:
                error = sys.exception()
                if error is not None:
                    errors.append(error)
                finished.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker, finished, errors


def test_auth_token_constructor_runs_readiness_then_exchange_when_deferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _FakeTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response({'session_id': _SESSION_ID})]
    )

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)

    sg = Shotgun(
        'https://x.example',
        login='me',
        password=_CREDENTIAL,
        auth_token=_CREDENTIAL,
        connect=False,
    )

    assert _method_names(sg) == [
        'info',
        'get_session_token',
    ]
    session_token = sg.config.session_token
    assert session_token == _SESSION_ID
    assert sg.config.auth_token is None
    assert sg.config.user_login is None
    assert sg.config.user_password is None


def test_authenticated_http_find_rejects_before_driver_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    def reject_transport(**_kwargs: object) -> _FakeTransport:
        raise AssertionError('find acquired its driver before the security guard')

    monkeypatch.setattr(client_module, 'Urllib3Transport', reject_transport)
    sg = Shotgun(
        'http://remote.example',
        script_name='s',
        api_key='k',
        connect=False,
    )

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        sg.find('Asset', [])


def test_authenticated_http_guard_allows_public_unauthenticated_info() -> None:
    sg = Shotgun(
        'http://remote.example',
        script_name='s',
        api_key='k',
        connect=False,
    )
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])
    sg._transport = transport

    assert sg.info() == {'version': [9, 0, 0]}
    assert _method_names(sg) == ['info']


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
def test_authenticated_http_guard_allows_secure_and_loopback_rpc(
    base_url: str,
    http_proxy: str | None,
) -> None:
    sg = Shotgun(base_url, http_proxy=http_proxy, script_name='s', api_key='k', connect=False)
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]}), _make_response(obj=True)])
    sg._transport = transport

    assert sg.delete('Asset', 1) is True
    assert _method_names(sg) == ['info', 'delete']


def test_authenticated_http_invoke_rejects_before_driver_call() -> None:

    class RejectingDriver:
        def call(self, *_args: object, **_kwargs: object) -> Never:
            raise AssertionError('driver call preceded the security guard')

    sg = Shotgun(
        'http://remote.example',
        script_name='s',
        api_key='k',
        connect=False,
    )
    invoke = cast('Callable[..., object]', sg._invoke)

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        invoke(
            RejectingDriver(),
            'read',
            {},
            config_module.snapshot(sg.config),
            client_module._InvocationOptions(),
        )


def test_authenticated_http_proxy_receives_basic_authorization() -> None:
    body = b'{"results": {"version": [9, 0, 0]}}'
    with _loopback_proxy(_proxy_response(body)) as (authority, observed):
        sg = Shotgun(
            'http://target.example',
            script_name='s',
            api_key='k',
            http_proxy=f'{_PROXY_USER}:{_PROXY_AUTH_VALUE}@{authority}',
        )
        assert sg.server_caps.version == (9, 0, 0)

    _assert_proxy_authorization(
        observed[0] if observed else None,
        _PROXY_USER,
        _PROXY_AUTH_VALUE,
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
def test_authenticated_http_rpc_rejects_before_transport_and_observer(
    monkeypatch: pytest.MonkeyPatch,
    base_url: str,
    http_proxy: str | None,
) -> None:
    constructions: list[dict[str, object]] = []

    def make_transport(**kwargs: object) -> _FakeTransport:
        constructions.append(kwargs)
        return _FakeTransport([])

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun(
        base_url,
        script_name='s',
        api_key='k',
        http_proxy=http_proxy,
        connect=False,
    )
    events: list[object] = []
    sg.config.on_request = events.append

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        sg.delete('Asset', 1)

    assert constructions == []
    assert events == []


def test_authenticated_http_session_token_rejects_before_driver_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    def reject_transport(**_kwargs: object) -> _FakeTransport:
        raise AssertionError('token exchange acquired its driver before the security guard')

    monkeypatch.setattr(client_module, 'Urllib3Transport', reject_transport)
    sg = Shotgun(
        'http://remote.example',
        script_name='s',
        api_key='k',
        connect=False,
    )

    with pytest.raises(ValueError, match=f'^{_INSECURE_RPC_ERROR}$'):
        sg.get_session_token()


def test_authenticated_https_connect_receives_basic_authorization() -> None:
    response = (
        b'HTTP/1.1 407 Proxy Authentication Required\r\n'
        b'Content-Length: 0\r\n'
        b'Connection: close\r\n\r\n'
    )
    with _loopback_proxy(response) as (authority, observed):
        sg = Shotgun(
            'https://target.example',
            script_name='s',
            api_key='k',
            http_proxy=f'{_PROXY_USER}:{_PROXY_AUTH_VALUE}@{authority}',
            connect=False,
        )
        with pytest.raises(ProxyError):
            sg.info()

    _assert_proxy_authorization(
        observed[0] if observed else None,
        _PROXY_USER,
        _PROXY_AUTH_VALUE,
    )


@pytest.mark.parametrize('invalid_config', [False, True])
def test_cached_session_token_validates_config_without_acquiring_driver(
    monkeypatch: pytest.MonkeyPatch,
    invalid_config: bool,
) -> None:
    sg = Shotgun('https://x.example', session_token=_SESSION_ID, connect=False)

    def reject_driver(_context: RequestContext) -> Never:
        raise AssertionError('cached token acquired a driver')

    monkeypatch.setattr(sg, '_driver_lease', reject_driver)
    if invalid_config:
        sg.config.update_fields(proxy_server='proxy.local', proxy_user='unpaired-user')
        with pytest.raises(ValueError, match='proxy'):
            sg.get_session_token()
    else:
        assert sg.get_session_token() == _SESSION_ID
    assert _connection_view(sg) is None


def test_capability_containers_parse_supported_server() -> None:
    sg = _fake_client([], server_info={'version': [9, 1, 2, 'Dev']})

    caps = sg.server_caps

    assert caps.host == 'x.example'
    assert caps.server_info == {'version': [9, 1, 2, 'Dev']}
    assert caps.version == (9, 1, 2)
    assert caps.is_dev is True
    assert caps.ensure_include_archived_projects() is True
    assert caps.ensure_paging_info_without_counts_support() is True
    assert caps.ensure_per_project_customization() is True
    assert caps.ensure_return_image_urls_support() is True
    assert caps.ensure_support_for_additional_filter_presets() is True
    assert caps.ensure_user_following_support() is True


def test_capability_metadata_is_detached_from_caller_and_accessors() -> None:
    metadata: dict[str, object] = {
        'version': [9, 1, 2],
        's3_enabled_upload_types': {'*': ['*']},
    }
    caps = capabilities_module.ServerCapabilities('x.example', metadata)

    cast('list[int]', metadata['version'])[0] = 7
    cast('dict[str, list[str]]', metadata['s3_enabled_upload_types'])['*'].clear()
    exposed = caps.server_info
    cast('list[int]', exposed['version'])[1] = 0
    cast('dict[str, list[str]]', exposed['s3_enabled_upload_types'])['*'].clear()

    assert caps.server_info == {
        'version': [9, 1, 2],
        's3_enabled_upload_types': {'*': ['*']},
    }


def test_capability_publication_rejects_origin_change_before_atomic_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = _fake_client(
        [_make_response({'version': [10, 0, 0], 'api_max_entities_per_page': 250})],
        server_info={'version': [9, 0, 0], 'api_max_entities_per_page': 123},
    )
    original_publish = config_module.publish_server_info
    publish_calls = 0

    def publish(
        config: Config,
        context: RequestContext,
        server_info: dict[str, object],
    ) -> bool:
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 1:
            config.set_server_params('https://new.example')
        return original_publish(config, context, server_info)

    monkeypatch.setattr(config_module, 'publish_server_info', publish)

    assert sg.server_caps.version == (9, 0, 0)
    assert sg.config.server_info == {}
    assert sg.server_caps.version == (10, 0, 0)
    assert sg.config.records_per_page == 250


def test_client_capabilities_report_ssl_version() -> None:
    sg = _fake_client([])

    assert sg.client_caps.ssl_version == capabilities_module.ssl.OPENSSL_VERSION


@pytest.mark.parametrize('error_type', [RuntimeError, SystemExit])
def test_close_attempts_both_replaced_transports_and_preserves_first_failure(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    sg = _fake_client([])
    old = _fake_transport(sg)
    replacement = _FakeTransport([])
    sg.connect()
    first_error = error_type('old close failed')
    close_calls: list[str] = []

    def close_old() -> None:
        close_calls.append('old')
        raise first_error

    def close_replacement() -> None:
        close_calls.append('replacement')
        raise RuntimeError('replacement close failed')

    monkeypatch.setattr(old, 'close', close_old)
    monkeypatch.setattr(replacement, 'close', close_replacement)
    sg._transport = replacement

    with pytest.raises(error_type) as raised:
        sg.close()

    assert raised.value is first_error
    assert close_calls == ['old', 'replacement']
    assert _connection_view(sg) is None
    sg.close()
    assert close_calls == ['old', 'replacement']


@pytest.mark.parametrize('replace_transport', [False, True])
def test_close_closes_each_distinct_known_and_current_transport_once(
    replace_transport: bool,
) -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    old = _BlockingTransport([_make_response({'version': [9, 0, 0]})])
    replacement = _BlockingTransport([]) if replace_transport else old
    sg._transport = cast('Transport', old)
    sg.connect()
    sg._transport = cast('Transport', replacement)

    sg.close()
    sg.close()

    assert old.close_calls == 1
    assert replacement.close_calls == 1
    assert _connection_view(sg) is None


def test_close_defers_replaced_transport_close_until_inflight_request_finishes() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    old = _BlockingTransport([_make_response({'version': [9, 0, 0]}), _make_response({'id': 7})])
    replacement = _BlockingTransport([])
    sg._transport = cast('Transport', old)
    results: list[object] = []
    worker = _start_daemon_call(lambda: results.append(_client_call(sg, 'read', {'type': 'Shot'})))
    try:
        assert old.entered_operation.wait(timeout=5)
        sg._transport = cast('Transport', replacement)
        sg.close()
        sg.close()
        assert old.close_calls == 0
        assert replacement.close_calls == 1
        assert _connection_view(sg) is None
    finally:
        old.release_operation.set()
        _finish_daemon_calls([worker])

    assert results == [{'id': 7}]
    assert old.close_calls == 1
    assert replacement.close_calls == 1


def test_close_defers_transport_close_until_inflight_request_finishes() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transport = _BlockingTransport(
        [_make_response({'version': [9, 0, 0]}), _make_response({'id': 7})]
    )
    sg._transport = cast('Transport', transport)
    results: list[object] = []
    worker = threading.Thread(
        target=lambda: results.append(_client_call(sg, 'read', {'type': 'Shot'}))
    )

    assert _connection_view(sg) is transport
    worker.start()
    assert transport.entered_operation.wait(timeout=5)
    try:
        sg.close()
        connection_after_close = _connection_view(sg)
        close_calls_during_io = transport.close_calls
    finally:
        transport.release_operation.set()
        worker.join(timeout=5)

    assert not worker.is_alive()
    assert connection_after_close is None
    assert close_calls_during_io == 0
    assert results == [{'id': 7}]
    assert transport.close_calls == 1


def test_close_preserves_sync_readiness_from_concurrent_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    old_transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])
    new_transport = _FakeTransport([_make_response({'version': [10, 0, 0]})])
    sg._transport = old_transport
    sg.connect()
    close_started = threading.Event()
    release_close = threading.Event()

    def blocking_close() -> None:
        close_started.set()
        assert release_close.wait(timeout=5)

    def replace_transport(_context: RequestContext) -> _FakeTransport:
        return new_transport

    monkeypatch.setattr(old_transport, 'close', blocking_close)
    monkeypatch.setattr(sg, '_new_transport', replace_transport)
    closing = _start_daemon_call(sg.close)
    started = [closing]
    try:
        assert close_started.wait(timeout=5)
        reconnecting = _start_daemon_call(sg.connect)
        started.append(reconnecting)
        assert reconnecting[1].wait(timeout=5)
    finally:
        release_close.set()
        _finish_daemon_calls(started)

    current_key = readiness_key(config_module.snapshot(sg.config))
    ready_caps = sg._ready_caps
    assert set(ready_caps) == {current_key}
    assert ready_caps[current_key].version == (10, 0, 0)


def test_close_prunes_readiness_without_validating_unrelated_config() -> None:
    sg = _fake_client([])
    sg.config.proxy_server = 'proxy.local'
    sg.connect()
    sg.config.update_fields(proxy_user='unpaired-user')

    sg.close()

    assert sg._ready_caps == {}


def test_close_rejects_late_sync_readiness_cache_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg._transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])
    original = cast(
        'Callable[[RequestContext, capabilities_module.ServerCapabilities], bool]',
        sg._publish_capabilities,
    )
    published = threading.Event()
    release = threading.Event()

    def pause_after_publication(
        context: RequestContext,
        capabilities: capabilities_module.ServerCapabilities,
    ) -> bool:
        result = original(context, capabilities)
        published.set()
        assert release.wait(timeout=5)
        return result

    monkeypatch.setattr(sg, '_publish_capabilities', pause_after_publication)
    connecting = _start_daemon_call(sg.connect)
    started = [connecting]
    try:
        assert published.wait(timeout=5)
        closing = _start_daemon_call(sg.close)
        started.append(closing)
        assert closing[1].wait(timeout=5)
    finally:
        release.set()
        _finish_daemon_calls(started)

    assert sg._ready_caps == {}


def test_close_rejects_session_token_publication_from_retired_transport() -> None:
    sg = Shotgun(
        'https://x.example',
        login='artist',
        password=_OLD_CREDENTIAL,
        connect=False,
    )
    sg.config.auth_token = _OLD_AUTH_VALUE
    transport = _BlockingTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'session_id': _SESSION_ID}),
        ]
    )
    sg._transport = cast('Transport', transport)
    original_generation = config_module.snapshot(sg.config).transport_generation
    results: list[str] = []
    worker = threading.Thread(target=lambda: results.append(sg.get_session_token()))

    worker.start()
    assert transport.entered_operation.wait(timeout=5)
    sg.close()
    retired_generation = config_module.snapshot(sg.config).transport_generation
    close_calls_during_io = transport.close_calls
    transport.release_operation.set()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert retired_generation == original_generation + 1
    assert close_calls_during_io == 0
    assert results == [_SESSION_ID]
    assert sg.config.session_token is None
    assert sg.config.auth_token == _OLD_AUTH_VALUE
    assert sg.config.user_login == 'artist'
    assert sg.config.user_password == _OLD_CREDENTIAL
    assert transport.close_calls == 1


def test_concurrent_first_calls_share_one_readiness_probe() -> None:
    sg = _fake_client([_make_response({'id': 1}), _make_response({'id': 2})])

    results = _run_in_threads(lambda: _client_call(sg, 'read', {'type': 'Shot'}), 2)

    result_ids: list[int] = []
    for result in results:
        assert isinstance(result, dict)
        result_id = cast('dict[str, object]', result).get('id')
        assert isinstance(result_id, int)
        result_ids.append(result_id)
    assert sorted(result_ids) == [1, 2]
    assert _method_names(sg).count('info') == 1
    assert _method_names(sg).count('read') == 2


def test_connect_false_defers_readiness_until_an_operation() -> None:
    sg = _fake_client([_make_response({'id': 7})])

    assert _fake_transport(sg).requests == []
    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}
    assert _method_names(sg) == ['info', 'read']
    assert _sent_payload(sg)['method_name'] == 'read'


def test_connect_true_eagerly_runs_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)

    sg = Shotgun('https://x.example', script_name='s', api_key='k')

    assert sg.server_caps.version == (9, 0, 0)
    assert len(transport.requests) == 1


def test_connection_close_reaches_underlying_transport() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transport = _BlockingTransport([])
    sg._transport = cast('Transport', transport)

    connection = _connection_view(sg)
    assert connection is transport
    assert connection is not None
    connection.close()

    assert transport.close_calls == 1


def test_connection_identity_is_stable_across_info(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _FakeTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            _make_response({'version': [9, 0, 0]}),
        ]
    )

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg.connect()
    connection = _connection_view(sg)

    assert sg.info() == {'version': [9, 0, 0]}
    assert _connection_view(sg) is connection


def test_connection_is_installed_transport_after_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)

    sg.connect()

    connection = _connection_view(sg)
    assert connection is transport
    assert connection is not None
    assert callable(connection.close)


def test_connection_is_none_after_public_close(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg.connect()

    sg.close()

    assert _connection_view(sg) is None


def test_connection_is_none_when_connect_is_false() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)

    assert _connection_view(sg) is None


def test_connection_is_read_only() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)

    with pytest.raises(AttributeError):
        object.__setattr__(sg, '_connection', None)

    assert '_connection' not in vars(sg)


@pytest.mark.parametrize('http_proxy', [None, ''])
def test_connection_proxy_info_is_none_without_proxy(
    monkeypatch: pytest.MonkeyPatch,
    http_proxy: str | None,
) -> None:
    def send(
        _transport: Urllib3Transport,
        _request: TransportRequest,
    ) -> TransportResponse:
        return _make_response({'version': [9, 0, 0]})

    monkeypatch.setattr(Urllib3Transport, 'send', send)
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy=http_proxy,
        connect=False,
    )

    try:
        sg.connect()
        connection = _connection_view(sg)
        assert isinstance(connection, Urllib3Transport)
        assert connection.proxy_info is None
    finally:
        sg.close()


def test_connection_proxy_info_is_unavailable_with_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def send(
        _transport: Urllib3Transport,
        _request: TransportRequest,
    ) -> TransportResponse:
        return _make_response({'version': [9, 0, 0]})

    monkeypatch.setattr(Urllib3Transport, 'send', send)
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='proxy.example:8080',
        connect=False,
    )

    try:
        sg.connect()
        connection = _connection_view(sg)
        assert isinstance(connection, Urllib3Transport)
        with pytest.raises(AttributeError, match='proxy_info'):
            _ = connection.proxy_info
    finally:
        sg.close()


def test_constructor_exposes_the_documented_signature() -> None:
    class_signature = inspect.signature(Shotgun)
    method_signature = inspect.signature(Shotgun.__init__)

    parameter_names = (
        'base_url',
        'script_name',
        'api_key',
        'convert_datetimes_to_utc',
        'http_proxy',
        'connect',
        'ca_certs',
        'login',
        'password',
        'sudo_as_login',
        'session_token',
        'auth_token',
    )
    assert tuple(class_signature.parameters) == parameter_names
    assert tuple(method_signature.parameters) == ('self', *parameter_names)
    expected_annotations = (
        str,
        str | None,
        str | None,
        bool,
        str | None,
        bool,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    )
    assert (
        tuple(parameter.annotation for parameter in class_signature.parameters.values())
        == expected_annotations
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in class_signature.parameters.values()
    )
    expected_defaults = (
        inspect.Parameter.empty,
        None,
        None,
        True,
        None,
        True,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    for parameter, expected_default in zip(
        class_signature.parameters.values(),
        expected_defaults,
        strict=True,
    ):
        assert parameter.default is expected_default
    for parameter, expected_default in zip(
        method_signature.parameters.values(),
        (inspect.Parameter.empty, *expected_defaults),
        strict=True,
    ):
        assert parameter.default is expected_default
    assert class_signature.return_annotation is None
    assert method_signature.return_annotation is None


def test_constructor_package_internal_binder_and_signature_helper() -> None:
    options = client_module.bind_constructor(
        ('https://x.example',),
        {'script_name': 's', 'api_key': 'k', 'connect': False},
        'SiblingClient',
    )

    assert isinstance(options, client_module.ConstructorOptions)
    assert options == client_module.ConstructorOptions(
        base_url='https://x.example',
        script_name='s',
        api_key='k',
        connect=False,
    )
    assert inspect.signature(Shotgun) == client_module.CONSTRUCTOR_SIGNATURE

    def variadic(*args: object, **kwargs: object) -> None:
        del args, kwargs

    exposed = client_module.expose_constructor_signature(variadic)

    assert exposed is variadic
    assert inspect.signature(exposed) == inspect.signature(Shotgun.__init__)
    with pytest.raises(
        TypeError,
        match=r"^SiblingClient\.__init__\(\) missing a required argument: 'base_url'$",
    ):
        client_module.bind_constructor((), {}, 'SiblingClient')


def test_constructor_preserves_empty_proxy_without_proxy_settings() -> None:
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='',
        connect=False,
    )

    assert sg.config.raw_http_proxy == ''
    assert sg.config.proxy_server is None
    assert sg.config.proxy_user is None
    assert sg.config.proxy_pass is None
    assert sg.config.proxy_port == 8080


@pytest.mark.parametrize(
    ('args', 'kwargs', 'message'),
    [
        ((), {}, "Shotgun.__init__() missing a required argument: 'base_url'"),
        (
            ('https://x.example',) * 13,
            {},
            'Shotgun.__init__() too many positional arguments',
        ),
        (
            ('https://x.example',),
            {'unexpected': True},
            "Shotgun.__init__() got an unexpected keyword argument 'unexpected'",
        ),
        (
            ('https://x.example', 's'),
            {'script_name': 'other'},
            "Shotgun.__init__() multiple values for argument 'script_name'",
        ),
    ],
)
def test_constructor_type_errors_identify_shotgun(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    constructor = cast('Callable[..., Shotgun]', Shotgun)
    with pytest.raises(TypeError) as caught:
        constructor(*args, **kwargs)

    assert str(caught.value) == message


def test_constructor_uses_ca_environment_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('SHOTGUN_API_CACERTS', '/prefix/certs.pem')
    captured: dict[str, object] = {}
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]})])

    def make_transport(**kwargs: object) -> _FakeTransport:
        captured.update(kwargs)
        return transport

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)

    Shotgun('https://x.example', script_name='s', api_key='k')

    assert captured['ca_certs'] == '/prefix/certs.pem'


def test_constructor_validates_auth_combinations() -> None:
    with pytest.raises(ValueError, match='script_name provided without api_key'):
        Shotgun('https://x.example', script_name='s', connect=False)
    with pytest.raises(ValueError, match='cannot use both'):
        Shotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            login='me',
            password=_CREDENTIAL,
            connect=False,
        )


@pytest.mark.parametrize('first_method', ['info', 'read'])
def test_failed_readiness_is_retried_by_the_next_operation(first_method: str) -> None:
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        connect=False,
    )
    transport = _FakeTransport(
        [
            ConnectError('temporary'),
            _make_response({'version': [9, 0, 0]}),
            _make_response({'id': 7}),
        ]
    )
    sg._transport = transport
    sg.config.max_rpc_attempts = 1

    first_call = (
        sg.info if first_method == 'info' else lambda: _client_call(sg, 'read', {'type': 'Shot'})
    )
    with pytest.raises(ConnectError, match='temporary'):
        first_call()

    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}
    assert _method_names(sg) == ['info', 'info', 'read']


def test_get_session_token_uses_fixture_pinned_result_shape() -> None:
    sg = _fake_client([_make_response({'session_id': _SESSION_ID})])

    assert sg.get_session_token() == _SESSION_ID
    session_token = sg.config.session_token
    assert session_token == _SESSION_ID


def test_invalid_config_during_readiness_wakes_all_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readiness_entered = threading.Event()
    release_readiness = threading.Event()
    waiter_waiting = threading.Event()

    def block_readiness() -> None:
        readiness_entered.set()
        assert release_readiness.wait(timeout=5)

    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transport = _FakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=b'{"results": {"version": [9, 0, 0]}}',
                on_close=block_readiness,
            ),
            _make_response({'version': [9, 0, 0]}),
        ]
    )
    sg._transport = transport
    sg.config.proxy_server = 'proxy.local'
    readiness = sg._readiness
    original_wait = readiness.wait

    def track_wait(timeout: float | None = None) -> bool:
        waiter_waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(readiness, 'wait', track_wait)
    owner_call = _start_daemon_call(sg.connect)
    owner_thread, _owner_finished, owner_errors = owner_call
    assert readiness_entered.wait(timeout=5)
    waiter_call = _start_daemon_call(sg.connect)
    waiter_thread, _waiter_finished, waiter_errors = waiter_call
    assert waiter_waiting.wait(timeout=5)
    sg.config.update_fields(proxy_user='unpaired-user')
    release_readiness.set()

    try:
        _finish_daemon_calls((owner_call, waiter_call))
    finally:
        if owner_thread.is_alive() or waiter_thread.is_alive():
            sg.config.proxy_user = None
            with readiness:
                sg._readiness_in_progress.clear()
                sg._readiness_owner.clear()
                readiness.notify_all()
            owner_thread.join(timeout=5)
            waiter_thread.join(timeout=5)

    assert not owner_thread.is_alive()
    assert not waiter_thread.is_alive()
    assert owner_errors == []
    assert waiter_errors == []
    assert sg._readiness_in_progress == set()
    assert sg._readiness_owner == {}


@pytest.mark.parametrize('invalid_page_limit', ['1.5', '9' * 5000])
def test_invalid_page_limit_releases_readiness_waiters_and_allows_retry(
    monkeypatch: pytest.MonkeyPatch,
    invalid_page_limit: str,
) -> None:
    readiness_entered = threading.Event()
    release_readiness = threading.Event()
    waiter_waiting = threading.Event()

    def block_readiness() -> None:
        readiness_entered.set()
        assert release_readiness.wait(timeout=5)

    invalid_metadata = {
        'version': [9, 0, 0],
        'api_max_entities_per_page': invalid_page_limit,
    }
    first_response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=json.dumps({'results': invalid_metadata}).encode(),
        on_close=block_readiness,
    )
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transport = _FakeTransport(
        [
            first_response,
            _make_response(invalid_metadata),
            _make_response(
                {
                    'version': [9, 0, 0],
                    'api_max_entities_per_page': '250',
                }
            ),
            _make_response({'id': 7}),
        ]
    )
    sg._transport = transport
    readiness = sg._readiness
    original_wait = readiness.wait

    def track_wait(timeout: float | None = None) -> bool:
        waiter_waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(readiness, 'wait', track_wait)
    first = _start_daemon_call(sg.connect)
    assert readiness_entered.wait(timeout=5)
    second = _start_daemon_call(sg.connect)
    assert waiter_waiting.wait(timeout=5)
    release_readiness.set()
    calls = (first, second)
    callers = [thread for thread, _finished, _errors in calls]
    for thread in callers:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in callers)
    assert all(finished.is_set() for _thread, finished, _errors in calls)
    errors = [error for _thread, _finished, recorded in calls for error in recorded]
    assert len(errors) == 2
    assert all(isinstance(error, _errors.ShotgunError) for error in errors)
    assert all(str(error) == 'The server returned an invalid page limit.' for error in errors)
    assert sg.config.server_info == {}
    assert sg._ready_caps == {}

    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}
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
    assert _method_names(sg) == ['info', 'info', 'info', 'read']


def test_malformed_proxy_is_rejected() -> None:
    with pytest.raises(ValueError, match='http_proxy must be'):
        Shotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            http_proxy='http://someserver.com',
            connect=False,
        )


def test_origin_change_invalidates_readiness_and_capabilities() -> None:
    sg = _fake_client(
        [
            _make_response({'id': 1}),
            _make_response({'version': [10, 0, 0]}),
            _make_response({'id': 2}),
        ]
    )

    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 1}
    first_caps = sg.server_caps
    sg.config.set_server_params('https://other.example')
    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 2}

    assert sg.server_caps is not first_caps
    assert sg.server_caps.version == (10, 0, 0)
    assert sg.config.server_info['version'] == [10, 0, 0]
    assert _method_names(sg) == ['info', 'read', 'info', 'read']


def test_proxy_connect_without_body_replies_after_headers() -> None:
    connection = MagicMock(spec=socket.socket)
    listener = MagicMock(spec=socket.socket)
    listener.accept.return_value = (connection, ('127.0.0.1', 1))
    connection.recv.side_effect = [
        b'CONNECT target.example:443 HTTP/1.1\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n'
    ]
    observed: list[str | None] = []
    response = b'HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n'

    _serve_proxy(listener, response, observed)

    connection.recv.assert_called_once()
    connection.sendall.assert_called_once_with(response)
    assert observed == ['Basic dXNlcjpwYXNz']


def test_proxy_consumes_segmented_request_body_before_reply() -> None:
    body = b'{"method_name":"info","params":null}'
    header = (
        b'POST http://target.example/api3/json HTTP/1.1\r\n'
        + f'Content-Length: {len(body)}\r\n'.encode('ascii')
        + b'Proxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n'
    )
    chunks = [header[:25], header[25:] + body[:7], body[7:]]
    events: list[tuple[str, bytes]] = []
    connection = MagicMock(spec=socket.socket)
    listener = MagicMock(spec=socket.socket)
    listener.accept.return_value = (connection, ('127.0.0.1', 1))

    def receive(_size: int) -> bytes:
        chunk = chunks.pop(0)
        events.append(('receive', chunk))
        return chunk

    def send(payload: bytes) -> None:
        events.append(('send', payload))

    connection.recv.side_effect = receive
    connection.sendall.side_effect = send
    response = _proxy_response(b'{}')
    observed: list[str | None] = []

    _serve_proxy(listener, response, observed)

    assert events == [
        ('receive', header[:25]),
        ('receive', header[25:] + body[:7]),
        ('receive', body[7:]),
        ('send', response),
    ]
    assert observed == ['Basic dXNlcjpwYXNz']
    connection.settimeout.assert_called_once()
    assert 0 < connection.settimeout.call_args.args[0] <= 5


@pytest.mark.parametrize(
    'code',
    _ASCII_CONTROL_CODES,
    ids=[f'ascii-{code}' for code in _ASCII_CONTROL_CODES],
)
def test_proxy_parsing_rejects_every_ascii_control_character(code: int) -> None:
    with pytest.raises(ValueError, match='ASCII control'):
        Shotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            http_proxy=f'proxy{chr(code)}.local',
            connect=False,
        )


@pytest.mark.parametrize(
    'http_proxy',
    [
        'user@proxy.local',
        ':password@proxy.local',
        'user:@proxy.local',
        'proxy local',
        'proxy%20local',
        'proxy.local/path',
        'proxy.local:0',
        'proxy.local:70000',
        'user:\ncredential@proxy.local',
    ],
)
def test_proxy_parsing_rejects_malformed_authority(http_proxy: str) -> None:
    with pytest.raises(ValueError, match='http_proxy'):
        Shotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            http_proxy=http_proxy,
            connect=False,
        )


def test_proxy_parsing_supports_auth_port_and_default_port() -> None:
    authenticated = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy=f'u:{_PROXY_AUTH}@proxy.local:3128',
        connect=False,
    )
    defaulted = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='proxy.local',
        connect=False,
    )

    assert authenticated.config.proxy_server == 'proxy.local'
    assert authenticated.config.proxy_port == 3128
    assert authenticated.config.proxy_user == 'u'
    assert authenticated.config.proxy_pass == _PROXY_AUTH
    assert defaulted.config.proxy_port == 8080


def test_proxy_rejects_premature_request_body_eof() -> None:
    connection = MagicMock(spec=socket.socket)
    listener = MagicMock(spec=socket.socket)
    listener.accept.return_value = (connection, ('127.0.0.1', 1))
    connection.recv.side_effect = [
        b'POST http://target.example/api3/json HTTP/1.1\r\nContent-Length: 4\r\n\r\na',
        b'',
    ]

    with pytest.raises(EOFError, match='request body'):
        _serve_proxy(listener, _proxy_response(), [])

    connection.sendall.assert_not_called()


def test_proxy_url_omits_reserved_character_credentials() -> None:
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='user:p/a?s#s@proxy.local:3128',
        connect=False,
    )

    assert config_module.snapshot(sg.config).proxy_url == 'http://proxy.local:3128'


def test_proxy_validation_error_does_not_expose_credentials() -> None:
    with pytest.raises(ValueError, match='http_proxy') as error:
        Shotgun(
            'https://x.example',
            script_name='s',
            api_key='k',
            http_proxy=f'{_PROXY_USER}:{_PROXY_AUTH_VALUE}@proxy.local:70000',
            connect=False,
        )
    if _PROXY_AUTH_VALUE in str(error.value):
        raise AssertionError('proxy validation exposed credentials')


def test_public_info_is_unauthenticated_and_avoids_duplicate_probe() -> None:
    sg = _fake_client([_make_response({'id': 7})])

    assert sg.info()['version'] == [9, 0, 0]
    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}

    requests = _fake_transport(sg).requests
    assert _method_names(sg) == ['info', 'read']
    assert requests[0].body is not None
    assert b'script_key' not in requests[0].body


def test_public_retarget_never_sends_old_auth_to_new_origin() -> None:
    sg = Shotgun('https://old.example', session_token=_ORIGIN_SESSION_VALUE, connect=False)
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
    transport = _FakeTransport([_make_response({'version': [9, 0, 0]}), _make_response({'id': 7})])
    sg._transport = transport

    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}

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


@pytest.mark.parametrize('method_name', ['info', 'read'])
def test_readiness_observer_reentry_does_not_self_wait_or_duplicate_probe(
    method_name: str,
) -> None:
    sg = _fake_client([_make_response({'id': 7})])
    reentered = False
    results: list[object] = []

    def reenter(_event: object) -> None:
        nonlocal reentered
        if reentered:
            return
        reentered = True
        _client_call(sg, 'read', {'type': 'Nested'})

    sg.config.on_request = reenter
    call = threading.Thread(
        target=lambda: results.append(
            sg.info() if method_name == 'info' else _client_call(sg, 'read', {'type': 'Shot'})
        ),
        daemon=True,
    )
    call.start()
    call.join(timeout=1)

    assert not call.is_alive()
    if method_name == 'info':
        assert results == [{'version': [9, 0, 0], 'api_max_entities_per_page': 500}]
        assert _method_names(sg) == ['info']
    else:
        assert results == [{'id': 7}]
        assert _method_names(sg) == ['info', 'read']


def test_repeated_close_and_connect_prune_sync_readiness_generations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycle_count = 5
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transports = [
        _FakeTransport([_make_response({'version': [9, 0, 0]})]) for _ in range(cycle_count)
    ]

    def new_transport(_context: RequestContext) -> _FakeTransport:
        return transports.pop(0)

    monkeypatch.setattr(sg, '_new_transport', new_transport)

    for _ in range(cycle_count):
        sg.connect()
        sg.close()

    assert sg._ready_caps == {}


def test_request_uses_snapshot_captured_before_readiness() -> None:
    sg = Shotgun('https://x.example', session_token=_ORIGIN_SESSION_VALUE, connect=False)
    sg.config.update_fields(
        extra_auth_params={'old-auth-marker': 'old'},
        session_uuid='old-uuid',
    )
    transport = _FakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=b'{"results": {"version": [9, 0, 0]}}',
                on_close=lambda: sg.config.set_server_params('https://other.example'),
            ),
            _make_response({'id': 7}),
        ]
    )
    sg._transport = transport

    assert _client_call(sg, 'read', {'type': 'Shot'}) == {'id': 7}
    assert transport.requests[1].url == 'https://x.example:443/api3/json'
    assert b'old-session' in cast('bytes', transport.requests[1].body)
    assert b'old-uuid' in cast('bytes', transport.requests[1].body)
    assert b'old-auth-marker' in cast('bytes', transport.requests[1].body)
    assert config_module.snapshot(sg.config).auth == {}
    assert sg.config.extra_auth_params == {}


def test_retired_close_failure_rolls_back_replacement_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_error = RuntimeError('old close failed')
    old = _FakeTransport([_make_response({'version': [9, 0, 0]})])
    replacement = _FakeTransport([_make_response({'version': [10, 0, 0]})])
    transports = iter((old, replacement))
    old_close_calls = 0
    replacement_close_calls = 0

    def close_old() -> None:
        nonlocal old_close_calls
        old_close_calls += 1
        raise old_error

    def close_replacement() -> None:
        nonlocal replacement_close_calls
        replacement_close_calls += 1

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return next(transports)

    monkeypatch.setattr(old, 'close', close_old)
    monkeypatch.setattr(replacement, 'close', close_replacement)
    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg.connect()
    sg.config.update_fields(proxy_port=8080)

    with pytest.raises(RuntimeError, match='old close failed') as raised:
        sg.connect()

    assert raised.value is old_error
    assert old_close_calls == 1
    assert id(replacement) not in sg._lease_counts
    sg.close()
    assert replacement_close_calls == 1


def test_retired_close_failure_rolls_back_replacement_lease_when_rollback_close_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_error = RuntimeError('old close failed')
    rollback_error = RuntimeError('replacement close failed')
    old = _FakeTransport([_make_response({'version': [9, 0, 0]})])
    replacement = _FakeTransport([_make_response({'version': [10, 0, 0]})])
    transports = iter((old, replacement))
    replacement_close_calls = 0

    def close_old() -> None:
        sg.close()
        raise old_error

    def close_replacement() -> None:
        nonlocal replacement_close_calls
        replacement_close_calls += 1
        raise rollback_error

    def make_transport(**_kwargs: object) -> _FakeTransport:
        return next(transports)

    monkeypatch.setattr(old, 'close', close_old)
    monkeypatch.setattr(replacement, 'close', close_replacement)
    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg.connect()
    sg.config.update_fields(proxy_port=8080)

    with pytest.raises(RuntimeError, match='old close failed') as raised:
        sg.connect()

    assert raised.value is old_error
    assert id(replacement) not in sg._lease_counts
    assert replacement_close_calls == 1


@pytest.mark.parametrize('server_info', [{}, {'version': [7, 9, 9]}])
def test_server_caps_rejects_missing_or_unsupported_version(
    server_info: dict[str, object],
) -> None:
    sg = _fake_client([], server_info=server_info)

    with pytest.raises(_errors.ShotgunError, match=r'7\.10\.0|did not report'):
        _ = sg.server_caps


def test_session_exchange_does_not_overwrite_newer_credentials() -> None:
    sg = Shotgun(
        'https://x.example',
        login='artist',
        password=_OLD_CREDENTIAL,
        connect=False,
    )
    sg.config.auth_token = _OLD_AUTH_VALUE

    def replace_credentials() -> None:
        sg.config.update_fields(
            auth_token=_NEW_AUTH_VALUE,
            user_login='new-user',
            user_password=_NEW_CREDENTIAL,
        )

    transport = _FakeTransport(
        [
            _make_response({'version': [9, 0, 0]}),
            TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=b'{"results": {"session_id": "stale-session"}}',
                on_close=replace_credentials,
            ),
        ]
    )
    sg._transport = transport

    assert sg.get_session_token() == 'stale-session'
    assert sg.config.session_token is None
    assert sg.config.auth_token == _NEW_AUTH_VALUE
    assert sg.config.user_login == 'new-user'
    assert sg.config.user_password == _NEW_CREDENTIAL


def test_transport_creation_uses_captured_proxy_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sg = Shotgun(
        'https://x.example',
        script_name='s',
        api_key='k',
        http_proxy='old:credential@old-proxy.example:3128',
        connect=False,
    )
    context = config_module.snapshot(sg.config)
    sg.config.update_fields(
        proxy_pass=_CREDENTIAL,
        proxy_port=8080,
        proxy_server='new-proxy.example',
        proxy_user='new',
        raw_http_proxy='new:new-credential@new-proxy.example:8080',
    )
    captured: dict[str, object] = {}

    def make_transport(**kwargs: object) -> _FakeTransport:
        captured.update(kwargs)
        return _FakeTransport([])

    monkeypatch.setattr(client_module, 'Urllib3Transport', make_transport)
    create_transport = cast(
        'Callable[[RequestContext], object]',
        sg._new_transport,
    )

    create_transport(context)

    assert captured['proxy_url'] == 'http://old-proxy.example:3128'
    proxy_headers = cast('dict[str, str]', captured['proxy_headers'])
    _assert_proxy_authorization(proxy_headers.get('proxy-authorization'), 'old', 'credential')


def test_unsupported_floor_blocks_requested_operation() -> None:
    sg = _fake_client(
        [_make_response({'id': 7})],
        server_info={'version': [7, 9, 9]},
    )

    with pytest.raises(_errors.ShotgunError, match=r'7\.10\.0'):
        _client_call(sg, 'read', {'type': 'Shot'})

    assert _method_names(sg) == ['info']


def test_user_agent_defaults_and_management() -> None:
    sg = _fake_client([])

    assert sg.config.user_agents == (
        'shotgun-json (0.1.2)',
        f'Python {sg.client_caps.py_version}',
        'sg-python-api',
    )
    sg.add_user_agent('my-tool/1.0')
    assert sg.config.user_agents[-1] == 'my-tool/1.0'
    sg.reset_user_agent()
    assert 'my-tool/1.0' not in sg.config.user_agents
