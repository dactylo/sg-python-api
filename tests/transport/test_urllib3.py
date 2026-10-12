import socket
import ssl
import threading
import time
from collections.abc import Iterator
from typing import Never, Protocol, cast

import pytest
import urllib3

from shotgun_api3._transport import (
    _CURRENT_ABORT,
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    TimeoutConfig,
    TLSError,
    TransportError,
    TransportRequest,
    Urllib3Transport,
    _AbortHandle,
    _StartedUrllib3Request,
)
from tests._loopback import receive_request

_ERROR_POOL = urllib3.HTTPConnectionPool('example.test')
_REQUEST_ID = 'request-1'


class _StartRequest(Protocol):
    def __call__(
        self,
        transport: Urllib3Transport,
        request: TransportRequest,
        *,
        preload_content: bool,
    ) -> _StartedUrllib3Request: ...


class _StubResponse:
    connection = None
    data = b'{}'
    reason = 'OK'
    status = 200

    def __init__(self) -> None:
        self.headers = {'content-type': 'application/json'}

    def stream(self, _size: int) -> Iterator[bytes]:
        yield self.data


def _raise(error: BaseException) -> object:
    def raise_error(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise error

    return raise_error


def _req(url: str) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url=url + '/api3/json',
        headers={'content-type': 'application/json'},
        body=b'{"method_name": "info", "params": []}',
        timeout=TimeoutConfig(connect=5.0, read=5.0),
        token=_REQUEST_ID,
    )


def _serve_one_request(listener: socket.socket) -> None:
    connection, _address = listener.accept()
    with connection:
        body = receive_request(connection)[3]
        content = b'{"echo": ' + str(len(body)).encode() + b'}'
        response = (
            b'HTTP/1.1 200 OK\r\n'
            b'Content-Type: application/json; charset=utf-8\r\n'
            + f'Content-Length: {len(content)}\r\n'.encode()
            + b'Connection: close\r\n\r\n'
            + content
        )
        connection.sendall(response)


def _start_request(
    transport: Urllib3Transport,
    request: TransportRequest,
    *,
    preload_content: bool,
) -> _StartedUrllib3Request:
    implementation = cast('_StartRequest', Urllib3Transport._start_request)
    return implementation(transport, request, preload_content=preload_content)


@pytest.fixture
def local_server() -> Iterator[str]:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        host, port = listener.getsockname()
        thread = threading.Thread(target=_serve_one_request, args=(listener,), daemon=True)
        thread.start()
        yield f'http://{host}:{port}'
        thread.join(timeout=5.0)
        assert not thread.is_alive()


def test_certificate_verification_ssl_error_maps_to_tls_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = urllib3.exceptions.SSLError(ssl.SSLCertVerificationError('certificate'))
    transport = Urllib3Transport()
    monkeypatch.setattr(urllib3.PoolManager, 'request', _raise(error))
    try:
        with pytest.raises(TLSError):
            transport.send(_req('https://example.test'))
    finally:
        transport.close()


def test_connection_refused_maps_to_connect_error() -> None:
    transport = Urllib3Transport()
    try:
        with pytest.raises(ConnectError):
            transport.send(_req('http://127.0.0.1:1'))
    finally:
        transport.close()


def test_generic_ssl_error_maps_to_send_error(monkeypatch: pytest.MonkeyPatch) -> None:
    error = urllib3.exceptions.SSLError('ambiguous')
    transport = Urllib3Transport()
    monkeypatch.setattr(urllib3.PoolManager, 'request', _raise(error))
    try:
        with pytest.raises(SendError):
            transport.send(_req('https://example.test'))
    finally:
        transport.close()


def test_max_retry_error_classifies_its_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    reason = urllib3.exceptions.ConnectTimeoutError('connect')
    error = urllib3.exceptions.MaxRetryError(_ERROR_POOL, '/api3/json', reason)
    transport = Urllib3Transport()
    monkeypatch.setattr(urllib3.PoolManager, 'request', _raise(error))
    try:
        with pytest.raises(ConnectError):
            transport.send(_req('https://example.test'))
    finally:
        transport.close()


def test_proxy_scheme_unsupported_maps_to_proxy_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = urllib3.exceptions.ProxySchemeUnsupported('unsupported')
    transport = Urllib3Transport()
    monkeypatch.setattr(urllib3.PoolManager, 'request', _raise(error))
    try:
        with pytest.raises(ProxyError):
            transport.send(_req('https://example.test'))
    finally:
        transport.close()


@pytest.mark.parametrize('streaming', [False, True])
def test_send_modes_own_started_handle_lifetime(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
) -> None:
    transport = Urllib3Transport()
    registry = transport._abort_registry
    releases: list[object] = []
    original_release = registry._release
    response = _StubResponse()

    def release(handle: _AbortHandle) -> None:
        releases.append(handle)
        original_release(handle)

    def return_response(*_args: object, **_kwargs: object) -> object:
        return response

    monkeypatch.setattr(registry, '_release', release)
    monkeypatch.setattr(urllib3.PoolManager, 'request', return_response)
    try:
        request = _req('https://example.test')
        if streaming:
            result = transport.send_streaming(request)
            assert releases == []
            result.close()
            result.close()
        else:
            result = transport.send(request)

        assert len(releases) == 1
    finally:
        transport.close()


def test_send_round_trip(local_server: str) -> None:
    transport = Urllib3Transport()
    try:
        response = transport.send(_req(local_server))
        assert response.status == 200
        assert response.content == b'{"echo": 37}'
        assert response.headers['CONTENT-TYPE'] == 'application/json; charset=utf-8'
    finally:
        transport.close()


@pytest.mark.parametrize('preload_content', [True, False])
def test_start_request_failure_releases_once_and_resets_context(
    monkeypatch: pytest.MonkeyPatch,
    preload_content: bool,
) -> None:
    transport = Urllib3Transport()
    registry = transport._abort_registry
    releases: list[object] = []
    original_release = registry._release

    def release(handle: _AbortHandle) -> None:
        releases.append(handle)
        original_release(handle)

    monkeypatch.setattr(registry, '_release', release)
    monkeypatch.setattr(
        urllib3.PoolManager,
        'request',
        _raise(urllib3.exceptions.ProtocolError('headers failed')),
    )
    try:
        with pytest.raises(SendError, match='headers failed'):
            _start_request(
                transport,
                _req('https://example.test'),
                preload_content=preload_content,
            )
    finally:
        transport.close()

    assert len(releases) == 1
    assert _CURRENT_ABORT.get() is None


@pytest.mark.parametrize('preload_content', [True, False])
def test_start_request_leaves_successful_handle_owned_by_caller(
    monkeypatch: pytest.MonkeyPatch,
    preload_content: bool,
) -> None:
    transport = Urllib3Transport()
    registry = transport._abort_registry
    releases: list[object] = []
    request_contexts: list[object] = []
    request_kwargs: list[dict[str, object]] = []
    original_release = registry._release
    response = _StubResponse()

    def release(handle: _AbortHandle) -> None:
        releases.append(handle)
        original_release(handle)

    def return_response(*_args: object, **kwargs: object) -> object:
        request_contexts.append(_CURRENT_ABORT.get())
        request_kwargs.append(kwargs)
        return response

    monkeypatch.setattr(registry, '_release', release)
    monkeypatch.setattr(urllib3.PoolManager, 'request', return_response)
    try:
        started = _start_request(
            transport,
            _req('https://example.test'),
            preload_content=preload_content,
        )

        assert started.response is response
        assert started.started <= time.monotonic()
        assert request_contexts == [(registry, started.handle)]
        assert _CURRENT_ABORT.get() is None
        assert releases == []
        assert request_kwargs[0]['preload_content'] is preload_content
        assert request_kwargs[0]['retries'] is False
        timeout = request_kwargs[0]['timeout']
        assert isinstance(timeout, urllib3.Timeout)
        assert timeout.connect_timeout == 5.0
        assert timeout.read_timeout == 5.0
        registry._release(started.handle)
        assert releases == [started.handle]
    finally:
        transport.close()


def test_transport_uses_abort_aware_pool_classes() -> None:
    transport = Urllib3Transport()
    try:
        pool = transport._pool
        pool_classes = pool.pool_classes_by_scheme
        http_pool = pool_classes['http']
        https_pool = pool_classes['https']

        assert issubclass(http_pool, urllib3.connectionpool.HTTPConnectionPool)
        assert http_pool is not urllib3.connectionpool.HTTPConnectionPool
        assert issubclass(https_pool, urllib3.connectionpool.HTTPSConnectionPool)
        assert https_pool is not urllib3.connectionpool.HTTPSConnectionPool
    finally:
        transport.close()


@pytest.mark.parametrize(
    ('urllib3_error', 'transport_error'),
    [
        (urllib3.exceptions.ConnectTimeoutError('connect'), ConnectError),
        (urllib3.exceptions.ProtocolError('ambiguous'), SendError),
        (urllib3.exceptions.ProxyError('proxy', OSError('failure')), ProxyError),
        (
            urllib3.exceptions.ReadTimeoutError(_ERROR_POOL, '/api3/json', 'read'),
            ReadTimeoutError,
        ),
    ],
)
def test_urllib3_errors_map_to_transport_errors(
    monkeypatch: pytest.MonkeyPatch,
    urllib3_error: urllib3.exceptions.HTTPError,
    transport_error: type[TransportError],
) -> None:
    transport = Urllib3Transport()
    monkeypatch.setattr(urllib3.PoolManager, 'request', _raise(urllib3_error))
    try:
        with pytest.raises(transport_error):
            transport.send(_req('https://example.test'))
    finally:
        transport.close()
