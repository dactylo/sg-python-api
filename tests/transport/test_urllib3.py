import io
import socket
import ssl
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Never

import pytest
import urllib3
from urllib3.connection import HTTPConnection

from shotgun_api3._transport import (
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    TimeoutConfig,
    TLSError,
    TransportError,
    TransportRequest,
    Urllib3Transport,
)
from tests._loopback import receive_request

if TYPE_CHECKING:
    from urllib3._base_connection import BaseHTTPConnection

_ERROR_POOL = urllib3.HTTPConnectionPool('example.test')


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
) -> urllib3.response.HTTPResponse:
    return transport._start_request(request, preload_content=preload_content)


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
    with socket.socket() as closed_port:
        closed_port.bind(('127.0.0.1', 0))
        port = closed_port.getsockname()[1]
        transport = Urllib3Transport()
        try:
            with pytest.raises(ConnectError):
                transport.send(_req(f'http://127.0.0.1:{port}'))
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
def test_start_request_failure_maps_to_send_error(
    monkeypatch: pytest.MonkeyPatch,
    preload_content: bool,
) -> None:
    transport = Urllib3Transport()
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


@pytest.mark.parametrize('preload_content', [True, False])
def test_start_request_preserves_response_and_transport_options(
    monkeypatch: pytest.MonkeyPatch,
    preload_content: bool,
) -> None:
    transport = Urllib3Transport()
    request_kwargs: list[dict[str, object]] = []
    response = _StubResponse()

    def return_response(*_args: object, **kwargs: object) -> object:
        request_kwargs.append(kwargs)
        return response

    monkeypatch.setattr(urllib3.PoolManager, 'request', return_response)
    try:
        received = _start_request(
            transport,
            _req('https://example.test'),
            preload_content=preload_content,
        )

        assert received is response
        assert request_kwargs[0]['preload_content'] is preload_content
        assert request_kwargs[0]['retries'] is False
        timeout = request_kwargs[0]['timeout']
        assert isinstance(timeout, urllib3.Timeout)
        assert timeout.connect_timeout == 5.0
        assert timeout.read_timeout == 5.0
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


@pytest.mark.parametrize('iteration_error', [False, True])
def test_stream_terminal_paths_return_connection_once(
    monkeypatch: pytest.MonkeyPatch,
    iteration_error: bool,
) -> None:
    class _FailingBody(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            if self.tell():
                raise OSError('body read failed')
            return super().read(size)

    transport = Urllib3Transport()
    pool = transport._pool.connection_from_url('http://127.0.0.1')
    connection = pool._get_conn()
    assert isinstance(connection, HTTPConnection)
    returned: list[object] = []
    original_put = pool._put_conn

    def put_connection(conn: 'BaseHTTPConnection | None') -> None:
        returned.append(conn)
        original_put(conn)

    body = _FailingBody(b'abcdef') if iteration_error else io.BytesIO(b'abcdef')
    raw_response = urllib3.response.HTTPResponse(
        body=body,
        headers={'Content-Length': '6'},
        pool=pool,
        connection=connection,
        preload_content=False,
    )

    def return_response(*_args: object, **_kwargs: object) -> object:
        return raw_response

    monkeypatch.setattr(pool, '_put_conn', put_connection)
    monkeypatch.setattr(urllib3.PoolManager, 'request', return_response)
    try:
        response = transport.send_streaming(_req('http://127.0.0.1'))
        chunks = response.iter_chunks(2)
        assert next(chunks) == b'ab'
        assert returned == []
        if iteration_error:
            with pytest.raises(SendError, match='body read failed'):
                next(chunks)
        else:
            response.close()
        response.close()
        assert returned == [connection]
    finally:
        transport.close()
