import socket
import threading
import time
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from typing import cast, get_type_hints

import pytest
import urllib3

from shotgun_api3._transport import (
    AbortedError,
    ReadTimeoutError,
    SendError,
    StreamingResponse,
    TimeoutConfig,
    Transport,
    TransportError,
    TransportRequest,
    TransportResponse,
    Urllib3Transport,
    _AbortHandle,
)
from tests._loopback import receive_request
from tests._transports import _FakeTransport

_REQUEST_ID = 'stream-request'


@contextmanager
def _loopback(handler: Callable[[socket.socket], None]) -> Generator[str, None, None]:
    errors: list[AssertionError | OSError | ValueError] = []
    stop = threading.Event()
    with socket.create_server(('127.0.0.1', 0)) as listener:
        listener.settimeout(0.1)

        def serve() -> None:
            try:
                while not stop.is_set():
                    try:
                        connection, _address = listener.accept()
                    except TimeoutError:
                        continue
                    with connection:
                        handler(connection)
                    return
            except (AssertionError, OSError, ValueError) as exc:
                errors.append(exc)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        host, port = listener.getsockname()
        try:
            yield f'http://{host}:{port}'
        finally:
            stop.set()
            thread.join(timeout=5.0)
            assert not thread.is_alive()
            if errors:
                raise errors[0]


def _request(
    url: str = 'https://example.test/file',
    method: str = 'GET',
    body: bytes | Iterator[bytes] | None = None,
    timeout: TimeoutConfig | None = None,
) -> TransportRequest:
    return TransportRequest(
        method=method,
        url=url,
        headers={},
        body=body,
        timeout=timeout or TimeoutConfig(connect=5.0, read=5.0),
        token=_REQUEST_ID,
    )


def _scripted_response(releases: list[str]) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers={'eTaG': 'abc123'},
        content=b'abcdef',
        on_close=lambda: releases.append('release'),
    )


def _send_response(
    connection: socket.socket,
    content: bytes,
    headers: tuple[tuple[str, str], ...] = (),
) -> None:
    header_bytes = b''.join(f'{name}: {value}\r\n'.encode() for name, value in headers)
    connection.sendall(
        b'HTTP/1.1 200 OK\r\n'
        + f'Content-Length: {len(content)}\r\n'.encode()
        + header_bytes
        + b'\r\n'
        + content
    )


def _track_releases(
    monkeypatch: pytest.MonkeyPatch,
    transport: Urllib3Transport,
    url: str,
) -> tuple[list[object], list[object | None]]:
    registry_releases: list[object] = []
    connection_releases: list[object | None] = []
    registry = transport._abort_registry
    original_registry_release = registry._release
    manager = transport._pool
    pool = manager.connection_from_url(url)
    pool_type = type(pool)
    original_connection_release = cast(
        'Callable[[object, object | None], None]',
        vars(pool_type)['_put_conn'],
    )

    def release_registry(handle: _AbortHandle) -> None:
        registry_releases.append(handle)
        original_registry_release(handle)

    def release_connection(active_pool: object, connection: object | None) -> None:
        if active_pool is pool:
            connection_releases.append(connection)
        original_connection_release(active_pool, connection)

    monkeypatch.setattr(registry, '_release', release_registry)
    monkeypatch.setattr(pool_type, '_put_conn', release_connection)
    return registry_releases, connection_releases


def test_fake_streaming_exhaustion_releases_once_and_preserves_headers() -> None:
    releases: list[str] = []
    fake = _FakeTransport([_scripted_response(releases)])

    response = fake.send_streaming(_request())

    assert response.status == 200
    assert response.reason == 'OK'
    assert response.headers['ETAG'] == 'abc123'
    assert list(response.iter_chunks(2)) == [b'ab', b'cd', b'ef']
    response.close()
    response.close()
    assert releases == ['release']
    assert fake.requests == [_request()]


def test_fake_streaming_explicit_close_without_consumption_releases_once() -> None:
    releases: list[str] = []
    fake = _FakeTransport([_scripted_response(releases)])
    response = fake.send_streaming(_request())

    response.close()
    response.close()

    assert releases == ['release']


def test_fake_streaming_rejects_consumption_after_close() -> None:
    releases: list[str] = []
    response = _FakeTransport([_scripted_response(releases)]).send_streaming(_request())

    response.close()

    with pytest.raises(TransportError, match='closed'):
        response.iter_chunks()
    assert releases == ['release']


def test_fake_streaming_rejects_second_consumption() -> None:
    releases: list[str] = []
    response = _FakeTransport([_scripted_response(releases)]).send_streaming(_request())

    chunks = response.iter_chunks(2)
    with pytest.raises(TransportError, match='already'):
        response.iter_chunks(2)

    assert b''.join(chunks) == b'abcdef'
    assert releases == ['release']


@pytest.mark.parametrize(
    ('request_bytes', 'message'),
    [
        (b'POST / HTTP/1.1\r\n', 'request ended before headers'),
        (
            b'POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\nab',
            'request ended before body',
        ),
    ],
)
def test_shared_request_reader_rejects_socket_early_eof(
    request_bytes: bytes, message: str
) -> None:
    client, server = socket.socketpair()
    with client, server:
        client.sendall(request_bytes)
        client.shutdown(socket.SHUT_WR)
        with pytest.raises(EOFError, match=message):
            receive_request(server)


@pytest.mark.parametrize('size', [0, -1])
def test_fake_streaming_rejects_non_positive_chunk_size(size: int) -> None:
    releases: list[str] = []
    response = _FakeTransport([_scripted_response(releases)]).send_streaming(_request())

    with pytest.raises(TransportError, match='positive'):
        response.iter_chunks(size)
    response.close()

    assert releases == ['release']


def test_streaming_iterator_failure_releases_once() -> None:
    releases: list[str] = []

    def broken_stream(_size: int) -> Iterator[bytes]:
        yield b'a'
        raise RuntimeError('body failed')

    response = StreamingResponse(
        status=200,
        reason='OK',
        headers={},
        stream=broken_stream,
        release=lambda: releases.append('release'),
    )
    chunks = response.iter_chunks()

    assert next(chunks) == b'a'
    with pytest.raises(RuntimeError, match='body failed'):
        next(chunks)
    response.close()

    assert releases == ['release']


def test_transport_vocabulary_supports_streaming() -> None:
    body_type = get_type_hints(TransportRequest)['body']

    assert body_type == bytes | Iterator[bytes] | None
    assert callable(Transport.send_streaming)


@pytest.mark.parametrize('streaming', [False, True])
def test_urllib3_iterator_body_error_maps_and_releases_once(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
) -> None:
    def receive_until_close(connection: socket.socket) -> None:
        while True:
            try:
                if not connection.recv(4096):
                    return
            except OSError:
                return

    def broken_body() -> Iterator[bytes]:
        yield b'aa'
        raise RuntimeError('request body failed')

    with _loopback(receive_until_close) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        try:
            request = _request(
                server_url + '/destination',
                method='PUT',
                body=broken_body(),
            )
            request.headers['Content-Length'] = '6'
            send_attempt = transport.send_streaming if streaming else transport.send
            with pytest.raises(SendError, match='request body failed'):
                send_attempt(request)
        finally:
            transport.close()

    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_send_accepts_iterator_body() -> None:
    observed_bodies: list[bytes] = []

    def respond(connection: socket.socket) -> None:
        method, path, _headers, body = receive_request(connection)
        assert (method, path) == ('PUT', '/destination')
        observed_bodies.append(body)
        _send_response(connection, b'', headers=(('Etag', 'upload-1'),))

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        try:
            request = _request(
                server_url + '/destination',
                method='PUT',
                body=iter((b'aa', b'bb', b'cc')),
            )
            request.headers['Content-Length'] = '6'
            response = transport.send(request)
        finally:
            transport.close()

    assert response.status == 200
    assert response.headers['etag'] == 'upload-1'
    assert observed_bodies == [b'aabbcc']


def test_urllib3_streaming_abort_during_body_read_releases_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allow_server_exit = threading.Event()
    first_chunk_received = threading.Event()

    def respond(connection: socket.socket) -> None:
        receive_request(connection)
        connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nab')
        allow_server_exit.wait(timeout=5.0)

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        response = transport.send_streaming(_request(server_url + '/stall'))
        outcome: list[AssertionError | TransportError] = []

        def consume() -> None:
            try:
                chunks = response.iter_chunks(2)
                assert next(chunks) == b'ab'
                first_chunk_received.set()
                next(chunks)
            except (AssertionError, TransportError) as exc:
                outcome.append(exc)

        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
        try:
            assert first_chunk_received.wait(timeout=5.0)
            transport.abort(_REQUEST_ID)
        finally:
            allow_server_exit.set()
            response.close()
            transport.close()
            thread.join(timeout=5.0)
            assert not thread.is_alive()

    assert len(outcome) == 1
    assert isinstance(outcome[0], AbortedError)
    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_body_error_maps_and_releases_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def respond(connection: socket.socket) -> None:
        receive_request(connection)
        connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nab')

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        try:
            response = transport.send_streaming(_request(server_url + '/truncated'))
            with pytest.raises(SendError):
                list(response.iter_chunks(2))
            response.close()
        finally:
            transport.close()

    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_close_failure_still_releases_connection_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls: list[urllib3.response.HTTPResponse] = []
    original_close = urllib3.response.HTTPResponse.close

    def respond(connection: socket.socket) -> None:
        receive_request(connection)
        _send_response(connection, b'unconsumed')

    def close_and_fail(response: urllib3.response.HTTPResponse) -> None:
        close_calls.append(response)
        original_close(response)
        raise RuntimeError('close failed')

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        monkeypatch.setattr(urllib3.response.HTTPResponse, 'close', close_and_fail)
        try:
            response = transport.send_streaming(_request(server_url + '/file'))
            with pytest.raises(RuntimeError, match='close failed'):
                response.close()
            response.close()
        finally:
            transport.close()

    assert len(close_calls) == 1
    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_enforces_cumulative_total_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def respond(connection: socket.socket) -> None:
        receive_request(connection)
        connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\na')
        for chunk in (b'b', b'c'):
            time.sleep(0.12)
            try:
                connection.sendall(chunk)
            except OSError:
                return

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        try:
            request = _request(
                server_url + '/slow',
                timeout=TimeoutConfig(connect=1.0, read=0.5, total=0.2),
            )
            response = transport.send_streaming(request)
            with pytest.raises(ReadTimeoutError, match='total'):
                list(response.iter_chunks(1))
        finally:
            transport.close()

    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_exhaustion_releases_registry_and_connection_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_requests: list[tuple[str, str, dict[str, str], bytes]] = []

    def respond(connection: socket.socket) -> None:
        observed_requests.append(receive_request(connection))
        _send_response(connection, b'0123456789', headers=(('eTaG', 'abc123'),))

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        try:
            response = transport.send_streaming(_request(server_url + '/file'))
            assert response.headers['ETAG'] == 'abc123'
            assert list(response.iter_chunks(4)) == [b'0123', b'4567', b'89']
            response.close()
            response.close()
        finally:
            transport.close()

    assert observed_requests[0][:2] == ('GET', '/file')
    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_explicit_close_releases_registry_and_connection_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def respond(connection: socket.socket) -> None:
        receive_request(connection)
        _send_response(connection, b'unconsumed')

    with _loopback(respond) as server_url:
        transport = Urllib3Transport()
        registry_releases, connection_releases = _track_releases(
            monkeypatch,
            transport,
            server_url,
        )
        try:
            response = transport.send_streaming(_request(server_url + '/file'))
            response.close()
            response.close()
        finally:
            transport.close()

    assert len(registry_releases) == 1
    assert len(connection_releases) == 1


def test_urllib3_streaming_header_error_releases_registry_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = Urllib3Transport()
    releases: list[object] = []
    request_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    registry = transport._abort_registry
    original_release = registry._release

    def release(handle: _AbortHandle) -> None:
        releases.append(handle)
        original_release(handle)

    def fail(*args: object, **kwargs: object) -> object:
        request_calls.append((args, kwargs))
        raise urllib3.exceptions.ProtocolError('headers failed')

    monkeypatch.setattr(registry, '_release', release)
    monkeypatch.setattr(urllib3.PoolManager, 'request', fail)
    try:
        with pytest.raises(SendError):
            transport.send_streaming(_request())
    finally:
        transport.close()

    assert len(releases) == 1
    assert len(request_calls) == 1
    assert request_calls[0][1]['preload_content'] is False
    assert request_calls[0][1]['retries'] is False


@pytest.mark.parametrize(
    ('abort_request', 'error_type'),
    [(False, SendError), (True, AbortedError)],
)
def test_urllib3_streaming_timeout_setup_failure_maps_and_releases_once(
    monkeypatch: pytest.MonkeyPatch,
    abort_request: bool,
    error_type: type[TransportError],
) -> None:
    transport = Urllib3Transport()

    class RacingSocket:
        def settimeout(self, _value: float) -> None:
            if abort_request:
                transport.abort(_REQUEST_ID)
            raise OSError('socket closed during timeout setup')

    class RacingConnection:
        sock: RacingSocket | None = RacingSocket()

    class StubResponse:
        def __init__(self) -> None:
            self.close_calls = 0
            self.connection: RacingConnection | None = RacingConnection()
            self.headers: dict[str, str] = {}
            self.reason = 'OK'
            self.release_calls = 0
            self.status = 200

        def close(self) -> None:
            self.close_calls += 1

        def release_conn(self) -> None:
            self.release_calls += 1
            self.connection = None

        def stream(self, _size: int) -> Iterator[bytes]:
            yield b'unreachable'

    stub_response = StubResponse()

    def return_response(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return stub_response

    monkeypatch.setattr(urllib3.PoolManager, 'request', return_response)
    try:
        response = transport.send_streaming(
            _request(timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0))
        )
        with pytest.raises(error_type):
            next(response.iter_chunks())
    finally:
        transport.close()

    assert stub_response.close_calls == 1
    assert stub_response.release_calls == 1
