import queue
import socket
import ssl
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from types import SimpleNamespace
from typing import ParamSpec, Protocol, TypeVar, cast

import pytest
import urllib3.connection

from shotgun_api3 import _transport
from shotgun_api3._transport import (
    AbortedError,
    TimeoutConfig,
    TransportError,
    TransportRequest,
    TransportResponse,
    Urllib3Transport,
    _AbortRegistry,
)

_FIRST_REQUEST_ID = 'first-request'
_REQUEST_ID = 'request-to-abort'
_SECOND_REQUEST_ID = 'second-request'
_SHARED_REQUEST_ID = 'shared-request'
_CallParams = ParamSpec('_CallParams')
_CallResult = TypeVar('_CallResult')


class _Handle(Protocol):
    token: str


class _Registry(Protocol):
    def _release(self, handle: _Handle) -> None: ...


def _delayed_call(
    function: Callable[_CallParams, _CallResult],
    started: threading.Event,
    release: threading.Event,
) -> Callable[_CallParams, _CallResult]:

    def delayed(*args: _CallParams.args, **kwargs: _CallParams.kwargs) -> _CallResult:
        started.set()
        release.wait(timeout=5.0)
        return function(*args, **kwargs)

    return delayed


def _receive_request(
    connection: socket.socket,
    first_byte_received: threading.Event | None = None,
) -> bool:
    request = b''
    while b'\r\n\r\n' not in request:
        chunk = connection.recv(4096)
        if not chunk:
            return False
        if first_byte_received is not None:
            first_byte_received.set()
        request += chunk
    header_block, body = request.split(b'\r\n\r\n', maxsplit=1)
    content_length = next(
        int(line.split(b':', maxsplit=1)[1].strip())
        for line in header_block.split(b'\r\n')[1:]
        if line.lower().startswith(b'content-length:')
    )
    while len(body) < content_length:
        chunk = connection.recv(4096)
        if not chunk:
            return False
        body += chunk
    return True


def _record_send(
    transport: Urllib3Transport,
    request: TransportRequest,
    outcome: list[TransportResponse | TransportError],
) -> None:
    try:
        outcome.append(transport.send(request))
    except TransportError as exc:
        outcome.append(exc)


def _request(server_url: str, request_id: str) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url=server_url + '/api3/json',
        headers={},
        body=b'{}',
        timeout=TimeoutConfig(connect=5.0, read=30.0),
        token=request_id,
    )


def _send_response(connection: socket.socket) -> None:
    content = b'{}'
    response = (
        b'HTTP/1.1 200 OK\r\n' + f'Content-Length: {len(content)}\r\n'.encode() + b'\r\n' + content
    )
    connection.sendall(response)


@pytest.fixture
def reusing_server() -> Iterator[tuple[str, threading.Event, threading.Event, threading.Event]]:
    first_response_sent = threading.Event()
    second_request_received = threading.Event()
    send_second_response = threading.Event()

    with socket.create_server(('127.0.0.1', 0)) as listener:

        def serve() -> None:
            connection, _address = listener.accept()
            with connection:
                try:
                    if not _receive_request(connection):
                        return
                    _send_response(connection)
                    first_response_sent.set()
                    if not _receive_request(connection):
                        return
                    second_request_received.set()
                    send_second_response.wait(timeout=5.0)
                    _send_response(connection)
                except OSError:
                    return

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        host, port = listener.getsockname()
        yield (
            f'http://{host}:{port}',
            first_response_sent,
            second_request_received,
            send_second_response,
        )
        send_second_response.set()
        thread.join(timeout=5.0)
        assert not thread.is_alive()


@pytest.fixture
def stalling_server() -> Iterator[tuple[str, queue.Queue[socket.socket], threading.Event]]:
    requests: queue.Queue[socket.socket] = queue.Queue()
    request_bytes_received = threading.Event()
    stop = threading.Event()
    connections: list[socket.socket] = []

    with socket.create_server(('127.0.0.1', 0)) as listener:
        listener.settimeout(0.2)

        def serve() -> None:
            while not stop.is_set():
                try:
                    connection, _address = listener.accept()
                except TimeoutError:
                    continue
                connections.append(connection)
                if _receive_request(connection, request_bytes_received):
                    requests.put(connection)

            for connection in connections:
                connection.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        host, port = listener.getsockname()
        yield f'http://{host}:{port}', requests, request_bytes_received
        stop.set()
        thread.join(timeout=5.0)
        assert not thread.is_alive()


def test_abort_does_not_close_connection_reused_by_next_request(
    monkeypatch: pytest.MonkeyPatch,
    reusing_server: tuple[str, threading.Event, threading.Event, threading.Event],
) -> None:
    server_url, first_response_sent, second_request_received, send_second_response = reusing_server
    transport = Urllib3Transport(pool_maxsize=1)
    first_release_entered = threading.Event()
    allow_first_release = threading.Event()
    first_outcome: list[TransportResponse | TransportError] = []
    second_outcome: list[TransportResponse | TransportError] = []
    registry: _Registry = cast('_Registry', vars(transport)['_abort_registry'])
    original_release = registry._release

    def delayed_release(handle: _Handle) -> None:
        if handle.token == _FIRST_REQUEST_ID:
            first_release_entered.set()
            allow_first_release.wait(timeout=5.0)
        original_release(handle)

    monkeypatch.setattr(registry, '_release', delayed_release)
    first_thread = threading.Thread(
        target=_record_send,
        args=(transport, _request(server_url, _FIRST_REQUEST_ID), first_outcome),
    )
    second_thread = threading.Thread(
        target=_record_send,
        args=(transport, _request(server_url, _SECOND_REQUEST_ID), second_outcome),
    )
    first_thread.start()
    try:
        assert first_response_sent.wait(timeout=5.0)
        assert first_release_entered.wait(timeout=5.0)
        second_thread.start()
        assert second_request_received.wait(timeout=5.0)
        transport.abort(_FIRST_REQUEST_ID)
        send_second_response.set()
        second_thread.join(timeout=5.0)
    finally:
        send_second_response.set()
        allow_first_release.set()
        first_thread.join(timeout=5.0)
        if second_thread.ident is not None:
            second_thread.join(timeout=5.0)
        transport.close()

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert len(first_outcome) == 1
    assert isinstance(first_outcome[0], TransportResponse)
    assert len(second_outcome) == 1
    assert isinstance(second_outcome[0], TransportResponse)


def test_abort_during_address_resolution_prevents_request_bytes(
    monkeypatch: pytest.MonkeyPatch,
    stalling_server: tuple[str, queue.Queue[socket.socket], threading.Event],
) -> None:
    server_url, requests, request_bytes_received = stalling_server
    transport = Urllib3Transport()
    resolution_started = threading.Event()
    continue_resolution = threading.Event()
    outcome: list[TransportResponse | TransportError] = []
    original_getaddrinfo = socket.getaddrinfo

    monkeypatch.setattr(
        socket,
        'getaddrinfo',
        _delayed_call(original_getaddrinfo, resolution_started, continue_resolution),
    )
    thread = threading.Thread(
        target=_record_send,
        args=(transport, _request(server_url, _REQUEST_ID), outcome),
    )
    thread.start()
    server_connection: socket.socket | None = None
    try:
        assert resolution_started.wait(timeout=5.0)
        transport.abort(_REQUEST_ID)
        continue_resolution.set()
        thread.join(timeout=0.5)
        alive_after_abort = thread.is_alive()
        if alive_after_abort:
            server_connection = requests.get(timeout=5.0)
            transport.abort(_REQUEST_ID)
            thread.join(timeout=5.0)
    finally:
        continue_resolution.set()
        if server_connection is not None:
            server_connection.close()
        transport.close()

    assert not alive_after_abort, 'pre-socket abort did not stop the request'
    assert not request_bytes_received.is_set(), 'request bytes reached the server after abort'
    assert len(outcome) == 1
    assert type(outcome[0]) is AbortedError


def test_abort_unblocks_all_inflight_requests_with_same_token(
    stalling_server: tuple[str, queue.Queue[socket.socket], threading.Event],
) -> None:
    server_url, requests, _request_bytes_received = stalling_server
    transport = Urllib3Transport(pool_maxsize=2)
    outcomes: list[list[TransportResponse | TransportError]] = [[], []]
    threads = [
        threading.Thread(
            target=_record_send,
            args=(transport, _request(server_url, _SHARED_REQUEST_ID), outcome),
        )
        for outcome in outcomes
    ]
    for thread in threads:
        thread.start()
    connections = [requests.get(timeout=5.0), requests.get(timeout=5.0)]
    try:
        transport.abort(_SHARED_REQUEST_ID)
        for thread in threads:
            thread.join(timeout=0.5)
        alive_after_abort = [thread.is_alive() for thread in threads]
    finally:
        for connection in connections:
            with suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            connection.close()
        for thread in threads:
            thread.join(timeout=5.0)
        transport.close()

    assert not any(alive_after_abort), 'abort did not reach every shared-token request'
    assert all(len(outcome) == 1 for outcome in outcomes)
    assert all(type(outcome[0]) is AbortedError for outcome in outcomes)


def test_abort_unblocks_inflight_request(
    stalling_server: tuple[str, queue.Queue[socket.socket], threading.Event],
) -> None:
    server_url, requests, _request_bytes_received = stalling_server
    transport = Urllib3Transport()
    outcome: list[TransportResponse | TransportError] = []
    thread = threading.Thread(
        target=_record_send,
        args=(transport, _request(server_url, _REQUEST_ID), outcome),
    )
    thread.start()
    requests.get(timeout=5.0)
    try:
        transport.abort(_REQUEST_ID)
        thread.join(timeout=5.0)
        assert not thread.is_alive(), 'abort did not unblock the request thread'
        assert len(outcome) == 1
        assert type(outcome[0]) is AbortedError
    finally:
        transport.close()


@pytest.mark.parametrize('tls', [False, True])
def test_windows_abort_disposes_socket_with_open_reader(
    monkeypatch: pytest.MonkeyPatch,
    tls: bool,
) -> None:
    # Force only the transport's platform branch; retain real socket ownership.
    monkeypatch.setattr(_transport, 'sys', SimpleNamespace(platform='win32'))
    client, peer = socket.socketpair()
    if tls:
        client = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).wrap_socket(
            client, server_hostname='localhost', do_handshake_on_connect=False
        )
    connection = urllib3.connection.HTTPConnection('localhost')
    connection.sock = client
    registry = _AbortRegistry()
    handle = registry._begin(_REQUEST_ID)
    registry._attach(handle, connection)
    descriptor = client.fileno()
    reader = client.makefile('rb')
    shutdown = client.shutdown
    both_aborts_entered = threading.Barrier(2)

    def concurrent_shutdown(sock: socket.socket, how: int) -> None:
        assert sock is client
        both_aborts_entered.wait(timeout=5.0)
        shutdown(how)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(type(client), 'shutdown', concurrent_shutdown)
            with ThreadPoolExecutor(max_workers=2) as executor:
                aborts = [executor.submit(registry._abort, _REQUEST_ID) for _ in range(2)]
                for abort in aborts:
                    abort.result(timeout=5.0)

        assert client.fileno() == -1, 'abort deferred disposal until buffered reader close'
        with (
            pytest.raises(OSError, match=r'Bad file descriptor|not a socket'),
            socket.socket(fileno=descriptor),
        ):
            pass
        assert connection.sock is None
        assert handle.aborted

        replacement, replacement_peer = socket.socketpair()
        with replacement, replacement_peer:
            # Closing old owners must not close a newly allocated descriptor.
            reader.close()
            client.close()
            registry._abort(_REQUEST_ID)
            replacement_peer.sendall(b'alive')
            assert replacement.recv(5) == b'alive'
    finally:
        reader.close()
        client.close()
        peer.close()
        registry._release(handle)


def test_windows_abort_tolerates_already_closed_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_transport, 'sys', SimpleNamespace(platform='win32'))
    connection = urllib3.connection.HTTPConnection('localhost')
    with socket.socket() as client:
        connection.sock = client
    registry = _AbortRegistry()
    handle = registry._begin(_REQUEST_ID)
    registry._attach(handle, connection)

    registry._abort(_REQUEST_ID)

    assert handle.aborted
    assert connection.sock is None
