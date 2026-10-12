import ast
import asyncio
import dataclasses
import gc
import socket
import ssl
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast, get_type_hints

import httpx2
import pytest

from shotgun_api3 import _files, _protocol
from shotgun_api3._async_httpx2 import Httpx2Transport, _ThreadedBody
from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._transport import (
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    TimeoutConfig,
    TLSError,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._loopback import receive_request

pytestmark = pytest.mark.async_required

_FILE_BATCH_SIZE = 1024 * 1024
_FILE_FINAL_BATCH_SIZE = 77_057
_FILE_TRANSFER_SIZE = 20_000_001
_REQUEST_ID = 'request-1'


class _BlockingBody(Iterator[bytes]):
    def __init__(self, close_error: Exception | None = None) -> None:
        self._close_error = close_error
        self.close_calls = 0
        self.close_raced = False
        self.closed = threading.Event()
        self.closed_on: int | None = None
        self.finished = threading.Event()
        self.release = threading.Event()
        self.started = threading.Event()

    def __next__(self) -> bytes:
        self.started.set()
        self.release.wait(timeout=5.0)
        self.finished.set()
        return b'BODY'

    def close(self) -> None:
        self.close_calls += 1
        self.close_raced = not self.finished.is_set()
        self.closed_on = threading.get_ident()
        self.closed.set()
        if self._close_error is not None:
            raise self._close_error


class _Body(Iterator[bytes]):
    def __init__(
        self,
        chunks: list[bytes],
        error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self._chunks = iter(chunks)
        self._close_error = close_error
        self._error = error
        self.closed_on: int | None = None
        self.read_on: list[int] = []

    def __next__(self) -> bytes:
        self.read_on.append(threading.get_ident())
        try:
            return next(self._chunks)
        except StopIteration:
            if self._error is not None:
                raise self._error from None
            raise

    def close(self) -> None:
        self.closed_on = threading.get_ident()
        if self._close_error is not None:
            raise self._close_error


class _ControlledClient:
    def __init__(self, response: httpx2.Response | None = None) -> None:
        self.closed = False
        self.close_calls = 0
        self.close_error: BaseException | None = None
        self.headers: Mapping[str, str] | None = None
        self.method: str | None = None
        self.request_body: bytes | None = None
        self.request_error: BaseException | None = None
        self.request_gate: asyncio.Event | None = None
        self.request_started = asyncio.Event()
        self.response = response or httpx2.Response(
            200,
            headers={'X-Response': 'value'},
            content=b'RESPONSE',
        )
        self.timeout: httpx2.Timeout | None = None
        self.url: str | None = None
        self.send_error: BaseException | None = None
        self.send_gate: asyncio.Event | None = None
        self.send_started = asyncio.Event()
        self.stream_request_body: bytes | None = None
        self.stream_response = httpx2.Response(200, content=b'STREAM')
        self.stream_timeout: httpx2.Timeout | None = None

    async def aclose(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error

    def build_request(
        self,
        method: str,
        url: str,
        *,
        content: bytes | AsyncIterator[bytes] | None,
        headers: Mapping[str, str],
        timeout: httpx2.Timeout,
    ) -> httpx2.Request:
        self.headers = headers
        self.method = method
        self.stream_timeout = timeout
        self.url = url
        return httpx2.Request(method, url, headers=headers, content=content)

    async def request(
        self,
        method: str,
        url: str,
        *,
        content: object,
        headers: Mapping[str, str],
        **options: object,
    ) -> httpx2.Response:
        self.headers = headers
        self.method = method
        self.request_started.set()
        self.timeout = cast('httpx2.Timeout', options['timeout'])
        self.url = url
        if self.request_gate is not None:
            await self.request_gate.wait()
        if self.request_error is not None:
            raise self.request_error
        if content is None:
            self.request_body = None
        elif isinstance(content, bytes):
            self.request_body = content
        else:
            chunks = cast('AsyncIterator[bytes]', content)
            self.request_body = b''.join([chunk async for chunk in chunks])
        return self.response

    async def send(self, request: httpx2.Request, *, stream: bool) -> httpx2.Response:
        assert stream
        self.send_started.set()
        if self.send_gate is not None:
            await self.send_gate.wait()
        if self.send_error is not None:
            raise self.send_error
        self.stream_request_body = await request.aread()
        return self.stream_response


class _ControlledStream(httpx2.AsyncByteStream):
    def __init__(
        self,
        script: list[bytes | BaseException],
        *,
        close_error: BaseException | None = None,
        close_gate: asyncio.Event | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.close_calls = 0
        self.close_error = close_error
        self.close_gate = close_gate
        self.close_finished = asyncio.Event()
        self.close_started = asyncio.Event()
        self.events: list[str] | None = None
        self.gate = gate
        self.pull_cancel_gate: asyncio.Event | None = None
        self.pull_cancelled = asyncio.Event()
        self.pull_finished = asyncio.Event()
        self.read_started = asyncio.Event()
        self.script = script

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            for item in self.script:
                self.read_started.set()
                if self.gate is not None:
                    try:
                        await self.gate.wait()
                    except asyncio.CancelledError:
                        if self.events is not None:
                            self.events.append('pull-cancelled')
                        self.pull_cancelled.set()
                        if self.pull_cancel_gate is not None:
                            await self.pull_cancel_gate.wait()
                        raise
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            if self.pull_cancelled.is_set():
                if self.events is not None:
                    self.events.append('pull-finished')
                self.pull_finished.set()

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.events is not None:
            self.events.append('response-close-started')
        self.close_started.set()
        if self.close_gate is not None:
            await self.close_gate.wait()
        if self.events is not None:
            self.events.append('response-close-finished')
        self.close_finished.set()
        if self.close_error is not None:
            raise self.close_error


class _GatedCloseBody(_Body):
    def __init__(self, error: Exception, close_error: Exception) -> None:
        super().__init__([b'PART'], error=error, close_error=close_error)
        self.close_calls = 0
        self.close_release = threading.Event()
        self.close_started = threading.Event()

    def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        self.close_release.wait(timeout=5.0)
        super().close()


class _ObservedBody(Iterator[bytes]):
    def __init__(self) -> None:
        self._chunks = iter((b'a', b'bc'))
        self.events: list[tuple[str, bytes]] = []
        self.observe_threads: list[int] = []
        self.pull_threads: list[int] = []

    def __next__(self) -> bytes:
        raise AssertionError('async adapter bypassed the pull/observe handshake')

    def observe_async_chunk(self, chunk: bytes) -> None:
        self.observe_threads.append(threading.get_ident())
        self.events.append(('observe', chunk))

    def pull_for_async(self) -> bytes | None:
        assert len(self.pull_threads) == len(self.observe_threads)
        chunk = next(self._chunks, None)
        if chunk is not None:
            self.pull_threads.append(threading.get_ident())
            self.events.append(('pull', chunk))
        return chunk


def _install_client(
    monkeypatch: pytest.MonkeyPatch,
    client: _ControlledClient,
) -> dict[str, object]:
    options: dict[str, object] = {}

    def create_client(**kwargs: object) -> _ControlledClient:
        options.update(kwargs)
        return client

    monkeypatch.setattr(httpx2, 'AsyncClient', create_client)
    return options


@asynccontextmanager
async def _loopback_response(
    parts: tuple[bytes, ...],
) -> AsyncGenerator[tuple[str, list[bytes]], None]:
    errors: list[AssertionError | OSError | ValueError] = []
    observed: list[bytes] = []
    stop = threading.Event()
    listener = socket.create_server(('127.0.0.1', 0))
    listener.settimeout(0.1)

    def serve() -> None:
        try:
            while not stop.is_set():
                try:
                    connection, _address = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if stop.is_set():
                        return
                    raise
                with connection:
                    observed.append(receive_request(connection)[3])
                    content_length = sum(map(len, parts))
                    connection.sendall(
                        b'HTTP/1.1 200 OK\r\n'
                        + f'Content-Length: {content_length}\r\n'.encode()
                        + b'X-Response: loopback\r\nConnection: close\r\n\r\n'
                    )
                    for part in parts:
                        connection.sendall(part)
                return
        except (AssertionError, OSError, ValueError) as error:
            errors.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    host, port = listener.getsockname()
    try:
        yield f'http://{host}:{port}', observed
    finally:
        stop.set()
        listener.close()
        await asyncio.to_thread(thread.join, 5.0)
        assert not thread.is_alive()
        if errors:
            raise errors[0]


def _request(
    *,
    body: bytes | Iterator[bytes] | None = b'{}',
    total: float | None = None,
) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url='https://example.test/api3/json',
        headers={'Content-Type': 'application/json', 'X-Request': 'value'},
        body=body,
        timeout=TimeoutConfig(connect=1.0, read=2.0, total=total),
        token=_REQUEST_ID,
    )


def _request_for_url(url: str) -> TransportRequest:
    request = _request()
    return TransportRequest(
        method=request.method,
        url=url,
        headers=request.headers,
        body=request.body,
        timeout=request.timeout,
        token=request.token,
    )


@pytest.mark.anyio
async def test_buffered_send_converts_request_response_and_body_off_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread = threading.get_ident()
    body = _Body([b'PART', b'TWO'])
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        response = await transport.send(_request(body=body))
    finally:
        await transport.close()

    assert (client.method, client.url) == ('POST', 'https://example.test/api3/json')
    assert client.headers == {'Content-Type': 'application/json', 'X-Request': 'value'}
    assert client.request_body == b'PARTTWO'
    assert client.timeout is not None
    assert vars(client.timeout) == {'connect': 1.0, 'read': 2.0, 'write': 2.0, 'pool': 1.0}
    assert body.read_on
    assert all(thread_id != main_thread for thread_id in body.read_on)
    assert body.closed_on is not None
    assert body.closed_on != main_thread
    assert response.status == 200
    assert response.content == b'RESPONSE'
    assert response.headers['x-response'] == 'value'


@pytest.mark.anyio
async def test_buffered_total_timeout_maps_to_read_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    client.request_gate = asyncio.Event()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(ReadTimeoutError, match='total timeout'):
            await transport.send(_request(total=0.02))
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_cancelled_consumer_returns_before_response_cleanup_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    close_gate = asyncio.Event()
    pull_cancel_gate = asyncio.Event()
    stream = _ControlledStream(
        [b'UNREACHABLE'],
        close_gate=close_gate,
        gate=asyncio.Event(),
    )
    stream.events = events
    stream.pull_cancel_gate = pull_cancel_gate
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    responses = [await transport.send_streaming(_request())]

    async def consume() -> None:
        async for _chunk in responses[0].aiter_chunks():
            pass

    consumer: asyncio.Task[None] | None = asyncio.create_task(consume())
    transport_close: asyncio.Task[None] | None = None
    await stream.read_started.wait()
    consumer.cancel()
    try:
        await stream.pull_cancelled.wait()
        assert consumer.done()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        assert not stream.pull_finished.is_set()
        assert not stream.close_started.is_set()

        responses.clear()
        consumer = None
        gc.collect()
        transport_close = asyncio.create_task(transport.close())
        await asyncio.sleep(0)
        assert not transport_close.done()

        pull_cancel_gate.set()
        await stream.close_started.wait()
        assert events[:3] == [
            'pull-cancelled',
            'pull-finished',
            'response-close-started',
        ]
        assert not stream.close_finished.is_set()
        assert not transport_close.done()
        await stream.close_started.wait()
        assert client.close_calls == 0
    finally:
        pull_cancel_gate.set()
        close_gate.set()
        if consumer is not None and not consumer.done():
            with pytest.raises(asyncio.CancelledError):
                await consumer
        if transport_close is None:
            transport_close = asyncio.create_task(transport.close())
        await transport_close

    assert stream.close_calls == 1
    assert events == [
        'pull-cancelled',
        'pull-finished',
        'response-close-started',
        'response-close-finished',
    ]
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_cancelled_consumer_does_not_report_background_pull_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        loop_errors.append(context)

    stream = _ControlledStream(
        [b'UNREACHABLE'],
        close_error=httpx2.CloseError('background response close failed'),
        gate=asyncio.Event(),
    )
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response = await transport.send_streaming(_request())

    async def consume() -> None:
        async for _chunk in response.aiter_chunks():
            pass

    consumer = asyncio.create_task(consume())
    transport_close: asyncio.Task[None] | None = None
    loop.set_exception_handler(capture_loop_error)
    try:
        await stream.read_started.wait()
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        transport_close = asyncio.create_task(transport.close())
        await transport_close
        await asyncio.sleep(0)
    finally:
        if not consumer.done():
            consumer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consumer
        if transport_close is None:
            transport_close = asyncio.create_task(transport.close())
        try:
            await transport_close
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

    assert stream.close_calls == 1
    assert client.close_calls == 1
    assert not loop_errors


def test_constructor_disables_environment_and_wires_proxy_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    options = _install_client(monkeypatch, client)
    Httpx2Transport(
        proxy_url='http://proxy.example:8080',
        proxy_headers={'Proxy-Authorization': 'secret'},
        pool_maxsize=7,
    )

    limits = cast('httpx2.Limits', options['limits'])
    proxy = cast('httpx2.Proxy', options['proxy'])
    assert options['trust_env'] is False
    assert options['timeout'] is None
    assert options['verify'] is True
    assert 'transport' not in options
    assert (limits.max_connections, limits.max_keepalive_connections) == (7, 7)
    assert str(proxy.url) == 'http://proxy.example:8080'
    assert proxy.headers['Proxy-Authorization'] == 'secret'


def test_constructor_uses_requested_ca_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    observed: list[str | None] = []

    def create_context(*, cafile: str | None = None) -> ssl.SSLContext:
        observed.append(cafile)
        return context

    client = _ControlledClient()
    options = _install_client(monkeypatch, client)
    monkeypatch.setattr(ssl, 'create_default_context', create_context)

    Httpx2Transport(ca_certs='/trusted/ca.pem')

    assert observed == ['/trusted/ca.pem']
    assert options['verify'] is context


@pytest.mark.anyio
async def test_first_pull_after_explicit_close_has_no_unobserved_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        loop_errors.append(context)

    stream = _ControlledStream([b'UNREACHABLE'])
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    responses = [await transport.send_streaming(_request())]
    iterators = [responses[0].aiter_chunks()]
    await responses[0].close()

    loop.set_exception_handler(capture_loop_error)
    try:
        with pytest.raises(TransportError, match='async transport stream is not active'):
            await anext(iterators[0])
        await asyncio.sleep(0)
        iterators.clear()
        responses.clear()
        gc.collect()
        await asyncio.sleep(0)
    finally:
        try:
            await transport.close()
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

    assert client.close_calls == 1
    assert stream.close_calls == 1
    assert not loop_errors


@pytest.mark.anyio
async def test_cancelled_response_close_retains_lease_until_cleanup_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_gate = asyncio.Event()
    stream = _ControlledStream([b'BODY'], close_gate=close_gate)
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response = await transport.send_streaming(_request())
    response_close = asyncio.create_task(response.close())
    await stream.close_started.wait()
    transport_close = asyncio.create_task(transport.close())

    response_close.cancel()
    try:
        done, _pending = await asyncio.wait({response_close}, timeout=0.1)
        assert done == {response_close}
        with pytest.raises(asyncio.CancelledError):
            await response_close
        await asyncio.sleep(0)
        assert not transport_close.done()
        assert client.close_calls == 0
    finally:
        close_gate.set()
        if not response_close.done():
            with pytest.raises(asyncio.CancelledError):
                await response_close
        await transport_close

    assert stream.close_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_close_drains_existing_lease_and_allows_its_final_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    transport.checkout()
    close_task = asyncio.create_task(transport.close())
    await asyncio.sleep(0)
    assert not close_task.done()

    transport.checkout()
    transport.checkin()
    response = await transport.send(_request())
    assert response.status == 200

    async def checkout_from_new_task() -> None:
        transport.checkout()

    with pytest.raises(TransportError, match='closing'):
        await asyncio.create_task(checkout_from_new_task())
    transport.checkin()
    await close_task

    assert client.close_calls == 1


@pytest.mark.anyio
async def test_close_error_does_not_leak_httpx2_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_error = httpx2.CloseError('pool close failed')
    client = _ControlledClient()
    client.close_error = close_error
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()

    with pytest.raises(SendError) as exc_info:
        await transport.close()

    assert exc_info.value.__cause__ is close_error
    with pytest.raises(SendError) as repeated_info:
        await transport.close()
    assert repeated_info.value.__cause__ is close_error


@pytest.mark.anyio
async def test_close_from_lease_holder_fails_instead_of_deadlocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    transport.checkout()

    with pytest.raises(TransportError, match='active lease'):
        await transport.close()
    transport.checkin()
    await transport.close()


@pytest.mark.anyio
async def test_close_is_idempotent_and_rejects_use_after_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()

    await asyncio.gather(transport.close(), transport.close())
    await transport.close()

    assert client.close_calls == 1
    with pytest.raises(TransportError, match='closed'):
        transport.checkout()
    with pytest.raises(TransportError, match='closed'):
        await transport.send(_request())
    with pytest.raises(TransportError, match='closed'):
        await transport.send_streaming(_request())


@pytest.mark.parametrize(
    ('error', 'expected'),
    [
        (httpx2.ProxyError('proxy'), ProxyError),
        (httpx2.ConnectError('connect'), ConnectError),
        (httpx2.ConnectTimeout('connect timeout'), ConnectError),
        (httpx2.PoolTimeout('pool'), ConnectError),
        (httpx2.ReadTimeout('read'), ReadTimeoutError),
        (httpx2.WriteError('write'), SendError),
        (httpx2.WriteTimeout('write timeout'), SendError),
        (httpx2.ReadError('read error'), SendError),
        (httpx2.CloseError('close'), SendError),
        (httpx2.LocalProtocolError('local protocol'), SendError),
        (httpx2.RemoteProtocolError('remote protocol'), SendError),
    ],
)
@pytest.mark.anyio
async def test_error_mapping_is_strictly_retry_safe(
    monkeypatch: pytest.MonkeyPatch,
    error: httpx2.HTTPError,
    expected: type[TransportError],
) -> None:
    client = _ControlledClient()
    client.request_error = error
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(expected) as exc_info:
            await transport.send(_request())
    finally:
        await transport.close()

    assert exc_info.value.__cause__ is error
    assert str(exc_info.value) == str(error)


@pytest.mark.anyio
async def test_explicit_request_body_close_error_maps_to_send_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_error = ValueError('request body close failed')
    body = _Body([b'BODY'], close_error=close_error)
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(SendError, match='request body close failed') as exc_info:
            await transport.send(_request(body=body))
    finally:
        await transport.close()

    assert exc_info.value.__cause__ is close_error
    assert body.closed_on is not None


def test_httpx2_is_imported_only_by_adapter() -> None:
    source_root = Path(__file__).parents[2] / 'src' / 'shotgun_api3'
    importers: list[str] = []
    for path in source_root.rglob('*.py'):
        tree = ast.parse(path.read_text())
        if any(
            (isinstance(node, ast.Import) and any(alias.name == 'httpx2' for alias in node.names))
            or (isinstance(node, ast.ImportFrom) and node.module == 'httpx2')
            for node in ast.walk(tree)
        ):
            importers.append(path.name)

    assert importers == ['_async_httpx2.py']


def test_public_transport_boundary_has_no_httpx2_types() -> None:
    send_hints = get_type_hints(Httpx2Transport.send)
    streaming_hints = get_type_hints(Httpx2Transport.send_streaming)

    assert send_hints == {'request': TransportRequest, 'return': TransportResponse}
    assert streaming_hints == {
        'request': TransportRequest,
        'return': AsyncStreamingResponse,
    }


@pytest.mark.anyio
@pytest.mark.parametrize('seed_mode', ['buffered', 'streaming'])
@pytest.mark.parametrize('request_mode', ['buffered', 'streaming'])
async def test_real_httpx2_client_does_not_persist_response_cookies(
    monkeypatch: pytest.MonkeyPatch, seed_mode: str, request_mode: str
) -> None:
    observed: list[httpx2.Request] = []

    def handle(request: httpx2.Request) -> httpx2.Response:
        observed.append(request)
        return httpx2.Response(
            200,
            headers={'Set-Cookie': '_session_id=response-session; Path=/; Secure'},
            content=b'RESPONSE',
        )

    def init_mock_transport(*_args: object, **_kwargs: object) -> httpx2.MockTransport:
        return httpx2.MockTransport(handle)

    monkeypatch.setattr(httpx2.AsyncClient, '_init_transport', init_mock_transport)
    transport = Httpx2Transport()

    async def send(request: TransportRequest, mode: str) -> None:
        if mode == 'buffered':
            assert (await transport.send(request)).content == b'RESPONSE'
        else:
            response = await transport.send_streaming(request)
            assert b''.join([chunk async for chunk in response.aiter_chunks()]) == b'RESPONSE'

    try:
        await send(_request(), seed_mode)
        await send(_request_for_url('https://example.test:8443/attachment'), request_mode)
        await send(
            dataclasses.replace(_request(), headers={'Cookie': '_session_id=explicit-session'}),
            request_mode,
        )
        assert [request.headers.get('cookie') for request in observed] == [
            None,
            None,
            '_session_id=explicit-session',
        ]
        assert list(transport._client.cookies.jar) == []
    finally:
        await transport.close()


@pytest.mark.anyio
@pytest.mark.parametrize('mode', ['buffered', 'streaming'])
async def test_real_httpx2_client_overlapping_explicit_cookies_remain_request_scoped(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    observed: list[str | None] = []

    async def handle(request: httpx2.Request) -> httpx2.Response:
        cookie = request.headers.get('cookie')
        observed.append(cookie)
        if cookie == 'session=first':
            first_started.set()
            await release_first.wait()
        return httpx2.Response(
            200, headers={'Set-Cookie': 'session=response; Path=/; Secure'}, content=b'RESPONSE'
        )

    def init_mock_transport(*_args: object, **_kwargs: object) -> httpx2.MockTransport:
        return httpx2.MockTransport(handle)

    monkeypatch.setattr(httpx2.AsyncClient, '_init_transport', init_mock_transport)
    transport = Httpx2Transport()

    async def send(cookie: str | None) -> None:
        request = dataclasses.replace(
            _request(), headers={} if cookie is None else {'Cookie': cookie}
        )
        if mode == 'buffered':
            assert (await transport.send(request)).content == b'RESPONSE'
        else:
            response = await transport.send_streaming(request)
            assert b''.join([chunk async for chunk in response.aiter_chunks()]) == b'RESPONSE'

    try:
        async with asyncio.timeout(5):
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(send('session=first'))
                try:
                    await first_started.wait()
                    await send('session=second')
                    await send(None)
                finally:
                    release_first.set()
            await send(None)
        assert observed == ['session=first', 'session=second', None, None]
        assert list(transport._client.cookies.jar) == []
    finally:
        release_first.set()
        await transport.close()


@pytest.mark.parametrize('mode', ['buffered', 'streaming'])
@pytest.mark.anyio
async def test_real_httpx2_client_round_trip_uses_shared_types(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.setenv('HTTP_PROXY', 'http://127.0.0.1:1')
    monkeypatch.setenv('HTTPS_PROXY', 'http://127.0.0.1:1')
    async with _loopback_response((b'RES', b'PONSE')) as (url, observed):
        transport = Httpx2Transport()
        try:
            if mode == 'buffered':
                response = await transport.send(_request_for_url(url))
                content = response.content
            else:
                streaming_response = await transport.send_streaming(_request_for_url(url))
                content = b''.join([chunk async for chunk in streaming_response.aiter_chunks(2)])
                response = streaming_response
        finally:
            await transport.close()

    assert observed == [b'{}']
    assert response.status == 200
    assert response.headers['x-response'] == 'loopback'
    assert content == b'RESPONSE'


@pytest.mark.anyio
async def test_request_body_cancellation_returns_before_thread_pull_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread = threading.get_ident()
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        loop_errors.append(context)

    body = _BlockingBody(close_error=ValueError('background close failed'))
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    loop.set_exception_handler(capture_loop_error)
    task = asyncio.create_task(transport.send(_request(body=body)))
    assert await asyncio.to_thread(body.started.wait, 1.0)
    close_task: asyncio.Task[None] | None = None

    task.cancel()
    try:
        done, _pending = await asyncio.wait({task}, timeout=0.1)
        assert done == {task}
        with pytest.raises(asyncio.CancelledError):
            await task
        close_task = asyncio.create_task(transport.close())
        done, _pending = await asyncio.wait({close_task}, timeout=0.05)
        assert done == set()
    finally:
        body.release.set()
        if not task.done():
            with pytest.raises(asyncio.CancelledError):
                await task
        if close_task is None:
            await transport.close()
        else:
            await close_task
        await asyncio.sleep(0)
        loop.set_exception_handler(previous_handler)

    assert await asyncio.to_thread(body.closed.wait, 1.0)
    assert body.close_calls == 1
    assert not body.close_raced
    assert body.closed_on is not None
    assert body.closed_on != main_thread
    assert not loop_errors


@pytest.mark.anyio
async def test_reserved_child_may_start_after_close_begins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    child_release = asyncio.Event()

    async def send_from_reserved_child() -> TransportResponse:
        await child_release.wait()
        transport.checkout()
        try:
            return await transport.send(_request())
        finally:
            transport.checkin()

    transport.checkout()
    parent_released = False
    close_task: asyncio.Task[None] | None = None
    child = asyncio.create_task(send_from_reserved_child())
    try:
        transport.reserve(child)
        transport.checkin()
        parent_released = True
        close_task = asyncio.create_task(transport.close())
        await asyncio.sleep(0)

        async def send_without_reservation() -> TransportResponse:
            return await transport.send(_request())

        with pytest.raises(TransportError, match=r'^async transport is closing$'):
            await asyncio.create_task(send_without_reservation())
        assert not close_task.done()

        child_release.set()
        response = await child
        assert response.status == 200
        await close_task
    finally:
        child_release.set()
        if not parent_released:
            transport.checkin()
        if not child.done():
            child.cancel()
        await asyncio.gather(child, return_exceptions=True)
        if close_task is None:
            await transport.close()
        else:
            await close_task

    assert client.close_calls == 1


@pytest.mark.anyio
async def test_send_cancellation_is_not_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _ControlledClient()
    client.request_gate = asyncio.Event()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    task = asyncio.create_task(transport.send(_request()))
    await client.request_started.wait()

    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await transport.close()


@pytest.mark.parametrize(
    ('error', 'expected'),
    [
        (httpx2.ReadTimeout('body timed out'), ReadTimeoutError),
        (httpx2.ReadError('body failed'), SendError),
    ],
)
@pytest.mark.anyio
async def test_streaming_body_error_is_mapped_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
    error: httpx2.HTTPError,
    expected: type[TransportError],
) -> None:
    stream = _ControlledStream([b'PART', error])
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response = await transport.send_streaming(_request())

    with pytest.raises(expected) as exc_info:
        _chunks = [chunk async for chunk in response.aiter_chunks()]
    await transport.close()

    assert exc_info.value.__cause__ is error
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_streaming_cancellation_closes_response_and_unblocks_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _ControlledStream([b'UNREACHABLE'], gate=asyncio.Event())
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response = await transport.send_streaming(_request())

    async def consume() -> None:
        async for _chunk in response.aiter_chunks():
            pass

    consume_task = asyncio.create_task(consume())
    await stream.read_started.wait()
    close_task = asyncio.create_task(transport.close())
    await asyncio.sleep(0)
    assert not close_task.done()

    consume_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consume_task
    await response.close()
    await response.close()
    await close_task

    assert stream.close_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_streaming_header_cancellation_closes_unstarted_request_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _Body([b'UNSENT'])
    client = _ControlledClient()
    client.send_gate = asyncio.Event()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    task = asyncio.create_task(transport.send_streaming(_request(body=body)))
    await client.send_started.wait()

    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await transport.close()

    assert not body.read_on
    assert body.closed_on is not None


@pytest.mark.parametrize(
    ('error', 'expected'),
    [
        (httpx2.RemoteProtocolError('headers failed'), SendError),
        (httpx2.ConnectTimeout('connect failed'), ConnectError),
    ],
)
@pytest.mark.anyio
async def test_streaming_header_error_is_mapped_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
    error: httpx2.HTTPError,
    expected: type[TransportError],
) -> None:
    client = _ControlledClient()
    client.send_error = error
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    with pytest.raises(expected) as exc_info:
        await transport.send_streaming(_request())

    await transport.close()

    assert exc_info.value.__cause__ is error
    assert client.closed


@pytest.mark.anyio
async def test_streaming_header_failure_closes_unstarted_request_body_off_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread = threading.get_ident()
    body = _Body([b'UNSENT'])
    client = _ControlledClient()
    client.send_error = httpx2.ConnectError('refused')
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(ConnectError):
            await transport.send_streaming(_request(body=body))
    finally:
        await transport.close()

    assert not body.read_on
    assert body.closed_on is not None
    assert body.closed_on != main_thread


@pytest.mark.anyio
async def test_streaming_response_close_error_is_mapped_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_error = httpx2.CloseError('response close failed')
    stream = _ControlledStream([b'BODY'], close_error=close_error)
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response = await transport.send_streaming(_request())

    with pytest.raises(SendError) as exc_info:
        await response.close()
    await transport.close()

    assert exc_info.value.__cause__ is close_error
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_streaming_send_converts_request_and_returns_shared_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_body = _Body([b'ONE', b'TWO'])
    stream = _ControlledStream([b'AB', b'CD'])
    client = _ControlledClient()
    client.stream_response = httpx2.Response(
        206,
        headers={'Content-Range': 'bytes 0-3/4'},
        stream=stream,
    )
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        response = await transport.send_streaming(_request(body=request_body))
        iterator = response.aiter_chunks(2)
        with pytest.raises(TransportError, match='already consumed'):
            response.aiter_chunks(2)
        content = b''.join([chunk async for chunk in iterator])
    finally:
        await transport.close()

    assert isinstance(response, AsyncStreamingResponse)
    assert (response.status, response.reason) == (206, 'Partial Content')
    assert response.headers['content-range'] == 'bytes 0-3/4'
    assert content == b'ABCD'
    assert client.stream_request_body == b'ONETWO'
    assert client.stream_timeout is not None
    assert vars(client.stream_timeout) == {
        'connect': 1.0,
        'read': 2.0,
        'write': 2.0,
        'pool': 1.0,
    }
    assert request_body.closed_on is not None
    assert stream.close_calls == 1


@pytest.mark.anyio
async def test_streaming_setup_timeout_returns_before_thread_pull_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _BlockingBody()
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    task = asyncio.create_task(transport.send_streaming(_request(body=body, total=0.02)))
    assert await asyncio.to_thread(body.started.wait, 1.0)
    close_task: asyncio.Task[None] | None = None

    try:
        done, _pending = await asyncio.wait({task}, timeout=0.1)
        assert done == {task}
        with pytest.raises(ReadTimeoutError, match='before response body'):
            await task
        close_task = asyncio.create_task(transport.close())
        done, _pending = await asyncio.wait({close_task}, timeout=0.05)
        assert done == set()
    finally:
        body.release.set()
        if not task.done():
            with pytest.raises(ReadTimeoutError):
                await task
        if close_task is None:
            await transport.close()
        else:
            await close_task

    assert await asyncio.to_thread(body.closed.wait, 1.0)
    assert body.close_calls == 1
    assert not body.close_raced


@pytest.mark.anyio
async def test_streaming_total_is_one_cumulative_monotonic_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    deadlines: list[float] = []
    phase_durations = iter((0.02, 0.03, 0.03, 0.03))

    @asynccontextmanager
    async def controlled_timeout_at(deadline: float) -> AsyncGenerator[None, None]:
        deadlines.append(deadline)
        clock[0] += next(phase_durations)
        yield
        if clock[0] >= deadline:
            raise TimeoutError

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, 'time', lambda: clock[0])
    monkeypatch.setattr(asyncio, 'timeout_at', controlled_timeout_at)
    stream = _ControlledStream([b'A', b'B', b'C'])
    client = _ControlledClient()
    client.stream_response = httpx2.Response(200, stream=stream)
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    response: AsyncStreamingResponse | None = None
    try:
        response = await transport.send_streaming(_request(total=0.1))
        iterator = response.aiter_chunks(1)
        assert await anext(iterator) == b'A'
        assert await anext(iterator) == b'B'
        with pytest.raises(ReadTimeoutError, match='total timeout'):
            await anext(iterator)
        assert stream.close_calls == 1
    finally:
        if response is not None:
            await response.close()
        await transport.close()

    assert deadlines == pytest.approx([100.1] * 4)
    assert clock[0] == pytest.approx(100.11)


@pytest.mark.anyio
async def test_streaming_total_timeout_covers_response_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _Body([b'UNSENT'])
    client = _ControlledClient()
    client.send_gate = asyncio.Event()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(ReadTimeoutError, match='before response body'):
            await transport.send_streaming(_request(body=body, total=0.02))
    finally:
        await transport.close()

    assert body.closed_on is not None


@pytest.mark.anyio
async def test_threaded_body_pulls_off_loop_and_observes_on_loop() -> None:
    loop_thread = threading.get_ident()
    source = _ObservedBody()
    body = _ThreadedBody(source)

    chunks = [chunk async for chunk in body]

    assert chunks == [b'a', b'bc']
    assert source.pull_threads
    assert all(thread != loop_thread for thread in source.pull_threads)
    assert source.observe_threads == [loop_thread, loop_thread]
    assert source.events == [
        ('pull', b'a'),
        ('observe', b'a'),
        ('pull', b'bc'),
        ('observe', b'bc'),
    ]


@pytest.mark.anyio
async def test_threaded_body_batches_exact_file_exchange_pulls(tmp_path: Path) -> None:
    content = b'a' * (_FILE_TRANSFER_SIZE - 1) + b'b'
    source_file = tmp_path / 'source.bin'
    source_file.write_bytes(content)
    exchange = _files.build_storage_upload_exchange(
        'test-agent',
        _protocol.classify_endpoint('object_put'),
        'https://storage.example/object',
        _files.StorageRange(
            length=_FILE_TRANSFER_SIZE,
            offset=0,
            path=str(source_file),
        ),
        chunk_size=_FILE_BATCH_SIZE,
    )
    source = cast('Iterator[bytes]', exchange.body_factory())

    chunks = [chunk async for chunk in _ThreadedBody(source)]

    assert len(chunks) <= 21
    assert [len(chunk) for chunk in chunks] == [_FILE_BATCH_SIZE] * 19 + [_FILE_FINAL_BATCH_SIZE]
    assert b''.join(chunks) == content


@pytest.mark.parametrize('mode', ['buffered', 'streaming'])
@pytest.mark.anyio
async def test_transport_close_drains_cleanup_after_prompt_request_body_failure(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        loop_errors.append(context)

    body_error = ValueError('disk read failed')
    body = _GatedCloseBody(body_error, ValueError('background close failed'))
    client = _ControlledClient()
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    drain_started = asyncio.Event()
    original_finish_close = transport._finish_close

    async def signal_drain_started() -> None:
        drain_started.set()
        await original_finish_close()

    monkeypatch.setattr(transport, '_finish_close', signal_drain_started)
    operation = transport.send if mode == 'buffered' else transport.send_streaming
    send_task = asyncio.create_task(operation(_request(body=body)))
    close_task: asyncio.Task[None] | None = None
    loop.set_exception_handler(capture_loop_error)
    try:
        assert await asyncio.to_thread(body.close_started.wait, 5.0)
        with pytest.raises(SendError, match='disk read failed') as exc_info:
            await asyncio.wait_for(asyncio.shield(send_task), timeout=5.0)
        assert body.closed_on is None
        close_task = asyncio.create_task(transport.close())
        await asyncio.wait_for(drain_started.wait(), timeout=5.0)
        assert not close_task.done()
        assert not client.closed
    finally:
        body.close_release.set()
        try:
            send_task.cancel()
            await asyncio.wait_for(asyncio.gather(send_task, return_exceptions=True), timeout=5.0)
        finally:
            close_task = close_task or asyncio.create_task(transport.close())
            try:
                await asyncio.wait_for(close_task, timeout=5.0)
                await asyncio.sleep(0)
            finally:
                loop.set_exception_handler(previous_handler)

    assert exc_info.value.__cause__ is body_error
    assert body.closed_on is not None
    assert body.close_calls == 1
    assert client.closed
    assert client.close_calls == 1
    assert not loop_errors


@pytest.mark.parametrize('error_type', [httpx2.ConnectError, httpx2.ConnectTimeout])
@pytest.mark.anyio
async def test_tls_connect_failure_maps_to_tls_error(
    monkeypatch: pytest.MonkeyPatch,
    error_type: Callable[[str], httpx2.HTTPError],
) -> None:
    error = error_type('TLS failed')
    intermediate = OSError('wrapped')
    intermediate.__cause__ = ssl.SSLCertVerificationError('certificate')
    error.__cause__ = intermediate
    client = _ControlledClient()
    client.request_error = error
    _install_client(monkeypatch, client)
    transport = Httpx2Transport()
    try:
        with pytest.raises(TLSError) as exc_info:
            await transport.send(_request())
    finally:
        await transport.close()

    assert exc_info.value.__cause__ is error
