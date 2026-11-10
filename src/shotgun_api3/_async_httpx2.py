"""Async httpx2 transport adapter."""

import asyncio
import dataclasses
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import suppress
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Self, TypedDict, cast

import httpx2

from ._async_transport import AsyncStreamingResponse, AsyncUploadObserver
from ._connections import SocketOption, keepalive_socket_options, tune_socket
from ._transport import (
    CaseInsensitiveHeaders,
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


class Httpx2Transport:
    """Provide the shared async transport interface over httpx2.

    Share an instance among tasks on one event loop. Logical RPC leases belong
    to the task that checks them out and must be checked in by that task.
    Streaming responses retain resources until they close.

    Await `close()` after releasing the calling task's leases and streams.
    Closing rejects unrelated new work, drains accepted work, and permanently
    closes the transport.
    """

    def __init__(  # noqa: PLR0913
        self,
        ca_certs: str | None = None,
        proxy_url: str | None = None,
        proxy_headers: dict[str, str] | None = None,
        pool_maxsize: int = 10,
        *,
        max_connection_idle_secs: float | None = 60.0,
        tcp_keepalive_timeout_secs: int | None = 60,
    ) -> None:
        """Initialize one shared async HTTP transport.

        Args:
            ca_certs: Optional certificate-authority bundle path.
            proxy_url: Optional proxy URL.
            proxy_headers: Optional headers sent to the proxy.
            pool_maxsize: Maximum concurrent and keep-alive connections.
            max_connection_idle_secs: Validated idle limit in seconds, or
                `None` to disable expiry.
            tcp_keepalive_timeout_secs: Validated nominal keepalive target in
                seconds, or `None` to leave sockets untuned.

        Raises:
            OSError: If the certificate-authority bundle cannot be read or
                loaded into the SSL context.
        """
        verify: ssl.SSLContext | bool = True
        if ca_certs is not None:
            verify = ssl.create_default_context(cafile=ca_certs)
        proxy = None
        if proxy_url is not None:
            proxy = httpx2.Proxy(proxy_url, headers=proxy_headers)
        self._client = httpx2.AsyncClient(
            cookies=CookieJar(policy=DefaultCookiePolicy(allowed_domains=())),
            verify=verify,
            proxy=proxy,
            trust_env=False,
            limits=httpx2.Limits(
                max_connections=pool_maxsize,
                max_keepalive_connections=pool_maxsize,
                keepalive_expiry=max_connection_idle_secs,
            ),
            timeout=None,
        )
        self._request_options: _TraceRequestOptions = {}
        if tcp_keepalive_timeout_secs is not None:
            options = keepalive_socket_options(tcp_keepalive_timeout_secs)
            self._request_options['extensions'] = {'trace': _keepalive_trace(options)}
        self._active = 0
        self._body_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._closing = False
        self._close_task: asyncio.Task[None] | None = None
        self._idle = asyncio.Event()
        self._idle.set()
        self._lease_holders: dict[asyncio.Task[object], int] = {}
        self._reserved_tasks: set[asyncio.Task[object]] = set()
        self._stream_owners: dict[asyncio.Task[object], int] = {}

    def _acquire_lease(self, task: asyncio.Task[object]) -> None:
        """Acquire one active lease for an explicit task."""
        self._active += 1
        self._idle.clear()
        self._lease_holders[task] = self._lease_holders.get(task, 0) + 1

    def _end_active(self) -> None:
        """Release one active lease and wake a draining close when idle."""
        if self._active <= 0:
            raise TransportError('async transport lease accounting underflow')
        self._active -= 1
        if self._active == 0:
            self._idle.set()

    def _finish_body_cleanup(self, task: asyncio.Task[None]) -> None:
        """Consume and release one completed request-body cleanup task."""
        with suppress(BaseException):
            task.exception()
        if task not in self._body_cleanup_tasks:
            return
        self._body_cleanup_tasks.remove(task)
        self._end_active()

    async def _finish_close(self) -> None:
        """Drain active work, close the pool, and seal the transport."""
        try:
            await self._idle.wait()
            try:
                await self._client.aclose()
            except (httpx2.HTTPError, httpx2.StreamError) as error:
                raise _map_httpx2_error(error) from error
        finally:
            self._closed = True

    def _has_lease(self, task: asyncio.Task[object]) -> bool:
        """Return whether a task owns a logical RPC lease."""
        return self._lease_holders.get(task, 0) > 0

    async def _open_stream(
        self,
        request: httpx2.Request,
    ) -> httpx2.Response:
        """Send through the response-header phase and map backend errors."""
        try:
            return await self._client.send(request, stream=True)
        except TimeoutError as error:
            raise SendError(str(error)) from error
        except (httpx2.HTTPError, httpx2.StreamError) as error:
            raise _map_httpx2_error(error) from error

    def _release_lease(self, task: asyncio.Task[object]) -> None:
        """Release one active lease owned by an explicit task."""
        count = self._lease_holders.get(task, 0)
        if count <= 0:
            raise TransportError('async transport lease accounting underflow')
        if count == 1:
            del self._lease_holders[task]
        else:
            self._lease_holders[task] = count - 1
        self._end_active()

    def _release_reservation(self, task: asyncio.Task[object]) -> None:
        """Release one completed child-task reservation."""
        if task not in self._reserved_tasks:
            return
        self._reserved_tasks.remove(task)
        self._release_lease(task)

    def _release_stream(self, owner: asyncio.Task[object]) -> None:
        """Release an open-stream lease owned by a task."""
        count = self._stream_owners.get(owner, 0)
        if count <= 0:
            raise TransportError('async streaming lease accounting underflow')
        if count == 1:
            del self._stream_owners[owner]
        else:
            self._stream_owners[owner] = count - 1
        self._end_active()

    def _require_send_available(self, task: asyncio.Task[object]) -> None:
        """Reject new work after close while allowing a leased retry loop."""
        if self._closed:
            raise TransportError('async transport is closed')
        if self._closing and not self._has_lease(task):
            raise TransportError('async transport is closing')

    def _begin_stream(self) -> asyncio.Task[object]:
        """Acquire an open-stream lease for the current task."""
        owner = _current_task()
        self._require_send_available(owner)
        self._active += 1
        self._idle.clear()
        self._stream_owners[owner] = self._stream_owners.get(owner, 0) + 1
        return owner

    def _retain_body_cleanup(self, body: 'bytes | _ThreadedBody | None') -> None:
        """Retain background request-body cleanup as active transport work."""
        task = _start_adapted_body_cleanup(body)
        if task is None or task in self._body_cleanup_tasks:
            return
        self._body_cleanup_tasks.add(task)
        self._active += 1
        self._idle.clear()
        task.add_done_callback(self._finish_body_cleanup)

    def _retain_reserved_task(self, task: asyncio.Task[object]) -> None:
        """Retain one accepted child task until its done callback runs."""
        if task in self._reserved_tasks:
            raise TransportError('async transport task is already reserved')
        self._reserved_tasks.add(task)
        self._acquire_lease(task)
        task.add_done_callback(self._release_reservation)

    def _reserve_stream_task(
        self,
        task: asyncio.Task[object],
        owner: asyncio.Task[object],
    ) -> None:
        """Retain one child task on behalf of an open stream owner."""
        if self._closed or self._stream_owners.get(owner, 0) <= 0:
            raise TransportError('async transport stream is not active')
        self._retain_reserved_task(task)

    async def _send_once(self, request: TransportRequest) -> TransportResponse:
        """Send and convert one buffered response."""
        body = _adapt_body(request.body)
        try:
            response = await self._client.request(
                request.method,
                request.url,
                headers=request.headers,
                content=body,
                timeout=_to_httpx2_timeout(request.timeout),
                **self._request_options,
            )
        except BaseException:
            self._retain_body_cleanup(body)
            raise
        await _close_adapted_body(body)
        return TransportResponse(
            status=response.status_code,
            reason=response.reason_phrase,
            headers=CaseInsensitiveHeaders(response.headers),
            content=response.content,
        )

    def checkin(self) -> None:
        """Release the current task's logical RPC lease.

        Raises:
            TransportError: If the current task owns no active lease.
        """
        owner = _current_task()
        if not self._has_lease(owner):
            raise TransportError('current task has no active lease')
        self._release_lease(owner)

    def checkout(self) -> None:
        """Acquire a logical RPC lease for the current task.

        Raises:
            TransportError: If the transport is closed, or is closing and the
                current task does not already hold a lease.
        """
        owner = _current_task()
        self._require_send_available(owner)
        self._acquire_lease(owner)

    async def close(self) -> None:
        """Drain active leases and close the pool exactly once.

        Raises:
            TransportError: If the current task owns a lease whose release
                would depend on this call returning, or backend pool cleanup
                fails.
        """
        owner = _current_task()
        if self._has_lease(owner) or self._stream_owners.get(owner, 0) > 0:
            raise TransportError('cannot close async transport while holding an active lease')
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._finish_close())
        await asyncio.shield(self._close_task)

    def reserve(self, task: asyncio.Task[object]) -> None:
        """Retain one child task until its done callback runs.

        Args:
            task: Child task whose scheduled work must survive close.

        Raises:
            TransportError: If the transport rejects the reserving owner or
                `task` is already reserved.
        """
        owner = _current_task()
        self._require_send_available(owner)
        self._retain_reserved_task(task)

    async def send(self, request: TransportRequest) -> TransportResponse:
        """Send one buffered request.

        Args:
            request: Request expressed in shared transport types.

        Returns:
            The complete response in shared transport types.

        Raises:
            TransportError: If httpx2 cannot complete the request.
        """
        self._require_send_available(_current_task())
        try:
            return await self._send_once(request)
        except (httpx2.HTTPError, httpx2.StreamError) as error:
            raise _map_httpx2_error(error) from error

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        """Send one request and stream its body.

        Args:
            request: Request expressed in shared transport types.

        Returns:
            A single-use streaming response in shared transport types.

        Raises:
            TransportError: If the request or response fails.
        """
        current_task = _current_task()
        self._require_send_available(current_task)
        body = _adapt_body(request.body)
        try:
            outgoing = self._client.build_request(
                request.method,
                request.url,
                headers=request.headers,
                content=body,
                timeout=_to_httpx2_timeout(request.timeout),
                **self._request_options,
            )
        except (httpx2.HTTPError, httpx2.StreamError) as error:
            self._retain_body_cleanup(body)
            raise _map_httpx2_error(error) from error
        except BaseException:
            self._retain_body_cleanup(body)
            raise

        owner = self._begin_stream()
        try:
            response = await self._open_stream(outgoing)
        except BaseException:
            self._retain_body_cleanup(body)
            self._release_stream(owner)
            raise
        try:
            await _close_adapted_body(body)
        except BaseException:
            with suppress(BaseException):
                await response.aclose()
            self._release_stream(owner)
            raise

        body_owner = _ResponseBody(
            response,
            lambda: self._release_stream(owner),
            lambda task: self._reserve_stream_task(task, owner),
        )

        return AsyncStreamingResponse(
            status=response.status_code,
            reason=response.reason_phrase,
            headers=CaseInsensitiveHeaders(response.headers),
            stream=body_owner.stream,
            close_fn=body_owner.close,
        )


@dataclasses.dataclass(frozen=True)
class _PulledChunk:
    """Carry one worker-thread pull into its event-loop observation phase."""

    chunk: bytes | None
    observe: Callable[[bytes], None] | None


class _ResponseBody:
    """Own response pulls and cleanup until the stream lease is released."""

    def __init__(
        self,
        response: httpx2.Response,
        release: Callable[[], None],
        reserve: Callable[[asyncio.Task[object]], None],
    ) -> None:
        """Initialize one async response body."""
        self._pending: asyncio.Task[bytes] | None = None
        self._release = release
        self._reserve = reserve
        self._response = response
        self._task: asyncio.Task[None] | None = None

    async def _finish(self) -> None:
        """Join the active pull, close the response, and release its lease."""
        pending = self._pending
        if pending is not None:
            with suppress(BaseException):
                await asyncio.shield(pending)
            if self._pending is pending:
                self._pending = None
        try:
            await self._response.aclose()
        except (httpx2.HTTPError, httpx2.StreamError) as error:
            raise _map_httpx2_error(error) from error
        finally:
            self._release()

    def _start(self) -> asyncio.Task[None]:
        """Start idempotent cleanup retained by the transport."""
        if self._task is None:
            pending = self._pending
            if pending is not None:
                pending.cancel()
            self._task = asyncio.create_task(self._finish())
            self._reserve(self._task)
            self._task.add_done_callback(_consume_task_exception)
        return self._task

    async def _pull(self, iterator: AsyncIterator[bytes]) -> bytes:
        """Run one retained response pull behind a shielded consumer wait."""
        if self._pending is not None:
            raise TransportError('async response already has an active pull')
        pull = asyncio.create_task(_next_response_chunk(iterator))
        self._pending = pull
        try:
            self._reserve(pull)
        except BaseException:
            pull.cancel()
            with suppress(BaseException):
                await pull
            if self._pending is pull:
                self._pending = None
            raise
        try:
            try:
                return await asyncio.shield(pull)
            except asyncio.CancelledError:
                pull.cancel()
                self._start()
                raise
        finally:
            if pull.done() and self._pending is pull:
                self._pending = None

    async def close(self) -> None:
        """Wait for cleanup unless the calling task is cancelling.

        An already-cancelling caller leaves the retained cleanup task running
        without waiting for it.

        Raises:
            TransportError: If awaited backend response cleanup fails.
        """
        cleanup = self._start()
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            return
        await asyncio.shield(cleanup)

    async def stream(self, size: int) -> AsyncIterator[bytes]:
        """Yield response chunks with mapped backend errors.

        Args:
            size: Maximum preferred chunk size in bytes.

        Yields:
            Response body chunks.

        Raises:
            TransportError: If reading or closing the response stream fails.
        """
        try:
            iterator = self._response.aiter_bytes(size)
        except (httpx2.HTTPError, httpx2.StreamError) as error:
            raise _map_httpx2_error(error) from error
        while True:
            try:
                yield await self._pull(iterator)
            except StopAsyncIteration:
                return


class _ThreadedBody(AsyncIterator[bytes]):
    """Serialize off-loop iterator pulls with idempotent cleanup."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        """Initialize one threaded iterator-backed request body."""
        self._chunks = chunks
        self._close_task: asyncio.Task[None] | None = None
        self._pending: asyncio.Task[_PulledChunk] | None = None

    def __aiter__(self) -> Self:
        """Return this asynchronous body iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Return the next body chunk from the iterator thread."""
        if self._close_task is not None:
            raise StopAsyncIteration
        pull = asyncio.create_task(asyncio.to_thread(_next_chunk, self._chunks))
        self._pending = pull
        try:
            try:
                pulled = await asyncio.shield(pull)
            except asyncio.CancelledError:
                self.start_close()
                raise
            except Exception as error:
                raise SendError(str(error)) from error
        finally:
            if pull.done():
                self._pending = None
        chunk = pulled.chunk
        if chunk is None:
            await self.aclose()
            raise StopAsyncIteration
        if chunk and pulled.observe is not None:
            pulled.observe(chunk)
        return chunk

    async def _finish_close(self) -> None:
        """Wait for an active pull before closing its iterator."""
        pending = self._pending
        if pending is not None:
            with suppress(BaseException):
                await asyncio.shield(pending)
        await _close_body(self._chunks)

    def start_close(self) -> asyncio.Task[None]:
        """Start idempotent cleanup and retain its task."""
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._finish_close())
            self._close_task.add_done_callback(_consume_task_exception)
        return self._close_task

    async def aclose(self) -> None:
        """Close the underlying iterator exactly once and off-loop.

        An already-cancelling caller leaves the retained cleanup task running
        without waiting for it.

        Raises:
            SendError: If awaited iterator cleanup fails.
        """
        close_task = self.start_close()
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            return
        await asyncio.shield(close_task)


class _TraceRequestOptions(TypedDict, total=False):
    """Omit the trace extension entirely when socket tuning is disabled."""

    extensions: dict[str, Callable[[str, Mapping[str, object]], Awaitable[None]]]


def _adapt_body(
    body: bytes | Iterator[bytes] | None,
) -> bytes | _ThreadedBody | None:
    """Adapt a shared request body for httpx2 without blocking the loop."""
    if body is None or isinstance(body, bytes):
        return body
    return _ThreadedBody(body)


async def _close_adapted_body(body: bytes | _ThreadedBody | None) -> None:
    """Close an adapted streaming request body when one is present."""
    if isinstance(body, _ThreadedBody):
        await body.aclose()


async def _close_body(chunks: Iterator[bytes]) -> None:
    """Close a synchronous request body off the event loop when supported."""
    close = getattr(chunks, 'close', None)
    if callable(close):
        try:
            await asyncio.to_thread(cast('Callable[[], object]', close))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise SendError(str(error)) from error


def _consume_task_exception(task: asyncio.Task[None]) -> None:
    """Retrieve a retained cleanup task's terminal exception."""
    with suppress(BaseException):
        task.exception()


def _current_task() -> asyncio.Task[object]:
    """Return the current task or reject use outside an event-loop task."""
    task = asyncio.current_task()
    if task is None:
        raise TransportError('async transport operation requires a running task')
    return task


def _is_tls_failure(error: BaseException) -> bool:
    """Return whether an exception cause chain contains an SSL failure."""
    cause = error.__cause__
    while cause is not None:
        if isinstance(cause, ssl.SSLError):
            return True
        cause = cause.__cause__
    return False


def _keepalive_trace(
    options: tuple[SocketOption, ...],
) -> Callable[[str, Mapping[str, object]], Awaitable[None]]:
    """Tune the socket exposed by each completed TCP connection trace event."""

    async def trace(event: str, info: Mapping[str, object]) -> None:
        if event != 'connection.connect_tcp.complete':
            return
        get_extra_info = getattr(info['return_value'], 'get_extra_info', None)
        if get_extra_info is None:
            return
        sock = get_extra_info('socket')
        if sock is not None:
            tune_socket(sock, options)

    return trace


def _map_httpx2_error(error: httpx2.HTTPError | httpx2.StreamError) -> TransportError:
    """Map an httpx2 failure into the shared transport vocabulary."""
    if isinstance(error, httpx2.ProxyError):
        return ProxyError(str(error))
    if isinstance(error, (httpx2.ConnectError, httpx2.ConnectTimeout)):
        if _is_tls_failure(error):
            return TLSError(str(error))
        return ConnectError(str(error))
    if isinstance(error, httpx2.PoolTimeout):
        return ConnectError(str(error) or 'timed out waiting for a connection from the pool')
    if isinstance(error, httpx2.ReadTimeout):
        return ReadTimeoutError(str(error))
    return SendError(str(error))


def _next_chunk(chunks: Iterator[bytes]) -> _PulledChunk:
    """Pull one chunk and capture its event-loop observer when supported."""
    if (
        isinstance(chunks, AsyncUploadObserver)
        and callable(chunks.pull_for_async)
        and callable(chunks.observe_async_chunk)
    ):
        return _PulledChunk(
            chunk=chunks.pull_for_async(),
            observe=chunks.observe_async_chunk,
        )
    return _PulledChunk(chunk=next(chunks, None), observe=None)


async def _next_response_chunk(
    iterator: AsyncIterator[bytes],
) -> bytes:
    """Read one response chunk and map backend errors."""
    try:
        return await anext(iterator)
    except TimeoutError as error:
        raise SendError(str(error)) from error
    except (httpx2.HTTPError, httpx2.StreamError) as error:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise asyncio.CancelledError from error
        raise _map_httpx2_error(error) from error


def _start_adapted_body_cleanup(
    body: bytes | _ThreadedBody | None,
) -> asyncio.Task[None] | None:
    """Start and return request-body cleanup without delaying an error path."""
    if isinstance(body, _ThreadedBody):
        return body.start_close()
    return None


def _to_httpx2_timeout(timeout: TimeoutConfig) -> httpx2.Timeout:
    """Convert shared timeout fields to httpx2 phase budgets."""
    return httpx2.Timeout(
        connect=timeout.connect,
        read=timeout.read,
        write=timeout.read,
        pool=timeout.connect,
    )
