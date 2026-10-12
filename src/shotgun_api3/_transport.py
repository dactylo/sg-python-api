"""Transport vocabulary and HTTP backend implementations."""

# Same-file collaboration follows module-boundary privacy.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import contextvars
import socket
import ssl
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, cast

import urllib3
import urllib3.connectionpool
import urllib3.exceptions
from urllib3.util.ssl_match_hostname import CertificateError

from ._errors import ShotgunError

if TYPE_CHECKING:
    from urllib3._base_connection import BaseHTTPConnection

_CERTIFICATE_ERRORS = (ssl.SSLCertVerificationError, CertificateError)
_CURRENT_ABORT: contextvars.ContextVar[tuple[_AbortRegistry, _AbortHandle] | None] = (
    contextvars.ContextVar('_CURRENT_ABORT', default=None)
)


class CaseInsensitiveHeaders(Mapping[str, str]):
    """Immutable response headers with case-insensitive lookup."""

    _items: tuple[tuple[str, str], ...]
    _lookup: Mapping[str, str]

    def __init__(self, headers: Mapping[str, str]) -> None:
        """Index response headers for case-insensitive lookup.

        Args:
            headers: Response headers in their received iteration order.
        """
        names: dict[str, str] = {}
        order: list[str] = []
        values: dict[str, str] = {}
        for name, value in headers.items():
            normalized = name.casefold()
            if normalized not in values:
                names[normalized] = name
                order.append(normalized)
            values[normalized] = value

        self._items = tuple((names[key], values[key]) for key in order)
        self._lookup = MappingProxyType(values)

    def __getitem__(self, key: str) -> str:
        """Return one header value by case-insensitive name."""
        return self._lookup[key.casefold()]

    def __iter__(self) -> Iterator[str]:
        """Iterate over headers in received order."""
        return (name for name, _value in self._items)

    def __len__(self) -> int:
        """Return the number of response headers."""
        return len(self._items)


@dataclass(init=False)
class StreamingResponse:
    """A response whose body is consumed incrementally exactly once.

    Fully consuming the body releases response resources. Call `close()` when
    leaving the body unconsumed or stopping early. Explicit closure is
    idempotent.

    Attributes:
        status: HTTP status code.
        reason: HTTP reason phrase.
        headers: Response headers.
    """

    status: int
    reason: str
    headers: CaseInsensitiveHeaders
    _stream: Callable[[int], Iterator[bytes]]
    _release: Callable[[], None] | None
    _claimed: bool
    _close_lock: threading.Lock

    def __init__(
        self,
        status: int,
        reason: str,
        headers: Mapping[str, str],
        stream: Callable[[int], Iterator[bytes]],
        release: Callable[[], None],
    ) -> None:
        """Initialize a one-use streaming response.

        Args:
            status: HTTP status code.
            reason: HTTP reason phrase.
            headers: Response headers.
            stream: Factory returning body chunks for a requested size.
            release: Callback that releases response resources.
        """
        self.status = status
        self.reason = reason
        self.headers = (
            headers
            if isinstance(headers, CaseInsensitiveHeaders)
            else CaseInsensitiveHeaders(headers)
        )
        self._stream = stream
        self._release = release
        self._claimed = False
        self._close_lock = threading.Lock()

    def close(self) -> None:
        """Release resources associated with the streaming response."""
        with self._close_lock:
            release = self._release
            self._release = None
        if release is not None:
            release()

    def _iterate(self, size: int) -> Iterator[bytes]:
        """Yield chunks while closing on every terminal iterator path."""
        try:
            with self._close_lock:
                if self._release is None:
                    raise TransportError('streaming response is closed')
            for chunk in self._stream(size):
                with self._close_lock:
                    if self._release is None:
                        raise TransportError('streaming response is closed')
                yield chunk
        finally:
            self.close()

    def iter_chunks(self, size: int = 65536) -> Iterator[bytes]:
        """Claim and return the single-use response-body iterator.

        Args:
            size: Maximum bytes per yielded chunk.

        Returns:
            An iterator over the response body.

        Raises:
            TransportError: If `size` is not positive, the response is closed,
                or its stream has already been claimed.
        """
        if size <= 0:
            raise TransportError('streaming chunk size must be positive')
        with self._close_lock:
            if self._release is None:
                raise TransportError('streaming response is closed')
            if self._claimed:
                raise TransportError('streaming response was already consumed')
            self._claimed = True
        return self._iterate(size)


@dataclass(frozen=True)
class TimeoutConfig:
    """Connect/read limits and total monotonic ceiling through body consumption.

    The total limit applies per attempt. A `None` field has no timeout limit.

    Attributes:
        connect: Connection-establishment timeout in seconds.
        read: Socket-read timeout in seconds.
        total: Total attempt timeout through body consumption.
    """

    connect: float | None
    read: float | None
    total: float | None = None


class TransportError(ShotgunError):
    """Base exception for transport-level failures."""


class AbortedError(TransportError):
    """Raised when a request is deliberately aborted via its token."""


class ConnectError(TransportError):
    """Raised when establishing a connection fails before transmission."""


class ProxyError(TransportError):
    """Raised when proxy connection or negotiation fails before transmission."""


class ReadTimeoutError(TransportError):
    """Raised when waiting for response data times out."""


class SendError(TransportError):
    """Raised after a request may have started transmitting."""


class TLSError(TransportError):
    """Raised when TLS setup or verification fails before transmission."""


@dataclass(frozen=True)
class TransportRequest:
    """One HTTP request in transport vocabulary.

    Attributes:
        method: HTTP method.
        url: Complete request URL.
        headers: Request headers.
        body: Buffered or streaming request body.
        timeout: Timeout limits for this request.
        token: Unique token used for cancellation.
    """

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | Iterator[bytes] | None
    timeout: TimeoutConfig
    token: str


@dataclass(frozen=True, init=False)
class TransportResponse:
    """One HTTP response in transport vocabulary.

    Attributes:
        status: HTTP status code.
        reason: HTTP reason phrase.
        headers: Response headers.
        content: Complete buffered response body.
    """

    status: int
    reason: str
    headers: CaseInsensitiveHeaders
    content: bytes
    _close_lock: threading.Lock = field(init=False, repr=False, compare=False)
    _on_close: Callable[[], None] | None = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        status: int,
        reason: str,
        headers: Mapping[str, str],
        content: bytes,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        """Initialize one buffered transport response.

        Args:
            status: HTTP status code.
            reason: HTTP reason phrase.
            headers: Response headers.
            content: Complete buffered response body.
            on_close: Optional callback invoked once when closed.
        """
        normalized_headers = (
            headers
            if isinstance(headers, CaseInsensitiveHeaders)
            else CaseInsensitiveHeaders(headers)
        )
        object.__setattr__(self, 'status', status)
        object.__setattr__(self, 'reason', reason)
        object.__setattr__(self, 'headers', normalized_headers)
        object.__setattr__(self, 'content', content)
        object.__setattr__(self, '_close_lock', threading.Lock())
        object.__setattr__(self, '_on_close', on_close)

    def close(self) -> None:
        """Release resources associated with the buffered response."""
        with self._close_lock:
            callback = self._on_close
            object.__setattr__(self, '_on_close', None)
        if callback is not None:
            callback()


class Transport(Protocol):
    """HTTP backend operations required by the driver."""

    def abort(self, token: str) -> None:
        """Abort the request identified by a token.

        Args:
            token: Unique request token.
        """
        ...

    def close(self) -> None:
        """Release resources owned by the transport."""
        ...

    def send(self, request: TransportRequest) -> TransportResponse:
        """Send one request and return its complete response.

        Args:
            request: Transport request to send.

        Returns:
            The complete transport response.

        Raises:
            TransportError: If the transport operation fails.
        """
        ...

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        """Send one request and return its single-use streaming response.

        Args:
            request: Transport request to send.

        Returns:
            The streaming transport response.

        Raises:
            TransportError: If the transport operation fails.
        """
        ...


class _AbortableConnection(Protocol):
    """Socket operations required to abort an urllib3 connection."""

    sock: socket.socket | ssl.SSLSocket | None

    def close(self) -> None:
        """Close the connection and prevent its socket from being reused."""


@dataclass(eq=False)
class _AbortHandle:
    """Track one request registration independently of token reuse."""

    token: str
    generation: int
    aborted: bool = False
    connection: _AbortableConnection | None = None


class _AbortRegistry:
    """Track live connections and aborted requests under a lock."""

    def __init__(self) -> None:
        """Initialize an empty transport-abort registry."""
        self._active: dict[str, dict[int, _AbortHandle]] = {}
        self._generation = 0
        self._lock = threading.Lock()

    def _abort(self, token: str) -> None:
        """Abort every active request registered for `token`."""
        with self._lock:
            connections: list[_AbortableConnection] = []
            for handle in self._active.get(token, {}).values():
                handle.aborted = True
                if handle.connection is not None:
                    connections.append(handle.connection)

        for connection in connections:
            live_socket = connection.sock
            if live_socket is not None:
                with suppress(OSError):
                    live_socket.shutdown(socket.SHUT_RDWR)
                if sys.platform == 'win32':
                    # Buffered readers defer close on Windows, leaving recv blocked.
                    # Transfer ownership so later reader closes cannot close a reused fd.
                    with suppress(OSError):
                        descriptor = live_socket.detach()
                        if descriptor != -1:
                            socket.close(descriptor)
            connection.close()

    def _attach(
        self,
        handle: _AbortHandle,
        connection: _AbortableConnection,
    ) -> bool:
        """Attach a checked-out connection and report an earlier abort."""
        with self._lock:
            active = self._active.get(handle.token, {}).get(handle.generation)
            if active is handle:
                handle.connection = connection
            return handle.aborted

    def _begin(self, token: str) -> _AbortHandle:
        """Begin tracking a request before it checks out a connection."""
        with self._lock:
            self._generation += 1
            handle = _AbortHandle(token=token, generation=self._generation)
            self._active.setdefault(token, {})[handle.generation] = handle
            return handle

    def _detach(self, handle: _AbortHandle) -> bool:
        """Detach a connection before urllib3 can return it to the pool."""
        with self._lock:
            handle.connection = None
            return handle.aborted

    def _release(self, handle: _AbortHandle) -> None:
        """Release exactly one request generation."""
        with self._lock:
            active = self._active.get(handle.token)
            if active is not None and active.get(handle.generation) is handle:
                active.pop(handle.generation)
                if not active:
                    self._active.pop(handle.token)
            handle.connection = None

    def _was_aborted(self, handle: _AbortHandle) -> bool:
        """Return whether a particular request was aborted."""
        with self._lock:
            return handle.aborted


@dataclass(frozen=True)
class _StartedUrllib3Request:
    """Hold request-start state whose lifetime remains caller-owned."""

    response: urllib3.response.HTTPResponse
    handle: _AbortHandle
    started: float


class _HTTPPool(urllib3.connectionpool.HTTPConnectionPool):
    """HTTP pool that aligns abort registration with connection ownership."""

    def _get_conn(self, timeout: float | None = None) -> BaseHTTPConnection:
        """Attach a request handle when a connection is checked out."""
        return _attach_checked_out_connection(super()._get_conn(timeout))

    def _put_conn(self, conn: BaseHTTPConnection | None) -> None:
        """Detach a request handle before returning a connection."""
        super()._put_conn(_detach_returned_connection(conn))

    def _validate_conn(self, conn: BaseHTTPConnection) -> None:
        """Connect while checking abort state before and after socket creation."""
        _raise_if_current_aborted(conn)
        super()._validate_conn(conn)
        if conn.is_closed:
            conn.connect()
        _raise_if_current_aborted(conn)


class _HTTPSPool(urllib3.connectionpool.HTTPSConnectionPool):
    """HTTPS pool that aligns abort registration with connection ownership."""

    def _get_conn(self, timeout: float | None = None) -> BaseHTTPConnection:
        """Attach a request handle when a connection is checked out."""
        return _attach_checked_out_connection(super()._get_conn(timeout))

    def _put_conn(self, conn: BaseHTTPConnection | None) -> None:
        """Detach a request handle before returning a connection."""
        super()._put_conn(_detach_returned_connection(conn))

    def _validate_conn(self, conn: BaseHTTPConnection) -> None:
        """Validate TLS while checking abort state around socket creation."""
        _raise_if_current_aborted(conn)
        super()._validate_conn(conn)
        _raise_if_current_aborted(conn)


class Urllib3Transport:
    """Transport implementation on a urllib3 connection pool.

    Attributes:
        proxy_info: Compatibility proxy metadata for a direct connection.
    """

    def __init__(
        self,
        ca_certs: str | None = None,
        proxy_url: str | None = None,
        proxy_headers: dict[str, str] | None = None,
        pool_maxsize: int = 10,
    ) -> None:
        """Initialize a urllib3 connection pool transport.

        Args:
            ca_certs: Optional certificate-authority bundle path.
            proxy_url: Optional proxy URL.
            proxy_headers: Optional headers sent to the proxy.
            pool_maxsize: Maximum connections retained by the pool.
        """
        self._abort_registry = _AbortRegistry()
        common: dict[str, Any] = {'maxsize': pool_maxsize}
        if ca_certs is not None:
            common['ca_certs'] = ca_certs
        if proxy_url is not None:
            self._pool: urllib3.PoolManager = urllib3.ProxyManager(
                proxy_url,
                proxy_headers=proxy_headers,
                **common,
            )
        else:
            self._pool = urllib3.PoolManager(**common)
        pool_classes: dict[
            str,
            type[
                urllib3.connectionpool.HTTPConnectionPool
                | urllib3.connectionpool.HTTPSConnectionPool
            ],
        ] = {
            'http': _HTTPPool,
            'https': _HTTPSPool,
        }
        self._pool.pool_classes_by_scheme = pool_classes

    def _start_request(
        self,
        request: TransportRequest,
        *,
        preload_content: bool,
    ) -> _StartedUrllib3Request:
        """Start one urllib3 request and leave its live handle caller-owned."""
        timeout = urllib3.Timeout(
            connect=request.timeout.connect,
            read=request.timeout.read,
            total=request.timeout.total,
        )
        handle = self._abort_registry._begin(request.token)
        context_token = _CURRENT_ABORT.set((self._abort_registry, handle))
        try:
            try:
                started = time.monotonic()
                response = cast(
                    'urllib3.response.HTTPResponse',
                    self._pool.request(
                        request.method,
                        request.url,
                        body=request.body,
                        headers=request.headers,
                        timeout=timeout,
                        retries=False,
                        preload_content=preload_content,
                    ),
                )
            except urllib3.exceptions.HTTPError as exc:
                if self._abort_registry._was_aborted(handle):
                    raise AbortedError(request.token) from exc
                raise _map_urllib3_error(exc) from exc
            except urllib3.exceptions.ProxySchemeUnsupported as exc:
                raise ProxyError(str(exc)) from exc
            except Exception as exc:
                raise _map_unexpected_request_error(
                    exc, self._abort_registry, handle, request.token
                ) from exc
        except BaseException:
            self._abort_registry._release(handle)
            raise
        finally:
            _CURRENT_ABORT.reset(context_token)
        return _StartedUrllib3Request(response=response, handle=handle, started=started)

    def abort(self, token: str) -> None:
        """Abort the in-flight request identified by a token.

        Args:
            token: Unique request token.
        """
        self._abort_registry._abort(token)

    def close(self) -> None:
        """Release pooled connections owned by the transport."""
        self._pool.clear()

    @property
    def proxy_info(self) -> None:
        """Return compatibility proxy metadata for a direct connection.

        Returns:
            `None` when no proxy is configured.

        Raises:
            AttributeError: If the transport is configured to use a proxy.
        """
        if isinstance(self._pool, urllib3.ProxyManager):
            raise AttributeError('proxy_info')  # noqa: TRY004

    def send(self, request: TransportRequest) -> TransportResponse:
        """Send one request without retrying it.

        Args:
            request: Transport request to send.

        Returns:
            The complete transport response.

        Raises:
            TransportError: If urllib3 reports a transport failure.
        """
        started = self._start_request(request, preload_content=True)
        try:
            return TransportResponse(
                status=started.response.status,
                reason=started.response.reason or '',
                headers=dict(started.response.headers),
                content=started.response.data,
            )
        finally:
            self._abort_registry._release(started.handle)

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        """Send one request and return its single-use streaming response.

        Args:
            request: Transport request to send.

        Returns:
            The streaming transport response.

        Raises:
            TransportError: If urllib3 reports a transport failure while
                receiving response headers.
        """
        started = self._start_request(request, preload_content=False)
        response = started.response

        deadline = (
            None if request.timeout.total is None else started.started + request.timeout.total
        )

        def release() -> None:
            _release_urllib3_stream(response, self._abort_registry, started.handle)

        def stream(size: int) -> Iterator[bytes]:
            attempt = _Urllib3StreamingAttempt(
                deadline=deadline,
                handle=started.handle,
                read_timeout=request.timeout.read,
                registry=self._abort_registry,
                response=response,
                token=request.token,
            )
            return _stream_urllib3_response(attempt, size)

        try:
            return StreamingResponse(
                status=response.status,
                reason=response.reason or '',
                headers=dict(response.headers),
                stream=stream,
                release=release,
            )
        except BaseException:
            release()
            raise


@dataclass(frozen=True)
class _Urllib3StreamingAttempt:
    """Bundle state that remains live through streaming body consumption."""

    deadline: float | None
    handle: _AbortHandle
    read_timeout: float | None
    registry: _AbortRegistry
    response: urllib3.response.HTTPResponse
    token: str


def _attach_checked_out_connection(
    connection: BaseHTTPConnection,
) -> BaseHTTPConnection:
    """Attach the active request handle to a checked-out connection."""
    current = _CURRENT_ABORT.get()
    if current is None:
        return connection
    registry, handle = current
    abortable = cast('_AbortableConnection', connection)
    if registry._attach(handle, abortable):
        connection.close()
        raise AbortedError(handle.token)
    return connection


def _detach_returned_connection(
    connection: BaseHTTPConnection | None,
) -> BaseHTTPConnection | None:
    """Detach the active handle and discard a connection lost to abort."""
    current = _CURRENT_ABORT.get()
    if current is None:
        return connection
    registry, handle = current
    if not registry._detach(handle):
        return connection
    if connection is not None:
        connection.close()
    return None


def _is_certificate_failure(exc: BaseException) -> bool:
    """Return whether an exception contains certificate-verification evidence."""
    if isinstance(exc, _CERTIFICATE_ERRORS):
        return True
    nested = (exc.__cause__, exc.__context__, *exc.args)
    return any(isinstance(item, _CERTIFICATE_ERRORS) for item in nested)


def _map_unexpected_request_error(
    exc: Exception,
    registry: _AbortRegistry,
    handle: _AbortHandle,
    token: str,
) -> TransportError:
    """Translate an unexpected request failure into transport vocabulary."""
    if registry._was_aborted(handle):
        return AbortedError(token)
    return SendError(str(exc))


def _map_urllib3_error(exc: urllib3.exceptions.HTTPError) -> TransportError:
    """Translate a urllib3 exception into the transport vocabulary."""
    unwrapped: BaseException = exc
    if isinstance(exc, urllib3.exceptions.MaxRetryError) and exc.reason is not None:
        unwrapped = exc.reason
    if isinstance(unwrapped, urllib3.exceptions.ProxyError):
        return ProxyError(str(unwrapped))
    if isinstance(unwrapped, urllib3.exceptions.SSLError) and _is_certificate_failure(unwrapped):
        return TLSError(str(unwrapped))
    if isinstance(unwrapped, urllib3.exceptions.ReadTimeoutError):
        return ReadTimeoutError(str(unwrapped))
    if isinstance(
        unwrapped,
        (urllib3.exceptions.NewConnectionError, urllib3.exceptions.ConnectTimeoutError),
    ):
        return ConnectError(str(unwrapped))
    return SendError(str(unwrapped))


def _next_urllib3_stream_chunk(
    attempt: _Urllib3StreamingAttempt,
    chunks: Iterator[bytes],
    total_limited: bool,
) -> bytes:
    """Read one chunk while translating every backend body failure."""
    try:
        return next(chunks)
    except StopIteration:
        raise
    except urllib3.exceptions.HTTPError as exc:
        if attempt.registry._was_aborted(attempt.handle):
            raise AbortedError(attempt.token) from exc
        mapped = _map_urllib3_error(exc)
        if total_limited and isinstance(mapped, ReadTimeoutError):
            raise ReadTimeoutError('total streaming timeout exceeded') from exc
        raise mapped from exc
    except urllib3.exceptions.ProxySchemeUnsupported as exc:
        raise ProxyError(str(exc)) from exc
    except Exception as exc:
        if attempt.registry._was_aborted(attempt.handle):
            raise AbortedError(attempt.token) from exc
        raise SendError(str(exc)) from exc


def _raise_if_current_aborted(connection: BaseHTTPConnection) -> None:
    """Close a connection and raise when its active request was aborted."""
    current = _CURRENT_ABORT.get()
    if current is None:
        return
    registry, handle = current
    if registry._was_aborted(handle):
        connection.close()
        raise AbortedError(handle.token)


def _release_urllib3_stream(
    response: urllib3.response.HTTPResponse,
    registry: _AbortRegistry,
    handle: _AbortHandle,
) -> None:
    """Release one streaming registry handle and any owned connection."""
    registry._release(handle)
    if response.connection is None:
        return
    try:
        response.close()
    finally:
        response.release_conn()


def _set_stream_read_timeout(
    attempt: _Urllib3StreamingAttempt,
) -> bool:
    """Apply the active timeout and report whether total is the limiter."""
    if attempt.deadline is None:
        return False
    remaining = attempt.deadline - time.monotonic()
    if remaining <= 0:
        raise ReadTimeoutError('total streaming timeout exceeded')
    total_limited = attempt.read_timeout is None or remaining <= attempt.read_timeout
    connection = attempt.response.connection
    if connection is None:
        return total_limited
    live_socket = connection.sock
    if live_socket is None:
        return total_limited
    try:
        live_socket.settimeout(
            remaining if attempt.read_timeout is None else min(attempt.read_timeout, remaining)
        )
    except Exception as exc:
        raise _map_unexpected_request_error(
            exc, attempt.registry, attempt.handle, attempt.token
        ) from exc
    return total_limited


def _stream_urllib3_response(
    attempt: _Urllib3StreamingAttempt,
    size: int,
) -> Iterator[bytes]:
    """Yield body chunks while mapping errors and enforcing the deadline."""
    chunks = attempt.response.stream(size)
    while True:
        if attempt.registry._was_aborted(attempt.handle):
            raise AbortedError(attempt.token)
        total_limited = _set_stream_read_timeout(attempt)
        context_token = _CURRENT_ABORT.set((attempt.registry, attempt.handle))
        try:
            try:
                chunk = _next_urllib3_stream_chunk(attempt, chunks, total_limited)
            except StopIteration:
                return
        finally:
            _CURRENT_ABORT.reset(context_token)
        if attempt.deadline is not None and time.monotonic() > attempt.deadline:
            raise ReadTimeoutError('total streaming timeout exceeded')
        yield chunk


def body_close(body: object) -> Callable[[], object] | None:
    """Return the body's callable close operation when present.

    Returns:
        Callable close operation, or `None` when unavailable.
    """
    close = getattr(body, 'close', None)
    if not callable(close):
        return None
    return cast('Callable[[], object]', close)


def content_length(headers: Mapping[str, str]) -> int | None:
    """Parse a nonnegative content length when present.

    Returns:
        Parsed nonnegative length, or `None` when absent or invalid.
    """
    raw = next(
        (value for name, value in headers.items() if name.casefold() == 'content-length'),
        None,
    )
    if raw is None:
        return None
    try:
        length = int(raw)
    except ValueError:
        return None
    return length if length >= 0 else None


def narrow_total_timeout(
    request: TransportRequest,
    remaining: float | None,
) -> TransportRequest:
    """Restrict a request to its nonnegative remaining total timeout.

    Args:
        request: Original transport request.
        remaining: Remaining total timeout in seconds, when limited.

    Returns:
        The original request when `remaining` is `None`. Otherwise, a
        replacement whose total timeout is `remaining` clamped to zero and
        capped by any existing total timeout. Headers and body are shared with
        the original request.
    """
    if remaining is None:
        return request
    narrowed_total = max(remaining, 0.0)
    if request.timeout.total is not None:
        narrowed_total = min(request.timeout.total, narrowed_total)
    return replace(
        request,
        timeout=replace(request.timeout, total=narrowed_total),
    )
