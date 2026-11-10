"""Transport vocabulary and HTTP backend implementations."""

from __future__ import annotations

import ssl
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, cast

import urllib3
import urllib3.exceptions
from urllib3.util.ssl_match_hostname import CertificateError

from ._connections import connection_pool_classes, keepalive_socket_options
from ._errors import ShotgunError

_CERTIFICATE_ERRORS = (ssl.SSLCertVerificationError, CertificateError)


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
        """Return one header value by case-insensitive name.

        Raises:
            KeyError: If no header matches the requested name.
        """
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
        """Run the response cleanup callback at most once.

        The callback is cleared before invocation, so a failed cleanup is not
        retried by later calls.

        Raises:
            Exception: If the supplied cleanup callback fails.
        """
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
    """Connect and read timeout limits.

    A `None` field has no timeout limit.

    Attributes:
        connect: Connection-establishment timeout in seconds.
        read: Socket-read timeout in seconds.
    """

    connect: float | None
    read: float | None


class TransportError(ShotgunError):
    """Base exception for transport-level failures."""


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
    """

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | Iterator[bytes] | None
    timeout: TimeoutConfig


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
        """Run the response cleanup callback at most once.

        The callback is cleared before invocation, so a failed cleanup is not
        retried by later calls.

        Raises:
            Exception: If the supplied cleanup callback fails.
        """
        with self._close_lock:
            callback = self._on_close
            object.__setattr__(self, '_on_close', None)
        if callback is not None:
            callback()


class Transport(Protocol):
    """HTTP backend operations required by the driver."""

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


class Urllib3Transport:
    """Transport implementation on a urllib3 connection pool.

    Attributes:
        proxy_info: Compatibility proxy metadata for a direct connection.
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
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize a urllib3 connection pool transport.

        Args:
            ca_certs: Optional certificate-authority bundle path.
            proxy_url: Optional proxy URL.
            proxy_headers: Optional headers sent to the proxy.
            pool_maxsize: Maximum connections retained by the pool.
            max_connection_idle_secs: Validated idle limit in seconds, or
                `None` to disable expiry. Counted after body preload for
                buffered responses, or after headers for streamed responses.
            tcp_keepalive_timeout_secs: Validated nominal keepalive target in
                seconds, or `None` to disable tuning.
            clock: Monotonic clock returning seconds.
        """
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
        self._pool.pool_classes_by_scheme = connection_pool_classes(
            max_connection_idle_secs, keepalive_socket_options(tcp_keepalive_timeout_secs), clock
        )

    def _start_request(
        self,
        request: TransportRequest,
        *,
        preload_content: bool,
    ) -> urllib3.response.HTTPResponse:
        """Start one urllib3 request and return its response."""
        timeout = urllib3.Timeout(
            connect=request.timeout.connect,
            read=request.timeout.read,
        )
        try:
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
            raise _map_urllib3_error(exc) from exc
        except urllib3.exceptions.ProxySchemeUnsupported as exc:
            raise ProxyError(str(exc)) from exc
        except Exception as exc:
            raise SendError(str(exc)) from exc
        return response

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
        response = self._start_request(request, preload_content=True)
        return TransportResponse(
            status=response.status,
            reason=response.reason or '',
            headers=dict(response.headers),
            content=response.data,
        )

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
        response = self._start_request(request, preload_content=False)

        def release() -> None:
            _release_urllib3_stream(response)

        def stream(size: int) -> Iterator[bytes]:
            return _stream_urllib3_response(response, size)

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


def _is_certificate_failure(exc: BaseException) -> bool:
    """Return whether an exception contains certificate-verification evidence."""
    if isinstance(exc, _CERTIFICATE_ERRORS):
        return True
    nested = (exc.__cause__, exc.__context__, *exc.args)
    return any(isinstance(item, _CERTIFICATE_ERRORS) for item in nested)


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
    chunks: Iterator[bytes],
) -> bytes:
    """Read one chunk while translating every backend body failure."""
    try:
        return next(chunks)
    except StopIteration:
        raise
    except urllib3.exceptions.HTTPError as exc:
        raise _map_urllib3_error(exc) from exc
    except urllib3.exceptions.ProxySchemeUnsupported as exc:
        raise ProxyError(str(exc)) from exc
    except Exception as exc:
        raise SendError(str(exc)) from exc


def _release_urllib3_stream(
    response: urllib3.response.HTTPResponse,
) -> None:
    """Close a streaming response and return its owned connection."""
    if response.connection is None:
        return
    try:
        response.close()
    finally:
        response.release_conn()


def _stream_urllib3_response(
    response: urllib3.response.HTTPResponse,
    size: int,
) -> Iterator[bytes]:
    """Yield body chunks while mapping backend errors."""
    chunks = response.stream(size)
    while True:
        try:
            chunk = _next_urllib3_stream_chunk(chunks)
        except StopIteration:
            return
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

    Args:
        headers: Response headers whose names are matched case-insensitively.

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
