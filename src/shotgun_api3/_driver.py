"""Common attempt, consumption, retry, and observer orchestration."""

import concurrent.futures
import copy
import dataclasses
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import suppress
from functools import partial
from io import BytesIO
from typing import Literal, Protocol, TypeVar, cast, runtime_checkable

from . import _protocol, _transport
from ._config import (
    EventRoute,
    ObserverCallback,
    Outcome,
    ProgressEvent,
    RequestContext,
    RequestEvent,
    RetryEvent,
    compatibility_warning_events,
    event_route,
    notify_observer,
    response_correlation_id,
)
from ._execution import (
    AttemptOutcomes,
    AttemptRetry,
    AttemptState,
    AttemptSuccess,
    ExceptionCapture,
    build_rpc_request,
)
from ._transport import (
    ReadTimeoutError,
    SendError,
    TransportError,
    TransportRequest,
)

_T = TypeVar('_T')

_HTTP_REDIRECT = 302


@dataclasses.dataclass(frozen=True)
class AttemptControl:
    """Consume attempt bytes under deadline and progress control."""

    _attempt: int
    _clock: Callable[[], float]
    _deadline: float | None
    _observer: ObserverCallback | None
    _operation: _protocol.OperationCategory
    _route: EventRoute

    def _check_deadline(self) -> None:
        """Raise when cumulative monotonic attempt time exceeds its limit."""
        if self._deadline is not None and self._clock() > self._deadline:
            raise ReadTimeoutError('total attempt timeout exceeded')

    def _consume_buffered(self, body: bytes, total_bytes: int | None = None) -> bytes:
        """Observe a received buffered body without rebuilding it."""
        self._check_deadline()
        if body:
            notify_observer(
                self._observer,
                ProgressEvent(
                    operation=self._operation,
                    route=self._route,
                    attempt=self._attempt,
                    bytes_transferred=len(body),
                    total_bytes=total_bytes,
                ),
            )
        return body

    def _remaining_timeout(self) -> float | None:
        """Return the remaining cumulative attempt timeout."""
        if self._deadline is None:
            return None
        remaining = self._deadline - self._clock()
        if remaining < 0:
            raise ReadTimeoutError('total attempt timeout exceeded')
        return remaining

    def consume_into(
        self,
        chunks: Iterable[bytes],
        sink: Callable[[bytes], object],
        total_bytes: int | None = None,
    ) -> None:
        """Write chunks while enforcing deadline and progress controls.

        Args:
            chunks: Fresh streaming body chunks for this attempt.
            sink: Attempt-local byte sink.
            total_bytes: Known complete body size, when available.

        Raises:
            ReadTimeoutError: If cumulative attempt time exceeds the total
                limit.
        """
        transferred = 0
        for chunk in chunks:
            sink(chunk)
            transferred += len(chunk)
            self._check_deadline()
            if chunk:
                notify_observer(
                    self._observer,
                    ProgressEvent(
                        operation=self._operation,
                        route=self._route,
                        attempt=self._attempt,
                        bytes_transferred=transferred,
                        total_bytes=total_bytes,
                    ),
                )

    def consume(self, chunks: Iterable[bytes], total_bytes: int | None = None) -> bytes:
        """Collect chunks while enforcing the total attempt deadline.

        Args:
            chunks: Fresh buffered or streaming body chunks for this attempt.
            total_bytes: Known complete body size, when available.

        Returns:
            Concatenated response bytes.

        Raises:
            ReadTimeoutError: If cumulative attempt time exceeds the total
                limit.
        """
        body = BytesIO()
        self.consume_into(chunks, body.write, total_bytes=total_bytes)
        return body.getvalue()


@dataclasses.dataclass(frozen=True)
class _DriverTiming:
    """Provide explicit deterministic attempt timing sources."""

    clock: Callable[[], float] = time.monotonic
    jitter_seed: Callable[[], float] = time.perf_counter
    sleep: Callable[[float], None] = time.sleep
    wall_clock: Callable[[], float] = time.time


class LiveResponse(Protocol):
    """Minimal response lifecycle required by the common executor.

    Attributes:
        headers: Read-only normalized response headers.
        status: HTTP response status code.
    """

    def close(self) -> None:
        """Release this attempt response exactly once."""
        ...

    @property
    def headers(self) -> Mapping[str, str]:
        """Return normalized response headers.

        Returns:
            Read-only response headers.
        """
        ...

    @property
    def status(self) -> int:
        """Return the HTTP response status."""
        ...


@dataclasses.dataclass(frozen=True)
class ExecutionOptions:
    """Control executor behavior that is not part of retry classification.

    Attributes:
        logical_method_name: Optional observer-facing operation name.
        redirect_factory: Optional request factory for redirect responses.
        streaming: Whether the response body is consumed as a stream.
    """

    logical_method_name: str | None = None
    redirect_factory: 'Callable[[LiveResponse], TransportRequest] | None' = None
    streaming: bool = False


class _AttemptTransport(Protocol):
    """Transport send boundary accepted by the common executor."""

    def send(self, request: TransportRequest) -> LiveResponse:
        """Send one request and return a live response.

        Args:
            request: Fresh request for this transport attempt.

        Returns:
            A live response owned by the executor.
        """
        ...

    def send_streaming(self, request: TransportRequest) -> LiveResponse:
        """Send one request and return a streaming live response.

        Args:
            request: Fresh request for this transport attempt.

        Returns:
            A live streaming response owned by the executor.
        """
        ...


@runtime_checkable
class _BufferedResponse(LiveResponse, Protocol):
    """Buffered response fields consumed only by the RPC adapter."""

    @property
    def content(self) -> bytes:
        """Return the buffered response body."""
        ...

    @property
    def reason(self) -> str:
        """Return the HTTP reason phrase."""
        ...


class _ObservedRequestBody:
    """Own a streamed source before its progress iterator starts."""

    def __init__(
        self,
        chunks: Iterator[bytes],
        total_bytes: int | None,
        state: AttemptState[AttemptControl],
    ) -> None:
        """Keep the source and its lazy progress iterator together."""
        self._chunks = chunks
        self._observed = _observe_request_body(chunks, total_bytes, state)
        self._closed = False

    def __iter__(self) -> Iterator[bytes]:
        """Return this single-use request body iterator."""
        return self

    def __next__(self) -> bytes:
        """Read one chunk and close the source after completion or failure."""
        if self._closed:
            raise StopIteration
        try:
            return next(self._observed)
        except StopIteration:
            self.close()
            raise
        except BaseException:
            # The iteration failure is primary; source cleanup is secondary.
            with suppress(BaseException):
                self.close()
            raise

    def close(self) -> None:
        """Close the source once even if its cleanup raises."""
        if self._closed:
            return
        self._closed = True
        try:
            self._observed.close()
        finally:
            close = _transport.body_close(self._chunks)
            if close is not None:
                close()


@dataclasses.dataclass(frozen=True)
class _ResponseCleanupBoundary:
    """Apply response-specific cleanup precedence to one close operation."""

    has_primary: bool

    def __enter__(self) -> None:
        """Enter the response cleanup boundary."""

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> bool:
        """Suppress secondary cleanup or redact cleanup-only failure."""
        if exception_type is None:
            return False
        if self.has_primary:
            return True
        raise SendError('response cleanup failed') from None


@dataclasses.dataclass(frozen=True)
class _TerminalResponseBoundary:
    """Publish one terminal response event after exceptional cleanup."""

    publish: Callable[[], None]

    def __enter__(self) -> None:
        """Enter the exceptional response boundary."""

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> Literal[False]:
        """Publish after cleanup without suppressing the original exception."""
        if exception_type is not None:
            self.publish()
        return False


class Driver:
    """Own synchronous transport attempts and response consumption."""

    def __init__(
        self,
        transport: _AttemptTransport,
        timing: _DriverTiming | None = None,
    ) -> None:
        """Initialize a driver around one transport and timing source.

        Args:
            transport: Transport used for every request attempt.
            timing: Optional deterministic timing sources.
        """
        sources = timing or _DriverTiming()
        self._clock = sources.clock
        self._outcomes = AttemptOutcomes(clock=sources.clock, jitter_seed=sources.jitter_seed)
        self._sleep = sources.sleep
        self._transport = transport
        self._wall_clock = sources.wall_clock

    def _attempt_state(
        self,
        operation: _protocol.OperationDescriptor,
        context: RequestContext,
        route: EventRoute,
        attempt: int,
    ) -> AttemptState[AttemptControl]:
        """Build immutable timing and observer state for one fresh attempt."""
        started = self._clock()
        deadline = None if context.timeout.total is None else started + context.timeout.total
        control = AttemptControl(
            _attempt=attempt,
            _clock=self._clock,
            _deadline=deadline,
            _observer=context.on_progress,
            _operation=operation.category,
            _route=route,
        )
        return AttemptState(
            context=context,
            control=control,
            number=attempt,
            operation=operation,
            route=route,
            started=started,
        )

    @staticmethod
    def _buffered_response(response: LiveResponse) -> _BufferedResponse:
        """Narrow an RPC response to its buffered transport contract."""
        if not isinstance(response, _BufferedResponse):
            raise TypeError('RPC transport must return a buffered response')
        return response

    def _consume_response(
        self,
        response: LiveResponse,
        state: AttemptState[AttemptControl],
        consumer: Callable[[LiveResponse, AttemptControl], _T],
    ) -> AttemptRetry | AttemptSuccess[_T]:
        """Classify and consume a response with exactly-once cleanup."""
        attempt_result: AttemptRetry | AttemptSuccess[_T] | None = None
        correlation_id: str | None = None
        exception_capture = ExceptionCapture()
        planned_outcome: Outcome = 'error'
        response_status: int | None = None
        try:
            with exception_capture:
                response_status = response.status
                response_headers = response.headers
                correlation_id = response_correlation_id(response_headers)
                state.control._check_deadline()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
                failure = _protocol.classify_http_failure(
                    response_status,
                    response_headers,
                    self._wall_clock(),
                )
                retry = (
                    None
                    if failure is None
                    else self._outcomes.retry(state, failure, response_status)
                )
                if retry is None:
                    attempt_result = AttemptSuccess(consumer(response, state.control))
                    planned_outcome = 'success'
                else:
                    attempt_result = retry
                    planned_outcome = 'retry'
        except TransportError as exc:
            with exception_capture:
                retry = self._outcomes.retry(
                    state,
                    _protocol.classify_transport_failure(exc, response_received=True),
                    response_status,
                )
                if retry is None:
                    raise
                attempt_result = retry
                planned_outcome = 'retry'
        finally:
            _close_response(
                response,
                has_primary=(
                    attempt_result is not None or exception_capture.exception is not None
                ),
            )
            if exception_capture.exception is None:
                self._outcomes.emit_response(
                    state,
                    status=response_status,
                    outcome=planned_outcome,
                    correlation_id=correlation_id,
                )
        if attempt_result is None:
            raise AssertionError('response consumption ended without an outcome')
        return attempt_result

    def _emit_warnings(self, context: RequestContext) -> None:
        """Publish fixed-message compatibility warnings."""
        for event in compatibility_warning_events(context.timeout):
            notify_observer(context.on_compatibility_warning, event)

    def _execute_rpc(
        self,
        method_name: str,
        params: object,
        context: RequestContext,
        include_auth: bool,
        options: ExecutionOptions,
    ) -> object:
        """Supply fresh RPC builders and a decoder to the common executor."""
        api_url = context.api_url
        if api_url is None:
            raise ValueError('server parameters must be configured before RPC I/O')
        operation = _protocol.classify_rpc_operation(
            method_name,
            options.logical_method_name,
            params,
            context.has_one_use_auth_token,
        )

        def request_factory(_attempt: int) -> TransportRequest:
            return build_rpc_request(method_name, params, context, include_auth=include_auth)

        def consume(response: LiveResponse, control: AttemptControl) -> object:
            buffered = self._buffered_response(response)
            _protocol.check_http_status(
                buffered.status,
                buffered.reason,
                context.canonical_origin or '',
                buffered.headers,
                buffered.content,
            )
            body = control._consume_buffered(  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
                buffered.content,
                total_bytes=len(buffered.content),
            )
            decoded = _protocol.decode_response(buffered.headers, body)
            _protocol.raise_fault(decoded)
            return _protocol.extract_results(decoded)

        return self.execute(
            operation,
            context,
            request_factory,
            consume,
            options=options,
        )

    def _prepare_request(
        self,
        request: TransportRequest,
        state: AttemptState[AttemptControl],
    ) -> TransportRequest:
        """Attach progress control and publish one redacted request event."""
        prepared = _request_with_progress(request, state)
        notify_observer(
            state.context.on_request,
            RequestEvent(
                operation=state.operation.category,
                route=state.route,
                http_method=prepared.method,
                attempt=state.number,
            ),
        )
        return prepared

    def _redirect_request(
        self,
        response: LiveResponse,
        state: AttemptState[AttemptControl],
        redirect_factory: Callable[[LiveResponse], TransportRequest],
    ) -> TransportRequest:
        """Close one redirect response and return its credential-scoped hop."""
        correlation_id: str | None = None
        exception_capture = ExceptionCapture()
        request: TransportRequest | None = None
        response_status: int | None = None
        try:
            with exception_capture:
                request = redirect_factory(response)
                response_status = response.status
                correlation_id = response_correlation_id(response.headers)
        finally:
            _close_response(
                response,
                has_primary=request is not None or exception_capture.exception is not None,
            )
            if exception_capture.exception is None:
                self._outcomes.emit_response(
                    state,
                    status=response_status,
                    outcome='success',
                    correlation_id=correlation_id,
                )
        return cast('TransportRequest', request)

    def _retry(
        self,
        observer: ObserverCallback | None,
        event: RetryEvent,
    ) -> None:
        """Publish a retry event and sleep after attempt cleanup."""
        notify_observer(observer, event)
        self._sleep(event.delay)

    def _terminal_boundary(
        self,
        response: LiveResponse,
        state: AttemptState[AttemptControl],
    ) -> '_TerminalResponseBoundary':
        """Build the exceptional terminal response event boundary."""
        return _TerminalResponseBoundary(
            partial(
                self._outcomes.emit_response,
                state,
                status=response.status,
                outcome='error',
                correlation_id=response_correlation_id(response.headers),
            )
        )

    def _send_request(
        self,
        request: TransportRequest,
        state: AttemptState[AttemptControl],
        *,
        redirected: bool,
        streaming: bool,
    ) -> tuple[LiveResponse, '_TerminalResponseBoundary'] | AttemptRetry:
        """Send once and keep a received response through body cleanup."""
        response: LiveResponse | None = None
        terminal: _TerminalResponseBoundary | None = None
        exception_capture = ExceptionCapture()
        try:
            try:
                with exception_capture:
                    if redirected:
                        request = _request_with_remaining_timeout(request, state.control)
                    if streaming:
                        response = self._transport.send_streaming(request)
                    else:
                        response = self._transport.send(request)
                    terminal = self._terminal_boundary(response, state)
            finally:
                if exception_capture.exception is None:
                    _close_request_body(request)
                else:
                    with suppress(BaseException):
                        _close_request_body(request)
        except BaseException as exc:
            if response is not None:
                _close_response(response, has_primary=True)
                if terminal is not None:
                    with terminal:
                        raise
                raise
            if not isinstance(exc, TransportError):
                raise
            retry = self._outcomes.send_failure(exc, state)
            if retry is None:
                raise
            return retry
        if response is None or terminal is None:
            raise AssertionError('transport returned without a response')
        return response, terminal

    def call(
        self,
        method_name: str,
        params: object,
        context: RequestContext,
        include_auth: bool = True,
        *,
        _logical_method_name: str | None = None,
    ) -> object:
        """Adapt one RPC to the common attempt executor.

        Args:
            method_name: Normalized RPC method name.
            params: Normalized method parameters.
            context: Immutable operation configuration.
            include_auth: Whether to include the captured authentication
                payload.
            _logical_method_name: Optional logical operation name when it
                differs from the wire method.

        Returns:
            Decoded RPC results.

        Raises:
            TypeError: If the RPC transport response lacks the buffered
                response contract (`reason` and `content`).
        """
        normalized_params = copy.deepcopy(params)
        options = ExecutionOptions(logical_method_name=_logical_method_name)
        return self._execute_rpc(
            method_name,
            normalized_params,
            context,
            include_auth=include_auth and method_name != 'info',
            options=options,
        )

    def call_pages(
        self,
        build_page_params: Callable[[int], object],
        first_page: int,
        method_name: str,
        context: RequestContext,
    ) -> Generator[object, None, None]:
        """Yield page results in order from one immutable context.

        The sequence is unbounded, so the consumer decides when to stop and
        must call `close()` on early exit. When prefetch is active, cleanup
        cancels and drains pending prefetch work.

        Args:
            build_page_params: Build independent parameters for a page number.
            first_page: First page number to request.
            method_name: Normalized RPC method name.
            context: Immutable operation configuration.

        Yields:
            Decoded RPC results in page order.
        """
        if context.page_prefetch <= 0:
            page = first_page
            while True:
                yield self.call(method_name, build_page_params(page), context)
                page += 1
            return

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=context.page_prefetch,
            thread_name_prefix='shotgun-api3-pages',
        )
        pending: dict[int, concurrent.futures.Future[object]] = {}

        def submit(page_number: int) -> None:
            if page_number not in pending:
                pending[page_number] = executor.submit(
                    self.call,
                    method_name,
                    build_page_params(page_number),
                    context,
                )

        page = first_page
        try:
            submit(page)
            while True:
                yield pending.pop(page).result()
                page += 1
                for page_number in range(page, page + context.page_prefetch):
                    submit(page_number)
        finally:
            for future in pending.values():
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)

    def execute(
        self,
        operation: _protocol.OperationDescriptor,
        context: RequestContext,
        request_factory: Callable[[int], TransportRequest],
        consumer: Callable[[LiveResponse, AttemptControl], _T],
        options: ExecutionOptions | None = None,
    ) -> _T:
        """Execute one logical operation through fresh transport attempts.

        Args:
            operation: Immutable replay and route classification.
            context: Immutable operation configuration.
            request_factory: Builder called once for every fresh attempt.
            consumer: Buffered or streaming response consumer using the
                supplied deadline/progress control.
            options: Explicit redirect, streaming, and sanitized
                compatibility-warning controls.

        Returns:
            The successful consumer result.

        Raises:
            TransportError: If a terminal or exhausted transport failure
                occurs.
            Exception: If status validation or response consumption fails.
        """
        execution_options = options or ExecutionOptions()
        self._emit_warnings(context)
        route = event_route(operation)
        for attempt in range(1, context.max_rpc_attempts + 1):
            state = self._attempt_state(operation, context, route, attempt)
            request = request_factory(attempt)
            redirected = False
            while True:
                request = self._prepare_request(request, state)
                sent = self._send_request(
                    request,
                    state,
                    redirected=redirected,
                    streaming=execution_options.streaming,
                )
                if isinstance(sent, AttemptRetry):
                    self._retry(context.on_retry, sent.event)
                    break
                response, terminal = sent
                redirect_factory = execution_options.redirect_factory
                if (
                    redirect_factory is not None
                    and not redirected
                    and response.status == _HTTP_REDIRECT
                ):
                    with terminal:
                        request = self._redirect_request(response, state, redirect_factory)
                    redirected = True
                    continue
                with terminal:
                    attempt_result = self._consume_response(response, state, consumer)
                if isinstance(attempt_result, AttemptRetry):
                    self._retry(context.on_retry, attempt_result.event)
                    break
                return attempt_result.result
        raise AssertionError('attempt loop exhausted without a terminal outcome')

    def poll(
        self,
        predicate: Callable[[], bool],
        attempts: int,
        interval: float,
    ) -> bool:
        """Poll a predicate with driver-owned sleeps between reads.

        Args:
            predicate: Read operation returning whether the source is ready.
            attempts: Maximum number of predicate calls.
            interval: Seconds to sleep between calls.

        Returns:
            Whether the predicate became true within the attempt bound.

        Raises:
            ValueError: If `attempts` is not positive or `interval` is
                negative.
        """
        if attempts <= 0:
            raise ValueError('poll attempts must be positive')
        if interval < 0:
            raise ValueError('poll interval must be nonnegative')
        for attempt in range(attempts):
            if predicate():
                return True
            if attempt + 1 < attempts:
                self._sleep(interval)
        return False


def _close_request_body(request: TransportRequest) -> None:
    """Close a streamed request body after its transport attempt."""
    close = _transport.body_close(request.body)
    if close is not None:
        close()


def _close_response(response: LiveResponse, has_primary: bool) -> None:
    """Close one response without replacing its selected primary outcome."""
    with _ResponseCleanupBoundary(has_primary=has_primary):
        response.close()


def _observe_request_body(
    chunks: Iterator[bytes],
    total_bytes: int | None,
    state: AttemptState[AttemptControl],
) -> Generator[bytes, None, None]:
    """Yield request chunks with attempt deadline and progress reporting."""
    transferred = 0
    for chunk in chunks:
        yield chunk
        transferred += len(chunk)
        if chunk:
            state.control._check_deadline()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            notify_observer(
                state.context.on_progress,
                ProgressEvent(
                    operation=state.operation.category,
                    route=state.route,
                    attempt=state.number,
                    bytes_transferred=transferred,
                    total_bytes=total_bytes,
                ),
            )


def _request_with_progress(
    request: TransportRequest,
    state: AttemptState[AttemptControl],
) -> TransportRequest:
    """Wrap only streamed request bodies in attempt progress control."""
    body = request.body
    if body is None or isinstance(body, bytes):
        return request
    return dataclasses.replace(
        request,
        body=_ObservedRequestBody(
            body,
            _transport.content_length(request.headers),
            state,
        ),
    )


def _request_with_remaining_timeout(
    request: TransportRequest,
    control: AttemptControl,
) -> TransportRequest:
    """Restrict a later hop to its remaining cumulative attempt timeout."""
    return _transport.narrow_total_timeout(
        request,
        control._remaining_timeout(),  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    )
