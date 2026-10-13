"""Async attempt, consumption, retry, and observer orchestration."""

import asyncio
import copy
import dataclasses
import inspect
import sys
import threading
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import suppress
from io import BytesIO
from typing import Protocol, TypeVar, cast

from . import _protocol, _transport
from ._async_transport import AsyncTransport
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
    SuppressedExceptionCapture,
    build_rpc_request,
)
from ._transport import (
    ReadTimeoutError,
    SendError,
    TransportError,
    TransportRequest,
)

_T = TypeVar('_T')

_DETACHED_CLEANUP_TASKS: set[asyncio.Task[None]] = set()
_HTTP_REDIRECT = 302


@dataclasses.dataclass(frozen=True)
class AsyncAttemptControl:
    """Consume async attempt bytes under deadline and progress control."""

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

    async def _consume_buffered(self, body: bytes, total_bytes: int | None = None) -> bytes:
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

    async def consume_into(
        self,
        chunks: AsyncIterator[bytes],
        sink: Callable[[bytes], object | Awaitable[object]],
        total_bytes: int | None = None,
    ) -> None:
        """Write async chunks under deadline and progress control.

        Args:
            chunks: Fresh response-body chunks for this attempt.
            sink: Synchronous or awaitable attempt-local byte sink.
            total_bytes: Known complete body size, when available.

        Raises:
            ReadTimeoutError: If cumulative attempt time exceeds the total
                limit.
        """
        transferred = 0
        async for chunk in chunks:
            sink_result = sink(chunk)
            if inspect.isawaitable(sink_result):
                await sink_result
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

    async def consume(
        self,
        chunks: AsyncIterator[bytes],
        total_bytes: int | None = None,
    ) -> bytes:
        """Collect async chunks while enforcing the attempt deadline.

        Args:
            chunks: Fresh response-body chunks for this attempt.
            total_bytes: Known complete body size, when available.

        Returns:
            Concatenated response bytes.

        Raises:
            ReadTimeoutError: If cumulative attempt time exceeds the total
                limit.
        """
        body = BytesIO()
        await self.consume_into(chunks, body.write, total_bytes=total_bytes)
        return body.getvalue()


@dataclasses.dataclass(frozen=True)
class _AsyncDriverTiming:
    """Provide explicit deterministic async attempt timing sources."""

    clock: Callable[[], float] = time.monotonic
    jitter_seed: Callable[[], float] = time.perf_counter
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    wall_clock: Callable[[], float] = time.time


class AsyncLiveResponse(Protocol):
    """Minimal response lifecycle required by the async executor.

    Attributes:
        headers: Read-only normalized response headers.
        status: HTTP response status code.
    """

    def close(self) -> Awaitable[None] | None:
        """Release this attempt response exactly once.

        Returns:
            Awaitable cleanup for async responses, otherwise `None`.
        """
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
        """Return the HTTP response status.

        Returns:
            HTTP status code.
        """
        ...


@dataclasses.dataclass(frozen=True)
class AsyncExecutionOptions:
    """Control async executor transport choreography.

    Attributes:
        logical_method_name: Optional observer-facing operation name.
        redirect_factory: Optional request factory for redirect responses.
        streaming: Whether the response body is consumed as a stream.
    """

    logical_method_name: str | None = None
    redirect_factory: Callable[['AsyncLiveResponse'], TransportRequest] | None = None
    streaming: bool = False


class AsyncDriver:
    """Own the only async transport attempt and retry loop."""

    def __init__(
        self,
        transport: AsyncTransport,
        timing: _AsyncDriverTiming | None = None,
    ) -> None:
        """Initialize an async driver around one transport and timing source.

        Args:
            transport: Async transport used for every request attempt.
            timing: Optional deterministic timing sources.
        """
        sources = timing or _AsyncDriverTiming()
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
    ) -> AttemptState[AsyncAttemptControl]:
        """Build immutable timing and observer state for one fresh attempt."""
        started = self._clock()
        deadline = None if context.timeout.total is None else started + context.timeout.total
        control = AsyncAttemptControl(
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

    async def _close_received_response_after_send_failure(
        self,
        response: AsyncLiveResponse,
        state: AttemptState[AsyncAttemptControl],
        primary_exception: BaseException,
        response_status: int | None,
        correlation_id: str | None,
    ) -> None:
        """Close a received response while preserving the active exception."""
        _close_error, cancellation = await _close_response_capturing(
            response,
            primary_exception,
        )
        self._outcomes.emit_response(
            state,
            status=response_status,
            outcome='error',
            correlation_id=correlation_id,
        )
        if cancellation is not None:
            raise cancellation

    async def _consume_response(
        self,
        response: AsyncLiveResponse,
        state: AttemptState[AsyncAttemptControl],
        consumer: Callable[[AsyncLiveResponse, AsyncAttemptControl], Awaitable[_T]],
    ) -> AttemptRetry | AttemptSuccess[_T]:
        """Classify and consume a response with exact-once cleanup."""
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
                    attempt_result = AttemptSuccess(await consumer(response, state.control))
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
            close_error, cancellation = await _close_response_capturing(
                response,
                exception_capture.exception,
            )
            self._outcomes.emit_response(
                state,
                status=response_status,
                outcome=(
                    'error'
                    if exception_capture.exception is not None or cancellation is not None
                    else planned_outcome
                ),
                correlation_id=correlation_id,
            )
            if cancellation is not None:
                raise cancellation
            if (
                close_error is not None
                and attempt_result is None
                and exception_capture.exception is None
            ):
                raise SendError('response cleanup failed') from None
        if attempt_result is None:
            raise AssertionError('response consumption ended without an outcome')
        return attempt_result

    def _prepare_request(
        self,
        request: TransportRequest,
        state: AttemptState[AsyncAttemptControl],
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

    async def _redirect_request(
        self,
        response: AsyncLiveResponse,
        state: AttemptState[AsyncAttemptControl],
        redirect_factory: Callable[[AsyncLiveResponse], TransportRequest],
    ) -> TransportRequest:
        """Close one redirect response and return its next request."""
        correlation_id: str | None = None
        exception_capture = ExceptionCapture()
        request: TransportRequest | None = None
        response_status: int | None = None
        try:
            with exception_capture:
                response_status = response.status
                correlation_id = response_correlation_id(response.headers)
                request = redirect_factory(response)
        finally:
            close_error, cancellation = await _close_response_capturing(
                response,
                exception_capture.exception,
            )
            self._outcomes.emit_response(
                state,
                status=response_status,
                outcome=(
                    'error'
                    if exception_capture.exception is not None or cancellation is not None
                    else 'success'
                ),
                correlation_id=correlation_id,
            )
            if cancellation is not None:
                raise cancellation
            if close_error is not None and request is None and exception_capture.exception is None:
                raise SendError('response cleanup failed') from None
        return cast('TransportRequest', request)

    async def _retry(self, observer: ObserverCallback | None, event: RetryEvent) -> None:
        """Publish a retry event and sleep after attempt cleanup."""
        notify_observer(observer, event)
        await self._sleep(event.delay)

    async def _send_request(
        self,
        request: TransportRequest,
        state: AttemptState[AsyncAttemptControl],
        *,
        redirected: bool,
        streaming: bool,
    ) -> AsyncLiveResponse | AttemptRetry:
        """Send once and close the request body under response ownership."""
        correlation_id: str | None = None
        response: AsyncLiveResponse | None = None
        response_status: int | None = None
        try:
            try:
                if redirected:
                    request = _request_with_remaining_timeout(request, state.control)
                if streaming:
                    response = await self._transport.send_streaming(request)
                else:
                    response = await self._transport.send(request)
                response_status = response.status
                correlation_id = response_correlation_id(response.headers)
            finally:
                await _close_request_body_after_send(request)
        except BaseException as exc:
            if response is not None:
                await self._close_received_response_after_send_failure(
                    response,
                    state,
                    exc,
                    response_status,
                    correlation_id,
                )
                raise
            if not isinstance(exc, TransportError):
                raise
            retry = self._outcomes.send_failure(exc, state)
            if retry is None:
                raise
            return retry
        if response is None:
            raise AssertionError('transport returned without a response')
        return response

    async def execute(
        self,
        operation: _protocol.OperationDescriptor,
        context: RequestContext,
        request_factory: Callable[[int], TransportRequest],
        consumer: Callable[[AsyncLiveResponse, AsyncAttemptControl], Awaitable[_T]],
        options: AsyncExecutionOptions | None = None,
    ) -> _T:
        """Execute one logical operation through fresh transport attempts.

        Args:
            operation: Immutable replay and route classification.
            context: Immutable operation configuration.
            request_factory: Synchronous fresh-attempt builder.
            consumer: Async buffered or streaming response consumer.
            options: Redirect, streaming, and observer controls.

        Returns:
            The successful consumer result.

        Raises:
            TransportError: If a terminal or exhausted transport failure
                occurs.
            Exception: If request construction, status validation, response
                consumption, or cleanup fails.
        """
        execution_options = options or AsyncExecutionOptions()
        for event in compatibility_warning_events(context.timeout):
            notify_observer(context.on_compatibility_warning, event)
        route = event_route(operation)
        self._transport.checkout()
        try:
            for attempt in range(1, context.max_rpc_attempts + 1):
                state = self._attempt_state(operation, context, route, attempt)
                request = request_factory(attempt)
                redirected = False
                while True:
                    request = self._prepare_request(request, state)
                    sent = await self._send_request(
                        request,
                        state,
                        redirected=redirected,
                        streaming=execution_options.streaming,
                    )
                    if isinstance(sent, AttemptRetry):
                        await self._retry(context.on_retry, sent.event)
                        break
                    response = sent

                    redirect_factory = execution_options.redirect_factory
                    if (
                        redirect_factory is not None
                        and not redirected
                        and response.status == _HTTP_REDIRECT
                    ):
                        request = await self._redirect_request(
                            response,
                            state,
                            redirect_factory,
                        )
                        redirected = True
                        continue

                    attempt_result = await self._consume_response(response, state, consumer)
                    if isinstance(attempt_result, AttemptRetry):
                        await self._retry(context.on_retry, attempt_result.event)
                        break
                    return attempt_result.result
        finally:
            self._transport.checkin()
        raise AssertionError('attempt loop exhausted without a terminal outcome')

    async def _execute_rpc(
        self,
        method_name: str,
        params: object,
        context: RequestContext,
        include_auth: bool,
        options: AsyncExecutionOptions,
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

        async def consume(
            response: AsyncLiveResponse,
            control: AsyncAttemptControl,
        ) -> object:
            buffered = cast('_AsyncBufferedResponse', response)
            _protocol.check_http_status(
                buffered.status,
                buffered.reason,
                context.canonical_origin or '',
                buffered.headers,
                buffered.content,
            )
            body = await control._consume_buffered(  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
                buffered.content,
                total_bytes=len(buffered.content),
            )
            decoded = _protocol.decode_response(buffered.headers, body)
            _protocol.raise_fault(decoded)
            return _protocol.extract_results(decoded)

        return await self.execute(
            operation,
            context,
            request_factory,
            consume,
            options=options,
        )

    async def call(
        self,
        method_name: str,
        params: object,
        context: RequestContext,
        include_auth: bool = True,
        *,
        _logical_method_name: str | None = None,
    ) -> object:
        """Adapt one RPC to the common async attempt executor.

        Args:
            method_name: Normalized RPC wire method name.
            params: Normalized method parameters.
            context: Immutable operation configuration.
            include_auth: Whether to include captured authentication.
            _logical_method_name: Optional logical operation name when it
                differs from the wire method.

        Returns:
            Decoded RPC results.

        Raises:
            TransportError: If a terminal or exhausted transport failure
                occurs.
            Exception: If request construction, status validation, response
                consumption, or cleanup fails.
        """
        normalized_params = copy.deepcopy(params)
        options = AsyncExecutionOptions(logical_method_name=_logical_method_name)
        return await self._execute_rpc(
            method_name,
            normalized_params,
            context,
            include_auth=False if method_name == 'info' else include_auth,
            options=options,
        )

    async def call_pages(
        self,
        build_page_params: Callable[[int], object],
        first_page: int,
        method_name: str,
        context: RequestContext,
    ) -> AsyncGenerator[object, None]:
        """Yield page results in order from one immutable context.

        The sequence is unbounded, so the consumer decides when to stop and
        must call `aclose()` on early exit. When prefetch is active, cleanup
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
                yield await self.call(method_name, build_page_params(page), context)
                page += 1

        pending: dict[int, asyncio.Task[object]] = {}

        def submit(page_number: int) -> None:
            if page_number not in pending:
                params = build_page_params(page_number)
                task = asyncio.create_task(self.call(method_name, params, context))
                pending[page_number] = task
                self._transport.reserve(task)

        page = first_page
        try:
            submit(page)
            while True:
                yield await pending.pop(page)
                page += 1
                for page_number in range(page, page + context.page_prefetch):
                    submit(page_number)
        finally:
            tasks = tuple(pending.values())
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def poll(
        self,
        predicate: Callable[[], Awaitable[bool]],
        attempts: int,
        interval: float,
    ) -> bool:
        """Poll an async predicate with driver-owned sleeps between reads.

        Args:
            predicate: Awaitable read operation returning readiness.
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
            if await predicate():
                return True
            if attempt + 1 < attempts:
                await self._sleep(interval)
        return False


class _AsyncBufferedResponse(AsyncLiveResponse, Protocol):
    """Buffered response fields consumed only by the RPC adapter."""

    @property
    def content(self) -> bytes:
        """Return the buffered response body.

        Returns:
            Complete response body bytes.
        """
        ...

    @property
    def reason(self) -> str:
        """Return the HTTP reason phrase.

        Returns:
            HTTP reason phrase.
        """
        ...


class _ObservedRequestBody(Iterator[bytes]):
    """Publish progress while owning exact-once request-body cleanup."""

    def __init__(
        self,
        chunks: Iterator[bytes],
        total_bytes: int | None,
        state: AttemptState[AsyncAttemptControl],
    ) -> None:
        """Initialize request-body observation for one attempt."""
        self._condition = threading.Condition()
        self._chunks: Iterator[bytes] | None = chunks
        self._active_pulls = 0
        self._closed = False
        self._closing = False
        self._state = state
        self._total_bytes = total_bytes
        self._transferred = 0

    def __iter__(self) -> '_ObservedRequestBody':
        """Return this request-body iterator."""
        return self

    def __next__(self) -> bytes:
        """Return and observe the next request-body chunk."""
        chunk = self.pull_for_async()
        if chunk is None:
            raise StopIteration
        self.observe_async_chunk(chunk)
        return chunk

    def close(self) -> None:
        """Close the source iterator exactly once."""
        with self._condition:
            if self._closed:
                return
            if self._closing:
                while not self._closed:
                    self._condition.wait()
                return
            self._closing = True
            while self._active_pulls:
                self._condition.wait()
            chunks = self._chunks
            self._chunks = None
        try:
            if chunks is not None:
                close = _transport.body_close(chunks)
                if close is not None:
                    close()
        finally:
            with self._condition:
                self._closed = True
                self._condition.notify_all()

    def observe_async_chunk(self, chunk: bytes) -> None:
        """Account for and report one asynchronously pulled chunk.

        Args:
            chunk: Chunk pulled from the source iterator.

        Raises:
            ReadTimeoutError: If the attempt deadline passed before the chunk
                could be returned.
        """
        self._transferred += len(chunk)
        if not chunk:
            return
        self._state.control._check_deadline()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        notify_observer(
            self._state.context.on_progress,
            ProgressEvent(
                operation=self._state.operation.category,
                route=self._state.route,
                attempt=self._state.number,
                bytes_transferred=self._transferred,
                total_bytes=self._total_bytes,
            ),
        )

    def pull_for_async(self) -> bytes | None:
        """Pull one source chunk without observing progress.

        Returns:
            The next chunk, or `None` when the source is exhausted or the body
            is closing or closed.
        """
        with self._condition:
            chunks = self._chunks
            if chunks is None or self._closing:
                return None
            self._active_pulls += 1
        try:
            return next(chunks, None)
        finally:
            with self._condition:
                self._active_pulls -= 1
                self._condition.notify_all()


async def _attempt_response_close(response: AsyncLiveResponse) -> None:
    """Run response cleanup to completion."""
    result = response.close()
    if inspect.isawaitable(result):
        await result


async def _close_response_capturing(
    response: AsyncLiveResponse,
    operation_exception: BaseException | None,
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Join response cleanup while retaining the first caller cancellation."""
    cancellation: asyncio.CancelledError | None = None
    current = asyncio.current_task()
    if (
        isinstance(operation_exception, asyncio.CancelledError)
        and current is not None
        and current.cancelling() > 0
    ):
        cancellation = operation_exception
        current.uncancel()
    cleanup = asyncio.create_task(_attempt_response_close(response))
    cleanup_capture = SuppressedExceptionCapture()
    while not cleanup.done():
        with cleanup_capture:
            await asyncio.shield(cleanup)
        error = cleanup_capture.exception
        if (
            isinstance(error, asyncio.CancelledError)
            and current is not None
            and current.cancelling() > 0
        ):
            if cancellation is None:
                cancellation = error
            current.uncancel()
            continue
        if error is not None:
            break
    result_capture = SuppressedExceptionCapture()
    with result_capture:
        cleanup.result()
    return result_capture.exception, cancellation


def _finish_detached_cleanup(task: asyncio.Task[None]) -> None:
    """Consume and release one completed detached cleanup task."""
    with suppress(BaseException):
        task.exception()
    _DETACHED_CLEANUP_TASKS.discard(task)


def _request_with_progress(
    request: TransportRequest,
    state: AttemptState[AsyncAttemptControl],
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
    control: AsyncAttemptControl,
) -> TransportRequest:
    """Restrict a redirect hop to its remaining cumulative attempt timeout."""
    return _transport.narrow_total_timeout(
        request,
        control._remaining_timeout(),  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    )


def _retain_detached_cleanup(task: asyncio.Task[None]) -> None:
    """Retain a detached cleanup task until its completion is observed."""
    _DETACHED_CLEANUP_TASKS.add(task)
    task.add_done_callback(_finish_detached_cleanup)


async def _close_request_body_after_send(request: TransportRequest) -> None:
    """Clean up a request body without replacing a primary exception."""
    body = request.body
    if body is None or isinstance(body, bytes):
        return
    primary_exception = sys.exception()

    def run_close() -> None:
        close = _transport.body_close(body)
        if close is not None:
            close()

    cleanup = asyncio.create_task(asyncio.to_thread(run_close))
    if isinstance(primary_exception, asyncio.CancelledError):
        _retain_detached_cleanup(cleanup)
        return
    try:
        await asyncio.wait({cleanup})
    except asyncio.CancelledError:
        _retain_detached_cleanup(cleanup)
        raise
    cleanup_error = asyncio.CancelledError() if cleanup.cancelled() else cleanup.exception()
    if primary_exception is None and cleanup_error is not None:
        raise cleanup_error
