"""Share pure execution, attempt, and lease logic between the sync and async twins."""

import dataclasses
import uuid
from collections.abc import Callable, Mapping
from typing import Generic, Literal, TypeVar

from . import _protocol
from ._config import (
    EventRoute,
    Outcome,
    RequestContext,
    ResponseEvent,
    RetryEvent,
    notify_observer,
    thaw_mapping,
)
from ._transport import TransportError, TransportRequest

_ControlT_co = TypeVar('_ControlT_co', covariant=True)
_T = TypeVar('_T')
_TransportT = TypeVar('_TransportT')


@dataclasses.dataclass(frozen=True)
class AttemptRetry:
    """Carry one post-cleanup retry event back to the executor."""

    event: RetryEvent


@dataclasses.dataclass(frozen=True)
class AttemptState(Generic[_ControlT_co]):
    """Bundle immutable metadata shared by one attempt's helper calls.

    Attributes:
        context: Configuration captured for the logical operation.
        control: Attempt-local deadline and progress control.
        number: One-based attempt number.
        operation: Logical operation and replay classification.
        route: Observer route for the attempt.
        started: Attempt-start time in seconds from the same monotonic clock
            used by `AttemptOutcomes` to calculate response latency.
    """

    context: RequestContext
    control: _ControlT_co
    number: int
    operation: _protocol.OperationDescriptor
    route: EventRoute
    started: float


@dataclasses.dataclass(frozen=True)
class AttemptOutcomes:
    """Choose retry outcomes and publish response events for one driver.

    Attributes:
        clock: Monotonic clock returning seconds on the same time base as
            `AttemptState.started`.
        jitter_seed: Source of per-attempt high-resolution numeric seeds.
            Values should differ between attempts; identical complete retry
            inputs intentionally reproduce the same jitter.
    """

    clock: Callable[[], float]
    jitter_seed: Callable[[], float]

    def emit_response(
        self,
        state: AttemptState[object],
        status: int | None,
        outcome: Outcome,
        correlation_id: str | None,
    ) -> None:
        """Publish one redacted response event outside driver locks."""
        latency = max(0.0, self.clock() - state.started)
        notify_observer(
            state.context.on_response,
            ResponseEvent(
                operation=state.operation.category,
                route=state.route,
                attempt=state.number,
                latency=latency,
                status=status,
                outcome=outcome,
                correlation_id=correlation_id,
            ),
        )

    def retry(
        self,
        state: AttemptState[object],
        failure: _protocol.Failure,
        status: int | None,
    ) -> AttemptRetry | None:
        """Return the retry for one failed attempt, or `None` when retrying stops."""
        delay = _protocol.decide(
            state.operation,
            state.number,
            failure,
            max_attempts=state.context.max_rpc_attempts,
            base_interval=state.context.rpc_attempt_interval / 1000.0,
            now=self.jitter_seed(),
        )
        if delay is None:
            return None
        return AttemptRetry(
            RetryEvent(
                operation=state.operation.category,
                route=state.route,
                attempt=state.number,
                delay=delay,
                status=status,
            )
        )

    def send_failure(
        self,
        error: TransportError,
        state: AttemptState[object],
    ) -> AttemptRetry | None:
        """Classify one pre-response transport failure and publish its event."""
        retry = self.retry(
            state,
            _protocol.classify_transport_failure(error, response_received=False),
            None,
        )
        self.emit_response(
            state,
            status=None,
            outcome='error' if retry is None else 'retry',
            correlation_id=None,
        )
        return retry


@dataclasses.dataclass(frozen=True)
class AttemptSuccess(Generic[_T]):
    """Carry one successful consumer result back to the executor."""

    result: _T


class ExceptionCapture:
    """Record an operation-local exception without suppressing propagation.

    Attributes:
        exception: Exception from the most recent context exit, or `None`
            before the first exit or after an exit without an exception.
    """

    def __init__(self) -> None:
        """Initialize an empty exception capture."""
        self.exception: BaseException | None = None

    def __enter__(self) -> None:
        """Enter the exception boundary."""

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: object,
    ) -> Literal[False]:
        """Record the current exception and preserve its propagation."""
        self.exception = exception
        return False


class SuppressedExceptionCapture:
    """Record and suppress an exception for explicit precedence selection.

    Attributes:
        exception: Captured exception, or `None` when no exception was
            captured. Reset to `None` on each context entry.
    """

    def __init__(self) -> None:
        """Initialize an empty exception capture."""
        self.exception: BaseException | None = None

    def __enter__(self) -> None:
        """Reset the exception at entry to this capture boundary."""
        self.exception = None

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: object,
    ) -> bool:
        """Record and suppress the current exception when present."""
        self.exception = exception
        return exception is not None


def build_rpc_request(
    method_name: str,
    params: object,
    context: RequestContext,
    *,
    include_auth: bool,
) -> TransportRequest:
    """Build fresh RPC bytes, headers, auth, and token for one attempt.

    Args:
        method_name: RPC method being invoked.
        params: Invocation-owned parameters, already detached at call
            boundaries.
        context: Captured configuration and credentials for the invocation.
        include_auth: Whether to include captured authentication fields.

    Returns:
        A fresh request without copying the invocation-owned parameters again.

    Raises:
        ValueError: If server parameters have not been configured.
    """
    api_url = context.api_url
    if api_url is None:
        raise ValueError('server parameters must be configured before RPC I/O')
    auth = thaw_mapping(context.auth) if include_auth else None
    payload = _protocol.build_payload(method_name, params, auth)
    headers = {
        'connection': 'keep-alive',
        'content-type': 'application/json; charset=utf-8',
        'user-agent': context.user_agent,
    }
    if context.localized:
        headers['locale'] = 'auto'
    return TransportRequest(
        method='POST',
        url=api_url,
        headers=headers,
        body=_protocol.encode_payload(payload),
        timeout=context.timeout,
        token=uuid.uuid4().hex,
    )


def release_lease(
    lease_counts: dict[int, int],
    retired: dict[int, _TransportT],
    transport: _TransportT,
) -> _TransportT | None:
    """Drop one lease and return a retired transport that this release left idle.

    Call with the owning client's transport lock held.
    """
    key = id(transport)
    remaining = lease_counts[key] - 1
    if remaining:
        lease_counts[key] = remaining
        return None
    del lease_counts[key]
    return retired.pop(key, None)


def retire_transport(
    lease_counts: Mapping[int, int],
    retired: dict[int, _TransportT],
    transport: _TransportT,
) -> _TransportT | None:
    """Retire a transport and return it when no lease delays closing it.

    Call with the owning client's transport lock held.
    """
    key = id(transport)
    if lease_counts.get(key, 0):
        retired[key] = transport
        return None
    return transport
