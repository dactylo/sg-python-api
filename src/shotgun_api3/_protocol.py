"""Sans-I/O helpers for the JSON-RPC protocol."""

import dataclasses
import datetime as dt
import hashlib
import json
import math
import re
import sys
import uuid as _uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC
from email.utils import parsedate_to_datetime
from types import MappingProxyType
from typing import Final, Literal, TypeAlias, TypeGuard, TypeVar, cast, overload

from . import _errors
from ._sg_timezone import SG_TIMEZONE
from ._transport import (
    AbortedError,
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    TLSError,
    TransportError,
)

OperationCategory = Literal[
    'read',
    'write',
    'batch',
    'human_authentication',
    'session_authentication',
    'one_use_auth_exchange',
    'download',
    'upload',
    'unknown',
]
_EndpointRole = Literal[
    'read',
    'download',
    'object_put',
    'part_put',
    'allocation',
    'link',
    'finalize',
]
_FailureCategory = Literal[
    'transient',
    'rate_limit',
    'maintenance',
    'proxy_auth',
    'tls',
    'authentication',
    'abort',
    'configuration',
]
_FailurePhase = Literal['pre_send', 'ambiguous_after_send', 'http_status', 'body_read']
_RetryAfter: TypeAlias = float | Literal['too-large'] | None
_T = TypeVar('_T')

_ASCTIME_PATTERN = re.compile(
    r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) '
    r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
    r'(?: [1-9]|[12][0-9]|3[01]) [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}'
)
_BACKOFF_CAP = 30.0
_DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
_ENTITY_OPTIMIZATION_KEYS = frozenset(
    {'id', 'type', 'url', 'name', 'content_type', 'local_path', 'storage'}
)
_ERR_2FA = 106
_ERR_AUTH = 102
_ERR_OXYGEN = 110
_ERR_SSO = 108
_FAULT_BY_CODE: Mapping[int, type[_errors.Fault]] = MappingProxyType(
    {
        _ERR_2FA: _errors.MissingTwoFactorAuthenticationFault,
        _ERR_AUTH: _errors.AuthenticationFault,
        _ERR_OXYGEN: _errors.UserCredentialsNotAllowedForOxygenAuthenticationFault,
        _ERR_SSO: _errors.UserCredentialsNotAllowedForSSOAuthenticationFault,
    }
)
_HTTP_FORBIDDEN = 403
_HTTP_LONG_WEEKDAYS = (
    'Monday',
    'Tuesday',
    'Wednesday',
    'Thursday',
    'Friday',
    'Saturday',
    'Sunday',
)
_HTTP_MAINTENANCE = 503
_HTTP_MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
_HTTP_PROXY_AUTHENTICATION = 407
_HTTP_RATE_LIMIT = 429
_HTTP_SHORT_WEEKDAYS = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
_HTTP_SUCCESS_MAX_EXCLUSIVE = 300
_HTTP_SUCCESS_MIN = 200
_IMF_FIXDATE_PATTERN = re.compile(
    r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} '
    r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
    r'[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT'
)
_JITTER_BYTES = 8
_JITTER_RANGE = 1 << (_JITTER_BYTES * 8)
_MIN_CONDITION_PARTS = 2
_MULTIPART_BOUNDARY_PATTERN = re.compile(r"[A-Za-z0-9'+._-]{1,70}")
_OPTIMIZABLE_RELATIONS = frozenset({'is', 'is_not', 'in', 'not_in'})
_RETRYABLE_STATUSES = frozenset({502, 503, 504})
_RETRY_AFTER_TOO_LARGE: Final = 'too-large'
_RFC850_PATTERN = re.compile(
    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), [0-9]{2}-'
    r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-'
    r'[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT'
)
_SAFE_BATCH_REQUEST_TYPES = frozenset({'read'})
_SAFE_RPC_METHODS = frozenset(
    {
        'activity_stream_read',
        'export_page',
        'followers',
        'following',
        'nav_expand',
        'nav_search_entity',
        'nav_search_string',
        'note_thread_read',
        'preferences_read',
        'read',
        'schema_entity_read',
        'schema_field_read',
        'schema_read',
        'summarize',
        'text_search',
        'user_subscriptions_read',
        'work_schedule_read',
    }
)
_TERMINAL_FAILURE_CATEGORIES = frozenset(
    {'abort', 'authentication', 'configuration', 'proxy_auth', 'tls'}
)
_WRITE_METHODS = frozenset(
    {
        'create',
        'delete',
        'follow',
        'revive',
        'schema_field_create',
        'schema_field_delete',
        'schema_field_update',
        'unfollow',
        'update',
        'update_project_last_accessed',
        'user_subscriptions_create',
        'work_schedule_update',
    }
)


@dataclasses.dataclass(frozen=True)
class AuthSpec:
    """Collect authentication values used to build a request payload.

    Field bindings are frozen. `extra_auth_params` is retained by reference and
    remains mutable.

    Attributes:
        script_name: Script name for script authentication.
        api_key: API key for script authentication.
        user_login: Login for human-user authentication.
        user_password: Password for human-user authentication.
        session_token: Existing session token.
        auth_token: Optional two-factor authentication token.
        sudo_as_login: Optional user login for sudo impersonation.
        session_uuid: Optional session UUID.
        extra_auth_params: Optional additional authentication parameters.
    """

    script_name: str | None
    api_key: str | None
    user_login: str | None
    user_password: str | None
    session_token: str | None
    auth_token: str | None
    sudo_as_login: str | None
    session_uuid: str | None
    extra_auth_params: dict[str, object] | None


@dataclasses.dataclass(frozen=True)
class Failure:
    """Describe one failed attempt in retry-policy vocabulary.

    Attributes:
        phase: Attempt phase in which failure occurred.
        category: Stable transport or protocol failure category.
        status: HTTP status, when a response was received.
        retry_after: Explicit server delay in seconds, `'too-large'` when it
            exceeds 30 seconds, or `None` when no usable explicit delay is
            available. Eligible retries with `None` use computed backoff;
            `'too-large'` stops retrying.
    """

    phase: _FailurePhase
    category: _FailureCategory
    status: int | None
    retry_after: _RetryAfter


@dataclasses.dataclass(frozen=True)
class FileSlot:
    """Placeholder for file bytes inside a multipart plan.

    Attributes:
        name: Multipart form field name.
        filename: Filename exposed in the multipart part.
        content_type: Media type exposed in the multipart part.
    """

    name: str
    filename: str
    content_type: str


@dataclasses.dataclass(frozen=True)
class MultipartPlan:
    """Byte segments and file slots forming a multipart/form-data body.

    Attributes:
        content_type: Multipart content type including its boundary.
        segments: Ordered encoded segments and file placeholders.
    """

    content_type: str
    segments: list[bytes | FileSlot]


@dataclasses.dataclass(frozen=True)
class OperationDescriptor:
    """Describe the replay properties of one logical operation.

    Attributes:
        category: Stable operation category.
        replay_safe: Whether a completed request may be replayed.
        body_restartable: Whether each attempt can obtain a fresh body.
        endpoint_role: Special endpoint role, when applicable.
    """

    category: OperationCategory
    replay_safe: bool
    body_restartable: bool
    endpoint_role: _EndpointRole | None


def _backoff(
    operation: OperationDescriptor,
    attempt: int,
    base_interval: float,
    now: float,
) -> float | None:
    """Return deterministic full-jitter backoff from caller-supplied time."""
    if attempt < 1 or base_interval < 0 or not math.isfinite(base_interval):
        return None
    if now < 0 or not math.isfinite(now):
        return None
    if base_interval == 0:
        return 0.0

    ceiling = base_interval
    for _ in range(attempt - 1):
        if ceiling >= _BACKOFF_CAP / 2:
            ceiling = _BACKOFF_CAP
            break
        ceiling *= 2
    ceiling = min(ceiling, _BACKOFF_CAP)
    seed = repr(
        (
            operation.category,
            operation.replay_safe,
            operation.body_restartable,
            operation.endpoint_role,
            attempt,
            base_interval,
            now,
        )
    )
    digest = hashlib.sha256(seed.encode('ascii')).digest()
    fraction = int.from_bytes(digest[:_JITTER_BYTES], 'big') / _JITTER_RANGE
    return ceiling * fraction


def _batch_is_replay_safe(normalized_params: object) -> bool:
    """Return whether every normalized batch member is a known safe request."""
    members = normalized_params
    if isinstance(members, Mapping):
        params = cast('Mapping[object, object]', members)
        members = params.get('requests')
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        return False
    batch_members = cast('Sequence[object]', members)
    if not batch_members:
        return False

    for member in batch_members:
        if not isinstance(member, Mapping):
            return False
        request = cast('Mapping[object, object]', member)
        request_type = request.get('request_type')
        if request_type not in _SAFE_BATCH_REQUEST_TYPES:
            return False
    return True


def _contains_one_use_auth_token(normalized_params: object) -> bool:
    """Return whether normalized parameters contain a one-use auth token."""
    if isinstance(normalized_params, Mapping):
        params = cast('Mapping[object, object]', normalized_params)
        if params.get('auth_token') is not None:
            return True
        return any(_contains_one_use_auth_token(value) for value in params.values())
    if isinstance(normalized_params, Sequence) and not isinstance(
        normalized_params,
        (str, bytes, bytearray),
    ):
        values = cast('Sequence[object]', normalized_params)
        return any(_contains_one_use_auth_token(value) for value in values)
    return False


def _classify_rpc(
    method_name: str,
    normalized_params: object,
    has_one_use_auth_token: bool,
) -> tuple[OperationCategory, bool]:
    """Return the logical category and replay safety of an RPC."""
    category: OperationCategory = 'unknown'
    replay_safe = False
    if method_name == 'batch':
        category = 'batch'
        replay_safe = _batch_is_replay_safe(normalized_params)
    elif method_name == 'authenticate_human_user':
        category = 'human_authentication'
        replay_safe = not has_one_use_auth_token
    elif method_name == 'get_session_token':
        if has_one_use_auth_token or _contains_one_use_auth_token(normalized_params):
            category = 'one_use_auth_exchange'
        else:
            category = 'session_authentication'
            replay_safe = True
    elif method_name in _SAFE_RPC_METHODS:
        category = 'read'
        replay_safe = True
    elif method_name in _WRITE_METHODS:
        category = 'write'
    return category, replay_safe


def _entity_optimization_disabled() -> bool:
    """Read the canonical public runtime setting without import-time coupling."""
    module = sys.modules.get(f'{__package__}.shotgun')
    if module is None:
        raise RuntimeError('shotgun module is not initialized')
    return cast('bool', module.SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION)


def _escape_disposition(value: str) -> str:
    """Make a value safe inside a Content-Disposition quoted-string."""
    return value.replace('\\', '\\\\').replace('"', '%22').replace('\r', '').replace('\n', '')


def _explicit_delay_is_safe(delay: float) -> bool:
    """Return whether a delay stays within the bounded retry policy."""
    return math.isfinite(delay) and 0 <= delay <= _BACKOFF_CAP


def _failure_is_retryable(failure: Failure) -> bool:
    """Return whether a failure category is eligible in its observed phase."""
    if failure.category in _TERMINAL_FAILURE_CATEGORIES:
        return False
    if failure.phase == 'pre_send':
        return failure.category == 'transient'
    if failure.phase in {'ambiguous_after_send', 'body_read'}:
        return failure.category == 'transient'
    if failure.phase != 'http_status' or failure.status is None:
        return False
    if failure.category == 'transient':
        return failure.status in _RETRYABLE_STATUSES
    return failure.category in {'rate_limit', 'maintenance'}


def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    """Return whether a value is a dictionary with object-typed contents."""
    return isinstance(value, dict)


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    """Return whether a value is a list with object-typed contents."""
    return isinstance(value, list)


def _is_object_sequence(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    """Return whether a value is a supported sequence with object contents."""
    return isinstance(value, (list, tuple))


def _is_object_tuple(value: object) -> TypeGuard[tuple[object, ...]]:
    """Return whether a value is a tuple with object-typed contents."""
    return isinstance(value, tuple)


def _logical_operator(filter_operator: object) -> str:
    """Translate an official filter operator to its wire representation."""
    if filter_operator is None or filter_operator == 'all':
        return 'and'
    if filter_operator == 'any':
        return 'or'
    raise _errors.Fault(f'Invalid filter_operator: {filter_operator!r}')


def _parse_http_date(value: str) -> float | None:
    """Parse one of the three exact HTTP-date wire forms as GMT."""
    form: Literal['asctime', 'imf_fixdate', 'rfc850']
    if _IMF_FIXDATE_PATTERN.fullmatch(value):
        form = 'imf_fixdate'
    elif _RFC850_PATTERN.fullmatch(value):
        form = 'rfc850'
    elif _ASCTIME_PATTERN.fullmatch(value):
        form = 'asctime'
    else:
        return None

    try:
        parsed = parsedate_to_datetime(value)
    except (OverflowError, TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        if form != 'asctime':
            return None
        parsed = parsed.replace(tzinfo=UTC)

    short_weekday = _HTTP_SHORT_WEEKDAYS[parsed.weekday()]
    long_weekday = _HTTP_LONG_WEEKDAYS[parsed.weekday()]
    month = _HTTP_MONTHS[parsed.month - 1]
    if form == 'imf_fixdate':
        canonical = (
            f'{short_weekday}, {parsed.day:02d} {month} {parsed.year:04d} '
            f'{parsed.hour:02d}:{parsed.minute:02d}:{parsed.second:02d} GMT'
        )
    elif form == 'rfc850':
        canonical = (
            f'{long_weekday}, {parsed.day:02d}-{month}-{parsed.year % 100:02d} '
            f'{parsed.hour:02d}:{parsed.minute:02d}:{parsed.second:02d} GMT'
        )
    else:
        canonical = (
            f'{short_weekday} {month} {parsed.day:2d} '
            f'{parsed.hour:02d}:{parsed.minute:02d}:{parsed.second:02d} {parsed.year:04d}'
        )
    if canonical != value:
        return None
    return parsed.timestamp()


def _parse_retry_after(headers: Mapping[str, str], now: float) -> _RetryAfter:
    """Parse a case-insensitive `Retry-After` header into delay seconds."""
    value = next(
        (candidate for key, candidate in headers.items() if key.lower() == 'retry-after'),
        None,
    )
    if value is None or not math.isfinite(now):
        return None

    delay: float | None = None
    value = value.strip()
    if value.isascii() and value.isdigit():
        normalized = value.lstrip('0') or '0'
        cap = str(int(_BACKOFF_CAP))
        if len(normalized) > len(cap) or (len(normalized) == len(cap) and normalized > cap):
            return _RETRY_AFTER_TOO_LARGE
        delay = float(normalized)
    else:
        retry_at = _parse_http_date(value)
        if retry_at is not None:
            delay = max(0.0, retry_at - now)
    if delay is None or not math.isfinite(delay) or delay < 0:
        return None
    if delay > _BACKOFF_CAP:
        return _RETRY_AFTER_TOO_LARGE
    return delay


def _validate_multipart_boundary(boundary: str) -> str:
    """Return a boundary safe for multipart framing and an unquoted header."""
    if not _MULTIPART_BOUNDARY_PATTERN.fullmatch(boundary):
        raise ValueError('multipart boundary must be 1 to 70 safe ASCII characters')
    return boundary


def _validated_nonnegative_integer(value: object, name: str) -> int:
    """Return a non-boolean, nonnegative integer paging value."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _errors.ShotgunError(f'{name} must be a nonnegative integer')
    return value


def _validated_positive_integer(value: object, name: str) -> int:
    """Return a non-boolean, positive integer paging value."""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise _errors.ShotgunError(f'{name} must be a positive integer')
    return value


def _walk_object(data: object, visit: Callable[[object], object]) -> object:
    """Recursively visit leaves while rebuilding supported containers."""
    if _is_object_dict(data):
        return {key: _walk_object(value, visit) for key, value in data.items()}
    if _is_object_list(data):
        return [_walk_object(value, visit) for value in data]
    if _is_object_tuple(data):
        return tuple(_walk_object(value, visit) for value in data)
    return visit(data)


def build_auth_params(spec: AuthSpec) -> dict[str, object]:
    """Build wire authentication parameters from a configuration snapshot.

    Complete script credentials take precedence over complete user credentials,
    which take precedence over a session token. Extra authentication parameters
    are applied last and can replace generated values.

    Args:
        spec: Authentication configuration to serialize.

    Returns:
        A new authentication parameter dictionary.
    """
    params: dict[str, object] = {}
    if spec.script_name is not None and spec.api_key is not None:
        params['script_name'] = spec.script_name
        params['script_key'] = spec.api_key
    elif spec.user_login is not None and spec.user_password is not None:
        params['user_login'] = spec.user_login
        params['user_password'] = spec.user_password
        if spec.auth_token is not None:
            params['auth_token'] = spec.auth_token
    elif spec.session_token is not None:
        params['session_token'] = spec.session_token

    if spec.sudo_as_login is not None:
        params['sudo_as_login'] = spec.sudo_as_login
    if spec.session_uuid is not None:
        params['session_uuid'] = spec.session_uuid
    if spec.extra_auth_params:
        params.update(spec.extra_auth_params)
    return params


def build_payload(
    method_name: str,
    params: object,
    auth: dict[str, object] | None,
) -> dict[str, object]:
    """Build a JSON-RPC request payload.

    Args:
        method_name: Server method to call.
        params: Method parameters to append when truthy.
        auth: Authentication parameters to append when provided.

    Returns:
        A request payload with authentication before method parameters.
    """
    call_params: list[object] = []
    if auth is not None:
        call_params.append(auth)
    if params:
        call_params.append(params)
    return {'method_name': method_name, 'params': call_params}


def classify_endpoint(
    endpoint_role: _EndpointRole,
    body_restartable: bool = True,
) -> OperationDescriptor:
    """Classify a non-RPC endpoint exchange.

    Args:
        endpoint_role: Endpoint role used for retry and route decisions.
        body_restartable: Whether the body can restart from byte zero.

    Returns:
        An immutable descriptor with conservative retry behavior.
    """
    category: OperationCategory = 'unknown'
    replay_safe = False
    known_role: _EndpointRole | None = None
    if endpoint_role in {'read', 'download'}:
        category = 'read' if endpoint_role == 'read' else 'download'
        replay_safe = True
        known_role = endpoint_role
    elif endpoint_role in {'object_put', 'part_put'}:
        category = 'upload'
        replay_safe = True
        known_role = endpoint_role
    elif endpoint_role in {'allocation', 'link', 'finalize'}:
        category = 'upload'
        known_role = endpoint_role
    return OperationDescriptor(
        category=category,
        replay_safe=replay_safe,
        body_restartable=body_restartable,
        endpoint_role=known_role,
    )


def build_multipart_plan(
    fields: dict[str, str],
    files: list[FileSlot],
    boundary: str | None = None,
) -> MultipartPlan:
    """Build a pure multipart/form-data framing plan.

    Args:
        fields: Text form fields in multipart emission order.
        files: File placeholders in multipart emission order.
        boundary: Explicit boundary of 1-70 ASCII letters, digits, apostrophes,
            plus signs, periods, underscores, or hyphens; `None` generates one.

    Returns:
        A content type and ordered framing segments for the multipart body.

    Raises:
        ValueError: If an explicit boundary is empty or unsafe for unquoted
            use.
    """
    multipart_boundary = (
        _uuid.uuid4().hex if boundary is None else _validate_multipart_boundary(boundary)
    )
    segments: list[bytes | FileSlot] = []
    for name, value in fields.items():
        segments.append(
            (
                f'--{multipart_boundary}\r\nContent-Disposition: form-data; '
                f'name="{_escape_disposition(name)}"'
                f'\r\n\r\n{value}\r\n'
            ).encode()
        )
    for slot in files:
        segments.append(
            (
                f'--{multipart_boundary}\r\nContent-Disposition: form-data; '
                f'name="{_escape_disposition(slot.name)}"; '
                f'filename="{_escape_disposition(slot.filename)}"'
                f'\r\nContent-Type: {slot.content_type}'
                '\r\n\r\n'
            ).encode()
        )
        segments.append(slot)
        segments.append(b'\r\n')
    segments.append(f'--{multipart_boundary}--\r\n'.encode())
    return MultipartPlan(
        content_type=f'multipart/form-data; boundary={multipart_boundary}',
        segments=segments,
    )


def classify(
    method_name: str,
    normalized_params: object,
    body_restartable: bool = True,
    has_one_use_auth_token: bool = False,
) -> OperationDescriptor:
    """Classify an operation for phase-aware retry decisions.

    Args:
        method_name: Normalized RPC method name.
        normalized_params: Normalized method parameters, including every batch
            member.
        body_restartable: Whether the request body can be sent again from byte
            zero.
        has_one_use_auth_token: Whether captured request authentication
            includes a one-use token outside method parameters.

    Returns:
        An immutable operation descriptor. Unknown inputs are not replay-safe.
    """
    category, replay_safe = _classify_rpc(
        method_name,
        normalized_params,
        has_one_use_auth_token,
    )
    return OperationDescriptor(
        category=category,
        replay_safe=replay_safe,
        body_restartable=body_restartable,
        endpoint_role=None,
    )


def classify_rpc_operation(
    wire_method_name: str,
    logical_method_name: str | None,
    normalized_params: object,
    has_one_use_auth_token: bool,
) -> OperationDescriptor:
    """Classify a driver RPC while preserving public `info` semantics.

    Args:
        wire_method_name: RPC method sent on the wire.
        logical_method_name: Optional logical operation name when it differs.
        normalized_params: Normalized method parameters.
        has_one_use_auth_token: Whether captured authentication has a one-use
            token.

    Returns:
        A safe read descriptor for the driver's `info` call, otherwise the
        standard logical-method classification.
    """
    if wire_method_name == 'info':
        return OperationDescriptor(
            category='read',
            replay_safe=True,
            body_restartable=True,
            endpoint_role=None,
        )
    return classify(
        logical_method_name or wire_method_name,
        normalized_params,
        has_one_use_auth_token=has_one_use_auth_token,
    )


def classify_transport_failure(
    error: TransportError,
    response_received: bool,
) -> Failure:
    """Map transport exceptions to conservative retry-policy vocabulary.

    Args:
        error: Shared transport exception raised by an adapter or consumer.
        response_received: Whether response headers arrived before failure.

    Returns:
        Shared retry-policy failure vocabulary.
    """
    if isinstance(error, AbortedError):
        phase: _FailurePhase = 'body_read' if response_received else 'pre_send'
        category: _FailureCategory = 'abort'
    elif isinstance(error, ProxyError):
        phase = 'pre_send'
        category = 'proxy_auth'
    elif isinstance(error, TLSError):
        phase = 'pre_send'
        category = 'tls'
    elif isinstance(error, ConnectError):
        phase = 'pre_send'
        category = 'transient'
    elif isinstance(error, ReadTimeoutError):
        phase = 'body_read'
        category = 'transient'
    elif isinstance(error, SendError):
        phase = 'body_read' if response_received else 'ambiguous_after_send'
        category = 'transient'
    else:
        phase = 'body_read' if response_received else 'ambiguous_after_send'
        category = 'transient'
    return Failure(phase=phase, category=category, status=None, retry_after=None)


def decide(  # noqa: PLR0913
    operation: OperationDescriptor,
    attempt: int,
    failure: Failure,
    *,
    max_attempts: int,
    base_interval: float,
    now: float,
) -> float | None:
    """Return the retry delay for a failed attempt, or stop retrying.

    Args:
        operation: Replay properties for the logical operation.
        attempt: One-based number of the failed attempt.
        failure: Phase and category of the failed attempt.
        max_attempts: Maximum total attempts, including the failed attempt.
        base_interval: Initial full-jitter backoff ceiling in seconds.
        now: Caller-supplied, per-attempt high-resolution numeric seed. Callers
            should make this value unique for each attempt. Identical complete
            inputs intentionally reproduce the same jitter without hidden clock
            or global random state.

    Returns:
        Nonnegative sleep seconds when retry is allowed, otherwise `None`.
    """
    stop_retrying = (
        attempt < 1
        or max_attempts < 1
        or attempt >= max_attempts
        or not operation.body_restartable
        or (failure.phase != 'pre_send' and not operation.replay_safe)
        or not _failure_is_retryable(failure)
    )
    if stop_retrying:
        return None
    if failure.retry_after is None:
        return _backoff(operation, attempt, base_interval, now)
    if failure.retry_after == _RETRY_AFTER_TOO_LARGE:
        return None
    if _explicit_delay_is_safe(failure.retry_after):
        return failure.retry_after
    return None


def decode_response(headers: Mapping[str, str], body: bytes) -> object:
    """Decode an RPC response according to its content type.

    Args:
        headers: Read-only response headers, queried with `content-type`.
        body: Raw response body.

    Returns:
        An empty string for an empty body. Otherwise, parsed JSON for
        `application/json` or `text/javascript` content types, or decoded text
        for other content types. An absent or empty content type defaults to
        `application/json`.

    Raises:
        JSONDecodeError: If a non-empty JSON response is malformed.
        UnicodeDecodeError: If a non-empty response is not valid UTF-8.
    """
    if not body:
        return body.decode('utf-8', errors='replace')

    content_type = (headers.get('content-type') or 'application/json').lower()
    text = body.decode('utf-8')
    if content_type.startswith(('application/json', 'text/javascript')):
        return json.loads(text)
    return text


def default_user_agent(version: str) -> tuple[str, ...]:
    """Build the official-compatible default user-agent entries.

    Args:
        version: Package version reported by this implementation.

    Returns:
        Immutable user-agent entries in wire order.
    """
    return (
        f'shotgun-json ({version})',
        f'Python {sys.version_info.major}.{sys.version_info.minor}',
        'sg-python-api',
    )


def encode_payload(payload: dict[str, object]) -> bytes:
    """Encode a JSON-RPC payload as UTF-8 JSON.

    Args:
        payload: JSON-RPC payload to encode.

    Returns:
        UTF-8 JSON bytes without ASCII escaping.
    """
    return json.dumps(payload, ensure_ascii=False).encode('utf-8')


def extract_results(response: object) -> object:
    """Extract the results member from a response when present.

    Args:
        response: Decoded RPC response.

    Returns:
        The results member when present, otherwise the original response.
    """
    if not isinstance(response, dict):
        return response

    rpc_response = cast('dict[str, object]', response)
    if 'results' in rpc_response:
        return rpc_response['results']
    return rpc_response


def inject_local_paths(records: list[_T], local_path_field: str | None) -> list[_T]:
    """Enrich direct local-link fields with official local path keys in place.

    Args:
        records: Result records to enrich in place.
        local_path_field: Platform-specific source key, or `None` when
            unavailable.

    Returns:
        The original records list after any local-link enrichment.
    """
    if local_path_field is None:
        return records
    for record in records:
        if not _is_object_dict(record):
            continue
        for value in record.values():
            if (
                _is_object_dict(value)
                and value.get('link_type') == 'local'
                and local_path_field in value
            ):
                local_path = value[local_path_field]
                value['local_path'] = local_path
                value['url'] = f'file://{local_path or ""}'
    return records


def is_http_success(status: int) -> bool:
    """Return whether an HTTP status is in the successful 2xx range.

    Args:
        status: HTTP response status code.

    Returns:
        Whether the status represents success.
    """
    return _HTTP_SUCCESS_MIN <= status < _HTTP_SUCCESS_MAX_EXCLUSIVE


def check_http_status(
    status: int,
    reason: str,
    server: str,
    headers: Mapping[str, str],
    body: bytes,
) -> None:
    """Raise a protocol error for a non-success HTTP status.

    Args:
        status: HTTP response status code.
        reason: HTTP response reason phrase.
        server: Server URL associated with the response.
        headers: Read-only response headers.
        body: Raw response body.

    Raises:
        ProtocolError: If `status` is outside the 2xx range.
    """
    if is_http_success(status):
        return

    message = (
        f'{reason}: {body.decode("utf-8", errors="replace")}'
        if status == _HTTP_FORBIDDEN
        else reason
    )
    raise _errors.ProtocolError(server, status, message, dict(headers))


def classify_http_failure(
    status: int,
    headers: Mapping[str, str],
    now: float,
) -> Failure | None:
    """Classify a non-success HTTP response conservatively.

    Args:
        status: HTTP response status code.
        headers: Read-only response headers.
        now: Unix timestamp used to parse an HTTP-date `Retry-After` value.

    Returns:
        Shared retry-policy failure vocabulary, or `None` for success.
    """
    if is_http_success(status):
        return None
    if status == _HTTP_RATE_LIMIT:
        category: _FailureCategory = 'rate_limit'
    elif status == _HTTP_MAINTENANCE:
        category = 'maintenance'
    elif status in _RETRYABLE_STATUSES:
        category = 'transient'
    elif status == _HTTP_PROXY_AUTHENTICATION:
        category = 'proxy_auth'
    elif status in {_HTTP_FORBIDDEN, 401}:
        category = 'authentication'
    else:
        category = 'configuration'
    retry_after = (
        _parse_retry_after(headers, now)
        if category in {'maintenance', 'rate_limit', 'transient'}
        else None
    )
    return Failure(
        phase='http_status',
        category=category,
        status=status,
        retry_after=retry_after,
    )


def optimize_entity(value: object) -> object:
    """Return an optimized copy of an entity value when possible.

    Args:
        value: Potential entity value.

    Returns:
        A shallow dictionary projection retaining only `id`, `type`, `url`,
        `name`, `content_type`, `local_path`, and `storage`. Returns the
        original value when it is not a dictionary containing `type` and `id`,
        or when `shotgun.SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION` is enabled.
    """
    if (
        _entity_optimization_disabled()
        or not _is_object_dict(value)
        or 'type' not in value
        or 'id' not in value
    ):
        return value
    return {key: item for key, item in value.items() if key in _ENTITY_OPTIMIZATION_KEYS}


def _translate_condition(condition: object, optimize: bool) -> dict[str, object]:
    """Compile one list- or tuple-syntax condition without mutating it."""
    if not _is_object_sequence(condition) or len(condition) < _MIN_CONDITION_PARTS:
        raise _errors.Fault('filter conditions must be lists or tuples with a path and relation')

    path = condition[0]
    relation = condition[1]
    if not isinstance(path, str) or not isinstance(relation, str):
        raise _errors.Fault('filter condition paths and relations must be strings')

    values = list(condition[2:])
    if len(values) == 1 and _is_object_sequence(values[0]):
        values = list(values[0])
    if optimize and path != 'id' and relation in _OPTIMIZABLE_RELATIONS:
        values = [optimize_entity(value) for value in values]
    return {'path': path, 'relation': relation, 'values': values}


def _translate_filters(
    filters: object,
    filter_operator: str | None,
    optimize: bool,
) -> dict[str, object]:
    """Recursively compile supported filter syntax."""
    if _is_object_dict(filters):
        operator = _logical_operator(filters.get('filter_operator'))
        inner = filters.get('filters', [])
    else:
        operator = _logical_operator(filter_operator)
        inner = filters
    if not _is_object_sequence(inner):
        raise _errors.Fault('filters must be a list or tuple')

    conditions: list[object] = []
    for item in inner:
        if _is_object_dict(item):
            conditions.append(_translate_filters(item, None, optimize))
        else:
            conditions.append(_translate_condition(item, optimize))
    return {'logical_operator': operator, 'conditions': conditions}


def page_verdict(
    paging_info: dict[str, object],
    entities_count: int,
    records_count: int,
    per_page: int,
    limit: int,
) -> Literal['stop', 'continue']:
    """Decide whether paging stops after the page just consumed.

    Args:
        paging_info: Paging metadata returned with the consumed page.
        entities_count: Number of entities on the consumed page.
        records_count: Total number of entities accumulated so far.
        per_page: Requested page size.
        limit: Overall result limit, where zero means unlimited.

    Returns:
        `'stop'` when no next page should be requested, otherwise `'continue'`.

    Raises:
        ShotgunError: If counters or the active paging metadata signal are
            malformed.
    """
    entities_count = _validated_nonnegative_integer(entities_count, 'entities_count')
    records_count = _validated_nonnegative_integer(records_count, 'records_count')
    per_page = _validated_positive_integer(per_page, 'per_page')
    limit = _validated_nonnegative_integer(limit, 'limit')
    if records_count < entities_count:
        raise _errors.ShotgunError('records_count must include entities_count')

    if entities_count == 0:
        return 'stop'
    if limit and records_count >= limit:
        return 'stop'

    raw_entity_count = paging_info.get('entity_count')
    if raw_entity_count is not None:
        entity_count = _validated_nonnegative_integer(
            raw_entity_count,
            'paging_info entity_count',
        )
        if records_count >= entity_count:
            return 'stop'
    elif 'has_next_page' in paging_info:
        has_next_page = paging_info['has_next_page']
        if not isinstance(has_next_page, bool):
            raise _errors.ShotgunError('paging_info has_next_page must be a boolean')
        if not has_next_page:
            return 'stop'
    elif entities_count < per_page:
        return 'stop'
    return 'continue'


def project_entities(value: object) -> object:
    """Recursively project entity dictionaries into a detached payload.

    Args:
        value: Request value whose supported containers must be rebuilt.

    Returns:
        A detached value with direct and nested entities projected.
    """
    projected = optimize_entity(value)
    if _is_object_dict(projected):
        return {key: project_entities(item) for key, item in projected.items()}
    if _is_object_list(projected):
        return [project_entities(item) for item in projected]
    if _is_object_tuple(projected):
        return tuple(project_entities(item) for item in projected)
    return projected


def raise_fault(response: object) -> None:
    """Raise the mapped fault for an RPC exception response.

    Args:
        response: Decoded RPC response.

    Raises:
        Fault: If the response reports an RPC exception.
    """
    if not isinstance(response, dict):
        return

    rpc_response = cast('dict[str, object]', response)
    if not rpc_response.get('exception'):
        return

    message = str(rpc_response.get('message', 'Unknown Error'))
    code = rpc_response.get('error_code')
    fault_class = (
        _FAULT_BY_CODE.get(code, _errors.Fault) if isinstance(code, int) else _errors.Fault
    )
    raise fault_class(message)


@overload
def transform_inbound(data: dict[str, _T], convert_utc: bool) -> dict[str, object]: ...


@overload
def transform_inbound(data: list[_T], convert_utc: bool) -> list[object]: ...


@overload
def transform_inbound(data: tuple[_T, ...], convert_utc: bool) -> tuple[object, ...]: ...


@overload
def transform_inbound(data: object, convert_utc: bool) -> object: ...


def transform_inbound(data: object, convert_utc: bool) -> object:
    """Transform datetime strings and escaped less-than entities recursively.

    Args:
        data: Response data whose supported container topology is preserved.
        convert_utc: If true, convert recognized UTC datetime strings to aware
            local datetimes. If false, return naive datetimes retaining the UTC
            wall-clock values.

    Returns:
        Rebuilt response data with transformed leaf values. Invalid datetime
        strings remain unchanged.
    """

    def visit(value: object) -> object:
        if not isinstance(value, str):
            return value
        if _DATETIME_PATTERN.fullmatch(value):
            try:
                parsed = dt.datetime(
                    int(value[0:4]),
                    int(value[5:7]),
                    int(value[8:10]),
                    int(value[11:13]),
                    int(value[14:16]),
                    int(value[17:19]),
                    tzinfo=UTC,
                )
                if convert_utc:
                    return parsed.astimezone(SG_TIMEZONE.local)
                return parsed.replace(tzinfo=None)
            except (OSError, OverflowError, ValueError):
                return value
        return value.replace('&lt;', '<')

    return _walk_object(data, visit)


@overload
def transform_outbound(data: dict[str, _T], convert_utc: bool) -> dict[str, object]: ...


@overload
def transform_outbound(data: list[_T], convert_utc: bool) -> list[object]: ...


@overload
def transform_outbound(data: tuple[_T, ...], convert_utc: bool) -> tuple[object, ...]: ...


@overload
def transform_outbound(data: object, convert_utc: bool) -> object: ...


def transform_outbound(data: object, convert_utc: bool) -> object:
    """Transform date and datetime leaves to official wire strings recursively.

    Args:
        data: Request data whose supported container topology is preserved.
        convert_utc: If true, convert aware datetimes from their own timezone
            and naive datetimes from local time to UTC. If false, retain
            datetime wall-clock values without timezone conversion.

    Returns:
        Rebuilt request data with dates formatted as `YYYY-MM-DD` and datetimes
        as `YYYY-MM-DDTHH:MM:SSZ`. Datetime microseconds are omitted.
        The `Z` suffix is emitted even when conversion is disabled.

    Raises:
        ValueError: If converting a naive local datetime to UTC exceeds the
            supported range. The message is `datetime conversion exceeds the
            supported range`, and the exception has no cause.
    """

    def visit(value: object) -> object:
        if isinstance(value, dt.datetime):
            when = value
            if convert_utc:
                if when.tzinfo is None:
                    try:
                        when = when.replace(tzinfo=SG_TIMEZONE.local).astimezone(UTC)
                    except (OverflowError, ValueError):
                        raise ValueError(
                            'datetime conversion exceeds the supported range'
                        ) from None
                else:
                    when = when.astimezone(UTC)
            return (
                f'{when.year:04d}-{when.month:02d}-{when.day:02d}'
                f'T{when.hour:02d}:{when.minute:02d}:{when.second:02d}Z'
            )
        if isinstance(value, dt.date):
            return f'{value.year:04d}-{value.month:02d}-{value.day:02d}'
        return value

    return _walk_object(data, visit)


def require_filter_container(
    filters: object,
    filter_operator: str | None = None,
) -> list[object] | tuple[object, ...] | dict[str, object]:
    """Narrow a filter container while preserving operator validation order.

    Args:
        filters: Candidate public filter container.
        filter_operator: Combination mode for a non-dictionary container.

    Returns:
        The unchanged list, tuple, or dictionary.

    Raises:
        Fault: If a non-container has an invalid operator or container type.
    """
    if not isinstance(filters, (list, tuple, dict)):
        _logical_operator(filter_operator)
        raise _errors.Fault('filters must be a list or tuple')
    return cast('list[object] | tuple[object, ...] | dict[str, object]', filters)


def translate_filters(
    filters: list[_T] | tuple[_T, ...] | dict[str, _T],
    filter_operator: str | None = None,
    optimize: bool = True,
) -> dict[str, object]:
    """Compile public filter syntax to the JSON-RPC condition tree.

    Args:
        filters: List-, tuple-, or recursively nested dictionary-syntax
            filters.
        filter_operator: Combination mode for list- or tuple-syntax filters.
        optimize: Whether entity values may be projected to their wire keys.

    Returns:
        A new condition tree using wire logical operators.

    Raises:
        Fault: If an operator, filter container, or condition shape is invalid.
    """
    return _translate_filters(filters, filter_operator, optimize)


def validate_auth(spec: AuthSpec) -> None:
    """Validate authentication credential combinations.

    Args:
        spec: Authentication configuration to validate.

    Raises:
        ValueError: If credentials are incomplete, conflicting, or absent.
    """
    has_script = spec.script_name is not None or spec.api_key is not None
    has_user = spec.user_login is not None or spec.user_password is not None

    if spec.script_name is not None and spec.api_key is None:
        raise ValueError('script_name provided without api_key')
    if spec.api_key is not None and spec.script_name is None:
        raise ValueError('api_key provided without script_name')
    if spec.user_login is not None and spec.user_password is None:
        raise ValueError('login provided without password')
    if spec.user_password is not None and spec.user_login is None:
        raise ValueError('password provided without login')
    if has_script and has_user:
        raise ValueError('cannot use both script-based and user-based authentication')
    if spec.session_token is not None and (has_script or has_user):
        raise ValueError('session_token cannot be combined with other credentials')
    if spec.auth_token is not None and (spec.user_login is None or spec.user_password is None):
        raise ValueError('auth_token requires login and password')
    if not has_script and not has_user and spec.session_token is None:
        raise ValueError('no authentication credentials provided')
