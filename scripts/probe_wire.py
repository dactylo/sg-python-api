#!/usr/bin/env python
"""Capture sanitized RPC wire fixtures from a licensed test site.

Run `python scripts/probe_wire.py` with `SG_TEST_SITE`, `SG_TEST_SCRIPT`, and
`SG_TEST_KEY` configured. The site must contain a usable Project and active
HumanUser.

Capture creates temporary Notes and deletes them during cleanup. It also
updates Project access metadata, which is not restored. Successful capture,
cleanup, and validation precede replacement of files in `tests/fixtures`.

Importing this module prepends the checkout's `src` directory to `sys.path`.
"""

from __future__ import annotations

import base64
import binascii
import copy
import importlib
import json
import os
import re
import shutil
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar, cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Mapping, Sequence
    from types import TracebackType

    from shotgun_api3._client import Shotgun
    from shotgun_api3._transport import (
        StreamingResponse,
        Transport,
        TransportRequest,
        TransportResponse,
        Urllib3Transport,
    )

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

_AUTHENTICATION_ERROR_CODE = 102
_CAPTURE_SPECS: Mapping[
    str,
    tuple[str, Literal['script', 'session'], Literal['success', 'fault']],
] = {
    'activity_stream_read': ('activity_stream', 'script', 'success'),
    'batch': ('batch', 'script', 'success'),
    'create': ('create', 'script', 'success'),
    'fault_bad_auth': ('read', 'session', 'fault'),
    'following': ('following', 'script', 'success'),
    'get_session_token': ('get_session_token', 'script', 'success'),
    'info': ('info', 'script', 'success'),
    'nav_expand': ('nav_expand', 'script', 'success'),
    'preferences_read': ('preferences_read', 'script', 'success'),
    'read': ('read', 'script', 'success'),
    'schema_entity_read': ('schema_entity_read', 'script', 'success'),
    'schema_read': ('schema_read', 'script', 'success'),
    'summarize': ('summarize', 'script', 'success'),
    'text_search': ('query_display_name_cache', 'script', 'success'),
    'update_project_last_accessed': (
        'update_project_last_accessed_by_current_user',
        'script',
        'success',
    ),
    'work_schedule_read': ('work_schedule_read', 'script', 'success'),
}
_COMMON_SENSITIVE_KEYS = frozenset(
    {
        'api_key',
        'auth_token',
        'authorization',
        'cookie',
        'credentials',
        'proxy_authorization',
        'script_key',
        'script_name',
        'secret',
        'session_id',
        'session_token',
        'session_uuid',
        'set_cookie',
        'token',
        'user_login',
        'user_password',
    }
)
_FIXTURE_DIRECTORY = Path(__file__).resolve().parents[1] / 'tests' / 'fixtures'
_HTTP_STATUS_MAX = 599
_HTTP_STATUS_MIN = 100
_IDENTIFIER_SECRET_CATEGORIES = frozenset({'login', 'script_name', 'site', 'user_login'})
_NORMALIZED_SESSION_VALUE = 'X' * 21
_READ_ENTITY_LIMIT = 2
_REDACTED = 'REDACTED'
_REQUEST_HEADER_ALLOWLIST = frozenset({'content-type'})
_REQUIRED_OPERATION_LABELS = tuple(_CAPTURE_SPECS)
_RESPONSE_HEADER_ALLOWLIST = frozenset(
    {'content-language', 'content-type', 'x-request-id', 'x-sg-request-id'}
)
_SCHEMA_VERSION = 1
_SENSITIVE_SUFFIXES = ('_cookie', '_key', '_password', '_secret', '_token')
_T = TypeVar('_T')
_WORK_SCHEDULE_DAY_COUNT = 5
_WORK_SCHEDULE_WEEKDAY_END = 10


@dataclass(frozen=True)
class _CaptureSpec:
    """Describe one required labeled RPC capture."""

    label: str
    rpc_method: str
    auth_mode: Literal['script', 'session']
    outcome: Literal['success', 'fault']


class _FailureCollector:
    """Collect and suppress one lifecycle callback failure."""

    def __init__(self, failures: list[BaseException]) -> None:
        """Bind one shared ordered failure list."""
        self._failures = failures

    def __enter__(self) -> None:
        """Enter one callback boundary."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Collect one exception and suppress it so later callbacks run."""
        del exc_type, traceback
        if exc is None:
            return False
        self._failures.append(exc)
        return True


class _FixtureError(ValueError):
    """Report a malformed or incomplete fixture set."""


class _ProbeError(RuntimeError):
    """Report a live probe invariant failure."""


class _RecordingTransport:
    """Record one explicitly labeled RPC while delegating real transport work."""

    def __init__(self, inner: Transport, secrets: Mapping[str, str]) -> None:
        """Initialize a recorder around one independently owned transport."""
        self._active: _CaptureSpec | None = None
        self._inner = inner
        self._matched = 0
        self._pending: tuple[dict[str, object], bytes] | None = None
        self._raw_response_content: dict[str, bytes] = {}
        self._secrets = {category: secret for category, secret in secrets.items() if secret}
        self.records: dict[str, dict[str, object]] = {}

    def _add_secret(self, category: str, secret: str) -> None:
        """Register a credential and resanitize previously recorded responses."""
        if not secret:
            return
        self._secrets[category] = secret
        for label, raw_content in self._raw_response_content.items():
            raw_record = copy.deepcopy(self.records[label])
            response = cast('dict[str, object]', raw_record['response'])
            response['content_b64'] = base64.b64encode(raw_content).decode('ascii')
            self.records[label] = _normalize_fixture_record(raw_record, self._secrets)

    def _record(
        self,
        spec: _CaptureSpec,
        request: TransportRequest,
        body: dict[str, object],
        response: TransportResponse,
    ) -> None:
        """Hold the active raw exchange until its operation finishes."""
        if spec.label in self.records:
            raise _ProbeError(f'duplicate capture for {spec.label}')
        outcome: dict[str, object] = {'kind': spec.outcome}
        if spec.outcome == 'fault':
            outcome['exception'] = 'AuthenticationFault'
        raw_record: dict[str, object] = {
            'schema_version': _SCHEMA_VERSION,
            'operation': spec.label,
            'auth_mode': spec.auth_mode,
            'outcome': outcome,
            'request': {
                'method': request.method,
                'path': urlsplit(request.url).path,
                'headers': dict(request.headers),
                'body': copy.deepcopy(body),
            },
            'response': {
                'status': response.status,
                'reason': response.reason,
                'headers': dict(response.headers),
                'content_b64': base64.b64encode(response.content).decode('ascii'),
            },
        }
        self._pending = (raw_record, response.content)

    def _require_one_match(self, spec: _CaptureSpec) -> _ProbeError | None:
        """Return the capture-count error for `spec`, if any."""
        if self._matched == 1:
            return None
        return _ProbeError(
            f'{spec.label} did not capture exactly one exchange; captured {self._matched}'
        )

    @contextmanager
    def _capture(self, spec: _CaptureSpec) -> Generator[None, None, None]:
        """Require exactly one exchange matching `spec` in the context."""
        if self._active is not None:
            raise _ProbeError('capture contexts cannot overlap')
        if spec.label in self.records:
            raise _ProbeError(f'duplicate capture for {spec.label}')
        self._active = spec
        self._matched = 0
        try:
            yield
        finally:
            primary = sys.exception()
            self._active = None
            pending = self._pending
            self._pending = None
            capture_error = self._require_one_match(spec)
            failures: list[BaseException] = []
            if capture_error is not None:
                failures.append(capture_error)
            elif pending is not None:
                raw_record, raw_content = pending
                with _FailureCollector(failures):
                    normalized = _normalize_fixture_record(raw_record, self._secrets)
                    self.records[spec.label] = normalized
                    self._raw_response_content[spec.label] = raw_content
            if primary is not None and failures:
                raise BaseExceptionGroup(
                    f'{spec.label} call and capture failed',
                    [primary, *failures],
                )
            if failures:
                raise failures[0]

    def abort(self, token: str) -> None:
        """Abort one delegated in-flight request.

        Args:
            token: Request token identifying the in-flight exchange.
        """
        self._inner.abort(token)

    def close(self) -> None:
        """Close the delegated transport."""
        self._inner.close()

    def send(self, request: TransportRequest) -> TransportResponse:
        """Delegate one request and record it when its RPC matches the active spec.

        Args:
            request: Complete transport request.

        Returns:
            The delegated response unchanged.

        Raises:
            _ProbeError: If a labeled request is malformed or duplicated.
            TransportError: If the delegated transport fails.
        """
        response = self._inner.send(request)
        spec = self._active
        if spec is None:
            return response
        body = _request_body(request)
        if body.get('method_name') != spec.rpc_method:
            return response
        self._matched += 1
        if self._matched != 1:
            raise _ProbeError(f'{spec.label} matched more than one exchange')
        self._record(spec, request, body, response)
        return response

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        """Delegate an unlabeled streaming request.

        Args:
            request: Complete streaming transport request.

        Returns:
            The delegated streaming response.

        Raises:
            _ProbeError: If an RPC fixture capture is active.
            TransportError: If the delegated transport fails.
        """
        if self._active is not None:
            raise _ProbeError('RPC fixture capture cannot use a streaming response')
        return self._inner.send_streaming(request)


class _CleanupClient(Protocol):
    """Describe client cleanup operations used by the probe."""

    def close(self) -> None:
        """Close client resources."""
        ...

    def delete(self, entity_type: str, entity_id: int) -> bool:
        """Delete one tracked entity.

        Returns:
            Whether the entity was deleted.
        """
        ...


@dataclass(frozen=True)
class _LiveState:
    """Bundle both isolated clients and their recording transports."""

    bad_client: Shotgun
    bad_recorder: _RecordingTransport
    client: Shotgun
    recorder: _RecordingTransport


class _ResourceRegistry:
    """Track every probe-created entity for reverse-order deletion."""

    def __init__(self) -> None:
        """Initialize an empty creation-order resource list."""
        self._resources: list[tuple[str, int]] = []

    def _cleanup(self, client: _CleanupClient) -> list[BaseException]:
        """Delete all tracked resources while collecting every failure."""
        failures: list[BaseException] = []
        for entity_type, entity_id in reversed(self._resources):
            deleted = False
            before = len(failures)
            with _FailureCollector(failures):
                deleted = client.delete(entity_type, entity_id)
            if len(failures) == before and not deleted:
                failures.append(_ProbeError(f'cleanup rejected {entity_type} {entity_id}'))
        return failures

    def _track(self, entity_type: str, entity_id: int) -> None:
        """Track one created entity or attachment."""
        if not entity_type or isinstance(entity_id, bool) or entity_id <= 0:
            raise _ProbeError('created resource identity is malformed')
        self._resources.append((entity_type, entity_id))


def _capture_spec(label: str) -> _CaptureSpec:
    """Build one immutable capture spec from the required inventory."""
    try:
        rpc_method, auth_mode, outcome = _CAPTURE_SPECS[label]
    except KeyError:
        raise _ProbeError(f'unknown capture label {label}') from None
    return _CaptureSpec(label, rpc_method, auth_mode, outcome)


def _capture_call(
    recorder: _RecordingTransport,
    label: str,
    call: Callable[[], _T],
) -> _T:
    """Capture exactly one labeled public client call."""
    with recorder._capture(_capture_spec(label)):  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        return call()


def _contains_exact_string(value: object, expected: str) -> bool:
    """Return whether a JSON-like value has an exact string key or scalar."""
    if isinstance(value, dict):
        return any(
            key == expected or _contains_exact_string(item, expected)
            for key, item in cast('dict[object, object]', value).items()
        )
    if isinstance(value, list):
        return any(_contains_exact_string(item, expected) for item in cast('list[object]', value))
    if isinstance(value, tuple):
        return any(
            _contains_exact_string(item, expected) for item in cast('tuple[object, ...]', value)
        )
    return value == expected


def _expect_dict(value: object, description: str) -> dict[str, object]:
    """Return a string-keyed dictionary or reject the fixture value."""
    if not isinstance(value, dict):
        raise _FixtureError(f'{description} must be an object with string keys')
    mapping = cast('dict[object, object]', value)
    if any(not isinstance(key, str) for key in mapping):
        raise _FixtureError(f'{description} must be an object with string keys')
    return cast('dict[str, object]', mapping)


def _expect_exact_keys(
    value: Mapping[str, object],
    keys: Collection[str],
    description: str,
) -> None:
    """Require one mapping to contain exactly the named keys."""
    actual = frozenset(value)
    expected = frozenset(keys)
    if actual != expected:
        raise _FixtureError(f'{description} keys are malformed')


def _is_sensitive_key(key: object) -> bool:
    """Return whether a mapping key commonly carries a credential or session value."""
    if not isinstance(key, str):
        return False
    with_word_boundaries = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', key)
    normalized = with_word_boundaries.replace('-', '_').casefold()
    return normalized in _COMMON_SENSITIVE_KEYS or normalized.endswith(_SENSITIVE_SUFFIXES)


def _assert_fixture_sanitized(value: object) -> None:
    """Reject a fixture whose common sensitive key retains a value."""
    if isinstance(value, dict):
        mapping = cast('dict[object, object]', value)
        for key, item in mapping.items():
            if _is_sensitive_key(key) and item != _REDACTED:
                raise _FixtureError(f'sensitive key {key!r} is not redacted')
            _assert_fixture_sanitized(item)
    elif isinstance(value, list):
        for item in cast('list[object]', value):
            _assert_fixture_sanitized(item)


def _load_client_class() -> type[Shotgun]:
    """Load this checkout's client class after installing its source path."""
    module = importlib.import_module('shotgun_api3._client')
    return cast('type[Shotgun]', module.Shotgun)


def _load_error_classes() -> tuple[type[Exception], type[Exception]]:
    """Load the exact authentication fault and its base class."""
    module = importlib.import_module('shotgun_api3._errors')
    authentication_fault = cast('type[Exception]', module.AuthenticationFault)
    fault = cast('type[Exception]', module.Fault)
    return authentication_fault, fault


def _capture_bad_auth(client: Shotgun, recorder: _RecordingTransport) -> None:
    """Capture the exact authentication fault from an isolated session client."""
    authentication_fault, fault = _load_error_classes()
    try:
        with recorder._capture(_capture_spec('fault_bad_auth')):  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            client.find_one('Project', [], ['id'])
    except authentication_fault:
        return
    except fault as exc:
        raise _ProbeError('bad-auth capture raised the wrong fault subclass') from exc
    raise _ProbeError('bad-auth capture did not raise AuthenticationFault')


def _load_transport_class() -> type[Urllib3Transport]:
    """Load this checkout's real sync transport class."""
    module = importlib.import_module('shotgun_api3._transport')
    return cast('type[Urllib3Transport]', module.Urllib3Transport)


def _make_recording_client(
    site: str,
    secrets: Mapping[str, str],
    *,
    api_key: str | None = None,
    script_name: str | None = None,
    session_token: str | None = None,
) -> tuple[Shotgun, _RecordingTransport]:
    """Create one client with one independently owned recording transport."""
    client = _load_client_class()(
        site,
        script_name=script_name,
        api_key=api_key,
        session_token=session_token,
        connect=False,
    )
    recorder = _RecordingTransport(
        _load_transport_class()(ca_certs=os.environ.get('SHOTGUN_API_CACERTS') or None),
        secrets,
    )
    client._transport = recorder  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # White-box fixture capture owns this transport.
    return client, recorder


def _normalization_error(operation: str, rule: str) -> _FixtureError:
    """Build one operation-named error without including rejected content."""
    return _FixtureError(f'{operation} fixture normalization failed: {rule}')


def _normalization_dict(
    operation: str,
    value: object,
    keys: Collection[str],
    rule: str,
) -> dict[str, object]:
    """Require one exact string-keyed mapping for normalization."""
    if not isinstance(value, dict):
        raise _normalization_error(operation, rule)
    mapping = cast('dict[object, object]', value)
    if any(not isinstance(key, str) for key in mapping) or frozenset(mapping) != frozenset(keys):
        raise _normalization_error(operation, rule)
    return cast('dict[str, object]', mapping)


def _normalization_exact(
    operation: str,
    value: object,
    expected: object,
    rule: str,
) -> None:
    """Require one exact closed-protocol value."""
    if value != expected:
        raise _normalization_error(operation, rule)


def _normalization_headers(operation: str, value: object, rule: str) -> None:
    """Require string headers before replacing their names and values."""
    if not isinstance(value, dict):
        raise _normalization_error(operation, rule)
    headers = cast('dict[object, object]', value)
    if any(
        not isinstance(name, str) or not isinstance(item, str) for name, item in headers.items()
    ):
        raise _normalization_error(operation, rule)


def _normalization_list(
    operation: str,
    value: object,
    rule: str,
    *,
    length: int | None = None,
) -> list[object]:
    """Require one list, optionally with an exact length."""
    if not isinstance(value, list):
        raise _normalization_error(operation, rule)
    items = cast('list[object]', value)
    if length is not None and len(items) != length:
        raise _normalization_error(operation, rule)
    return items


def _assert_no_secret_bytes(
    record: dict[str, object],
    secrets: Mapping[str, str],
    private_canaries: Collection[str] = (),
) -> None:
    """Reject actual configured secret values without printing them."""
    serialized = json.dumps(record, ensure_ascii=False).encode()
    response = cast('Mapping[str, object]', record['response'])
    decoded_response = base64.b64decode(cast('str', response['content_b64']), validate=True)
    decoded_json = json.loads(decoded_response)
    operation = cast('str', record['operation'])
    for category, secret in secrets.items():
        if not secret:
            continue
        if category in _IDENTIFIER_SECRET_CATEGORIES:
            if _contains_exact_string(record, secret) or _contains_exact_string(
                decoded_json,
                secret,
            ):
                raise _ProbeError(f'{operation} fixture final scan failed for {category}')
            continue
        encoded = json.dumps(secret, ensure_ascii=False)[1:-1].encode()
        if encoded in serialized or encoded in decoded_response:
            raise _ProbeError(f'{operation} fixture final scan failed for {category}')
    for canary in private_canaries:
        if not canary:
            continue
        encoded = json.dumps(canary, ensure_ascii=False)[1:-1].encode()
        if encoded in serialized or encoded in decoded_response:
            raise _ProbeError(f'{operation} fixture final scan failed for private canary')


def _normalization_filters(operation: str, value: object) -> dict[str, object]:
    """Accept only the empty filter grammar used by the replay recipes."""
    filters = _normalization_dict(
        operation,
        value,
        {'conditions', 'logical_operator'},
        'request filters',
    )
    _normalization_exact(operation, filters['logical_operator'], 'and', 'request filters')
    _normalization_list(operation, filters['conditions'], 'request filters', length=0)
    return {'logical_operator': 'and', 'conditions': []}


def _normalization_nav_targets(operation: str, value: object) -> None:
    """Validate navigation target metadata without copying schema labels."""
    target = _normalization_dict(
        operation,
        value,
        {'additional_filter_presets', 'type'},
        'nav target',
    )
    if not isinstance(target['type'], str):
        raise _normalization_error(operation, 'nav target')
    presets = _normalization_list(
        operation,
        target['additional_filter_presets'],
        'nav presets',
    )
    for preset_value in presets:
        preset = _normalization_dict(
            operation,
            preset_value,
            {'path', 'preset_name', 'seed'},
            'nav preset',
        )
        if not isinstance(preset['path'], str) or not isinstance(preset['preset_name'], str):
            raise _normalization_error(operation, 'nav preset')
        seed = _normalization_dict(operation, preset['seed'], {'field', 'type'}, 'nav seed')
        if not isinstance(seed['field'], str) or not isinstance(seed['type'], str):
            raise _normalization_error(operation, 'nav seed')


def _normalization_nonnegative_int(operation: str, value: object, rule: str) -> None:
    """Require a nonnegative non-boolean integer without returning its raw value."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _normalization_error(operation, rule)


def _normalization_params(
    operation: str,
    body: Mapping[str, object],
    tail_length: int,
) -> tuple[list[object], list[object]]:
    """Validate authentication parameters and return raw and normalized tails."""
    _rpc_method, auth_mode, _outcome = _CAPTURE_SPECS[operation]
    auth_length = 0 if operation == 'info' else 1
    params = _normalization_list(
        operation,
        body['params'],
        'request params',
        length=auth_length + tail_length,
    )
    normalized: list[object] = []
    if auth_mode == 'script' and operation != 'info':
        auth = _normalization_dict(
            operation,
            params[0],
            {'script_key', 'script_name'},
            'script authentication',
        )
        if not isinstance(auth['script_name'], str) or not isinstance(auth['script_key'], str):
            raise _normalization_error(operation, 'script authentication')
        normalized.append({'script_name': _REDACTED, 'script_key': _REDACTED})
    elif auth_mode == 'session':
        auth = _normalization_dict(
            operation,
            params[0],
            {'session_token'},
            'session authentication',
        )
        if not isinstance(auth['session_token'], str):
            raise _normalization_error(operation, 'session authentication')
        normalized.append({'session_token': _REDACTED})
    return params[auth_length:], normalized


def _normalization_parts(
    value: Mapping[str, object],
    operation: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Validate common record surfaces and return raw request and response JSON."""
    record = _normalization_dict(
        operation,
        value,
        {'auth_mode', 'operation', 'outcome', 'request', 'response', 'schema_version'},
        'record shape',
    )
    rpc_method, auth_mode, outcome_kind = _CAPTURE_SPECS[operation]
    _normalization_exact(operation, record['schema_version'], _SCHEMA_VERSION, 'schema version')
    _normalization_exact(operation, record['operation'], operation, 'operation label')
    _normalization_exact(operation, record['auth_mode'], auth_mode, 'auth mode')
    expected_outcome: dict[str, object] = {'kind': outcome_kind}
    if outcome_kind == 'fault':
        expected_outcome['exception'] = 'AuthenticationFault'
    _normalization_exact(operation, record['outcome'], expected_outcome, 'outcome')
    request = _normalization_dict(
        operation,
        record['request'],
        {'body', 'headers', 'method', 'path'},
        'request shape',
    )
    _normalization_exact(operation, request['method'], 'POST', 'request method')
    _normalization_exact(operation, request['path'], '/api3/json', 'request path')
    _normalization_headers(operation, request['headers'], 'request headers')
    body = _normalization_dict(
        operation,
        request['body'],
        {'method_name', 'params'},
        'request body',
    )
    _normalization_exact(operation, body['method_name'], rpc_method, 'request RPC')
    response = _normalization_dict(
        operation,
        record['response'],
        {'content_b64', 'headers', 'reason', 'status'},
        'response shape',
    )
    if not isinstance(response['status'], int) or isinstance(response['status'], bool):
        raise _normalization_error(operation, 'response status')
    _normalization_exact(operation, response['status'], 200, 'response status')
    if not isinstance(response['reason'], str):
        raise _normalization_error(operation, 'response reason')
    _normalization_headers(operation, response['headers'], 'response headers')
    content_b64 = response['content_b64']
    if not isinstance(content_b64, str):
        raise _normalization_error(operation, 'response content')
    try:
        decoded = json.loads(base64.b64decode(content_b64, validate=True))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _normalization_error(operation, 'response content') from exc
    if not isinstance(decoded, dict):
        raise _normalization_error(operation, 'response envelope')
    return body, cast('dict[str, object]', decoded)


def _normalization_positive_int(operation: str, value: object, rule: str) -> None:
    """Require a positive non-boolean integer without returning its raw value."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _normalization_error(operation, rule)


def _normalization_entity(
    operation: str,
    value: object,
    expected_type: str,
    rule: str,
    *,
    with_name: bool,
) -> None:
    """Validate one relationship entity while discarding its identity."""
    keys = {'id', 'name', 'type'} if with_name else {'id', 'type'}
    entity = _normalization_dict(operation, value, keys, rule)
    _normalization_exact(operation, entity['type'], expected_type, rule)
    _normalization_positive_int(operation, entity['id'], rule)
    if with_name and not isinstance(entity['name'], str):
        raise _normalization_error(operation, rule)


def _normalization_fields(
    operation: str,
    value: object,
    subject: str,
) -> list[dict[str, object]]:
    """Validate the fixed Note field pair and replace its open values."""
    fields = _normalization_list(operation, value, 'Note fields', length=2)
    subject_field = _normalization_dict(
        operation,
        fields[0],
        {'field_name', 'value'},
        'Note subject field',
    )
    project_field = _normalization_dict(
        operation,
        fields[1],
        {'field_name', 'value'},
        'Note project field',
    )
    _normalization_exact(operation, subject_field['field_name'], 'subject', 'Note subject field')
    if not isinstance(subject_field['value'], str):
        raise _normalization_error(operation, 'Note subject field')
    _normalization_exact(operation, project_field['field_name'], 'project', 'Note project field')
    _normalization_entity(
        operation,
        project_field['value'],
        'Project',
        'Note project field',
        with_name=False,
    )
    return [
        {'field_name': 'subject', 'value': subject},
        {'field_name': 'project', 'value': {'type': 'Project', 'id': 1}},
    ]


def _normalization_nav_ref(operation: str, value: object) -> None:
    """Validate one navigation reference without copying it."""
    reference = _normalization_dict(operation, value, {'kind', 'value'}, 'nav reference')
    if not isinstance(reference['kind'], str):
        raise _normalization_error(operation, 'nav reference')
    target = reference['value']
    if target is not None:
        entity = _normalization_dict(operation, target, {'id', 'type'}, 'nav reference entity')
        if not isinstance(entity['type'], str):
            raise _normalization_error(operation, 'nav reference entity')
        _normalization_positive_int(operation, entity['id'], 'nav reference entity')


def _normalization_note_result(operation: str, value: object, subject: str) -> dict[str, object]:
    """Validate one returned Note and replace every open-ended field."""
    note = _normalization_dict(
        operation,
        value,
        {'id', 'project', 'subject', 'type'},
        'Note response',
    )
    _normalization_exact(operation, note['type'], 'Note', 'Note response')
    _normalization_positive_int(operation, note['id'], 'Note response')
    if not isinstance(note['subject'], str):
        raise _normalization_error(operation, 'Note response')
    _normalization_entity(
        operation,
        note['project'],
        'Project',
        'Note project response',
        with_name=True,
    )
    return {
        'id': 1,
        'subject': subject,
        'project': {'id': 1, 'name': 'Fixture Project', 'type': 'Project'},
        'type': 'Note',
    }


def _normalization_read_params(
    operation: str,
    value: object,
    return_field: str,
    entities_per_page: int,
) -> dict[str, object]:
    """Validate and normalize the fixed read request used by one recipe."""
    params = _normalization_dict(
        operation,
        value,
        {
            'api_return_image_urls',
            'filters',
            'paging',
            'return_fields',
            'return_only',
            'type',
        },
        'read request',
    )
    _normalization_exact(operation, params['type'], 'Project', 'read entity type')
    _normalization_exact(operation, params['return_fields'], [return_field], 'read fields')
    _normalization_exact(operation, params['return_only'], 'active', 'read return mode')
    if params['api_return_image_urls'] is not True:
        raise _normalization_error(operation, 'read image mode')
    filters = _normalization_filters(operation, params['filters'])
    paging = _normalization_dict(
        operation,
        params['paging'],
        {'current_page', 'entities_per_page'},
        'read paging',
    )
    _normalization_exact(operation, paging['current_page'], 1, 'read paging')
    _normalization_exact(
        operation,
        paging['entities_per_page'],
        entities_per_page,
        'read paging',
    )
    return {
        'type': 'Project',
        'return_fields': [return_field],
        'filters': filters,
        'return_only': 'active',
        'paging': {'entities_per_page': entities_per_page, 'current_page': 1},
        'api_return_image_urls': True,
    }


def _normalization_schema_value(operation: str, value: object, rule: str) -> None:
    """Validate one schema metadata wrapper while discarding its value."""
    wrapper = _normalization_dict(operation, value, {'editable', 'value'}, rule)
    if not isinstance(wrapper['editable'], bool):
        raise _normalization_error(operation, rule)


def _normalization_schema_field(operation: str, value: object) -> None:
    """Validate one complete field-schema entry without copying labels or values."""
    field = _normalization_dict(
        operation,
        value,
        {
            'custom_metadata',
            'data_type',
            'description',
            'editable',
            'entity_type',
            'mandatory',
            'name',
            'properties',
            'ui_value_displayable',
            'unique',
            'visible',
        },
        'schema field',
    )
    for name in (
        'custom_metadata',
        'data_type',
        'description',
        'editable',
        'entity_type',
        'mandatory',
        'name',
        'ui_value_displayable',
        'unique',
        'visible',
    ):
        _normalization_schema_value(operation, field[name], 'schema field metadata')
    properties = field['properties']
    if not isinstance(properties, dict):
        raise _normalization_error(operation, 'schema field properties')
    property_mapping = cast('dict[object, object]', properties)
    if any(not isinstance(name, str) for name in property_mapping):
        raise _normalization_error(operation, 'schema field properties')
    for property_value in property_mapping.values():
        _normalization_schema_value(operation, property_value, 'schema field property')


def _normalization_strings(
    operation: str,
    value: object,
    rule: str,
) -> list[str]:
    """Require one list containing only strings."""
    items = _normalization_list(operation, value, rule)
    if any(not isinstance(item, str) for item in items):
        raise _normalization_error(operation, rule)
    return cast('list[str]', items)


def _normalized_record(
    operation: str,
    params: list[object],
    response_content: Mapping[str, object],
) -> dict[str, object]:
    """Build one deterministic complete record before independent controls."""
    rpc_method, auth_mode, outcome_kind = _CAPTURE_SPECS[operation]
    outcome: dict[str, object] = {'kind': outcome_kind}
    if outcome_kind == 'fault':
        outcome['exception'] = 'AuthenticationFault'
    content = json.dumps(response_content, ensure_ascii=False, separators=(',', ':')).encode()
    return {
        'schema_version': _SCHEMA_VERSION,
        'operation': operation,
        'auth_mode': auth_mode,
        'outcome': outcome,
        'request': {
            'method': 'POST',
            'path': '/api3/json',
            'headers': {'content-type': 'application/json; charset=utf-8'},
            'body': {'method_name': rpc_method, 'params': params},
        },
        'response': {
            'status': 200,
            'reason': 'OK',
            'headers': {'content-type': 'application/json'},
            'content_b64': base64.b64encode(content).decode('ascii'),
        },
    }


def _normalize_activity_stream_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed activity-stream request and empty-update response."""
    operation = 'activity_stream_read'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    activity = _normalization_dict(
        operation,
        tail[0],
        {'entity_fields', 'id', 'limit', 'max_id', 'min_id', 'type'},
        'activity request',
    )
    _normalization_exact(operation, activity['type'], 'Project', 'activity entity type')
    _normalization_positive_int(operation, activity['id'], 'activity entity ID')
    for name in ('limit', 'max_id', 'min_id'):
        _normalization_exact(operation, activity[name], None, 'activity bounds')
    _normalization_exact(operation, activity['entity_fields'], {}, 'activity entity fields')
    _normalization_dict(
        operation,
        response,
        {'earliest_update_id', 'entity_id', 'entity_type', 'latest_update_id', 'updates'},
        'activity response',
    )
    _normalization_exact(operation, response['entity_type'], 'Project', 'activity response type')
    for name in ('earliest_update_id', 'entity_id', 'latest_update_id'):
        _normalization_nonnegative_int(operation, response[name], 'activity response identity')
    _normalization_list(operation, response['updates'], 'activity response updates', length=0)
    params.append(
        {
            'type': 'Project',
            'id': 1,
            'max_id': None,
            'min_id': None,
            'limit': None,
            'entity_fields': {},
        }
    )
    return _normalized_record(
        operation,
        params,
        {
            'entity_type': 'Project',
            'entity_id': 1,
            'latest_update_id': 1,
            'earliest_update_id': 1,
            'updates': [],
        },
    )


def _normalize_batch(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed single-create batch exchange."""
    operation = 'batch'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    requests = _normalization_list(operation, tail[0], 'batch requests', length=1)
    request = _normalization_dict(
        operation,
        requests[0],
        {'fields', 'request_type', 'return_fields', 'type'},
        'batch request',
    )
    _normalization_exact(operation, request['request_type'], 'create', 'batch request type')
    _normalization_exact(operation, request['type'], 'Note', 'batch entity type')
    _normalization_exact(operation, request['return_fields'], ['id'], 'batch return fields')
    fields = _normalization_fields(
        operation,
        request['fields'],
        'sg-python-api-wire-probe-replay-batch',
    )
    envelope = _normalization_dict(operation, response, {'results'}, 'batch response')
    results = _normalization_list(operation, envelope['results'], 'batch results', length=1)
    note = _normalization_note_result(
        operation,
        results[0],
        'sg-python-api-wire-probe-replay-batch',
    )
    params.append(
        [
            {
                'request_type': 'create',
                'type': 'Note',
                'fields': fields,
                'return_fields': ['id'],
            }
        ]
    )
    return _normalized_record(operation, params, {'results': [note]})


def _normalize_create(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed Note create exchange."""
    operation = 'create'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(
        operation,
        tail[0],
        {'fields', 'return_fields', 'type'},
        'create request',
    )
    _normalization_exact(operation, request['type'], 'Note', 'create entity type')
    _normalization_exact(operation, request['return_fields'], ['id'], 'create return fields')
    fields = _normalization_fields(
        operation,
        request['fields'],
        'sg-python-api-wire-probe-replay',
    )
    envelope = _normalization_dict(operation, response, {'results'}, 'create response')
    note = _normalization_note_result(
        operation,
        envelope['results'],
        'sg-python-api-wire-probe-replay',
    )
    params.append({'type': 'Note', 'fields': fields, 'return_fields': ['id']})
    return _normalized_record(operation, params, {'results': note})


def _normalize_fault_bad_auth(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed authentication-fault exchange."""
    operation = 'fault_bad_auth'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    params.append(_normalization_read_params(operation, tail[0], 'id', 1))
    fault = _normalization_dict(
        operation,
        response,
        {'error_code', 'exception', 'message'},
        'fault response',
    )
    if fault['exception'] is not True:
        raise _normalization_error(operation, 'fault exception')
    _normalization_exact(
        operation,
        fault['error_code'],
        _AUTHENTICATION_ERROR_CODE,
        'fault code',
    )
    if not isinstance(fault['message'], str):
        raise _normalization_error(operation, 'fault message')
    return _normalized_record(
        operation,
        params,
        {'exception': True, 'message': _REDACTED, 'error_code': _AUTHENTICATION_ERROR_CODE},
    )


def _normalize_following(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the following relationship exchange."""
    operation = 'following'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(operation, tail[0], {'user'}, 'following request')
    _normalization_entity(
        operation,
        request['user'],
        'HumanUser',
        'following user',
        with_name=False,
    )
    envelope = _normalization_dict(operation, response, {'results'}, 'following response')
    results = _normalization_list(operation, envelope['results'], 'following results')
    for result in results:
        entity = _normalization_dict(
            operation,
            result,
            {'id', 'type'},
            'following relationship',
        )
        if not isinstance(entity['type'], str):
            raise _normalization_error(operation, 'following relationship')
        _normalization_positive_int(operation, entity['id'], 'following relationship')
    params.append({'user': {'type': 'HumanUser', 'id': 1}})
    return _normalized_record(
        operation,
        params,
        {'results': [{'type': 'HumanUser', 'id': 1}]},
    )


def _normalize_get_session_token(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the issued-session exchange directly to synthetic values."""
    operation = 'get_session_token'
    body, response = _normalization_parts(value, operation)
    _tail, params = _normalization_params(operation, body, 0)
    session = _normalization_dict(
        operation,
        response,
        {'session_id', 'user_id', 'user_type'},
        'session response',
    )
    if not isinstance(session['session_id'], str):
        raise _normalization_error(operation, 'session credential')
    _normalization_exact(operation, session['user_type'], 'ApiUser', 'session user type')
    _normalization_positive_int(operation, session['user_id'], 'session user ID')
    return _normalized_record(
        operation,
        params,
        {
            'session_id': _NORMALIZED_SESSION_VALUE,
            'user_type': 'ApiUser',
            'user_id': 1,
        },
    )


def _normalize_info(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize server information to fixed protocol-only values."""
    operation = 'info'
    body, response = _normalization_parts(value, operation)
    _tail, params = _normalization_params(operation, body, 0)
    keys = {
        'analytics_site_id',
        'analytics_site_name',
        'api_max_entities_per_page',
        'authentication_app_session_launcher_enabled',
        'full_version',
        's3_direct_uploads_enabled',
        's3_enabled_upload_types',
        's3_uploads_enabled',
        'shotgunlocalhost_browser_integration_enabled',
        'unified_login_flow_enabled',
        'user_authentication_method',
        'version',
    }
    info = _normalization_dict(operation, response, keys, 'info response')
    for name in ('version', 'full_version'):
        versions = _normalization_list(operation, info[name], 'info version')
        if any(isinstance(item, bool) or not isinstance(item, int) for item in versions):
            raise _normalization_error(operation, 'info version')
    for name in (
        'authentication_app_session_launcher_enabled',
        's3_direct_uploads_enabled',
        's3_uploads_enabled',
        'shotgunlocalhost_browser_integration_enabled',
        'unified_login_flow_enabled',
    ):
        if not isinstance(info[name], bool):
            raise _normalization_error(operation, 'info flags')
    if not isinstance(info['s3_enabled_upload_types'], dict):
        raise _normalization_error(operation, 'info upload types')
    for name in ('analytics_site_id', 'analytics_site_name', 'user_authentication_method'):
        if not isinstance(info[name], str):
            raise _normalization_error(operation, 'info labels')
    _normalization_positive_int(
        operation,
        info['api_max_entities_per_page'],
        'info page limit',
    )
    return _normalized_record(
        operation,
        params,
        {
            'version': [9, 0, 0],
            'full_version': [9, 0, 0, 0],
            's3_uploads_enabled': False,
            's3_direct_uploads_enabled': False,
            's3_enabled_upload_types': {},
            'shotgunlocalhost_browser_integration_enabled': False,
            'unified_login_flow_enabled': False,
            'authentication_app_session_launcher_enabled': False,
            'analytics_site_id': 'fixture-site',
            'analytics_site_name': 'Fixture Site',
            'user_authentication_method': 'default',
            'api_max_entities_per_page': 500,
        },
    )


def _normalize_nav_expand(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize navigation expansion to a site-independent root node."""
    operation = 'nav_expand'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(operation, tail[0], {'path'}, 'nav request')
    _normalization_exact(operation, request['path'], '/', 'nav path')
    node = _normalization_dict(
        operation,
        response,
        {'children', 'has_children', 'label', 'path', 'ref', 'target_entities'},
        'nav response',
    )
    if not isinstance(node['label'], str) or not isinstance(node['path'], str):
        raise _normalization_error(operation, 'nav response')
    if not isinstance(node['has_children'], bool):
        raise _normalization_error(operation, 'nav response')
    _normalization_nav_ref(operation, node['ref'])
    _normalization_nav_targets(operation, node['target_entities'])
    children = _normalization_list(operation, node['children'], 'nav children')
    for child_value in children:
        child = _normalization_dict(
            operation,
            child_value,
            {'has_children', 'label', 'parent_path', 'path', 'ref', 'target_entities'},
            'nav child',
        )
        if (
            not isinstance(child['has_children'], bool)
            or not isinstance(child['label'], str)
            or not isinstance(child['parent_path'], str)
            or not isinstance(child['path'], str)
        ):
            raise _normalization_error(operation, 'nav child')
        _normalization_nav_ref(operation, child['ref'])
        _normalization_nav_targets(operation, child['target_entities'])
    params.append({'path': '/'})
    return _normalized_record(
        operation,
        params,
        {
            'label': 'Fixture Root',
            'ref': {'kind': 'root', 'value': None},
            'path': '/',
            'target_entities': {'type': 'Project', 'additional_filter_presets': []},
            'has_children': False,
            'children': [],
        },
    )


def _normalize_preferences_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize preferences to deterministic values for the fixed key set."""
    operation = 'preferences_read'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(operation, tail[0], {'prefs'}, 'preferences request')
    _normalization_list(operation, request['prefs'], 'preferences request', length=0)
    expected_types: dict[str, type[object]] = {
        'format_date_fields': str,
        'date_component_order': str,
        'format_time_hour_fields': str,
        'format_currency_fields_display_dollar_sign': bool,
        'format_currency_fields_decimal_options': str,
        'format_currency_fields_negative_options': str,
        'format_number_fields': str,
        'format_float_fields': str,
        'format_float_fields_rounding': str,
        'format_footage_fields': str,
        'support_local_storage': bool,
        'view_master_settings': str,
        'duration_units': str,
        'hours_per_day': float,
        'enable_rv_integration': bool,
        'enable_shotgun_review_for_rv': bool,
        'creative_review_settings': str,
    }
    preferences = _normalization_dict(
        operation,
        response,
        expected_types,
        'preferences response',
    )
    for name, expected_type in expected_types.items():
        if not isinstance(preferences[name], expected_type):
            raise _normalization_error(operation, 'preferences response')
    params.append({'prefs': []})
    return _normalized_record(
        operation,
        params,
        {
            'format_date_fields': '%Y-%m-%d',
            'date_component_order': 'YMD',
            'format_time_hour_fields': '24-hour',
            'format_currency_fields_display_dollar_sign': True,
            'format_currency_fields_decimal_options': '2',
            'format_currency_fields_negative_options': 'minus',
            'format_number_fields': '0',
            'format_float_fields': '0.00',
            'format_float_fields_rounding': 'round',
            'format_footage_fields': 'frames',
            'support_local_storage': False,
            'view_master_settings': 'default',
            'duration_units': 'hours',
            'hours_per_day': 8.0,
            'enable_rv_integration': False,
            'enable_shotgun_review_for_rv': False,
            'creative_review_settings': '{}',
        },
    )


def _normalize_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed two-project read exchange."""
    operation = 'read'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    params.append(_normalization_read_params(operation, tail[0], 'name', 2))
    envelope = _normalization_dict(operation, response, {'results'}, 'read response')
    results = _normalization_dict(
        operation,
        envelope['results'],
        {'entities', 'paging_info'},
        'read results',
    )
    entities = _normalization_list(operation, results['entities'], 'read entities')
    if len(entities) > _READ_ENTITY_LIMIT:
        raise _normalization_error(operation, 'read entities')
    for entity in entities:
        _normalization_entity(
            operation,
            entity,
            'Project',
            'read entity',
            with_name=True,
        )
    paging = _normalization_dict(
        operation,
        results['paging_info'],
        {'current_page', 'entities_per_page', 'entity_count', 'page_count'},
        'read paging response',
    )
    for item in paging.values():
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise _normalization_error(operation, 'read paging response')
    normalized_entities = [
        {'type': 'Project', 'id': index, 'name': f'Fixture Project {index}'}
        for index in range(1, len(entities) + 1)
    ]
    return _normalized_record(
        operation,
        params,
        {
            'results': {
                'entities': normalized_entities,
                'paging_info': {
                    'page_count': 1,
                    'current_page': 1,
                    'entities_per_page': 2,
                    'entity_count': len(normalized_entities),
                },
            }
        },
    )


def _normalize_schema_entity_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize entity-schema labels and metadata to one fixed entity."""
    operation = 'schema_entity_read'
    body, response = _normalization_parts(value, operation)
    _tail, params = _normalization_params(operation, body, 0)
    envelope = _normalization_dict(operation, response, {'results'}, 'entity schema response')
    results = envelope['results']
    if not isinstance(results, dict) or not results:
        raise _normalization_error(operation, 'entity schema results')
    entities = cast('dict[object, object]', results)
    if any(not isinstance(name, str) for name in entities):
        raise _normalization_error(operation, 'entity schema results')
    for entity_value in entities.values():
        entity = _normalization_dict(
            operation,
            entity_value,
            {'name', 'visible'},
            'entity schema entry',
        )
        _normalization_schema_value(operation, entity['name'], 'entity schema name')
        _normalization_schema_value(operation, entity['visible'], 'entity schema visibility')
    return _normalized_record(
        operation,
        params,
        {
            'results': {
                'Project': {
                    'name': {'value': 'Project', 'editable': False},
                    'visible': {'value': True, 'editable': False},
                }
            }
        },
    )


def _normalize_summarize(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed count summary exchange."""
    operation = 'summarize'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(
        operation,
        tail[0],
        {'filters', 'summaries', 'type'},
        'summarize request',
    )
    _normalization_exact(operation, request['type'], 'Project', 'summarize entity type')
    filters = _normalization_filters(operation, request['filters'])
    summaries = _normalization_list(operation, request['summaries'], 'summarize fields', length=1)
    summary = _normalization_dict(
        operation,
        summaries[0],
        {'field', 'type'},
        'summarize field',
    )
    _normalization_exact(operation, summary, {'field': 'id', 'type': 'count'}, 'summarize field')
    envelope = _normalization_dict(operation, response, {'results'}, 'summarize response')
    results = _normalization_dict(
        operation,
        envelope['results'],
        {'groups', 'summaries'},
        'summarize results',
    )
    summary_values = _normalization_dict(
        operation,
        results['summaries'],
        {'id'},
        'summarize values',
    )
    if isinstance(summary_values['id'], bool) or not isinstance(summary_values['id'], int):
        raise _normalization_error(operation, 'summarize values')
    _normalization_list(operation, results['groups'], 'summarize groups', length=0)
    params.append(
        {
            'type': 'Project',
            'filters': filters,
            'summaries': [{'field': 'id', 'type': 'count'}],
        }
    )
    return _normalized_record(
        operation,
        params,
        {'results': {'summaries': {'id': 1}, 'groups': []}},
    )


def _normalize_text_search(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed project text search exchange."""
    operation = 'text_search'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(
        operation,
        tail[0],
        {'entity_types', 'max_results', 'project_ids', 'text'},
        'text search request',
    )
    if not isinstance(request['text'], str):
        raise _normalization_error(operation, 'text search text')
    _normalization_exact(operation, request['project_ids'], [], 'text search projects')
    _normalization_exact(operation, request['max_results'], None, 'text search limit')
    entity_types = _normalization_dict(
        operation,
        request['entity_types'],
        {'Project'},
        'text search entity types',
    )
    filters = _normalization_filters(operation, entity_types['Project'])
    search = _normalization_dict(
        operation,
        response,
        {'matches', 'terms'},
        'text search response',
    )
    _normalization_list(operation, search['matches'], 'text search matches', length=0)
    _normalization_strings(operation, search['terms'], 'text search terms')
    params.append(
        {
            'text': 'wire probe',
            'entity_types': {'Project': filters},
            'project_ids': [],
            'max_results': None,
        }
    )
    return _normalized_record(operation, params, {'matches': [], 'terms': ['wire', 'probe']})


def _normalize_update_project_last_accessed(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Normalize the project-access update exchange."""
    operation = 'update_project_last_accessed'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(
        operation,
        tail[0],
        {'project_id', 'user_id'},
        'project access request',
    )
    _normalization_positive_int(operation, request['project_id'], 'project ID')
    _normalization_positive_int(operation, request['user_id'], 'user ID')
    envelope = _normalization_dict(operation, response, {'results'}, 'project access response')
    if envelope['results'] is not True:
        raise _normalization_error(operation, 'project access result')
    params.append({'project_id': 1, 'user_id': 1})
    return _normalized_record(operation, params, {'results': True})


def _normalize_work_schedule_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize the fixed five-day work schedule exchange."""
    operation = 'work_schedule_read'
    body, response = _normalization_parts(value, operation)
    tail, params = _normalization_params(operation, body, 1)
    request = _normalization_dict(
        operation,
        tail[0],
        {'end_date', 'start_date'},
        'work schedule request',
    )
    _normalization_exact(operation, request['start_date'], '2026-01-05', 'schedule start')
    _normalization_exact(operation, request['end_date'], '2026-01-09', 'schedule end')
    envelope = _normalization_dict(operation, response, {'results'}, 'work schedule response')
    results = envelope['results']
    if not isinstance(results, dict):
        raise _normalization_error(operation, 'work schedule results')
    schedule = cast('dict[object, object]', results)
    if len(schedule) != _WORK_SCHEDULE_DAY_COUNT:
        raise _normalization_error(operation, 'work schedule results')
    if any(not isinstance(day, str) for day in schedule):
        raise _normalization_error(operation, 'work schedule results')
    for day_value in schedule.values():
        day = _normalization_dict(
            operation,
            day_value,
            {'description', 'reason', 'working'},
            'work schedule day',
        )
        if (
            not isinstance(day['working'], bool)
            or day['description'] is not None
            or not isinstance(day['reason'], str)
        ):
            raise _normalization_error(operation, 'work schedule day')
    params.append({'start_date': '2026-01-05', 'end_date': '2026-01-09'})
    normalized_schedule = {
        f'2026-01-{day:02d}': {
            'working': day < _WORK_SCHEDULE_WEEKDAY_END,
            'description': None,
            'reason': 'Fixture schedule',
        }
        for day in range(5, 10)
    }
    return _normalized_record(operation, params, {'results': normalized_schedule})


def _normalized_schema_field() -> dict[str, object]:
    """Return one deterministic field-schema entry."""
    immutable_false = {'value': False, 'editable': False}
    return {
        'name': {'value': 'Id', 'editable': False},
        'description': {'value': 'Fixture identifier', 'editable': False},
        'custom_metadata': {'value': None, 'editable': False},
        'entity_type': {'value': 'Project', 'editable': False},
        'data_type': {'value': 'number', 'editable': False},
        'editable': copy.deepcopy(immutable_false),
        'mandatory': {'value': True, 'editable': False},
        'unique': {'value': True, 'editable': False},
        'properties': {'default_value': {'value': None, 'editable': False}},
        'visible': {'value': True, 'editable': False},
        'ui_value_displayable': {'value': True, 'editable': False},
    }


def _normalize_schema_read(value: Mapping[str, object]) -> dict[str, object]:
    """Normalize full schema labels and values to one fixed field."""
    operation = 'schema_read'
    body, response = _normalization_parts(value, operation)
    _tail, params = _normalization_params(operation, body, 0)
    envelope = _normalization_dict(operation, response, {'results'}, 'schema response')
    results = envelope['results']
    if not isinstance(results, dict) or not results:
        raise _normalization_error(operation, 'schema results')
    entities = cast('dict[object, object]', results)
    if any(not isinstance(name, str) for name in entities):
        raise _normalization_error(operation, 'schema results')
    for entity_value in entities.values():
        if not isinstance(entity_value, dict):
            raise _normalization_error(operation, 'schema entity')
        fields = cast('dict[object, object]', entity_value)
        if any(not isinstance(name, str) for name in fields):
            raise _normalization_error(operation, 'schema entity')
        for field in fields.values():
            _normalization_schema_field(operation, field)
    return _normalized_record(
        operation,
        params,
        {'results': {'Project': {'id': _normalized_schema_field()}}},
    )


def _fixture_normalizers() -> dict[str, Callable[[Mapping[str, object]], dict[str, object]]]:
    """Return the ordered fixture-normalization registry."""
    return {
        'activity_stream_read': _normalize_activity_stream_read,
        'batch': _normalize_batch,
        'create': _normalize_create,
        'fault_bad_auth': _normalize_fault_bad_auth,
        'following': _normalize_following,
        'get_session_token': _normalize_get_session_token,
        'info': _normalize_info,
        'nav_expand': _normalize_nav_expand,
        'preferences_read': _normalize_preferences_read,
        'read': _normalize_read,
        'schema_entity_read': _normalize_schema_entity_read,
        'schema_read': _normalize_schema_read,
        'summarize': _normalize_summarize,
        'text_search': _normalize_text_search,
        'update_project_last_accessed': _normalize_update_project_last_accessed,
        'work_schedule_read': _normalize_work_schedule_read,
    }


def _redact_text(value: str, secrets: Collection[str]) -> str:
    """Replace every configured secret substring in one text value."""
    redacted = value
    for secret in sorted((item for item in secrets if item), key=len, reverse=True):
        redacted = redacted.replace(secret, _REDACTED)
    return redacted


def _request_body(request: TransportRequest) -> dict[str, object]:
    """Decode one complete JSON RPC request body."""
    if not isinstance(request.body, bytes):
        raise _ProbeError('captured RPC request body is not complete bytes')
    try:
        value = json.loads(request.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _ProbeError('captured RPC request body is not valid JSON') from exc
    if not isinstance(value, dict):
        raise _ProbeError('captured RPC request body is not a JSON object')
    mapping = cast('dict[object, object]', value)
    if any(not isinstance(key, str) for key in mapping):
        raise _ProbeError('captured RPC request body is not a JSON object')
    return cast('dict[str, object]', mapping)


def _require_entity(value: object, expected_type: str) -> dict[str, object]:
    """Return one live entity with an integer ID or fail the probe."""
    if not isinstance(value, dict):
        raise _ProbeError(f'live setup did not return a {expected_type}')
    entity = cast('dict[str, object]', value)
    entity_id = entity.get('id')
    if isinstance(entity_id, bool) or not isinstance(entity_id, int) or entity_id <= 0:
        raise _ProbeError(f'live setup returned a malformed {expected_type}')
    entity_type = entity.get('type')
    if entity_type not in {None, expected_type}:
        raise _ProbeError(f'live setup returned the wrong {expected_type} type')
    return {'type': expected_type, 'id': entity_id}


def _run_guarded(
    operation: Callable[[], _T],
    resources: _ResourceRegistry,
    cleanup_client: _CleanupClient,
    clients: Sequence[_CleanupClient],
) -> _T:
    """Run work, reverse-clean its resources, and close every client."""
    result: _T | None = None
    try:
        result = operation()
    finally:
        primary = sys.exception()
        failures = ([primary] if primary is not None else []) + resources._cleanup(cleanup_client)  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        for client in clients:
            with _FailureCollector(failures):
                client.close()
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise BaseExceptionGroup('wire probe operation and lifecycle failures', failures)
    return result


def _run_live_operations(
    state: _LiveState,
    resources: _ResourceRegistry,
    run_name: str,
    secrets: dict[str, str],
) -> list[dict[str, object]]:
    """Execute the complete required live inventory under one namespace."""
    bad_client = state.bad_client
    bad_recorder = state.bad_recorder
    client = state.client
    recorder = state.recorder
    project = _require_entity(client.find_one('Project', [], ['id']), 'Project')
    user = _require_entity(
        client.find_one('HumanUser', [['sg_status_list', 'is', 'act']], ['id']),
        'HumanUser',
    )
    _capture_call(recorder, 'info', client.info)
    _capture_call(recorder, 'read', lambda: client.find('Project', [], ['name'], limit=2))

    def create_note() -> dict[str, object]:
        result = client.create(
            'Note',
            {'subject': run_name, 'project': project},
        )
        note = _require_entity(result, 'Note')
        resources._track('Note', cast('int', note['id']))  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        return result

    _capture_call(recorder, 'create', create_note)
    _capture_call(
        recorder,
        'summarize',
        lambda: client.summarize('Project', [], [{'field': 'id', 'type': 'count'}]),
    )
    _capture_call(
        recorder,
        'text_search',
        lambda: client.text_search(run_name, {'Project': []}),
    )
    _capture_call(recorder, 'schema_read', client.schema_read)
    _capture_call(recorder, 'schema_entity_read', client.schema_entity_read)
    _capture_call(recorder, 'preferences_read', client.preferences_read)
    _capture_call(
        recorder,
        'activity_stream_read',
        lambda: client.activity_stream_read('Project', cast('int', project['id'])),
    )
    _capture_call(recorder, 'following', lambda: client.following(user))
    _capture_call(recorder, 'nav_expand', lambda: client.nav_expand('/'))
    _capture_call(
        recorder,
        'work_schedule_read',
        lambda: client.work_schedule_read('2026-01-05', '2026-01-09'),
    )
    _capture_call(
        recorder,
        'update_project_last_accessed',
        lambda: client.update_project_last_accessed(project, user=user),
    )

    def create_batch_note() -> list[object]:
        batch_results = client.batch(
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Note',
                    'data': {'subject': f'{run_name}-batch', 'project': project},
                }
            ]
        )
        if len(batch_results) != 1:
            raise _ProbeError('batch capture did not create exactly one entity')
        batch_note = _require_entity(batch_results[0], 'Note')
        resources._track('Note', cast('int', batch_note['id']))  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        return batch_results

    _capture_call(recorder, 'batch', create_batch_note)
    session_token = _capture_call(recorder, 'get_session_token', client.get_session_token)
    secrets['session_token'] = session_token
    recorder._add_secret('session_token', session_token)  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    bad_recorder._add_secret('session_token', session_token)  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _capture_bad_auth(bad_client, bad_recorder)
    return [*recorder.records.values(), *bad_recorder.records.values()]


def _sanitize(value: object, secrets: Collection[str]) -> object:
    """Recursively redact common sensitive keys and configured secret values."""
    if isinstance(value, dict):
        sanitized: dict[object, object] = {}
        for key, item in cast('dict[object, object]', value).items():
            sanitized[key] = _REDACTED if _is_sensitive_key(key) else _sanitize(item, secrets)
        return sanitized
    if isinstance(value, list):
        return [_sanitize(item, secrets) for item in cast('list[object]', value)]
    if isinstance(value, tuple):
        return tuple(_sanitize(item, secrets) for item in cast('tuple[object, ...]', value))
    if isinstance(value, str):
        return _redact_text(value, secrets)
    if isinstance(value, bytes):
        sanitized_bytes = value
        for secret in sorted((item for item in secrets if item), key=len, reverse=True):
            sanitized_bytes = sanitized_bytes.replace(secret.encode(), _REDACTED.encode())
        return sanitized_bytes
    return value


def _select_headers(
    headers: Mapping[str, str],
    allowlist: Collection[str],
    secrets: Collection[str],
) -> dict[str, str]:
    """Select a small response/request header set case-insensitively."""
    allowed = {name.casefold() for name in allowlist}
    selected: dict[str, str] = {}
    for name, value in headers.items():
        normalized = name.casefold()
        if normalized not in allowed:
            continue
        selected[normalized] = (
            _REDACTED if _is_sensitive_key(name) else _redact_text(value, secrets)
        )
    return {name: selected[name] for name in sorted(selected)}


def _validate_operation_contract(
    operation: str,
    auth_mode: object,
    kind: str,
    body: Mapping[str, object],
) -> None:
    """Validate operation-specific RPC, auth, and outcome metadata."""
    spec_tuple = _CAPTURE_SPECS.get(operation)
    if spec_tuple is None:
        return
    rpc_method, expected_auth, expected_outcome = spec_tuple
    if body['method_name'] != rpc_method:
        raise _FixtureError('fixture request RPC does not match its operation')
    if auth_mode != expected_auth or kind != expected_outcome:
        raise _FixtureError('fixture auth or outcome does not match its operation')


def _validate_outcome(value: object) -> tuple[dict[str, object], Literal['success', 'fault']]:
    """Validate and return one explicit success or authentication-fault outcome."""
    outcome = _expect_dict(value, 'fixture outcome')
    kind = outcome.get('kind')
    if kind == 'success':
        _expect_exact_keys(outcome, {'kind'}, 'success outcome')
        return outcome, 'success'
    if kind == 'fault':
        _expect_exact_keys(outcome, {'exception', 'kind'}, 'fault outcome')
        if outcome['exception'] != 'AuthenticationFault':
            raise _FixtureError('fault outcome maps the wrong exception')
        return outcome, 'fault'
    raise _FixtureError('fixture outcome cannot be skipped or unknown')


def _validate_request(value: object) -> tuple[dict[str, object], dict[str, object]]:
    """Validate request metadata and return its full JSON RPC body."""
    request = _expect_dict(value, 'fixture request')
    _expect_exact_keys(request, {'body', 'headers', 'method', 'path'}, 'fixture request')
    if request['method'] != 'POST':
        raise _FixtureError('fixture request method must be POST')
    if not isinstance(request['path'], str) or not request['path'].startswith('/'):
        raise _FixtureError('fixture request path must be origin-relative')
    request_headers = _expect_dict(request['headers'], 'fixture request headers')
    if not frozenset(request_headers) <= _REQUEST_HEADER_ALLOWLIST or any(
        not isinstance(item, str) for item in request_headers.values()
    ):
        raise _FixtureError('fixture request headers are malformed')
    body = _expect_dict(request['body'], 'fixture request body')
    if not isinstance(body.get('method_name'), str) or 'params' not in body:
        raise _FixtureError('fixture request body lacks RPC metadata')
    return request, body


def _validate_response(
    value: object,
    kind: Literal['success', 'fault'],
) -> tuple[dict[str, object], dict[str, object]]:
    """Validate response metadata, exact bytes, and expected envelope kind."""
    response = _expect_dict(value, 'fixture response')
    _expect_exact_keys(
        response,
        {'content_b64', 'headers', 'reason', 'status'},
        'fixture response',
    )
    status = response['status']
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or not _HTTP_STATUS_MIN <= status <= _HTTP_STATUS_MAX
    ):
        raise _FixtureError('fixture response status is malformed')
    if not isinstance(response['reason'], str):
        raise _FixtureError('fixture response reason is malformed')
    response_headers = _expect_dict(response['headers'], 'fixture response headers')
    if not frozenset(response_headers) <= _RESPONSE_HEADER_ALLOWLIST or any(
        not isinstance(item, str) for item in response_headers.values()
    ):
        raise _FixtureError('fixture response headers are malformed')
    content_b64 = response['content_b64']
    if not isinstance(content_b64, str):
        raise _FixtureError('fixture response content_b64 is malformed')
    try:
        decoded = json.loads(base64.b64decode(content_b64, validate=True))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _FixtureError('fixture response content is not base64 JSON') from exc
    envelope = _expect_dict(decoded, 'fixture response envelope')
    is_fault = envelope.get('exception') is True
    if kind == 'fault' and (
        not is_fault or envelope.get('error_code') != _AUTHENTICATION_ERROR_CODE
    ):
        raise _FixtureError('fixture response is not the expected authentication fault')
    if kind == 'success' and is_fault:
        raise _FixtureError('successful fixture contains a fault envelope')
    return response, envelope


def _validate_fixture(value: object) -> dict[str, object]:
    """Validate one fixture against the exact versioned schema."""
    record = _expect_dict(value, 'fixture')
    _expect_exact_keys(
        record,
        {'auth_mode', 'operation', 'outcome', 'request', 'response', 'schema_version'},
        'fixture',
    )
    if record['schema_version'] != _SCHEMA_VERSION:
        raise _FixtureError('fixture schema version is unsupported')
    operation = record['operation']
    if not isinstance(operation, str) or not operation:
        raise _FixtureError('fixture operation must be a nonempty string')
    auth_mode = record['auth_mode']
    if auth_mode not in {'script', 'session'}:
        raise _FixtureError('fixture auth_mode is unsupported')
    _outcome, kind = _validate_outcome(record['outcome'])
    _request, body = _validate_request(record['request'])
    _response, _envelope = _validate_response(record['response'], kind)
    _validate_operation_contract(operation, auth_mode, kind, body)
    _assert_fixture_sanitized(record)
    return record


def _normalize_fixture_record(
    value: object,
    secrets: Mapping[str, str] | None = None,
    private_canaries: Collection[str] = (),
) -> dict[str, object]:
    """Normalize one complete fixture, then apply independent publication controls."""
    if not isinstance(value, dict):
        raise _FixtureError('fixture normalization failed: operation')
    record = cast('dict[str, object]', value)
    operation = record.get('operation')
    normalizers = _fixture_normalizers()
    if not isinstance(operation, str) or operation not in normalizers:
        raise _FixtureError('fixture normalization failed: operation')
    normalized = normalizers[operation](record)
    configured = {category: secret for category, secret in (secrets or {}).items() if secret}
    request = cast('dict[str, object]', normalized['request'])
    request['headers'] = _select_headers(
        cast('Mapping[str, str]', request['headers']),
        _REQUEST_HEADER_ALLOWLIST,
        configured.values(),
    )
    response = cast('dict[str, object]', normalized['response'])
    response['headers'] = _select_headers(
        cast('Mapping[str, str]', response['headers']),
        _RESPONSE_HEADER_ALLOWLIST,
        configured.values(),
    )
    response['reason'] = _redact_text(cast('str', response['reason']), configured.values())
    _assert_no_secret_bytes(normalized, configured, private_canaries)
    return _validate_fixture(normalized)


def _validate_fixture_inventory(
    records: Collection[dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Require exactly one valid fixture for every planned operation."""
    if not records:
        raise _FixtureError('fixture inventory is empty')
    inventory: dict[str, dict[str, object]] = {}
    for candidate in records:
        record = _validate_fixture(candidate)
        label = cast('str', record['operation'])
        if label not in _REQUIRED_OPERATION_LABELS:
            raise _FixtureError(f'unknown fixture label {label}')
        if label in inventory:
            raise _FixtureError(f'duplicate fixture label {label}')
        inventory[label] = record
    missing = frozenset(_REQUIRED_OPERATION_LABELS) - inventory.keys()
    if missing:
        raise _FixtureError('fixture inventory is missing required labels')
    return inventory


def _load_fixture_directory(directory: Path) -> dict[str, dict[str, object]]:
    """Load and validate every fixture from one directory."""
    paths = sorted(directory.glob('*.json')) if directory.is_dir() else []
    if not paths:
        raise _FixtureError('fixture directory is empty')
    records: list[dict[str, object]] = []
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _FixtureError(f'{path.name} is not readable fixture JSON') from exc
        record = _validate_fixture(value)
        if path.stem != record['operation']:
            raise _FixtureError(f'{path.name} does not match its operation label')
        records.append(record)
    return _validate_fixture_inventory(records)


def _run_live_probe(
    site: str,
    script_name: str,
    api_key: str,
) -> tuple[list[dict[str, object]], dict[str, str], frozenset[str]]:
    """Create isolated clients, run the inventory, and enforce lifecycle cleanup."""
    run_name = f'sg-python-api-wire-probe-{uuid.uuid4().hex}'
    bad_session = f'invalid-{uuid.uuid4().hex}'
    secrets = {
        'api_key': api_key,
        'invalid_session': bad_session,
        'script_name': script_name,
        'site': site,
    }
    client, recorder = _make_recording_client(
        site,
        secrets,
        api_key=api_key,
        script_name=script_name,
    )
    try:
        bad_client, bad_recorder = _make_recording_client(
            site,
            secrets,
            session_token=bad_session,
        )
    except Exception:
        client.close()
        raise
    resources = _ResourceRegistry()
    state = _LiveState(
        bad_client=bad_client,
        bad_recorder=bad_recorder,
        client=client,
        recorder=recorder,
    )
    records = _run_guarded(
        lambda: _run_live_operations(
            state,
            resources,
            run_name,
            secrets,
        ),
        resources,
        client,
        [client, bad_client],
    )
    inventory = _validate_fixture_inventory(records)
    for record in inventory.values():
        _assert_no_secret_bytes(record, secrets, {run_name})
    return list(inventory.values()), secrets, frozenset({run_name})


def _write_fixture_directory(
    directory: Path,
    records: Sequence[dict[str, object]],
    *,
    secrets: Mapping[str, str] | None = None,
    private_canaries: Collection[str] = (),
) -> None:
    """Publish a complete validated fixture inventory transactionally."""
    normalized_records = [
        _normalize_fixture_record(record, secrets, private_canaries) for record in records
    ]
    inventory = _validate_fixture_inventory(normalized_records)
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging = directory.parent / f'.{directory.name}.{uuid.uuid4().hex}.staging'
    backup = directory.parent / f'.{directory.name}.{uuid.uuid4().hex}.backup'
    staging.mkdir()
    moved_existing = False
    try:
        for label in sorted(inventory):
            (staging / f'{label}.json').write_text(
                json.dumps(inventory[label], ensure_ascii=False, indent=2) + '\n',
                encoding='utf-8',
            )
        _load_fixture_directory(staging)
        if directory.exists():
            directory.replace(backup)
            moved_existing = True
        try:
            staging.replace(directory)
        except BaseException:
            if moved_existing:
                backup.replace(directory)
            raise
        if moved_existing:
            shutil.rmtree(backup)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main() -> int:
    """Capture, sanitize, validate, and publish the full live fixture inventory.

    Returns:
        Process exit status zero after successful capture, cleanup, and
        publication.

    Raises:
        KeyError: If a required `SG_TEST_*` environment variable is absent.
        BaseException: If capture, cleanup, close, validation, or publication
            fails.
    """
    site = os.environ['SG_TEST_SITE']
    script_name = os.environ['SG_TEST_SCRIPT']
    api_key = os.environ['SG_TEST_KEY']
    records, secrets, private_canaries = _run_live_probe(site, script_name, api_key)
    _write_fixture_directory(
        _FIXTURE_DIRECTORY,
        records,
        secrets=secrets,
        private_canaries=private_canaries,
    )
    for label in sorted(_REQUIRED_OPERATION_LABELS):
        print(f'captured {label}')
    print('cleanup: passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
