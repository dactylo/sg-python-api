"""Locked client configuration and immutable operation snapshots."""

# Same-file collaborators access configuration implementation state.
# pyright: reportPrivateUsage=false

import base64
import copy
import dataclasses
import ipaddress
import logging
import math
import os
import threading
import urllib.parse
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Literal, TypeAlias, cast

from . import _errors, _protocol
from ._sentinel import Unset as _Unset
from ._transport import TimeoutConfig
from ._version import PACKAGE_VERSION

CompatibilityWarningCode: TypeAlias = Literal[
    'deprecated-entity-types-alias',
    'deprecated-schema-alias',
    'legacy-infinite-timeout',
]
EventRoute: TypeAlias = Literal[
    'allocation',
    'download',
    'finalize',
    'link',
    'object_put',
    'part_put',
    'read',
    'rpc',
]
ObserverEvent: TypeAlias = (
    'CompatibilityWarningEvent | ProgressEvent | RequestEvent | ResponseEvent | RetryEvent'
)
ObserverCallback: TypeAlias = Callable[[ObserverEvent], None]

Outcome: TypeAlias = Literal['error', 'retry', 'success']

_LOG = logging.getLogger('shotgun_api3')

_ASCII_CONTROL_LIMIT = 32
_ASCII_DELETE = 127
_AUTH_FIELDS = (
    'api_key',
    'auth_token',
    'extra_auth_params',
    'script_name',
    'session_token',
    'session_uuid',
    'sudo_as_login',
    'user_login',
    'user_password',
)
_DEDICATED_AUTH_STRING_FIELDS = frozenset(
    {
        'api_key',
        'auth_token',
        'script_name',
        'session_token',
        'session_uuid',
        'sudo_as_login',
        'user_login',
        'user_password',
    }
)
_DEFAULT_CONNECT_TIMEOUT = 15.0
_DEFAULT_PROXY_PORT = 8080
_DEFAULT_READ_TIMEOUT = 300.0
_MAX_PORT = 65535
_OBSERVER_FIELDS = (
    'on_compatibility_warning',
    'on_progress',
    'on_request',
    'on_response',
    'on_retry',
)
_ORIGIN_FIELDS = frozenset({'api_path', 'scheme', 'server'})
_ORIGIN_STRING_FIELDS = frozenset({'scheme', 'server'})
_UNSET: Final = _Unset()
_WARNING_MESSAGES: Mapping[CompatibilityWarningCode, str] = {
    'deprecated-entity-types-alias': 'The entity_types alias is deprecated.',
    'deprecated-schema-alias': 'The schema alias is deprecated.',
    'legacy-infinite-timeout': 'Explicit None timeout enables legacy infinite waits.',
}
_PUBLIC_FIELDS = frozenset(
    {
        *_AUTH_FIELDS,
        *_OBSERVER_FIELDS,
        'api_path',
        'api_ver',
        'convert_datetimes_to_utc',
        'localized',
        'max_rpc_attempts',
        'page_prefetch',
        'pool_maxsize',
        'proxy_pass',
        'proxy_port',
        'proxy_server',
        'proxy_user',
        'raw_http_proxy',
        'rpc_attempt_interval',
        'scheme',
        'server',
        'server_info',
        'session_uuid',
        'timeout_secs',
        'user_agents',
    }
)
_TRANSPORT_FIELDS = frozenset(
    {
        *_ORIGIN_FIELDS,
        'pool_maxsize',
        'proxy_pass',
        'proxy_port',
        'proxy_server',
        'proxy_user',
        'raw_http_proxy',
    }
)


@dataclasses.dataclass(frozen=True)
class CompatibilityWarningEvent:
    """Report sanitized compatibility behavior through a stable code.

    Attributes:
        code: Stable compatibility warning code.
        message: Fixed user-facing warning text.
    """

    code: CompatibilityWarningCode
    message: str


@dataclasses.dataclass(frozen=True)
class ProgressEvent:
    """Report byte consumption for one attempt.

    Attributes:
        operation: Logical operation category.
        route: Transport route being measured.
        attempt: One-based attempt number.
        bytes_transferred: Bytes consumed so far.
        total_bytes: Expected total bytes, when known.
    """

    operation: _protocol.OperationCategory
    route: EventRoute
    attempt: int
    bytes_transferred: int
    total_bytes: int | None


@dataclasses.dataclass(frozen=True)
class RequestContext:
    """Capture every operation-visible setting before I/O begins.

    Attributes:
        canonical_origin: Normalized site origin, when configured.
        api_url: Complete RPC endpoint URL, when configured.
        auth: Frozen authentication payload.
        extra_auth_params: Frozen additional authentication values.
        user_agent: Complete request user-agent value.
        session_token: Captured session token, when configured.
        session_uuid: Captured session UUID, when configured.
        timeout: Effective transport timeout limits.
        localized: Whether localized display names are requested.
        convert_datetimes_to_utc: Whether datetime values are normalized to
            UTC.
        records_per_page: Effective server page size.
        page_prefetch: Maximum concurrent page requests; zero keeps paging
            serial.
        pool_maxsize: Maximum transport connection-pool size.
        proxy_url: Normalized proxy URL, when configured.
        proxy_headers: Frozen proxy authentication headers.
        max_rpc_attempts: Maximum number of RPC attempts.
        rpc_attempt_interval: Base retry interval in milliseconds.
        origin_generation: Captured origin-state generation.
        transport_generation: Captured transport-state generation.
        auth_generation: Captured authentication generation, when shared.
        has_one_use_auth_token: Whether the snapshot carries a one-use token.
        on_request: Optional request observer.
        on_response: Optional response observer.
        on_retry: Optional retry observer.
        on_progress: Optional transfer-progress observer.
        on_compatibility_warning: Optional compatibility-warning observer.
        site_url: Displayable site URL, when configured.
    """

    canonical_origin: str | None
    api_url: str | None
    auth: Mapping[str, object] = dataclasses.field(repr=False)
    extra_auth_params: Mapping[str, object] = dataclasses.field(repr=False)
    user_agent: str
    session_token: str | None = dataclasses.field(repr=False)
    session_uuid: str | None = dataclasses.field(repr=False)
    timeout: TimeoutConfig
    localized: bool
    convert_datetimes_to_utc: bool
    records_per_page: int
    page_prefetch: int
    pool_maxsize: int
    proxy_url: str | None
    proxy_headers: Mapping[str, str] = dataclasses.field(repr=False)
    max_rpc_attempts: int
    rpc_attempt_interval: int
    origin_generation: int
    transport_generation: int
    auth_generation: int | None
    has_one_use_auth_token: bool
    on_request: ObserverCallback | None
    on_response: ObserverCallback | None
    on_retry: ObserverCallback | None
    on_progress: ObserverCallback | None
    on_compatibility_warning: ObserverCallback | None
    site_url: str | None


class Config:
    """Provide official configuration names with coherent locked snapshots.

    Writable attributes are grouped as credentials and session (`api_key`,
    `auth_token`, `extra_auth_params`, `script_name`, `session_token`,
    `session_uuid`, `sudo_as_login`, `user_login`, and `user_password`), origin
    and API (`api_path`, `api_ver`, `scheme`, `server`, and `server_info`),
    proxy (`proxy_pass`, `proxy_port`, `proxy_server`, `proxy_user`, and
    `raw_http_proxy`), transport, retry, and pagination (`max_rpc_attempts`,
    `page_prefetch`, `pool_maxsize`, `rpc_attempt_interval`, `timeout_secs`,
    and `user_agents`), conversion and localization (`convert_datetimes_to_utc`
    and `localized`), and observers (`on_compatibility_warning`, `on_progress`,
    `on_request`, `on_response`, and `on_retry`).

    Reads return detached values except that observer callbacks and the unset
    sentinel retain their identities. Supported assignments are applied
    atomically; some combined settings are validated when a snapshot is taken.
    Changing the canonical origin clears omitted authentication and session
    values. Use `update_fields()` to supply an origin and replacement
    credentials atomically.

    For `timeout_secs`, the unset sentinel selects 15-second connect and
    300-second read limits, `None` disables both limits, and a finite positive
    number at most `threading.TIMEOUT_MAX` sets both limits.
    `rpc_attempt_interval` is in milliseconds. `page_prefetch=0` keeps paging
    serial; a positive N permits N concurrent page requests with at most N-1
    later pages fetched ahead. N=1 offers no processing lookahead.

    Observer callbacks run synchronously on the calling thread, a prefetch
    worker thread, or the async client's event loop. They are not awaited.
    Ordinary callback exceptions are suppressed with a sanitized warning.
    Callbacks must not reenter an operation that waits on their own in-progress
    readiness check.

    Attributes:
        records_per_page: Effective server page limit, defaulting to 500.
    """

    _lock: threading.Lock
    _auth_generation: int
    _origin_generation: int
    _transport_generation: int
    _values: dict[str, object]

    api_key: str | None
    api_path: str | None
    api_ver: str
    auth_token: str | None
    convert_datetimes_to_utc: bool
    extra_auth_params: dict[str, object] | None
    localized: bool
    max_rpc_attempts: int
    on_compatibility_warning: ObserverCallback | None
    on_progress: ObserverCallback | None
    on_request: ObserverCallback | None
    on_response: ObserverCallback | None
    on_retry: ObserverCallback | None
    page_prefetch: int
    pool_maxsize: int
    proxy_pass: str | None
    proxy_port: int
    proxy_server: str | None
    proxy_user: str | None
    raw_http_proxy: str | None
    rpc_attempt_interval: int
    scheme: str | None
    script_name: str | None
    server: str | None
    server_info: dict[str, object]
    session_token: str | None
    session_uuid: str | None
    sudo_as_login: str | None
    timeout_secs: float | _Unset | None
    user_agents: tuple[str, ...]
    user_login: str | None
    user_password: str | None

    def __init__(self) -> None:
        """Initialize official configuration defaults and synchronization state.

        Read `SHOTGUN_API_RETRY_INTERVAL` as an integer number of milliseconds,
        defaulting to 3000.

        Raises:
            ValueError: If the environment value is not a nonnegative integer.
        """
        object.__setattr__(self, '_lock', threading.Lock())
        object.__setattr__(self, '_auth_generation', 0)
        object.__setattr__(self, '_origin_generation', 0)
        object.__setattr__(self, '_transport_generation', 0)
        interval = int(os.environ.get('SHOTGUN_API_RETRY_INTERVAL', '3000'))
        if interval < 0:
            raise ValueError('rpc_attempt_interval must be >= 0')
        object.__setattr__(
            self,
            '_values',
            {
                'api_key': None,
                'api_path': None,
                'api_ver': 'api3',
                'auth_token': None,
                'convert_datetimes_to_utc': True,
                'extra_auth_params': None,
                'localized': False,
                'max_rpc_attempts': 3,
                'on_compatibility_warning': None,
                'on_progress': None,
                'on_request': None,
                'on_response': None,
                'on_retry': None,
                'page_prefetch': 0,
                'pool_maxsize': 10,
                'proxy_pass': None,
                'proxy_port': 8080,
                'proxy_server': None,
                'proxy_user': None,
                'raw_http_proxy': None,
                'rpc_attempt_interval': interval,
                'scheme': None,
                'script_name': None,
                'server': None,
                'server_info': {},
                'session_token': None,
                'session_uuid': None,
                'sudo_as_login': None,
                'timeout_secs': _UNSET,
                'user_agents': _protocol.default_user_agent(PACKAGE_VERSION),
                'user_login': None,
                'user_password': None,
            },
        )

    if not TYPE_CHECKING:

        def __getattr__(self, name: str) -> object:
            """Read configuration, preserving observer and unset-sentinel identities.

            Args:
                name: Supported configuration field name.

            Returns:
                The observer or unset sentinel itself, or a detached field
                value.

            Raises:
                AttributeError: If the field name is unsupported.
            """
            if name not in _PUBLIC_FIELDS:
                raise AttributeError(name)
            with self._lock:
                value = self._values[name]
            if name in _OBSERVER_FIELDS:
                return value
            return value if value is _UNSET else copy.deepcopy(value)

        def __setattr__(self, name: str, value: object) -> None:
            """Route direct official configuration writes through the lock."""
            if name.startswith('_'):
                object.__setattr__(self, name, value)
                return
            self.update_fields(**{name: value})

    def _advance_generations(
        self,
        *,
        auth_changed: bool,
        origin_changed: bool,
        retired_values: list[object],
        transport_changed: bool,
    ) -> None:
        """Advance locked state identities for the changed field groups."""
        if origin_changed:
            retired_values.append(self._values['server_info'])
            self._values['server_info'] = {}
            self._origin_generation += 1
        if transport_changed:
            self._transport_generation += 1
        if auth_changed:
            self._auth_generation += 1

    def append_user_agent(self, agent: str) -> None:
        """Append one user-agent entry under the configuration lock.

        Args:
            agent: User-agent fragment to append.

        Raises:
            TypeError: If `agent` is not a string or stored agents are
                malformed.
        """
        owned_agent = _owned_string(agent)
        previous_agents: object = None
        with self._lock:
            agents = self._values['user_agents']
            if type(agents) is not tuple:
                raise TypeError('user_agents must be a tuple')
            typed_agents = cast('tuple[str, ...]', agents)
            previous_agents = typed_agents
            self._values['user_agents'] = (*typed_agents, owned_agent)
        del previous_agents

    @property
    def records_per_page(self) -> int:
        """Return the server page limit, defaulting to 500.

        Raises:
            TypeError: If stored server metadata is not a mapping.
            ShotgunError: If the server metadata contains an invalid page
                limit.
        """
        with self._lock:
            server_info = self._values['server_info']
        if not isinstance(server_info, Mapping):
            raise TypeError('server_info must be a mapping')
        typed_server_info = cast('Mapping[str, object]', server_info)
        return server_page_limit(typed_server_info)

    def reset_user_agent(self) -> None:
        """Restore the official-compatible default user-agent entries."""
        previous_agents: object = None
        with self._lock:
            previous_agents = self._values['user_agents']
            self._values['user_agents'] = _protocol.default_user_agent(PACKAGE_VERSION)
        del previous_agents

    def update_fields(self, **fields: object) -> None:
        """Atomically update supported configuration fields.

        Changing the canonical scheme, host, or effective port clears
        origin-scoped authentication and session values omitted from this call.
        Supplying replacement credentials in the same call retains those
        replacements. An `api_path`-only change retains authentication.
        Setting an unconfigured or invalid scheme/server combination also
        retires omitted authentication.

        Args:
            **fields: Supported field names and replacement values.

        Raises:
            AttributeError: If a field name is not a supported configuration
                knob.
            TypeError: If a field value has an unsupported type.
            ValueError: If a retry or timeout setting is outside its supported
                range.
        """
        normalized_fields = {_owned_string(name): value for name, value in fields.items()}
        unknown = normalized_fields.keys() - _PUBLIC_FIELDS
        if unknown:
            raise AttributeError(next(iter(sorted(unknown))))
        copied = {
            name: _copy_for_storage(name, value) for name, value in normalized_fields.items()
        }
        _validate_assignment_values(copied)
        auth_changed = any(name in _AUTH_FIELDS for name in copied)
        origin_changed = any(name in _ORIGIN_FIELDS for name in copied)
        transport_changed = any(name in _TRANSPORT_FIELDS for name in copied)
        prepared = [(name, value, name in _OBSERVER_FIELDS) for name, value in copied.items()]
        for name, observer, is_observer in prepared:
            if not is_observer:
                continue
            if observer is not None and not callable(observer):
                raise TypeError(f'{name} must be callable or None')
        retired_values: list[object] = []
        with self._lock:
            old_canonical_origin = _canonical_origin_or_none(self._values)
            for name, value, is_observer in prepared:
                if not is_observer or self._values[name] is not value:
                    retired_values.append(self._values[name])
                    self._values[name] = value
            new_canonical_origin = _canonical_origin_or_none(self._values)
            origin_boundary_crossed = bool({'scheme', 'server'} & copied.keys()) and (
                old_canonical_origin is None
                or new_canonical_origin is None
                or old_canonical_origin != new_canonical_origin
            )
            if origin_boundary_crossed:
                _retire_omitted_auth(self._values, copied, retired_values)
                auth_changed = True
            self._advance_generations(
                auth_changed=auth_changed,
                origin_changed=origin_changed,
                retired_values=retired_values,
                transport_changed=transport_changed,
            )
        retired_values.clear()

    def set_server_params(self, base_url: str) -> None:
        """Atomically set the canonical server tuple from a base URL.

        Changing the scheme, normalized host, or effective port clears
        origin-scoped authentication and session values. A path-only change
        retains them. Use `update_fields()` when the new origin and replacement
        credentials must be supplied atomically.

        Args:
            base_url: HTTP or HTTPS site URL.

        Raises:
            ValueError: If `base_url` has an unsupported or invalid origin.
        """
        scheme, server, api_path = parse_server_params(base_url)
        self.update_fields(scheme=scheme, server=server, api_path=api_path)

    def set_session_uuid(self, session_uuid: str | None) -> None:
        """Set the session UUID through the locked mutation path.

        Args:
            session_uuid: Session UUID or `None` to clear it.
        """
        self.update_fields(session_uuid=session_uuid)


@dataclasses.dataclass(frozen=True)
class RequestEvent:
    """Report allowlisted metadata before one transport attempt.

    Attributes:
        operation: Logical operation category.
        route: Transport route selected for the attempt.
        http_method: HTTP method sent by the attempt.
        attempt: One-based attempt number.
    """

    operation: _protocol.OperationCategory
    route: EventRoute
    http_method: str
    attempt: int


@dataclasses.dataclass(frozen=True)
class ResponseEvent:
    """Report allowlisted metadata after one transport attempt.

    Attributes:
        operation: Logical operation category.
        route: Transport route used by the attempt.
        attempt: One-based attempt number.
        latency: Attempt duration in seconds.
        status: HTTP status, when a response was received.
        outcome: Stable attempt outcome category.
        correlation_id: Server correlation identifier, when supplied.
    """

    operation: _protocol.OperationCategory
    route: EventRoute
    attempt: int
    latency: float
    status: int | None
    outcome: Outcome
    correlation_id: str | None


@dataclasses.dataclass(frozen=True)
class RetryEvent:
    """Report a retry decision after attempt cleanup.

    Attributes:
        operation: Logical operation category.
        route: Transport route used by the failed attempt.
        attempt: One-based failed attempt number.
        delay: Delay before the next attempt in seconds.
        status: HTTP status, when a response was received.
    """

    operation: _protocol.OperationCategory
    route: EventRoute
    attempt: int
    delay: float
    status: int | None


@dataclasses.dataclass(frozen=True)
class _ObserverBoundary:
    """Suppress and sanitively log ordinary observer exceptions."""

    event_type: str

    def __enter__(self) -> None:
        """Enter the observer exception boundary."""

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> bool:
        """Suppress ordinary callback failures without formatting their values."""
        if exception_type is None or not issubclass(exception_type, Exception):
            return False
        _LOG.warning('observer callback failed for %s', self.event_type)
        return True


def _canonical_server(server: str, scheme: str) -> tuple[str, int]:
    """Return an IDNA host and effective port from a configured server."""
    parsed = urllib.parse.urlsplit(f'//{server}')
    if parsed.hostname is None or parsed.username is not None or parsed.password is not None:
        raise ValueError('server must contain a host without credentials')
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('server contains an invalid port') from exc
    host = parsed.hostname.encode('idna').decode('ascii').lower()
    return host, port if port is not None else (443 if scheme == 'https' else 80)


def _contains_ascii_control(value: str) -> bool:
    """Return whether a string contains an ASCII control character."""
    return any(
        ord(character) < _ASCII_CONTROL_LIMIT or ord(character) == _ASCII_DELETE
        for character in value
    )


def _effective_timeout(value: object) -> TimeoutConfig:
    """Resolve official timeout semantics from one locked value."""
    if value is _UNSET:
        return TimeoutConfig(connect=_DEFAULT_CONNECT_TIMEOUT, read=_DEFAULT_READ_TIMEOUT)
    if value is None:
        return TimeoutConfig(connect=None, read=None)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError('timeout_secs must be a number, None, or UNSET')
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError('timeout_secs must be finite and positive')
    if value <= 0 or value > threading.TIMEOUT_MAX:
        raise ValueError('timeout_secs must be positive and at most threading.TIMEOUT_MAX')
    return TimeoutConfig(connect=value, read=value)


def _freeze(value: object) -> object:
    """Recursively copy mutable values into immutable containers."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        return MappingProxyType({key: _freeze(item) for key, item in mapping.items()})
    if isinstance(value, list | tuple):
        sequence = cast('list[object] | tuple[object, ...]', value)
        return tuple(_freeze(item) for item in sequence)
    if isinstance(value, set | frozenset):
        members = cast('set[object] | frozenset[object]', value)
        return frozenset(_freeze(item) for item in members)
    return copy.deepcopy(value)


def _integer(value: object, name: str) -> int:
    """Validate one integer configuration value."""
    if not isinstance(value, int):
        raise TypeError(f'{name} must be an integer')
    return value


def _observer(values: Mapping[str, object], name: str) -> ObserverCallback | None:
    """Return one exact observer object from captured values."""
    return cast('ObserverCallback | None', values[name])


def _origin_components(values: Mapping[str, object]) -> tuple[str, str, int] | None:
    """Validate shared origin fields and return scheme, URL host, and effective port."""
    scheme_value = values['scheme']
    server_value = values['server']
    if scheme_value is None or server_value is None:
        return None
    if not isinstance(scheme_value, str) or scheme_value.lower() not in {'http', 'https'}:
        raise ValueError('scheme must be http or https')
    if not isinstance(server_value, str):
        raise TypeError('server must be a string')
    scheme = scheme_value.lower()
    host, port = _canonical_server(server_value, scheme)
    host_for_url = f'[{host}]' if ':' in host else host
    return scheme, host_for_url, port


def _canonical_origin(values: Mapping[str, object]) -> str | None:
    """Build a canonical effective-port origin without the API path."""
    components = _origin_components(values)
    if components is None:
        return None
    scheme, host_for_url, port = components
    return f'{scheme}://{host_for_url}:{port}'


def _canonical_origin_or_none(values: Mapping[str, object]) -> str | None:
    """Return a canonical origin, treating invalid state as unconfigured."""
    try:
        return _canonical_origin(values)
    except (TypeError, ValueError):
        return None


def _origin(values: Mapping[str, object]) -> tuple[str | None, str | None]:
    """Build a canonical effective-port origin and API URL."""
    api_path_value = values['api_path']
    canonical_origin = _canonical_origin(values)
    if canonical_origin is None or api_path_value is None:
        return None, None
    if not isinstance(api_path_value, str):
        raise TypeError('api_path must be a string')
    api_path = f'/{api_path_value.lstrip("/")}'
    return canonical_origin, f'{canonical_origin}{api_path}'


def _owned_plain_value(value: object) -> object:
    """Recursively normalize caller containers into owned built-in values."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        return {copy.deepcopy(key): _owned_plain_value(item) for key, item in mapping.items()}
    if isinstance(value, list):
        sequence = cast('list[object]', value)
        return [_owned_plain_value(item) for item in sequence]
    if isinstance(value, tuple):
        sequence = cast('tuple[object, ...]', value)
        return tuple(_owned_plain_value(item) for item in sequence)
    if isinstance(value, set):
        members = cast('set[object]', value)
        return {_owned_plain_value(item) for item in members}
    if isinstance(value, frozenset):
        members = cast('frozenset[object]', value)
        return frozenset(_owned_plain_value(item) for item in members)
    return copy.deepcopy(value)


def _owned_extra_auth(value: object) -> dict[str, object] | None:
    """Validate and normalize extra authentication into an owned plain dict."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError('extra_auth_params must be a dictionary or None')
    typed_value = cast('dict[object, object]', value)
    owned = _owned_plain_value(typed_value)
    if not isinstance(owned, dict):
        raise TypeError('extra_auth_params must be a dictionary or None')
    return cast('dict[str, object]', owned)


def _owned_string(value: str) -> str:
    """Normalize a string subclass into an exact owned built-in string."""
    owned = str.__str__(value)
    return owned if type(owned) is str else f'{owned}'


def _owned_optional_string(value: object) -> str | None:
    """Normalize an optional string into exact owned built-in state."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError('authentication values must be strings or None')
    return _owned_string(value)


def _auth_spec(values: Mapping[str, object]) -> _protocol.AuthSpec:
    """Build a copied authentication specification from captured values."""
    return _protocol.AuthSpec(
        script_name=_owned_optional_string(values['script_name']),
        api_key=_owned_optional_string(values['api_key']),
        user_login=_owned_optional_string(values['user_login']),
        user_password=_owned_optional_string(values['user_password']),
        session_token=_owned_optional_string(values['session_token']),
        auth_token=_owned_optional_string(values['auth_token']),
        sudo_as_login=_owned_optional_string(values['sudo_as_login']),
        session_uuid=_owned_optional_string(values['session_uuid']),
        extra_auth_params=_owned_extra_auth(values['extra_auth_params']),
    )


def _owned_server_info(value: object) -> dict[str, object]:
    """Normalize server metadata into a detached dict with string keys."""
    if not isinstance(value, Mapping):
        raise TypeError('server_info must be a mapping')
    mapping = cast('Mapping[object, object]', value)
    owned: dict[str, object] = {}
    for key, item in mapping.items():
        if not isinstance(key, str):
            raise TypeError('server_info keys must be strings')
        owned[_owned_string(key)] = _owned_plain_value(item)
    return owned


def _owned_user_agents(value: object) -> tuple[str, ...]:
    """Normalize user-agent entries into an exact built-in tuple of strings."""
    if not isinstance(value, tuple):
        raise TypeError('user_agents must be a tuple')
    entries = cast('tuple[object, ...]', value)
    if not all(isinstance(entry, str) for entry in entries):
        raise TypeError('user_agents entries must be strings')
    typed_entries = cast('tuple[str, ...]', entries)
    return tuple(_owned_string(entry) for entry in typed_entries)


def _copy_for_storage(name: str, value: object) -> object:
    """Copy mutable caller-owned configuration before storing it."""
    if name in _OBSERVER_FIELDS:
        return value
    if name in _DEDICATED_AUTH_STRING_FIELDS:
        return _owned_optional_string(value)
    if name == 'extra_auth_params':
        return _owned_extra_auth(value)
    if name in _ORIGIN_STRING_FIELDS:
        copied_origin = value if isinstance(value, str) else copy.deepcopy(value)
        return _owned_string(copied_origin) if isinstance(copied_origin, str) else copied_origin
    if name == 'user_agents':
        copied = _owned_user_agents(value)
    elif name == 'server_info':
        copied = _owned_server_info(value)
    else:
        copied = value if name == 'timeout_secs' and value is _UNSET else copy.deepcopy(value)
    return copied


def _proxy_headers(values: Mapping[str, object]) -> Mapping[str, str]:
    """Build immutable Basic proxy authorization from captured credentials."""
    user = _owned_optional_string(values['proxy_user'])
    password = _owned_optional_string(values['proxy_pass'])
    if user is None and password is None:
        return MappingProxyType({})
    if user is None or password is None:
        raise ValueError('proxy_user and proxy_pass must be configured together')
    try:
        credentials = f'{user}:{password}'.encode('latin-1')
    except UnicodeEncodeError:
        raise ValueError('proxy credentials must contain only ISO-8859-1 characters') from None
    token = base64.b64encode(credentials).decode('ascii')
    return MappingProxyType({'proxy-authorization': f'Basic {token}'})


def _proxy_url(values: Mapping[str, object]) -> str | None:
    """Build a credential-free proxy URL from one captured configuration."""
    server = _owned_optional_string(values['proxy_server'])
    if server is None:
        return None
    port = _integer(values['proxy_port'], 'proxy_port')
    if not 1 <= port <= _MAX_PORT:
        raise ValueError('proxy_port must be between 1 and 65535')
    return f'http://{server}:{port}'


def _retire_omitted_auth(
    values: dict[str, object],
    copied: Mapping[str, object],
    retired_values: list[object],
) -> None:
    """Retire origin-scoped values absent from one atomic update."""
    for name in _AUTH_FIELDS:
        if name in copied:
            continue
        retired_values.append(values[name])
        values[name] = {} if name == 'extra_auth_params' else None


def _site_url(values: Mapping[str, object]) -> str | None:
    """Derive the configured site spelling from captured origin values."""
    components = _origin_components(values)
    if components is None:
        return None
    scheme, host_for_url, port = components
    server_value = values['server']
    configured_port = urllib.parse.urlsplit(f'//{server_value}').port
    server = f'{host_for_url}:{port}' if configured_port is not None else host_for_url
    base = f'{scheme}://{server}'
    api_path = str(values['api_path'])
    suffix = '/api3/json'
    if api_path.endswith(suffix):
        prefix = api_path[: -len(suffix)].rstrip('/')
        return f'{base}/{prefix.lstrip("/")}' if prefix else base
    return base


def _split_proxy_authority(
    http_proxy: str,
) -> tuple[str, str | None, str | None]:
    """Validate proxy credentials and return the remaining authority."""
    if _contains_ascii_control(http_proxy):
        raise ValueError('http_proxy must not contain ASCII control characters')
    credentials, marker, authority = http_proxy.rpartition('@')
    if not marker:
        return http_proxy, None, None
    user, separator, password = credentials.partition(':')
    if not separator or not user or not password:
        raise ValueError('http_proxy must be [user:pass@]host[:port]')
    return authority, user, password


def _parse_proxy_settings(
    http_proxy: str,
) -> tuple[str, int, str | None, str | None]:
    """Validate and split an official proxy authority."""
    authority, user, password = _split_proxy_authority(http_proxy)
    if not authority or authority.endswith(':'):
        raise ValueError('http_proxy must be [user:pass@]host[:port]')
    try:
        parsed = urllib.parse.urlsplit(f'//{authority}')
        port = parsed.port
    except (UnicodeError, ValueError):
        raise ValueError('http_proxy must be [user:pass@]host[:port]') from None
    hostname = parsed.hostname
    if (
        hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or any(character.isspace() for character in hostname)
    ):
        raise ValueError('http_proxy must be [user:pass@]host[:port]')
    effective_port = port if port is not None else _DEFAULT_PROXY_PORT
    if not 1 <= effective_port <= _MAX_PORT:
        raise ValueError('http_proxy must use a port from 1 to 65535')
    if ':' in hostname:
        server = f'[{hostname.lower()}]'
    else:
        try:
            server = hostname.encode('idna').decode('ascii').lower()
        except UnicodeError:
            raise ValueError('http_proxy contains an invalid host') from None
        if any(not (character.isalnum() or character in '.-') for character in server):
            raise ValueError('http_proxy contains an invalid host')
    return server, effective_port, user, password


def _parse_http_proxy(http_proxy: str) -> dict[str, object]:
    """Parse an official proxy authority into configuration fields."""
    server, port, user, password = _parse_proxy_settings(http_proxy)
    return {
        'proxy_pass': password,
        'proxy_port': port,
        'proxy_server': server,
        'proxy_user': user,
        'raw_http_proxy': http_proxy,
    }


def _thaw(value: object) -> object:
    """Convert immutable snapshot containers into fresh JSON containers."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        return {key: _thaw(item) for key, item in mapping.items()}
    if isinstance(value, tuple):
        sequence = cast('tuple[object, ...]', value)
        return [_thaw(item) for item in sequence]
    if isinstance(value, frozenset):
        members = cast('frozenset[object]', value)
        return [_thaw(item) for item in members]
    return copy.deepcopy(value)


def _validated_retry_value(name: str, value: object) -> int:
    """Validate and return one retry configuration integer."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f'{name} must be an integer')
    minimum = 1 if name == 'max_rpc_attempts' else 0
    if value < minimum:
        raise ValueError(f'{name} must be at least {minimum}')
    return value


def _validate_assignment_values(copied: dict[str, object]) -> None:
    """Validate independent field values before committing an assignment batch."""
    for name in ('page_prefetch', 'pool_maxsize', 'proxy_port'):
        if name in copied:
            _integer(copied[name], name)
    if 'server_info' in copied and not isinstance(copied['server_info'], Mapping):
        raise TypeError('server_info must be a mapping')
    for name in ('max_rpc_attempts', 'rpc_attempt_interval'):
        if name in copied:
            copied[name] = _validated_retry_value(name, copied[name])
    if 'timeout_secs' in copied:
        _effective_timeout(copied['timeout_secs'])


def advance_transport_generation(config: Config) -> None:
    """Retire the transport identity captured by earlier snapshots.

    Args:
        config: Internal configuration whose transport identity is retiring.
    """
    lock = cast('threading.Lock', object.__getattribute__(config, '_lock'))
    with lock:
        generation = cast('int', object.__getattribute__(config, '_transport_generation'))
        object.__setattr__(config, '_transport_generation', generation + 1)


def auth_spec(config: Config) -> _protocol.AuthSpec:
    """Return a detached authentication specification.

    Args:
        config: Client configuration to capture.

    Returns:
        Authentication values captured under the configuration lock.
        Field bindings are frozen, but additional authentication parameters
        remain a mutable dictionary detached from this configuration.
    """
    with config._lock:
        values = dict(config._values)
    return _auth_spec(values)


def compatibility_warning_events(
    warning_codes: tuple[CompatibilityWarningCode, ...],
    timeout: TimeoutConfig,
) -> tuple[CompatibilityWarningEvent, ...]:
    """Build fixed-message compatibility warnings for one operation.

    Args:
        warning_codes: Sanitized compatibility notices from the client.
        timeout: Captured operation timeout semantics.

    Returns:
        Immutable compatibility-warning events in publication order.
    """
    codes = list(warning_codes)
    if timeout.connect is None and timeout.read is None:
        codes.append('legacy-infinite-timeout')
    return tuple(
        CompatibilityWarningEvent(code=code, message=_WARNING_MESSAGES[code]) for code in codes
    )


def constructor_http_proxy_fields(http_proxy: object) -> dict[str, object] | None:
    """Return configuration updates for one constructor proxy value.

    Args:
        http_proxy: Optional proxy constructor value.

    Returns:
        Proxy field updates, or `None` when no update is required.

    Raises:
        TypeError: If `http_proxy` is not a string or `None`.
        ValueError: If a non-empty proxy authority is malformed.
    """
    if http_proxy is None:
        return None
    if not isinstance(http_proxy, str):
        raise TypeError('http_proxy must be a string or None')
    owned_http_proxy = _owned_string(http_proxy)
    if str.__len__(owned_http_proxy) == 0:
        return {'raw_http_proxy': owned_http_proxy}
    return _parse_http_proxy(owned_http_proxy)


def event_route(operation: _protocol.OperationDescriptor) -> EventRoute:
    """Map an operation descriptor to an allowlisted observer route.

    Args:
        operation: Shared operation descriptor.

    Returns:
        Endpoint role or the common RPC route.
    """
    return operation.endpoint_role or 'rpc'


def notify_observer(observer: ObserverCallback | None, event: ObserverEvent) -> None:
    """Invoke an observer and suppress ordinary `Exception` failures.

    Process-control exceptions propagate. Logged warnings identify only the
    event type without formatting callback values.

    Args:
        observer: Optional synchronous event callback.
        event: Sanitized immutable event to publish.
    """
    if observer is None:
        return
    with _ObserverBoundary(type(event).__name__):
        observer(event)


def parse_server_params(base_url: str) -> tuple[str, str, str]:
    """Parse a base URL into normalized server parameters.

    Args:
        base_url: HTTP or HTTPS site URL.

    Returns:
        The normalized scheme, server authority, and API path.

    Raises:
        ValueError: If `base_url` has an unsupported or invalid origin.
    """
    parsed = urllib.parse.urlsplit(base_url)
    scheme = parsed.scheme.lower()
    if scheme not in {'http', 'https'}:
        raise ValueError('base_url must be http or https')
    if parsed.hostname is None or parsed.username is not None or parsed.password is not None:
        raise ValueError('base_url must contain a host without credentials')
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('base_url contains an invalid port') from exc
    host = parsed.hostname.encode('idna').decode('ascii').lower()
    host_for_server = f'[{host}]' if ':' in host else host
    server = f'{host_for_server}:{port}' if port is not None else host_for_server
    prefix = parsed.path.rstrip('/')
    api_path = f'{prefix}/api3/json' if prefix else '/api3/json'
    return scheme, server, api_path


def publish_server_info(
    config: Config,
    context: RequestContext,
    server_info: dict[str, object],
) -> bool:
    """Publish origin metadata only for its captured transport generation.

    Args:
        config: Client configuration to capture or update.
        context: Request state that produced the metadata.
        server_info: Validated server metadata to publish.

    Returns:
        Whether the metadata was published.

    Raises:
        TypeError: If `server_info` cannot be copied as a dictionary.
    """
    copied = _owned_plain_value(server_info)
    if not isinstance(copied, dict):
        raise TypeError('server_info must be a dictionary')
    typed_copied = cast('dict[str, object]', copied)
    previous_server_info: object = None
    with config._lock:
        if (
            context.origin_generation != config._origin_generation
            or context.transport_generation != config._transport_generation
        ):
            return False
        previous_server_info = config._values['server_info']
        config._values['server_info'] = typed_copied
    del previous_server_info
    return True


def publish_session_token(config: Config, context: RequestContext, session_token: str) -> bool:
    """Atomically publish a token for an unchanged captured request state.

    Publishing a one-use authentication exchange also clears the stored login,
    password, and authentication token, and removes `auth_token` from the extra
    authentication parameters.

    Args:
        config: Client configuration to capture or update.
        context: Request state that produced the token.
        session_token: Session token to publish.

    Returns:
        Whether the token was published. Returns `False` without changing
        configuration when the captured origin, transport, or authentication
        generation is stale.

    Raises:
        TypeError: If the token or captured authentication values are
            malformed.
    """
    owned_session_token = _owned_optional_string(session_token)
    if owned_session_token is None:
        raise TypeError('session_token must be a string')
    retained_extra_auth_params: dict[str, object] | None = None
    if context.has_one_use_auth_token:
        retained = _owned_plain_value(context.extra_auth_params)
        if not isinstance(retained, dict):
            raise TypeError('captured extra authentication must be a mapping')
        retained_extra_auth_params = cast('dict[str, object]', retained)
        retained_extra_auth_params.pop('auth_token', None)

    previous_auth_token: object = None
    previous_extra_auth_params: object = None
    previous_session_token: object = None
    previous_user_login: object = None
    previous_user_password: object = None
    with config._lock:
        if (
            context.origin_generation != config._origin_generation
            or context.transport_generation != config._transport_generation
            or context.auth_generation != config._auth_generation
        ):
            return False
        previous_session_token = config._values['session_token']
        config._values['session_token'] = owned_session_token
        if context.has_one_use_auth_token:
            previous_auth_token = config._values['auth_token']
            previous_extra_auth_params = config._values['extra_auth_params']
            previous_user_login = config._values['user_login']
            previous_user_password = config._values['user_password']
            config._values['auth_token'] = None
            config._values['user_login'] = None
            config._values['user_password'] = None
            config._values['extra_auth_params'] = retained_extra_auth_params
        config._auth_generation += 1
    del previous_auth_token
    del previous_extra_auth_params
    del previous_session_token
    del previous_user_login
    del previous_user_password
    return True


def readiness_identity(config: Config) -> tuple[str, int] | None:
    """Return the current readiness identity without full validation.

    Args:
        config: Client configuration to capture.

    Returns:
        Canonical origin and transport generation, or `None` when the origin is
        unconfigured or invalid.
    """
    with config._lock:
        try:
            origin = _canonical_origin(config._values)
        except (TypeError, ValueError):
            return None
        if origin is None:
            return None
        return origin, config._transport_generation


def readiness_key(context: RequestContext) -> tuple[str, int]:
    """Return the capability and transport readiness identity.

    Args:
        context: Immutable configuration snapshot for one operation.

    Returns:
        Canonical origin and transport generation for the snapshot.

    Raises:
        ValueError: If server parameters have not been configured.
    """
    if context.canonical_origin is None:
        raise ValueError('server parameters must be configured before readiness')
    return context.canonical_origin, context.transport_generation


def require_secure_rpc(context: RequestContext, include_auth: bool) -> None:
    """Allow captured authentication over HTTP only for direct loopback connections.

    Args:
        context: Immutable request settings captured before operation I/O.
        include_auth: Whether the RPC would include captured authentication.

    Raises:
        ValueError: If authenticated HTTP uses a proxy or a non-loopback
            origin.
    """
    if not include_auth or not context.auth or context.canonical_origin is None:
        return
    parsed = urllib.parse.urlsplit(context.canonical_origin)
    if parsed.scheme != 'http':
        return
    host = parsed.hostname
    if context.proxy_url is None and host is not None:
        if host.casefold() == 'localhost':
            return
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if address.is_loopback and (
                isinstance(address, ipaddress.IPv4Address)
                or address == ipaddress.IPv6Address('::1')
            ):
                return
    raise ValueError('authenticated RPCs require HTTPS or a direct literal loopback HTTP origin')


def response_correlation_id(headers: Mapping[str, str]) -> str | None:
    """Extract only the allowlisted response correlation identifier.

    Args:
        headers: Read-only response headers.

    Returns:
        Correlation identifier when present.
    """
    return headers.get('x-sg-correlation-id')


def server_host(context: RequestContext) -> str:
    """Return the configured host spelling for capability metadata.

    Args:
        context: Immutable configuration snapshot for one operation.

    Returns:
        The host, including a non-default port when configured.

    Raises:
        ValueError: If the snapshot has no valid configured origin.
    """
    if context.canonical_origin is None:
        raise ValueError('server parameters must be configured before readiness')
    parsed = urllib.parse.urlsplit(context.canonical_origin)
    hostname = parsed.hostname
    if hostname is None:
        raise ValueError('configured origin must contain a host')
    host = f'[{hostname}]' if ':' in hostname else hostname
    port = parsed.port
    if port is not None and port != (443 if parsed.scheme == 'https' else 80):
        return f'{host}:{port}'
    return host


def server_page_limit(server_info: Mapping[str, object]) -> int:
    """Return the server page limit, defaulting to 500.

    Args:
        server_info: Server capability metadata.

    Returns:
        Configured page limit or the compatibility default.

    Raises:
        ShotgunError: If the server returned an invalid page limit.
    """
    value = server_info.get('api_max_entities_per_page', 500)
    if not value:
        return 500
    if isinstance(value, bool):
        raise _errors.ShotgunError('The server returned an invalid page limit.')
    if isinstance(value, int):
        limit = value
    elif isinstance(value, str) and value.isdecimal():
        try:
            limit = int(value)
        except ValueError:
            raise _errors.ShotgunError('The server returned an invalid page limit.') from None
    else:
        raise _errors.ShotgunError('The server returned an invalid page limit.')
    if limit <= 0:
        raise _errors.ShotgunError('The server returned an invalid page limit.')
    return limit


def snapshot(config: Config, auth_spec: _protocol.AuthSpec | None = None) -> RequestContext:
    """Capture one immutable coherent operation context.

    Args:
        config: Client configuration to capture or update.
        auth_spec: Optional operation-local authentication override.

    Returns:
        A frozen snapshot isolated from later configuration writes.

    Raises:
        TypeError: If stored configuration values have invalid types.
        ValueError: If stored URL, proxy, retry, or timeout values are invalid.
        ShotgunError: If stored server metadata contains an invalid page limit.
    """
    with config._lock:
        values = dict(config._values)
        auth_generation = config._auth_generation
        origin_generation = config._origin_generation
        transport_generation = config._transport_generation
    if auth_spec is None:
        copied_spec = _auth_spec(values)
    else:
        copied_spec = dataclasses.replace(
            auth_spec,
            session_uuid=(
                auth_spec.session_uuid
                if auth_spec.session_uuid is not None
                else _owned_optional_string(values['session_uuid'])
            ),
            extra_auth_params=_owned_extra_auth(auth_spec.extra_auth_params),
        )
    auth = _freeze(_protocol.build_auth_params(copied_spec))
    extra = _freeze(copied_spec.extra_auth_params or {})
    canonical_origin, api_url = _origin(values)
    server_info = values['server_info']
    if not isinstance(auth, Mapping) or not isinstance(extra, Mapping):
        raise TypeError('frozen authentication parameters must be mappings')
    if not isinstance(server_info, Mapping):
        raise TypeError('server_info must be a mapping')
    typed_auth = cast('Mapping[str, object]', auth)
    typed_extra = cast('Mapping[str, object]', extra)
    typed_server_info = cast('Mapping[str, object]', server_info)
    records_per_page = server_page_limit(typed_server_info)
    user_agents = values['user_agents']
    if not isinstance(user_agents, tuple):
        raise TypeError('user_agents must be a tuple')
    object_agents = cast('tuple[object, ...]', user_agents)
    if not all(isinstance(agent, str) for agent in object_agents):
        raise TypeError('user_agents entries must be strings')
    typed_user_agents = cast('tuple[str, ...]', user_agents)
    site_url = _site_url(values)
    return RequestContext(
        canonical_origin=canonical_origin,
        api_url=api_url,
        auth=typed_auth,
        extra_auth_params=typed_extra,
        user_agent='; '.join(typed_user_agents),
        session_token=copied_spec.session_token,
        session_uuid=copied_spec.session_uuid,
        timeout=_effective_timeout(values['timeout_secs']),
        localized=bool(values['localized']),
        convert_datetimes_to_utc=bool(values['convert_datetimes_to_utc']),
        records_per_page=records_per_page,
        page_prefetch=_integer(values['page_prefetch'], 'page_prefetch'),
        pool_maxsize=_integer(values['pool_maxsize'], 'pool_maxsize'),
        proxy_url=_proxy_url(values),
        proxy_headers=_proxy_headers(values),
        max_rpc_attempts=_integer(values['max_rpc_attempts'], 'max_rpc_attempts'),
        rpc_attempt_interval=_integer(
            values['rpc_attempt_interval'],
            'rpc_attempt_interval',
        ),
        origin_generation=origin_generation,
        transport_generation=transport_generation,
        auth_generation=auth_generation if auth_spec is None else None,
        has_one_use_auth_token=typed_auth.get('auth_token') is not None,
        on_request=_observer(values, 'on_request'),
        on_response=_observer(values, 'on_response'),
        on_retry=_observer(values, 'on_retry'),
        on_progress=_observer(values, 'on_progress'),
        on_compatibility_warning=_observer(values, 'on_compatibility_warning'),
        site_url=site_url,
    )


def thaw_mapping(value: Mapping[str, object]) -> dict[str, object]:
    """Thaw one immutable authentication mapping.

    Args:
        value: Recursively frozen authentication snapshot.

    Returns:
        A detached dictionary containing JSON-compatible built-in containers.

    Raises:
        TypeError: If the snapshot does not thaw to a dictionary.
    """
    thawed = _thaw(value)
    if not isinstance(thawed, dict):
        raise TypeError('authentication snapshot must thaw to a dictionary')
    return cast('dict[str, object]', thawed)
