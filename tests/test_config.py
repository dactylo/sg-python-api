import ast
import base64
import copy
import dataclasses
import json
import sys
import threading
import traceback
import urllib.parse
from collections import UserDict
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Self, cast, get_args, get_type_hints

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3 import _protocol
from shotgun_api3._config import (
    CompatibilityWarningEvent,
    Config,
    ProgressEvent,
    RequestContext,
    RequestEvent,
    ResponseEvent,
    RetryEvent,
    _parse_http_proxy,
    constructor_http_proxy_fields,
    parse_server_params,
    readiness_key,
    server_host,
    server_page_limit,
)
from shotgun_api3._errors import ShotgunError
from tests._pyright import run_pyright

_AUTH_CREDENTIAL_REPR_VALUE = 'auth-token-epsilon-2236'
_AUTH_VALUE = 'value'
_EXTRA_AUTH_REPR_VALUE = 'extra-auth-eta-1732'
_NEW_AUTH_VALUE = 'new-token'
_NEW_CREDENTIAL = 'new-password'
_OLD_AUTH_VALUE = 'old-token'
_OLD_CREDENTIAL = 'old-password'
_ONE_USE_VALUE = 'one-use'
_ORIGIN_AUTH_VALUE = 'old-auth-token'
_ORIGIN_CREDENTIAL = 'old-password'
_ORIGIN_PROXY_CREDENTIAL = 'old-proxy-password'
_ORIGIN_SESSION_VALUE = 'old-session'
_PROXY_CREDENTIAL_REPR_VALUE = 'proxy-pass?#beta-3141'
_PROXY_USER_REPR_VALUE = 'proxy user/alpha-2718'
_PROXY_VALUE = 'p/a?s#s'
_SCRIPT_KEY_REPR_VALUE = 'script-key-gamma-1618'
_SESSION_CREDENTIAL_REPR_VALUE = 'session-token-zeta-1414'
_SESSION_UUID_REPR_VALUE = 'session-uuid-theta-2449'
_SESSION_VALUE = 'session-value'
_USER_CREDENTIAL_REPR_VALUE = 'user-password-delta-5772'


class _LockContainingObserver:
    def __init__(self) -> None:
        self.lock = threading.Lock()

    def __call__(self, _event: object) -> None:
        pass


class _ReentrantAuthMapping(Mapping[str, object]):
    def __init__(self, config: Config) -> None:
        self.config = config

    def __getitem__(self, _key: str) -> object:
        config_module.snapshot(self.config)
        raise KeyError

    def __iter__(self) -> Iterator[str]:
        return iter(())

    def __len__(self) -> int:
        return 0


@dataclasses.dataclass
class _ReentrantCopyState:
    calls: int = 0
    helpers: list[threading.Thread] = dataclasses.field(default_factory=list[threading.Thread])


class _ReentrantDestructorState:
    def __init__(self, config: Config) -> None:
        self.armed = False
        self.calls = 0
        self.config = config
        self.helpers: list[threading.Thread] = []
        self.under_lock = False

    def record(self) -> None:
        if not self.armed:
            return
        self.calls += 1
        completed = threading.Event()

        def reenter() -> None:
            self.config.update_fields(localized=True)
            completed.set()

        helper = threading.Thread(target=reenter, daemon=True)
        self.helpers.append(helper)
        helper.start()
        if not completed.wait(timeout=0.2):
            self.under_lock = True


class _ReentrantDestructorString(str):
    __slots__ = ('state',)

    state: _ReentrantDestructorState

    def __new__(
        cls,
        value: str,
        state: _ReentrantDestructorState,
    ) -> Self:
        instance = super().__new__(cls, value)
        instance.state = state
        return instance

    def __deepcopy__(self, _memo: dict[int, object]) -> Self:
        return type(self)(str(self), self.state)

    def __del__(self) -> None:
        self.state.record()


class _ReentrantDestructorValue:
    def __init__(self, state: _ReentrantDestructorState) -> None:
        self.state = state

    def __deepcopy__(self, _memo: dict[int, object]) -> '_ReentrantDestructorValue':
        return _ReentrantDestructorValue(self.state)

    def __del__(self) -> None:
        self.state.record()


class _ReentrantEqualityObserver:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.enabled = False
        self.equality_calls = 0

    def __call__(self, _event: object) -> None:
        pass

    def __deepcopy__(self, _memo: dict[int, object]) -> '_ReentrantEqualityObserver':
        return self

    def __eq__(self, _other: object) -> bool:
        if self.enabled:
            self.equality_calls += 1
            config_module.snapshot(self.config)
        return self is _other

    def __hash__(self) -> int:
        return id(self)


class _ReentrantExtraAuthDict(dict[str, object]):
    def __init__(
        self,
        config: Config,
        values: dict[str, object],
        state: _ReentrantCopyState | None = None,
    ) -> None:
        super().__init__(values)
        self.config = config
        self.state = state or _ReentrantCopyState()

    def __deepcopy__(self, memo: dict[int, object]) -> '_ReentrantExtraAuthDict':
        values = {
            copy.deepcopy(key, memo): copy.deepcopy(value, memo) for key, value in self.items()
        }
        return _ReentrantExtraAuthDict(self.config, values, self.state)

    def copy(self) -> dict[str, object]:
        self.state.calls += 1
        completed = threading.Event()

        def reenter() -> None:
            self.config.update_fields(localized=True)
            completed.set()

        helper = threading.Thread(target=reenter, daemon=True)
        self.state.helpers.append(helper)
        helper.start()
        if not completed.wait(timeout=0.2):
            raise RuntimeError('extra auth copy ran under the config lock')
        helper.join(timeout=1)
        return dict(self)

    @property
    def copy_calls(self) -> int:
        return self.state.calls

    @property
    def helpers(self) -> list[threading.Thread]:
        return self.state.helpers


class _ReentrantFieldName(str):
    __slots__ = ('state',)

    state: _ReentrantDestructorState

    def __new__(
        cls,
        value: str,
        state: _ReentrantDestructorState,
    ) -> Self:
        instance = super().__new__(cls, value)
        instance.state = state
        return instance

    def __eq__(self, value: object) -> bool:
        self.state.record()
        result = super().__eq__(value)
        return False if result is NotImplemented else result

    def __hash__(self) -> int:
        self.state.record()
        return super().__hash__()


class _ReentrantOriginState:
    def __init__(self, config: Config) -> None:
        self.calls = 0
        self.config = config
        self.helpers: list[threading.Thread] = []
        self.under_lock = False

    def record(self) -> None:
        if self.calls:
            return
        self.calls = 1
        completed = threading.Event()

        def reenter() -> None:
            self.config.update_fields(localized=True)
            completed.set()

        helper = threading.Thread(target=reenter, daemon=True)
        self.helpers.append(helper)
        helper.start()
        if not completed.wait(timeout=0.2):
            self.under_lock = True


class _ReentrantOriginString(str):
    __slots__ = ('state',)

    state: _ReentrantOriginState

    def __new__(
        cls,
        value: str,
        state: _ReentrantOriginState,
    ) -> Self:
        instance = super().__new__(cls, value)
        instance.state = state
        return instance

    def __deepcopy__(self, _memo: dict[int, object]) -> Self:
        return type(self)(str.__str__(self), self.state)

    def __format__(self, format_spec: str) -> str:
        self.state.record()
        return str.__format__(str.__str__(self), format_spec)

    def lower(self) -> str:
        self.state.record()
        return str.lower(str.__str__(self))


class _ReentrantOriginWrapper:
    def __init__(self, value: str, state: _ReentrantOriginState) -> None:
        self.state = state
        self.value = value

    def __deepcopy__(self, _memo: dict[int, object]) -> _ReentrantOriginString:
        return _ReentrantOriginString(self.value, self.state)


class _ReentrantUserAgentString(str):
    __slots__ = ('state',)

    state: _ReentrantDestructorState

    def __new__(
        cls,
        value: str,
        state: _ReentrantDestructorState,
    ) -> Self:
        instance = super().__new__(cls, value)
        instance.state = state
        return instance


class _ReentrantUserAgents(tuple[str, ...]):
    __slots__ = ()

    def __deepcopy__(self, _memo: dict[int, object]) -> Self:
        return self

    def __iter__(self) -> Iterator[str]:
        entries = cast('tuple[str, ...]', self)
        first = entries[0]
        assert isinstance(first, _ReentrantUserAgentString)
        first.state.record()
        return super().__iter__()


class _StatefulObserver:
    def __init__(self) -> None:
        self.copy_calls = 0

    def __call__(self, _event: object) -> None:
        pass

    def __deepcopy__(self, _memo: dict[int, object]) -> '_StatefulObserver':
        self.copy_calls += 1
        return _StatefulObserver()


class _ToggleCopyValue:
    def __init__(self, state: dict[str, bool], label: str) -> None:
        self.label = label
        self.state = state

    def __copy__(self) -> '_ToggleCopyValue':
        if self.state['raise']:
            raise RuntimeError('extra auth copy failed')
        return _ToggleCopyValue(self.state, self.label)

    def __deepcopy__(self, _memo: dict[int, object]) -> '_ToggleCopyValue':
        return copy.copy(self)


def _assert_context_repr_omits(context: RequestContext, secrets: tuple[str, ...]) -> None:
    if any(secret in repr(context) for secret in secrets):
        raise AssertionError('RequestContext repr exposed a secret sentinel')


def test_api_path_and_equivalent_origin_changes_preserve_authentication() -> None:
    config = Config()
    config.update_fields(
        api_key='key',
        api_path='/api3/json',
        scheme='https',
        script_name='script',
        server='x.example',
        session_uuid='uuid',
    )
    generation = config_module.snapshot(config).auth_generation

    config.api_path = '/alternate'
    config.server = 'X.EXAMPLE:443'

    assert config.api_key == 'key'
    assert config.script_name == 'script'
    assert config.session_uuid == 'uuid'
    assert config_module.snapshot(config).auth_generation == generation


def test_auth_override_snapshot_cannot_publish_shared_session_state() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_ONE_USE_VALUE,
        user_login='artist',
        user_password=_AUTH_VALUE,
    )

    context = config_module.snapshot(config, config_module.auth_spec(config))

    assert not config_module.publish_session_token(config, context, 'override-session')
    assert config.session_token is None
    assert config.auth_token == _ONE_USE_VALUE


def test_canonical_origin_change_clears_all_omitted_origin_authentication() -> None:
    config = Config()
    config.update_fields(
        api_key='old-key',
        api_path='/api3/json',
        auth_token=_ORIGIN_AUTH_VALUE,
        extra_auth_params={'claim': 'old'},
        scheme='https',
        script_name='old-script',
        server='old.example',
        proxy_pass=_ORIGIN_PROXY_CREDENTIAL,
        proxy_server='proxy.example',
        proxy_user='old-proxy-user',
        session_token=_ORIGIN_SESSION_VALUE,
        session_uuid='old-uuid',
        sudo_as_login='old-sudo',
        user_login='old-user',
        user_password=_ORIGIN_CREDENTIAL,
    )
    before = config_module.snapshot(config)

    config.set_server_params('https://new.example')

    after = config_module.snapshot(config)
    assert config.api_key is None
    assert config.auth_token is None
    assert config.extra_auth_params == {}
    assert config.script_name is None
    assert config.proxy_pass == _ORIGIN_PROXY_CREDENTIAL
    assert config.proxy_server == 'proxy.example'
    assert config.proxy_user == 'old-proxy-user'
    assert config.session_token is None
    assert config.session_uuid is None
    assert config.sudo_as_login is None
    assert config.user_login is None
    assert config.user_password is None
    assert before.auth_generation is not None
    assert after.auth_generation == before.auth_generation + 1


@pytest.mark.parametrize('field', ['page_prefetch', 'pool_maxsize', 'proxy_port', 'server_info'])
@pytest.mark.parametrize('value', [None, 'invalid', 1.5, []])
@pytest.mark.parametrize('mixed_update', [False, True])
def test_config_assignment_rejects_wrong_types_atomically(
    field: str,
    value: object,
    *,
    mixed_update: bool,
) -> None:
    config = Config()
    before = config_module.snapshot(config)
    previous = getattr(config, field)
    if mixed_update:
        with pytest.raises(TypeError, match=field):
            config.update_fields(
                scheme='https', server='other.example', api_key='replacement', **{field: value}
            )
    else:
        with pytest.raises(TypeError, match=field):
            setattr(config, field, value)
    assert getattr(config, field) == previous
    assert config_module.snapshot(config) == before


def test_config_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('SHOTGUN_API_RETRY_INTERVAL', raising=False)
    config = Config()

    assert config.max_rpc_attempts == 3
    assert config.rpc_attempt_interval == 3000
    assert config.timeout_secs is config_module._UNSET
    assert config.convert_datetimes_to_utc is True
    assert config.api_ver == 'api3'
    assert config.proxy_port == 8080
    assert config.localized is False
    assert config.page_prefetch == 0
    assert config.pool_maxsize == 10
    assert config.records_per_page == 500
    assert config.user_agents == (
        'shotgun-json (0.1.2)',
        f'Python {sys.version_info.major}.{sys.version_info.minor}',
        'sg-python-api',
    )


def test_config_module_keeps_assignments_in_preamble() -> None:
    source_path = config_module.__file__
    assert source_path is not None
    module = ast.parse(Path(source_path).read_text())
    definition_seen = False
    interstitial_lines: list[int] = []

    for statement in module.body:
        if isinstance(statement, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            definition_seen = True
        elif definition_seen and isinstance(statement, ast.Assign | ast.AnnAssign | ast.AugAssign):
            interstitial_lines.append(statement.lineno)

    assert interstitial_lines == []


def test_config_static_surface_rejects_invalid_fields(tmp_path: Path) -> None:
    probe = tmp_path / 'config_probe.py'
    probe.write_text(
        """from shotgun_api3._config import Config

config = Config()
config.page_prefetch = 'invalid'
config.pool_maxsize = 'invalid'
config.proxy_port = 'invalid'
config.server_info = []
config.timeout_secs = 'invalid'
config.page_preftech = 2
print(config.page_preftech)
""",
        encoding='utf-8',
    )
    result = run_pyright(probe, cwd=Path(__file__).parents[1])
    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    errors = [item for item in report['generalDiagnostics'] if item['severity'] == 'error']
    assert {item['range']['start']['line'] for item in errors} == set(range(3, 10)), result.stdout
    assert {item['rule'] for item in errors} == {'reportAttributeAccessIssue'}


def test_constructor_http_proxy_fields_normalizes_str_subclasses() -> None:
    class DeceptiveProxy(str):
        __slots__ = ()

        def __eq__(self, other: object) -> bool:
            return isinstance(other, str)

        def __hash__(self) -> int:
            return str.__hash__(self)

        def __len__(self) -> int:
            return 0

    fields = constructor_http_proxy_fields(DeceptiveProxy('proxy.example:3128'))

    assert fields is not None
    assert fields == {
        'proxy_pass': None,
        'proxy_port': 3128,
        'proxy_server': 'proxy.example',
        'proxy_user': None,
        'raw_http_proxy': 'proxy.example:3128',
    }
    assert type(fields['raw_http_proxy']) is str


def test_constructor_http_proxy_fields_preserves_exact_empty_value() -> None:
    assert constructor_http_proxy_fields('') == {'raw_http_proxy': ''}


def test_constructor_http_proxy_fields_rejects_falsy_non_string() -> None:
    with pytest.raises(TypeError):
        constructor_http_proxy_fields(0)


def test_constructor_http_proxy_fields_returns_no_update_for_none() -> None:
    assert constructor_http_proxy_fields(None) is None


@pytest.mark.parametrize(
    ('field_name', 'value'),
    [('scheme', 'HTTPS'), ('server', 'X.EXAMPLE:443')],
)
def test_copied_origin_strings_are_owned_before_locked_canonicalization(
    field_name: str,
    value: str,
) -> None:
    config = Config()
    config.set_server_params('https://x.example')
    state = _ReentrantOriginState(config)

    config.update_fields(**{field_name: _ReentrantOriginWrapper(value, state)})
    for helper in state.helpers:
        helper.join(timeout=1)

    assert state.calls == 0
    assert state.under_lock is False
    assert type(getattr(config, field_name)) is str


def test_default_timeout_uses_private_typed_singleton() -> None:
    sentinel = getattr(config_module, '_UNSET', None)
    first = Config()
    second = Config()

    assert sentinel is not None
    assert first.timeout_secs is sentinel
    assert second.timeout_secs is sentinel
    assert set(get_args(get_type_hints(Config)['timeout_secs'])) == {
        float,
        type(None),
        type(sentinel),
    }


@pytest.mark.parametrize('field', ['page_prefetch', 'pool_maxsize', 'proxy_port'])
@pytest.mark.parametrize('value', [True, False, -1, 0, 65536])
def test_integer_assignment_retains_existing_boolean_and_range_handling(
    field: str,
    value: int,
) -> None:
    config = Config()
    setattr(config, field, value)
    assert getattr(config, field) == value
    config_module.snapshot(config)
    if field == 'proxy_port' and not 1 <= value <= 65535:
        config.proxy_server = 'proxy.example'
        with pytest.raises(ValueError, match='proxy_port'):
            config_module.snapshot(config)


def test_invalid_origin_transition_clears_authentication_until_atomic_replacement() -> None:
    config = Config()
    config.update_fields(
        api_key='old-key',
        api_path='/api3/json',
        scheme='https',
        script_name='old-script',
        server='old.example',
    )

    config.scheme = 'ftp'

    assert config.api_key is None
    assert config.script_name is None
    config.update_fields(
        api_key='new-key',
        scheme='https',
        script_name='new-script',
        server='new.example',
    )
    assert config_module.snapshot(config).auth == {
        'script_name': 'new-script',
        'script_key': 'new-key',
    }


def test_lock_containing_observer_is_stored_without_copying() -> None:
    config = Config()
    observer = _LockContainingObserver()

    config.on_request = observer

    assert config.on_request is observer
    assert config_module.snapshot(config).on_request is observer


def test_locked_mutators_have_next_snapshot_visibility() -> None:
    events: list[object] = []
    observer = events.append
    config = Config()

    config.append_user_agent('integration')
    config.set_session_uuid('request-session')
    config.update_fields(localized=True, on_request=observer)
    context = config_module.snapshot(config)

    assert context.user_agent.endswith('; integration')
    assert context.session_uuid == 'request-session'
    assert context.localized is True
    assert context.on_request is observer

    config.reset_user_agent()
    assert config_module.snapshot(config).user_agent == '; '.join(
        _protocol.default_user_agent('0.1.2')
    )


def test_multi_field_updates_and_snapshots_never_observe_mixed_credentials() -> None:
    config = Config()
    config.update_fields(script_name='old', api_key='old-key')
    start = threading.Event()
    observed: list[tuple[object, object]] = []

    def mutate() -> None:
        start.wait()
        for _ in range(500):
            config.update_fields(script_name='new', api_key='new-key')
            config.update_fields(script_name='old', api_key='old-key')

    thread = threading.Thread(target=mutate)
    thread.start()
    start.set()
    for _ in range(1000):
        auth = config_module.snapshot(config).auth
        observed.append((auth['script_name'], auth['script_key']))
    thread.join()

    assert set(observed) <= {('old', 'old-key'), ('new', 'new-key')}


def test_notify_event_hint_resolves_exact_event_union() -> None:
    expected_hint = (
        CompatibilityWarningEvent | ProgressEvent | RequestEvent | ResponseEvent | RetryEvent
    )

    assert get_type_hints(config_module.notify_observer)['event'] == expected_hint


def test_observer_change_detection_never_invokes_reentrant_equality() -> None:
    config = Config()
    observer = _ReentrantEqualityObserver(config)
    config.on_retry = observer
    observer.enabled = True

    update = threading.Thread(target=lambda: config.update_fields(on_retry=observer), daemon=True)
    update.start()
    update.join(timeout=1)

    assert not update.is_alive()
    assert observer.equality_calls == 0
    assert config_module.snapshot(config).on_retry is observer


def test_origin_change_atomically_clears_server_info() -> None:
    config = Config()
    config.set_server_params('https://old.example')
    config.server_info = {'api_max_entities_per_page': 123, 'version': [9, 0, 0]}

    config.set_server_params('https://new.example')

    assert config.server_info == {}
    assert config.records_per_page == 500
    current = config_module.snapshot(config)
    assert config_module.publish_server_info(config, current, {'api_max_entities_per_page': 321})
    assert config.records_per_page == 321


def test_origin_change_invalidates_generation_checked_token_publication() -> None:
    config = Config()
    config.set_server_params('https://old.example')
    config.update_fields(
        auth_token=_ONE_USE_VALUE,
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    old_context = config_module.snapshot(config)
    config.set_server_params('https://new.example')

    assert not config_module.publish_session_token(config, old_context, 'stale-token')
    assert config.session_token is None

    current_context = config_module.snapshot(config)
    assert config_module.publish_session_token(config, current_context, _SESSION_VALUE)
    assert config.session_token == _SESSION_VALUE
    assert config.auth_token is None
    assert config.user_login is None
    assert config.user_password is None


def test_origin_change_keeps_only_atomic_replacement_credentials() -> None:
    config = Config()
    config.update_fields(
        api_key='old-key',
        api_path='/api3/json',
        scheme='https',
        script_name='old-script',
        server='old.example',
        session_token=_ORIGIN_SESSION_VALUE,
        session_uuid='old-uuid',
    )

    config.update_fields(
        api_key='new-key',
        api_path='/custom',
        scheme='https',
        script_name='new-script',
        server='new.example:443',
    )

    assert config.api_key == 'new-key'
    assert config.script_name == 'new-script'
    assert config.session_token is None
    assert config.session_uuid is None
    assert config_module.snapshot(config).canonical_origin == 'https://new.example:443'


def test_origin_change_never_destroys_server_info_values_under_lock() -> None:
    config = Config()
    config.set_server_params('https://old.example')
    destructor_state = _ReentrantDestructorState(config)
    context = config_module.snapshot(config)
    assert config_module.publish_server_info(
        config,
        context,
        {
            'guarded': _ReentrantDestructorValue(destructor_state),
            'version': [9, 0, 0],
        },
    )
    destructor_state.armed = True

    config.set_server_params('https://new.example')
    for helper in destructor_state.helpers:
        helper.join(timeout=1)
    destructor_state.armed = False

    assert destructor_state.calls >= 1
    assert destructor_state.under_lock is False
    assert config.server_info == {}
    config.localized = False
    assert config.localized is False


def test_parse_http_proxy_rejects_empty_authority() -> None:
    with pytest.raises(ValueError, match='http_proxy'):
        _parse_http_proxy('')


def test_parse_http_proxy_returns_shared_config_fields() -> None:
    assert _parse_http_proxy('u:p@proxy.example:3128') == {
        'proxy_pass': 'p',
        'proxy_port': 3128,
        'proxy_server': 'proxy.example',
        'proxy_user': 'u',
        'raw_http_proxy': 'u:p@proxy.example:3128',
    }
    assert _parse_http_proxy('proxy.example')['proxy_port'] == 8080
    with pytest.raises(ValueError, match='http_proxy'):
        _parse_http_proxy('http://proxy.example/path')


def test_per_request_auth_override_never_mutates_shared_auth() -> None:
    config = Config()
    config.update_fields(script_name='shared', api_key='shared-key')
    override = _protocol.AuthSpec(
        script_name=None,
        api_key=None,
        user_login='person',
        user_password=_AUTH_VALUE,
        session_token=None,
        auth_token=None,
        sudo_as_login=None,
        session_uuid='override-session',
        extra_auth_params={'scope': 'one'},
    )

    overridden = config_module.snapshot(config, override)
    shared = config_module.snapshot(config)

    assert overridden.auth == {
        'user_login': 'person',
        'user_password': _AUTH_VALUE,
        'session_uuid': 'override-session',
        'scope': 'one',
    }
    assert overridden.session_uuid == 'override-session'
    assert shared.auth == {'script_name': 'shared', 'script_key': 'shared-key'}


def test_proxy_snapshot_separates_url_and_authorization() -> None:
    config = Config()
    config.update_fields(
        proxy_pass=_PROXY_VALUE,
        proxy_port=3128,
        proxy_server='proxy.local',
        proxy_user='artist name',
        raw_http_proxy='artist name:p/a?s#s@proxy.local:3128',
    )

    context = config_module.snapshot(config)

    assert config.raw_http_proxy == 'artist name:p/a?s#s@proxy.local:3128'
    assert context.proxy_url == 'http://proxy.local:3128'
    assert isinstance(context.proxy_headers, MappingProxyType)
    mutable_headers = cast('dict[str, str]', context.proxy_headers)
    with pytest.raises(TypeError):
        mutable_headers['proxy-authorization'] = 'replacement'


def test_readiness_identity_helpers_use_captured_origin() -> None:
    config = Config()
    config.set_server_params('https://[2001:db8::1]:8443')
    context = config_module.snapshot(config)

    assert readiness_key(context) == ('https://[2001:db8::1]:8443', 1)
    assert server_host(context) == '[2001:db8::1]:8443'


def test_readiness_identity_ignores_unrelated_invalid_configuration() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    origin, generation = readiness_key(config_module.snapshot(config))

    config.update_fields(proxy_user='unpaired-user')

    assert config_module.readiness_identity(config) == (origin, generation + 1)
    config.scheme = 'ftp'
    assert config_module.readiness_identity(config) is None


def test_records_per_page_uses_an_isolated_server_info_copy() -> None:
    server_info: dict[str, object] = {'api_max_entities_per_page': 123}
    config = Config()
    config.server_info = server_info
    server_info['api_max_entities_per_page'] = 999

    assert config.records_per_page == 123


def test_records_per_page_uses_only_the_live_capability_key() -> None:
    config = Config()
    config.server_info = {'records_per_page_limit': 123}

    assert config.records_per_page == 500
    assert config_module.snapshot(config).records_per_page == 500

    config.server_info = {'api_max_entities_per_page': 321}

    assert config.records_per_page == 321
    assert config_module.snapshot(config).records_per_page == 321


def test_request_context_observer_type_resolves_exact_event_union() -> None:
    observer_hint = next(
        member
        for member in get_args(get_type_hints(RequestContext)['on_request'])
        if member is not type(None)
    )
    parameters, return_type = get_args(observer_hint)

    assert return_type is None
    assert isinstance(parameters, list)
    typed_parameters = cast('list[object]', parameters)
    assert len(typed_parameters) == 1
    assert set(get_args(typed_parameters[0])) == {
        CompatibilityWarningEvent,
        ProgressEvent,
        RequestEvent,
        ResponseEvent,
        RetryEvent,
    }


def test_request_context_repr_omits_script_and_proxy_secrets() -> None:
    config = Config()
    config.update_fields(
        api_key=_SCRIPT_KEY_REPR_VALUE,
        extra_auth_params={'secret': _EXTRA_AUTH_REPR_VALUE},
        proxy_pass=_PROXY_CREDENTIAL_REPR_VALUE,
        proxy_port=3128,
        proxy_server='proxy.local',
        proxy_user=_PROXY_USER_REPR_VALUE,
        script_name='repr-script',
        session_uuid=_SESSION_UUID_REPR_VALUE,
    )

    context = config_module.snapshot(config)
    proxy_token = base64.b64encode(
        f'{_PROXY_USER_REPR_VALUE}:{_PROXY_CREDENTIAL_REPR_VALUE}'.encode('latin-1')
    ).decode()

    _assert_context_repr_omits(
        context,
        (
            _PROXY_USER_REPR_VALUE,
            _PROXY_CREDENTIAL_REPR_VALUE,
            urllib.parse.quote(_PROXY_USER_REPR_VALUE, safe=''),
            urllib.parse.quote(_PROXY_CREDENTIAL_REPR_VALUE, safe=''),
            proxy_token,
            _SCRIPT_KEY_REPR_VALUE,
            _EXTRA_AUTH_REPR_VALUE,
            _SESSION_UUID_REPR_VALUE,
        ),
    )


def test_request_context_repr_omits_session_token() -> None:
    context = config_module.snapshot(
        Config(),
        _protocol.AuthSpec(
            script_name=None,
            api_key=None,
            user_login=None,
            user_password=None,
            session_token=_SESSION_CREDENTIAL_REPR_VALUE,
            auth_token=None,
            sudo_as_login=None,
            session_uuid=None,
            extra_auth_params=None,
        ),
    )

    _assert_context_repr_omits(context, (_SESSION_CREDENTIAL_REPR_VALUE,))


def test_request_context_repr_omits_user_credentials() -> None:
    context = config_module.snapshot(
        Config(),
        _protocol.AuthSpec(
            script_name=None,
            api_key=None,
            user_login='repr-user',
            user_password=_USER_CREDENTIAL_REPR_VALUE,
            session_token=None,
            auth_token=_AUTH_CREDENTIAL_REPR_VALUE,
            sudo_as_login=None,
            session_uuid=None,
            extra_auth_params=None,
        ),
    )

    _assert_context_repr_omits(
        context,
        (_USER_CREDENTIAL_REPR_VALUE, _AUTH_CREDENTIAL_REPR_VALUE),
    )


@pytest.mark.parametrize('proxy_server', [None, 'proxy.example', 'localhost'])
def test_secure_rpc_allows_explicitly_unauthenticated_http(proxy_server: str | None) -> None:
    config = Config()
    config.set_server_params('http://remote.example')
    config.proxy_server = proxy_server
    config.update_fields(api_key='k', script_name='s')

    config_module.require_secure_rpc(config_module.snapshot(config), include_auth=False)


@pytest.mark.parametrize('proxy_server', [None, 'proxy.example', 'localhost'])
def test_secure_rpc_allows_http_with_empty_authentication(proxy_server: str | None) -> None:
    config = Config()
    config.set_server_params('http://remote.example')
    config.proxy_server = proxy_server

    config_module.require_secure_rpc(config_module.snapshot(config), include_auth=True)


@pytest.mark.parametrize(
    ('base_url', 'rejected'),
    [
        ('https://remote.example', False),
        ('http://localhost', False),
        ('http://127.0.0.1', False),
        ('http://[::1]', False),
        ('http://[::ffff:127.0.0.1]', True),
        ('http://remote.example', True),
        ('http://192.0.2.10', True),
        ('http://[2001:db8::10]', True),
    ],
)
@pytest.mark.parametrize('proxy_server', [None, 'proxy.example', 'localhost'])
def test_secure_rpc_allows_https_and_literal_loopback_only(
    base_url: str,
    rejected: bool,
    proxy_server: str | None,
) -> None:
    config = Config()
    config.set_server_params(base_url)
    config.proxy_server = proxy_server
    config.update_fields(api_key='k', script_name='s')
    context = config_module.snapshot(config)

    if rejected or (base_url.startswith('http:') and proxy_server is not None):
        with pytest.raises(
            ValueError,
            match=r'^authenticated RPCs require HTTPS or a direct literal loopback HTTP origin$',
        ):
            config_module.require_secure_rpc(context, include_auth=True)
    else:
        config_module.require_secure_rpc(context, include_auth=True)


@pytest.mark.parametrize(
    ('field_name', 'value'),
    [('scheme', 'HTTPS'), ('server', 'X.EXAMPLE:443')],
)
def test_origin_strings_are_owned_before_locked_canonicalization(
    field_name: str,
    value: str,
) -> None:
    config = Config()
    config.set_server_params('https://x.example')
    state = _ReentrantOriginState(config)

    config.update_fields(**{field_name: _ReentrantOriginString(value, state)})
    for helper in state.helpers:
        helper.join(timeout=1)

    assert state.calls == 0
    assert state.under_lock is False
    assert type(getattr(config, field_name)) is str


@pytest.mark.parametrize(
    ('base_url', 'expected_path'),
    [
        ('https://studio.example', '/api3/json'),
        ('https://studio.example/', '/api3/json'),
        ('https://studio.example/shotgun', '/shotgun/api3/json'),
        ('https://studio.example/shotgun/', '/shotgun/api3/json'),
    ],
)
def test_parse_server_params_preserves_path_prefix(
    base_url: str,
    expected_path: str,
) -> None:
    scheme, server, api_path = parse_server_params(base_url)
    assert scheme == 'https'
    assert server == 'studio.example'
    assert api_path == expected_path


@pytest.mark.parametrize(
    ('field', 'value', 'error'),
    [
        ('max_rpc_attempts', None, TypeError),
        ('max_rpc_attempts', True, TypeError),
        ('max_rpc_attempts', 1.0, TypeError),
        ('max_rpc_attempts', 0, ValueError),
        ('rpc_attempt_interval', None, TypeError),
        ('rpc_attempt_interval', False, TypeError),
        ('rpc_attempt_interval', 1.0, TypeError),
        ('rpc_attempt_interval', -1, ValueError),
    ],
)
def test_retry_assignment_rejects_invalid_values_atomically(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    config = Config()
    before = config_module.snapshot(config)
    with pytest.raises(error):
        config.update_fields(**{field: value, 'pool_maxsize': 99})
    assert config_module.snapshot(config) == before


def test_server_info_assignment_copies_nested_values_outside_lock() -> None:
    config = Config()
    state = _ReentrantDestructorState(config)
    state.armed = True

    class CopyProbe:
        def __deepcopy__(self, _memo: dict[int, object]) -> dict[str, bool]:
            state.record()
            return {'copied': True}

    config.update_fields(server_info=UserDict({'nested': CopyProbe()}))
    for helper in state.helpers:
        helper.join(timeout=1)

    assert state.calls == 1
    assert not state.under_lock
    assert config.localized is True
    assert config.server_info == {'nested': {'copied': True}}


def test_server_info_assignment_rejects_non_string_keys_atomically() -> None:
    config = Config()
    config.server_info = {'version': [9, 0]}
    before = config_module.snapshot(config)

    with pytest.raises(TypeError, match=r'server_info.*string'):
        config.server_info = cast('dict[str, object]', {1: 'invalid'})

    assert config.server_info == {'version': [9, 0]}

    with pytest.raises(TypeError, match=r'server_info.*string'):
        config.update_fields(server_info={1: 'invalid'}, pool_maxsize=99)

    assert config_module.snapshot(config) == before
    assert config.server_info == {'version': [9, 0]}


def test_server_info_assignment_retains_detached_mapping_support() -> None:
    config = Config()
    metadata: UserDict[str, object] = UserDict(
        {'api_max_entities_per_page': 123, 'version': [9, 0]}
    )
    config.update_fields(server_info=metadata)
    metadata['api_max_entities_per_page'] = 999
    cast('list[int]', metadata['version'])[0] = 7
    detached = config.server_info
    assert type(detached) is dict
    detached['api_max_entities_per_page'] = 456
    cast('list[int]', detached['version'])[1] = 2
    assert config_module.snapshot(config).records_per_page == 123
    assert config.server_info['version'] == [9, 0]


def test_server_info_publication_checks_origin_and_transport_generations() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    context = config_module.snapshot(config)
    config.pool_maxsize = 20

    assert not config_module.publish_server_info(
        config, context, {'api_max_entities_per_page': 123}
    )
    assert config.server_info == {}

    current = config_module.snapshot(config)
    assert config_module.publish_server_info(config, current, {'api_max_entities_per_page': 321})
    assert config.records_per_page == 321


def test_server_page_limit_derivation_preserves_raw_metadata() -> None:
    config = Config()
    config.server_info = {'api_max_entities_per_page': '250'}

    assert config.records_per_page == 250
    assert config_module.snapshot(config).records_per_page == 250
    assert config.server_info == {'api_max_entities_per_page': '250'}


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ({}, 500),
        ({'api_max_entities_per_page': None}, 500),
        ({'api_max_entities_per_page': False}, 500),
        ({'api_max_entities_per_page': 0}, 500),
        ({'api_max_entities_per_page': ''}, 500),
        ({'api_max_entities_per_page': 250}, 250),
        ({'api_max_entities_per_page': '250'}, 250),
    ],
)
def test_server_page_limit_derives_supported_values(
    raw: dict[str, object],
    expected: int,
) -> None:
    assert server_page_limit(raw) == expected


def test_server_page_limit_redacts_decimal_conversion_failure() -> None:
    with pytest.raises(
        ShotgunError,
        match=r'^The server returned an invalid page limit\.$',
    ) as caught:
        server_page_limit({'api_max_entities_per_page': '9' * 5000})

    rendered = ''.join(traceback.format_exception(caught.value))
    assert 'integer string conversion' not in rendered
    assert '5000 digits' not in rendered


@pytest.mark.parametrize(
    'value',
    [True, -1, '0', '-1', '1.5', 1.5, object()],
)
def test_server_page_limit_rejects_invalid_truthy_values(value: object) -> None:
    with pytest.raises(
        ShotgunError,
        match=r'^The server returned an invalid page limit\.$',
    ):
        server_page_limit({'api_max_entities_per_page': value})


def test_set_server_params_canonicalizes_origin_and_changes_generations() -> None:
    config = Config()
    initial = config_module.snapshot(config)

    config.set_server_params('HTTPS://B\N{LATIN SMALL LETTER U WITH DIAERESIS}CHER.Example/path')
    context = config_module.snapshot(config)

    assert config.scheme == 'https'
    assert config.server == 'xn--bcher-kva.example'
    assert config.api_path == '/path/api3/json'
    assert context.canonical_origin == 'https://xn--bcher-kva.example:443'
    assert context.api_url == 'https://xn--bcher-kva.example:443/path/api3/json'
    assert context.origin_generation == initial.origin_generation + 1
    assert context.transport_generation == initial.transport_generation + 1

    with pytest.raises(ValueError, match='http or https'):
        config.set_server_params('ftp://x.example')


def test_snapshot_captures_current_site_fallback_for_custom_api_path() -> None:
    config = Config()
    config.update_fields(
        api_path='/custom-rpc',
        scheme='https',
        server='studio.example',
    )

    assert config_module.snapshot(config).site_url == 'https://studio.example'


@pytest.mark.parametrize(
    ('base_url', 'expected'),
    [
        ('https://studio.example', 'https://studio.example'),
        ('https://studio.example:443', 'https://studio.example:443'),
        ('https://studio.example:8443', 'https://studio.example:8443'),
        ('https://studio.example/shotgun', 'https://studio.example/shotgun'),
        ('https://studio.example:443/shotgun/', 'https://studio.example:443/shotgun'),
    ],
)
def test_snapshot_captures_current_site_spelling(base_url: str, expected: str) -> None:
    config = Config()

    config.set_server_params(base_url)

    assert config_module.snapshot(config).site_url == expected


@pytest.mark.parametrize(
    ('server', 'expected'),
    [
        (
            'B\N{LATIN CAPITAL LETTER U WITH DIAERESIS}CHER.Example',
            'https://xn--bcher-kva.example/shotgun',
        ),
        (
            'B\N{LATIN CAPITAL LETTER U WITH DIAERESIS}CHER.Example:443',
            'https://xn--bcher-kva.example:443/shotgun',
        ),
        (
            'B\N{LATIN CAPITAL LETTER U WITH DIAERESIS}CHER.Example:8443',
            'https://xn--bcher-kva.example:8443/shotgun',
        ),
    ],
)
def test_snapshot_captures_normalized_current_site_from_controlled_update(
    server: str,
    expected: str,
) -> None:
    config = Config()
    config.update_fields(
        api_path='shotgun/api3/json',
        scheme='HTTPS',
        server=server,
    )

    assert config_module.snapshot(config).site_url == expected


def test_snapshot_copies_and_freezes_auth_and_extra_parameters() -> None:
    nested = {'claims': ['one']}
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        script_name='script',
        api_key='key',
        extra_auth_params=nested,
        session_uuid='session',
    )

    context = config_module.snapshot(config)
    nested['claims'].append('two')

    assert isinstance(context.auth, MappingProxyType)
    assert isinstance(context.extra_auth_params, MappingProxyType)
    assert context.auth == {
        'script_name': 'script',
        'script_key': 'key',
        'session_uuid': 'session',
        'claims': ('one',),
    }
    assert context.extra_auth_params == {'claims': ('one',)}
    mutable_auth = cast('dict[str, object]', context.auth)
    with pytest.raises(TypeError):
        mutable_auth['script_key'] = 'changed'


def test_stateful_observer_retains_identity_without_deepcopy() -> None:
    config = Config()
    observer = _StatefulObserver()

    config.update_fields(on_response=observer)

    assert observer.copy_calls == 0
    assert config.on_response is observer
    assert config_module.snapshot(config).on_response is observer


@pytest.mark.parametrize('value', [None, 0.5, 12, threading.TIMEOUT_MAX])
def test_timeout_assignment_accepts_supported_values(value: float | None) -> None:
    config = Config()
    config.update_fields(timeout_secs=value)
    assert config.timeout_secs == value


def test_timeout_assignment_preserves_unset_sentinel_in_atomic_update() -> None:
    sentinel = config_module._UNSET
    config = Config()
    config.update_fields(timeout_secs=12)

    config.update_fields(timeout_secs=sentinel, pool_maxsize=99)

    snapshot = config_module.snapshot(config)
    assert config.timeout_secs is sentinel
    assert config.pool_maxsize == 99
    assert snapshot.timeout.connect == 15.0
    assert snapshot.timeout.read == 300.0
    assert snapshot.pool_maxsize == 99


@pytest.mark.parametrize('value', [10**400, threading.TIMEOUT_MAX + 1])
@pytest.mark.parametrize('mixed_update', [False, True])
def test_timeout_assignment_rejects_values_above_platform_limit(
    value: float,
    *,
    mixed_update: bool,
) -> None:
    config = Config()
    before = config_module.snapshot(config)
    name = 'timeout_secs'
    if mixed_update:
        with pytest.raises(ValueError, match=name):
            config.update_fields(pool_maxsize=99, api_key='replacement', timeout_secs=value)
    else:
        with pytest.raises(ValueError, match=name):
            setattr(config, name, value)
    assert config_module.snapshot(config) == before
    assert config.timeout_secs is config_module._UNSET


@pytest.mark.parametrize('value', [0, -1, float('nan'), float('inf'), -float('inf')])
def test_timeout_assignment_rejects_nonpositive_or_nonfinite_values(
    value: float,
) -> None:
    config = Config()
    before = config_module.snapshot(config)
    with pytest.raises(ValueError, match='timeout_secs'):
        config.update_fields(timeout_secs=value, pool_maxsize=99)
    assert config_module.snapshot(config) == before


@pytest.mark.parametrize('value', [True, False, '15', object()])
def test_timeout_assignment_rejects_wrong_types(value: object) -> None:
    config = Config()
    before = config_module.snapshot(config)
    with pytest.raises(TypeError):
        config.update_fields(timeout_secs=value, pool_maxsize=99)
    assert config_module.snapshot(config) == before


def test_timeout_semantics_and_retry_interval_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config()
    assert (
        config_module.snapshot(config).timeout.connect,
        config_module.snapshot(config).timeout.read,
    ) == (
        15.0,
        300.0,
    )

    config.timeout_secs = None
    assert (
        config_module.snapshot(config).timeout.connect,
        config_module.snapshot(config).timeout.read,
    ) == (None, None)
    config.timeout_secs = 12
    assert (
        config_module.snapshot(config).timeout.connect,
        config_module.snapshot(config).timeout.read,
    ) == (12.0, 12.0)

    monkeypatch.setenv('SHOTGUN_API_RETRY_INTERVAL', '1500')
    assert Config().rpc_attempt_interval == 1500
    monkeypatch.setenv('SHOTGUN_API_RETRY_INTERVAL', '-1')
    with pytest.raises(ValueError, match='rpc_attempt_interval'):
        Config()


def test_token_publication_clears_one_use_auth_from_final_wire_mapping() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        extra_auth_params={'auth_token': _ONE_USE_VALUE, 'scope': 'session'},
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    context = config_module.snapshot(config)

    assert context.has_one_use_auth_token is True
    assert config_module.publish_session_token(config, context, _SESSION_VALUE)
    assert config.session_token == _SESSION_VALUE
    assert config.extra_auth_params == {'scope': 'session'}
    assert config.user_login is None
    assert config.user_password is None


def test_token_publication_copy_failure_is_atomic_and_lock_remains_usable() -> None:
    state = {'raise': False}
    guarded = _ToggleCopyValue(state, 'guarded')
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_ONE_USE_VALUE,
        extra_auth_params={
            'auth_token': _ONE_USE_VALUE,
            'guarded': guarded,
            'scope': 'session',
        },
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    context = config_module.snapshot(config)
    state['raise'] = True

    with pytest.raises(RuntimeError, match='extra auth copy failed'):
        config_module.publish_session_token(config, context, _SESSION_VALUE)

    assert config.session_token is None
    assert config.auth_token == _ONE_USE_VALUE
    assert config.user_login == 'artist'
    assert config.user_password == _AUTH_VALUE
    config.localized = True
    assert config.localized is True

    state['raise'] = False
    assert config_module.publish_session_token(config, context, _SESSION_VALUE)
    assert config.session_token == _SESSION_VALUE
    assert config.auth_token is None
    assert config.user_login is None
    assert config.user_password is None
    retained = config.extra_auth_params
    assert isinstance(retained, dict)
    assert set(retained) == {'guarded', 'scope'}
    retained_guard = retained['guarded']
    assert isinstance(retained_guard, _ToggleCopyValue)
    assert retained_guard.label == 'guarded'


def test_token_publication_does_not_call_request_mapping_under_config_lock() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_ONE_USE_VALUE,
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    context = dataclasses.replace(
        config_module.snapshot(config), auth=_ReentrantAuthMapping(config)
    )
    published: list[bool] = []
    publish = threading.Thread(
        target=lambda: published.append(
            config_module.publish_session_token(config, context, _SESSION_VALUE)
        ),
        daemon=True,
    )

    publish.start()
    publish.join(timeout=1)

    assert not publish.is_alive()
    assert published == [True]


def test_token_publication_never_copies_reentrant_dict_under_lock() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    extra_auth = _ReentrantExtraAuthDict(
        config,
        {'auth_token': _ONE_USE_VALUE, 'scope': 'session'},
    )
    config.update_fields(
        extra_auth_params=extra_auth,
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    context = config_module.snapshot(config)
    errors: list[Exception] = []

    def publish() -> None:
        try:
            assert config_module.publish_session_token(config, context, _SESSION_VALUE)
        except (AssertionError, RuntimeError) as exc:
            errors.append(exc)

    worker = threading.Thread(target=publish, daemon=True)
    worker.start()
    worker.join(timeout=1)
    for helper in extra_auth.helpers:
        helper.join(timeout=1)

    assert not worker.is_alive()
    assert errors == []
    assert extra_auth.copy_calls == 0
    assert config.session_token == _SESSION_VALUE
    assert config.auth_token is None
    assert config.user_login is None
    assert config.user_password is None
    retained = config.extra_auth_params
    assert type(retained) is dict
    assert retained == {'scope': 'session'}
    config.localized = False
    assert config.localized is False


def test_token_publication_owns_strings_and_never_destroys_credentials_under_lock() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    credential_state = _ReentrantDestructorState(config)
    published_state = _ReentrantDestructorState(config)
    config.update_fields(
        auth_token=_ReentrantDestructorString(_ONE_USE_VALUE, credential_state),
        user_login=_ReentrantDestructorString('artist', credential_state),
        user_password=_ReentrantDestructorString(_AUTH_VALUE, credential_state),
    )
    assert type(config.auth_token) is str
    assert type(config.user_login) is str
    assert type(config.user_password) is str
    context = config_module.snapshot(config)
    credential_state.armed = True

    assert config_module.publish_session_token(
        config,
        context,
        _ReentrantDestructorString(_SESSION_VALUE, published_state),
    )
    for helper in credential_state.helpers:
        helper.join(timeout=1)
    credential_state.armed = False

    assert credential_state.under_lock is False
    assert type(config.session_token) is str
    assert config.session_token == _SESSION_VALUE
    assert config.auth_token is None
    assert config.user_login is None
    assert config.user_password is None
    config.localized = False
    assert config.localized is False


def test_token_publication_preserves_newer_authentication_state() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_OLD_AUTH_VALUE,
        user_login='old-user',
        user_password=_OLD_CREDENTIAL,
    )
    context = config_module.snapshot(config)
    config.update_fields(
        auth_token=_NEW_AUTH_VALUE,
        user_login='new-user',
        user_password=_NEW_CREDENTIAL,
    )

    assert not config_module.publish_session_token(config, context, 'stale-session')
    assert config.session_token is None
    assert config.auth_token == _NEW_AUTH_VALUE
    assert config.user_login == 'new-user'
    assert config.user_password == _NEW_CREDENTIAL


def test_transport_change_invalidates_token_publication() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_ONE_USE_VALUE,
        user_login='artist',
        user_password=_AUTH_VALUE,
    )
    context = config_module.snapshot(config)
    config.pool_maxsize = 20

    assert not config_module.publish_session_token(config, context, 'stale-session')
    assert config.session_token is None
    assert config.auth_token == _ONE_USE_VALUE


def test_transport_configuration_changes_only_transport_generation() -> None:
    config = Config()
    before = config_module.snapshot(config)

    config.update_fields(proxy_server='proxy.example', pool_maxsize=20)
    after = config_module.snapshot(config)

    assert after.origin_generation == before.origin_generation
    assert after.transport_generation == before.transport_generation + 1


def test_update_fields_never_hashes_or_compares_caller_field_names_under_lock() -> None:
    config = Config()
    state = _ReentrantDestructorState(config)
    field_name = _ReentrantFieldName('localized', state)
    state.armed = True

    config.update_fields(**{field_name: True})
    for helper in state.helpers:
        helper.join(timeout=1)
    state.armed = False

    assert state.calls >= 1
    assert state.under_lock is False
    assert config.localized is True
    config.localized = False
    assert config.localized is False


def test_user_agents_are_owned_exact_builtins_before_locked_iteration() -> None:
    config = Config()
    state = _ReentrantDestructorState(config)
    first_agent = _ReentrantUserAgentString('first', state)
    state.armed = True
    config.user_agents = _ReentrantUserAgents((first_agent,))

    config.append_user_agent(_ReentrantUserAgentString('second', state))
    for helper in state.helpers:
        helper.join(timeout=1)
    state.armed = False

    assert state.calls >= 1
    assert state.under_lock is False
    agents = config.user_agents
    assert type(agents) is tuple
    typed_agents = cast('tuple[object, ...]', agents)
    assert typed_agents == ('first', 'second')
    assert all(type(agent) is str for agent in typed_agents)

    config.reset_user_agent()
    reset_agents = config.user_agents
    assert type(reset_agents) is tuple
    typed_reset_agents = cast('tuple[object, ...]', reset_agents)
    assert all(type(agent) is str for agent in typed_reset_agents)

    with pytest.raises(TypeError, match='user_agents must be a tuple'):
        config.update_fields(user_agents=['invalid'])
    with pytest.raises(TypeError, match='user_agents entries must be strings'):
        config.update_fields(user_agents=('valid', 1))
    assert config.user_agents == reset_agents
