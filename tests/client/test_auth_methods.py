import concurrent.futures
import inspect
import json
import threading
from collections.abc import Callable
from typing import TypeAlias, cast

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ObserverEvent, RequestEvent, ResponseEvent, RetryEvent
from shotgun_api3._transport import SendError, TransportResponse
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

_AuthenticateHumanUserCallable: TypeAlias = Callable[
    [str, str, str | None],
    dict[str, object] | None,
]


def _fault(code: int) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=json.dumps(
            {'exception': True, 'error_code': code, 'message': 'rejected'}
        ).encode(),
    )


def _params(sg: Shotgun, index: int = 0) -> dict[str, object]:
    params = cast('list[object]', _sent_payload(sg, index)['params'])
    return cast('dict[str, object]', params[1])


def _payload_auth(payload: dict[str, object]) -> dict[str, object]:
    params = cast('list[object]', payload['params'])
    return cast('dict[str, object]', params[0])


def _auth(sg: Shotgun, index: int = 0) -> dict[str, object]:
    return _payload_auth(_sent_payload(sg, index))


def _static_callable_contract(sg: Shotgun) -> _AuthenticateHumanUserCallable:
    return sg.authenticate_human_user


def test_authenticate_human_user_accepts_empty_documented_strings() -> None:
    sg = _fake_client([_make_response({'entities': [], 'paging_info': {'entity_count': 0}})])

    assert sg.authenticate_human_user('', '', '') is None

    assert _auth(sg) == {
        'user_login': '',
        'user_password': '',
        'auth_token': '',
    }
    assert _params(sg)['filters'] == {
        'logical_operator': 'and',
        'conditions': [
            {'path': 'sg_status_list', 'relation': 'is', 'values': ['act']},
            {'path': 'login', 'relation': 'is', 'values': ['']},
        ],
    }


@pytest.mark.parametrize(
    ('auth_token', 'operation_attempts', 'retry_events'),
    [(None, 2, 1), ('one-use', 1, 0)],
)
def test_authenticate_human_user_ambiguous_send_uses_logical_operation_policy(
    auth_token: str | None,
    operation_attempts: int,
    retry_events: int,
) -> None:
    events: list[ObserverEvent] = []
    response: dict[str, object] = {
        'entities': [],
        'paging_info': {'entity_count': 0},
    }
    sg = _fake_client([SendError('ambiguous'), _make_response(response)])
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    if auth_token is None:
        assert sg.authenticate_human_user('me', 'password') is None
    else:
        with pytest.raises(SendError, match='ambiguous'):
            sg.authenticate_human_user('me', 'password', auth_token)

    operation_events = [
        event
        for event in events
        if isinstance(event, (RequestEvent, ResponseEvent, RetryEvent))
        and event.operation == 'human_authentication'
    ]
    assert sum(isinstance(event, RequestEvent) for event in operation_events) == operation_attempts
    assert sum(isinstance(event, RetryEvent) for event in operation_events) == retry_events
    assert all(event.operation == 'human_authentication' for event in operation_events)
    assert len(_fake_transport(sg).requests) == 1 + operation_attempts


def test_authenticate_human_user_bad_credentials_returns_none() -> None:
    sg = _fake_client([_fault(102)])
    before = config_module.snapshot(sg.config)

    assert sg.authenticate_human_user('me', 'wrong-password') is None

    after = config_module.snapshot(sg.config)
    assert after.auth == before.auth
    assert after.auth_generation == before.auth_generation


def test_authenticate_human_user_builds_exact_read_payload_from_one_snapshot() -> None:
    sg = _fake_client([_make_response({'entities': [], 'paging_info': {'entity_count': 0}})])
    sg.config.session_uuid = 'captured-session'
    response_count = 0

    def change_session_after_readiness(event: ObserverEvent) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            sg.config.session_uuid = 'later-session'

    sg.config.on_response = change_session_after_readiness

    assert sg.authenticate_human_user('me', 'password', 'two-factor') is None

    payload = _sent_payload(sg)
    assert payload['method_name'] == 'read'
    assert _payload_auth(payload) == {
        'user_login': 'me',
        'user_password': 'password',
        'auth_token': 'two-factor',
        'session_uuid': 'captured-session',
    }
    assert _params(sg) == {
        'type': 'HumanUser',
        'return_fields': ['id', 'login'],
        'filters': {
            'logical_operator': 'and',
            'conditions': [
                {'path': 'sg_status_list', 'relation': 'is', 'values': ['act']},
                {'path': 'login', 'relation': 'is', 'values': ['me']},
            ],
        },
        'return_only': 'active',
        'paging': {'entities_per_page': 1, 'current_page': 1},
        'api_return_image_urls': True,
    }
    assert sg.config.session_uuid == 'later-session'
    assert sg.config.script_name == 's'
    assert sg.config.api_key == 'k'


def test_authenticate_human_user_excludes_configured_extra_auth_and_owns_config() -> None:
    sentinels = ['configured-extra-auth-secret']
    configured_extra: dict[str, object] = {
        'nested': {'sentinels': sentinels},
    }
    expected_extra = {
        'nested': {'sentinels': ['configured-extra-auth-secret']},
    }
    sg = _fake_client([_make_response({'entities': [], 'paging_info': {'entity_count': 0}})])
    sg.config.extra_auth_params = configured_extra
    sentinels.append('caller-mutation')
    before = sg.config.extra_auth_params

    assert sg.authenticate_human_user('me', 'password') is None

    assert _auth(sg) == {
        'user_login': 'me',
        'user_password': 'password',
    }
    after = sg.config.extra_auth_params
    assert before == expected_extra
    assert after == expected_extra
    assert after is not before
    assert sg.config.script_name == 's'
    assert sg.config.api_key == 'k'


def test_authenticate_human_user_exposes_exact_signature_and_static_contract() -> None:
    signature = inspect.signature(Shotgun.authenticate_human_user)

    assert tuple(signature.parameters) == (
        'self',
        'user_login',
        'user_password',
        'auth_token',
    )
    assert tuple(parameter.default for parameter in signature.parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in signature.parameters.values()
    )
    assert tuple(parameter.annotation for parameter in signature.parameters.values()) == (
        inspect.Parameter.empty,
        str,
        str,
        str | None,
    )
    assert signature.return_annotation == dict[str, object] | None

    _static_callable_contract(_fake_client([]))


def test_authenticate_human_user_forwards_and_owns_annotated_out_values() -> None:
    login = ['original-login']
    password = {'nested': ['original-password']}
    token = ['original-token']
    response: dict[str, object] = {
        'entities': [],
        'paging_info': {'entity_count': 0},
    }
    sg = _fake_client([_make_response(response)])

    def mutate_after_readiness(event: ObserverEvent) -> None:
        if isinstance(event, ResponseEvent):
            login[0] = 'changed-login'
            password['nested'][0] = 'changed-password'
            token[0] = 'changed-token'

    sg.config.on_response = mutate_after_readiness
    authenticate = cast('Callable[..., object]', sg.authenticate_human_user)

    assert authenticate(login, password, token) is None
    assert _auth(sg) == {
        'user_login': ['original-login'],
        'user_password': {'nested': ['original-password']},
        'auth_token': ['original-token'],
    }
    filters = cast('dict[str, object]', _params(sg)['filters'])
    conditions = cast('list[dict[str, object]]', filters['conditions'])
    assert conditions[1]['values'] == ['original-login']


def test_authenticate_human_user_keeps_concurrent_configured_auth_isolated() -> None:
    response: dict[str, object] = {
        'entities': [],
        'paging_info': {'entity_count': 0},
    }
    sg = _fake_client([_make_response(response), _make_response(response)])
    sg.connect()
    barrier = threading.Barrier(2)

    def synchronize_requests(event: ObserverEvent) -> None:
        if isinstance(event, RequestEvent):
            barrier.wait(timeout=5)

    sg.config.on_request = synchronize_requests

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        auth_future = executor.submit(
            sg.authenticate_human_user,
            'human-login',
            'human-password',
            'human-token',
        )
        configured_future = executor.submit(sg.preferences_read)
        assert auth_future.result(timeout=5) is None
        assert configured_future.result(timeout=5) == response

    payloads = [_sent_payload(sg, index) for index in range(2)]
    auth_payload = next(
        payload
        for payload in payloads
        if _payload_auth(payload).get('user_login') == 'human-login'
    )
    configured_payload = next(payload for payload in payloads if payload is not auth_payload)
    assert _payload_auth(auth_payload) == {
        'user_login': 'human-login',
        'user_password': 'human-password',
        'auth_token': 'human-token',
    }
    assert _payload_auth(configured_payload) == {
        'script_name': 's',
        'script_key': 'k',
    }
    assert sg.config.script_name == 's'
    assert sg.config.api_key == 'k'
    assert sg.config.user_login is None
    assert sg.config.user_password is None
    assert sg.config.auth_token is None


def test_authenticate_human_user_propagates_missing_two_factor_fault() -> None:
    sg = _fake_client([_fault(106)])

    with pytest.raises(_errors.MissingTwoFactorAuthenticationFault):
        sg.authenticate_human_user('me', 'password')


@pytest.mark.parametrize(
    ('code', 'fault_type'),
    [
        (999, _errors.Fault),
        (108, _errors.UserCredentialsNotAllowedForSSOAuthenticationFault),
        (110, _errors.UserCredentialsNotAllowedForOxygenAuthenticationFault),
    ],
)
def test_authenticate_human_user_propagates_other_faults(
    code: int,
    fault_type: type[_errors.Fault],
) -> None:
    sg = _fake_client([_fault(code)])

    with pytest.raises(fault_type) as error:
        sg.authenticate_human_user('login-secret', 'password-secret')

    assert str(error.value) == 'rejected'
    assert 'login-secret' not in str(error.value)
    assert 'password-secret' not in str(error.value)


def test_authenticate_human_user_returns_exact_first_call_boundary_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_entity = {'type': 'HumanUser', 'id': 7, 'login': 'me'}
    second_entity = {'type': 'HumanUser', 'id': 8, 'login': 'other'}

    def call(
        _self: Shotgun,
        _method_name: str,
        _params: object,
        **_kwargs: object,
    ) -> object:
        return {'entities': [first_entity, second_entity]}

    monkeypatch.setattr(Shotgun, '_call', call)
    sg = _fake_client([])

    result = sg.authenticate_human_user('me', 'password')

    assert result is first_entity


def test_authenticate_human_user_returns_none_when_no_user_matches() -> None:
    sg = _fake_client([_make_response({'entities': [], 'paging_info': {'entity_count': 0}})])

    assert sg.authenticate_human_user('missing', 'password') is None


def test_authenticate_human_user_secrets_stay_out_of_errors_events_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[ObserverEvent] = []
    sg = _fake_client([_make_response(['response-secret'])])
    sg.config.on_request = events.append
    sg.config.on_response = events.append

    with (
        caplog.at_level('WARNING', logger='shotgun_api3'),
        pytest.raises(_errors.ShotgunError) as error,
    ):
        sg.authenticate_human_user('login-secret', 'password-secret', 'token-secret')

    rendered = repr((error.value, events, caplog.text))
    for secret in (
        'login-secret',
        'password-secret',
        'token-secret',
        'response-secret',
    ):
        assert secret not in rendered


def test_authenticate_human_user_success_leaves_config_untouched() -> None:
    entity = {'type': 'HumanUser', 'id': 7, 'login': 'me'}
    sg = _fake_client([_make_response({'entities': [entity], 'paging_info': {'entity_count': 1}})])
    before = config_module.snapshot(sg.config)

    result = sg.authenticate_human_user('me', 'password-secret')

    assert result == entity
    assert _auth(sg) == {
        'user_login': 'me',
        'user_password': 'password-secret',
    }
    after = config_module.snapshot(sg.config)
    assert after.auth == before.auth
    assert after.auth_generation == before.auth_generation
    assert sg.config.script_name == 's'
    assert sg.config.api_key == 'k'
    assert sg.config.user_login is None
    assert sg.config.user_password is None
    assert sg.config.auth_token is None


@pytest.mark.parametrize(
    ('result', 'message'),
    [
        (['response-secret'], 'did not return a dictionary'),
        (
            {'entities': {'response-secret': 'container'}},
            'did not return an entities list',
        ),
        (
            {'entities': [{'id': 7}, 'response-secret-member']},
            'returned a non-dictionary entity',
        ),
    ],
)
def test_authenticate_human_user_validates_response_shape_without_values(
    result: object,
    message: str,
) -> None:
    sg = _fake_client([_make_response(result)])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^authenticate_human_user {message}$',
    ) as error:
        sg.authenticate_human_user('login-secret', 'password-secret')

    rendered = str(error.value)
    assert 'response-secret' not in rendered
    assert 'login-secret' not in rendered
    assert 'password-secret' not in rendered
