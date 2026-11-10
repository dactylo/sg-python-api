import copy
import dataclasses
import datetime
import sys
import typing
from typing import TYPE_CHECKING, cast

import pytest

import shotgun_api3.shotgun as shotgun_module
from shotgun_api3 import _errors, _protocol
from shotgun_api3._transport import CaseInsensitiveHeaders

if TYPE_CHECKING:
    from collections.abc import Callable

_AUTH_VALUE = 'value'
_SCRIPT = _protocol.AuthSpec(
    script_name='s',
    api_key='k',
    user_login=None,
    user_password=None,
    session_token=None,
    auth_token=None,
    sudo_as_login=None,
    session_uuid=None,
    extra_auth_params=None,
)
_SESSION_VALUE = 'session'


def test_auth_spec_is_frozen() -> None:
    spec = _SCRIPT
    with pytest.raises(dataclasses.FrozenInstanceError):
        type(spec).__setattr__(spec, 'script_name', 'other')


def test_build_auth_params_appends_optional_and_extra_keys_in_order() -> None:
    extra_auth_params: dict[str, object] = {
        'script_key': 'override',
        'sudo_as_login': 'override-sudo',
        'custom': ['value'],
    }
    original_extra_auth_params = extra_auth_params.copy()
    spec = dataclasses.replace(
        _SCRIPT,
        sudo_as_login='artist',
        session_uuid='u-1',
        extra_auth_params=extra_auth_params,
    )

    params = _protocol.build_auth_params(spec)

    assert params == {
        'script_name': 's',
        'script_key': 'override',
        'sudo_as_login': 'override-sudo',
        'session_uuid': 'u-1',
        'custom': ['value'],
    }
    assert list(params) == [
        'script_name',
        'script_key',
        'sudo_as_login',
        'session_uuid',
        'custom',
    ]
    assert extra_auth_params == original_extra_auth_params
    assert spec.extra_auth_params is extra_auth_params


def test_build_auth_params_returns_independent_dictionaries() -> None:
    first = _protocol.build_auth_params(_SCRIPT)
    second = _protocol.build_auth_params(_SCRIPT)

    first['script_name'] = 'changed'

    assert second == {'script_name': 's', 'script_key': 'k'}
    assert _SCRIPT.script_name == 's'


def test_build_auth_params_uses_fixture_pinned_script_keys() -> None:
    params = _protocol.build_auth_params(_SCRIPT)

    assert params == {'script_name': 's', 'script_key': 'k'}
    assert list(params) == ['script_name', 'script_key']


def test_build_auth_params_uses_fixture_pinned_session_key() -> None:
    spec = dataclasses.replace(
        _SCRIPT,
        script_name=None,
        api_key=None,
        session_token=_SESSION_VALUE,
    )

    assert _protocol.build_auth_params(spec) == {'session_token': _SESSION_VALUE}


def test_build_auth_params_uses_fixture_pinned_user_keys() -> None:
    spec = dataclasses.replace(
        _SCRIPT,
        script_name=None,
        api_key=None,
        user_login='me',
        user_password=_AUTH_VALUE,
        auth_token=_AUTH_VALUE,
    )

    params = _protocol.build_auth_params(spec)

    assert params == {
        'user_login': 'me',
        'user_password': _AUTH_VALUE,
        'auth_token': _AUTH_VALUE,
    }
    assert list(params) == ['user_login', 'user_password', 'auth_token']
    assert _protocol.build_auth_params(dataclasses.replace(spec, auth_token=None)) == {
        'user_login': 'me',
        'user_password': _AUTH_VALUE,
    }


def test_build_payload_appends_auth_then_params() -> None:
    payload = _protocol.build_payload('read', {'type': 'Shot'}, {'script_name': 's'})

    assert payload == {
        'method_name': 'read',
        'params': [{'script_name': 's'}, {'type': 'Shot'}],
    }


def test_build_payload_auth_only_when_params_falsy() -> None:
    payload = _protocol.build_payload('schema_read', None, {'session_token': 'x'})

    assert payload == {
        'method_name': 'schema_read',
        'params': [{'session_token': 'x'}],
    }


def test_build_payload_info_has_empty_params() -> None:
    assert _protocol.build_payload('info', None, None) == {
        'method_name': 'info',
        'params': [],
    }


def test_check_http_status_accepts_success() -> None:
    class UndecodableBytes(bytes):
        def decode(self, encoding: str = 'utf-8', errors: str = 'strict') -> str:
            del encoding, errors
            raise AssertionError('successful status decoded the response body')

    _protocol.check_http_status(299, 'Success', 'x.example', {}, UndecodableBytes(b'ok'))


def test_check_http_status_includes_exact_403_body() -> None:
    headers = CaseInsensitiveHeaders({'Retry-After': '3'})

    with pytest.raises(_errors.ProtocolError) as error:
        _protocol.check_http_status(
            403,
            'Forbidden',
            'x.example',
            headers,
            b'access \xffdenied',
        )

    assert error.value.errmsg == 'Forbidden: access \ufffddenied'
    assert error.value.headers == {'Retry-After': '3'}


def test_check_http_status_raises_protocol_error() -> None:
    class UndecodableBytes(bytes):
        def decode(self, encoding: str = 'utf-8', errors: str = 'strict') -> str:
            del encoding, errors
            raise AssertionError('non-403 status decoded the response body')

    with pytest.raises(_errors.ProtocolError) as error:
        _protocol.check_http_status(
            502,
            'Bad Gateway',
            'x.example',
            {},
            UndecodableBytes(b'ignored'),
        )

    assert error.value.url == 'x.example'
    assert error.value.errcode == 502
    assert error.value.errmsg == 'Bad Gateway'


@pytest.mark.parametrize('content_type', ['application/json; charset=utf-8', 'text/javascript'])
def test_decode_response_decodes_json_content_types(content_type: str) -> None:
    body = b'{"results": {"id": 1}}'
    headers = CaseInsensitiveHeaders({'Content-Type': content_type})

    assert _protocol.decode_response(headers, body) == {'results': {'id': 1}}


def test_decode_response_defaults_to_json_without_content_type() -> None:
    assert _protocol.decode_response({}, b'{"results": 1}') == {'results': 1}


@pytest.mark.parametrize(
    'headers',
    [{}, CaseInsensitiveHeaders({'Content-Type': 'application/json'})],
)
def test_decode_response_returns_empty_text_for_empty_body(
    headers: CaseInsensitiveHeaders | dict[str, str],
) -> None:
    assert _protocol.decode_response(headers, b'') == ''


def test_decode_response_returns_non_json_text() -> None:
    headers = CaseInsensitiveHeaders({'Content-Type': 'text/plain; charset=utf-8'})

    assert _protocol.decode_response(headers, 'caf\u00e9'.encode()) == 'caf\u00e9'


def test_default_user_agent_is_the_single_official_builder() -> None:
    assert _protocol.default_user_agent('0.1.0') == (
        'shotgun-json (0.1.0)',
        f'Python {sys.version_info.major}.{sys.version_info.minor}',
        'sg-python-api',
    )


def test_dict_filters_value_must_be_a_list_or_tuple() -> None:
    with pytest.raises(_errors.Fault):
        _protocol.translate_filters({'filter_operator': 'all', 'filters': 'not-a-list'})


def test_dict_syntax_compiles_recursively_without_mutating_input() -> None:
    filters: dict[str, object] = {
        'filter_operator': 'any',
        'filters': [
            ['code', 'is', 'A'],
            {'filter_operator': 'all', 'filters': [['id', 'greater_than', 5]]},
        ],
    }
    original = copy.deepcopy(filters)

    result = _protocol.translate_filters(filters)

    assert result == {
        'logical_operator': 'or',
        'conditions': [
            {'path': 'code', 'relation': 'is', 'values': ['A']},
            {
                'logical_operator': 'and',
                'conditions': [{'path': 'id', 'relation': 'greater_than', 'values': [5]}],
            },
        ],
    }
    assert filters == original


def test_encode_payload_is_utf8_json() -> None:
    assert _protocol.encode_payload({'a': '\u00e9'}) == '{"a": "\u00e9"}'.encode()


def test_entity_optimization_can_be_disabled_per_call() -> None:
    entity = {'type': 'Project', 'id': 1, 'sg_status': 'x'}

    result = _protocol.translate_filters([['project', 'is', entity]], optimize=False)

    assert result['conditions'] == [{'path': 'project', 'relation': 'is', 'values': [entity]}]


def test_entity_optimization_runtime_flag_is_checked_on_every_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entity = {'type': 'Project', 'id': 1, 'sg_status': 'x'}
    filters = [['project', 'is', entity]]

    monkeypatch.setattr(shotgun_module, 'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION', True)
    disabled = _protocol.translate_filters(filters)
    direct_disabled = _protocol.optimize_entity(entity)
    monkeypatch.setattr(shotgun_module, 'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION', False)
    enabled = _protocol.translate_filters(filters)
    direct_enabled = _protocol.optimize_entity(entity)

    assert disabled['conditions'] == [{'path': 'project', 'relation': 'is', 'values': [entity]}]
    assert direct_disabled is entity
    assert enabled['conditions'] == [
        {'path': 'project', 'relation': 'is', 'values': [{'type': 'Project', 'id': 1}]}
    ]
    assert direct_enabled == {'type': 'Project', 'id': 1}


def test_extract_results_returns_original_response_when_results_are_absent() -> None:
    response = {'value': 1}

    assert _protocol.extract_results(response) is response


def test_extract_results_returns_present_value() -> None:
    assert _protocol.extract_results({'results': None}) is None


def test_inject_local_paths_handles_multiple_records_without_touching_unrelated_values() -> None:
    attachment_a: dict[str, object] = {
        'link_type': 'local',
        'local_path_linux': '/mnt/a.mov',
    }
    attachment_b: dict[str, object] = {
        'link_type': 'local',
        'local_path_linux': '/mnt/b.mov',
    }
    web: dict[str, object] = {
        'link_type': 'web',
        'local_path_linux': '/mnt/web.mov',
    }
    nested: dict[str, object] = {
        'attachment': {'link_type': 'local', 'local_path_linux': '/mnt/nested.mov'}
    }
    items: list[object] = [{'link_type': 'local', 'local_path_linux': '/mnt/list.mov'}]
    records: list[object] = [
        {
            'attachment': attachment_a,
            'web': web,
            'nested': nested,
            'items': items,
        },
        {'attachment': attachment_b},
        'not-a-record',
    ]
    original_web = copy.deepcopy(web)
    original_nested = copy.deepcopy(nested)
    original_items = copy.deepcopy(items)

    _protocol.inject_local_paths(records, 'local_path_linux')

    assert attachment_a['url'] == 'file:///mnt/a.mov'
    assert attachment_b['url'] == 'file:///mnt/b.mov'
    assert web == original_web
    assert nested == original_nested
    assert items == original_items
    assert records[2] == 'not-a-record'


def test_inject_local_paths_ignores_local_links_missing_the_platform_field() -> None:
    records: list[object] = [{'attachment': {'link_type': 'local', 'name': 'x.mov'}}]
    original = copy.deepcopy(records)

    _protocol.inject_local_paths(records, 'local_path_linux')

    assert records == original


def test_inject_local_paths_mutates_and_returns_the_official_records_list() -> None:
    attachment: dict[str, object] = {
        'link_type': 'local',
        'local_path_linux': '/mnt/x.mov',
    }
    records: list[object] = [{'attachment': attachment}]

    out = _protocol.inject_local_paths(records, 'local_path_linux')

    assert out is records
    assert attachment == {
        'link_type': 'local',
        'local_path_linux': '/mnt/x.mov',
        'local_path': '/mnt/x.mov',
        'url': 'file:///mnt/x.mov',
    }


def test_inject_local_paths_none_field_returns_records_unchanged() -> None:
    records: list[object] = [
        {'attachment': {'link_type': 'local', 'local_path_linux': '/mnt/x.mov'}}
    ]
    original = copy.deepcopy(records)

    out = _protocol.inject_local_paths(records, None)

    assert out is records
    assert records == original


@pytest.mark.parametrize('local_path', ['', None])
def test_inject_local_paths_preserves_empty_and_none_paths(local_path: str | None) -> None:
    attachment: dict[str, object] = {
        'link_type': 'local',
        'local_path_linux': local_path,
    }
    records: list[object] = [{'attachment': attachment}]

    _protocol.inject_local_paths(records, 'local_path_linux')

    assert attachment['local_path'] is local_path
    assert attachment['url'] == 'file://'


def test_optimize_entity_keeps_only_fixture_pinned_keys_without_mutating_input() -> None:
    entity = {
        'type': 'Project',
        'id': 1,
        'name': 'P',
        'url': 'https://example.invalid/project/1',
        'content_type': 'application/json',
        'local_path': '/projects/p',
        'storage': {'type': 'LocalStorage', 'id': 2},
        'sg_status': 'Active',
    }
    original = copy.deepcopy(entity)

    result = _protocol.optimize_entity(entity)

    assert result == {
        'type': 'Project',
        'id': 1,
        'name': 'P',
        'url': 'https://example.invalid/project/1',
        'content_type': 'application/json',
        'local_path': '/projects/p',
        'storage': {'type': 'LocalStorage', 'id': 2},
    }
    assert result is not entity
    assert entity == original


def test_page_verdict_limit_and_empty_page_always_stop() -> None:
    assert _protocol.page_verdict({'entity_count': 900}, 500, 500, 500, 500) == 'stop'
    assert _protocol.page_verdict({'entity_count': 900}, 0, 0, 500, 0) == 'stop'
    assert _protocol.page_verdict({'has_next_page': True}, 0, 500, 500, 0) == 'stop'


@pytest.mark.parametrize(
    ('entities_count', 'records_count', 'per_page', 'limit', 'message'),
    [
        (True, 1, 500, 0, 'entities_count must be a nonnegative integer'),
        (-1, 1, 500, 0, 'entities_count must be a nonnegative integer'),
        (1, True, 500, 0, 'records_count must be a nonnegative integer'),
        (1, -1, 500, 0, 'records_count must be a nonnegative integer'),
        (1, 0, 500, 0, 'records_count must include entities_count'),
        (1, 1, True, 0, 'per_page must be a positive integer'),
        (1, 1, 0, 0, 'per_page must be a positive integer'),
        (1, 1, 500, True, 'limit must be a nonnegative integer'),
        (1, 1, 500, -1, 'limit must be a nonnegative integer'),
    ],
)
def test_page_verdict_rejects_invalid_counters(
    entities_count: object,
    records_count: object,
    per_page: object,
    limit: object,
    message: str,
) -> None:
    page_verdict = cast('Callable[..., object]', _protocol.page_verdict)

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        page_verdict({}, entities_count, records_count, per_page, limit)


@pytest.mark.parametrize(
    ('paging_info', 'message'),
    [
        (
            {'entity_count': True},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'entity_count': -1},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'entity_count': '900'},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'has_next_page': 1},
            'paging_info has_next_page must be a boolean',
        ),
        (
            {'has_next_page': 'false'},
            'paging_info has_next_page must be a boolean',
        ),
    ],
)
def test_page_verdict_rejects_malformed_active_metadata(
    paging_info: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        _protocol.page_verdict(paging_info, 500, 500, 500, 0)


def test_page_verdict_treats_none_entity_count_as_absent() -> None:
    assert _protocol.page_verdict({'entity_count': None}, 499, 499, 500, 0) == 'stop'
    assert _protocol.page_verdict({'entity_count': None}, 500, 500, 500, 0) == 'continue'
    assert (
        _protocol.page_verdict(
            {'entity_count': None, 'has_next_page': False},
            500,
            500,
            500,
            0,
        )
        == 'stop'
    )
    assert (
        _protocol.page_verdict(
            {'entity_count': None, 'has_next_page': True},
            499,
            499,
            500,
            0,
        )
        == 'continue'
    )


def test_page_verdict_uses_documented_exhaustion_precedence() -> None:
    assert _protocol.page_verdict({'entity_count': 502}, 2, 500, 500, 0) == 'continue'
    assert _protocol.page_verdict({'entity_count': 502}, 2, 502, 500, 0) == 'stop'
    assert (
        _protocol.page_verdict(
            {'entity_count': 502, 'has_next_page': False},
            2,
            500,
            500,
            0,
        )
        == 'continue'
    )
    assert _protocol.page_verdict({'has_next_page': True}, 500, 500, 500, 0) == 'continue'
    assert _protocol.page_verdict({'has_next_page': False}, 500, 500, 500, 0) == 'stop'
    assert _protocol.page_verdict({}, 499, 499, 500, 0) == 'stop'
    assert _protocol.page_verdict({}, 500, 500, 500, 0) == 'continue'


def test_page_verdict_validates_only_the_active_metadata_signal() -> None:
    assert (
        _protocol.page_verdict(
            {'entity_count': 502, 'has_next_page': 'ignored'},
            2,
            500,
            500,
            0,
        )
        == 'continue'
    )
    assert _protocol.page_verdict({'entity_count': 'ignored'}, 500, 500, 500, 500) == 'stop'
    assert _protocol.page_verdict({'entity_count': 'ignored'}, 0, 0, 500, 0) == 'stop'


def test_page_verdict_validates_signal_active_after_none_entity_count() -> None:
    with pytest.raises(
        _errors.ShotgunError,
        match=r'^paging_info has_next_page must be a boolean$',
    ):
        _protocol.page_verdict(
            {'entity_count': None, 'has_next_page': 'invalid'},
            500,
            500,
            500,
            0,
        )


def test_project_entities_recursively_projects_and_detaches_payload() -> None:
    payload = {
        'project': {'type': 'Project', 'id': 1, 'name': 'P', 'sg_status': 'Active'},
        'assets': [
            {'type': 'Asset', 'id': 2, 'name': 'A', 'sg_asset_type': 'Model'},
        ],
        'metadata': {'tags': ['original']},
    }
    original = copy.deepcopy(payload)

    result = _protocol.project_entities(payload)

    assert result == {
        'project': {'type': 'Project', 'id': 1, 'name': 'P'},
        'assets': [{'type': 'Asset', 'id': 2, 'name': 'A'}],
        'metadata': {'tags': ['original']},
    }
    assert result is not payload
    assert isinstance(result, dict)
    typed_result = typing.cast('dict[str, object]', result)
    metadata = typed_result['metadata']
    assert isinstance(metadata, dict)
    typed_metadata = typing.cast('dict[str, object]', metadata)
    tags = typed_metadata['tags']
    assert isinstance(tags, list)
    tags[0] = 'changed'
    assert payload == original


def test_raise_fault_ignores_success() -> None:
    _protocol.raise_fault({'results': 1})


@pytest.mark.parametrize(
    ('code', 'exc'),
    [
        (102, _errors.AuthenticationFault),
        (106, _errors.MissingTwoFactorAuthenticationFault),
        (108, _errors.UserCredentialsNotAllowedForSSOAuthenticationFault),
        (110, _errors.UserCredentialsNotAllowedForOxygenAuthenticationFault),
    ],
)
def test_raise_fault_maps_error_codes(code: int, exc: type[_errors.Fault]) -> None:
    with pytest.raises(exc, match='boom'):
        _protocol.raise_fault({'exception': True, 'error_code': code, 'message': 'boom'})


@pytest.mark.parametrize(
    ('response', 'message'),
    [
        ({'exception': True, 'error_code': 999, 'message': 'boom'}, 'boom'),
        ({'exception': True, 'message': 'boom'}, 'boom'),
        ({'exception': True}, 'Unknown Error'),
    ],
)
def test_raise_fault_uses_plain_fault_defaults(
    response: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(_errors.Fault, match=message) as error:
        _protocol.raise_fault(response)

    assert type(error.value) is _errors.Fault


def test_simple_list_filters() -> None:
    result = _protocol.translate_filters([['sg_status_list', 'is', 'ip']])

    assert result == {
        'logical_operator': 'and',
        'conditions': [{'path': 'sg_status_list', 'relation': 'is', 'values': ['ip']}],
    }


def test_transforms_convert_leaves_while_preserving_container_topology() -> None:
    naive = datetime.datetime(2026, 7, 4, 12, tzinfo=datetime.UTC).replace(tzinfo=None)
    inbound = {
        'items': ['2026-07-04T12:00:00Z', ('a &lt;b', 7)],
    }

    inbound_result = _protocol.transform_inbound(inbound, convert_utc=False)

    assert inbound_result == {
        'items': [naive, ('a <b', 7)],
    }
    assert isinstance(inbound_result, dict)
    assert isinstance(inbound_result['items'], list)
    assert isinstance(inbound_result['items'][1], tuple)

    outbound = {
        'items': [
            naive,
            (datetime.date(2026, 7, 5), 7),
        ],
    }

    outbound_result = _protocol.transform_outbound(outbound, convert_utc=False)

    assert outbound_result == {
        'items': ['2026-07-04T12:00:00Z', ('2026-07-05', 7)],
    }
    assert isinstance(outbound_result, dict)
    assert isinstance(outbound_result['items'], list)
    assert isinstance(outbound_result['items'][1], tuple)


def test_translate_filters_annotation_matches_runtime_container_contract() -> None:
    annotation = typing.get_type_hints(_protocol.translate_filters)['filters']

    assert {typing.get_origin(member) for member in typing.get_args(annotation)} == {
        list,
        tuple,
        dict,
    }


def test_tuple_filters_and_nested_values_preserve_value_order() -> None:
    result = _protocol.translate_filters((('id', 'in', (3, 1, 2)),))

    assert result == {
        'logical_operator': 'and',
        'conditions': [{'path': 'id', 'relation': 'in', 'values': [3, 1, 2]}],
    }


@pytest.mark.parametrize(
    'spec',
    [
        _SCRIPT,
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_login='me',
            user_password=_AUTH_VALUE,
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_login='me',
            user_password=_AUTH_VALUE,
            auth_token=_AUTH_VALUE,
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            session_token=_SESSION_VALUE,
        ),
    ],
    ids=['script', 'user', 'user-with-auth-token', 'session'],
)
def test_validate_auth_accepts_documented_modes(spec: _protocol.AuthSpec) -> None:
    _protocol.validate_auth(spec)


@pytest.mark.parametrize(
    'spec',
    [
        dataclasses.replace(_SCRIPT, api_key=None),
        dataclasses.replace(_SCRIPT, script_name=None),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_login='me',
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_password=_AUTH_VALUE,
        ),
        dataclasses.replace(_SCRIPT, user_login='me', user_password=_AUTH_VALUE),
        dataclasses.replace(_SCRIPT, session_token=_SESSION_VALUE),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_login='me',
            user_password=_AUTH_VALUE,
            session_token=_SESSION_VALUE,
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            auth_token=_AUTH_VALUE,
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_login='me',
            auth_token=_AUTH_VALUE,
        ),
        dataclasses.replace(
            _SCRIPT,
            script_name=None,
            api_key=None,
            user_password=_AUTH_VALUE,
            auth_token=_AUTH_VALUE,
        ),
        dataclasses.replace(_SCRIPT, script_name=None, api_key=None),
    ],
    ids=[
        'script-without-key',
        'key-without-script',
        'login-without-password',
        'password-without-login',
        'script-with-user',
        'script-with-session',
        'user-with-session',
        'auth-token-without-user',
        'auth-token-without-password',
        'auth-token-without-login',
        'no-credentials',
    ],
)
def test_validate_auth_rejects_invalid_combinations(spec: _protocol.AuthSpec) -> None:
    with pytest.raises(ValueError, match=r'.'):
        _protocol.validate_auth(spec)


@pytest.mark.parametrize('filter_operator', ['', 'ALL', 'sometimes'])
def test_bad_filter_operator_raises_fault(filter_operator: str) -> None:
    with pytest.raises(_errors.Fault):
        _protocol.translate_filters([], filter_operator=filter_operator)


@pytest.mark.parametrize('relation', ['is', 'is_not', 'in', 'not_in'])
def test_declared_entity_relations_are_optimized(relation: str) -> None:
    entity = {'type': 'Project', 'id': 1, 'name': 'P', 'sg_status': 'x'}
    original = entity.copy()

    result = _protocol.translate_filters([['project', relation, [entity]]])

    assert result['conditions'] == [
        {
            'path': 'project',
            'relation': relation,
            'values': [{'type': 'Project', 'id': 1, 'name': 'P'}],
        }
    ]
    assert entity == original


@pytest.mark.parametrize(
    ('path', 'relation'),
    [('id', 'is'), ('project', 'greater_than')],
)
def test_entity_optimization_is_limited_to_non_id_declared_relations(
    path: str,
    relation: str,
) -> None:
    entity = {'type': 'Project', 'id': 1, 'sg_status': 'x'}

    result = _protocol.translate_filters([[path, relation, entity]])

    assert result['conditions'] == [{'path': path, 'relation': relation, 'values': [entity]}]


@pytest.mark.parametrize(
    ('filter_operator', 'logical_operator'),
    [(None, 'and'), ('all', 'and'), ('any', 'or')],
)
def test_filter_operator_mapping(
    filter_operator: str | None,
    logical_operator: str,
) -> None:
    result = _protocol.translate_filters([], filter_operator=filter_operator)

    assert result['logical_operator'] == logical_operator


@pytest.mark.parametrize(
    'filters',
    [
        ['not-a-condition'],
        [[]],
        [['path-only']],
        [[1, 'is', 'value']],
        [['path', 1, 'value']],
    ],
)
def test_malformed_condition_shapes_raise_fault(filters: list[object]) -> None:
    with pytest.raises(_errors.Fault):
        _protocol.translate_filters(filters)


@pytest.mark.parametrize(
    'value',
    [
        None,
        'Project',
        {'type': 'Project'},
        {'id': 1},
        ['Project', 1],
    ],
)
def test_optimize_entity_returns_non_entities_unchanged(value: object) -> None:
    assert _protocol.optimize_entity(value) is value
