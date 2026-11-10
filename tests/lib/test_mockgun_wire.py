import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import cast

import pytest

from shotgun_api3._config import TimeoutConfig
from shotgun_api3._transport import TransportRequest
from shotgun_api3.lib.mockgun import Site
from shotgun_api3.lib.mockgun._errors import ServerFaultError
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._server import FakeServer
from shotgun_api3.lib.mockgun._sessions import RequestContext
from shotgun_api3.lib.mockgun._wire import (
    decode_call,
    fault_response,
    format_wire_datetime,
    parse_wire_datetime,
)
from tests.conftest import entity_id
from tests.lib._requests import request
from tests.lib._wire_recordings import FIXTURES, recording, response

from .conftest import SCHEMA, SCHEMA_ENTITY, WORKFLOW_SCHEMA, WORKFLOW_SCHEMA_ENTITY, raw_call

_HANDLED = frozenset(path.stem for path in FIXTURES.glob('*.json'))


def _configure_site_data(site: FakeServer, name: str) -> None:
    if name == 'preferences_read':
        Site(site).set_preferences(cast('dict[str, object]', response(recording(name))))
    if name in {'user_subscriptions_read', 'update_project_last_accessed'}:
        context = RequestContext(None, datetime.now(UTC))
        site.store.create('Project', {'name': 'P'}, context=context)
        for login in ('artist', 'second'):
            site.store.create(
                'HumanUser',
                {'login': login, 'name': login, 'sg_status_list': 'act'},
                context=context,
            )
        if name == 'user_subscriptions_read':
            recorded = cast('dict[str, object]', response(recording(name)))
            values = cast('dict[str, str]', recorded['results'])
            Site(site).set_subscriptions({int(key): value for key, value in values.items()})
    if name in {'nav_expand', 'nav_search_entity', 'nav_search_string'}:
        request = cast('dict[str, object]', recording(name)['request'])
        body = cast('dict[str, object]', request['body'])
        params = cast('list[object]', body['params'])
        Site(site).set_canned_response(
            name, cast('dict[str, object]', params[-1]), response(recording(name))
        )


def _request(recording: dict[str, object], *, bad_auth: bool = False) -> TransportRequest:
    source = cast('dict[str, object]', recording['request'])
    body = deepcopy(cast('dict[str, object]', source['body']))
    params = cast('list[object]', body['params'])
    if params and isinstance(params[0], dict):
        auth = cast('dict[str, object]', params[0])
        if {'script_name', 'user_login', 'session_token', 'auth_token'} & auth.keys():
            params[0] = (
                {'user_login': 'unknown-wire-login', 'user_password': 'secret'}
                if bad_auth
                else {'script_name': 'wire-test', 'script_key': 'secret'}
            )
    return TransportRequest(
        cast('str', source['method']),
        'http://unit-test.example' + cast('str', source['path']),
        cast('dict[str, str]', source['headers']),
        json.dumps(body).encode(),
        TimeoutConfig(connect=None, read=None),
    )


def _shape(actual: object, expected: object) -> None:
    assert type(actual) is type(expected)
    if isinstance(expected, dict):
        actual_dict = cast('dict[str, object]', actual)
        expected_dict = cast('dict[str, object]', expected)
        assert actual_dict.keys() == expected_dict.keys()
        for key, value in expected_dict.items():
            _shape(actual_dict[key], value)
    elif isinstance(expected, list):
        actual_list = cast('list[object]', actual)
        expected_list = cast('list[object]', expected)
        if expected_list:
            assert actual_list
            for value in actual_list:
                _shape(value, expected_list[0])
        else:
            assert not actual_list


def _site(name: str) -> FakeServer:
    if name in {'schema_read', 'schema_entity_read'}:
        recorded = cast('dict[str, object]', response(recording(name)))['results']
        if name == 'schema_read':
            fields = cast('dict[str, dict[str, object]]', recorded)
            entities: dict[str, dict[str, object]] = {}
        else:
            fields = {}
            entities = cast('dict[str, dict[str, object]]', recorded)
    elif name in {
        'note_thread_read',
        'activity_stream_read',
        'following',
        'user_subscriptions_read',
        'update_project_last_accessed',
    }:
        fields = cast('dict[str, dict[str, object]]', deepcopy(WORKFLOW_SCHEMA))
        entities = cast('dict[str, dict[str, object]]', deepcopy(WORKFLOW_SCHEMA_ENTITY))
    else:
        fields = deepcopy(cast('dict[str, dict[str, object]]', SCHEMA))
        fields['Note'] = {
            'subject': deepcopy(cast('dict[str, object]', fields['Project']['name'])),
            'project': {
                'data_type': {'value': 'entity'},
                'properties': {
                    'default_value': {'value': None},
                    'valid_types': {'value': ['Project']},
                },
            },
        }
        entities = deepcopy(cast('dict[str, dict[str, object]]', SCHEMA_ENTITY))
    site = FakeServer(SiteSchema(fields, entities))
    if name == 'info':
        # The recording normalizes this dictionary (PW l.1333-1335).
        site.update_server_metadata({'s3_enabled_upload_types': {}})
    if name in {'read', 'create', 'batch', 'summarize'}:
        now = datetime.now(UTC)
        context = RequestContext(None, now)
        site.store.create('Project', {'name': 'First'}, context=context)
        site.store.create('Project', {'name': 'Second'}, context=context)
    if name == 'note_thread_read':
        project = cast(
            'dict[str, object]',
            raw_call(
                site,
                'create',
                {
                    'type': 'Project',
                    'fields': [{'field_name': 'name', 'value': 'P'}],
                    'return_fields': [],
                },
            )['results'],
        )
        note = cast(
            'dict[str, object]',
            raw_call(
                site,
                'create',
                {
                    'type': 'Note',
                    'fields': [
                        {'field_name': 'subject', 'value': 'wire'},
                        {
                            'field_name': 'project',
                            'value': {'type': 'Project', 'id': project['id']},
                        },
                    ],
                    'return_fields': [],
                },
            )['results'],
        )
        raw_call(
            site,
            'create',
            {
                'type': 'Reply',
                'fields': [
                    {'field_name': 'entity', 'value': {'type': 'Note', 'id': note['id']}},
                    {'field_name': 'content', 'value': 'wire reply'},
                ],
                'return_fields': [],
            },
        )
    if name == 'activity_stream_read':
        # Project creation taints its activity; seed this existing wire fixture's Project.
        project = site.store.create(
            'Project', {'name': 'P'}, context=RequestContext(None, datetime.now(UTC))
        )
        raw_call(
            site,
            'create',
            {
                'type': 'Note',
                'fields': [
                    {'field_name': 'project', 'value': {'type': 'Project', 'id': project['id']}},
                    {'field_name': 'subject', 'value': 'wire'},
                    {'field_name': 'content', 'value': 'body'},
                ],
                'return_fields': [],
            },
        )
    if name == 'following':
        user = site.store.create(
            'HumanUser',
            {'login': 'artist', 'name': 'artist', 'sg_status_list': 'act'},
            context=RequestContext(None, datetime.now(UTC)),
        )
        followed = True
        site.store.side_set('follows', (entity_id(user), 'HumanUser', entity_id(user)), followed)
    _configure_site_data(site, name)
    return site


@pytest.mark.parametrize('name', sorted(_HANDLED))
def test_handled_recordings_match_wire_shape(name: str) -> None:
    fixture_recording = recording(name)
    site = _site(name)
    site_response = site.handle(_request(fixture_recording, bad_auth=name == 'fault_bad_auth'))
    recorded_response = cast('dict[str, object]', fixture_recording['response'])
    assert site_response.status == recorded_response['status']
    actual = json.loads(site_response.content)
    expected = response(fixture_recording)
    if name == 'note_thread_read':
        actual_rows = cast('list[object]', actual)
        expected_rows = cast('list[object]', expected)
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows, strict=True):
            _shape(actual_row, expected_row)
    else:
        _shape(actual, expected)
    if name in {
        'schema_read',
        'schema_entity_read',
        'preferences_read',
        'nav_expand',
        'nav_search_entity',
        'nav_search_string',
    }:
        assert actual == expected
    if name == 'summarize':
        assert actual == {'results': {'summaries': {'id': 2}, 'groups': []}}
    if name == 'text_search':
        assert actual == expected
    if name == 'fault_bad_auth':
        assert cast('dict[str, object]', actual)['error_code'] == 102


def test_wire_datetimes_are_utc_whole_seconds() -> None:
    value = parse_wire_datetime('2025-01-02T03:04:05Z')
    assert value == datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert format_wire_datetime(value.replace(microsecond=1234)) == '2025-01-02T03:04:05Z'


def test_wire_fault_preserves_recorded_shape() -> None:
    response = fault_response(ServerFaultError('rejected', error_code=102))
    assert response.status == 200
    assert response.headers['content-type'] == 'application/json'
    assert json.loads(response.content) == {
        'exception': True,
        'message': 'rejected',
        'error_code': 102,
    }


@pytest.mark.parametrize(
    'key', ['script_name', 'user_login', 'session_token', 'auth_token', 'sudo_as_login']
)
def test_wire_separates_auth_from_params(key: str) -> None:
    payload = {'method_name': 'read', 'params': [{key: 'value'}, {'type': 'Project'}]}
    call = decode_call(request(body=json.dumps(payload).encode()))
    assert call.method_name == 'read'
    assert call.auth == {key: 'value'}
    assert call.params == {'type': 'Project'}
