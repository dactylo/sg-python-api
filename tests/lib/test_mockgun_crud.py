import copy
import datetime
import inspect
from typing import Any, cast

import pytest

from shotgun_api3 import ShotgunError
from shotgun_api3.lib import mockgun

_SCALAR_VALUE_CASES = [
    pytest.param('checkbox', True, 1, id='checkbox'),
    pytest.param('date', '2026-07-18', datetime.date(2026, 7, 18), id='date'),
    pytest.param(
        'date_time',
        datetime.datetime(2026, 7, 18, 12, 30, tzinfo=datetime.UTC),
        '2026-07-18T12:30:00',
        id='date-time',
    ),
    pytest.param('duration', 120, 120.0, id='duration'),
    pytest.param('entity_type', 'Shot', {'type': 'Shot'}, id='entity-type'),
    pytest.param('float', 1.5, 1, id='float'),
    pytest.param('list', 'one,two', ['one', 'two'], id='list'),
    pytest.param('number', 7, 7.0, id='number'),
    pytest.param('percent', 50, 50.0, id='percent'),
    pytest.param('serializable', {'key': ['value']}, 'value', id='serializable'),
    pytest.param('status_list', 'ip', 1, id='status-list'),
    pytest.param('text', 'value', 1, id='text'),
    pytest.param('url', {'url': 'https://example.com'}, 'https://example.com', id='url'),
]


def _database(client: mockgun.Shotgun) -> dict[str, dict[int, dict[str, Any]]]:
    return cast('dict[str, dict[int, dict[str, Any]]]', vars(client)['_db'])


def _user_link(entity_id: int, **extra: object) -> dict[str, object]:
    return {'type': 'HumanUser', 'id': entity_id, **extra}


def test_batch_forwards_options_and_preserves_order_shapes_and_inputs(
    mockgun_client: mockgun.Shotgun,
) -> None:
    requests: list[dict[str, object]] = [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {
                'code': 'batch',
                'reviewers': [_user_link(1, name='first')],
            },
            'return_fields': ['code', 'reviewers'],
        },
        {
            'request_type': 'update',
            'entity_type': 'Shot',
            'entity_id': 1,
            'data': {'reviewers': [_user_link(2, name='second')]},
            'multi_entity_update_modes': {'reviewers': 'add'},
        },
        {
            'request_type': 'delete',
            'entity_type': 'Shot',
            'entity_id': 1,
        },
    ]
    original = copy.deepcopy(requests)

    result = mockgun_client.batch(requests)

    assert result == [
        {
            'code': 'batch',
            'id': 1,
            'reviewers': [_user_link(1)],
            'type': 'Shot',
        },
        {
            'id': 1,
            'reviewers': [_user_link(1), _user_link(2)],
            'type': 'Shot',
        },
        True,
    ]
    assert requests == original
    assert _database(mockgun_client)['Shot'][1]['__retired'] is True


def test_batch_has_exact_public_signature() -> None:
    parameters = inspect.signature(mockgun.Shotgun.batch).parameters

    assert tuple(parameters) == ('self', 'requests')
    assert all(parameter.default is inspect.Parameter.empty for parameter in parameters.values())
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in parameters.values()
    )


def test_batch_propagates_delegated_validation_errors(
    mockgun_client: mockgun.Shotgun,
) -> None:
    requests = [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {'missing': 'value'},
        }
    ]
    original = copy.deepcopy(requests)

    with pytest.raises(ShotgunError, match=r'missing.*valid field.*Shot'):
        mockgun_client.batch(requests)

    assert requests == original


def test_batch_rejects_invalid_data_key_before_mutation(
    mockgun_client: mockgun.Shotgun,
) -> None:
    requests = [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {'code': 'batch', 1: 'invalid'},
        }
    ]

    with pytest.raises(ShotgunError, match='field names must be nonempty strings'):
        mockgun_client.batch(requests)

    assert _database(mockgun_client)['Shot'] == {}


@pytest.mark.parametrize(
    ('requests', 'message'),
    [
        ('not-a-list', 'batch requests must be a list'),
        ([None], 'batch request 0 must be a dictionary'),
        ([{'entity_type': 'Shot'}], 'batch request 0.*request_type'),
        ([{'request_type': 'create'}], 'batch request 0.*entity_type'),
        (
            [{'request_type': 'revive', 'entity_type': 'Shot', 'entity_id': 1}],
            'Invalid request type revive',
        ),
        (
            [{'request_type': 'create', 'entity_type': 'Shot'}],
            'batch create request 0.*data',
        ),
        (
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'data': {'code': 'x'},
                }
            ],
            'batch update request 0.*entity_id',
        ),
        (
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 1,
                }
            ],
            'batch update request 0.*data',
        ),
        (
            [{'request_type': 'delete', 'entity_type': 'Shot'}],
            'batch delete request 0.*entity_id',
        ),
    ],
)
def test_batch_rejects_malformed_requests_without_mutating_them(
    mockgun_client: mockgun.Shotgun,
    requests: object,
    message: str,
) -> None:
    original = copy.deepcopy(requests)

    with pytest.raises(ShotgunError, match=message):
        mockgun_client.batch(cast('list[dict[str, Any]]', requests))

    assert requests == original


@pytest.mark.parametrize(('field_name', 'accepted', '_invalid'), _SCALAR_VALUE_CASES)
def test_create_accepts_each_scalar_schema_type(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
    accepted: object,
    _invalid: object,
) -> None:
    assert mockgun_client.create('ScalarTypes', {field_name: accepted}) == {
        field_name: accepted,
        'id': 1,
        'type': 'ScalarTypes',
    }


def test_create_assigns_per_type_ids_and_applies_independent_defaults(
    mockgun_client: mockgun.Shotgun,
) -> None:
    first = mockgun_client.create('Shot', {'code': 'one'})
    second = mockgun_client.create('Shot', {'code': 'two'})
    project = mockgun_client.create('Project', {'name': 'main'})
    event = mockgun_client.create('EventLogEntry', {'event_type': 'Test_Event'})
    database = _database(mockgun_client)

    assert first == {'code': 'one', 'type': 'Shot', 'id': 1}
    assert second['id'] == 2
    assert project['id'] == 1
    assert event['id'] == 2
    assert database['Shot'][1] == {
        '__retired': False,
        'code': 'one',
        'id': 1,
        'project': None,
        'reviewers': [],
        'sg_status_list': None,
        'type': 'Shot',
    }
    assert database['Shot'][2]['reviewers'] == []
    assert database['Shot'][1]['reviewers'] is not database['Shot'][2]['reviewers']


def test_create_expands_local_paths_without_mutating_nested_input(
    mockgun_client: mockgun.Shotgun,
) -> None:
    metadata: dict[str, object] = {
        'local_path': '/work/example.mov',
        'local_path_linux': '/mnt/example.mov',
    }
    data = {'metadata': metadata}
    original = copy.deepcopy(data)

    result = mockgun_client.create('EventLogEntry', data)

    expected_metadata = {
        'local_path': '/work/example.mov',
        'local_path_linux': '/mnt/example.mov',
        'local_path_mac': '/work/example.mov',
        'local_path_windows': '/work/example.mov',
        'local_storage': {
            'id': 0,
            'name': 'auto_generated_by_mockgun',
            'type': 'LocalStorage',
        },
    }
    assert result == {
        'id': 2,
        'metadata': expected_metadata,
        'type': 'EventLogEntry',
    }
    assert _database(mockgun_client)['EventLogEntry'][2]['metadata'] == expected_metadata
    assert data == original


def test_create_has_exact_public_signature() -> None:
    parameters = inspect.signature(mockgun.Shotgun.create).parameters

    assert tuple(parameters) == ('self', 'entity_type', 'data', 'return_fields')
    assert tuple(parameter.default for parameter in parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in parameters.values()
    )


def test_create_normalizes_links_without_mutating_or_aliasing_inputs(
    mockgun_client: mockgun.Shotgun,
) -> None:
    data: dict[str, object] = {
        'code': 'linked',
        'project': {'type': 'HumanUser', 'id': 9, 'name': 'permitted source quirk'},
        'reviewers': [
            {'type': 'HumanUser', 'id': 1, 'name': 'one'},
            {'type': 'HumanUser', 'id': 2, 'name': 'two'},
        ],
    }
    return_fields = ['project', 'reviewers']
    original_data = copy.deepcopy(data)
    original_return_fields = copy.deepcopy(return_fields)

    result = mockgun_client.create('Shot', data, return_fields)

    assert result == {
        'id': 1,
        'project': {'type': 'HumanUser', 'id': 9},
        'reviewers': [
            {'type': 'HumanUser', 'id': 1},
            {'type': 'HumanUser', 'id': 2},
        ],
        'type': 'Shot',
    }
    assert data == original_data
    assert return_fields == original_return_fields

    cast('dict[str, object]', data['project'])['id'] = 99
    cast('list[dict[str, object]]', data['reviewers'])[0]['id'] = 99

    row = _database(mockgun_client)['Shot'][1]
    assert row['project'] == {'type': 'HumanUser', 'id': 9}
    assert row['reviewers'] == [
        {'type': 'HumanUser', 'id': 1},
        {'type': 'HumanUser', 'id': 2},
    ]


@pytest.mark.parametrize(('field_name', '_accepted', 'invalid'), _SCALAR_VALUE_CASES)
def test_create_rejects_invalid_value_for_each_scalar_schema_type(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
    _accepted: object,
    invalid: object,
) -> None:
    with pytest.raises(
        ShotgunError,
        match=rf'ScalarTypes\.{field_name}.*schema type',
    ):
        mockgun_client.create('ScalarTypes', {field_name: invalid})


@pytest.mark.parametrize(
    ('entity_type', 'data', 'return_fields', 'message'),
    [
        ('NotAnEntity', {}, None, 'NotAnEntity.*valid entity'),
        ('Shot', {'missing': 'value'}, None, 'missing.*valid field.*Shot'),
        ('Shot', {'code': 'value'}, ['missing'], 'missing.*valid field.*Shot'),
        ('Shot', {'id': 99}, None, 'id.*type.*create or update'),
        ('Shot', {'type': 'Asset'}, None, 'id.*type.*create or update'),
        ('Shot', {'__retired': True}, None, 'cannot be set'),
        ('Shot', {'_private': True}, None, 'cannot be set'),
    ],
)
def test_create_rejects_unknown_and_direct_fields(
    mockgun_client: mockgun.Shotgun,
    entity_type: str,
    data: dict[str, object],
    return_fields: list[str] | None,
    message: str,
) -> None:
    with pytest.raises(ShotgunError, match=message):
        mockgun_client.create(entity_type, data, return_fields)


def test_create_rejects_unsupported_scalar_schema_type(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(
        ShotgunError,
        match=r'ScalarTypes\.unsupported.*schema type unsupported.*not implemented',
    ):
        mockgun_client.create('ScalarTypes', {'unsupported': 'value'})


def test_create_returns_only_created_or_requested_fields(
    mockgun_client: mockgun.Shotgun,
) -> None:
    created = mockgun_client.create(
        'Shot',
        {'code': 'created', 'sg_status_list': 'ip'},
    )
    requested = mockgun_client.create(
        'Shot',
        {'code': 'requested', 'sg_status_list': 'fin'},
        return_fields=['project', 'code'],
    )

    assert created == {
        'code': 'created',
        'id': 1,
        'sg_status_list': 'ip',
        'type': 'Shot',
    }
    assert requested == {
        'code': 'requested',
        'id': 2,
        'project': None,
        'type': 'Shot',
    }


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('project', [], 'Shot.project.*entity.*dictionary'),
        ('project', {'type': 'Project'}, 'Shot.project.*type.*id'),
        ('reviewers', {'type': 'HumanUser', 'id': 1}, 'Shot.reviewers.*multi_entity.*list'),
        ('reviewers', [1], 'Shot.reviewers.*non-dictionary'),
        ('reviewers', [{'type': 'HumanUser'}], 'Shot.reviewers.*type.*id'),
        ('reviewers', [{'type': 'Project', 'id': 1}], 'Shot.reviewers.*invalid type'),
    ],
)
def test_create_validates_relation_shapes(
    mockgun_client: mockgun.Shotgun,
    field: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ShotgunError, match=message):
        mockgun_client.create('Shot', {field: value})


def test_delete_and_revive_report_state_transitions(
    mockgun_client: mockgun.Shotgun,
) -> None:
    shot = mockgun_client.create('Shot', {'code': 'retirement'})
    assert isinstance(shot['id'], int)
    row = _database(mockgun_client)['Shot'][shot['id']]

    assert mockgun_client.revive('Shot', shot['id']) is False
    assert row['__retired'] is False
    assert mockgun_client.delete('Shot', shot['id']) is True
    assert row['__retired'] is True
    assert mockgun_client.delete('Shot', shot['id']) is False
    assert row['__retired'] is True
    assert mockgun_client.revive('Shot', shot['id']) is True
    assert row['__retired'] is False
    assert mockgun_client.revive('Shot', shot['id']) is False
    assert row['__retired'] is False


@pytest.mark.parametrize('invalid_first', [False, True])
def test_multi_entity_none_is_rejected_before_mutation(
    mockgun_client: mockgun.Shotgun,
    invalid_first: bool,
) -> None:
    data = (
        {'reviewers': None, 'code': 'changed', 'sg_status_list': 'fin'}
        if invalid_first
        else {'code': 'changed', 'reviewers': None, 'sg_status_list': 'fin'}
    )

    shot = mockgun_client.create('Shot', {'code': 'original', 'sg_status_list': 'ip'})
    assert isinstance(shot['id'], int)
    before = copy.deepcopy(_database(mockgun_client)['Shot'][shot['id']])

    with pytest.raises(ShotgunError, match=r'Shot\.reviewers.*multi_entity.*list'):
        mockgun_client.update('Shot', shot['id'], data)
    assert _database(mockgun_client)['Shot'][shot['id']] == before

    with pytest.raises(ShotgunError, match=r'Shot\.reviewers.*multi_entity.*list'):
        mockgun_client.create('Shot', data)
    assert _database(mockgun_client)['Shot'] == {shot['id']: before}


@pytest.mark.parametrize('invalid_key', [1, None, ''])
@pytest.mark.parametrize('operation', ['create', 'update'])
def test_data_keys_must_be_nonempty_strings_before_mutation(
    mockgun_client: mockgun.Shotgun,
    invalid_key: object,
    operation: str,
) -> None:
    shot = mockgun_client.create('Shot', {'code': 'original', 'sg_status_list': 'ip'})
    assert isinstance(shot['id'], int)
    before = copy.deepcopy(_database(mockgun_client)['Shot'][shot['id']])
    data = cast(
        'dict[str, Any]',
        {'code': 'changed', invalid_key: 'invalid', 'sg_status_list': 'fin'},
    )

    if operation == 'create':
        with pytest.raises(ShotgunError, match='field names must be nonempty strings'):
            mockgun_client.create('Shot', data)
    else:
        with pytest.raises(ShotgunError, match='field names must be nonempty strings'):
            mockgun_client.update('Shot', shot['id'], data)

    assert _database(mockgun_client)['Shot'] == {shot['id']: before}


def test_nullable_fields_and_empty_multi_entity_lists_remain_valid(
    mockgun_client: mockgun.Shotgun,
) -> None:
    shot = mockgun_client.create(
        'Shot',
        {'code': None, 'project': None, 'reviewers': [_user_link(1)]},
    )
    assert isinstance(shot['id'], int)

    result = mockgun_client.update(
        'Shot',
        shot['id'],
        {'code': None, 'project': None, 'reviewers': []},
    )

    assert result == [
        {'code': None, 'id': shot['id'], 'project': None, 'reviewers': [], 'type': 'Shot'}
    ]


@pytest.mark.parametrize('method_name', ['delete', 'revive'])
def test_retirement_methods_have_exact_public_signatures(method_name: str) -> None:
    method = getattr(mockgun.Shotgun, method_name)
    parameters = inspect.signature(method).parameters

    assert tuple(parameters) == ('self', 'entity_type', 'entity_id')
    assert all(parameter.default is inspect.Parameter.empty for parameter in parameters.values())
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in parameters.values()
    )


@pytest.mark.parametrize('method_name', ['delete', 'revive'])
def test_retirement_methods_reject_unknown_and_missing_entities(
    mockgun_client: mockgun.Shotgun,
    method_name: str,
) -> None:
    method = getattr(mockgun_client, method_name)

    with pytest.raises(ShotgunError, match=r'NotAnEntity.*valid entity'):
        method('NotAnEntity', 1)
    with pytest.raises(ShotgunError, match=r'No entity.*Shot.*999'):
        method('Shot', 999)


def test_update_has_exact_public_signature() -> None:
    parameters = inspect.signature(mockgun.Shotgun.update).parameters

    assert tuple(parameters) == (
        'self',
        'entity_type',
        'entity_id',
        'data',
        'multi_entity_update_modes',
    )
    assert tuple(parameter.default for parameter in parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in parameters.values()
    )


def test_update_multi_entity_remove_pins_observed_quirks(
    mockgun_client: mockgun.Shotgun,
) -> None:
    links = [_user_link(1), _user_link(2), _user_link(3)]
    shot = mockgun_client.create('Shot', {'reviewers': links})
    assert isinstance(shot['id'], int)

    removed_one = mockgun_client.update(
        'Shot',
        shot['id'],
        {'reviewers': [_user_link(2)]},
        {'reviewers': 'remove'},
    )
    assert removed_one[0]['reviewers'] == [_user_link(1), _user_link(3)]

    mockgun_client.update('Shot', shot['id'], {'reviewers': links})
    removed_two = mockgun_client.update(
        'Shot',
        shot['id'],
        {'reviewers': [_user_link(2), _user_link(3)]},
        {'reviewers': 'remove'},
    )
    assert removed_two[0]['reviewers'] == [
        _user_link(1),
        _user_link(1),
        _user_link(2),
        _user_link(3),
    ]

    mockgun_client.update('Shot', shot['id'], {'reviewers': links})
    removed_empty = mockgun_client.update(
        'Shot',
        shot['id'],
        {'reviewers': []},
        {'reviewers': 'remove'},
    )
    assert removed_empty[0]['reviewers'] == []


def test_update_multi_entity_replacement_and_add_preserve_observed_order(
    mockgun_client: mockgun.Shotgun,
) -> None:
    shot = mockgun_client.create('Shot', {'code': 'relations'})
    assert isinstance(shot['id'], int)
    replacement = [_user_link(2), _user_link(2), _user_link(1)]

    replaced = mockgun_client.update('Shot', shot['id'], {'reviewers': replacement})

    assert replaced == [
        {
            'id': shot['id'],
            'reviewers': [_user_link(2), _user_link(2), _user_link(1)],
            'type': 'Shot',
        }
    ]

    additions = [
        _user_link(2, name='existing'),
        _user_link(3, name='new'),
        _user_link(3, name='duplicate'),
    ]
    modes = {'reviewers': 'add'}
    original_additions = copy.deepcopy(additions)
    original_modes = copy.deepcopy(modes)

    added = mockgun_client.update(
        'Shot',
        shot['id'],
        {'reviewers': additions},
        modes,
    )

    assert added == [
        {
            'id': shot['id'],
            'reviewers': [
                _user_link(2),
                _user_link(2),
                _user_link(1),
                _user_link(3),
            ],
            'type': 'Shot',
        }
    ]
    assert additions == original_additions
    assert modes == original_modes


@pytest.mark.parametrize(
    ('entity_type', 'entity_id', 'data', 'message'),
    [
        ('NotAnEntity', 1, {}, 'NotAnEntity.*valid entity'),
        ('Shot', 999, {'code': 'missing'}, 'No entity.*Shot.*999'),
        ('Shot', 1, {'missing': 'value'}, 'missing.*valid field.*Shot'),
        ('Shot', 1, {'id': 2}, 'id.*type.*create or update'),
        ('Shot', 1, {'type': 'Asset'}, 'id.*type.*create or update'),
        ('Shot', 1, {'__retired': True}, 'cannot be set'),
    ],
)
def test_update_rejects_unknown_missing_and_direct_fields(
    mockgun_client: mockgun.Shotgun,
    entity_type: str,
    entity_id: int,
    data: dict[str, object],
    message: str,
) -> None:
    mockgun_client.create('Shot', {'code': 'existing'})

    with pytest.raises(ShotgunError, match=message):
        mockgun_client.update(entity_type, entity_id, data)


def test_update_replaces_fields_and_returns_one_item_without_aliasing(
    mockgun_client: mockgun.Shotgun,
) -> None:
    shot = mockgun_client.create('Shot', {'code': 'before'})
    assert isinstance(shot['id'], int)
    data: dict[str, object] = {
        'code': 'after',
        'project': {'type': 'HumanUser', 'id': 8, 'name': 'source-compatible type'},
    }
    original = copy.deepcopy(data)

    result = mockgun_client.update('Shot', shot['id'], data)

    assert result == [
        {
            'code': 'after',
            'id': shot['id'],
            'project': {'type': 'HumanUser', 'id': 8},
            'type': 'Shot',
        }
    ]
    assert data == original

    cast('dict[str, object]', data['project'])['id'] = 88

    row = _database(mockgun_client)['Shot'][shot['id']]
    assert row['project'] == {'type': 'HumanUser', 'id': 8}
    assert row['sg_status_list'] is None


def test_update_unsupported_multi_entity_mode_is_a_silent_no_op(
    mockgun_client: mockgun.Shotgun,
) -> None:
    shot = mockgun_client.create('Shot', {'reviewers': [_user_link(1)]})
    assert isinstance(shot['id'], int)
    data = {'reviewers': [_user_link(2, name='ignored')]}
    modes = {'reviewers': 'future_mode'}
    original_data = copy.deepcopy(data)
    original_modes = copy.deepcopy(modes)

    result = mockgun_client.update('Shot', shot['id'], data, modes)

    assert result == [
        {
            'id': shot['id'],
            'reviewers': [_user_link(1)],
            'type': 'Shot',
        }
    ]
    assert data == original_data
    assert modes == original_modes


@pytest.mark.parametrize(
    ('data', 'message'),
    [
        ({'project': []}, 'Shot.project.*entity.*dictionary'),
        (
            {'reviewers': [{'type': 'Project', 'id': 1}]},
            'Shot.reviewers.*invalid type',
        ),
    ],
)
def test_update_validates_relation_values(
    mockgun_client: mockgun.Shotgun,
    data: dict[str, object],
    message: str,
) -> None:
    shot = mockgun_client.create('Shot', {'code': 'existing'})
    assert isinstance(shot['id'], int)

    with pytest.raises(ShotgunError, match=message):
        mockgun_client.update('Shot', shot['id'], data)
