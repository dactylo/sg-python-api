import asyncio
import copy
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun._errors import MockgunError
from tests.conftest import entity_id
from tests.lib._events import cursor, delta

from .conftest import EFFECTS_SCHEMA_ENTITY, FakeSiteCase, raw_call, raw_read, seed


def _entry(
    display_name: str,
    entity_type: str,
    data_type: str,
    properties: dict[str, dict[str, object]],
    *,
    description: str = '',
) -> dict[str, object]:
    return {
        'name': {'value': display_name, 'editable': True},
        'description': {'value': description, 'editable': True},
        'custom_metadata': {'value': '', 'editable': True},
        'entity_type': {'value': entity_type, 'editable': False},
        'data_type': {'value': data_type, 'editable': False},
        'editable': {'value': True, 'editable': False},
        'mandatory': {'value': False, 'editable': False},
        'unique': {'value': False, 'editable': False},
        'properties': properties,
        'visible': {'value': True, 'editable': True},
        'ui_value_displayable': {'value': True, 'editable': False},
    }


def _identity(row: dict[str, object]) -> dict[str, object]:
    return {'type': row['type'], 'id': row['id']}


def _properties(
    *,
    default: object = None,
    summary: str = 'none',
    valid_values: list[str] | None = None,
    valid_types: list[str] | None = None,
    data_type: str = 'text',
) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {
        'default_value': {'value': default, 'editable': data_type in {'list', 'checkbox'}},
        'summary_default': {'value': summary, 'editable': True},
    }
    if valid_values is not None:
        result['valid_values'] = {'value': valid_values, 'editable': True}
    if valid_types is not None:
        result['valid_types'] = {'value': valid_types, 'editable': data_type != 'multi_entity'}
    return result


def _wire_create(
    case: FakeSiteCase,
    entity_type: str,
    data_type: str,
    name: str,
    properties: dict[str, object] | None = None,
) -> dict[str, object]:
    pairs: list[dict[str, object]] = [{'property_name': 'name', 'value': name}]
    pairs.extend(
        {'property_name': key, 'value': value} for key, value in (properties or {}).items()
    )
    return raw_call(
        case.client._server,
        'schema_field_create',
        {'type': entity_type, 'data_type': data_type, 'properties': pairs},
    )


def _wire_update(
    case: FakeSiteCase, field: str, properties: dict[str, object]
) -> dict[str, object]:
    return raw_call(
        case.client._server,
        'schema_field_update',
        {
            'type': 'Asset',
            'field_name': field,
            'properties': [
                {'property_name': key, 'value': value} for key, value in properties.items()
            ],
        },
    )


@pytest.mark.anyio
async def test_create_names_entries_and_immediate_use(effects_site: FakeSiteCase) -> None:
    field = await effects_site.call('schema_field_create', 'Asset', 'text', 'Mixed Capitals')
    assert field == 'sg_mixed_capitals'
    assert await effects_site.call('schema_field_read', 'Asset', field) == {
        field: _entry('Mixed Capitals', 'Asset', 'text', _properties())
    }
    asset = cast(
        'dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a', field: 'x'})
    )
    assert await effects_site.call('find_one', 'Asset', [['id', 'is', asset['id']]], [field]) == {
        **_identity(asset),
        field: 'x',
    }
    assert await effects_site.call('find', 'Asset', [[field, 'is', 'x']], [field]) == [
        {**_identity(asset), field: 'x'}
    ]
    number = await effects_site.call(
        'schema_field_create',
        'Shot',
        'number',
        'Digits 27 !? Dot.Test',
        {'summary_default': 'sum'},
    )
    assert number == 'sg_digits_27____dot_test'
    assert await effects_site.call('schema_field_read', 'Shot', number) == {
        number: _entry('Digits 27 !? Dot.Test', 'Shot', 'number', _properties(summary='sum'))
    }


@pytest.mark.anyio
async def test_create_name_length_and_collisions(effects_site: FakeSiteCase) -> None:
    assert (
        len(cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'A' * 80)))
        == 83
    )
    for display_name in ('A' * 81, 'Caf\u00e9', 'With_Underscore'):
        with pytest.raises(MockgunError):
            await effects_site.call('schema_field_create', 'Asset', 'text', display_name)
    assert [
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Mixed Capitals')
        for _ in range(3)
    ] == ['sg_mixed_capitals', 'sg_mixed_capitals_1', 'sg_mixed_capitals_2']
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Mixed Capitals')
    assert (
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Asset Type')
        == 'sg_asset_type_1'
    )


@pytest.mark.anyio
async def test_delete_reserves_name_and_rejects_field_use(effects_site: FakeSiteCase) -> None:
    field = cast(
        'str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Mixed Capitals')
    )
    assert await effects_site.call('schema_field_delete', 'Asset', field) is True
    schema = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read'))
    assert field not in schema['Asset']
    assert field not in cast(
        'dict[str, object]', await effects_site.call('schema_field_read', 'Asset')
    )
    with pytest.raises(Fault):
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Mixed Capitals')
    response = _wire_create(effects_site, 'Asset', 'text', 'Mixed Capitals')
    assert response['error_code'] == 105
    assert response['message'] == (
        'API schema_field_create() failed, there is a retired field with the same field_name: '
        'sg_mixed_capitals. Delete the retired field forever from the Trash Page in Shotgun '
        'and try again.'
    )
    with pytest.raises(Fault):
        await effects_site.call('find', 'Asset', [[field, 'is', 'x']])
    assert (
        raw_read(effects_site.client._server, 'Asset', [(field, 'is', ['x'])])['error_code'] == 103
    )
    with pytest.raises(Fault):
        await effects_site.call('create', 'Asset', {field: 'x'})
    assert (
        raw_call(
            effects_site.client._server,
            'create',
            {
                'type': 'Asset',
                'fields': [{'field_name': field, 'value': 'x'}],
                'return_fields': ['id'],
            },
        )['error_code']
        == 103
    )
    asset = seed(effects_site, 'Asset', {'code': 'a'})
    assert (
        await effects_site.call('find_one', 'Asset', [['id', 'is', asset['id']]], [field]) == asset
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('data_type', 'name', 'properties', 'expected'),
    [
        (
            'list',
            'Options',
            {'valid_values': ['OptionA', 'OptionB']},
            _properties(data_type='list', valid_values=['OptionA', 'OptionB']),
        ),
        ('entity', 'Target', {'valid_types': ['Shot']}, _properties(valid_types=['Shot'])),
        ('float', 'Decimal', {'description': 'd'}, _properties()),
        (
            'checkbox',
            'Flag',
            {'default_value': False},
            _properties(default=False, data_type='checkbox'),
        ),
        (
            'multi_entity',
            'Targets',
            {'valid_types': ['Shot']},
            _properties(valid_types=['Shot'], data_type='multi_entity'),
        ),
    ],
)
async def test_create_supported_type_entries(
    effects_site: FakeSiteCase,
    data_type: str,
    name: str,
    properties: dict[str, object],
    expected: dict[str, dict[str, object]],
) -> None:
    field = await effects_site.call('schema_field_create', 'Asset', data_type, name, properties)
    assert field == f'sg_{name.lower()}'
    assert await effects_site.call('schema_field_read', 'Asset', field) == {
        field: _entry(
            name, 'Asset', data_type, expected, description='d' if data_type == 'float' else ''
        )
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    [
        ('Asset', 'number', 'N', {'default_value': 7}, 103),
        ('Asset', 'float', 'F', {'default_value': 1.5}, 103),
        ('Asset', 'status_list', 'S', None, 103),
        ('Asset', 'multi_entity', 'M', {'valid_types': ['Shot', 'Asset']}, 103),
        ('Asset', 'checkbox', 'C', None, 105),
        ('Asset', 'color', 'Color', None, 103),
        ('Asset', 'no_such_type', 'Unknown', None, 103),
        ('NoSuchEntity', 'text', 'Unknown', None, 103),
        ('Asset', 'text', '', None, 103),
    ],
)
async def test_create_faults_change_nothing(
    effects_site: FakeSiteCase,
    scenario: tuple[str, str, str, dict[str, object] | None, int],
) -> None:
    entity_type, data_type, name, properties, code = scenario
    before = copy.deepcopy(await effects_site.call('schema_read'))
    response = _wire_create(effects_site, entity_type, data_type, name, properties)
    assert response['error_code'] == code
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('data_type', 'name', 'properties'),
    [
        ('number', 'N', {'summary_default': 'none', 'default_value': 7}),
        ('status_list', 'S', {'description': 'x'}),
        ('color', 'Color', {'description': 'x'}),
    ],
)
async def test_create_competing_errors_raise_without_change(
    effects_site: FakeSiteCase,
    data_type: str,
    name: str,
    properties: dict[str, object],
) -> None:
    before = copy.deepcopy(await effects_site.call('schema_read'))
    with pytest.raises(MockgunError, match='several errors'):
        _wire_create(effects_site, 'Asset', data_type, name, properties)
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('entity_type', 'data_type', 'name', 'properties'),
    [
        ('Asset', 'status_list', 'S', {'valid_values': ['wtg']}),
        *[
            ('Asset', kind, kind.title(), None)
            for kind in ('date', 'date_time', 'duration', 'percent', 'url', 'timecode', 'currency')
        ],
        ('Asset', 'text', 'Description', {'description': 'd'}),
        ('Asset', 'number', 'N', None),
        ('Asset', 'list', 'L', None),
        ('Asset', 'list', 'L', {'valid_values': []}),
        ('Asset', 'entity', 'E', None),
        ('Asset', 'entity', 'E', {'valid_types': []}),
        ('Asset', 'entity', 'E', {'valid_types': ['Shot', 'Asset']}),
        ('Asset', 'multi_entity', 'M', {'valid_types': []}),
        ('Asset', 'text', 'T', {'unique': True}),
        ('Asset', 'number', 'N', {'summary_default': 'none'}),
        ('Asset', 'checkbox', 'C', {'default_value': True}),
        ('NoSuchEntity', 'text', '', None),
    ],
)
async def test_unsettled_create_raises_without_change(
    effects_site: FakeSiteCase,
    entity_type: str,
    data_type: str,
    name: str,
    properties: dict[str, object] | None,
) -> None:
    before = copy.deepcopy(await effects_site.call('schema_read'))
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_create', entity_type, data_type, name, properties)
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
async def test_update_name_description_and_list_values(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    assert field == 'sg_field_f'
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'name': 'Renamed', 'description': 'about'}
        )
        is True
    )
    entry = cast(
        'dict[str, object]', await effects_site.call('schema_field_read', 'Asset', field)
    )[field]
    assert entry == _entry('Renamed', 'Asset', 'text', _properties(), description='about')
    list_field = cast(
        'str',
        await effects_site.call(
            'schema_field_create',
            'Asset',
            'list',
            'List L',
            {'valid_values': ['OptionA', 'OptionB']},
        ),
    )
    old = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Asset', {'code': 'old', list_field: 'OptionA'}),
    )
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', list_field, {'valid_values': ['OptionB', 'OptionC']}
        )
        is True
    )
    list_entry = cast(
        'dict[str, object]', await effects_site.call('schema_field_read', 'Asset', list_field)
    )[list_field]
    assert list_entry == _entry(
        'List L',
        'Asset',
        'list',
        _properties(data_type='list', valid_values=['OptionB', 'OptionC']),
    )
    assert await effects_site.call(
        'find_one', 'Asset', [['id', 'is', old['id']]], [list_field]
    ) == {**_identity(old), list_field: 'OptionA'}
    with pytest.raises(Fault):
        await effects_site.call('create', 'Asset', {'code': 'invalid', list_field: 'OptionA'})
    assert (
        raw_call(
            effects_site.client._server,
            'create',
            {
                'type': 'Asset',
                'fields': [
                    {'field_name': 'code', 'value': 'invalid'},
                    {'field_name': list_field, 'value': 'OptionA'},
                ],
                'return_fields': ['id'],
            },
        )['error_code']
        == 104
    )
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', list_field, {'default_value': 'OptionB'}
        )
        is True
    )
    new = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'new'}))
    assert await effects_site.call(
        'find_one', 'Asset', [['id', 'is', new['id']]], [list_field]
    ) == {**_identity(new), list_field: 'OptionB'}
    assert await effects_site.call(
        'find_one', 'Asset', [['id', 'is', old['id']]], [list_field]
    ) == {**_identity(old), list_field: 'OptionA'}


@pytest.mark.anyio
async def test_update_and_delete_rejections_leave_schema_unchanged(
    effects_site: FakeSiteCase,
) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_field_read', 'Asset', field))
    for property_name, value in (('data_type', 'number'), ('mandatory', True)):
        with pytest.raises(Fault):
            await effects_site.call('schema_field_update', 'Asset', field, {property_name: value})
        assert _wire_update(effects_site, field, {property_name: value})['error_code'] == 103
        assert await effects_site.call('schema_field_read', 'Asset', field) == before
    with pytest.raises(Fault):
        await effects_site.call('schema_field_update', 'Asset', 'sg_absent', {'name': 'x'})
    assert _wire_update(effects_site, 'sg_absent', {'name': 'x'})['error_code'] == 103
    with pytest.raises(Fault):
        await effects_site.call('schema_field_delete', 'Asset', 'sg_absent')
    assert (
        raw_call(
            effects_site.client._server,
            'schema_field_delete',
            {'type': 'Asset', 'field_name': 'sg_absent'},
        )['error_code']
        == 103
    )
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_update', 'Asset', field, {'unique': True})
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_update', 'Asset', 'code', {'description': 'x'})
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_delete', 'Asset', 'code')
    assert await effects_site.call('schema_field_read', 'Asset', field) == before


@pytest.mark.anyio
async def test_existing_records_get_created_field_values(effects_site: FakeSiteCase) -> None:
    old = seed(effects_site, 'Asset', {'code': 'old'})
    text_field = cast(
        'str', await effects_site.call('schema_field_create', 'Asset', 'text', 'After Text')
    )
    assert await effects_site.call(
        'find_one', 'Asset', [['id', 'is', old['id']]], [text_field]
    ) == {**old, text_field: None}
    checkbox = cast(
        'str',
        await effects_site.call(
            'schema_field_create', 'Asset', 'checkbox', 'After Flag', {'default_value': False}
        ),
    )
    with pytest.raises(MockgunError):
        await effects_site.call('find_one', 'Asset', [['id', 'is', old['id']]], [checkbox])
    targets = cast(
        'str',
        await effects_site.call(
            'schema_field_create',
            'Asset',
            'multi_entity',
            'After Targets',
            {'valid_types': ['Shot']},
        ),
    )
    assert await effects_site.call('find_one', 'Asset', [['id', 'is', old['id']]], [targets]) == {
        **old,
        targets: [],
    }
    new = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'new'}))
    assert await effects_site.call('find_one', 'Asset', [['id', 'is', new['id']]], [checkbox]) == {
        **_identity(new),
        checkbox: False,
    }


@pytest.mark.anyio
async def test_delete_removes_values_from_existing_records(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    asset = cast(
        'dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a', field: 'x'})
    )
    assert await effects_site.call('schema_field_delete', 'Asset', field) is True
    assert field not in effects_site.client._server.store.require('Asset', entity_id(asset))
    assert await effects_site.call(
        'find_one', 'Asset', [['id', 'is', asset['id']]], [field]
    ) == _identity(asset)


@pytest.mark.anyio
async def test_retired_collision_suffix_is_unsettled(effects_site: FakeSiteCase) -> None:
    assert (
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Asset Type')
        == 'sg_asset_type_1'
    )
    assert await effects_site.call('schema_field_delete', 'Asset', 'sg_asset_type_1') is True
    with pytest.raises(MockgunError):
        await effects_site.call('schema_field_create', 'Asset', 'text', 'Asset Type')


@pytest.mark.anyio
@pytest.mark.parametrize(
    'project',
    [
        {'type': 'Project', 'id': 999},
        {'type': 'Shot', 'id': 1},
    ],
)
async def test_update_rejects_nonexistent_or_nonproject_link_first(
    effects_site: FakeSiteCase,
    project: dict[str, object],
) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_field_read', 'Asset', field))
    with pytest.raises(MockgunError):
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'mandatory': True}, project_entity=project
        )
    assert await effects_site.call('schema_field_read', 'Asset', field) == before


@pytest.mark.anyio
async def test_valid_project_does_not_scope_name_update(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'name': 'Renamed'}, project_entity=project
        )
        is True
    )
    assert await effects_site.call('schema_field_read', 'Asset', field) == {
        field: _entry('Renamed', 'Asset', 'text', _properties())
    }


@pytest.mark.anyio
async def test_project_visibility_scopes_schema_reads_only(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    other = seed(effects_site, 'Project', {'name': 'Beta'})
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    unscoped_before = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read'))

    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'visible': False}, project_entity=project
        )
        is True
    )
    scoped = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read', project))
    unscoped = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read'))
    other_scope = cast(
        'dict[str, dict[str, object]]', await effects_site.call('schema_read', other)
    )
    assert (
        cast('dict[str, dict[str, object]]', scoped['Asset'][field])['visible']['value'] is False
    )
    assert (
        cast('dict[str, dict[str, object]]', unscoped['Asset'][field])['visible']['value'] is True
    )
    assert (
        cast('dict[str, dict[str, object]]', other_scope['Asset'][field])['visible']['value']
        is True
    )
    assert unscoped == unscoped_before
    assert await effects_site.call('schema_field_read', 'Asset', field, project) == {
        field: scoped['Asset'][field]
    }

    cast('dict[str, object]', scoped['Asset'][field]).clear()
    assert await effects_site.call('schema_field_read', 'Asset', field, project) != {field: {}}
    assert await effects_site.call('schema_read') == unscoped_before
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'visible': True}, project_entity=project
        )
        is True
    )
    restored = cast(
        'dict[str, dict[str, object]]', await effects_site.call('schema_read', project)
    )
    assert (
        cast('dict[str, dict[str, object]]', restored['Asset'][field])['visible']['value'] is True
    )


@pytest.mark.anyio
async def test_hidden_field_remains_usable_in_records(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    assert (
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'visible': False}, project_entity=project
        )
        is True
    )
    asset = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Asset', {'code': 'a', 'project': project, field: 'x'}),
    )
    assert asset[field] == 'x'
    expected = {'type': 'Asset', 'id': asset['id'], field: 'x'}
    assert (
        await effects_site.call('find_one', 'Asset', [['id', 'is', asset['id']]], [field])
        == expected
    )
    assert await effects_site.call('find', 'Asset', [[field, 'is', 'x']], [field]) == [expected]


@pytest.mark.anyio
async def test_project_description_update_is_site_wide(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    assert (
        await effects_site.call(
            'schema_field_update',
            'Asset',
            field,
            {'description': 'scoped'},
            project_entity=project,
        )
        is True
    )
    for scope in (None, project):
        fields = cast(
            'dict[str, dict[str, object]]',
            await effects_site.call('schema_field_read', 'Asset', field, scope),
        )
        assert cast('dict[str, object]', fields[field]['description'])['value'] == 'scoped'


@pytest.mark.anyio
async def test_visibility_without_project_raises_fault(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_read'))
    with pytest.raises(Fault, match='visible'):
        await effects_site.call('schema_field_update', 'Asset', field, {'visible': False})
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
async def test_visibility_without_project_has_wire_fault_104(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_read'))
    assert _wire_update(effects_site, field, {'visible': False})['error_code'] == 104
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
async def test_visibility_update_rejects_missing_project(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_read'))
    with pytest.raises(MockgunError, match='schema_field_update'):
        await effects_site.call(
            'schema_field_update',
            'Asset',
            field,
            {'visible': False},
            project_entity={'type': 'Project', 'id': 999},
        )
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
async def test_project_visibility_adds_wrapper_to_loaded_field(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    before = copy.deepcopy(await effects_site.call('schema_field_read', 'Asset', 'sg_asset_type'))
    assert 'visible' not in cast('dict[str, dict[str, object]]', before)['sg_asset_type']
    assert (
        await effects_site.call(
            'schema_field_update',
            'Asset',
            'sg_asset_type',
            {'visible': False},
            project_entity=project,
        )
        is True
    )
    scoped = cast(
        'dict[str, dict[str, object]]',
        await effects_site.call('schema_field_read', 'Asset', 'sg_asset_type', project),
    )
    assert scoped['sg_asset_type']['visible'] == {'value': False, 'editable': True}
    assert await effects_site.call('schema_field_read', 'Asset', 'sg_asset_type') == before


@pytest.mark.anyio
@pytest.mark.parametrize('method', ['schema_read', 'schema_field_read'])
async def test_project_visibility_preserves_loaded_wrapper_metadata(
    effects_site: FakeSiteCase, method: str
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    entry = cast(
        'dict[str, object]', effects_site.client._server.schema.fields['Asset']['sg_asset_type']
    )
    original = {'value': True, 'editable': False, 'metadata': {'source': 'loaded'}}
    entry['visible'] = copy.deepcopy(original)
    assert (
        await effects_site.call(
            'schema_field_update',
            'Asset',
            'sg_asset_type',
            {'visible': False},
            project_entity=project,
        )
        is True
    )
    args = ('Asset', 'sg_asset_type') if method == 'schema_field_read' else ()
    scoped = cast(
        'dict[str, object]', await effects_site.call(method, *args, project_entity=project)
    )
    fields = cast('dict[str, object]', scoped['Asset']) if method == 'schema_read' else scoped
    wrapper = cast('dict[str, dict[str, object]]', fields['sg_asset_type'])['visible']
    assert wrapper == {**original, 'value': False}
    wrapper['value'] = True
    cast('dict[str, object]', wrapper['metadata'])['source'] = 'mutated'
    fresh = cast(
        'dict[str, object]', await effects_site.call(method, *args, project_entity=project)
    )
    fresh_fields = cast('dict[str, object]', fresh['Asset']) if method == 'schema_read' else fresh
    assert cast('dict[str, object]', fresh_fields['sg_asset_type'])['visible'] == {
        **original,
        'value': False,
    }
    unscoped = cast('dict[str, object]', await effects_site.call(method, *args))
    loaded = cast('dict[str, object]', unscoped['Asset']) if method == 'schema_read' else unscoped
    assert cast('dict[str, object]', loaded['sg_asset_type'])['visible'] == original
    assert entry['visible'] == original


@pytest.mark.anyio
async def test_project_entity_schema_read_is_unsettled(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    assert await effects_site.call('schema_entity_read') == EFFECTS_SCHEMA_ENTITY
    with pytest.raises(MockgunError, match='schema_entity_read'):
        await effects_site.call('schema_entity_read', project)


@pytest.mark.anyio
@pytest.mark.parametrize('scoped', [False, True])
async def test_missing_field_schema_read_raises_fault(
    effects_site: FakeSiteCase, scoped: bool
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    scope = project if scoped else None
    with pytest.raises(Fault, match=r'Asset\.sg_absent'):
        await effects_site.call('schema_field_read', 'Asset', 'sg_absent', scope)


@pytest.mark.anyio
@pytest.mark.parametrize('scoped', [False, True])
async def test_missing_field_schema_read_has_exact_wire_fault(
    effects_site: FakeSiteCase, scoped: bool
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    message = (
        "API schema_field_read() Asset.sg_absent doesn't exist:\n"
        '{"type" => "Asset", "field_name" => "sg_absent"}'
    )
    params: dict[str, object] = {'type': 'Asset', 'field_name': 'sg_absent'}
    if scoped:
        params['project'] = project
    response = raw_call(effects_site.client._server, 'schema_field_read', params)
    assert response['error_code'] == 103
    assert response['message'] == message


@pytest.mark.anyio
async def test_missing_project_schema_read_is_unscoped_and_detached(
    effects_site: FakeSiteCase,
) -> None:
    missing = {'type': 'Project', 'id': -1}
    unscoped = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read'))
    scoped = cast('dict[str, dict[str, object]]', await effects_site.call('schema_read', missing))
    assert scoped == unscoped
    fields = cast(
        'dict[str, object]', await effects_site.call('schema_field_read', 'Asset', 'code', missing)
    )
    assert fields == {'code': scoped['Asset']['code']}
    cast('dict[str, object]', scoped['Asset']['code']).clear()
    cast('dict[str, object]', fields['code']).clear()
    assert await effects_site.call('schema_read') == unscoped


@pytest.mark.anyio
async def test_loaded_custom_field_accepts_name_and_description_update(
    effects_site: FakeSiteCase,
) -> None:
    field = 'sg_asset_type'
    assert (
        await effects_site.call(
            'schema_field_update',
            'Asset',
            field,
            {'name': 'Asset Category', 'description': 'kind of asset'},
        )
        is True
    )
    entry = cast(
        'dict[str, object]', await effects_site.call('schema_field_read', 'Asset', field)
    )[field]
    assert cast('dict[str, object]', entry)['name'] == {
        'value': 'Asset Category',
        'editable': True,
    }
    assert cast('dict[str, object]', entry)['description'] == {
        'value': 'kind of asset',
        'editable': True,
    }
    assert (
        cast('dict[str, object]', cast('dict[str, object]', entry)['data_type'])['value'] == 'list'
    )


@pytest.mark.anyio
async def test_multi_property_update_is_atomic(effects_site: FakeSiteCase) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_field_read', 'Asset', field))
    with pytest.raises(Fault):
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'name': 'Renamed', 'mandatory': True}
        )
    assert await effects_site.call('schema_field_read', 'Asset', field) == before


@pytest.mark.anyio
async def test_unsupported_field_updates_leave_entry_unchanged(
    effects_site: FakeSiteCase,
) -> None:
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field F'))
    before = copy.deepcopy(await effects_site.call('schema_field_read', 'Asset', field))
    for properties in (
        {'default_value': 'x'},
        {'valid_values': ['x']},
    ):
        with pytest.raises(MockgunError):
            await effects_site.call('schema_field_update', 'Asset', field, properties)
        assert await effects_site.call('schema_field_read', 'Asset', field) == before
    with pytest.raises(Fault, match='visible'):
        await effects_site.call(
            'schema_field_update', 'Asset', field, {'name': 'Renamed', 'visible': False}
        )
    assert await effects_site.call('schema_field_read', 'Asset', field) == before


@pytest.mark.anyio
async def test_repeated_property_create_is_unsettled(effects_site: FakeSiteCase) -> None:
    before = copy.deepcopy(await effects_site.call('schema_read'))
    with pytest.raises(MockgunError):
        raw_call(
            effects_site.client._server,
            'schema_field_create',
            {
                'type': 'Asset',
                'data_type': 'text',
                'properties': [
                    {'property_name': 'name', 'value': 'Field F'},
                    {'property_name': 'name', 'value': 'Field F'},
                ],
            },
        )
    assert await effects_site.call('schema_read') == before


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update', 'delete'])
async def test_text_schema_events_preserve_common_null_fields(
    effects_site: FakeSiteCase, operation: str
) -> None:
    before = await cursor(effects_site)
    field = await effects_site.call('schema_field_create', 'Asset', 'text', 'Common Nulls')
    if operation != 'create':
        before = await cursor(effects_site)
        if operation == 'update':
            await effects_site.call(
                'schema_field_update', 'Asset', field, {'description': 'about'}
            )
        else:
            await effects_site.call('schema_field_delete', 'Asset', field)
    for unknown in ('entity', 'meta', 'created_at', 'description'):
        with pytest.raises(MockgunError, match=rf'EventLogEntry\.{unknown}'):
            await delta(effects_site, before, [unknown])
    fields = [
        'cached_display_name',
        'image',
        'filmstrip_image',
        'image_blur_hash',
        'image_source_entity',
    ]
    rows = await delta(effects_site, before, [*fields, 'audit_trail', 'project'])
    assert len(rows) == (1 if operation == 'create' else 2)
    assert rows == [
        {'type': 'EventLogEntry', 'id': row['id'], 'project': None, **dict.fromkeys(fields)}
        for row in rows
    ]


@pytest.mark.anyio
async def test_text_schema_write_events_and_unknown_columns(effects_site: FakeSiteCase) -> None:
    before = await cursor(effects_site)
    field = cast('str', await effects_site.call('schema_field_create', 'Asset', 'text', 'Field G'))
    rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'user', 'project'])
    assert [(row['event_type'], row['attribute_name'], row['project']) for row in rows] == [
        ('Shotgun_DisplayColumn_New', None, None)
    ]
    assert rows[0]['user'] == {'type': 'ApiUser', 'id': 1, 'name': 'queries'}
    for unknown in ('entity', 'meta'):
        with pytest.raises(MockgunError):
            await effects_site.call('find', 'EventLogEntry', [], [unknown])
    before = await cursor(effects_site)
    assert (
        await effects_site.call('schema_field_update', 'Asset', field, {'description': 'about'})
        is True
    )
    rows = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        ('Shotgun_DisplayColumn_Change', 'description'),
        ('Shotgun_DisplayColumn_Change', 'properties'),
    ]
    before = await cursor(effects_site)
    assert await effects_site.call('schema_field_delete', 'Asset', field) is True
    rows = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        ('Shotgun_DisplayColumn_Change', 'retirement_date'),
        ('Shotgun_DisplayColumn_Retirement', None),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('variant', ['name', 'list', 'sudo'])
async def test_unsettled_schema_write_taints_event_log(
    effects_site: FakeSiteCase,
    variant: str,
) -> None:
    if variant == 'list':
        await effects_site.call(
            'schema_field_create', 'Asset', 'list', 'Options', {'valid_values': ['A', 'B']}
        )
    elif variant == 'sudo':
        human = seed(effects_site, 'HumanUser', {'login': 'artist', 'name': 'Artist'})
        assert human['type'] == 'HumanUser'
        raw_call(
            effects_site.client._server,
            'schema_field_create',
            {
                'type': 'Asset',
                'data_type': 'text',
                'properties': [{'property_name': 'name', 'value': 'Field G'}],
            },
            auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
        )
    else:
        field = await effects_site.call('schema_field_create', 'Asset', 'text', 'Field G')
        await effects_site.call('schema_field_update', 'Asset', field, {'name': 'Renamed'})
    with pytest.raises(MockgunError):
        await effects_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
async def test_schema_writes_do_not_change_pickle_files_or_another_site(
    effects_site: FakeSiteCase,
) -> None:
    schema_path = Path(cast('str', mockgun.Shotgun._schema_path))
    entity_path = Path(cast('str', mockgun.Shotgun._schema_entity_path))
    before = (
        await asyncio.to_thread(schema_path.read_bytes),
        await asyncio.to_thread(entity_path.read_bytes),
    )
    field = await effects_site.call('schema_field_create', 'Asset', 'text', 'Field G')
    await effects_site.call('schema_field_update', 'Asset', field, {'description': 'about'})
    await effects_site.call('schema_field_delete', 'Asset', field)
    assert (
        await asyncio.to_thread(schema_path.read_bytes),
        await asyncio.to_thread(entity_path.read_bytes),
    ) == before
    client = (mockgun.Shotgun if effects_site.kind == 'sync' else mockgun.AsyncShotgun)(
        'http://unit-test.example', script_name='queries', api_key='key'
    )
    try:
        other = FakeSiteCase(effects_site.kind, client)
        assert field not in cast(
            'dict[str, object]', await other.call('schema_field_read', 'Asset')
        )
    finally:
        await FakeSiteCase(effects_site.kind, client).call('close')
