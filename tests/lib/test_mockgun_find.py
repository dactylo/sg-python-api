import copy
import datetime
import inspect
import json
import pickle
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from shotgun_api3 import Fault, ShotgunError, _protocol
from shotgun_api3 import Shotgun as ClientShotgun
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._server import FakeServer
from tests.conftest import entity_id
from tests.lib._requests import raw_request

from .conftest import SCHEMA, SCHEMA_ENTITY, FakeSiteCase, raw_read

if TYPE_CHECKING:
    from collections.abc import Callable


def _records(client: mockgun.Shotgun, entity_type: str) -> dict[object, dict[str, object]]:
    return {
        row['id']: row
        for row in client.find(entity_type, [], list(client.schema_field_read(entity_type)))
    }


@pytest.fixture(params=['sync', 'async'])
async def relation_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AsyncIterator[FakeSiteCase]:
    schema = copy.deepcopy(SCHEMA)
    shot = cast('dict[str, object]', schema['Shot'])
    for name, data_type in (
        ('sg_cut_order', 'number'),
        ('start_date', 'date'),
        ('updated_at', 'date_time'),
        ('color', 'color'),
        ('uploaded_movie', 'url'),
    ):
        shot[name] = {
            'data_type': {'value': data_type},
            'properties': {'default_value': {'value': None}},
        }
    schema_path = tmp_path / 'relation-schema.pickle'
    entity_path = tmp_path / 'relation-entities.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=2))
    entity_path.write_bytes(pickle.dumps(SCHEMA_ENTITY, protocol=2))

    class RelationShotgun(mockgun.Shotgun):
        pass

    class RelationAsyncShotgun(mockgun.AsyncShotgun):
        pass

    RelationShotgun.set_schema_paths(str(schema_path), str(entity_path))
    await RelationAsyncShotgun.set_schema_paths(str(schema_path), str(entity_path))
    kind = str(request.param)
    client = (RelationShotgun if kind == 'sync' else RelationAsyncShotgun)(
        'https://mockgun.example', script_name='fixture', api_key='secret'
    )
    case = FakeSiteCase(kind, client)
    yield case
    await case.call('close')


def test_checkbox_relations_reject_null_operands_and_stored_nulls(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('ScalarTypes', {'checkbox': True})
    mockgun_client.create('ScalarTypes', {'checkbox': False})
    omitted = mockgun_client.create('ScalarTypes', {})
    with pytest.raises(mockgun.MockgunError, match=r'ScalarTypes\.checkbox'):
        mockgun_client.find('ScalarTypes', [['checkbox', 'is', True]])
    mockgun_client.delete('ScalarTypes', entity_id(omitted))
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['checkbox', 'is', True]])
    ] == [1]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['checkbox', 'is_not', True]])
    ] == [2]
    with pytest.raises(Fault):
        mockgun_client.find('ScalarTypes', [['checkbox', 'is', None]])


def test_dotted_filters_traverse_links_and_return_fields_omit_multi_entity_paths(
    mockgun_client: mockgun.Shotgun,
) -> None:
    project = mockgun_client.create('Project', {'name': 'Alpha'})
    first = mockgun_client.create('HumanUser', {'login': 'FIRST', 'project': project})
    second = mockgun_client.create('HumanUser', {'login': 'second'})
    mockgun_client.create(
        'Shot',
        {'code': 'linked', 'project': project, 'reviewers': [first, second]},
    )
    fields = [
        'project.Project.name',
        'reviewers.HumanUser.login',
        'reviewers.HumanUser.project.Project.name',
    ]
    filters = [
        {
            'filter_operator': 'all',
            'filters': [
                ['reviewers.HumanUser.login', 'contains', 'first'],
                ['reviewers.HumanUser.project.Project.name', 'is', 'ALPHA'],
            ],
        }
    ]
    original_fields = copy.deepcopy(fields)
    original_filters = copy.deepcopy(filters)

    result = mockgun_client.find('Shot', filters, fields)

    assert result == [
        {
            'id': 1,
            'project.Project.name': 'Alpha',
            'type': 'Shot',
        }
    ]
    assert fields == original_fields
    assert filters == original_filters


def test_dotted_traversal_handles_none_missing_and_requested_type_selection(
    mockgun_client: mockgun.Shotgun,
    tmp_path: Path,
) -> None:
    schema = cast('dict[str, dict[str, Any]]', mockgun_client.schema_read())
    schema['Shot']['reviewers']['properties']['valid_types']['value'].append('Project')
    schema_path = tmp_path / 'mixed-schema.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=2))
    _, entity_path = mockgun.Shotgun.get_schema_paths()
    assert entity_path is not None

    class MixedShotgun(mockgun.Shotgun):
        pass

    MixedShotgun.set_schema_paths(str(schema_path), entity_path)
    client = MixedShotgun('https://mockgun.example', script_name='fixture', api_key='secret')
    project = client.create('Project', {'name': 'named'})
    user = client.create('HumanUser', {'login': 'user'})
    client.create('Shot', {'code': 'none'})
    client.create('Shot', {'code': 'mixed', 'project': project, 'reviewers': [project, user]})
    assert client.find(
        'Shot',
        [],
        [
            'project.Project.name',
            'reviewers.HumanUser.login',
            'reviewers.Project.name',
        ],
    ) == [
        {
            'type': 'Shot',
            'id': 1,
            'project.Project.name': None,
        },
        {
            'type': 'Shot',
            'id': 2,
            'project.Project.name': 'named',
        },
    ]


def test_entity_relations_use_identity_and_case_insensitive_display_names(
    mockgun_client: mockgun.Shotgun,
) -> None:
    alpha = mockgun_client.create('Project', {'name': 'AlphaProject'})
    beta = mockgun_client.create('Project', {'name': 'BetaProject'})
    mockgun_client.create('Shot', {'code': 'alpha', 'project': alpha})
    mockgun_client.create('Shot', {'code': 'beta', 'project': beta})
    alpha_with_extra = {**alpha, 'name': 'ignored'}

    expected_ids = {
        'is': [1],
        'is_not': [2],
        'type_is': [1, 2],
        'type_is_not': [],
        'name_contains': [1],
        'name_not_contains': [2],
    }
    filters = {
        'is': ['project', 'is', alpha_with_extra],
        'is_not': ['project', 'is_not', alpha_with_extra],
        'type_is': ['project', 'type_is', 'Project'],
        'type_is_not': ['project', 'type_is_not', 'Project'],
        'name_contains': ['project', 'name_contains', 'alpha'],
        'name_not_contains': ['project', 'name_not_contains', 'Alpha'],
    }

    for relation, condition in filters.items():
        assert [row['id'] for row in mockgun_client.find('Shot', [condition])] == expected_ids[
            relation
        ]

    for relation in ('name_starts_with', 'name_ends_with'):
        with pytest.raises(Fault, match=f'entity does not support {relation}'):
            mockgun_client.find('Shot', [['project', relation, 'Alpha']])

    assert [
        row['id']
        for row in mockgun_client.find('Shot', [['project', 'in', [alpha, alpha_with_extra]]])
    ] == [1]
    assert [
        row['id'] for row in mockgun_client.find('Shot', [['project', 'in', [alpha, beta]]])
    ] == [1, 2]
    empty_entities: list[dict[str, object]] = []
    with pytest.raises(mockgun.MockgunError, match=r'Shot\.project.*entity.*in'):
        mockgun_client.find('Shot', [['project', 'in', empty_entities]])
    mockgun_client.create('Shot', {'code': 'none'})
    assert [row['id'] for row in mockgun_client.find('Shot', [['project', 'is', None]])] == [3]
    assert [row['id'] for row in mockgun_client.find('Shot', [['project', 'is_not', None]])] == [
        1,
        2,
    ]
    assert _records(mockgun_client, 'Shot')[1]['project'] == {
        'type': 'Project',
        'id': 1,
        'name': 'AlphaProject',
    }


def test_entity_type_supports_only_case_sensitive_is(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('ScalarTypes', {'entity_type': 'Shot'})
    mockgun_client.create('ScalarTypes', {'entity_type': 'shot'})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['entity_type', 'is', 'Shot']])
    ] == [1]
    with pytest.raises(mockgun.MockgunError, match=r'entity_type.*is_not'):
        mockgun_client.find('ScalarTypes', [['entity_type', 'is_not', 'Shot']])


def test_find_and_find_one_match_the_public_client_signatures() -> None:
    for method_name in ('find', 'find_one'):
        client_method = getattr(ClientShotgun, method_name)
        mockgun_method = getattr(mockgun.Shotgun, method_name)
        expected = inspect.signature(client_method)
        actual = inspect.signature(mockgun_method)

        assert tuple(actual.parameters) == tuple(expected.parameters)
        for name, expected_parameter in expected.parameters.items():
            actual_parameter = actual.parameters[name]
            assert actual_parameter.kind is expected_parameter.kind
            assert actual_parameter.annotation == expected_parameter.annotation
            assert actual_parameter.default is expected_parameter.default
        assert actual.return_annotation == expected.return_annotation


def test_find_compiles_nested_filters_once_without_mutating_inputs(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mockgun_client.create('Shot', {'code': 'Alpha', 'sg_status_list': 'ip'})
    mockgun_client.create('Shot', {'code': 'Beta', 'sg_status_list': 'fin'})
    filters: dict[str, object] = {
        'filter_operator': 'any',
        'filters': (
            ('code', 'is', 'ALPHA'),
            {
                'filter_operator': 'all',
                'filters': (('sg_status_list', 'is', 'fin'),),
            },
        ),
    }
    fields = ['code']
    original_filters = copy.deepcopy(filters)
    original_fields = copy.deepcopy(fields)
    translate_filters = _protocol.translate_filters
    compiler_calls: list[tuple[object, str | None, bool]] = []

    def capture_compiler_call(
        source_filters: list[object] | tuple[object, ...] | dict[str, object],
        filter_operator: str | None = None,
        optimize: bool = True,
    ) -> dict[str, object]:
        compiler_calls.append((source_filters, filter_operator, optimize))
        return translate_filters(source_filters, filter_operator, optimize)

    monkeypatch.setattr(_protocol, 'translate_filters', capture_compiler_call)

    result = mockgun_client.find('Shot', filters, fields)

    assert result == [
        {'code': 'Alpha', 'id': 1, 'type': 'Shot'},
        {'code': 'Beta', 'id': 2, 'type': 'Shot'},
    ]
    assert mockgun_client.finds == 1
    assert compiler_calls == [(filters, None, True)]
    assert filters == original_filters
    assert fields == original_fields

    result[0]['code'] = 'changed'
    assert _records(mockgun_client, 'Shot')[1]['code'] == 'Alpha'


def test_find_counter_increments_before_validation_errors(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(ShotgunError, match=r'MissingEntity.*valid entity'):
        mockgun_client.find('MissingEntity', [])

    assert mockgun_client.finds == 1


def test_find_empty_group_matches_all_rows(mockgun_client: mockgun.Shotgun) -> None:
    assert mockgun_client.find('Shot', [{}]) == []
    created = mockgun_client.create('Shot', {'code': 'present'})
    assert mockgun_client.find('Shot', [{}], ['code']) == [created]


def test_find_flattens_explicit_list_as_scalar_relation_operand(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})

    assert mockgun_client.find('Shot', [['code', 'is', ['keep']]], ['code']) == [
        {'type': 'Shot', 'id': 1, 'code': 'keep'},
    ]


def test_find_flattens_scalar_list_operand_in_nested_filters(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = {
        'filter_operator': 'all',
        'filters': [
            {
                'filter_operator': 'any',
                'filters': [['code', 'is', ['keep']]],
            }
        ],
    }

    assert mockgun_client.find('Shot', filters, ['code']) == [
        {'type': 'Shot', 'id': 1, 'code': 'keep'},
    ]


def test_find_group_matching_and_nonmatching_leaves(mockgun_client: mockgun.Shotgun) -> None:
    mockgun_client.create('Shot', {'code': 'present'})
    mockgun_client.create('Shot', {'code': 'other'})
    conditions = [['code', 'is', 'missing']]
    filters = {'filter_operator': 'all', 'filters': conditions}
    assert mockgun_client.find('Shot', filters) == []
    conditions[0][2] = 'present'
    assert mockgun_client.find('Shot', filters, ['code']) == [
        {'code': 'present', 'id': 1, 'type': 'Shot'}
    ]


def test_find_membership_without_operand_uses_empty_values(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    assert mockgun_client.find('Shot', [['code', 'not_in']], ['code']) == [
        {'type': 'Shot', 'id': 1, 'code': 'keep'},
    ]
    assert mockgun_client.find('Shot', [['code', 'in']], ['code']) == []


def test_find_nested_all_and_any_groups(mockgun_client: mockgun.Shotgun) -> None:
    for code, status in (('Alpha', 'ip'), ('Alpha', 'fin'), ('Beta', 'fin')):
        mockgun_client.create('Shot', {'code': code, 'sg_status_list': status})
    filters = {
        'filter_operator': 'any',
        'filters': [
            {
                'filter_operator': 'all',
                'filters': [
                    ['code', 'is', 'alpha'],
                    ['sg_status_list', 'is', 'ip'],
                ],
            },
            {
                'filter_operator': 'all',
                'filters': [
                    ['code', 'is', 'beta'],
                    ['sg_status_list', 'is', 'fin'],
                ],
            },
        ],
    }
    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == [1, 3]


def test_find_nested_all_validates_malformed_nodes_before_matching(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = [{'filter_operator': 'all', 'filters': [['code', 'is', 'keep'], 42]}]
    with pytest.raises(Fault, match='filter conditions must be lists or tuples'):
        mockgun_client.find('Shot', filters)


def test_find_nested_any_short_circuits_then_rejects_invalid_node(
    mockgun_client: mockgun.Shotgun,
) -> None:
    created = mockgun_client.create('Shot', {'code': 'keep'})
    filters = [
        {
            'filter_operator': 'any',
            'filters': [
                ['code', 'is', 'missing'],
                ['code', 'is', 'keep'],
            ],
        }
    ]
    original_filters = copy.deepcopy(filters)

    assert mockgun_client.find('Shot', filters) == [
        {'id': created['id'], 'type': 'Shot'},
    ]
    assert filters == original_filters

    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', [42])

    assert type(caught.value) is Fault
    assert str(caught.value) == (
        'filter conditions must be lists or tuples with a path and relation'
    )


def test_find_nested_any_validates_malformed_nodes_before_matching(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = [{'filter_operator': 'any', 'filters': [['code', 'is', 'keep'], 42]}]
    with pytest.raises(Fault, match='filter conditions must be lists or tuples'):
        mockgun_client.find('Shot', filters)


def test_find_one_applies_filter_order_and_retired_scope(mockgun_client: mockgun.Shotgun) -> None:
    for code in ('one', 'other', 'one'):
        created = mockgun_client.create('Shot', {'code': code})
        assert isinstance(created['id'], int)
        mockgun_client.delete('Shot', created['id'])
    assert mockgun_client.find_one(
        'Shot',
        [('code', 'is', 'one')],
        ['code'],
        [{'field_name': 'id', 'direction': 'desc'}],
        'any',
        retired_only=True,
        include_archived_projects=False,
    ) == {'type': 'Shot', 'id': 3, 'code': 'one'}


def test_find_partitions_retired_rows_and_accepts_the_archive_option(
    mockgun_client: mockgun.Shotgun,
) -> None:
    for code in ('one', 'two', 'three'):
        mockgun_client.create('Shot', {'code': code})
    mockgun_client.delete('Shot', 2)

    active = mockgun_client.find(
        'Shot',
        [],
        ['code'],
        limit=1,
        page=9,
        include_archived_projects=False,
    )
    retired = mockgun_client.find('Shot', [], ['code'], retired_only=True)

    assert active == []
    assert [row['code'] for row in mockgun_client.find('Shot', [], ['code'], limit=1, page=2)] == [
        'three'
    ]
    assert [row['code'] for row in retired] == ['two']


def test_find_rejects_missing_scalar_operand_before_rows(mockgun_client: mockgun.Shotgun) -> None:
    with pytest.raises(Fault, match='is requires exactly 1 value'):
        mockgun_client.find('Shot', [('code', 'is')])


def test_find_rejects_unknown_filter_field_before_rows(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(ShotgunError, match="doesn't exist"):
        mockgun_client.find('Shot', [['missing', 'is', None]])


def test_find_resolves_self_linked_fields_in_linear_traversal(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    schema = cast('dict[str, dict[str, Any]]', mockgun_client.schema_read())
    schema['Shot']['parent'] = {
        'data_type': {'value': 'entity'},
        'properties': {'default_value': {'value': None}, 'valid_types': {'value': ['Shot']}},
    }
    schema_path = tmp_path / 'linked-schema.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=2))
    _, entity_path = mockgun.Shotgun.get_schema_paths()
    assert entity_path is not None

    class LinkedShotgun(mockgun.Shotgun):
        pass

    LinkedShotgun.set_schema_paths(str(schema_path), entity_path)
    client = LinkedShotgun('https://mockgun.example', script_name='fixture', api_key='secret')
    original_field_type = SiteSchema.field_type
    visits = 0

    def count_field_type(schema: SiteSchema, entity_type: str, field_name: str) -> str:
        nonlocal visits
        visits += 1
        return original_field_type(schema, entity_type, field_name)

    monkeypatch.setattr(SiteSchema, 'field_type', count_field_type)
    depths = (2, 8, 12)
    assert client.find('Shot', [], ['code.MissingEntity.id']) == []
    assert client.find('Shot', [], ['parent.MissingEntity.id']) == []
    client.create('Shot', {'code': 'root'})
    observed_visits: list[int] = []
    for depth in depths:
        visits = 0
        path = 'parent.Shot.' * depth + 'id'
        assert client.find('Shot', [], [path]) == [{'type': 'Shot', 'id': 1, path: None}]
        observed_visits.append(visits)
    assert all(
        0 < observed <= depth + 1 for depth, observed in zip(depths, observed_visits, strict=True)
    )


def test_find_returns_independent_direct_and_linked_values(
    mockgun_client: mockgun.Shotgun,
) -> None:
    project = mockgun_client.create('Project', {'name': 'project'})
    user = mockgun_client.create('HumanUser', {'login': 'user'})
    mockgun_client.create('Shot', {'project': project, 'reviewers': [user]})

    result = mockgun_client.find(
        'Shot',
        [],
        ['project', 'reviewers', 'project.Project.name'],
    )
    cast('dict[str, object]', result[0]['project'])['id'] = 999
    cast('list[dict[str, object]]', result[0]['reviewers'])[0]['id'] = 999

    assert _records(mockgun_client, 'Shot')[1]['project'] == {
        'type': 'Project',
        'id': 1,
        'name': 'project',
    }
    assert _records(mockgun_client, 'Shot')[1]['reviewers'] == [{'type': 'HumanUser', 'id': 1}]


def test_find_short_circuiting_does_not_skip_tree_validation(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = {
        'filter_operator': 'any',
        'filters': [
            ['code', 'is', 'keep'],
            ['code', 'type_is', 'Shot'],
        ],
    }

    with pytest.raises(Fault, match=r'text.*type_is'):
        mockgun_client.find('Shot', filters)


@pytest.mark.parametrize(
    ('operator', 'first', 'expected'),
    [
        ('all', 'drop', []),
        ('any', 'keep', [{'type': 'Shot', 'id': 1}]),
    ],
)
def test_find_short_circuits_before_unsettled_link(
    mockgun_client: mockgun.Shotgun,
    operator: str,
    first: str,
    expected: list[dict[str, object]],
) -> None:
    mockgun_client.create('Shot', {'code': 'keep', 'project': {'type': 'Project', 'id': 999}})
    unresolved = ['project.Project.name', 'is', 'missing']
    with pytest.raises(mockgun.MockgunError):
        mockgun_client.find('Shot', [unresolved])
    filters = {
        'filter_operator': operator,
        'filters': [
            ['code', 'is', first],
            unresolved,
        ],
    }
    assert mockgun_client.find('Shot', filters) == expected


@pytest.mark.parametrize('method_name', ['find', 'find_one'])
@pytest.mark.parametrize('fields', ['code', b'code'])
def test_find_rejects_string_fields(
    mockgun_client: mockgun.Shotgun, method_name: str, fields: object
) -> None:
    method = cast(
        'Callable[[str, list[object], object], object]', getattr(mockgun_client, method_name)
    )

    with pytest.raises(ShotgunError, match=r'fields'):
        method('Shot', [], fields)


@pytest.mark.parametrize('method_name', ['find', 'find_one'])
@pytest.mark.parametrize('fields', [['code'], ('code',)])
def test_find_accepts_string_field_sequences(
    mockgun_client: mockgun.Shotgun, method_name: str, fields: object
) -> None:
    created = mockgun_client.create('Shot', {'code': 'sh010'})
    method = cast(
        'Callable[[str, list[object], object], object]', getattr(mockgun_client, method_name)
    )

    result = method('Shot', [], fields)

    expected = {'type': 'Shot', 'id': created['id'], 'code': 'sh010'}
    assert result == ([expected] if method_name == 'find' else expected)


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    ('condition', 'message'),
    [
        (
            ['sg_cut_order', 'not_between', [1, 2]],
            'number does not support not_between. Valid relations:',
        ),
        (
            ['project', 'name_starts_with', 'x'],
            'entity does not support name_starts_with. Valid relations:',
        ),
        (['uploaded_movie', 'is', None], 'url data type cannot be used in a filter'),
    ],
)
async def test_filter_rejects_invalid_declared_relations(
    relation_site: FakeSiteCase,
    populated: bool,
    condition: list[object],
    message: str,
) -> None:
    if populated:
        await relation_site.call('create', 'Shot', {'code': 'x'})
    before = await relation_site.call('find', 'Shot', [])
    with pytest.raises(Fault) as caught:
        await relation_site.call('find', 'Shot', [condition])
    assert message in str(caught.value)
    assert await relation_site.call('find', 'Shot', []) == before


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    ('condition', 'message'),
    [
        (['updated_at', 'not_in', cast('list[object]', [])], 'date_time.*not_in'),
        (['start_date', 'in_last', 0, 'DAY'], 'date.*in_last'),
        (['updated_at', 'in_next', 1.5, 'DAY'], 'date_time.*in_next'),
        (['updated_at', 'is', 'not-a-date'], 'date_time.*not-a-date'),
        (['updated_at', 'is', '0001-01-01T00:00:00+01:00'], 'date_time.*out of range'),
        (['updated_at', 'is', '9999-12-31T23:59:59-01:00'], 'date_time.*out of range'),
    ],
)
async def test_filter_missing_emulation_escapes_client_fault_handling(
    relation_site: FakeSiteCase,
    populated: bool,
    condition: list[object],
    message: str,
) -> None:
    if populated:
        await relation_site.call('create', 'Shot', {'code': 'x'})
    before = await relation_site.call('find', 'Shot', [])

    async def find_with_client_fault_trap() -> None:
        try:
            await relation_site.call('find', 'Shot', [condition])
        except ShotgunError:
            pytest.fail('missing emulation became a client fault')

    with pytest.raises(mockgun.MockgunError, match=message):
        await find_with_client_fault_trap()
    assert await relation_site.call('find', 'Shot', []) == before


def test_find_skips_from_conditions_recursively(mockgun_client: mockgun.Shotgun) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    mockgun_client.create('Shot', {'code': 'drop'})
    from_condition = ['$FROM$Shot.id', 'is', 999]
    filters = {
        'filter_operator': 'all',
        'filters': [
            from_condition,
            {'filter_operator': 'any', 'filters': [from_condition, ['code', 'is', 'keep']]},
        ],
    }
    assert mockgun_client.find('Shot', filters, ['code']) == [
        {'code': 'keep', 'id': 1, 'type': 'Shot'}
    ]


def test_multi_entity_relations_match_members_by_type_and_id(
    mockgun_client: mockgun.Shotgun,
) -> None:
    first = mockgun_client.create('HumanUser', {'login': 'first'})
    second = mockgun_client.create('HumanUser', {'login': 'second'})
    mockgun_client.create('Shot', {'code': 'linked', 'reviewers': [first]})
    empty = mockgun_client.create('Shot', {'code': 'empty', 'reviewers': [second]})
    assert isinstance(empty['id'], int)
    mockgun_client.update('Shot', empty['id'], {'reviewers': []})
    same_id_wrong_type = {'type': 'Project', 'id': first['id']}

    assert [
        row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is', same_id_wrong_type]])
    ] == []
    assert [
        row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is_not', second]])
    ] == [1, 2]
    assert [row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is', None]])] == [2]
    assert [row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is_not', None]])] == [
        1
    ]


def test_order_first_clause_is_primary_with_id_ties(
    mockgun_client: mockgun.Shotgun,
) -> None:
    for code, status in (('B', 'z'), ('A', 'z'), ('C', 'a')):
        mockgun_client.create('Shot', {'code': code, 'sg_status_list': status})
    order = [
        {'field_name': 'code', 'direction': 'asc'},
        {'field_name': 'sg_status_list', 'direction': 'asc'},
    ]
    original_order = copy.deepcopy(order)

    ascending = mockgun_client.find('Shot', [], ['code'], order)
    descending = mockgun_client.find(
        'Shot',
        [],
        ['code'],
        [{'field_name': 'code', 'direction': 'desc'}],
    )

    assert [row['code'] for row in ascending] == ['A', 'B', 'C']
    assert [row['code'] for row in descending] == ['C', 'B', 'A']
    assert order == original_order


def test_order_requires_field_name_and_defaults_to_ascending(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(ShotgunError, match="order item 0 is missing 'field_name'"):
        mockgun_client.find('Shot', [], order=[{'direction': 'asc'}])
    for code in ('b', 'a'):
        mockgun_client.create('Shot', {'code': code})
    assert [
        row['code'] for row in mockgun_client.find('Shot', [], ['code'], [{'field_name': 'code'}])
    ] == ['a', 'b']


def test_order_supports_dotted_fields_and_none_links(mockgun_client: mockgun.Shotgun) -> None:
    second = mockgun_client.create('Project', {'name': 'second'})
    first = mockgun_client.create('Project', {'name': 'first'})
    mockgun_client.create('Shot', {'code': 'second', 'project': second})
    mockgun_client.create('Shot', {'code': 'none'})
    mockgun_client.create('Shot', {'code': 'first', 'project': first})

    result = mockgun_client.find(
        'Shot',
        [],
        ['code'],
        [{'field_name': 'project.Project.name', 'direction': 'asc'}],
    )

    assert [row['code'] for row in result] == ['first', 'second', 'none']


@pytest.mark.parametrize(
    ('field', 'data_type', 'relation', 'values', 'message'),
    [
        (
            'sg_cut_order',
            'number',
            'not_between',
            [1, 2],
            (
                'does not support not_between. Valid relations: is, is_not, greater_than, '
                'less_than, between, in, not_in'
            ),
        ),
        (
            'project',
            'entity',
            'name_starts_with',
            ['x'],
            (
                'does not support name_starts_with. Valid relations: is, is_not, name_contains, '
                'name_not_contains, name_is, type_is, type_is_not, in, not_in'
            ),
        ),
        ('uploaded_movie', 'url', 'is', [None], 'data type cannot be used in a filter'),
    ],
)
def test_raw_filter_fault_code_for_declared_rejections(
    field: str,
    data_type: str,
    relation: str,
    values: list[object],
    message: str,
) -> None:
    schema = copy.deepcopy(SCHEMA)
    shot = cast('dict[str, object]', schema['Shot'])
    if field not in shot:
        shot[field] = {
            'data_type': {'value': data_type},
            'properties': {'default_value': {'value': None}},
        }
    server = FakeServer(
        SiteSchema(
            cast('dict[str, dict[str, object]]', schema),
            cast('dict[str, dict[str, object]]', SCHEMA_ENTITY),
        )
    )
    body = json.dumps(
        {
            'method_name': 'read',
            'params': [
                {
                    'type': 'Shot',
                    'return_fields': ['id'],
                    'filters': {
                        'logical_operator': 'and',
                        'conditions': [{'path': field, 'relation': relation, 'values': values}],
                    },
                    'paging': {'entities_per_page': 10, 'current_page': 1},
                }
            ],
        }
    ).encode()
    response = raw_request(server, 'POST', 'https://mockgun.example/api3/json', body=body)
    payload = json.loads(response.content)
    assert payload['error_code'] == 103
    assert payload['message'] == f'API read() Shot.{field} {data_type} {message}'


@pytest.mark.parametrize(
    ('link', 'missing'),
    [
        ({'type': 'HumanUser', 'id': 1}, False),
        ({'type': 'Project', 'id': 999}, True),
    ],
)
def test_dotted_return_fields_on_wrong_type_and_missing_links(
    mockgun_client: mockgun.Shotgun,
    link: dict[str, object],
    missing: bool,
) -> None:
    mockgun_client.create('HumanUser', {'login': 'user'})
    mockgun_client.create('Shot', {'code': 'invalid', 'project': link})
    if missing:
        with pytest.raises(mockgun.MockgunError):
            mockgun_client.find('Shot', [], ['project.Project.name'])
    else:
        assert mockgun_client.find('Shot', [], ['project.Project.name']) == [
            {'type': 'Shot', 'id': 1}
        ]


@pytest.mark.parametrize('method_name', ['find', 'find_one'])
@pytest.mark.parametrize(
    ('args', 'kwargs', 'suffix'),
    [
        ((), {}, 'entity_type'),
        (
            ('Asset', cast('object', []), None, None, None, False, True, None, 'extra'),
            {},
            'positional',
        ),
        (
            ('Asset', cast('object', [])),
            {'unexpected': True},
            'unexpected',
        ),
        (
            ('Asset', cast('object', [])),
            {'entity_type': 'Shot'},
            'entity_type',
        ),
    ],
)
def test_find_binding_errors_match_the_public_client(
    method_name: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    suffix: str,
) -> None:
    if method_name == 'find' and len(args) == 9:
        args = (*args[:5], 0, args[5], 0, *args[6:])
    expected_method = f'Shotgun.{method_name}()'

    for class_ in (ClientShotgun, mockgun.Shotgun):
        client = object.__new__(class_)
        method = cast('Callable[..., object]', getattr(client, method_name))
        with pytest.raises(TypeError) as caught:
            method(*args, **kwargs)

        assert expected_method in str(caught.value)
        assert suffix in str(caught.value)


@pytest.mark.parametrize(('operator', 'expected_ids'), [('all', [1, 2]), ('any', [])])
def test_find_skipped_conditions_use_empty_group_semantics(
    mockgun_client: mockgun.Shotgun,
    operator: str,
    expected_ids: list[int],
) -> None:
    mockgun_client.create('Shot', {'code': 'one'})
    mockgun_client.create('Shot', {'code': 'two'})
    filters = {'filter_operator': operator, 'filters': [['$FROM$Shot.id', 'is', 999]]}
    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == expected_ids


@pytest.mark.parametrize(
    'group',
    [
        {'filter_operator': 'all'},
        {'filters': []},
        {'extra': None, 'filters': []},
    ],
)
def test_find_groups_use_default_operator_and_empty_filters(
    mockgun_client: mockgun.Shotgun,
    group: dict[str, object],
) -> None:
    assert mockgun_client.find('Shot', [group]) == []
    created = mockgun_client.create('Shot', {'code': 'present'})
    assert mockgun_client.find('Shot', [group], ['code']) == [created]


@pytest.mark.parametrize(
    'condition',
    [
        ['text', 'is'],
        ['text', 'contains', 'a', 'b'],
        ['number', 'between', 1],
        ['number', 'between', 1, 2, 3],
        ['checkbox', 'in', [True]],
    ],
)
def test_invalid_arity_and_unsupported_field_relations_fail_without_rows(
    mockgun_client: mockgun.Shotgun,
    condition: list[object],
) -> None:
    with pytest.raises(Fault):
        mockgun_client.find('ScalarTypes', [condition])


@pytest.mark.parametrize('field_name', ['list', 'status_list'])
def test_list_field_relations_ignore_case(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
) -> None:
    mockgun_client.create('ScalarTypes', {field_name: 'Alpha'})
    mockgun_client.create('ScalarTypes', {field_name: 'beta'})
    mockgun_client.create('ScalarTypes', {})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'is', 'Alpha']])
    ] == [1]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'is', 'alpha']])
    ] == [1]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'in', ['Alpha', 'missing']]])
    ] == [1]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'not_in', ['Alpha']]])
    ] == [2, 3]


@pytest.mark.parametrize(
    ('field_name', 'values'),
    [
        ('float', (1.5, 2.5, 3.5)),
        ('number', (1, 2, 3)),
        ('date', ('2026-07-01', '2026-07-02', '2026-07-03')),
        (
            'date_time',
            (
                datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC),
                datetime.datetime(2026, 7, 2, tzinfo=datetime.UTC),
                datetime.datetime(2026, 7, 3, tzinfo=datetime.UTC),
            ),
        ),
    ],
)
def test_numeric_date_and_datetime_relations(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
    values: tuple[object, object, object],
) -> None:
    low, middle, high = values
    for value in values:
        mockgun_client.create('ScalarTypes', {field_name: value})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'is', middle]])
    ] == [2]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'is_not', middle]])
    ] == [1, 3]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'less_than', middle]])
    ] == [1]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'greater_than', middle]])
    ] == [3]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'between', low, middle]])
    ] == [1, 2]
    with pytest.raises(Fault, match=f'{field_name} does not support not_between'):
        mockgun_client.find('ScalarTypes', [[field_name, 'not_between', low, middle]])
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'in', [low, high]]])
    ] == [1, 3]


@pytest.mark.parametrize('field_name', ['project', 'reviewers'])
def test_order_on_entity_and_multi_entity_fields(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
) -> None:
    project = mockgun_client.create('Project', {'name': 'project'})
    user = mockgun_client.create('HumanUser', {'login': 'user'})
    other_user = mockgun_client.create('HumanUser', {'login': 'other'})
    first_value = project if field_name == 'project' else [user]
    second_value = project if field_name == 'project' else [other_user]
    mockgun_client.create('Shot', {field_name: first_value})
    mockgun_client.create('Shot', {field_name: second_value})

    order = [{'field_name': field_name, 'direction': 'asc'}]
    if field_name == 'project':
        assert [row['id'] for row in mockgun_client.find('Shot', [], order=order)] == [1, 2]
    else:
        with pytest.raises(mockgun.MockgunError, match=r'Shot.reviewers'):
            mockgun_client.find('Shot', [], order=order)


@pytest.mark.parametrize('field_name', ['checkbox', 'serializable', 'url'])
def test_order_on_checkbox_serializable_and_url_fields(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
) -> None:
    value: object = True if field_name == 'checkbox' else {'value': 1}
    mockgun_client.create('ScalarTypes', {field_name: value})
    mockgun_client.create('ScalarTypes', {})

    order = [{'field_name': field_name, 'direction': 'asc'}]
    if field_name == 'url':
        assert [row['id'] for row in mockgun_client.find('ScalarTypes', [], order=order)] == [1, 2]
    else:
        with pytest.raises(mockgun.MockgunError, match=rf'ScalarTypes.{field_name}'):
            mockgun_client.find('ScalarTypes', [], order=order)


@pytest.mark.parametrize(
    ('field_name', 'values', 'ascending_ids', 'descending_ids'),
    [
        ('number', (-1, 2, None), [1, 2, 3], [2, 1, 3]),
        ('text', ('b', None, 'a'), [3, 1, 2], [1, 3, 2]),
        (
            'date_time',
            (
                datetime.datetime.fromisoformat('2026-01-02T00:00:00'),
                None,
                datetime.datetime.fromisoformat('2026-01-01T00:00:00'),
            ),
            [3, 1, 2],
            [1, 3, 2],
        ),
    ],
)
def test_order_places_nulls_last_in_both_directions(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
    values: tuple[object, object, object],
    ascending_ids: list[int],
    descending_ids: list[int],
) -> None:
    for value in values:
        mockgun_client.create('ScalarTypes', {} if value is None else {field_name: value})

    ascending = mockgun_client.find(
        'ScalarTypes',
        [],
        order=[{'field_name': field_name, 'direction': 'asc'}],
    )
    descending = mockgun_client.find(
        'ScalarTypes',
        [],
        order=[{'field_name': field_name, 'direction': 'desc'}],
    )

    assert [row['id'] for row in ascending] == ascending_ids
    assert [row['id'] for row in descending] == descending_ids


@pytest.mark.parametrize('filters', [None, 42, 'invalid'])
def test_find_rejects_noncontainer_filters_before_rows(
    mockgun_client: mockgun.Shotgun,
    filters: object,
) -> None:
    with pytest.raises(Fault, match='filters must be a list or tuple'):
        mockgun_client.find('Shot', cast('Any', filters))


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('route', ['client', 'raw'])
@pytest.mark.parametrize(
    ('field', 'relation'),
    [
        ('project', 'not_in'),
        ('reviewers', 'in'),
        ('reviewers', 'not_in'),
        ('color', 'in'),
        ('color', 'not_in'),
        ('sg_cut_order', 'in'),
        ('project', 'in'),
    ],
)
async def test_null_membership_fault_precedes_missing_emulation(
    relation_site: FakeSiteCase,
    populated: bool,
    route: str,
    field: str,
    relation: str,
) -> None:
    if populated:
        await relation_site.call('create', 'Shot', {'code': 'present'})
    before = await relation_site.call('find', 'Shot', [])
    if route == 'client':
        with pytest.raises(Fault, match=r'API read\(\) Read failed for entity type \[Shot\]'):
            await relation_site.call('find', 'Shot', [[field, relation, [None]]])
    else:
        result = raw_read(
            relation_site.client._server, 'Shot', [(field, relation, [None])], auth={}
        )
        assert result['error_code'] == 104
        assert result['message'] == 'API read() Read failed for entity type [Shot]'
    assert await relation_site.call('find', 'Shot', []) == before


@pytest.mark.anyio
async def test_mockgun_async_find_errors_name_async_shotgun(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client  # The fixture configures the shared schema paths.
    client = mockgun.AsyncShotgun('https://mockgun.example', connect=False)
    find = cast('Callable[..., Any]', client.find)
    with pytest.raises(TypeError) as caught:
        await find('Shot', [], misspelled=True)
    assert 'AsyncShotgun.find()' in str(caught.value)
    assert 'misspelled' in str(caught.value)
    assert client.finds == 0
    await client.close()


def test_text_relations_are_case_insensitive_and_define_none_behavior(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('ScalarTypes', {'text': 'Alpha'})
    mockgun_client.create('ScalarTypes', {'text': 'BETA'})
    mockgun_client.create('ScalarTypes', {})

    expected_ids = {
        'is': [1],
        'is_not': [2, 3],
        'in': [1],
        'not_in': [2, 3],
        'contains': [1],
        'not_contains': [2, 3],
        'starts_with': [1],
        'ends_with': [2],
    }
    filters = {
        'is': ['text', 'is', 'ALPHA'],
        'is_not': ['text', 'is_not', 'ALPHA'],
        'in': ['text', 'in', ['alpha', 'missing']],
        'not_in': ['text', 'not_in', ['ALPHA']],
        'contains': ['text', 'contains', 'PH'],
        'not_contains': ['text', 'not_contains', 'PH'],
        'starts_with': ['text', 'starts_with', 'al'],
        'ends_with': ['text', 'ends_with', 'ta'],
    }

    for relation, condition in filters.items():
        assert [
            row['id'] for row in mockgun_client.find('ScalarTypes', [condition])
        ] == expected_ids[relation]
