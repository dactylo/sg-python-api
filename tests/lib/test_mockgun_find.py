import copy
import datetime
import inspect
from typing import TYPE_CHECKING, Any, cast, get_overloads

import pytest

from shotgun_api3 import Shotgun as ClientShotgun
from shotgun_api3 import ShotgunError, _protocol
from shotgun_api3.lib import mockgun

if TYPE_CHECKING:
    from collections.abc import Callable


class _EqualitySpoofingOperator:
    def __init__(self) -> None:
        self.equality_calls = 0

    def __eq__(self, _other: object) -> bool:
        self.equality_calls += 1
        return True

    def __format__(self, _format_spec: str) -> str:
        raise AssertionError('operator formatting must not be invoked')

    def __hash__(self) -> int:
        return object.__hash__(self)

    def __repr__(self) -> str:
        raise AssertionError('operator repr must not be invoked')

    def __str__(self) -> str:
        raise AssertionError('operator str must not be invoked')


class _HostileFilterKey:
    def __eq__(self, _other: object) -> bool:
        raise AssertionError('filter key equality must not be invoked')

    def __format__(self, _format_spec: str) -> str:
        raise AssertionError('filter key formatting must not be invoked')

    def __hash__(self) -> int:
        return hash('filter_operator')

    def __repr__(self) -> str:
        raise AssertionError('filter key repr must not be invoked')

    def __str__(self) -> str:
        raise AssertionError('filter key str must not be invoked')


class _HostileRepresentationOperator:
    def __init__(self) -> None:
        self.equality_calls = 0

    def __eq__(self, _other: object) -> bool:
        self.equality_calls += 1
        return False

    def __format__(self, _format_spec: str) -> str:
        raise AssertionError('operator formatting must not be invoked')

    def __hash__(self) -> int:
        return object.__hash__(self)

    def __repr__(self) -> str:
        raise AssertionError('operator repr must not be invoked')

    def __str__(self) -> str:
        raise AssertionError('operator str must not be invoked')


class _HostileStringFilterKey(str):
    __slots__ = ()

    def __eq__(self, _other: object) -> bool:
        raise AssertionError('filter key equality must not be invoked')

    def __format__(self, _format_spec: str) -> str:
        raise AssertionError('filter key formatting must not be invoked')

    def __hash__(self) -> int:
        return str.__hash__(self)

    def __repr__(self) -> str:
        raise AssertionError('filter key repr must not be invoked')

    def __str__(self) -> str:
        raise AssertionError('filter key str must not be invoked')


class _HostileStringOperator(str):
    __slots__ = ()

    def __eq__(self, _other: object) -> bool:
        raise AssertionError('operator equality must not be invoked')

    def __format__(self, _format_spec: str) -> str:
        raise AssertionError('operator formatting must not be invoked')

    def __hash__(self) -> int:
        return str.__hash__(self)

    def __repr__(self) -> str:
        raise AssertionError('operator repr must not be invoked')

    def __str__(self) -> str:
        raise AssertionError('operator str must not be invoked')


def _database(client: mockgun.Shotgun) -> dict[str, dict[int, dict[str, Any]]]:
    return cast('dict[str, dict[int, dict[str, Any]]]', vars(client)['_db'])


def test_checkbox_relations_and_none_behavior(mockgun_client: mockgun.Shotgun) -> None:
    mockgun_client.create('ScalarTypes', {'checkbox': True})
    mockgun_client.create('ScalarTypes', {'checkbox': False})
    mockgun_client.create('ScalarTypes', {})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['checkbox', 'is', True]])
    ] == [1]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['checkbox', 'is_not', True]])
    ] == [2, 3]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['checkbox', 'is', None]])
    ] == [3]


def test_dotted_filters_and_return_fields_traverse_single_multi_and_recursive_links(
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
            'reviewers.HumanUser.login': ['FIRST', 'second'],
            'reviewers.HumanUser.project.Project.name': ['Alpha', None],
            'type': 'Shot',
        }
    ]
    assert fields == original_fields
    assert filters == original_filters


@pytest.mark.parametrize(
    ('stored_value', 'error_type'),
    [
        ({'type': 'HumanUser', 'id': 1}, ShotgunError),
        ('not-a-link', ShotgunError),
        ({'type': 'Project', 'id': 999}, KeyError),
    ],
)
def test_dotted_single_link_errors_match_the_source_boundary(
    mockgun_client: mockgun.Shotgun,
    stored_value: object,
    error_type: type[Exception],
) -> None:
    mockgun_client.create('HumanUser', {'login': 'user'})
    mockgun_client.create('Shot', {'code': 'invalid'})
    _database(mockgun_client)['Shot'][1]['project'] = stored_value

    with pytest.raises(error_type):
        mockgun_client.find('Shot', [], ['project.Project.name'])


def test_dotted_traversal_handles_none_missing_and_requested_type_selection(
    mockgun_client: mockgun.Shotgun,
) -> None:
    project = mockgun_client.create('Project', {'name': 'present'})
    user = mockgun_client.create('HumanUser', {'login': 'user'})
    mockgun_client.create('Shot', {'code': 'none'})
    mockgun_client.create('Shot', {'code': 'mixed', 'project': project, 'reviewers': [user]})
    assert isinstance(project['id'], int)
    _database(mockgun_client)['Project'][project['id']].pop('name')
    _database(mockgun_client)['Shot'][2]['reviewers'].insert(
        0,
        {'type': 'Project', 'id': project['id']},
    )

    result = mockgun_client.find(
        'Shot',
        [],
        [
            'project.Project.name',
            'reviewers.HumanUser.login',
            'reviewers.Project.name',
        ],
    )

    assert result == [
        {
            'id': 1,
            'project.Project.name': None,
            'reviewers.HumanUser.login': [],
            'reviewers.Project.name': [],
            'type': 'Shot',
        },
        {
            'id': 2,
            'project.Project.name': None,
            'reviewers.HumanUser.login': ['user'],
            'reviewers.Project.name': [None],
            'type': 'Shot',
        },
    ]


def test_entity_relations_use_identity_and_case_sensitive_resolved_names(
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
        'name_starts_with': [1],
        'name_ends_with': [1, 2],
    }
    filters = {
        'is': ['project', 'is', alpha_with_extra],
        'is_not': ['project', 'is_not', alpha_with_extra],
        'type_is': ['project', 'type_is', 'Project'],
        'type_is_not': ['project', 'type_is_not', 'Project'],
        'name_contains': ['project', 'name_contains', 'Alpha'],
        'name_not_contains': ['project', 'name_not_contains', 'Alpha'],
        'name_starts_with': ['project', 'name_starts_with', 'Alpha'],
        'name_ends_with': ['project', 'name_ends_with', 'Project'],
    }

    for relation, condition in filters.items():
        assert [row['id'] for row in mockgun_client.find('Shot', [condition])] == expected_ids[
            relation
        ]

    assert [
        row['id']
        for row in mockgun_client.find('Shot', [['project', 'in', [alpha, alpha_with_extra]]])
    ] == [1]
    assert mockgun_client.find('Shot', [['project', 'in', [alpha, beta]]]) == []
    empty_entities: list[dict[str, object]] = []
    assert [
        row['id'] for row in mockgun_client.find('Shot', [['project', 'in', empty_entities]])
    ] == [1, 2]
    mockgun_client.create('Shot', {'code': 'none'})
    assert [row['id'] for row in mockgun_client.find('Shot', [['project', 'is', None]])] == [3]
    assert [row['id'] for row in mockgun_client.find('Shot', [['project', 'is_not', None]])] == [
        1,
        2,
    ]
    assert _database(mockgun_client)['Shot'][1]['project'] == {'type': 'Project', 'id': 1}


def test_entity_type_supports_only_case_sensitive_is(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('ScalarTypes', {'entity_type': 'Shot'})
    mockgun_client.create('ScalarTypes', {'entity_type': 'shot'})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [['entity_type', 'is', 'Shot']])
    ] == [1]
    with pytest.raises(ShotgunError, match=r'is_not.*entity_type'):
        mockgun_client.find('ScalarTypes', [['entity_type', 'is_not', 'Shot']])


def test_find_all_short_circuits_after_false_branch(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = {
        'filter_operator': 'all',
        'filters': [
            ['code', 'is', 'drop'],
            ['project', 'name_contains', 'missing'],
        ],
    }

    assert mockgun_client.find('Shot', filters) == []


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
        assert tuple(
            inspect.signature(overload) for overload in get_overloads(mockgun_method)
        ) == (tuple(inspect.signature(overload) for overload in get_overloads(client_method)))


def test_find_any_short_circuits_after_true_branch(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters = {
        'filter_operator': 'any',
        'filters': [
            ['code', 'is', 'keep'],
            ['project', 'name_contains', 'missing'],
        ],
    }

    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == [1]


@pytest.mark.parametrize('method_name', ['find', 'find_one'])
@pytest.mark.parametrize(
    ('args', 'kwargs', 'suffix'),
    [
        ((), {}, "missing a required argument: 'entity_type'"),
        (
            ('Asset', cast('object', []), None, None, None, False, True, None, 'extra'),
            {},
            'too many positional arguments',
        ),
        (
            ('Asset', cast('object', [])),
            {'unexpected': True},
            "got an unexpected keyword argument 'unexpected'",
        ),
        (
            ('Asset', cast('object', [])),
            {'entity_type': 'Shot'},
            "multiple values for argument 'entity_type'",
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
    expected_message = f'Shotgun.{method_name}() {suffix}'

    for class_ in (ClientShotgun, mockgun.Shotgun):
        client = object.__new__(class_)
        method = cast('Callable[..., object]', getattr(client, method_name))
        with pytest.raises(TypeError) as caught:
            method(*args, **kwargs)

        assert str(caught.value) == expected_message


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
    assert compiler_calls == [(filters, None, False)]
    assert filters == original_filters
    assert fields == original_fields

    result[0]['code'] = 'changed'
    assert _database(mockgun_client)['Shot'][1]['code'] == 'Alpha'


@pytest.mark.parametrize(('operator', 'expected_ids'), [('and', [1, 2]), ('or', [])])
def test_find_condition_tree_all_skipped_conditions_use_empty_tree_semantics(
    mockgun_client: mockgun.Shotgun,
    operator: str,
    expected_ids: list[int],
) -> None:
    mockgun_client.create('Shot', {'code': 'one'})
    mockgun_client.create('Shot', {'code': 'two'})
    filters = {
        'logical_operator': operator,
        'conditions': [{'path': '$FROM$Shot.id', 'relation': 'is', 'values': [999]}],
    }

    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == expected_ids


def test_find_condition_tree_copies_nested_inputs(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = mockgun_client.create('Project', {'name': 'project'})
    mockgun_client.create('Shot', {'code': 'shot', 'project': project})
    expected: dict[str, Any] = {
        'type': 'Project',
        'id': project['id'],
        'metadata': {'labels': ['caller']},
    }
    filters: dict[str, Any] = {
        'logical_operator': 'and',
        'conditions': [{'path': 'project', 'relation': 'is', 'values': [expected]}],
    }
    original_filters = copy.deepcopy(filters)
    captured: list[dict[str, object]] = []
    validate_tree = cast(
        'Callable[[mockgun.Shotgun, str, dict[str, object]], None]',
        vars(mockgun.Shotgun)['_validate_tree'],
    )

    def capture_tree(
        client: mockgun.Shotgun,
        entity_type: str,
        tree: dict[str, object],
    ) -> None:
        captured.append(tree)
        validate_tree(client, entity_type, tree)

    monkeypatch.setattr(mockgun.Shotgun, '_validate_tree', capture_tree)

    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == [1]
    adapted_conditions = cast('list[dict[str, Any]]', captured[0]['conditions'])
    adapted_value = cast('dict[str, Any]', adapted_conditions[0]['values'][0])
    adapted_value['metadata']['labels'].append('adapter')

    assert filters == original_filters
    expected['metadata']['labels'].append('changed')
    assert adapted_value['metadata']['labels'] == ['caller', 'adapter']


def test_find_condition_tree_matching_and_nonmatching_leaves(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'present'})
    mockgun_client.create('Shot', {'code': 'other'})
    filters: dict[str, Any] = {
        'logical_operator': 'and',
        'conditions': [{'path': 'code', 'relation': 'is', 'values': ['missing']}],
    }

    assert mockgun_client.find('Shot', filters) == []
    filters['conditions'][0]['values'] = ['present']
    assert mockgun_client.find('Shot', filters, ['code']) == [
        {'code': 'present', 'id': 1, 'type': 'Shot'}
    ]


def test_find_condition_tree_nested_and_or_nodes(mockgun_client: mockgun.Shotgun) -> None:
    mockgun_client.create('Shot', {'code': 'Alpha', 'sg_status_list': 'ip'})
    mockgun_client.create('Shot', {'code': 'Alpha', 'sg_status_list': 'fin'})
    mockgun_client.create('Shot', {'code': 'Beta', 'sg_status_list': 'fin'})
    filters = {
        'logical_operator': 'or',
        'conditions': (
            {
                'logical_operator': 'and',
                'conditions': (
                    {'path': 'code', 'relation': 'is', 'values': ['alpha']},
                    {'path': 'sg_status_list', 'relation': 'is', 'values': ['ip']},
                ),
            },
            {
                'logical_operator': 'and',
                'conditions': (
                    {'path': 'code', 'relation': 'is', 'values': ['beta']},
                    {'path': 'sg_status_list', 'relation': 'is', 'values': ['fin']},
                ),
            },
        ),
    }

    assert [row['id'] for row in mockgun_client.find('Shot', filters)] == [1, 3]


@pytest.mark.parametrize(
    ('filters', 'message'),
    cast(
        'list[tuple[dict[str, object], str]]',
        [
            ({'conditions': []}, 'logical_operator'),
            ({'logical_operator': 'and'}, 'conditions'),
            ({'logical_operator': 'any', 'conditions': []}, 'logical_operator'),
            ({'logical_operator': [], 'conditions': []}, 'logical_operator'),
            (
                {
                    'logical_operator': 'and',
                    'conditions': [{'logical_operator': {}, 'conditions': []}],
                },
                'logical_operator',
            ),
            ({'logical_operator': 'and', 'conditions': 'invalid'}, 'conditions.*sequence'),
            ({'logical_operator': 'and', 'conditions': [42]}, 'nodes.*dictionaries'),
            (
                {'logical_operator': 'and', 'conditions': [{'relation': 'is', 'values': [1]}]},
                'path.*string',
            ),
            (
                {
                    'logical_operator': 'and',
                    'conditions': [{'path': 1, 'relation': 'is', 'values': [1]}],
                },
                'path.*string',
            ),
            (
                {
                    'logical_operator': 'and',
                    'conditions': [{'path': 'id', 'relation': 1, 'values': [1]}],
                },
                'relation.*string',
            ),
            (
                {
                    'logical_operator': 'and',
                    'conditions': [{'path': 'id', 'relation': 'is', 'values': (1,)}],
                },
                'values.*list',
            ),
            (
                {'logical_operator': 'and', 'conditions': [{'conditions': []}]},
                'logical_operator',
            ),
        ],
    ),
)
def test_find_condition_tree_rejects_malformed_shapes(
    mockgun_client: mockgun.Shotgun,
    filters: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ShotgunError, match=message):
        mockgun_client.find('Shot', filters)


def test_find_condition_tree_skips_from_conditions_recursively(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    mockgun_client.create('Shot', {'code': 'drop'})
    from_condition = {'path': '$FROM$Shot.id', 'relation': 'is', 'values': [999]}
    filters = {
        'logical_operator': 'and',
        'conditions': [
            from_condition,
            {
                'logical_operator': 'or',
                'conditions': [
                    copy.deepcopy(from_condition),
                    {'path': 'code', 'relation': 'is', 'values': ['keep']},
                ],
            },
        ],
    }

    assert mockgun_client.find('Shot', filters, ['code']) == [
        {'code': 'keep', 'id': 1, 'type': 'Shot'}
    ]


def test_find_counter_increments_before_validation_errors(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(ShotgunError, match=r'MissingEntity.*valid entity'):
        mockgun_client.find('MissingEntity', [])

    assert mockgun_client.finds == 1


def test_find_defers_group_missing_required_keys_until_a_row_is_evaluated(
    mockgun_client: mockgun.Shotgun,
) -> None:
    filters: list[dict[str, object]] = [{}]

    assert mockgun_client.find('Shot', filters) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == (
        "Bad filter operator, requires keys 'filter_operator' and 'filters', found "
    )


def test_find_defers_invalid_traditional_node_until_a_row_is_evaluated(
    mockgun_client: mockgun.Shotgun,
) -> None:
    assert mockgun_client.find('Shot', [42]) == []


def test_find_defers_malformed_tuple_node_with_generic_message(
    mockgun_client: mockgun.Shotgun,
) -> None:
    filters = [('code', 'is')]

    assert mockgun_client.find('Shot', filters) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'invalid traditional filter structure'


def test_find_defers_nested_group_with_hostile_key_without_inspection(
    mockgun_client: mockgun.Shotgun,
) -> None:
    malformed_group: dict[object, object] = {_HostileFilterKey(): []}
    filters: list[object] = [malformed_group]

    assert mockgun_client.find('Shot', filters) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'invalid traditional filter structure'


@pytest.mark.parametrize(
    ('group', 'expected_message'),
    [
        (
            {'filter_operator': 'all'},
            (
                "Bad filter operator, requires keys 'filter_operator' and 'filters', "
                'found filter_operator'
            ),
        ),
        (
            {'filters': []},
            "Bad filter operator, requires keys 'filter_operator' and 'filters', found filters",
        ),
        (
            {'extra': None, 'filters': []},
            (
                "Bad filter operator, requires keys 'filter_operator' and 'filters', "
                'found extra, filters'
            ),
        ),
    ],
)
def test_find_defers_partially_specified_group_with_present_keys(
    mockgun_client: mockgun.Shotgun,
    group: dict[str, object],
    expected_message: str,
) -> None:
    filters = [group]

    assert mockgun_client.find('Shot', filters) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == expected_message


def test_find_defers_top_level_group_with_hostile_string_key_without_inspection(
    mockgun_client: mockgun.Shotgun,
) -> None:
    malformed_group: dict[object, object] = {
        _HostileStringFilterKey('logical_operator'): [],
    }

    assert mockgun_client.find('Shot', cast('Any', malformed_group)) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', cast('Any', malformed_group))

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'invalid traditional filter structure'


def test_find_defers_unsupported_nested_operator_until_a_row_is_evaluated(
    mockgun_client: mockgun.Shotgun,
) -> None:
    filters: list[dict[str, object]] = [{'filter_operator': 'unsupported', 'filters': []}]

    assert mockgun_client.find('Shot', filters) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'Unknown filter_operator type: unsupported'


@pytest.mark.parametrize('operator_location', ['nested', 'top_level'])
@pytest.mark.parametrize(
    'operator_kind',
    ['equality_spoofing', 'hostile_representation', 'string_subclass'],
)
def test_find_defers_untrusted_filter_operators_without_inspecting_them(
    mockgun_client: mockgun.Shotgun,
    operator_location: str,
    operator_kind: str,
) -> None:
    if operator_kind == 'equality_spoofing':
        operator: object = _EqualitySpoofingOperator()
    elif operator_kind == 'hostile_representation':
        operator = _HostileRepresentationOperator()
    else:
        operator = _HostileStringOperator('all')
    kwargs: dict[str, Any] = {}
    filters: object = []
    if operator_location == 'nested':
        nested_filter: dict[str, object] = {
            'filter_operator': operator,
            'filters': [],
        }
        filters = [nested_filter]
    else:
        kwargs['filter_operator'] = operator

    assert mockgun_client.find('Shot', filters, **kwargs) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', filters, **kwargs)

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'invalid traditional filter structure'
    if isinstance(operator, (_EqualitySpoofingOperator, _HostileRepresentationOperator)):
        assert operator.equality_calls == 0


def test_find_nested_all_short_circuits_before_invalid_node(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})
    filters: list[dict[str, object]] = [
        {
            'filter_operator': 'all',
            'filters': [['code', 'is', 'missing'], 42],
        }
    ]

    assert mockgun_client.find('Shot', filters) == []


def test_find_nested_any_short_circuits_before_invalid_node(
    mockgun_client: mockgun.Shotgun,
) -> None:
    created = mockgun_client.create('Shot', {'code': 'keep'})
    filters: list[dict[str, object]] = [
        {
            'filter_operator': 'any',
            'filters': [['code', 'is', 'keep'], 42],
        }
    ]

    assert mockgun_client.find('Shot', filters) == [
        {'id': created['id'], 'type': 'Shot'},
    ]


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

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'Filters can only be lists or dictionaries, not int.'


def test_find_one_delegates_with_the_approved_arguments(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    filters = [('code', 'is', 'one')]
    fields = ['code']
    order = [{'field_name': 'code', 'direction': 'desc'}]
    presets = [{'preset_name': 'LATEST'}]

    def fake_find(*args: object, **kwargs: object) -> list[dict[str, object]]:
        calls.append((args, kwargs))
        return [{'type': 'Shot', 'id': 1, 'code': 'one'}]

    monkeypatch.setattr(mockgun.Shotgun, 'find', fake_find)

    result = mockgun_client.find_one(
        'Shot',
        filters,
        fields,
        order,
        'any',
        retired_only=True,
        include_archived_projects=False,
        additional_filter_presets=presets,
    )

    assert result == {'type': 'Shot', 'id': 1, 'code': 'one'}
    assert calls == [
        (
            (mockgun_client, 'Shot', filters),
            {
                'additional_filter_presets': presets,
                'fields': fields,
                'filter_operator': 'any',
                'include_archived_projects': False,
                'limit': 1,
                'order': order,
                'retired_only': True,
            },
        )
    ]


def test_find_partitions_retired_rows_and_accepts_source_noop_options(
    mockgun_client: mockgun.Shotgun,
) -> None:
    for code in ('one', 'two', 'three'):
        mockgun_client.create('Shot', {'code': code})
    mockgun_client.delete('Shot', 2)
    presets = [{'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'}]
    original_presets = copy.deepcopy(presets)

    active = mockgun_client.find(
        'Shot',
        [],
        ['code'],
        limit=1,
        page=9,
        include_archived_projects=False,
        additional_filter_presets=presets,
    )
    retired = mockgun_client.find('Shot', [], ['code'], retired_only=True)

    assert [row['code'] for row in active] == ['one', 'three']
    assert [row['code'] for row in retired] == ['two']
    assert presets == original_presets


def test_find_preserves_explicit_list_as_scalar_relation_operand(
    mockgun_client: mockgun.Shotgun,
) -> None:
    mockgun_client.create('Shot', {'code': 'keep'})

    assert mockgun_client.find('Shot', [['code', 'is', ['keep']]], ['code']) == []


def test_find_preserves_scalar_list_operand_in_nested_filters(
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

    assert mockgun_client.find('Shot', filters, ['code']) == []


def test_find_rejects_traditional_condition_without_right_hand_operand(
    mockgun_client: mockgun.Shotgun,
) -> None:
    with pytest.raises(ShotgunError):
        mockgun_client.find('Shot', [['code', 'not_in']], ['code'])


def test_find_resolves_self_linked_fields_in_linear_traversal(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = cast('dict[str, dict[str, Any]]', mockgun_client._schema)
    schema['Shot']['parent'] = {
        'data_type': {'value': 'entity'},
        'properties': {
            'default_value': {'value': None},
            'valid_types': {'value': ['Shot']},
        },
    }
    original_field_type = mockgun.Shotgun._field_type
    visits = 0

    def count_field_type(
        client: mockgun.Shotgun,
        entity_type: str,
        field_name: str,
    ) -> str:
        nonlocal visits
        visits += 1
        return original_field_type(client, entity_type, field_name)

    monkeypatch.setattr(mockgun.Shotgun, '_field_type', count_field_type)

    depths = (2, 8, 12)
    observed_visits: list[int] = []
    for depth in depths:
        visits = 0
        field_name = 'parent.Shot.' * depth + 'id'

        assert mockgun_client.find('Shot', [], [field_name]) == []
        observed_visits.append(visits)

    with pytest.raises(ShotgunError, match=r'code.*valid linked field.*Shot'):
        mockgun_client.find('Shot', [], ['code.MissingEntity.id'])
    with pytest.raises(ShotgunError, match=r'MissingEntity.*valid entity'):
        mockgun_client.find('Shot', [], ['parent.MissingEntity.id'])
    assert observed_visits == [depth + 1 for depth in depths]


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

    assert _database(mockgun_client)['Shot'][1]['project'] == {'type': 'Project', 'id': 1}
    assert _database(mockgun_client)['Shot'][1]['reviewers'] == [{'type': 'HumanUser', 'id': 1}]


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

    with pytest.raises(ShotgunError, match=r'type_is.*text'):
        mockgun_client.find('Shot', filters)


@pytest.mark.parametrize(
    ('filters', 'fields', 'order'),
    [
        ([['missing', 'is', None]], None, None),
        ([], ['missing'], None),
        ([], ['project.Project.missing'], None),
        ([], None, [{'field_name': 'missing', 'direction': 'asc'}]),
    ],
)
def test_find_validates_filter_requested_and_order_paths_before_rows(
    mockgun_client: mockgun.Shotgun,
    filters: list[list[object]],
    fields: list[str] | None,
    order: list[dict[str, str]] | None,
) -> None:
    with pytest.raises(ShotgunError, match='valid field'):
        mockgun_client.find('Shot', filters, fields, order)


@pytest.mark.parametrize(
    'condition',
    [
        ['text', 'is'],
        ['text', 'contains', 'a', 'b'],
        ['number', 'between', 1],
        ['number', 'between', 1, 2, 3],
        ['checkbox', 'in', [True]],
        ['reviewers', 'in', [{'type': 'HumanUser', 'id': 1}]],
    ],
)
def test_invalid_arity_and_unsupported_field_relations_fail_without_rows(
    mockgun_client: mockgun.Shotgun,
    condition: list[object],
) -> None:
    with pytest.raises(ShotgunError):
        mockgun_client.find('ScalarTypes' if condition[0] != 'reviewers' else 'Shot', [condition])


@pytest.mark.parametrize('field_name', ['list', 'status_list'])
def test_list_fields_use_case_sensitive_relations(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
) -> None:
    mockgun_client.create('ScalarTypes', {field_name: 'Alpha'})
    mockgun_client.create('ScalarTypes', {field_name: 'beta'})
    mockgun_client.create('ScalarTypes', {})

    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'is', 'Alpha']])
    ] == [1]
    assert mockgun_client.find('ScalarTypes', [[field_name, 'is', 'alpha']]) == []
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'in', ['Alpha', 'missing']]])
    ] == [1]
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'not_in', ['Alpha']]])
    ] == [2, 3]


def test_multi_entity_relations_preserve_id_only_and_empty_list_quirks(
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
    ] == [1]
    assert [
        row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is_not', second]])
    ] == [1, 2]
    assert [row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is', None]])] == [2]
    assert [row['id'] for row in mockgun_client.find('Shot', [['reviewers', 'is_not', None]])] == [
        1
    ]


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
    assert [
        row['id']
        for row in mockgun_client.find('ScalarTypes', [[field_name, 'not_between', low, middle]])
    ] == [3]
    assert [
        row['id'] for row in mockgun_client.find('ScalarTypes', [[field_name, 'in', [low, high]]])
    ] == [1, 3]


@pytest.mark.parametrize(
    ('order', 'error_type', 'message'),
    [
        ([{'direction': 'asc'}], ValueError, 'field_name.*direction'),
        ([{'field_name': 'code'}], KeyError, 'direction'),
        ([{'field_name': 'code', 'direction': 'sideways'}], ValueError, 'direction'),
    ],
)
def test_order_clause_errors_match_the_source_boundary(
    mockgun_client: mockgun.Shotgun,
    order: list[dict[str, str]],
    error_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error_type, match=message):
        mockgun_client.find('Shot', [], order=order)


def test_order_preserves_source_stability_and_last_clause_precedence(
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

    assert [row['code'] for row in ascending] == ['C', 'A', 'B']
    assert [row['code'] for row in descending] == ['C', 'B', 'A']
    assert order == original_order


@pytest.mark.parametrize('field_name', ['project', 'reviewers'])
def test_order_preserves_unsortable_relation_errors(
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

    with pytest.raises(TypeError):
        mockgun_client.find(
            'Shot',
            [],
            order=[{'field_name': field_name, 'direction': 'asc'}],
        )


@pytest.mark.parametrize('field_name', ['checkbox', 'serializable', 'url'])
def test_order_preserves_unsortable_scalar_errors(
    mockgun_client: mockgun.Shotgun,
    field_name: str,
) -> None:
    value: object = True if field_name == 'checkbox' else {'value': 1}
    mockgun_client.create('ScalarTypes', {field_name: value})
    mockgun_client.create('ScalarTypes', {})

    with pytest.raises(TypeError):
        mockgun_client.find(
            'ScalarTypes',
            [],
            order=[{'field_name': field_name, 'direction': 'asc'}],
        )


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

    assert [row['code'] for row in result] == ['none', 'first', 'second']


@pytest.mark.parametrize(
    ('field_name', 'values', 'ascending_ids', 'descending_ids'),
    [
        ('number', (-1, 2, None), [1, 3, 2], [2, 3, 1]),
        ('text', ('b', None, 'a'), [2, 3, 1], [1, 3, 2]),
        (
            'date_time',
            (
                datetime.datetime.fromisoformat('2026-01-02T00:00:00'),
                None,
                datetime.datetime.fromisoformat('2026-01-01T00:00:00'),
            ),
            [2, 3, 1],
            [1, 3, 2],
        ),
    ],
)
def test_order_uses_source_none_sentinels_by_field_type(
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
def test_find_defers_noncontainer_filters_until_a_row_is_evaluated(
    mockgun_client: mockgun.Shotgun,
    filters: object,
) -> None:
    assert mockgun_client.find('Shot', cast('Any', filters)) == []

    mockgun_client.create('Shot', {'code': 'present'})
    with pytest.raises(ShotgunError) as caught:
        mockgun_client.find('Shot', cast('Any', filters))

    assert type(caught.value) is ShotgunError
    assert str(caught.value) == 'invalid traditional filter structure'


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
