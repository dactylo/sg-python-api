import copy
import os
import subprocess
import sys
import typing
from pathlib import Path

import pytest

import shotgun_api3.shotgun as shotgun_module
from shotgun_api3 import _errors, _protocol


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


def test_entity_optimization_can_be_disabled_per_call() -> None:
    entity = {'type': 'Project', 'id': 1, 'sg_status': 'x'}

    result = _protocol.translate_filters([['project', 'is', entity]], optimize=False)

    assert result['conditions'] == [{'path': 'project', 'relation': 'is', 'values': [entity]}]


def test_entity_optimization_flag_initializes_from_environment() -> None:
    repository = Path(__file__).parents[2]
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(repository / 'src')
    environment['SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION'] = '1'
    source = (
        'from pathlib import Path; '
        'import shotgun_api3.shotgun as module; '
        "assert Path(module.__file__).resolve() == Path('src/shotgun_api3/shotgun.py').resolve(); "
        'assert module.SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION is True'
    )

    subprocess.run(
        [sys.executable, '-c', source],
        check=True,
        cwd=repository,
        env=environment,
    )


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


def test_entity_optimization_keys_are_fixture_pinned() -> None:
    assert (
        frozenset({'id', 'type', 'url', 'name', 'content_type', 'local_path', 'storage'})
        == _protocol._ENTITY_OPTIMIZATION_KEYS
    )


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


def test_simple_list_filters() -> None:
    result = _protocol.translate_filters([['sg_status_list', 'is', 'ip']])

    assert result == {
        'logical_operator': 'and',
        'conditions': [{'path': 'sg_status_list', 'relation': 'is', 'values': ['ip']}],
    }


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
