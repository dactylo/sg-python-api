import pickle
from collections.abc import AsyncIterator, Mapping
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import AsyncShotgun, MockgunError, Shotgun

from .conftest import QUERY_NOW, QUERY_SCHEMA, QUERY_SCHEMA_ENTITY, FakeSiteCase, raw_call, seed

_SHORT_PREFIX = (
    'API query_display_name_cache() query text too short! must be 3 or more characters :\n'
)


def _raw_search(case: FakeSiteCase, params: dict[str, object]) -> dict[str, object]:
    return raw_call(case.client._server, 'query_display_name_cache', params)


def _record(
    case: FakeSiteCase, project: dict[str, object], kind: str, data: Mapping[str, object]
) -> dict[str, object]:
    defaults: dict[str, object] = {'project': project}
    if kind == 'Shot':
        defaults['sg_status_list'] = 'ip'
    return seed(case, kind, {**defaults, **data})


@pytest.fixture(params=['sync', 'async'])
async def image_site(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[FakeSiteCase]:
    schema = cast('dict[str, dict[str, object]]', deepcopy(QUERY_SCHEMA))
    schema['Shot']['image'] = {
        'data_type': {'value': 'image'},
        'properties': {'default_value': {'value': 'mockgun-image'}},
    }
    schema_path = tmp_path / 'search-image-schema.pickle'
    entity_path = tmp_path / 'search-image-entities.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=2))
    entity_path.write_bytes(pickle.dumps(QUERY_SCHEMA_ENTITY, protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    client = (Shotgun if request.param == 'sync' else AsyncShotgun)(
        'http://unit-test.example', script_name='queries', api_key='key'
    )
    case = FakeSiteCase(str(request.param), client)
    client.site.set_time(QUERY_NOW)
    yield case
    await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('text', ['', 'a', 'ab', 'wy'])
@pytest.mark.parametrize('with_shot', [False, True])
async def test_short_search_fault_includes_parameters(
    query_site: FakeSiteCase, text: str, with_shot: bool
) -> None:
    if with_shot:
        seed(query_site, 'Shot', {'code': 'ab'})
    with pytest.raises(Fault):
        await query_site.call('text_search', text, {'Shot': []})
    params: dict[str, object] = {
        'text': text,
        'entity_types': {'Shot': {'logical_operator': 'and', 'conditions': []}},
        'project_ids': [],
        'max_results': None,
    }
    raw = _raw_search(query_site, params)
    assert raw['error_code'] == 103
    assert raw['message'] == _SHORT_PREFIX + repr(params)


@pytest.mark.anyio
@pytest.mark.parametrize('other', ['filter', 'limit'])
async def test_short_search_with_other_invalid_input_is_unsupported(
    query_site: FakeSiteCase, other: str
) -> None:
    filters = [['no_such', 'is', 1]] if other == 'filter' else []
    limit = 0 if other == 'limit' else None
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', 'ab', {'Shot': filters}, limit=limit)


@pytest.mark.anyio
async def test_valid_length_preserves_filter_fault(query_site: FakeSiteCase) -> None:
    with pytest.raises(Fault):
        await query_site.call('text_search', 'abc', {'Shot': [['no_such', 'is', 1]]})
    params: dict[str, object] = {
        'text': 'abc',
        'entity_types': {
            'Shot': {
                'logical_operator': 'and',
                'conditions': [{'path': 'no_such', 'relation': 'is', 'values': [1]}],
            }
        },
        'project_ids': [],
        'max_results': None,
    }
    assert _raw_search(query_site, params)['error_code'] == 103


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('text', 'terms'),
    [
        ('Foo-Bar  BAZ', ['foo-bar', 'baz']),
        ('a.b,c', ['a.b,c']),
        ('abc', ['abc']),
        ('shot_010', ['shot_010']),
        ('abcd e', ['abcd', 'e']),
        ('zeta wy z', ['zeta', 'wy', 'z']),
    ],
)
async def test_supported_search_terms(
    query_site: FakeSiteCase, text: str, terms: list[str]
) -> None:
    assert await query_site.call('text_search', text, {'Shot': []}) == {
        'matches': [],
        'terms': terms,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    'text', ['shot/010', 'caf\u00e9 bar', 'abc\tdef', ' abcd', 'abcd ', '     ', 'a b']
)
async def test_unsupported_search_text_raises(query_site: FakeSiteCase, text: str) -> None:
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', text, {'Shot': []})


@pytest.mark.anyio
@pytest.mark.parametrize('limit', [0, -1])
async def test_search_requires_positive_limit(query_site: FakeSiteCase, limit: int) -> None:
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', 'abcde', {'Shot': []}, limit=limit)


@pytest.mark.anyio
@pytest.mark.parametrize('retired_project', [False, True])
async def test_search_rejects_unknown_and_retired_projects(
    query_site: FakeSiteCase, retired_project: bool
) -> None:
    seed(query_site, 'Project', {'name': 'Active'})
    seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    retired = seed(query_site, 'Project', {'name': 'Gone'})
    assert await query_site.call('delete', 'Project', retired['id']) is True
    project_id = retired['id'] if retired_project else 999
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', 'abcde', {'Shot': []}, project_ids=[project_id])


@pytest.mark.anyio
@pytest.mark.parametrize('project_id', ['1', True])
async def test_search_rejects_noninteger_project_id(
    query_site: FakeSiteCase, project_id: object
) -> None:
    params: dict[str, object] = {
        'text': 'abcde',
        'entity_types': {'Shot': {'logical_operator': 'and', 'conditions': []}},
        'project_ids': [project_id],
        'max_results': None,
    }
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        _raw_search(query_site, params)


@pytest.mark.anyio
async def test_search_requires_entity_type(query_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', 'abcde', {})


@pytest.mark.anyio
async def test_search_rejects_undeclared_entity_type(query_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        await query_site.call('text_search', 'abcde', {'NoSuch': []})


@pytest.mark.anyio
async def test_search_accepts_project_without_archived_field(fake_site: FakeSiteCase) -> None:
    project = seed(fake_site, 'Project', {'name': 'P'})
    assert await fake_site.call(
        'text_search', 'abcde', {'Shot': []}, project_ids=[project['id']]
    ) == {
        'matches': [],
        'terms': ['abcde'],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_non_mapping_search_payload_is_rejected(query_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        raw_call(query_site.client._server, 'query_display_name_cache', [])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('text', 12345),
        ('entity_types', []),
        ('project_ids', {}),
        ('max_results', '1'),
        ('max_results', True),
    ],
)
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_search_rejects_wrong_parameter_json_types(
    query_site: FakeSiteCase, field: str, value: object
) -> None:
    params: dict[str, object] = {
        'text': 'abcde',
        'entity_types': {'Shot': {'logical_operator': 'and', 'conditions': []}},
        'project_ids': [],
        'max_results': None,
    }
    params[field] = value
    with pytest.raises(MockgunError, match='query_display_name_cache'):
        raw_call(query_site.client._server, 'query_display_name_cache', params)


@pytest.mark.anyio
async def test_empty_search_accepts_limit_and_project_ids(query_site: FakeSiteCase) -> None:
    active = seed(query_site, 'Project', {'name': 'Active'})
    archived = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})

    assert await query_site.call('text_search', 'abcde', {'Shot': []}, limit=1) == {
        'matches': [],
        'terms': ['abcde'],
    }
    for project in (active, archived):
        assert await query_site.call(
            'text_search', 'abcde', {'Shot': []}, project_ids=[project['id']]
        ) == {'matches': [], 'terms': ['abcde']}


@pytest.mark.anyio
async def test_empty_search_uses_unwrapped_wire_body(query_site: FakeSiteCase) -> None:
    assert await query_site.call('text_search', 'wire probe', {'Shot': []}) == {
        'matches': [],
        'terms': ['wire', 'probe'],
    }
    assert raw_call(
        query_site.client._server,
        'query_display_name_cache',
        {
            'text': 'wire probe',
            'entity_types': {'Shot': {'logical_operator': 'and', 'conditions': []}},
            'project_ids': [],
            'max_results': None,
        },
    ) == {'matches': [], 'terms': ['wire', 'probe']}


@pytest.mark.anyio
async def test_project_cascade_reaches_empty_search_without_new_events(
    query_site: FakeSiteCase,
) -> None:
    project = seed(query_site, 'Project', {'name': 'Retired'})
    seed(query_site, 'Shot', {'code': 'wire probe', 'project': project})
    assert await query_site.call('delete', 'Project', project['id']) is True
    store = query_site.client._server.store
    before_events = len(store.rows('EventLogEntry', retired=False))

    assert await query_site.call('text_search', 'wire probe', {'Shot': []}) == {
        'matches': [],
        'terms': ['wire', 'probe'],
    }
    assert len(store.rows('EventLogEntry', retired=False)) == before_events


@pytest.mark.anyio
async def test_project_taint_reaches_search(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    seed(query_site, 'Playlist', {'code': 'wire probe', 'project': project})
    assert await query_site.call('delete', 'Project', project['id']) is True

    with pytest.raises(MockgunError, match=r'Playlist.*Project'):
        await query_site.call('text_search', 'wire probe', {'Playlist': []})


def _match(
    record: dict[str, object],
    name: str,
    project: dict[str, object],
    *,
    links: list[str] | None = None,
    status: str | None = 'ip',
) -> dict[str, object]:
    return {
        **record,
        'name': name,
        'project_id': project['id'],
        'links': ['', ''] if links is None else links,
        'status': status if record['type'] == 'Shot' else None,
        'image': None,
    }


@pytest.mark.anyio
@pytest.mark.parametrize('text', ['ZETA', 'plate', 'zet', 's010 ZETA', 's020 zeta'])
async def test_search_matches_all_terms_in_name_only(query_site: FakeSiteCase, text: str) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    _record(query_site, project, 'Shot', {'code': 'other S020', 'description': 'zeta'})
    matches = [] if text == 's020 zeta' else [_match(shot, 'zeta-plate S010', project)]
    assert await query_site.call('text_search', text, {'Shot': []}) == {
        'matches': matches,
        'terms': text.lower().split(),
    }


@pytest.mark.anyio
@pytest.mark.parametrize('limit', [1, 20])
async def test_search_applies_limit_filters_and_project_scope(
    query_site: FakeSiteCase, limit: int
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    _record(query_site, project, 'Shot', {'code': 'other S020', 'description': 'zeta'})
    expected = {'matches': [_match(shot, 'zeta-plate S010', project)], 'terms': ['zeta']}
    assert await query_site.call('text_search', 'zeta', {'Shot': []}, limit=limit) == expected
    assert (
        await query_site.call('text_search', 'zeta', {'Shot': []}, project_ids=[project['id']])
        == expected
    )
    assert await query_site.call(
        'text_search', 'zeta', {'Shot': [['sg_status_list', 'is', 'fin']]}
    ) == {'matches': [], 'terms': ['zeta']}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('names', 'kind', 'types', 'reverse'),
    [
        (('zeta-plate S010', 'zeta-paint S030'), 'Shot', ['Shot'], True),
        (('zeta-plate S010', 'zeta-paint S030'), 'Shot', ['Shot', 'Asset'], True),
        (('zeta-plate S010', 'shot zeta 1'), 'Shot', ['Shot'], False),
        (('zeta-plate S010', 'zeta-pzzzz A010'), 'Asset', ['Asset', 'Shot'], False),
        (('x/zeta1', 'xzeta22'), 'Shot', ['Shot'], True),
    ],
)
async def test_search_orders_by_position_then_bounded_name(
    query_site: FakeSiteCase,
    names: tuple[str, str],
    kind: str,
    types: list[str],
    *,
    reverse: bool,
) -> None:
    first, second = names
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    one = _record(query_site, project, 'Shot', {'code': first})
    two = _record(query_site, project, kind, {'code': second})
    matches = [_match(one, first, project), _match(two, second, project)]
    if reverse:
        matches.reverse()
    requested: dict[str, list[object]] = {kind: [] for kind in types}
    assert await query_site.call('text_search', 'zeta', requested) == {
        'matches': matches,
        'terms': ['zeta'],
    }
    assert await query_site.call('text_search', 'zeta', requested, limit=1) == {
        'matches': matches[:1],
        'terms': ['zeta'],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('limit', [None, 1])
@pytest.mark.parametrize(
    ('names', 'kind', 'text'),
    [
        (('zeta-plate S010', 'zeta-comp S030'), 'Shot', 'zeta'),
        (('zeta-plate S010', 'zeta-paint S030'), 'Shot', 'zeta s0'),
        (('zeta-plate S010', 'zeta zeta'), 'Shot', 'zeta'),
        (('zeta-plate S010', 'my zeta asset1'), 'Asset', 'zeta'),
        (('zeta-b S010', 'zeta_a S020'), 'Shot', 'zeta'),
        (('zeta-a S010', 'zeta_a S010'), 'Shot', 'zeta'),
        (('zeta 11a', 'zeta 2aa'), 'Shot', 'zeta'),
    ],
)
async def test_search_rejects_unsettled_order_before_limit(
    query_site: FakeSiteCase, names: tuple[str, str], kind: str, text: str, limit: int | None
) -> None:
    first, second = names
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Shot', {'code': first})
    _record(query_site, project, kind, {'code': second})
    requested: dict[str, list[object]] = {entity: [] for entity in ('Shot', kind)}
    with pytest.raises(MockgunError):
        await query_site.call('text_search', text, requested, limit=limit)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('second_type', 'message'),
    [
        ('Shot', r'Shot\.name: equal collation keys'),
        ('Asset', r'Asset,Shot\.name: equal collation keys'),
    ],
)
async def test_search_collation_error_names_match_types(
    query_site: FakeSiteCase, second_type: str, message: str
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Shot', {'code': 'zeta-a S010'})
    _record(query_site, project, second_type, {'code': 'zeta_a S010'})
    requested: dict[str, list[object]] = {'Shot': [], second_type: []}
    with pytest.raises(MockgunError, match=message):
        await query_site.call('text_search', 'zeta', requested)


@pytest.mark.anyio
async def test_search_order_checks_only_matches(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    other = _record(query_site, project, 'Shot', {'code': 'zeta-comp S030'})
    assert await query_site.call('text_search', 'zeta-comp', {'Shot': []}) == {
        'matches': [_match(other, 'zeta-comp S030', project)],
        'terms': ['zeta-comp'],
    }


@pytest.mark.anyio
async def test_search_projects_filter_before_order(query_site: FakeSiteCase) -> None:
    alpha = seed(query_site, 'Project', {'name': 'Alpha'})
    beta = seed(query_site, 'Project', {'name': 'Beta'})
    one = _record(query_site, alpha, 'Shot', {'code': 'zeta-plate S010'})
    two = _record(query_site, beta, 'Shot', {'code': 'zeta-beta'})
    for project, record, name in [(alpha, one, 'zeta-plate S010'), (beta, two, 'zeta-beta')]:
        assert await query_site.call(
            'text_search', 'zeta', {'Shot': []}, project_ids=[project['id']]
        ) == {'matches': [_match(record, name, project)], 'terms': ['zeta']}
    with pytest.raises(MockgunError):
        await query_site.call(
            'text_search', 'zeta', {'Shot': []}, project_ids=[alpha['id'], beta['id']]
        )
    assert await query_site.call('text_search', 'zeta', {'Shot': [['project', 'is', beta]]}) == {
        'matches': [_match(two, 'zeta-beta', beta)],
        'terms': ['zeta'],
    }


@pytest.mark.anyio
async def test_search_archive_scope_does_not_change_find(query_site: FakeSiteCase) -> None:
    active = seed(query_site, 'Project', {'name': 'Active'})
    archived = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    one = _record(query_site, active, 'Shot', {'code': 'zeta-one'})
    two = _record(query_site, archived, 'Shot', {'code': 'zeta-two'})
    expected_find = [{**one, 'code': 'zeta-one'}, {**two, 'code': 'zeta-two'}]
    assert await query_site.call('find', 'Shot', [], ['code']) == expected_find
    matches = [_match(one, 'zeta-one', active), _match(two, 'zeta-two', archived)]
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == {
        'matches': matches[:1],
        'terms': ['zeta'],
    }
    assert await query_site.call(
        'text_search', 'zeta', {'Shot': []}, project_ids=[archived['id']]
    ) == {'matches': matches[1:], 'terms': ['zeta']}
    assert await query_site.call(
        'text_search', 'zeta', {'Shot': []}, project_ids=[active['id'], archived['id']]
    ) == {'matches': matches, 'terms': ['zeta']}
    assert await query_site.call('find', 'Shot', [], ['code']) == expected_find
    await query_site.call('update', 'Project', archived['id'], {'archived': False})
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == {
        'matches': matches,
        'terms': ['zeta'],
    }


@pytest.mark.anyio
async def test_search_reads_current_retirement_and_name(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    await query_site.call('delete', 'Shot', shot['id'])
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == {
        'matches': [],
        'terms': ['zeta'],
    }
    await query_site.call('revive', 'Shot', shot['id'])
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == {
        'matches': [_match(shot, 'zeta-plate S010', project)],
        'terms': ['zeta'],
    }
    await query_site.call('update', 'Shot', shot['id'], {'code': 'beta-plate S010'})
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == {
        'matches': [],
        'terms': ['zeta'],
    }
    assert await query_site.call('text_search', 'beta-plate', {'Shot': []}) == {
        'matches': [_match(shot, 'beta-plate S010', project)],
        'terms': ['beta-plate'],
    }


@pytest.mark.anyio
async def test_search_shot_null_status_and_sequence_links(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    sequence = _record(query_site, project, 'Sequence', {'code': 'seq-a'})
    shot = seed(
        query_site,
        'Shot',
        {'code': 'zeta-plate S010', 'project': project, 'sg_sequence': sequence},
    )
    expected = {
        'matches': [_match(shot, 'zeta-plate S010', project, status=None)],
        'terms': ['zeta'],
    }
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == expected
    await query_site.call('delete', 'Sequence', sequence['id'])
    assert await query_site.call('text_search', 'zeta', {'Shot': []}) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('scoped', [False, True])
async def test_search_match_requires_project(query_site: FakeSiteCase, *, scoped: bool) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    seed(query_site, 'Shot', {'code': 'zeta-plate S010'})
    with pytest.raises(MockgunError):
        await query_site.call(
            'text_search', 'zeta', {'Shot': []}, project_ids=[project['id']] if scoped else []
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'data', 'name', 'linked'),
    [
        ('Task', {'content': 'zeta layout', 'entity': 'shot'}, 'zeta layout', True),
        ('Version', {'code': 'zeta-v', 'entity': 'shot'}, 'zeta-v', True),
        (
            'Note',
            {'subject': 'zeta', 'content': 'body', 'note_links': ['shot']},
            'zeta - body',
            False,
        ),
        ('Note', {'subject': 'other', 'content': 'zeta body'}, 'other - zeta body', False),
        ('Sequence', {'code': 'zeta-x'}, 'zeta-x', False),
        ('Cut', {'code': 'zeta-x', 'entity': 'sequence'}, 'zeta-x', False),
        ('CutItem', {'code': 'zeta-x', 'cut': 'cut', 'shot': 'shot'}, 'zeta-x', False),
        (
            'PublishedFile',
            {'code': 'zeta-pf', 'entity': 'shot', 'version': 'version'},
            'zeta-pf',
            False,
        ),
        ('Asset', {'code': 'zeta-asset'}, 'zeta-asset', False),
        ('Playlist', {'code': 'zeta-x'}, 'zeta-x', False),
    ],
)
async def test_search_type_specific_match_values(
    query_site: FakeSiteCase, kind: str, data: dict[str, object], name: str, *, linked: bool
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'S0'})
    sequence = _record(query_site, project, 'Sequence', {'code': 'q0'})
    targets = {
        'shot': shot,
        'sequence': sequence,
        'cut': _record(query_site, project, 'Cut', {'code': 'cu0', 'entity': sequence}),
        'version': _record(query_site, project, 'Version', {'code': 'v0'}),
    }
    values = dict(data)
    for field, value in data.items():
        if isinstance(value, str) and value in targets:
            values[field] = targets[value]
        elif isinstance(value, list):
            values[field] = [shot]
    record = _record(query_site, project, kind, values)
    assert await query_site.call('text_search', 'zeta', {kind: []}) == {
        'matches': [_match(record, name, project, links=['Shot', 'S0'] if linked else None)],
        'terms': ['zeta'],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    'case', ['task-null', 'task-asset', 'version-null', 'asset-parent', 'playlist-version']
)
async def test_search_rejects_unobserved_match_links(query_site: FakeSiteCase, case: str) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    asset = _record(query_site, project, 'Asset', {'code': 'a0'})
    version = _record(query_site, project, 'Version', {'code': 'v0'})
    kind, data = {
        'task-null': ('Task', {'content': 'zeta layout'}),
        'task-asset': ('Task', {'content': 'zeta comp', 'entity': asset}),
        'version-null': ('Version', {'code': 'zeta-v'}),
        'asset-parent': ('Asset', {'code': 'zeta-asset', 'parents': [asset]}),
        'playlist-version': ('Playlist', {'code': 'zeta-x', 'versions': [version]}),
    }[case]
    _record(query_site, project, kind, data)
    with pytest.raises(MockgunError):
        await query_site.call('text_search', 'zeta', {kind: []})


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'data'),
    [
        ('Asset', {'code': 'other', 'description': 'zeta'}),
        ('Version', {'code': 'v001', 'description': 'zeta'}),
        ('Task', {'content': 'layout', 'sg_description': 'zeta'}),
        ('Note', {'subject': 'other', 'content': 'body'}),
        ('PublishedFile', {'code': 'pf-a', 'name': 'pf-b'}),
    ],
)
async def test_search_ignores_observed_unsearched_text_fields(
    query_site: FakeSiteCase, kind: str, data: dict[str, object]
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, kind, data)
    assert await query_site.call('text_search', 'zeta', {kind: []}) == {
        'matches': [],
        'terms': ['zeta'],
    }


@pytest.mark.anyio
async def test_search_filter_excludes_asset(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Asset', {'code': 'zeta-asset'})
    assert await query_site.call('text_search', 'zeta', {'Asset': [['code', 'is', 'none']]}) == {
        'matches': [],
        'terms': ['zeta'],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('subject', ['other', 'zeta note'])
async def test_search_never_matches_reply(query_site: FakeSiteCase, subject: str) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    note = _record(query_site, project, 'Note', {'subject': subject, 'content': 'body'})
    seed(query_site, 'Reply', {'content': 'zeta reply', 'entity': note})
    matches = [] if subject == 'other' else [_match(note, 'zeta note - body', project)]
    assert await query_site.call('text_search', 'zeta', {'Reply': [], 'Note': []}) == {
        'matches': matches,
        'terms': ['zeta'],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['PublishedFile', 'EventLogEntry', 'Project'])
async def test_search_rejects_unobserved_fields_or_types(
    query_site: FakeSiteCase, kind: str
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    if kind == 'PublishedFile':
        _record(query_site, project, kind, {'code': 'pf-a', 'name': 'zeta-pf'})
    with pytest.raises(MockgunError):
        await query_site.call('text_search', 'zeta', {kind: []})


@pytest.mark.anyio
async def test_search_accepts_unsupported_type_without_candidates(
    query_site: FakeSiteCase,
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    assert await query_site.call(
        'text_search', 'zeta', {'Shot': [], 'EventLogEntry': [['id', 'is', -1]]}
    ) == {'matches': [_match(shot, 'zeta-plate S010', project)], 'terms': ['zeta']}


@pytest.mark.anyio
@pytest.mark.parametrize('text', ['kilo wy', 'kilo q', 'kilo x_y'])
async def test_search_matches_short_and_literal_underscore_terms(
    query_site: FakeSiteCase, text: str
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'kilo wy q x_y'})
    _record(query_site, project, 'Shot', {'code': 'kilo'})
    assert await query_site.call('text_search', text, {'Shot': []}) == {
        'matches': [_match(shot, 'kilo wy q x_y', project)],
        'terms': text.split(),
    }


@pytest.mark.anyio
@pytest.mark.parametrize('code', ['kilo xay', 'kilo x\ny'])
async def test_search_raises_for_underscore_wildcard_ambiguity(
    query_site: FakeSiteCase, code: str
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Shot', {'code': code})
    with pytest.raises(MockgunError):
        await query_site.call('text_search', 'kilo x_y', {'Shot': []})


@pytest.mark.anyio
@pytest.mark.parametrize('subject', ['kilo', 'kilo-a'])
async def test_search_note_join_ambiguity(query_site: FakeSiteCase, subject: str) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    note = _record(query_site, project, 'Note', {'subject': subject, 'content': 'body'})
    if subject == 'kilo':
        with pytest.raises(MockgunError):
            await query_site.call('text_search', 'kilo -', {'Note': []})
    else:
        assert await query_site.call('text_search', 'kilo -', {'Note': []}) == {
            'matches': [_match(note, 'kilo-a - body', project)],
            'terms': ['kilo', '-'],
        }


@pytest.mark.anyio
@pytest.mark.parametrize('code', [None, 'zeta caf\u00e9'])
async def test_search_rejects_unsettled_or_nonascii_candidate_name(
    query_site: FakeSiteCase, code: str | None
) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    _record(query_site, project, 'Shot', {'code': 'zeta-plate S010'})
    _record(query_site, project, 'Shot', {'code': code})
    with pytest.raises(MockgunError):
        await query_site.call('text_search', 'zeta', {'Shot': []})


@pytest.mark.anyio
async def test_search_rejects_derived_task_name(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Alpha'})
    shot = _record(query_site, project, 'Shot', {'code': 'S0'})
    task = _record(query_site, project, 'Task', {'content': 'zeta t', 'entity': shot})
    await query_site.call('update', 'Task', task['id'], {'content': None})
    with pytest.raises(MockgunError):
        await query_site.call('text_search', 'zeta', {'Task': []})


@pytest.mark.anyio
async def test_search_display_name_reads_tainted_task_content(query_site: FakeSiteCase) -> None:
    await query_site.call('create', 'Task', {})
    with pytest.raises(MockgunError, match=r'Task\.content.*unknown'):
        await query_site.call('text_search', 'zeta', {'Task': []})


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['Shot', 'Task'])
async def test_search_rejects_nonnull_own_or_inherited_image(
    image_site: FakeSiteCase, kind: str
) -> None:
    project = seed(image_site, 'Project', {'name': 'Alpha'})
    shot = _record(image_site, project, 'Shot', {'code': 'zeta-img' if kind == 'Shot' else 'S0'})
    if kind == 'Task':
        _record(image_site, project, 'Task', {'content': 'zeta t', 'entity': shot})
    assert await image_site.call('find', 'Shot', [], ['image']) == [
        {**shot, 'image': 'mockgun-image'}
    ]
    with pytest.raises(MockgunError, match='image'):
        await image_site.call('text_search', 'zeta', {kind: []})


@pytest.mark.anyio
@pytest.mark.parametrize('whole_name', [False, True])
async def test_single_task_token_search_has_no_match(
    query_site: FakeSiteCase, whole_name: bool
) -> None:
    project = seed(query_site, 'Project', {'name': 'P'})
    shot = _record(query_site, project, 'Shot', {'code': 'S'})
    task = _record(query_site, project, 'Task', {'content': 'owned-task-zeta', 'entity': shot})
    text = 'owned-task-zeta' if whole_name else 'zeta'
    assert await query_site.call('text_search', text, {'Task': [['project', 'is', project]]}) == {
        'matches': [],
        'terms': [text],
    }
    assert await query_site.call('find_one', 'Task', [['id', 'is', task['id']]], ['content']) == {
        'type': 'Task',
        'id': task['id'],
        'content': 'owned-task-zeta',
    }


@pytest.mark.anyio
@pytest.mark.parametrize('form', ['project_ids', 'multiple_rows', 'multiple_terms', 'spaced_name'])
async def test_task_search_uncontradicted_forms_keep_matches(
    query_site: FakeSiteCase, form: str
) -> None:
    project = seed(query_site, 'Project', {'name': 'P'})
    shot = _record(query_site, project, 'Shot', {'code': 'S'})
    name = 'owned task zeta' if form == 'spaced_name' else 'owned-task-zeta'
    task = _record(query_site, project, 'Task', {'content': name, 'entity': shot})
    if form == 'multiple_rows':
        _record(query_site, project, 'Task', {'content': 'unmatched', 'entity': shot})
    text = 'owned zeta' if form == 'multiple_terms' else 'zeta'
    result = cast(
        'dict[str, object]',
        await query_site.call(
            'text_search',
            text,
            {'Task': []},
            project_ids=[project['id']] if form == 'project_ids' else None,
        ),
    )
    matches = cast('list[dict[str, object]]', result['matches'])
    assert [row['id'] for row in matches] == [task['id']]
