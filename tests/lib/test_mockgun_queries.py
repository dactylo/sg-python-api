import pickle
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import AsyncShotgun, MockgunError, Shotgun
from shotgun_api3.lib.mockgun._query import check_collation, collation_key, project_rows
from tests.conftest import entity_id
from tests.lib._records import create_link

from .conftest import QUERY_SCHEMA, QUERY_SCHEMA_ENTITY, FakeSiteCase, raw_read, seed

_TEST_CREDENTIAL = 'offline'

_LINK_CASES: list[tuple[str, str, str, list[object], list[str]]] = [
    ('Shot', 'project', 'in', ['PA', 'PB'], ['S1', 'S2']),
    ('Shot', 'project', 'not_in', ['PA'], ['S2', 'S3']),
    ('Shot', 'project', 'is_not', ['PA'], ['S2', 'S3']),
    ('Shot', 'project', 'name_is', ['alphaproject'], ['S1']),
    ('Shot', 'project', 'name_contains', ['ALPHA'], ['S1']),
    ('Shot', 'project', 'name_not_contains', ['alpha'], ['S2', 'S3']),
    ('Shot', 'project', 'type_is_not', ['Project'], ['S3']),
    ('Task', 'entity', 'name_is', ['ALPHA'], ['T1']),
    ('Task', 'entity', 'name_not_contains', ['alpha'], ['T2', 'T3']),
    ('Task', 'entity', 'type_is', ['Shot'], ['T1']),
    ('Task', 'entity', 'type_is_not', ['Shot'], ['T2', 'T3']),
    ('Note', 'note_links', 'is', ['S1'], ['N1', 'N3']),
    ('Note', 'note_links', 'is_not', ['S1'], ['N2', 'N4', 'N5']),
    ('Note', 'note_links', 'is', [None], ['N4', 'N5']),
    ('Note', 'note_links', 'is_not', [None], ['N1', 'N2', 'N3']),
    ('Note', 'note_links', 'in', ['S1'], ['N1', 'N3']),
    ('Note', 'note_links', 'not_in', ['S1'], ['N2', 'N4', 'N5']),
    ('Note', 'note_links', 'in', ['S1', 'A1'], ['N1', 'N2', 'N3']),
    ('Note', 'note_links', 'not_in', ['S1', 'A1'], ['N4', 'N5']),
    ('Note', 'note_links', 'type_is', ['Shot'], ['N1', 'N3']),
    ('Note', 'note_links', 'type_is_not', ['Shot'], ['N2', 'N4', 'N5']),
    ('Note', 'note_links', 'name_is', ['alpha'], ['N1', 'N3']),
    ('Note', 'note_links', 'name_is', ['ALPHA'], ['N1', 'N3']),
    ('Note', 'note_links', 'name_contains', ['alp'], ['N1', 'N3']),
    ('Note', 'note_links', 'name_contains', ['LP'], ['N1', 'N3']),
    ('Note', 'note_links', 'name_not_contains', ['alp'], ['N2', 'N4', 'N5']),
    ('Note', 'note_links', 'name_not_contains', ['ALP'], ['N2', 'N4', 'N5']),
    ('Note', 'note_links', 'is', ['A0'], []),
    ('Note', 'note_links', 'in', ['A0', 'A1'], ['N2', 'N3']),
    ('Note', 'note_links.Shot.code', 'is', ['alpha'], ['N1', 'N3']),
    ('Task', 'entity.Shot.code', 'is', ['alpha'], ['T1']),
    ('Task', 'entity.Asset.code', 'is', ['prop'], ['T2']),
    ('Task', 'entity.Shot.project.Project.name', 'is', ['AlphaProject'], ['T1']),
]
_SCALAR_CASES: list[tuple[str, str, list[object], list[int]]] = [
    ('sg_number', 'greater_than', [5], [3, 5]),
    ('sg_number', 'less_than', [10], [1, 2]),
    ('sg_number', 'between', [5, 10], [2, 3]),
    ('sg_number', 'between', [10, 5], [2, 3]),
    ('sg_number', 'between', [None, 10], []),
    ('sg_number', 'between', [5, None], []),
    ('sg_number', 'is', [None], [4]),
    ('sg_number', 'is_not', [5], [1, 3, 4, 5]),
    ('sg_number', 'in', [1, 10], [1, 3]),
    ('sg_number', 'not_in', [1, 10], [2, 4, 5]),
    ('sg_float', 'in', [1.5, 2.5], [1, 2]),
    ('sg_float', 'not_in', [1.5], [2, 3]),
    ('sg_float', 'greater_than', [1.0], [1, 2]),
    ('sg_float', 'between', [2.5, 1.5], [1, 2]),
    ('sg_float', 'between', [3.0, 2.0], [2]),
    ('sg_float', 'between', [None, 2.0], []),
    ('sg_float', 'between', [2.0, None], []),
    ('sg_date', 'between', ['2026-07-10', '2026-07-01'], [1, 2]),
    ('sg_date', 'between', [None, '2026-07-10'], []),
    ('sg_date', 'not_in', ['2026-07-01'], [2, 3]),
    ('sg_date_time', 'is', ['2026-07-01T02:00:00+02:00'], [1]),
    ('sg_date_time', 'greater_than', ['2026-07-01T00:00:00Z'], [2]),
    ('sg_date_time', 'not_in', ['2026-07-01T00:00:00Z'], [2, 3]),
    ('sg_date_time', 'between', ['2026-07-02T00:00:00Z', '2026-07-01T00:00:00Z'], [1, 2]),
    ('sg_date_time', 'between', ['2026-07-03T00:00:00Z', '2026-07-02T00:00:00Z'], [2]),
    ('sg_date_time', 'between', [None, '2026-07-02T00:00:00Z'], []),
    ('sg_date_time', 'between', ['2026-07-01T00:00:00Z', None], []),
    ('sg_duration', 'is', [120], [2]),
    ('sg_duration', 'is_not', [120], [1, 3, 4]),
    ('sg_duration', 'greater_than', [60], [2, 4]),
    ('sg_duration', 'between', [240, 60], [1, 2, 4]),
    ('sg_duration', 'in', [60, 240], [1, 4]),
    ('sg_duration', 'not_in', [60, 240], [2, 3]),
    ('sg_duration', 'between', [None, 240], []),
    ('sg_duration', 'between', [60, None], []),
    ('sg_percent', 'is_not', [50], [1, 3]),
    ('sg_percent', 'greater_than', [10], [2]),
    ('sg_percent', 'between', [10, 50], [1, 2]),
    ('sg_percent', 'between', [50, 10], [1, 2]),
    ('sg_percent', 'between', [None, 50], []),
    ('sg_percent', 'between', [10, None], []),
    ('sg_percent', 'not_in', [10], [2, 3]),
    ('sg_text', 'is', ['ALPHA'], [1]),
    ('sg_text', 'is', [''], [3, 4]),
    ('sg_text', 'is_not', [''], [1, 2, 5]),
    ('sg_text', 'contains', [''], [1, 2, 5]),
    ('sg_text', 'not_contains', [''], [3, 4]),
    ('sg_text', 'contains', ['_'], [5]),
    ('sg_text', 'contains', ['%'], [5]),
    ('sg_text', 'not_contains', ['alpha'], [3, 4, 5]),
    ('sg_text', 'in', [], []),
    ('sg_text', 'not_in', [], [1, 2, 3, 4, 5]),
    ('sg_list', 'is', ['prop'], [1]),
    ('sg_list', 'is', ['CHARACTER'], [2]),
    ('sg_list', 'is_not', ['pROP'], [2, 3]),
    ('sg_list', 'in', ['cHARACTER'], [2]),
    ('sg_list', 'not_in', ['pROP'], [2, 3]),
    ('sg_list', 'in', ['pROP', 'CHARACTER'], [1, 2]),
    ('sg_list', 'not_in', ['pROP', 'CHARACTER'], [3]),
    ('sg_list', 'is_not', ['Character'], [1, 3]),
    ('sg_status', 'is', ['IP'], [1]),
    ('sg_status', 'is_not', ['IP'], [2, 3]),
    ('sg_status', 'in', ['IP'], [1]),
    ('sg_status', 'not_in', ['IP', 'FIN'], [3]),
    ('sg_status', 'is_not', ['ip'], [2, 3]),
    ('sg_checkbox', 'is', [True], [1]),
    ('sg_checkbox', 'is', [False], [2, 3]),
    ('sg_checkbox', 'is_not', [True], [2, 3]),
    ('image', 'is', [None], [1, 2]),
    ('image', 'is_not', [None], []),
    ('sg_currency', 'is', [None], [1, 2]),
    ('sg_currency', 'is_not', [None], []),
    ('sg_timecode', 'is', [None], [1, 2]),
    ('sg_timecode', 'is_not', [None], []),
    ('sg_float', 'in', [], []),
    ('sg_date', 'in', [], []),
    ('sg_date_time', 'in', [], []),
    ('sg_list', 'in', [], []),
    ('sg_status', 'in', [], []),
    ('sg_list', 'not_in', [], [1, 2, 3]),
    ('sg_status', 'not_in', [], [1, 2, 3]),
    ('sg_text', 'starts_with', [''], [1, 2, 3, 4, 5]),
    ('sg_text', 'ends_with', [''], [1, 2, 3, 4, 5]),
    ('sg_text', 'in', [''], []),
    ('sg_text', 'not_in', [''], [1, 2, 3, 4, 5]),
    ('sg_number', 'is', [True], [1]),
    ('sg_float', 'is', [True], []),
    ('sg_checkbox', 'is', [1], [1]),
    ('sg_checkbox', 'is_not', [False], [1]),
    ('sg_checkbox', 'is_not', [None], [1, 2, 3]),
]
_SCALAR_ERRORS: list[tuple[str, str, list[object]]] = [
    ('sg_number', 'in', [5.0, None]),
    ('sg_number', 'greater_than', [{'a': 1}]),
    ('sg_float', 'not_in', []),
    ('sg_duration', 'is', [60.0]),
    ('sg_percent', 'is', [10.5]),
    ('image', 'is', ['x']),
    ('sg_currency', 'is', [5]),
    ('sg_currency', 'greater_than', [5]),
    ('sg_timecode', 'is', [5]),
    ('sg_timecode', 'greater_than', [5]),
]
_SCALAR_FAULTS: list[tuple[str, str, list[object], int, str]] = [
    ('sg_number', 'in', [1, None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_float', 'in', [1.5, None], 104, 'API read() Read failed for entity type [Scalar]'),
    (
        'sg_date_time',
        'in',
        ['2026-07-01T00:00:00Z', None],
        104,
        'API read() Read failed for entity type [Scalar]',
    ),
    ('sg_duration', 'in', [60, None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_percent', 'in', [10, None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_text', 'in', ['ALPHA', None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_list', 'in', ['Prop', None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_number', 'not_in', [1, None], 104, 'API read() Read failed for entity type [Scalar]'),
    ('sg_number', 'in', [], 103, 'API read() Scalar.sg_number in requires at least one value'),
    (
        'sg_number',
        'not_in',
        [],
        103,
        'API read() Scalar.sg_number not_in requires at least one value',
    ),
    (
        'sg_number',
        'is',
        [5.0],
        103,
        'API read() Scalar.sg_number number operand has the wrong type: 5.0',
    ),
    (
        'sg_number',
        'is',
        ['5'],
        103,
        "API read() Scalar.sg_number number operand has the wrong type: '5'",
    ),
    ('sg_float', 'is', [2], 103, 'API read() Scalar.sg_float float operand has the wrong type: 2'),
    (
        'sg_date',
        'is',
        ['2026-07-01T00:00:00Z'],
        103,
        "API read() Scalar.sg_date date operand has the wrong type: '2026-07-01T00:00:00Z'",
    ),
    (
        'sg_date_time',
        'is',
        ['2026-07-01'],
        103,
        "API read() Scalar.sg_date_time date_time operand has the wrong type: '2026-07-01'",
    ),
    ('sg_text', 'is', [5], 103, 'API read() Scalar.sg_text text operand has the wrong type: 5'),
    (
        'sg_checkbox',
        'is',
        [None],
        103,
        (
            'API read() Scalar.sg_checkbox expected [FalseClass, TrueClass] data type(s) '
            'but got NilClass:\nnil'
        ),
    ),
    (
        'sg_checkbox',
        'is',
        ['true'],
        103,
        (
            'API read() Scalar.sg_checkbox expected [FalseClass, TrueClass] data type(s) '
            'but got String:\n"true"'
        ),
    ),
    (
        'sg_currency',
        'not_between',
        [1, 2],
        103,
        'API read() Scalar.sg_currency currency does not support not_between',
    ),
    (
        'sg_timecode',
        'not_between',
        [1, 2],
        103,
        'API read() Scalar.sg_timecode timecode does not support not_between',
    ),
]
_SCALAR_ROWS: dict[str, list[object]] = {
    'sg_number': [1, 5, 10, None, 20],
    'sg_float': [1.5, 2.5, None],
    'sg_date': ['2026-07-01', '2026-07-10', None],
    'sg_date_time': ['2026-07-01T00:00:00Z', '2026-07-02T00:00:00Z', None],
    'sg_duration': [60, 120, None, 240],
    'sg_percent': [10, 50, None],
    'sg_text': ['Alpha', 'alpha-x', None, '', '50%_off'],
    'sg_list': ['Prop', 'Character', None],
    'sg_status': ['ip', 'fin', None],
    'sg_checkbox': [True, False, None],
    'image': [None, None],
    'sg_currency': [None, None],
    'sg_timecode': [None, None],
}


async def _ordered_ids(
    case: FakeSiteCase, kind: str, clauses: list[tuple[str, str]]
) -> list[object]:
    rows = cast(
        'list[dict[str, object]]',
        await case.call(
            'find',
            kind,
            [],
            order=[{'field_name': field, 'direction': direction} for field, direction in clauses],
        ),
    )
    return [row['id'] for row in rows]


async def _query_records(case: FakeSiteCase) -> dict[str, dict[str, object]]:
    project = await create_link(case, 'Project', {'name': 'Alpha'})
    sequence = await create_link(case, 'Sequence', {'code': 'seq-a', 'project': project})
    first = await create_link(case, 'Asset', {'code': 'A1'})
    second = await create_link(case, 'Asset', {'code': 'A2'})
    shot = await create_link(
        case,
        'Shot',
        {'code': 'S1', 'project': project, 'sg_sequence': sequence, 'assets': [first, second]},
    )
    empty = await create_link(case, 'Shot', {'code': 'S2'})
    version = await create_link(case, 'Version', {'code': 'v1', 'entity': shot})
    return {'shot': shot, 'empty': empty, 'version': version}


async def _relation_records(case: FakeSiteCase) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for label, name in [('PA', 'AlphaProject'), ('PB', 'BetaProject')]:
        records[label] = await create_link(case, 'Project', {'name': name})
    records['S1'] = await create_link(case, 'Shot', {'code': 'alpha', 'project': records['PA']})
    records['S2'] = await create_link(case, 'Shot', {'code': 'beta', 'project': records['PB']})
    records['S3'] = await create_link(case, 'Shot', {'code': 'none'})
    records['A0'] = await create_link(case, 'Asset', {'code': 'spare'})
    records['A1'] = await create_link(case, 'Asset', {'code': 'prop'})
    assert records['A0']['id'] == records['S1']['id']
    assert records['A1']['id'] != records['S1']['id']
    records['T1'] = await create_link(case, 'Task', {'content': 't1', 'entity': records['S1']})
    records['T2'] = await create_link(case, 'Task', {'content': 't2', 'entity': records['A1']})
    records['T3'] = await create_link(case, 'Task', {'content': 't3'})
    for label, links in [('N1', ['S1']), ('N2', ['A1']), ('N3', ['S1', 'A1']), ('N4', [])]:
        records[label] = await create_link(
            case, 'Note', {'subject': label.lower(), 'note_links': [records[key] for key in links]}
        )
    records['N5'] = await create_link(case, 'Note', {'subject': 'n5'})
    return records


async def _retired_link_records(case: FakeSiteCase) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    records['Q'] = await create_link(case, 'Sequence', {'code': 'seq-a'})
    records['S1'] = await create_link(case, 'Shot', {'code': 'S010', 'sg_sequence': records['Q']})
    records['S2'] = await create_link(case, 'Shot', {'code': 'S020', 'sg_sequence': records['Q']})
    records['T'] = await create_link(case, 'Task', {'content': 'Layout', 'entity': records['S1']})
    records['V1'] = await create_link(
        case,
        'Version',
        {'code': 'v001', 'entity': records['S1'], 'sg_task': records['T']},
    )
    records['V2'] = await create_link(case, 'Version', {'code': 'v002', 'entity': records['S2']})
    records['PF'] = await create_link(
        case,
        'PublishedFile',
        {'code': 'pf-1', 'entity': records['S1'], 'version': records['V1']},
    )
    records['PL'] = await create_link(
        case, 'Playlist', {'code': 'pl-1', 'versions': [records['V1'], records['V2']]}
    )
    records['A1'] = await create_link(
        case, 'Asset', {'code': 'a1', 'shots': [records['S1'], records['S2']]}
    )
    records['A2'] = await create_link(case, 'Asset', {'code': 'a2', 'parents': [records['A1']]})
    records['K'] = await create_link(case, 'Cut', {'code': 'cut-1', 'entity': records['Q']})
    records['CI'] = await create_link(
        case,
        'CutItem',
        {
            'code': 'ci-1',
            'cut': records['K'],
            'shot': records['S1'],
            'version': records['V1'],
        },
    )
    return records


async def _scalar_rows(case: FakeSiteCase, field: str) -> None:
    for value in _SCALAR_ROWS[field]:
        await create_link(case, 'Scalar', {field: value} if value is not None else {})


async def _shot_ids(case: FakeSiteCase, asset: dict[str, object]) -> list[object]:
    found = cast(
        'dict[str, object]',
        await case.call('find_one', 'Asset', [['id', 'is', asset['id']]], ['shots']),
    )
    return [link['id'] for link in cast('list[dict[str, object]]', found['shots'])]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('retired', 'holder', 'field', 'remaining'),
    [
        ('S1', 'PF', 'entity', None),
        ('S1', 'CI', 'shot', None),
        ('S1', 'V1', 'entity', None),
        ('S1', 'A1', 'shots', ['S2']),
        ('Q', 'S2', 'sg_sequence', None),
        ('Q', 'K', 'entity', None),
        ('V1', 'PF', 'version', None),
        ('V1', 'CI', 'version', None),
        ('V1', 'PL', 'versions', ['V2']),
        ('T', 'V1', 'sg_task', None),
        ('A1', 'A2', 'parents', []),
    ],
)
async def test_retired_target_hides_listed_active_holder_link(
    query_site: FakeSiteCase,
    retired: str,
    holder: str,
    field: str,
    remaining: list[str] | None,
) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', records[retired]['type'], records[retired]['id'])
    if (retired, holder) == ('S1', 'V1'):
        assert (
            await query_site.call(
                'find_one', 'Version', [['id', 'is', records['V1']['id']]], retired_only=True
            )
            == records['V1']
        )
        assert await query_site.call('revive', 'Version', records['V1']['id'])
    result = cast(
        'dict[str, object]',
        await query_site.call(
            'find_one', records[holder]['type'], [['id', 'is', records[holder]['id']]], [field]
        ),
    )
    expected = (
        [{**records[key], 'name': {'S2': 'S020', 'V2': 'v002'}[key]} for key in remaining]
        if remaining is not None
        else None
    )
    assert result[field] == expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('holder', 'field', 'expected'), [('PF', 'entity', 'S1'), ('A1', 'shots', 'S1')]
)
async def test_revived_target_restores_link(
    query_site: FakeSiteCase, holder: str, field: str, expected: str
) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', 'Shot', records['S1']['id'])
    assert await query_site.call('revive', 'Shot', records['S1']['id'])
    result = cast(
        'dict[str, object]',
        await query_site.call(
            'find_one', records[holder]['type'], [['id', 'is', records[holder]['id']]], [field]
        ),
    )
    first = {**records[expected], 'name': 'S010'}
    assert result[field] == (
        first if holder == 'PF' else [first, {**records['S2'], 'name': 'S020'}]
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('path', 'relation', 'operand', 'raises'),
    [
        ('entity', 'is', 'S1', False),
        ('entity.Shot.id', 'is', 'S1_ID', False),
        ('entity', 'is_not', 'S1', True),
        ('entity', 'is', None, True),
        ('entity', 'name_contains', 'S', True),
        ('entity', 'type_is', 'Shot', True),
    ],
)
async def test_hidden_single_link_filter(
    query_site: FakeSiteCase,
    path: str,
    relation: str,
    operand: str | None,
    raises: bool,
) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', 'Shot', records['S1']['id'])
    if operand == 'S1_ID':
        value: object = records['S1']['id']
    elif operand == 'S1':
        value = records['S1']
    else:
        value = operand
    if raises:
        with pytest.raises(MockgunError, match=r'PublishedFile\.entity'):
            await query_site.call('find', 'PublishedFile', [[path, relation, value]])
    else:
        result = cast(
            'list[dict[str, object]]',
            await query_site.call('find', 'PublishedFile', [[path, relation, value]]),
        )
        assert result == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('relation', 'member', 'raises', 'matches'),
    [('is', 'V1', False, False), ('is', 'V2', False, True), ('is_not', 'V2', True, False)],
)
async def test_hidden_multi_link_filter(
    query_site: FakeSiteCase, relation: str, member: str, raises: bool, matches: bool
) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', 'Version', records['V1']['id'])
    if raises:
        with pytest.raises(MockgunError, match=r'Playlist\.versions'):
            await query_site.call('find', 'Playlist', [['versions', relation, records[member]]])
    else:
        result = cast(
            'list[dict[str, object]]',
            await query_site.call('find', 'Playlist', [['versions', relation, records[member]]]),
        )
        assert bool(result) is matches


@pytest.mark.anyio
async def test_deep_return_through_hidden_link_raises(query_site: FakeSiteCase) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', 'Shot', records['S1']['id'])
    with pytest.raises(MockgunError, match=r'PublishedFile\.entity'):
        await query_site.call('find', 'PublishedFile', [], ['entity.Shot.code'])


@pytest.mark.anyio
async def test_retired_holder_hides_retired_target_link(query_site: FakeSiteCase) -> None:
    records = await _retired_link_records(query_site)
    assert await query_site.call('delete', 'Shot', records['S1']['id'])
    assert await query_site.call('delete', 'PublishedFile', records['PF']['id'])
    result = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'PublishedFile', [], ['entity'], retired_only=True),
    )
    assert result == [{**records['PF'], 'entity': None}]


@pytest.mark.anyio
async def test_unknown_direct_return_field_is_omitted_from_client_and_raw_read(
    query_site: FakeSiteCase,
) -> None:
    records = await _query_records(query_site)
    shot = records['shot']
    expected = {**shot, 'code': 'S1'}
    assert (
        await query_site.call('find_one', 'Shot', [['id', 'is', shot['id']]], ['code', 'no_such'])
        == expected
    )
    raw = raw_read(
        query_site.client._server,
        'Shot',
        [('id', 'is', [shot['id']])],
        return_fields=['code', 'no_such'],
    )
    assert cast('dict[str, object]', raw['results'])['entities'] == [expected]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('record', 'path'),
    [
        ('shot', 'project.Project.no_such'),
        ('empty', 'project.Project.no_such'),
        ('shot', 'code.Shot.code'),
        ('shot', 'sg_sequence.Asset.code'),
        ('shot', 'sg_sequence.NoSuch.code'),
        ('shot', 'assets.Asset.code'),
        ('shot', 'no_such.Shot.code'),
    ],
)
async def test_invalid_return_paths_are_omitted(
    query_site: FakeSiteCase, record: str, path: str
) -> None:
    records = await _query_records(query_site)
    link = records[record]
    assert await query_site.call('find_one', 'Shot', [['id', 'is', link['id']]], [path]) == link


@pytest.mark.anyio
async def test_null_link_with_unknown_requested_type_raises(query_site: FakeSiteCase) -> None:
    records = await _query_records(query_site)
    empty = records['empty']
    with pytest.raises(MockgunError):
        await query_site.call(
            'find_one', 'Shot', [['id', 'is', empty['id']]], ['sg_sequence.NoSuch.code']
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('record', 'path', 'value'),
    [
        ('empty', 'sg_sequence.Asset.code', None),
        ('empty', 'sg_sequence.Sequence.code', None),
        ('version', 'entity.Shot.sg_sequence.Sequence.code', 'seq-a'),
        ('version', 'entity.Shot.sg_sequence.Sequence.project.Project.name', 'Alpha'),
    ],
)
async def test_valid_return_paths_follow_links_and_nulls(
    query_site: FakeSiteCase, record: str, path: str, value: object
) -> None:
    records = await _query_records(query_site)
    link = records[record]
    found = cast(
        'dict[str, object]',
        await query_site.call('find_one', link['type'], [['id', 'is', link['id']]], [path]),
    )
    assert found == {**link, path: value}


@pytest.mark.anyio
async def test_missing_link_target_raises_mockgun_error(fake_site: FakeSiteCase) -> None:
    shot = await create_link(
        fake_site, 'Shot', {'code': 'S1', 'project': {'type': 'Project', 'id': 999}}
    )
    with pytest.raises(MockgunError):
        await fake_site.call(
            'find_one', 'Shot', [['id', 'is', shot['id']]], ['project.Project.name']
        )


@pytest.mark.anyio
async def test_missing_listed_link_keeps_identity_without_name(query_site: FakeSiteCase) -> None:
    missing = {'type': 'Sequence', 'id': 999}
    shot = await create_link(query_site, 'Shot', {'code': 'S1', 'sg_sequence': missing})
    projected = await query_site.call('find', 'Shot', [['id', 'is', shot['id']]], ['sg_sequence'])
    assert projected == [{**shot, 'sg_sequence': missing}]
    assert await query_site.call('find', 'Shot', [['sg_sequence', 'is', missing]], ['id']) == [
        shot
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('multi', [False, True])
async def test_hidden_retired_link_needs_no_target_name(
    query_site: FakeSiteCase, multi: bool
) -> None:
    if multi:
        target = await create_link(query_site, 'Shot', {'code': 'S1'})
        holder = await create_link(query_site, 'Asset', {'code': 'A1', 'shots': [target]})
        kind, field, expected = 'Asset', 'shots', []
    else:
        target = await create_link(query_site, 'Sequence', {'code': 'Q1'})
        holder = await create_link(query_site, 'Shot', {'code': 'S1', 'sg_sequence': target})
        kind, field, expected = 'Shot', 'sg_sequence', None
    await query_site.call('update', target['type'], target['id'], {'code': None})
    assert await query_site.call('delete', target['type'], target['id'])
    assert await query_site.call('find_one', kind, [['id', 'is', holder['id']]], [field]) == {
        **holder,
        field: expected,
    }
    with pytest.raises(MockgunError, match=rf'{kind}\.{field}.*name_is'):
        await query_site.call('find', kind, [[field, 'name_is', 'S1']])


@pytest.mark.anyio
@pytest.mark.parametrize(('field', 'relation', 'values', 'expected'), _SCALAR_CASES)
async def test_scalar_relation_results(
    query_site: FakeSiteCase, field: str, relation: str, values: list[object], expected: list[int]
) -> None:
    await _scalar_rows(query_site, field)
    result = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [[field, relation, values]]),
    )
    assert [row['id'] for row in result] == expected


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('route', ['client', 'raw'])
@pytest.mark.parametrize('fault', _SCALAR_FAULTS)
async def test_scalar_relation_faults_before_rows(
    query_site: FakeSiteCase,
    populated: bool,
    route: str,
    fault: tuple[str, str, list[object], int, str],
) -> None:
    field, relation, values, code, message = fault
    if populated:
        await _scalar_rows(query_site, field)
    before = await query_site.call('find', 'Scalar', [], [field])
    if route == 'client':
        with pytest.raises(Fault, match=re.escape(message)):
            await query_site.call('find', 'Scalar', [[field, relation, values]])
    else:
        raw = raw_read(query_site.client._server, 'Scalar', [(field, relation, values)])
        assert raw['message'] == message
        assert raw['error_code'] == code
    assert await query_site.call('find', 'Scalar', [], [field]) == before


@pytest.mark.anyio
@pytest.mark.parametrize(('field', 'relation', 'values'), _SCALAR_ERRORS)
async def test_scalar_unsettled_operands_name_the_field(
    query_site: FakeSiteCase, field: str, relation: str, values: list[object]
) -> None:
    await _scalar_rows(query_site, field)
    before = await query_site.call('find', 'Scalar', [], [field])
    with pytest.raises(MockgunError, match=re.escape(f'Scalar.{field}')):
        await query_site.call('find', 'Scalar', [[field, relation, values]])
    with pytest.raises(MockgunError, match=re.escape(f'Scalar.{field}')):
        raw_read(query_site.client._server, 'Scalar', [(field, relation, values)])
    assert await query_site.call('find', 'Scalar', [], [field]) == before


@pytest.mark.anyio
async def test_stored_null_checkbox_is_unsettled(fake_site: FakeSiteCase) -> None:
    await create_link(fake_site, 'ScalarTypes', {'checkbox': True})
    await create_link(fake_site, 'ScalarTypes', {})
    with pytest.raises(MockgunError, match=r'ScalarTypes\.checkbox'):
        await fake_site.call('find', 'ScalarTypes', [['checkbox', 'is', True]])


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('route', ['client', 'raw'])
@pytest.mark.parametrize(
    'fault',
    [
        ('Scalar', 'no_such', "API read() Scalar.no_such doesn't exist"),
        ('Shot', 'project.Project.no_such', "API read() Project.no_such doesn't exist"),
    ],
)
async def test_missing_filter_fields_have_server_fault(
    query_site: FakeSiteCase,
    populated: bool,
    route: str,
    fault: tuple[str, str, str],
) -> None:
    kind, field, message = fault
    if populated:
        await create_link(query_site, kind, {'code': 'S1'} if kind == 'Shot' else {})
    if route == 'client':
        with pytest.raises(Fault, match=re.escape(message)):
            await query_site.call('find', kind, [[field, 'is', 1]])
    else:
        raw = raw_read(query_site.client._server, kind, [(field, 'is', [1])])
        assert raw['message'] == message
        assert raw['error_code'] == 103


@pytest.mark.anyio
@pytest.mark.parametrize('operand', [None, False])
@pytest.mark.parametrize('relation', ['contains', 'not_contains'])
async def test_text_fragment_falsy_operands_preserve_match_semantics(
    query_site: FakeSiteCase, operand: object, relation: str
) -> None:
    await _scalar_rows(query_site, 'sg_text')
    result = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [['sg_text', relation, operand]]),
    )
    expected = [1, 2, 3, 4, 5] if relation == 'contains' else []
    assert [row['id'] for row in result] == expected


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_currency', 'sg_timecode'])
async def test_unknown_membership_vocabulary_requires_emulation(
    query_site: FakeSiteCase, field: str
) -> None:
    with pytest.raises(MockgunError, match=re.escape(f'Scalar.{field}')):
        await query_site.call('find', 'Scalar', [[field, 'in', [None]]])


@pytest.mark.anyio
@pytest.mark.parametrize('case', _LINK_CASES)
async def test_link_relations(
    query_site: FakeSiteCase,
    case: tuple[str, str, str, list[object], list[str]],
) -> None:
    kind, field, relation, operands, expected = case
    records = await _relation_records(query_site)
    values = [records.get(value, value) if isinstance(value, str) else value for value in operands]
    result = cast(
        'list[dict[str, object]]',
        await query_site.call('find', kind, [[field, relation, values]]),
    )
    assert [row['id'] for row in result] == [records[key]['id'] for key in expected]


@pytest.mark.anyio
async def test_link_identity_ignores_extra_keys(query_site: FakeSiteCase) -> None:
    records = await _relation_records(query_site)
    result = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Shot', [['project', 'is', {**records['PA'], 'name': 'x'}]]),
    )
    assert [row['id'] for row in result] == [records['S1']['id']]
    shot = await create_link(query_site, 'Shot', {'code': 'S010'})
    version = await create_link(query_site, 'Version', {'code': 'v1', 'entity': shot})
    assert await query_site.call('find', 'Version', [['entity', 'is', shot]]) == [version]


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(('kind', 'field'), [('Shot', 'project'), ('Note', 'note_links')])
@pytest.mark.parametrize('relation', ['in', 'not_in'])
async def test_link_empty_membership_is_unsettled(
    query_site: FakeSiteCase, populated: bool, kind: str, field: str, relation: str
) -> None:
    if populated:
        await _relation_records(query_site)
    values: list[object] = []
    with pytest.raises(MockgunError, match=re.escape(f'{kind}.{field}')):
        await query_site.call('find', kind, [[field, relation, values]])


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('route', ['client', 'raw'])
@pytest.mark.parametrize('fault', ['string_id', 'null_list', 'multi_is'])
async def test_link_relation_faults(
    query_site: FakeSiteCase, populated: bool, route: str, fault: str
) -> None:
    records = await _relation_records(query_site) if populated else {}
    project = records.get('PA', {'type': 'Project', 'id': 1})
    values: list[object]
    if fault == 'string_id':
        kind, field, relation, code = 'Shot', 'project', 'is', 103
        values = [{'type': 'Project', 'id': '1'}]
        message = (
            "API read() invalid/missing entity hash integer 'id':\n"
            '{"type" => "Project", "id" => "1"}'
        )
    elif fault == 'null_list':
        kind, field, relation, code = 'Shot', 'project', 'in', 104
        values = [project, None]
        message = 'API read() Read failed for entity type [Shot]'
    else:
        kind, field, relation, code = 'Note', 'note_links', 'is', 103
        values = [
            records.get('S1', {'type': 'Shot', 'id': 1}),
            records.get('A1', {'type': 'Asset', 'id': 2}),
        ]
        message = "API read() 'is' relation expects a single value for Note.note_links"
    before = await query_site.call('find', kind, [], ['id'])
    if route == 'client':
        with pytest.raises(Fault, match=re.escape(message)):
            await query_site.call('find', kind, [[field, relation, values]])
    else:
        response = raw_read(query_site.client._server, kind, [(field, relation, values)])
        assert response['error_code'] == code
        assert response['message'] == message
    assert await query_site.call('find', kind, [], ['id']) == before


@pytest.mark.anyio
async def test_link_null_display_field_is_unsettled(query_site: FakeSiteCase) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'alpha'})
    await create_link(query_site, 'Scalar', {'sg_link': shot})
    await query_site.call('update', 'Shot', shot['id'], {'code': None})
    with pytest.raises(MockgunError, match=r'Scalar\.sg_link.*entity.*name_contains'):
        await query_site.call('find', 'Scalar', [['sg_link', 'name_contains', 'a']])


@pytest.mark.anyio
async def test_link_unmodeled_multi_display_name_is_unsettled(fake_site: FakeSiteCase) -> None:
    user = await create_link(fake_site, 'HumanUser', {'login': 'alpha'})
    await create_link(fake_site, 'Shot', {'code': 'shot', 'reviewers': [user]})
    with pytest.raises(MockgunError, match=r'Shot\.reviewers.*multi_entity.*name_contains'):
        await fake_site.call('find', 'Shot', [['reviewers', 'name_contains', 'a']])


@pytest.mark.anyio
@pytest.mark.parametrize('derived', ['task_fallback', 'cut_note'])
async def test_link_derived_display_names_are_unsettled_for_relations(
    query_site: FakeSiteCase, derived: str
) -> None:
    if derived == 'task_fallback':
        records = await _relation_records(query_site)
        await create_link(query_site, 'Version', {'code': 'v1', 'sg_task': records['T1']})
        await create_link(query_site, 'Version', {'code': 'v3', 'sg_task': records['T3']})
        await query_site.call('update', 'Task', records['T3']['id'], {'content': None})
        kind, field, relation, operand = 'Version', 'sg_task', 'name_contains', 't'
    else:
        note = await create_link(query_site, 'Note', {'content': 'c' * 300})
        await create_link(query_site, 'Scalar', {'sg_link': note})
        kind, field, relation, operand = 'Scalar', 'sg_link', 'name_is', 'c' * 255
    with pytest.raises(MockgunError, match=re.escape(f'{kind}.{field}') + f'.*entity.*{relation}'):
        await query_site.call('find', kind, [[field, relation, operand]])


@pytest.mark.anyio
async def test_link_deep_type_mismatch_matching_null_is_unsettled(
    query_site: FakeSiteCase,
) -> None:
    await _relation_records(query_site)
    with pytest.raises(MockgunError, match=r'Task\.entity\.Shot\.code.*text.*is_not'):
        await query_site.call('find', 'Task', [['entity.Shot.code', 'is_not', 'alpha']])


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(('kind', 'field'), [('Shot', 'project'), ('Note', 'note_links')])
async def test_link_null_list_with_invalid_id_has_unsettled_precedence(
    query_site: FakeSiteCase, populated: bool, kind: str, field: str
) -> None:
    if populated:
        await _relation_records(query_site)
    values: list[object] = [{'type': 'Project', 'id': '1'}, None]
    with pytest.raises(MockgunError, match=re.escape(f'{kind}.{field}')):
        await query_site.call('find', kind, [[field, 'in', values]])


@pytest.mark.anyio
@pytest.mark.parametrize('relation', ['name_is', 'name_contains', 'name_not_contains'])
async def test_link_multi_names_validate_members_after_a_match(
    query_site: FakeSiteCase, relation: str
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'alpha'})
    asset = await create_link(query_site, 'Asset', {'code': 'prop'})
    await create_link(query_site, 'Note', {'subject': 'n1', 'note_links': [shot, asset]})
    await query_site.call('update', 'Asset', asset['id'], {'code': None})
    with pytest.raises(MockgunError, match=r'Note\.note_links.*multi_entity.*' + relation):
        await query_site.call('find', 'Note', [['note_links', relation, 'alpha']])


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_link', 'sg_links'])
async def test_link_deep_missing_target_is_unsettled(query_site: FakeSiteCase, field: str) -> None:
    link = {'type': 'Shot', 'id': 999}
    await create_link(query_site, 'Scalar', {field: [link] if field == 'sg_links' else link})
    with pytest.raises(MockgunError, match=re.escape(f'Scalar.{field}.Shot.code') + '.*text.*is'):
        await query_site.call('find', 'Scalar', [[f'{field}.Shot.code', 'is', 'alpha']])


@pytest.mark.anyio
async def test_link_deep_missing_wrong_type_target_is_unsettled(query_site: FakeSiteCase) -> None:
    await create_link(
        query_site, 'Note', {'subject': 'n1', 'note_links': [{'type': 'Asset', 'id': 999}]}
    )
    with pytest.raises(
        MockgunError, match=re.escape("Note.note_links.Shot.code text is ['alpha']")
    ):
        await query_site.call('find', 'Note', [['note_links.Shot.code', 'is', 'alpha']])


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_link_deep_target_outside_partial_schema_is_unsettled(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema_path = tmp_path / 'partial-query-schema.pickle'
    entity_path = tmp_path / 'partial-query-entities.pickle'
    schema_path.write_bytes(
        pickle.dumps(
            {key: value for key, value in QUERY_SCHEMA.items() if key != 'Asset'}, protocol=2
        )
    )
    entity_path.write_bytes(
        pickle.dumps(
            {key: value for key, value in QUERY_SCHEMA_ENTITY.items() if key != 'Asset'},
            protocol=2,
        )
    )
    monkeypatch.setattr(Shotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    client = (Shotgun if kind == 'sync' else AsyncShotgun)(
        'http://unit-test.example', script_name='queries', api_key='key'
    )
    case = FakeSiteCase(kind, client)
    try:
        await create_link(
            case, 'Note', {'subject': 'n1', 'note_links': [{'type': 'Asset', 'id': 1}]}
        )
        with pytest.raises(
            MockgunError, match=re.escape("Note.note_links.Shot.code text is ['alpha']")
        ):
            await case.call('find', 'Note', [['note_links.Shot.code', 'is', 'alpha']])
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_link_multi_deep_type_mismatch_matching_null_is_unsettled(
    query_site: FakeSiteCase,
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'alpha'})
    asset = await create_link(query_site, 'Asset', {'code': 'prop'})
    await create_link(query_site, 'Note', {'subject': 'n1', 'note_links': [shot, asset]})
    with pytest.raises(MockgunError, match=r'Note\.note_links\.Shot\.code.*text.*is_not'):
        await query_site.call('find', 'Note', [['note_links.Shot.code', 'is_not', 'beta']])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('clauses', 'expected'),
    [
        ([('sg_number', 'asc'), ('sg_text', 'desc')], [2, 4, 1, 3]),
        ([('sg_text', 'asc'), ('sg_number', 'desc')], [3, 4, 1, 2]),
    ],
)
async def test_order_first_clause_is_primary(
    query_site: FakeSiteCase, clauses: list[tuple[str, str]], expected: list[int]
) -> None:
    for number, text in [(2, 'b'), (1, 'b'), (2, 'a'), (1, 'a')]:
        await create_link(query_site, 'Scalar', {'sg_number': number, 'sg_text': text})
    assert await _ordered_ids(query_site, 'Scalar', clauses) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('field', 'values'),
    [
        ('sg_number', [1, None, 2]),
        ('sg_float', [1.0, None, 2.0]),
        ('sg_duration', [60, None, 120]),
        ('sg_date', ['2026-01-01', None, '2026-01-02']),
        ('sg_date_time', ['2026-01-01T00:00:00Z', None, '2026-01-02T00:00:00Z']),
        ('sg_list', ['Character', None, 'Prop']),
        ('sg_status', ['wtg', None, 'fin']),
        ('sg_text', ['a', None, 'b']),
    ],
)
async def test_order_nulls_follow_values(
    query_site: FakeSiteCase, direction: str, field: str, values: list[object]
) -> None:
    for value in values:
        await create_link(query_site, 'Scalar', {field: value})
    expected = [1, 3, 2] if direction == 'asc' else [3, 1, 2]
    assert await _ordered_ids(query_site, 'Scalar', [(field, direction)]) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('values', 'expected'),
    [
        (['b', 'B', 'a', 'A', '10', '9', 'Zed'], ([5, 6, 3, 4, 1, 2, 7], [7, 1, 2, 3, 4, 6, 5])),
        (
            ['b', 'B', 'a', 'A', '10', '9', 'Zed', '_x'],
            ([5, 6, 3, 4, 1, 2, 8, 7], [7, 8, 1, 2, 3, 4, 6, 5]),
        ),
        (
            ['ab', 'a_c', 'a-d', 'a.e', 'a f', 'a1', 'a10', 'a2', 'aB', 'A_a'],
            ([6, 7, 8, 10, 1, 9, 2, 3, 4, 5], [5, 4, 3, 2, 1, 9, 10, 8, 7, 6]),
        ),
        (['x/y', 'ab'], ([2, 1], [1, 2])),  # Guard: punctuation cannot decide this pair.
        (['shot_010', 'shot_020'], ([1, 2], [2, 1])),
        (['tie', 'tie', 'tie'], ([1, 2, 3], [1, 2, 3])),
    ],
)
async def test_order_text_collation(
    query_site: FakeSiteCase,
    direction: str,
    values: list[str],
    expected: tuple[list[int], list[int]],
) -> None:
    for value in values:
        await create_link(query_site, 'Scalar', {'sg_text': value})
    assert (
        await _ordered_ids(query_site, 'Scalar', [('sg_text', direction)])
        == expected[direction == 'desc']
    )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize('values', [['ab', 'a_b'], ['a/c', 'ab'], ['caf\u00e9', 'cafe']])
async def test_order_rejects_unsettled_collation(
    query_site: FakeSiteCase, direction: str, values: list[str]
) -> None:
    for value in values:
        await create_link(query_site, 'Scalar', {'sg_text': value})
    with pytest.raises(MockgunError, match=r'Scalar.sg_text'):
        await _ordered_ids(query_site, 'Scalar', [('sg_text', direction)])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('numbers', 'lists', 'clauses', 'expected'),
    [
        ([1, 2], ['Prop', 'Prop'], [('sg_number', 'asc'), ('sg_text', 'asc')], [1, 2]),  # Guard.
        ([1, 2], ['Prop', 'Prop'], [('sg_number', 'desc'), ('sg_text', 'asc')], [2, 1]),
        ([1, 1], ['Prop', 'Prop'], [('sg_number', 'asc'), ('sg_text', 'asc')], None),
        (
            [1, 1],
            ['Prop', 'Character'],
            [('sg_number', 'asc'), ('sg_list', 'asc'), ('sg_text', 'asc')],
            [2, 1],
        ),
        (
            [1, 1],
            ['Prop', 'Prop'],
            [('sg_number', 'asc'), ('sg_list', 'asc'), ('sg_text', 'asc')],
            None,
        ),
    ],
)
async def test_order_collation_checks_only_preceding_clause_ties(
    query_site: FakeSiteCase,
    numbers: list[int],
    lists: list[str],
    clauses: list[tuple[str, str]],
    expected: list[int] | None,
) -> None:
    for number, item, text in zip(numbers, lists, ['a_b', 'ab'], strict=True):
        await create_link(
            query_site, 'Scalar', {'sg_number': number, 'sg_list': item, 'sg_text': text}
        )
    if expected is None:
        with pytest.raises(MockgunError, match=r'Scalar.sg_text'):
            await _ordered_ids(query_site, 'Scalar', clauses)
    else:
        assert await _ordered_ids(query_site, 'Scalar', clauses) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('field', 'values', 'expected'),
    [
        ('sg_status', ['fin', 'wtg', 'ip'], ([2, 3, 1], [1, 3, 2])),
        ('sg_list', ['Prop', 'Character', 'Environment'], ([2, 3, 1], [1, 3, 2])),  # Control.
        ('sg_checkbox', [True, False, None], ([2, 3, 1], [1, 2, 3])),  # Control.
        ('sg_float', [None, 2.5, None, 2.5], ([2, 4, 1, 3], [2, 4, 1, 3])),
        ('sg_url', [None, {'url': 'https://example.invalid/a', 'name': 'a'}], ([2, 1], [2, 1])),
    ],
)
async def test_order_scalar_type_rules(
    query_site: FakeSiteCase,
    direction: str,
    field: str,
    values: list[object],
    expected: tuple[list[int], list[int]],
) -> None:
    for value in values:
        await create_link(query_site, 'Scalar', {} if value is None else {field: value})
    assert (
        await _ordered_ids(query_site, 'Scalar', [(field, direction)])
        == expected[direction == 'desc']
    )


@pytest.mark.anyio
async def test_order_status_rejects_default_outside_valid_values(query_site: FakeSiteCase) -> None:
    metadata = cast(
        'dict[str, object]', query_site.client._server.schema.fields['Scalar']['sg_status']
    )
    properties = cast('dict[str, object]', metadata['properties'])
    properties['default_value'] = {'value': 'na'}
    await create_link(query_site, 'Scalar', {})
    await create_link(query_site, 'Scalar', {'sg_status': 'wtg'})
    with pytest.raises(MockgunError, match=r'Scalar.sg_status'):
        await _ordered_ids(query_site, 'Scalar', [('sg_status', 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_order_null_checkbox_ties(fake_site: FakeSiteCase, direction: str) -> None:
    await create_link(fake_site, 'ScalarTypes', {})
    await create_link(fake_site, 'ScalarTypes', {})
    assert await _ordered_ids(fake_site, 'ScalarTypes', [('checkbox', direction)]) == [1, 2]


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_url', 'sg_serializable'])
async def test_order_rejects_multiple_opaque_values(query_site: FakeSiteCase, field: str) -> None:
    for number in [1, 2]:
        await create_link(
            query_site,
            'Scalar',
            {'sg_number': number, field: {'url': 'https://example.invalid/a'}},
        )
    with pytest.raises(MockgunError, match=rf'Scalar.{field}'):
        await _ordered_ids(query_site, 'Scalar', [(field, 'asc')])
    if field == 'sg_serializable':
        with pytest.raises(MockgunError, match=rf'Scalar.{field}'):
            await _ordered_ids(query_site, 'Scalar', [('sg_number', 'asc'), (field, 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('clauses', 'expected'),
    [
        ([('no_such', 'asc')], [1, 2]),
        ([('no_such', 'asc'), ('sg_number', 'desc')], [2, 1]),
        ([('sg_number', 'desc'), ('no_such', 'asc')], [2, 1]),
    ],
)
async def test_order_ignores_unknown_top_level_fields(
    query_site: FakeSiteCase, clauses: list[tuple[str, str]], expected: list[int]
) -> None:
    for number in [1, 2]:
        await create_link(query_site, 'Scalar', {'sg_number': number})
    assert await _ordered_ids(query_site, 'Scalar', clauses) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize('field', ['sg_sequence', 'sg_sequence.Sequence.code'])
async def test_order_entity_names_and_deep_values(
    query_site: FakeSiteCase, direction: str, field: str
) -> None:
    for name in ['b-seq', 'a-seq']:
        sequence = await create_link(query_site, 'Sequence', {'code': name})
        await create_link(query_site, 'Shot', {'sg_sequence': sequence})
    await create_link(query_site, 'Shot', {})
    expected = [2, 1, 3] if direction == 'asc' else [1, 2, 3]
    assert await _ordered_ids(query_site, 'Shot', [(field, direction)]) == expected


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_order_rejects_distinct_entities_with_equal_names(
    query_site: FakeSiteCase, direction: str
) -> None:
    for name in ['seq-a', 'SEQ-A']:
        sequence = await create_link(query_site, 'Sequence', {'code': name})
        await create_link(query_site, 'Shot', {'sg_sequence': sequence})
    with pytest.raises(MockgunError, match=r'Shot.sg_sequence'):
        await _ordered_ids(query_site, 'Shot', [('sg_sequence', direction)])


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_order_same_entity_and_null_ties(query_site: FakeSiteCase, direction: str) -> None:
    sequence = await create_link(query_site, 'Sequence', {'code': 'seq-a'})
    data_rows: list[dict[str, object]] = [
        {'sg_sequence': sequence},
        {'sg_sequence': sequence},
        {},
        {},
    ]
    for data in data_rows:
        await create_link(query_site, 'Shot', data)
    assert await _ordered_ids(query_site, 'Shot', [('sg_sequence', direction)]) == [1, 2, 3, 4]


@pytest.mark.anyio
async def test_order_entity_names_across_target_types(query_site: FakeSiteCase) -> None:
    targets = [
        await create_link(query_site, kind, {'code': name})
        for kind, name in [('Shot', 'b-shot'), ('Asset', 'a-asset'), ('Sequence', 'c-seq')]
    ]
    for target in [*targets, None]:
        await create_link(query_site, 'Version', {'code': 'v', 'entity': target})
    assert await _ordered_ids(query_site, 'Version', [('entity', 'asc')]) == [2, 1, 3, 4]
    assert await _ordered_ids(query_site, 'Version', [('entity', 'desc')]) == [3, 1, 2, 4]


@pytest.mark.anyio
@pytest.mark.parametrize('names', [('same', 'SAME'), ('ab', 'a/z')])
async def test_order_entity_names_across_types_rejects_ambiguous_collation(
    query_site: FakeSiteCase, names: tuple[str, str]
) -> None:
    for kind, name in zip(('Shot', 'Asset'), names, strict=True):
        target = await create_link(query_site, kind, {'code': name})
        await create_link(query_site, 'Version', {'code': 'v', 'entity': target})
    with pytest.raises(MockgunError, match=r'Version\.entity'):
        await _ordered_ids(query_site, 'Version', [('entity', 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['mixed', 'derived', 'multi'])
@pytest.mark.parametrize('tied', [False, True])
async def test_order_link_checks_only_preceding_clause_ties(
    query_site: FakeSiteCase, kind: str, tied: bool
) -> None:
    if kind == 'mixed':
        first = await create_link(query_site, 'Shot', {'code': 's0'})
        second = await create_link(query_site, 'Asset', {'code': 'a1'})
        entity_type, primary, field = 'Scalar', 'sg_number', 'sg_link'
        values: list[object] = [first, second]
        keys: list[object] = [1, 1 if tied else 2]
    elif kind == 'derived':
        first = await create_link(query_site, 'Task', {'content': 'b'})
        second = await create_link(query_site, 'Task', {'content': 'a'})
        entity_type, primary, field = 'Version', 'code', 'sg_task'
        values = [first, second]
        keys = ['v', 'v' if tied else 'v2']
        await query_site.call('update', 'Task', second['id'], {'content': None})
    else:
        first = await create_link(query_site, 'Asset', {'code': 'a1'})
        second = await create_link(query_site, 'Asset', {'code': 'a2'})
        entity_type, primary, field = 'Shot', 'sg_cut_order', 'assets'
        values = [[first], [second]]
        keys = [1, 1 if tied else 2]
    for value, key in zip(values, keys, strict=True):
        await create_link(query_site, entity_type, {primary: key, field: value})
    clauses = [(primary, 'asc'), (field, 'asc')]
    if tied and kind != 'mixed':
        with pytest.raises(MockgunError, match=rf'{entity_type}.{field}'):
            await _ordered_ids(query_site, entity_type, clauses)
    else:
        assert await _ordered_ids(query_site, entity_type, clauses) == ([2, 1] if tied else [1, 2])


@pytest.mark.anyio
async def test_order_task_names_rejects_derived_name(query_site: FakeSiteCase) -> None:
    for name in ['b', 'a']:
        task = await create_link(query_site, 'Task', {'content': name})
        await create_link(query_site, 'Version', {'code': name, 'sg_task': task})
    assert await _ordered_ids(query_site, 'Version', [('sg_task', 'asc')]) == [2, 1]
    await query_site.call('update', 'Task', 2, {'content': None})
    with pytest.raises(MockgunError, match=r'Version.sg_task'):
        await _ordered_ids(query_site, 'Version', [('sg_task', 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize('populated', [0, 1, 2])
async def test_order_multi_entity_occupancy(
    query_site: FakeSiteCase, direction: str, populated: int
) -> None:
    for index in range(populated):
        asset = await create_link(query_site, 'Asset', {'code': f'a{index}'})
        await create_link(query_site, 'Shot', {'assets': [asset]})
    for _ in range(2 if populated == 0 else 1):
        await create_link(query_site, 'Shot', {})
    if populated == 2:
        with pytest.raises(MockgunError, match=r'Shot.assets'):
            await _ordered_ids(query_site, 'Shot', [('assets', direction)])
    else:
        assert await _ordered_ids(query_site, 'Shot', [('assets', direction)]) == [1, 2]


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['assets.Asset.code', 'sg_sequence.Sequence.no_such'])
@pytest.mark.parametrize('count', [0, 1, 2])
async def test_order_rejects_static_deep_paths_before_rows(
    query_site: FakeSiteCase, field: str, count: int
) -> None:
    for _ in range(count):
        await create_link(query_site, 'Shot', {})
    with pytest.raises(MockgunError, match=rf'Shot.{field}'):
        await _ordered_ids(query_site, 'Shot', [(field, 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_sequence', 'sg_sequence.Sequence.code'])
async def test_order_rejects_hidden_links(query_site: FakeSiteCase, field: str) -> None:
    sequence = await create_link(query_site, 'Sequence', {'code': 'b'})
    await create_link(query_site, 'Shot', {'sg_sequence': sequence})
    await create_link(query_site, 'Shot', {})
    await query_site.call('delete', 'Sequence', sequence['id'])
    with pytest.raises(MockgunError, match=rf'Shot.{field}'):
        await _ordered_ids(query_site, 'Shot', [(field, 'asc')])


@pytest.mark.anyio
@pytest.mark.parametrize(
    'field', ['sg_serializable', 'sg_link', 'sg_links', 'sg_url', 'sg_checkbox']
)
async def test_order_single_candidate_reads_no_key(query_site: FakeSiteCase, field: str) -> None:
    await create_link(query_site, 'Scalar', {})
    assert await _ordered_ids(query_site, 'Scalar', [(field, 'asc')]) == [1]


@pytest.mark.parametrize(('value', 'expected'), [('A_b-2.0 X', 'ab20x'), ('ÄZß09', 'z09')])
def test_collation_key_keeps_only_ascii_letters_and_digits(value: str, expected: str) -> None:
    assert collation_key(value) == expected


@pytest.mark.parametrize(
    'values', [[], ['a/b'], ['A', 'a'], ['a/c', 'a/d'], ['x/y', 'ab'], ['a/b', 'a/bc']]
)
def test_check_collation_accepts_equal_lowered_values_and_unambiguous_pairs(
    values: list[str],
) -> None:
    check_collation(values, entity_type='Scalar', field_name='sg_text')


@pytest.mark.parametrize('values', [['a_b', 'ab'], ['a/c', 'ab'], ['café', 'cafe'], ['É', 'é']])
def test_check_collation_reports_the_supplied_field_for_unsettled_pairs(values: list[str]) -> None:
    with pytest.raises(MockgunError, match=r'Scalar.sg_text'):
        check_collation(values, entity_type='Scalar', field_name='sg_text')


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_order_serializable_rejection_ignores_preceding_ties(
    query_site: FakeSiteCase, direction: str
) -> None:
    for number in [1, 2]:
        await create_link(query_site, 'Scalar', {'sg_number': number})
    with pytest.raises(MockgunError, match=r'Scalar.sg_serializable'):
        await _ordered_ids(
            query_site, 'Scalar', [('sg_number', direction), ('sg_serializable', direction)]
        )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_order_image_null_placement(query_site: FakeSiteCase, direction: str) -> None:
    record = await create_link(query_site, 'Scalar', {})
    row = query_site.client._server.store.row('Scalar', entity_id(record))
    assert row is not None
    # Image upload is outside the fake site's read-side contract.
    row['image'] = 'image.jpg'
    await create_link(query_site, 'Scalar', {})
    assert await _ordered_ids(query_site, 'Scalar', [('image', direction)]) == (
        [2, 1] if direction == 'asc' else [1, 2]
    )


@pytest.mark.anyio
async def test_order_identical_derived_target_needs_no_name(query_site: FakeSiteCase) -> None:
    task = await create_link(query_site, 'Task', {'content': 'task'})
    for _ in range(2):
        await create_link(query_site, 'Version', {'code': 'v', 'sg_task': task})
    await query_site.call('update', 'Task', task['id'], {'content': None})
    assert await _ordered_ids(query_site, 'Version', [('sg_task', 'asc')]) == [1, 2]


@pytest.mark.anyio
@pytest.mark.parametrize('tied', [False, True])
@pytest.mark.parametrize('state', ['different_type', 'hidden', 'missing'])
async def test_order_deep_candidate_checks_only_preceding_clause_ties(
    query_site: FakeSiteCase, tied: bool, state: str
) -> None:
    sequence = await create_link(query_site, 'Sequence', {'code': 'seq'})
    target = sequence
    if state == 'different_type':
        target = await create_link(query_site, 'Asset', {'code': 'asset'})
    await create_link(query_site, 'Shot', {'sg_cut_order': 1, 'sg_sequence': target})
    await create_link(query_site, 'Shot', {'sg_cut_order': 1 if tied else 2})
    if state == 'hidden':
        await query_site.call('delete', 'Sequence', sequence['id'])
    elif state == 'missing':
        # Store corruption isolates a dangling link without adding a public mutation API.
        query_site.client._server.store._db['Sequence'].pop(entity_id(sequence))
    clauses = [('sg_cut_order', 'asc'), ('sg_sequence.Sequence.code', 'asc')]
    if tied:
        with pytest.raises(MockgunError, match=r'Shot.sg_sequence.Sequence.code'):
            await _ordered_ids(query_site, 'Shot', clauses)
    else:
        assert await _ordered_ids(query_site, 'Shot', clauses) == [1, 2]


@pytest.mark.anyio
@pytest.mark.parametrize('read_kind', ['absent', 'anonymous', 'human', 'sudo', 'script'])
async def test_projection_orders_note_links_by_name_in_every_context(
    query_site: FakeSiteCase, read_kind: str
) -> None:
    shot = seed(query_site, 'Shot', {'code': 'shot'})
    asset = seed(query_site, 'Asset', {'code': 'asset'})
    note = seed(query_site, 'Note', {'note_links': [shot, asset]})
    server = query_site.client._server
    row = server.store.row('Note', entity_id(note))
    assert row is not None
    row['created_by'] = {'type': 'ApiUser', 'id': 1}
    script_auth: dict[str, object] = {'script_name': 'queries', 'script_key': 'key'}
    server.store.context = server.context_for(script_auth, method_name='read')
    auth = {
        'anonymous': None,
        'human': {'session_token': 'projection-human'},
        'sudo': {**script_auth, 'sudo_as_login': 'projection-sudo'},
        'script': script_auth,
    }
    context = (
        None if read_kind == 'absent' else server.context_for(auth[read_kind], method_name='read')
    )
    result = project_rows(server.store, server.schema, [row], ['note_links'], context=context)
    links = cast('list[dict[str, object]]', result[0]['note_links'])
    assert [link['type'] for link in links] == ['Asset', 'Shot']
    links[0]['name'] = 'detached'
    assert row['note_links'] == [shot, asset]


@pytest.mark.anyio
async def test_single_row_order_does_not_read_unknown_key(effects_site: FakeSiteCase) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {'content': 't'}))
    assert await effects_site.call(
        'find', 'Task', [], ['id'], order=[{'field_name': 'due_date', 'direction': 'asc'}]
    ) == [{'type': 'Task', 'id': task['id']}]


@pytest.mark.anyio
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_query_raw_read_accepts_authenticated_and_anonymous_requests(
    query_site: FakeSiteCase,
) -> None:
    shot = cast('dict[str, object]', await query_site.call('create', 'Shot', {'code': 'S1'}))
    server = query_site.client._server
    for auth in (None, dict[str, object]()):
        result = raw_read(server, 'Shot', [('code', 'is', ['S1'])], auth=auth)
        body = cast('dict[str, object]', result['results'])
        assert body['entities'] == [{'type': 'Shot', 'id': shot['id']}]


@pytest.mark.anyio
async def test_paging_slices_and_server_maximum(fake_site: FakeSiteCase) -> None:
    shots = [
        cast('dict[str, object]', await fake_site.call('create', 'Shot', {'code': f'shot-{i}'}))
        for i in range(5)
    ]
    identities = [{'type': 'Shot', 'id': shot['id']} for shot in shots]
    assert await fake_site.call('find', 'Shot', [], limit=2) == identities[:2]
    for page, expected in [(2, identities[2:4]), (3, identities[4:]), (4, [])]:
        assert await fake_site.call('find', 'Shot', [], limit=2, page=page) == expected
    fake_site.client.site.update_server_metadata({'api_max_entities_per_page': 2})
    await fake_site.call('info')
    fake_site.client.finds = 0
    assert await fake_site.call('find', 'Shot', []) == identities
    assert fake_site.client.finds == 1
    assert await fake_site.call('find', 'Shot', [], limit=3) == identities[:3]


@pytest.mark.anyio
async def test_deep_datetime_filter_projection_and_null_order(query_site: FakeSiteCase) -> None:
    when = datetime(2025, 2, 3, 4, 5, 6, 123456, tzinfo=UTC)
    shot = cast(
        'dict[str, object]',
        await query_site.call('create', 'Shot', {'code': 'timed', 'created_at': when}),
    )
    empty = cast(
        'dict[str, object]',
        await query_site.call('create', 'Shot', {'code': 'empty', 'created_at': None}),
    )
    ordered = cast(
        'list[dict[str, object]]',
        await query_site.call(
            'find', 'Shot', [], order=[{'field_name': 'created_at', 'direction': 'asc'}]
        ),
    )
    assert [item['id'] for item in ordered] == [shot['id'], empty['id']]
    note = cast(
        'dict[str, object]',
        await query_site.call('create', 'Note', {'subject': 'dated', 'note_links': [shot]}),
    )
    path = 'note_links.Shot.created_at'
    found = cast(
        'list[dict[str, object]]',
        await query_site.call(
            'find', 'Note', [[path, 'greater_than', when - timedelta(seconds=1)]], [path]
        ),
    )
    assert len(found) == 1
    assert found == [{'type': 'Note', 'id': note['id']}]


@pytest.mark.anyio
async def test_nested_multi_entity_paths_filter_but_are_left_out_of_results(
    query_site: FakeSiteCase,
) -> None:
    when = datetime(2025, 2, 3, 4, 5, 6, 123456, tzinfo=UTC)
    shot = cast(
        'dict[str, object]',
        await query_site.call('create', 'Shot', {'code': 'timed', 'created_at': when}),
    )
    missing = cast(
        'dict[str, object]',
        await query_site.call('create', 'Shot', {'code': 'missing', 'created_at': None}),
    )
    note = cast(
        'dict[str, object]',
        await query_site.call(
            'create', 'Note', {'subject': 'note', 'note_links': [shot, missing]}
        ),
    )
    # Single-entity links retain structural-only type validation in this phase.
    branch = cast(
        'dict[str, object]',
        await query_site.call('create', 'Shot', {'code': 'branch', 'sg_sequence': note}),
    )
    outer = cast(
        'dict[str, object]',
        await query_site.call('create', 'Note', {'subject': 'outer', 'note_links': [branch]}),
    )
    # The multi-entity path remains valid for filters but is omitted from results.
    path = 'note_links.Shot.sg_sequence.Note.note_links.Shot.created_at'
    found = cast(
        'list[dict[str, object]]',
        await query_site.call(
            'find', 'Note', [['id', 'is', outer['id']], [path, 'is', when]], [path]
        ),
    )
    assert found == [{'type': 'Note', 'id': outer['id']}]


@pytest.mark.anyio
async def test_members_read_sorted_by_current_name(query_site: FakeSiteCase) -> None:
    shots = {code: await create_link(query_site, 'Shot', {'code': code}) for code in 'mza'}
    created = cast(
        'dict[str, object]',
        await query_site.call(
            'create', 'Asset', {'code': 'h', 'shots': [shots[code] for code in 'zam']}, ['shots']
        ),
    )
    expected = [shots[code]['id'] for code in 'amz']
    assert [link['id'] for link in cast('list[dict[str, object]]', created['shots'])] == expected
    assert await _shot_ids(query_site, created) == expected
    await query_site.call('update', 'Shot', shots['a']['id'], {'code': 'zz'})
    assert await _shot_ids(query_site, created) == [shots[code]['id'] for code in 'mza']


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('written', 'expected'),
    [
        (['B1', 'a1'], ['a1', 'B1']),
        (['s9', 's10'], ['s10', 's9']),
        (['xa', 'x_b'], ['x_b', 'xa']),
        (['ya', 'y-b'], ['y-b', 'ya']),
        (['wa', 'w b'], ['w b', 'wa']),
        (['xa', 'x.b'], ['x.b', 'xa']),
        (['xa', 'x1'], ['x1', 'xa']),
        (['x~', 'xa'], ['xa', 'x~']),
        (['xZ', 'x_'], ['x_', 'xZ']),
        (['x\u00e9', 'xf'], ['xf', 'x\u00e9']),
        (['x\u00ea', 'x\u00e8'], ['x\u00e8', 'x\u00ea']),
    ],
)
async def test_member_names_compare_case_blind_by_character(
    query_site: FakeSiteCase, written: list[str], expected: list[str]
) -> None:
    shots = {code: await create_link(query_site, 'Shot', {'code': code}) for code in written}
    asset = await create_link(query_site, 'Asset', {'code': 'h', 'shots': list(shots.values())})
    assert await _shot_ids(query_site, asset) == [shots[code]['id'] for code in expected]


@pytest.mark.anyio
async def test_members_with_equal_names_read_by_id(query_site: FakeSiteCase) -> None:
    older = await create_link(query_site, 'Shot', {'code': 'Ab'})
    newer = await create_link(query_site, 'Shot', {'code': 'ab'})
    asset = await create_link(query_site, 'Asset', {'code': 'h', 'shots': [newer, older]})
    assert await _shot_ids(query_site, asset) == [older['id'], newer['id']]
    same = await create_link(query_site, 'Asset', {'code': 'Ab'})
    note = await create_link(query_site, 'Note', {'subject': 'n', 'note_links': [newer, same]})
    found = cast(
        'dict[str, object]',
        await query_site.call('find_one', 'Note', [['id', 'is', note['id']]], ['note_links']),
    )
    links = cast('list[dict[str, object]]', found['note_links'])
    assert [(link['type'], link['id']) for link in links] == sorted(
        [('Shot', newer['id']), ('Asset', same['id'])], key=lambda pair: cast('int', pair[1])
    )


@pytest.mark.anyio
async def test_member_order_raises_where_unobserved(query_site: FakeSiteCase) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'x\u00c9'})
    asset = await create_link(query_site, 'Asset', {'code': 'x\u00e8'})
    create = query_site.call('create', 'Note', {'subject': 'n', 'note_links': [shot, asset]})
    with pytest.raises(MockgunError, match=r'cannot order Note\.note_links members'):
        await create
    assert await query_site.call('find', 'Note', []) == []


@pytest.mark.anyio
async def test_unnamed_members_keep_stored_order_only_when_consistent(
    query_site: FakeSiteCase,
) -> None:
    first = await create_link(query_site, 'Shot', {'code': 'a'})
    second = await create_link(query_site, 'Shot', {'code': 'b'})
    missing = {'type': 'Shot', 'id': 9999}
    kept = await create_link(
        query_site, 'Asset', {'code': 'h1', 'shots': [first, missing, second]}
    )
    assert await _shot_ids(query_site, kept) == [first['id'], 9999, second['id']]
    create = query_site.call('create', 'Asset', {'code': 'h2', 'shots': [second, missing, first]})
    with pytest.raises(MockgunError, match='without a modeled display name'):
        await create
    assert await query_site.call('find', 'Asset', [['code', 'is', 'h2']]) == []


@pytest.mark.anyio
async def test_note_links_read_in_name_order(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'S'}))
    asset = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'A'}))
    other = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'Other'}))
    await case.call('create', 'Note', {'subject': 'shared', 'note_links': [shot, asset, other]})
    found = cast('dict[str, object]', await case.call('find_one', 'Note', [], ['note_links']))
    assert found['note_links'] == [
        {'type': 'Asset', 'id': asset['id'], 'name': 'A'},
        {'type': 'Shot', 'id': other['id'], 'name': 'Other'},
        {'type': 'Shot', 'id': shot['id'], 'name': 'S'},
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('creator_kind', 'reader_kind'),
    [('script', 'sudo'), ('script', 'human'), ('sudo', 'script'), ('human', 'script')],
)
async def test_note_link_order_is_the_same_for_every_actor(
    workflow_site: FakeSiteCase, creator_kind: str, reader_kind: str
) -> None:
    case = workflow_site
    shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'S'}))
    asset = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'A'}))
    await case.call('create', 'HumanUser', {'login': 'artist', 'name': 'artist'})
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    human = FakeSiteCase(
        case.kind, case.client.new_client(login='artist', password=_TEST_CREDENTIAL)
    )
    clients = {'script': case, 'sudo': sudo, 'human': human}
    try:
        note = cast(
            'dict[str, object]',
            await clients[creator_kind].call(
                'create', 'Note', {'subject': 'actor-boundary', 'note_links': [shot, asset]}
            ),
        )
        found = cast(
            'dict[str, object]',
            await clients[reader_kind].call(
                'find_one', 'Note', [['id', 'is', note['id']]], ['note_links']
            ),
        )
        assert found['note_links'] == [
            {'type': 'Asset', 'id': asset['id'], 'name': 'A'},
            {'type': 'Shot', 'id': shot['id'], 'name': 'S'},
        ]
    finally:
        await sudo.call('close')
        await human.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('path', 'multiple_members'),
    [('shots.Shot.project', True), ('shots.Shot.assets', False)],
)
async def test_multi_traversal_return_paths_are_omitted(
    query_site: FakeSiteCase, path: str, multiple_members: bool
) -> None:
    shot_data: dict[str, object] = {'code': 'S010'}
    if multiple_members:
        shot_data['project'] = await create_link(query_site, 'Project', {'name': 'Alpha'})
    shot = await create_link(query_site, 'Shot', shot_data)
    members = [shot]
    if multiple_members:
        members.append(await create_link(query_site, 'Shot', {'code': 'empty'}))
    asset = await create_link(query_site, 'Asset', {'code': 'asset', 'shots': members})
    assert await query_site.call('find_one', 'Asset', [['id', 'is', asset['id']]], [path]) == asset
