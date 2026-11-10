import copy
import pickle
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._sync_client import Shotgun as SiteShotgun

from .conftest import QUERY_SCHEMA, QUERY_SCHEMA_ENTITY, FakeSiteCase, raw_call, seed

_COUNT = [{'field': 'id', 'type': 'record_count'}]
_FILTERS: dict[str, object] = {'logical_operator': 'and', 'conditions': []}
_NUMBER_TYPES = '[:record_count, :count, :sum, :maximum, :minimum, :average]'
_DATE_GROUPS = '[:exact, :day, :week, :month, :quarter, :year, :clustered_date]'


async def _assert_groups(
    case: FakeSiteCase,
    entity_type: str,
    grouping: list[dict[str, str]],
    expected: list[dict[str, object]],
    **kwargs: object,
) -> None:
    summaries = kwargs.pop('summaries', _COUNT)
    filters = kwargs.pop('filters', [])
    result = await case.call(
        'summarize', entity_type, filters, summaries, grouping=grouping, **kwargs
    )
    ungrouped = await case.call('summarize', entity_type, filters, summaries, **kwargs)
    assert isinstance(ungrouped, dict)
    assert result == {'summaries': ungrouped['summaries'], 'groups': expected}


def _by(field: str, grouping_type: str, direction: str = 'asc') -> dict[str, str]:
    return {'field': field, 'type': grouping_type, 'direction': direction}


def _entry(field: str, summary_type: str, **kwargs: object) -> dict[str, object]:
    return {'field': field, 'type': summary_type, **kwargs}


def _group_link(link: dict[str, object], name: str) -> dict[str, object]:
    return {'type': link['type'], 'id': link['id'], 'name': name, 'valid': 'valid'}


def _group_shots(
    case: FakeSiteCase,
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    q1 = seed(case, 'Sequence', {'code': 'seq-a'})
    q2 = seed(case, 'Sequence', {'code': 'seq-b'})
    data: list[dict[str, object]] = [
        {'code': 'S1', 'sg_sequence': q1, 'sg_cut_order': 1, 'sg_status_list': 'ip'},
        {'code': 'S2', 'sg_sequence': q1, 'sg_cut_order': 9, 'sg_status_list': 'fin'},
        {'code': 'S3', 'sg_sequence': q2, 'sg_cut_order': 12, 'sg_status_list': 'ip'},
        {'code': 'S4', 'sg_status_list': 'fin'},
    ]
    shots = [seed(case, 'Shot', item) for item in data]
    return q1, q2, shots


def _invalid_group_message(grouping_type: str) -> str:
    return (
        f"API summarize() grouping option '{grouping_type}' not valid for field "
        f"Scalar.sg_date with data type 'date'\nValid grouping types:\n{_DATE_GROUPS}"
    )


def _invalid_summary_message(field: str, summary_type: str, data_type: str, valid: str) -> str:
    return (
        f"API summarize() summary type '{summary_type}' not valid for field "
        f"Scalar.{field} with data type '{data_type}'\nValid summary types:\n{valid}"
    )


def _raw_summary(
    query_site: FakeSiteCase,
    summaries: object,
    grouping: object | None = None,
    filters: dict[str, object] | None = None,
) -> dict[str, object]:
    params: dict[str, object] = {
        'type': 'Scalar',
        'filters': _FILTERS if filters is None else filters,
        'summaries': summaries,
    }
    if grouping is not None:
        params['grouping'] = grouping
    return raw_call(query_site.client._server, 'summarize', params)


def _seed_scalar_population(query_site: FakeSiteCase) -> None:
    shot = seed(query_site, 'Shot', {'code': 'S010'})
    seed(
        query_site,
        'Scalar',
        {
            'sg_number': 1,
            'sg_float': 1.5,
            'sg_text': 'a',
            'sg_status': 'ip',
            'sg_list': 'Prop',
            'sg_date': '2026-07-01',
            'sg_link': shot,
        },
    )
    seed(query_site, 'Scalar', {})


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    'case',
    [
        (
            [_entry('sg_text', 'sum')],
            None,
            103,
            _invalid_summary_message('sg_text', 'sum', 'text', '[:record_count, :count]'),
        ),
        pytest.param(
            (
                [_entry('sg_link.Shot.project.Project.name', 'sum')],
                None,
                103,
                _invalid_summary_message(
                    'sg_link.Shot.project.Project.name',
                    'sum',
                    'text',
                    '[:record_count, :count]',
                ),
            ),
            id='multi_link_invalid_summary',
        ),
        *[
            (
                [_entry(field, name)],
                None,
                103,
                _invalid_summary_message(field, name, data_type, _NUMBER_TYPES),
            )
            for field, name, data_type in [
                ('sg_number', 'max', 'number'),
                ('sg_float', 'min', 'float'),
                ('sg_float', 'avg', 'float'),
            ]
        ],
        *[
            (
                [_entry(field, name)],
                None,
                103,
                _invalid_summary_message(field, name, data_type, valid),
            )
            for field, name, data_type, valid in [
                ('sg_checkbox', 'count', 'checkbox', '[:record_count, :checked, :unchecked]'),
                (
                    'sg_status',
                    'percentage',
                    'status_list',
                    (
                        '[:record_count, :status_list, :status_percentage, '
                        ':status_percentage_as_float]'
                    ),
                ),
                (
                    'sg_duration',
                    'earliest',
                    'duration',
                    '[:record_count, :count, :sum, :minimum, :maximum, :average]',
                ),
                ('sg_url', 'count', 'url', '[:record_count]'),
                ('sg_date', 'bogus', 'date', '[:record_count, :count, :earliest, :latest]'),
                ('image', 'record_count', 'image', '[]'),
                ('sg_serializable', 'record_count', 'serializable', '[]'),
            ]
        ],
        (
            [_entry('sg_number', 'sum'), _entry('sg_number', 'average')],
            None,
            103,
            'API summarize() field Scalar.sg_number has more than one summary type',
        ),
        *[
            (
                [_entry(field, 'count')],
                None,
                None,
                f'Uncaught exception! Summary type not supported: {field} count',
            )
            for field in ['sg_link', 'sg_links']
        ],
        *[
            ([_entry('sg_status', name, **value)], None, 103, 'Percentage-based summaries require')
            for name, value in [
                ('status_percentage', {'value': ['ip']}),
                ('status_percentage', {}),
                ('status_percentage_as_float', {}),
            ]
        ],
        (
            [_entry('sg_status', 'status_percentage', values=['ip'])],
            None,
            103,
            'unrecognized values key',
        ),
        ([_entry('sg_list', 'percentage')], None, 103, 'Percentage-based summaries require'),
        ([_entry('no_such', 'count')], None, 103, "API summarize() Scalar.no_such doesn't exist"),
        (_COUNT, [_by('no_such', 'exact')], 103, "API summarize() Scalar.no_such doesn't exist"),
        (
            _COUNT,
            [{'field': 'sg_number', 'direction': 'asc'}],
            103,
            "invalid/missing string 'type'",
        ),
        (
            _COUNT,
            [{'field': 'sg_link', 'type': 'exact'}],
            None,
            "invalid/missing string 'direction'",
        ),
        (_COUNT, [_by('sg_link', 'exact', 'up')], None, "invalid/missing string 'direction'"),
        *[
            (
                _COUNT,
                [_by('sg_date', name)],
                103,
                _invalid_group_message(name),
            )
            for name in ['oneday', 'fivedays', 'tens', 'firstletter', 'bogus']
        ],
    ],
)
async def test_summary_faults_before_reading_rows(
    query_site: FakeSiteCase,
    populated: bool,
    case: tuple[list[dict[str, object]], list[dict[str, str]] | None, int | None, str],
) -> None:
    summaries, grouping, code, message = case
    if populated:
        _seed_scalar_population(query_site)
    with pytest.raises(Fault, match=r'summarize|Summary type'):
        await query_site.call('summarize', 'Scalar', [], summaries, grouping=grouping)
    raw = _raw_summary(query_site, summaries, grouping)
    assert raw['error_code'] == code
    assert message in str(raw['message'])


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('key', ['type', 'direction'])
async def test_explicit_null_grouping_options_are_unsupported(
    query_site: FakeSiteCase, populated: bool, key: str
) -> None:
    if populated:
        _seed_scalar_population(query_site)
    grouping: dict[str, object] = {'field': 'sg_number', 'type': 'exact', 'direction': 'asc'}
    grouping[key] = None
    with pytest.raises(MockgunError, match=f'invalid {key}'):
        await query_site.call('summarize', 'Scalar', [], _COUNT, grouping=[grouping])
    del grouping[key]
    with pytest.raises(Fault, match=f"invalid/missing string '{key}'"):
        await query_site.call('summarize', 'Scalar', [], _COUNT, grouping=[grouping])


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    ('summaries', 'grouping', 'fragment'),
    [
        *[
            ([_entry(field, 'record_count')], None, f'{field} {data_type}')
            for field, data_type in [
                ('sg_currency', 'currency'),
                ('sg_timecode', 'timecode'),
                ('type', 'entity_type'),
            ]
        ],
        ([_entry('sg_number', 'sum', value='x')], None, 'sg_number number'),
        ([_entry('sg_number', 'sum'), _entry('sg_number', 'sum')], None, 'sg_number number'),
        ([_entry('sg_list', 'percentage', value=['Prop'])], None, 'sg_list list'),
        ([_entry('sg_status', 'status_percentage', value=5)], None, 'sg_status status_list'),
        *[
            (_COUNT, [_by(field, name)], f'{field} {data_type}')
            for field, name, data_type in [
                ('sg_number', 'bogus', 'number'),
                ('sg_text', 'bogus', 'text'),
                ('sg_float', 'tens', 'float'),
                ('sg_duration', 'hundreds', 'duration'),
                ('sg_number', 'firstletter', 'number'),
                ('sg_links', 'entitytype', 'multi_entity'),
                ('sg_duration', 'exact', 'duration'),
                ('sg_percent', 'exact', 'percent'),
                ('sg_url', 'exact', 'url'),
                ('sg_date_time', 'clustered_date', 'date_time'),
                ('sg_date_time', 'oneday', 'date_time'),
                ('sg_link.Task.start_date', 'oneday', 'date'),
            ]
        ],
        (_COUNT, [_by('sg_link', 'exact'), _by('sg_link', 'exact')], 'sg_link entity'),
        *[
            ([_entry(path, 'count')], None, path)
            for path in [
                'sg_links.Shot.code',
                'sg_text.Shot.code',
                'sg_link.NoSuch.code',
                'sg_link.Shot.no_such',
                'sg_link.Shot.sg_sequence',
            ]
        ],
        pytest.param(
            [_entry('sg_link.Shot.code.Project.name', 'count')],
            None,
            'sg_link.Shot.code.Project.name',
            id='multi_link_invalid_traversal',
        ),
        (_COUNT, [_by('sg_link.Shot.sg_sequence', 'exact')], 'sg_link.Shot.sg_sequence'),
        ([_entry('sg_number', 'max')], [{'field': 'sg_link', 'type': 'exact'}], "'direction'"),
        ([_entry('sg_number', 'max')], [_by('sg_date', 'oneday')], "grouping option 'oneday'"),
        ([_entry('sg_number', 'max'), _entry('sg_currency', 'sum')], None, 'sg_currency'),
    ],
)
async def test_unsettled_summary_requests_name_the_input(
    query_site: FakeSiteCase,
    populated: bool,
    summaries: list[dict[str, object]],
    grouping: list[dict[str, str]] | None,
    fragment: str,
) -> None:
    if populated:
        _seed_scalar_population(query_site)
    with pytest.raises(MockgunError, match='summarize') as error:
        await query_site.call('summarize', 'Scalar', [], summaries, grouping=grouping)
    assert fragment in str(error.value)
    if fragment == 'sg_currency':
        assert "summary type 'max'" in str(error.value)


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
async def test_summary_fault_and_invalid_filter_precedence(
    query_site: FakeSiteCase, populated: bool
) -> None:
    if populated:
        _seed_scalar_population(query_site)
    invalid: dict[str, object] = {
        'logical_operator': 'and',
        'conditions': [{'path': 'no_such', 'relation': 'is', 'values': [1]}],
    }
    with pytest.raises(MockgunError, match="summary type 'max'") as error:
        await query_site.call(
            'summarize', 'Scalar', [['no_such', 'is', 1]], [_entry('sg_number', 'max')]
        )
    assert "Scalar.no_such doesn't exist" in str(error.value)
    with pytest.raises(Fault, match=r"Scalar\.no_such doesn't exist"):
        await query_site.call('summarize', 'Scalar', [['no_such', 'is', 1]], _COUNT)
    raw = _raw_summary(query_site, _COUNT, filters=invalid)
    assert raw['error_code'] == 103
    assert raw['message'] == "API read() Scalar.no_such doesn't exist"


@pytest.mark.anyio
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_summary_request_shape_guards(query_site: FakeSiteCase) -> None:
    for summaries, grouping in [
        ([{'type': 'count'}], None),
        ([{'field': 'id', 'type': 'count', 'label': 'x'}], None),
        ([], None),
        (_COUNT, []),
        (_COUNT, [_by('sg_number', 'exact')] * 4),
    ]:
        with pytest.raises(MockgunError):
            _raw_summary(query_site, summaries, grouping)
    with pytest.raises(MockgunError):
        raw_call(
            query_site.client._server,
            'summarize',
            {'type': 'NoSuch', 'filters': _FILTERS, 'summaries': _COUNT},
        )


@pytest.mark.anyio
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_non_mapping_summary_payload_is_rejected(query_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match='summarize'):
        raw_call(query_site.client._server, 'summarize', [])


@pytest.mark.anyio
async def test_record_count_uses_selected_rows_and_wrapped_wire_body(
    query_site: FakeSiteCase,
) -> None:
    seed(query_site, 'Scalar', {'sg_number': 1})
    seed(query_site, 'Scalar', {'sg_number': 2})
    seed(query_site, 'Scalar', {})

    assert await query_site.call('summarize', 'Scalar', [], _COUNT) == {
        'summaries': {'id': 3},
        'groups': [],
    }
    assert await query_site.call(
        'summarize', 'Scalar', [['sg_number', 'greater_than', 1]], _COUNT
    ) == {'summaries': {'id': 1}, 'groups': []}
    assert raw_call(
        query_site.client._server,
        'summarize',
        {'type': 'Scalar', 'filters': _FILTERS, 'summaries': _COUNT},
    ) == {'results': {'summaries': {'id': 3}, 'groups': []}}


@pytest.mark.anyio
async def test_project_cascade_reaches_summary_without_new_events(
    query_site: FakeSiteCase,
) -> None:
    project = seed(query_site, 'Project', {'name': 'Retired'})
    seed(query_site, 'Shot', {'code': 'shot', 'project': project})
    assert await query_site.call('delete', 'Project', project['id']) is True
    store = query_site.client._server.store
    before_events = len(store.rows('EventLogEntry', retired=False))

    assert await query_site.call('summarize', 'Shot', [], _COUNT) == {
        'summaries': {'id': 0},
        'groups': [],
    }
    assert len(store.rows('EventLogEntry', retired=False)) == before_events


@pytest.mark.anyio
async def test_project_taint_reaches_summary(query_site: FakeSiteCase) -> None:
    project = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    seed(query_site, 'Playlist', {'code': 'playlist', 'project': project})
    assert await query_site.call('delete', 'Project', project['id']) is True

    with pytest.raises(MockgunError, match=r'Playlist.*Project'):
        await query_site.call('summarize', 'Playlist', [], _COUNT)


@pytest.mark.anyio
async def test_record_count_excludes_retired_rows(query_site: FakeSiteCase) -> None:
    first = seed(query_site, 'Scalar', {'sg_number': 1})
    seed(query_site, 'Scalar', {'sg_number': 2})
    seed(query_site, 'Scalar', {})
    assert await query_site.call('delete', 'Scalar', first['id']) is True

    assert await query_site.call('summarize', 'Scalar', [], _COUNT) == {
        'summaries': {'id': 2},
        'groups': [],
    }


@pytest.mark.anyio
async def test_record_count_respects_archived_project_scope(query_site: FakeSiteCase) -> None:
    active = seed(query_site, 'Project', {'name': 'Active'})
    archived = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    seed(query_site, 'Scalar', {})
    seed(query_site, 'Scalar', {'project': active})
    seed(query_site, 'Scalar', {'project': archived})

    for include, expected in [(None, 3), (True, 3), (False, 2)]:
        options = {} if include is None else {'include_archived_projects': include}
        assert await query_site.call('summarize', 'Scalar', [], _COUNT, **options) == {
            'summaries': {'id': expected},
            'groups': [],
        }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'values', 'summary_type', 'expected'),
    [
        ('sg_text', ['a', 'b', 'c', None], 'count', 3),
        ('sg_text', ['a', 'b', 'c', None], 'record_count', 4),
        ('sg_url', [None, None], 'record_count', 2),
        *[
            (field, [1, 2, None], summary_type, expected)
            for field in ['sg_number', 'sg_duration']
            for summary_type, expected in [
                ('sum', 3),
                ('maximum', 2),
                ('minimum', 1),
                ('average', 1.5),
            ]
        ],
        *[
            ('sg_float', [0.1, 0.2, 0.7, None], summary_type, expected)
            for summary_type, expected in [
                ('sum', 1.0),
                ('maximum', 0.7),
                ('minimum', 0.1),
                ('average', 0.3333),
            ]
        ],
        ('sg_float', [0.1, 0.2], 'sum', 0.3),
        ('sg_float', [0.1, 0.2], 'average', 0.15),
        ('sg_float', [0.25, 0.5], 'sum', 0.75),
        ('sg_percent', [0, 25, 100, None], 'average', 41.6667),
        ('sg_number', [1, 1, 2], 'average', 1.3333),
        ('sg_number', [1, 2, 2], 'average', 1.6667),
        ('sg_number', [None, None], 'count', 0),
        ('sg_date', ['2026-07-10', '2026-07-01', None], 'earliest', '2026-07-01'),
        ('sg_date', ['2026-07-10', '2026-07-01', None], 'latest', '2026-07-10'),
        ('sg_checkbox', [True, False, None], 'checked', 1),
        ('sg_checkbox', [True, False, None], 'unchecked', 2),
    ],
)
async def test_summary_scalar_values(
    query_site: FakeSiteCase, field: str, values: list[object], summary_type: str, expected: object
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {} if value is None else {field: value})
    before = await query_site.call('find', 'Scalar', [], ['sg_number', 'sg_text'])
    result = await query_site.call('summarize', 'Scalar', [], [_entry(field, summary_type)])
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {field: expected}, 'groups': []}
    assert type(summaries[field]) is type(expected)
    assert await query_site.call('find', 'Scalar', [], ['sg_number', 'sg_text']) == before


@pytest.mark.anyio
async def test_summary_counts_and_numeric_results_in_request_order(
    query_site: FakeSiteCase,
) -> None:
    values: dict[str, object] = {
        'sg_number': 1,
        'sg_float': 1.5,
        'sg_duration': 60,
        'sg_percent': 10,
        'sg_date': '2026-07-01',
        'sg_date_time': datetime(2026, 7, 1, tzinfo=UTC),
        'sg_list': 'Prop',
    }
    seed(query_site, 'Scalar', values)
    seed(query_site, 'Scalar', {})
    result = await query_site.call('summarize', 'Scalar', [], [_entry(f, 'count') for f in values])
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': dict.fromkeys(values, 1), 'groups': []}
    assert list(summaries) == list(values)
    assert all(type(value) is int for value in summaries.values())


@pytest.mark.anyio
async def test_summary_multiple_numeric_fields(query_site: FakeSiteCase) -> None:
    for value in [1, 2, None]:
        seed(
            query_site,
            'Scalar',
            {} if value is None else {'sg_number': value, 'sg_duration': value},
        )
    result = await query_site.call(
        'summarize', 'Scalar', [], [_entry('sg_number', 'sum'), _entry('sg_duration', 'maximum')]
    )
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {'sg_number': 3, 'sg_duration': 2}, 'groups': []}
    assert list(summaries) == ['sg_number', 'sg_duration']
    assert all(type(value) is int for value in summaries.values())


@pytest.mark.anyio
@pytest.mark.parametrize('summary_type', ['earliest', 'latest'])
async def test_summary_datetime_values(query_site: FakeSiteCase, summary_type: str) -> None:
    for value in [datetime(2026, 7, 2, 12, 30, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC)]:
        seed(query_site, 'Scalar', {'sg_date_time': value})
    expected = (
        '2026-07-01 00:00:00 UTC' if summary_type == 'earliest' else '2026-07-02 12:30:00 UTC'
    )
    result = await query_site.call(
        'summarize', 'Scalar', [], [_entry('sg_date_time', summary_type)]
    )
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {'sg_date_time': expected}, 'groups': []}
    assert type(summaries['sg_date_time']) is str


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'values', 'summary_type'),
    [
        ('sg_float', [0.12345, 0.1], 'sum'),
        ('sg_float', [1000000000000000.0, -999999999999999.9], 'sum'),
        ('sg_float', [1000000000000000.0, -999999999999999.9], 'average'),
        ('sg_float', [0.00005, 0.00005], 'average'),
        *[('sg_percent', [0, 25, 100, None], name) for name in ['sum', 'maximum', 'minimum']],
        *[('sg_number', [None, None], name) for name in ['sum', 'maximum', 'average']],
        ('sg_date', [None, None], 'earliest'),
        *[
            (
                'sg_date_time',
                [datetime(2026, 7, 2, 12, 30, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC), None],
                name,
            )
            for name in ['earliest', 'latest']
        ],
    ],
)
async def test_summary_unsettled_scalar_values_guard(
    query_site: FakeSiteCase, field: str, values: list[object], summary_type: str
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {} if value is None else {field: value})
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Scalar', [], [_entry(field, summary_type)])


@pytest.mark.anyio
async def test_sum_rejects_sixteenth_significant_digit(query_site: FakeSiteCase) -> None:
    seed(query_site, 'Scalar', {'sg_float': 100000000000.0001})
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Scalar', [], [_entry('sg_float', 'sum')])


@pytest.mark.anyio
async def test_sum_accepts_fifteen_significant_digits(query_site: FakeSiteCase) -> None:
    seed(query_site, 'Scalar', {'sg_float': 10000000000.0001})
    assert await query_site.call('summarize', 'Scalar', [], [_entry('sg_float', 'sum')]) == {
        'summaries': {'sg_float': 10000000000.0001},
        'groups': [],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('statuses', 'label', 'value', 'percentages'),
    [
        (['ip', *['fin'] * 7], 'g1', 'ip', (13, 12)),
        (['ip', 'ip', 'fin'], 'g2', 'ip', (67, 66)),
        (['ip', *['wtg'] * 5], 'g3', 'ip', (17, 16)),
        (['ip', 'ip', 'fin'], 'g2', 'wtg', (0, 0)),
        (['ip', 'ip', 'ip'], 'g4', 'ip', (100, 100)),
    ],
)
async def test_summary_status_percentages(
    query_site: FakeSiteCase,
    statuses: list[str],
    label: str,
    value: str,
    percentages: tuple[int, int],
) -> None:
    for status in statuses:
        seed(query_site, 'Scalar', {'sg_status': status, 'sg_text': label})
    for name, expected in zip(
        ['status_percentage', 'status_percentage_as_float'], percentages, strict=True
    ):
        result = await query_site.call(
            'summarize',
            'Scalar',
            [['sg_text', 'is', label]],
            [_entry('sg_status', name, value=value)],
        )
        assert isinstance(result, dict)
        summaries = cast('dict[str, object]', result['summaries'])
        assert result == {'summaries': {'sg_status': expected}, 'groups': []}
        assert type(summaries['sg_status']) is int


@pytest.mark.anyio
@pytest.mark.parametrize('summary_type', ['status_percentage', 'status_percentage_as_float'])
@pytest.mark.parametrize(
    ('statuses', 'value'),
    [(['ip', None], 'ip'), (['ip', 'ip', 'fin'], 'IP'), (['ip', 'ip', 'fin'], 'na')],
)
async def test_summary_status_percentage_uncertainty_guard(
    query_site: FakeSiteCase, summary_type: str, statuses: list[str | None], value: str
) -> None:
    for status in statuses:
        seed(query_site, 'Scalar', {} if status is None else {'sg_status': status})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Scalar', [], [_entry('sg_status', summary_type, value=value)]
        )


@pytest.mark.anyio
async def test_summary_uniform_status_list(query_site: FakeSiteCase) -> None:
    for _ in range(3):
        seed(query_site, 'Scalar', {'sg_status': 'ip', 'sg_text': 'g4'})
    assert await query_site.call(
        'summarize', 'Scalar', [['sg_text', 'is', 'g4']], [_entry('sg_status', 'status_list')]
    ) == {'summaries': {'sg_status': 'ip'}, 'groups': []}


@pytest.mark.anyio
@pytest.mark.parametrize('statuses', [['ip', 'ip', 'fin'], ['ip', None]])
async def test_summary_nonuniform_status_list_guard(
    query_site: FakeSiteCase, statuses: list[str | None]
) -> None:
    for status in statuses:
        seed(query_site, 'Scalar', {} if status is None else {'sg_status': status})
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Scalar', [], [_entry('sg_status', 'status_list')])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('values', 'expected'),
    [
        (['Prop', 'Prop', 'Character'], 67),
        (['Prop', 'Character'], 50),
        (['Prop', 'Character', 'Environment', 'Prop', None], 40),
        (['Prop', None], 50),
    ],
)
async def test_summary_list_percentage(
    query_site: FakeSiteCase, values: list[str | None], expected: int
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {} if value is None else {'sg_list': value})
    result = await query_site.call(
        'summarize', 'Scalar', [], [_entry('sg_list', 'percentage', value='Prop')]
    )
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {'sg_list': expected}, 'groups': []}
    assert type(summaries['sg_list']) is int


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('values', 'selector'), [(['Prop', *['Character'] * 7], 'Prop'), (['Prop'], 'prop')]
)
async def test_summary_list_percentage_uncertainty_guard(
    query_site: FakeSiteCase, values: list[str], selector: str
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {'sg_list': value})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Scalar', [], [_entry('sg_list', 'percentage', value=selector)]
        )


@pytest.mark.anyio
async def test_summary_null_checkbox_guard(fake_site: FakeSiteCase) -> None:
    seed(fake_site, 'ScalarTypes', {'checkbox': True})
    seed(fake_site, 'ScalarTypes', {})
    for name in ['checked', 'unchecked']:
        with pytest.raises(MockgunError):
            await fake_site.call('summarize', 'ScalarTypes', [], [_entry('checkbox', name)])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'summary_type', 'expected', 'options'),
    [
        *[
            (field, name, 0, {})
            for field in ['sg_number', 'sg_duration']
            for name in ['sum', 'maximum', 'minimum']
        ],
        *[(field, 'average', 0.0, {}) for field in ['sg_number', 'sg_duration']],
        *[('sg_float', name, 0.0, {}) for name in ['sum', 'maximum', 'minimum', 'average']],
        ('sg_text', 'count', 0, {}),
        ('id', 'record_count', 0, {}),
        ('sg_status', 'status_percentage', 0, {'value': 'ip'}),
        ('sg_status', 'status_percentage_as_float', -1, {'value': 'ip'}),
        ('sg_list', 'percentage', 0, {'value': 'Prop'}),
        *[
            (field, name, None, {})
            for field in ['sg_date', 'sg_date_time']
            for name in ['earliest', 'latest']
        ],
        ('sg_checkbox', 'checked', 0, {}),
        ('sg_checkbox', 'unchecked', 0, {}),
    ],
)
async def test_summary_empty_population(
    query_site: FakeSiteCase,
    field: str,
    summary_type: str,
    expected: object,
    options: dict[str, object],
) -> None:
    seed(query_site, 'Scalar', {'sg_text': 'x'})
    seed(query_site, 'Scalar', {})
    result = await query_site.call(
        'summarize',
        'Scalar',
        [['sg_text', 'is', 'none-such']],
        [_entry(field, summary_type, **options)],
    )
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {field: expected}, 'groups': []}
    assert type(summaries[field]) is type(expected)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'summary_type'), [('sg_percent', 'average'), ('sg_status', 'status_list')]
)
async def test_summary_unsettled_empty_population_guard(
    query_site: FakeSiteCase, field: str, summary_type: str
) -> None:
    seed(query_site, 'Scalar', {'sg_text': 'x'})
    seed(query_site, 'Scalar', {})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Scalar', [['sg_text', 'is', 'none-such']], [_entry(field, summary_type)]
        )


@pytest.mark.anyio
async def test_summary_deep_count_and_retired_link_guard(query_site: FakeSiteCase) -> None:
    sequence = seed(query_site, 'Sequence', {'code': 'seq-a'})
    for code in ['S1', 'S2', 'S3']:
        seed(query_site, 'Shot', {'code': code, 'sg_sequence': sequence})
    seed(query_site, 'Shot', {'code': 'S4'})
    entries = [_entry('sg_sequence.Sequence.id', 'count')]
    result = await query_site.call('summarize', 'Shot', [], entries)
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {'sg_sequence.Sequence.id': 3}, 'groups': []}
    assert type(summaries['sg_sequence.Sequence.id']) is int
    assert await query_site.call('delete', 'Sequence', sequence['id']) is True
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Shot', [], entries)


@pytest.mark.anyio
async def test_summary_deep_mismatched_link_guard(query_site: FakeSiteCase) -> None:
    shot = seed(query_site, 'Shot', {'code': 'S5', 'sg_cut_order': 5})
    asset = seed(query_site, 'Asset', {'code': 'a1'})
    for link in [shot, asset]:
        seed(query_site, 'Scalar', {'sg_link': link})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Scalar', [], [_entry('sg_link.Shot.sg_cut_order', 'sum')]
        )


@pytest.mark.anyio
async def test_summary_created_task_content_taint_guard(query_site: FakeSiteCase) -> None:
    await query_site.call('create', 'Task', {})
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Task', [], [_entry('content', 'count')])


@pytest.mark.anyio
async def test_summary_record_count_does_not_read_tainted_value(query_site: FakeSiteCase) -> None:
    await query_site.call('create', 'Task', {})
    result = await query_site.call('summarize', 'Task', [], [_entry('content', 'record_count')])
    assert isinstance(result, dict)
    summaries = cast('dict[str, object]', result['summaries'])
    assert result == {'summaries': {'content': 1}, 'groups': []}
    assert type(summaries['content']) is int


def _group(
    name: object,
    value: object,
    count: int | dict[str, object],
    children: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    result = {
        'group_name': name,
        'group_value': value,
        'summaries': {'id': count} if isinstance(count, int) else count,
    }
    if children is not None:
        result['groups'] = children
    return result


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('field', 'values', 'groups'),
    [
        (
            'sg_text',
            ['Zed', 'beta', 'alpha', '_x', '1x', 'beta', None],
            [('1x', 1), ('alpha', 1), ('beta', 2), ('_x', 1), ('Zed', 1), (None, 1)],
        ),
        ('sg_text', ['a-c', 'ab'], [('ab', 1), ('a-c', 1)]),
        ('sg_text', ['a2', 'a10'], [('a10', 1), ('a2', 1)]),
        (
            'sg_list',
            ['Character', 'Environment', 'Character', None],
            [('Character', 2), ('Environment', 1), (None, 1)],
        ),
        ('sg_status', ['ip', 'ip', None], [('ip', 2), (None, 1)]),
        ('sg_review', ['hld', 'hld', None], [('hld', 2), (None, 1)]),
        (
            'sg_number',
            [-15, 5, 100, 1000, 12345, 5, None],
            [('-15', 1), ('5', 2), ('100', 1), ('1000', 1), ('12345', 1), (None, 1)],
        ),
        (
            'sg_float',
            [1.5, -0.5, 10.0, 0.1, 1.5, 2.25, None],
            [('-0.5', 1), ('0.1', 1), ('1.5', 2), ('2.25', 1), ('10.0', 1), (None, 1)],
        ),
        ('sg_date', [None, None], [(None, 2)]),
        ('sg_text', [None, None], [(None, 2)]),
        (
            'sg_date_time',
            [
                datetime(2026, 1, 5, 0, 30, tzinfo=UTC),
                datetime(2026, 1, 5, 12, tzinfo=UTC),
                datetime(2025, 12, 31, 23, 30, tzinfo=UTC),
                None,
            ],
            [
                ('2025-12-31 23:30:00', 1),
                ('2026-01-05 00:30:00', 1),
                ('2026-01-05 12:00:00', 1),
                (None, 1),
            ],
        ),
    ],
)
async def test_group_exact_scalar(
    query_site: FakeSiteCase,
    direction: str,
    field: str,
    values: list[object],
    groups: list[tuple[object, int]],
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {} if value is None else {field: value})
    present = [item for item in groups if item[0] is not None]
    if direction == 'desc':
        present.reverse()
    ordered = present + [item for item in groups if item[0] is None]
    await _assert_groups(
        query_site,
        'Scalar',
        [_by(field, 'exact', direction)],
        [_group('' if value is None else value, value, count) for value, count in ordered],
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'kind', 'values'),
    [
        ('sg_text', 'exact', ['beta', 'Beta']),
        ('sg_text', 'exact', ['x', '_x']),
        ('sg_text', 'exact', ['a/c', 'ab']),
        ('sg_text', 'exact', ['caf\u00e9', 'cafe']),
        ('sg_list', 'exact', ['Prop', 'Character']),
        ('sg_status', 'exact', ['ip', 'fin']),
        ('sg_review', 'exact', ['hld', 'na']),
        ('sg_float', 'exact', [1000.5]),
        ('sg_float', 'exact', [0.00005]),
        ('sg_float', 'exact', [-0.0]),
        ('sg_text', 'firstletter', ['-x', 'ab']),
        ('sg_text', 'firstletter', ['\u00e9t\u00e9', 'ab']),
        ('sg_number', 'tens', [-1]),
        ('sg_number', 'hundreds', [-1]),
        ('sg_date', 'clustered_date', ['2026-01-05']),
    ],
)
async def test_group_scalar_uncertainty_guards(
    query_site: FakeSiteCase, field: str, kind: str, values: list[object]
) -> None:
    for value in values:
        seed(query_site, 'Scalar', {} if value is None else {field: value})
    with pytest.raises(MockgunError):
        await query_site.call('summarize', 'Scalar', [], _COUNT, grouping=[_by(field, kind)])


@pytest.mark.anyio
async def test_group_checkbox_defaults(query_site: FakeSiteCase) -> None:
    for value in [False, None, False]:
        seed(query_site, 'Scalar', {} if value is None else {'sg_checkbox': value})
    await _assert_groups(
        query_site, 'Scalar', [_by('sg_checkbox', 'exact')], [_group('[ ]', 'f', 3)]
    )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_checkbox_true_and_false(query_site: FakeSiteCase, direction: str) -> None:
    for value in [True, False, False]:
        seed(query_site, 'Scalar', {'sg_checkbox': value})
    groups = [_group('[ ]', 'f', 2), _group('[x]', 't', 1)]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(query_site, 'Scalar', [_by('sg_checkbox', 'exact', direction)], groups)


@pytest.mark.anyio
async def test_group_checkbox_stored_null_guard(fake_site: FakeSiteCase) -> None:
    seed(fake_site, 'ScalarTypes', {'checkbox': False})
    seed(fake_site, 'ScalarTypes', {})
    with pytest.raises(MockgunError):
        await fake_site.call(
            'summarize', 'ScalarTypes', [], _COUNT, grouping=[_by('checkbox', 'exact')]
        )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_firstletter(query_site: FakeSiteCase, direction: str) -> None:
    for value in ['Zed', 'beta', 'alpha', '_x', '1x', 'Alpha', 'Beta', None]:
        seed(query_site, 'Scalar', {} if value is None else {'sg_text': value})
    groups = [
        _group(value, value, count)
        for value, count in [('_', 1), ('1', 1), ('A', 2), ('B', 2), ('Z', 1)]
    ]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('sg_text', 'firstletter', direction)],
        [*groups, _group('', None, 1)],
    )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_shot_fields(query_site: FakeSiteCase, direction: str) -> None:
    q1, q2, _ = _group_shots(query_site)
    cases: list[tuple[str, str, list[dict[str, object]], dict[str, object] | None]] = [
        (
            'sg_sequence',
            'exact',
            [
                _group('seq-a', _group_link(q1, 'seq-a'), 2),
                _group('seq-b', _group_link(q2, 'seq-b'), 1),
            ],
            _group('', None, 1),
        ),
        (
            'sg_sequence.Sequence.code',
            'exact',
            [_group('seq-a', 'seq-a', 2), _group('seq-b', 'seq-b', 1)],
            _group('', None, 1),
        ),
        ('code', 'exact', [_group(f'S{i}', f'S{i}', 1) for i in range(1, 5)], None),
        ('code', 'firstletter', [_group('S', 'S', 4)], None),
        ('description', 'firstletter', [], _group('', None, 4)),
        (
            'sg_cut_order',
            'exact',
            [_group(str(i), str(i), 1) for i in [1, 9, 12]],
            _group('', None, 1),
        ),
        ('sg_cut_order', 'tens', [_group(0, 0, 2), _group(10, 10, 1)], _group('', None, 1)),
        ('assets', 'exact', [], _group('', [], 4)),
    ]
    for field, kind, groups, null in cases:
        if direction == 'desc':
            groups.reverse()
        await _assert_groups(
            query_site,
            'Shot',
            [_by(field, kind, direction)],
            groups + ([] if null is None else [null]),
        )
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Shot', [], _COUNT, grouping=[_by('sg_status_list', 'exact')]
        )


@pytest.mark.anyio
async def test_group_summaries(query_site: FakeSiteCase) -> None:
    q1, q2, _ = _group_shots(query_site)
    for summary, field, counts in [
        (
            _entry('sg_status_list', 'status_percentage', value='ip'),
            'sg_status_list',
            [50, 100, 0],
        ),
        (_entry('sg_sequence.Sequence.id', 'count'), 'sg_sequence.Sequence.id', [2, 1, 0]),
    ]:
        await _assert_groups(
            query_site,
            'Shot',
            [_by('sg_sequence', 'exact')],
            [
                _group('seq-a', _group_link(q1, 'seq-a'), {field: counts[0]}),
                _group('seq-b', _group_link(q2, 'seq-b'), {field: counts[1]}),
                _group('', None, {field: counts[2]}),
            ],
            summaries=[summary],
        )
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize',
            'Shot',
            [],
            [_entry('sg_cut_order', 'sum')],
            grouping=[_by('sg_cut_order', 'tens')],
        )


@pytest.mark.anyio
async def test_group_entity_name_order(query_site: FakeSiteCase) -> None:
    q1, q2, _ = _group_shots(query_site)
    await query_site.call('update', 'Sequence', q2['id'], {'code': 'Seq-b'})
    groups = [
        _group('seq-a', _group_link(q1, 'seq-a'), 2),
        _group('Seq-b', _group_link(q2, 'Seq-b'), 1),
        _group('', None, 1),
    ]
    await _assert_groups(query_site, 'Shot', [_by('sg_sequence', 'exact')], groups)
    q3 = seed(query_site, 'Sequence', {'code': 'aaa'})
    seed(query_site, 'Shot', {'code': 'S5', 'sg_sequence': q3, 'sg_status_list': 'ip'})
    await _assert_groups(
        query_site,
        'Shot',
        [_by('sg_sequence', 'exact')],
        [_group('aaa', _group_link(q3, 'aaa'), 1), *groups],
    )


@pytest.mark.anyio
@pytest.mark.parametrize('mutation', ['punctuation', 'equal', 'hidden', 'null'])
async def test_group_entity_name_guards(query_site: FakeSiteCase, mutation: str) -> None:
    q1, q2, _ = _group_shots(query_site)
    if mutation == 'hidden':
        await query_site.call('delete', 'Sequence', q1['id'])
    elif mutation == 'punctuation':
        await query_site.call('update', 'Sequence', q1['id'], {'code': 'a/z'})
        await query_site.call('update', 'Sequence', q2['id'], {'code': 'ab'})
    else:
        await query_site.call(
            'update', 'Sequence', q2['id'], {'code': None if mutation == 'null' else 'SEQ-A'}
        )
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Shot', [], _COUNT, grouping=[_by('sg_sequence', 'exact')]
        )


@pytest.mark.anyio
async def test_group_task_derived_name(query_site: FakeSiteCase) -> None:
    t1 = seed(query_site, 'Task', {'content': 'b'})
    t2 = seed(query_site, 'Task', {'content': 'a'})
    seed(query_site, 'Version', {'code': 'v1', 'sg_task': t1})
    seed(query_site, 'Version', {'code': 'v2', 'sg_task': t2})
    await _assert_groups(
        query_site,
        'Version',
        [_by('sg_task', 'exact')],
        [_group('a', _group_link(t2, 'a'), 1), _group('b', _group_link(t1, 'b'), 1)],
    )
    await query_site.call('update', 'Task', t2['id'], {'content': None})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Version', [], _COUNT, grouping=[_by('sg_task', 'exact')]
        )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize('kind', ['exact', 'entitytype'])
async def test_group_mixed_entities(query_site: FakeSiteCase, direction: str, kind: str) -> None:
    x = seed(query_site, 'Shot', {'code': 'S1'})
    a = seed(query_site, 'Asset', {'code': 'zz-asset'})
    q = seed(query_site, 'Sequence', {'code': 'seq-a'})
    for i, link in enumerate([x, a, q, x, None]):
        seed(query_site, 'Version', {'code': f'v{i}', 'entity': link})
    groups = [
        _group(
            name if kind == 'exact' else link['type'],
            _group_link(link, name) if kind == 'exact' else link['type'],
            count,
        )
        for name, link, count in [('zz-asset', a, 1), ('seq-a', q, 1), ('S1', x, 2)]
    ]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site, 'Version', [_by('entity', kind, direction)], [*groups, _group('', None, 1)]
    )
    if kind == 'exact':
        await query_site.call('update', 'Asset', a['id'], {'code': 'a/z'})
        await query_site.call('update', 'Sequence', q['id'], {'code': 'ab'})
        groups = [
            _group(name, _group_link(link, name), count)
            for name, link, count in [('a/z', a, 1), ('ab', q, 1), ('S1', x, 2)]
        ]
        if direction == 'desc':
            groups.reverse()
        await _assert_groups(
            query_site, 'Version', [_by('entity', kind, direction)], [*groups, _group('', None, 1)]
        )
    pl = seed(query_site, 'Playlist', {'code': 'pl'})
    seed(query_site, 'Version', {'code': 'v6', 'entity': pl})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Version', [], _COUNT, grouping=[_by('entity', kind, direction)]
        )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_multi_entity(query_site: FakeSiteCase, direction: str) -> None:
    x1, x2, x3 = [seed(query_site, 'Shot', {'code': name}) for name in ['Zeta', 'alpha', 'Beta']]
    for i, links in enumerate([[x1, x2], [x2], [x2, x3], [], []]):
        seed(query_site, 'Asset', {'code': f'b{i}', 'shots': links})
    groups = [
        _group(name, [_group_link(link, name)], count)
        for name, link, count in [('alpha', x2, 3), ('Beta', x3, 1), ('Zeta', x1, 1)]
    ]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site, 'Asset', [_by('shots', 'exact', direction)], [*groups, _group('', [], 2)]
    )


@pytest.mark.anyio
@pytest.mark.parametrize('mutation', ['equal', 'hidden', 'mixed'])
async def test_group_multi_entity_guards(query_site: FakeSiteCase, mutation: str) -> None:
    x1 = seed(query_site, 'Shot', {'code': 'alpha'})
    x2 = seed(
        query_site,
        'Asset' if mutation == 'mixed' else 'Shot',
        {'code': 'ALPHA' if mutation == 'equal' else 'beta'},
    )
    seed(query_site, 'Scalar', {'sg_links': [x1, x2]})
    entity_type, field = 'Scalar', 'sg_links'
    if mutation == 'hidden':
        seed(query_site, 'Asset', {'code': 'b1', 'shots': [x1, x2]})
        await query_site.call('delete', 'Shot', x1['id'])
        entity_type, field = 'Asset', 'shots'
    with pytest.raises(MockgunError):
        await query_site.call('summarize', entity_type, [], _COUNT, grouping=[_by(field, 'exact')])


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('kind', 'buckets'),
    [
        ('tens', [(0, 2), (10, 1), (100, 1), (12340, 1)]),
        ('hundreds', [(0, 3), (100, 1), (12300, 1)]),
        ('thousands', [(0, 4), (12000, 1)]),
        ('tensofthousands', [(0, 4), (10000, 1)]),
        ('hundredsofthousands', [(0, 5)]),
        ('millions', [(0, 5)]),
    ],
)
async def test_group_numeric_buckets(
    query_site: FakeSiteCase, direction: str, kind: str, buckets: list[tuple[int, int]]
) -> None:
    _group_shots(query_site)
    for value in [105, 12345]:
        seed(query_site, 'Shot', {'code': f'S{value}', 'sg_cut_order': value})
    groups = [_group(value, value, count) for value, count in buckets]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site, 'Shot', [_by('sg_cut_order', kind, direction)], [*groups, _group('', None, 1)]
    )
    if kind in {'tens', 'hundreds'}:
        seed(query_site, 'Shot', {'code': 'negative', 'sg_cut_order': -1})
        with pytest.raises(MockgunError):
            await query_site.call(
                'summarize', 'Shot', [], _COUNT, grouping=[_by('sg_cut_order', kind, direction)]
            )


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_date', 'sg_date_time'])
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize(
    ('kind', 'buckets'),
    [
        (
            'day',
            [
                ('December 31, 2025', '2025-12-31', 1),
                ('January 03, 2026', '2026-01-03', 1),
                ('January 04, 2026', '2026-01-04', 1),
                ('January 05, 2026', '2026-01-05', 2),
                ('March 31, 2026', '2026-03-31', 1),
                ('April 01, 2026', '2026-04-01', 1),
            ],
        ),
        (
            'month',
            [
                ('December 2025', '2025-12-01', 1),
                ('January 2026', '2026-01-01', 4),
                ('March 2026', '2026-03-01', 1),
                ('April 2026', '2026-04-01', 1),
            ],
        ),
        (
            'quarter',
            [
                ('Q4 2025', '2025-10-01', 1),
                ('Q1 2026', '2026-01-01', 5),
                ('Q2 2026', '2026-04-01', 1),
            ],
        ),
        ('year', [('2025', '2025-01-01', 1), ('2026', '2026-01-01', 6)]),
    ],
)
async def test_group_date_intervals(
    query_site: FakeSiteCase,
    field: str,
    direction: str,
    kind: str,
    buckets: list[tuple[str, str, int]],
) -> None:
    for day, hour, minute in [
        ('2025-12-31', 23, 30),
        ('2026-01-03', 23, 30),
        ('2026-01-04', 0, 30),
        ('2026-01-05', 0, 30),
        ('2026-01-05', 12, 0),
        ('2026-03-31', 23, 30),
        ('2026-04-01', 0, 30),
    ]:
        value = (
            day
            if field == 'sg_date'
            else datetime.fromisoformat(day).replace(hour=hour, minute=minute, tzinfo=UTC)
        )
        seed(query_site, 'Scalar', {field: value})
    seed(query_site, 'Scalar', {})
    groups = [_group(name, value, count) for name, value, count in buckets]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site, 'Scalar', [_by(field, kind, direction)], [*groups, _group('', None, 1)]
    )
    if field == 'sg_date' and kind == 'day':
        params = {
            'type': 'Scalar',
            'filters': _FILTERS,
            'summaries': _COUNT,
            'grouping': [_by(field, kind, direction)],
        }
        auth: dict[str, object] = {
            'script_name': 'queries',
            'script_key': 'key',
            'sudo_as_login': 'artist',
        }
        result = raw_call(query_site.client._server, 'summarize', params, auth=auth)['results']
        assert result == {'summaries': {'id': 8}, 'groups': [*groups, _group('', None, 1)]}
        params.pop('grouping')
        assert raw_call(query_site.client._server, 'summarize', params, auth=auth)['results'] == {
            'summaries': {'id': 8},
            'groups': [],
        }


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_week_names(query_site: FakeSiteCase, direction: str) -> None:
    for value in ['2026-07-12', '2026-07-18', '2026-07-19']:
        seed(query_site, 'Scalar', {'sg_date': value})
    seed(query_site, 'Scalar', {})
    groups = [
        _group('July 12, 2026 to July 18, 2026', '2026-07-12', 2),
        _group('July 19, 2026 to July 25, 2026', '2026-07-19', 1),
    ]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site, 'Scalar', [_by('sg_date', 'week', direction)], [*groups, _group('', None, 1)]
    )


@pytest.mark.anyio
async def test_group_week_crosses_year(query_site: FakeSiteCase) -> None:
    seed(query_site, 'Scalar', {'sg_date': '2025-12-31'})
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('sg_date', 'week')],
        [_group('December 28, 2025 to January 03, 2026', '2025-12-28', 1)],
    )


@pytest.mark.anyio
async def test_group_datetime_week_uses_utc_date(query_site: FakeSiteCase) -> None:
    for value in [
        datetime(2026, 7, 18, 23, 30, tzinfo=UTC),
        datetime(2026, 7, 19, 0, 30, tzinfo=UTC),
    ]:
        seed(query_site, 'Scalar', {'sg_date_time': value})
    groups = [
        _group('July 12, 2026 to July 18, 2026', '2026-07-12', 1),
        _group('July 19, 2026 to July 25, 2026', '2026-07-19', 1),
    ]
    await _assert_groups(query_site, 'Scalar', [_by('sg_date_time', 'week')], groups)
    params = {
        'type': 'Scalar',
        'filters': _FILTERS,
        'summaries': _COUNT,
        'grouping': [_by('sg_date_time', 'week')],
    }
    auth: dict[str, object] = {
        'script_name': 'queries',
        'script_key': 'key',
        'sudo_as_login': 'artist',
    }
    with pytest.raises(MockgunError, match=r'Scalar.sg_date_time'):
        raw_call(query_site.client._server, 'summarize', params, auth=auth)


@pytest.mark.anyio
async def test_group_exact_date_names(query_site: FakeSiteCase) -> None:
    for value in ['2026-07-05', '2026-01-15']:
        seed(query_site, 'Scalar', {'sg_date': value})
    seed(query_site, 'Scalar', {})
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('sg_date', 'exact')],
        [
            _group('Jan 15 26', '2026-01-15', 1),
            _group('Jul 05 26', '2026-07-05', 1),
            _group('', None, 1),
        ],
    )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
async def test_group_clustered_date_names(query_site: FakeSiteCase, direction: str) -> None:
    for value in [
        '2026-07-05',
        '2026-07-14',
        '2026-07-15',
        '2026-07-16',
        '2026-07-18',
        '2026-07-19',
    ]:
        seed(query_site, 'Scalar', {'sg_date': value})
    seed(query_site, 'Scalar', {})
    groups = [
        _group(name, name, 1)
        for name in ['last_week', 'yesterday', 'today', 'tomorrow', 'later_this_week', 'next_week']
    ]
    if direction == 'desc':
        groups.reverse()
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('sg_date', 'clustered_date', direction)],
        [*groups, _group('', None, 1)],
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('today', 'value'),
    [
        (datetime(2026, 7, 15, 12, tzinfo=UTC), '2026-07-13'),
        (datetime(2026, 7, 15, 12, tzinfo=UTC), '2026-07-04'),
        (datetime(2026, 7, 15, 12, tzinfo=UTC), '2026-07-26'),
        (datetime(2026, 7, 12, 12, tzinfo=UTC), '2026-07-11'),
        (datetime(2026, 7, 18, 12, tzinfo=UTC), '2026-07-19'),
    ],
)
async def test_group_clustered_date_unsettled(
    query_site: FakeSiteCase, today: datetime, value: str
) -> None:
    query_site.client.site.set_time(today)
    seed(query_site, 'Scalar', {'sg_date': value})
    seed(query_site, 'Scalar', {})
    with pytest.raises(MockgunError, match=r'Scalar.sg_date'):
        await query_site.call(
            'summarize', 'Scalar', [], _COUNT, grouping=[_by('sg_date', 'clustered_date')]
        )


@pytest.mark.anyio
async def test_group_clustered_date_sudo_unsettled(query_site: FakeSiteCase) -> None:
    seed(query_site, 'Scalar', {'sg_date': '2026-07-15'})
    seed(query_site, 'Scalar', {})
    params = {
        'type': 'Scalar',
        'filters': _FILTERS,
        'summaries': _COUNT,
        'grouping': [_by('sg_date', 'clustered_date')],
    }
    auth: dict[str, object] = {
        'script_name': 'queries',
        'script_key': 'key',
        'sudo_as_login': 'artist',
    }
    with pytest.raises(MockgunError, match=r'Scalar.sg_date'):
        raw_call(query_site.client._server, 'summarize', params, auth=auth)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'kind'),
    [('sg_date', 'week'), ('sg_date', 'clustered_date'), ('sg_date_time', 'week')],
)
async def test_group_unsettled_dates_null_only(
    query_site: FakeSiteCase, field: str, kind: str
) -> None:
    seed(query_site, 'Scalar', {})
    await _assert_groups(query_site, 'Scalar', [_by(field, kind)], [_group('', None, 1)])


@pytest.mark.anyio
async def test_deep_clustered_date_rejected_for_null_only_group(query_site: FakeSiteCase) -> None:
    task = seed(query_site, 'Task', {})
    seed(query_site, 'Scalar', {'sg_link': task})
    await _assert_groups(
        query_site, 'Scalar', [_by('sg_date', 'clustered_date')], [_group('', None, 1)]
    )
    with pytest.raises(MockgunError, match=r'clustered_date.*sg_link\.Task\.start_date'):
        await query_site.call(
            'summarize',
            'Scalar',
            [],
            _COUNT,
            grouping=[_by('sg_link.Task.start_date', 'clustered_date')],
        )


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['exact', 'day'])
async def test_group_human_datetime(query_site: FakeSiteCase, kind: str) -> None:
    row = seed(query_site, 'Scalar', {})
    params = {
        'type': 'Scalar',
        'filters': _FILTERS,
        'summaries': _COUNT,
        'grouping': [_by('sg_date_time', kind)],
    }
    auth: dict[str, object] = {
        'script_name': 'queries',
        'script_key': 'key',
        'sudo_as_login': 'artist',
    }
    assert raw_call(query_site.client._server, 'summarize', params, auth=auth)['results'] == {
        'summaries': {'id': 1},
        'groups': [_group('', None, 1)],
    }
    ungrouped = {key: value for key, value in params.items() if key != 'grouping'}
    assert raw_call(query_site.client._server, 'summarize', ungrouped, auth=auth)['results'] == {
        'summaries': {'id': 1},
        'groups': [],
    }
    await query_site.call(
        'update',
        'Scalar',
        row['id'],
        {'sg_date_time': datetime(2026, 1, 5, tzinfo=UTC), 'sg_date': '2026-01-05'},
    )
    with pytest.raises(MockgunError):
        raw_call(query_site.client._server, 'summarize', params, auth=auth)
    params['grouping'] = [_by('sg_date', 'day')]
    assert raw_call(query_site.client._server, 'summarize', params, auth=auth)['results'] == {
        'summaries': {'id': 1},
        'groups': [_group('January 05, 2026', '2026-01-05', 1)],
    }
    assert raw_call(query_site.client._server, 'summarize', ungrouped, auth=auth)['results'] == {
        'summaries': {'id': 1},
        'groups': [],
    }


@pytest.mark.anyio
async def test_group_deep_numeric(query_site: FakeSiteCase) -> None:
    _, _, shots = _group_shots(query_site)
    for i, link in enumerate([shots[0], shots[2], None]):
        seed(query_site, 'Version', {'code': f'v{i}', 'entity': link})
    await _assert_groups(
        query_site,
        'Version',
        [_by('entity.Shot.sg_cut_order', 'tens')],
        [_group(0, 0, 1), _group(10, 10, 1), _group('', None, 1)],
    )
    asset = seed(query_site, 'Asset', {'code': 'a'})
    seed(query_site, 'Version', {'code': 'v4', 'entity': asset})
    with pytest.raises(MockgunError):
        await query_site.call(
            'summarize', 'Version', [], _COUNT, grouping=[_by('entity.Shot.sg_cut_order', 'tens')]
        )


@pytest.mark.anyio
async def test_group_nested(query_site: FakeSiteCase) -> None:
    pa, pb = [seed(query_site, 'Project', {'name': name}) for name in ['Alpha', 'Beta']]
    sa, sb = [seed(query_site, 'Shot', {'code': code}) for code in ['S010', 'S020']]
    for project, link, number in [(pa, sa, 1), (pa, sa, 15), (pa, sb, 3), (pb, sa, 5)]:
        seed(query_site, 'Scalar', {'project': project, 'sg_link': link, 'sg_number': number})
    seed(query_site, 'Scalar', {})
    expected = [
        _group(
            'Alpha',
            _group_link(pa, 'Alpha'),
            3,
            [
                _group('S010', _group_link(sa, 'S010'), 2, [_group(0, 0, 1), _group(10, 10, 1)]),
                _group('S020', _group_link(sb, 'S020'), 1, [_group(0, 0, 1)]),
            ],
        ),
        _group(
            'Beta',
            _group_link(pb, 'Beta'),
            1,
            [_group('S010', _group_link(sa, 'S010'), 1, [_group(0, 0, 1)])],
        ),
        _group('', None, 1, [_group('', None, 1, [_group('', None, 1)])]),
    ]
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('project', 'exact'), _by('sg_link', 'exact'), _by('sg_number', 'tens')],
        expected,
    )


@pytest.mark.anyio
@pytest.mark.parametrize('include_archived', [True, False])
async def test_group_archive_scope(query_site: FakeSiteCase, include_archived: bool) -> None:
    p1 = seed(query_site, 'Project', {'name': 'Active'})
    p2 = seed(query_site, 'Project', {'name': 'Archived', 'archived': True})
    t1 = seed(query_site, 'Shot', {'code': 'T1', 'project': p1})
    t2 = seed(query_site, 'Shot', {'code': 'T2', 'project': p2})
    kwargs = {} if include_archived else {'include_archived_projects': False}
    projects = [_group('Active', _group_link(p1, 'Active'), 1)]
    if include_archived:
        projects.append(_group('Archived', _group_link(p2, 'Archived'), 1))
    await _assert_groups(query_site, 'Shot', [_by('project', 'exact')], projects, **kwargs)
    seed(query_site, 'Version', {'code': 'v1', 'project': p1, 'entity': t2})
    seed(query_site, 'Scalar', {'sg_link': p2})
    seed(query_site, 'Asset', {'code': 'b1', 'project': p1, 'shots': [t1, t2]})
    b2 = seed(query_site, 'Asset', {'code': 'b2', 'project': p1, 'shots': [t1]})
    await _assert_groups(
        query_site,
        'Asset',
        [_by('shots', 'exact')],
        [_group('T1', [_group_link(t1, 'T1')], 1)],
        filters=[['id', 'is', b2['id']]],
        **kwargs,
    )
    cases = [
        ('Version', 'entity', [_group('T2', _group_link(t2, 'T2'), 1)]),
        ('Scalar', 'sg_link', [_group('Archived', _group_link(p2, 'Archived'), 1)]),
        (
            'Asset',
            'shots',
            [_group('T1', [_group_link(t1, 'T1')], 2), _group('T2', [_group_link(t2, 'T2')], 1)],
        ),
    ]
    for entity_type, field, groups in cases:
        if include_archived:
            await _assert_groups(query_site, entity_type, [_by(field, 'exact')], groups, **kwargs)
        else:
            with pytest.raises(MockgunError):
                await query_site.call(
                    'summarize', entity_type, [], _COUNT, grouping=[_by(field, 'exact')], **kwargs
                )


@pytest.mark.anyio
@pytest.mark.parametrize('include_archived', [True, False])
async def test_group_without_archive_declaration(
    fake_site: FakeSiteCase, include_archived: bool
) -> None:
    groups: list[dict[str, object]] = []
    for name in ['A', 'B']:
        project = seed(fake_site, 'Project', {'name': name})
        seed(fake_site, 'Shot', {'code': name, 'project': project})
        groups.append(_group(name, _group_link(project, name), 1))
    await _assert_groups(
        fake_site,
        'Shot',
        [_by('project', 'exact')],
        groups,
        include_archived_projects=include_archived,
    )


@pytest.mark.anyio
async def test_group_collation_checks_siblings_only(query_site: FakeSiteCase) -> None:
    for number, text in [(1, 'a/c'), (2, 'ab')]:
        seed(query_site, 'Scalar', {'sg_number': number, 'sg_text': text})
    await _assert_groups(
        query_site,
        'Scalar',
        [_by('sg_number', 'exact'), _by('sg_text', 'exact')],
        [
            _group('1', '1', 1, [_group('a/c', 'a/c', 1)]),
            _group('2', '2', 1, [_group('ab', 'ab', 1)]),
        ],
    )


@pytest.mark.anyio
@pytest.mark.parametrize('direction', ['asc', 'desc'])
@pytest.mark.parametrize('names', [{'Shot': 'Shot Entity'}, {'Asset': 'Zebra', 'Shot': 'Alpha'}])
async def test_group_schema_display_names(
    query_site: FakeSiteCase, direction: str, names: dict[str, str]
) -> None:
    # Copy the query schema's entity metadata before constructing this independent site.
    entities = copy.deepcopy(QUERY_SCHEMA_ENTITY)
    for entity_type, name in names.items():
        entities[entity_type]['name']['value'] = name
    with tempfile.TemporaryDirectory() as directory:
        schema_path = Path(directory) / 'schema.pickle'
        entity_path = Path(directory) / 'entities.pickle'
        schema_path.write_bytes(pickle.dumps(QUERY_SCHEMA, protocol=2))
        entity_path.write_bytes(pickle.dumps(entities, protocol=2))
        client_class = type(query_site.client)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(SiteShotgun, '_schema_path', str(schema_path))
            patch.setattr(SiteShotgun, '_schema_entity_path', str(entity_path))
            client = client_class('http://unit-test.example', script_name='queries', api_key='key')
        case = FakeSiteCase(query_site.kind, client)
        try:
            x = seed(case, 'Shot', {'code': 'S1'})
            a = seed(case, 'Asset', {'code': 'zz-asset'})
            q = seed(case, 'Sequence', {'code': 'seq-a'})
            versions = [
                seed(case, 'Version', {'code': f'v{i}', 'entity': link})
                for i, link in enumerate([x, a, q, x, None])
            ]
            grouping = [_by('entity', 'exact', direction)]
            if 'Asset' in names:
                with pytest.raises(MockgunError):
                    await case.call('summarize', 'Version', [], _COUNT, grouping=grouping)
            else:
                groups = [
                    _group(name, _group_link(link, name), count)
                    for name, link, count in [('zz-asset', a, 1), ('seq-a', q, 1), ('S1', x, 2)]
                ]
                if direction == 'desc':
                    groups.reverse()
                await _assert_groups(case, 'Version', grouping, [*groups, _group('', None, 1)])
            filters = [['id', 'in', [versions[0]['id'], versions[4]['id']]]]
            await _assert_groups(
                case,
                'Version',
                grouping,
                [_group('S1', _group_link(x, 'S1'), 1), _group('', None, 1)],
                filters=filters,
            )
            with pytest.raises(MockgunError):
                await case.call(
                    'summarize', 'Version', filters, _COUNT, grouping=[_by('entity', 'entitytype')]
                )
            await _assert_groups(
                case,
                'Version',
                [_by('entity', 'entitytype')],
                [_group('', None, 1)],
                filters=[['id', 'is', versions[4]['id']]],
            )
        finally:
            await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('archived', [False, True])
async def test_group_archive_check_resolves_retired_targets(
    query_site: FakeSiteCase, archived: bool
) -> None:
    project = seed(query_site, 'Project', {'name': 'P', 'archived': archived})
    shot = seed(query_site, 'Shot', {'code': 'S', 'project': project})
    await query_site.call('delete', 'Shot', shot['id'])
    seed(query_site, 'Scalar', {'sg_link': shot})
    grouping = [_by('sg_link', 'exact')]
    if archived:
        with pytest.raises(MockgunError):
            await query_site.call(
                'summarize',
                'Scalar',
                [],
                _COUNT,
                grouping=grouping,
                include_archived_projects=False,
            )
    else:
        await _assert_groups(
            query_site,
            'Scalar',
            grouping,
            [_group('S', _group_link(shot, 'S'), 1)],
            include_archived_projects=False,
        )


@pytest.mark.anyio
async def test_summary_combines_errors_before_faults_in_validation_order(
    query_site: FakeSiteCase,
) -> None:
    with pytest.raises(MockgunError) as error:
        _raw_summary(
            query_site,
            [_entry('missing_summary', 'count'), _entry('sg_number.bad', 'count')],
            [_by('missing_group', 'exact'), _by('sg_date.bad', 'exact')],
        )
    assert str(error.value) == (
        'mockgun cannot summarize: '
        'summarize field sg_number.bad has an unsupported path; '
        'summarize field sg_date.bad has an unsupported path; '
        "API summarize() Scalar.missing_summary doesn't exist; "
        "API summarize() Scalar.missing_group doesn't exist"
    )
