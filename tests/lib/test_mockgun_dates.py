from datetime import UTC, datetime
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id

from .conftest import FakeSiteCase, raw_call, raw_read


async def _rows(case: FakeSiteCase, field: str, values: list[str | None]) -> list[int]:
    ids: list[int] = []
    for value in values:
        data: dict[str, object] = {}
        if value is not None:
            data[field] = (
                datetime.fromisoformat(value).replace(tzinfo=UTC)
                if field == 'sg_date_time'
                else value
            )
        row = cast('dict[str, object]', await case.call('create', 'Scalar', data))
        ids.append(entity_id(row))
    return ids


@pytest.mark.anyio
async def test_unparseable_stored_date_keeps_absolute_comparison(query_site: FakeSiteCase) -> None:
    ids = await _rows(query_site, 'sg_date', ['not-a-date'])
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [['sg_date', 'is', 'not-a-date']], ['sg_date']),
    )
    assert rows == [{'type': 'Scalar', 'id': ids[0], 'sg_date': 'not-a-date'}]


@pytest.mark.anyio
@pytest.mark.parametrize('operands', [('in_last', 1, 'DAY'), ('in_calendar_day', 0)])
async def test_unparseable_stored_date_relative_filter_is_unsettled(
    query_site: FakeSiteCase, operands: tuple[str, int, str] | tuple[str, int]
) -> None:
    await _rows(query_site, 'sg_date', ['not-a-date'])
    with pytest.raises(MockgunError, match=rf'Scalar\.sg_date date {operands[0]}') as error:
        await query_site.call('find', 'Scalar', [['sg_date', *operands]])
    assert repr(list(operands[1:])) in str(error.value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('relation', 'amount', 'unit', 'expected'),
    [
        ('in_last', 1, 'DAY', [1, 2]),
        ('not_in_last', 1, 'DAY', [0, 3, 4]),
        ('in_next', 1, 'DAY', [3]),
        ('not_in_next', 1, 'DAY', [0, 1, 2, 4]),
        ('in_last', 2, 'HOUR', [2]),
        ('in_last', 1, 'WEEK', [0, 1, 2]),
        ('in_last', 0, 'DAY', []),
    ],
)
async def test_datetime_rolling_windows(
    query_site: FakeSiteCase, relation: str, amount: int, unit: str, expected: list[int]
) -> None:
    ids = await _rows(
        query_site,
        'sg_date_time',
        ['2026-07-14T11:00', '2026-07-14T13:00', '2026-07-15T11:00', '2026-07-15T13:00', None],
    )
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [['sg_date_time', relation, amount, unit]]),
    )
    assert [row['id'] for row in rows] == [ids[index] for index in expected]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('value', 'relation', 'amount'),
    [
        ('2026-07-14T12:00', 'in_last', 1),
        ('2026-07-15T12:00', 'in_last', 1),
        ('2026-07-15T12:00', 'in_next', 1),
        ('2026-07-15T12:00', 'in_last', 0),
    ],
)
async def test_datetime_rolling_endpoint_is_unsettled(
    query_site: FakeSiteCase, value: str, relation: str, amount: int
) -> None:
    await _rows(query_site, 'sg_date_time', [value])
    with pytest.raises(MockgunError, match=r'Scalar\.sg_date_time'):
        await query_site.call('find', 'Scalar', [['sg_date_time', relation, amount, 'DAY']])


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    [
        (
            'sg_date_time',
            'in_last',
            2,
            'MONTH',
            ['2026-05-15T11:00', '2026-05-15T13:00', '2026-05-16T13:00'],
            [1, 2],
        ),
        ('sg_date_time', 'in_next', 1, 'MONTH', ['2026-08-15T11:00', '2026-08-15T13:00'], [0]),
        ('sg_date_time', 'in_last', 3, 'YEAR', ['2023-07-15T11:00', '2023-07-15T13:00'], [1]),
        ('sg_date', 'in_last', 2, 'MONTH', ['2026-05-14', '2026-05-15'], [1]),
        ('sg_date', 'in_next', 1, 'MONTH', ['2026-08-15', '2026-08-16'], [0]),
        ('sg_date', 'in_last', 3, 'YEAR', ['2023-07-14', '2023-07-15'], [1]),
    ],
)
async def test_rolling_calendar_shifts(
    query_site: FakeSiteCase,
    scenario: tuple[str, str, int, str, list[str | None], list[int]],
) -> None:
    field, relation, amount, unit, values, expected = scenario
    ids = await _rows(query_site, field, values)
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [[field, relation, amount, unit]]),
    )
    assert [row['id'] for row in rows] == [ids[index] for index in expected]


@pytest.mark.anyio
@pytest.mark.parametrize('field', ['sg_date_time', 'sg_date'])
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    'shift',
    [('2026-03-31T12:00', 'in_last', 'MONTH'), ('2028-02-29T12:00', 'in_next', 'YEAR')],
)
async def test_missing_calendar_shift_day_is_unsettled(
    query_site: FakeSiteCase, field: str, populated: bool, shift: tuple[str, str, str]
) -> None:
    now, relation, unit = shift
    if populated:
        await _rows(query_site, field, [None])
    query_site.client.site.set_time(datetime.fromisoformat(now).replace(tzinfo=UTC))
    with pytest.raises(MockgunError, match=rf'Scalar\.{field}'):
        await query_site.call('find', 'Scalar', [[field, relation, 1, unit]])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('relation', 'amount', 'unit', 'expected'),
    [
        ('in_last', 1, 'DAY', [3, 4]),
        ('in_next', 1, 'DAY', [4, 5]),
        ('not_in_last', 1, 'DAY', [0, 1, 2, 5, 6, 7]),
        ('in_last', 1, 'WEEK', [1, 2, 3, 4]),
        ('in_last', 23, 'HOUR', [4]),
        ('in_last', 24, 'HOUR', [3, 4]),
        ('in_last', 49, 'HOUR', [2, 3, 4]),
        ('in_next', 48, 'HOUR', [4, 5, 6]),
    ],
)
async def test_date_inclusive_windows(
    query_site: FakeSiteCase, relation: str, amount: int, unit: str, expected: list[int]
) -> None:
    ids = await _rows(
        query_site,
        'sg_date',
        [
            '2026-07-07',
            '2026-07-08',
            '2026-07-13',
            '2026-07-14',
            '2026-07-15',
            '2026-07-16',
            '2026-07-17',
            None,
        ],
    )
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [['sg_date', relation, amount, unit]]),
    )
    assert [row['id'] for row in rows] == [ids[index] for index in expected]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    [
        (
            'sg_date_time',
            'in_calendar_day',
            0,
            ['2026-07-14T23:00', '2026-07-15T00:00', '2026-07-15T23:59:59', '2026-07-16T00:00'],
            [1, 2],
        ),
        (
            'sg_date_time',
            'in_calendar_day',
            -1,
            ['2026-07-14T23:00', '2026-07-15T00:00', '2026-07-15T23:59:59', '2026-07-16T00:00'],
            [0],
        ),
        (
            'sg_date_time',
            'in_calendar_day',
            2,
            ['2026-07-16T00:00', '2026-07-17T23:00', '2026-07-18T00:00'],
            [0, 1],
        ),
        (
            'sg_date_time',
            'in_calendar_week',
            0,
            [
                '2026-07-11T23:00',
                '2026-07-12T00:00',
                '2026-07-13T00:00',
                '2026-07-18T23:59:59',
                '2026-07-19T00:00',
            ],
            [1, 2, 3],
        ),
        (
            'sg_date_time',
            'in_calendar_week',
            -1,
            [
                '2026-07-11T23:00',
                '2026-07-12T00:00',
                '2026-07-13T00:00',
                '2026-07-18T23:59:59',
                '2026-07-19T00:00',
            ],
            [0],
        ),
        (
            'sg_date_time',
            'in_calendar_month',
            -2,
            ['2026-04-30T23:00', '2026-05-01T00:00', '2026-06-30T23:00', '2026-07-01T00:00'],
            [1, 2],
        ),
        (
            'sg_date_time',
            'in_calendar_year',
            2,
            ['2026-12-31T23:00', '2027-01-01T00:00', '2028-12-31T23:00', '2029-01-01T00:00'],
            [1, 2],
        ),
        (
            'sg_date',
            'in_calendar_week',
            0,
            ['2026-07-11', '2026-07-12', '2026-07-18', '2026-07-19'],
            [1, 2],
        ),
    ],
)
async def test_calendar_period_spans(
    query_site: FakeSiteCase,
    scenario: tuple[str, str, int, list[str | None], list[int]],
) -> None:
    field, relation, offset, values, expected = scenario
    ids = await _rows(query_site, field, [*values, None])
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Scalar', [[field, relation, offset]]),
    )
    assert [row['id'] for row in rows] == [ids[index] for index in expected]


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    ('relation', 'values'),
    [
        ('in_last', [-1, 'DAY']),
        ('in_last', [1.5, 'DAY']),
        ('in_last', ['1', 'DAY']),
        ('in_last', [1, 'day']),
        ('in_last', [1, 'BOGUS']),
        ('in_last', [1]),
        ('in_calendar_day', []),
        ('in_calendar_day', [0.5]),
    ],
)
async def test_relative_operand_faults_precede_rows_and_session(
    query_site: FakeSiteCase, populated: bool, relation: str, values: list[object]
) -> None:
    if populated:
        await _rows(query_site, 'sg_date_time', ['2026-07-15T11:00'])
    before = await query_site.call('find', 'Scalar', [])
    with pytest.raises(Fault, match=rf'Scalar\.sg_date_time {relation}'):
        await query_site.call('find', 'Scalar', [['sg_date_time', relation, *values]])
    result = raw_read(
        query_site.client._server, 'Scalar', [('sg_date_time', relation, values)], auth={}
    )
    assert result['error_code'] == 103
    assert f'Scalar.sg_date_time {relation}' in str(result['message'])
    assert await query_site.call('find', 'Scalar', []) == before


@pytest.mark.anyio
@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize(
    ('field', 'relation', 'values'),
    [
        ('sg_date_time', 'in_next', [1.5, 'DAY']),
        ('sg_date', 'in_last', [0, 'DAY']),
        ('sg_date_time', 'in_calendar_week', [0.5]),
    ],
)
async def test_unsettled_relative_operands(
    query_site: FakeSiteCase, populated: bool, field: str, relation: str, values: list[object]
) -> None:
    if populated:
        await _rows(query_site, field, [None])
    with pytest.raises(MockgunError, match=rf'Scalar\.{field}'):
        await query_site.call('find', 'Scalar', [[field, relation, *values]])


@pytest.mark.anyio
@pytest.mark.parametrize(
    'auth',
    [
        {'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
        {'session_token': 'unknown-token'},
        {},
    ],
)
async def test_timezone_unknown_sessions(
    query_site: FakeSiteCase, auth: dict[str, object]
) -> None:
    await _rows(query_site, 'sg_date_time', ['2026-07-14T11:00', '2026-07-15T11:00', None])
    server = query_site.client._server
    conditions: list[tuple[str, str, list[object]]] = [
        ('sg_date_time', 'in_calendar_day', [0]),
        ('sg_date', 'in_last', [1, 'DAY']),
        ('sg_date_time', 'in_last', [2, 'MONTH']),
    ]
    for field, relation, values in conditions:
        with pytest.raises(MockgunError, match=rf'Scalar\.{field}'):
            raw_read(server, 'Scalar', [(field, relation, values)], auth=auth)
    condition: list[tuple[str, str, list[object]]] = [('sg_date_time', 'in_last', [1, 'DAY'])]
    assert raw_read(server, 'Scalar', condition, auth=auth) == raw_read(
        server, 'Scalar', condition
    )


@pytest.mark.anyio
async def test_script_session_token_retains_utc_calendar(query_site: FakeSiteCase) -> None:
    await _rows(query_site, 'sg_date_time', ['2026-07-14T11:00', '2026-07-15T11:00', None])
    server = query_site.client._server
    token = raw_call(server, 'get_session_token', {})['session_id']
    condition: list[tuple[str, str, list[object]]] = [('sg_date_time', 'in_calendar_day', [0])]
    assert raw_read(server, 'Scalar', condition, auth={'session_token': token}) == raw_read(
        server, 'Scalar', condition
    )


@pytest.mark.anyio
async def test_deep_datetime_relative_filter(query_site: FakeSiteCase) -> None:
    ids: list[object] = []
    for index, value in enumerate(['2026-07-15T11:00', '2026-07-12T12:00']):
        shot = await query_site.call(
            'create',
            'Shot',
            {'code': f's{index}', 'created_at': datetime.fromisoformat(value).replace(tzinfo=UTC)},
        )
        version = cast(
            'dict[str, object]',
            await query_site.call('create', 'Version', {'code': f'v{index}', 'entity': shot}),
        )
        ids.append(version['id'])
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call(
            'find', 'Version', [['entity.Shot.created_at', 'in_last', 1, 'DAY']]
        ),
    )
    assert [row['id'] for row in rows] == ids[:1]
