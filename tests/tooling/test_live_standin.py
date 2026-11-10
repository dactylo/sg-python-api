from __future__ import annotations

import inspect
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from tests import _parity
from tests._live_safety import LiveBudget
from tests.live import conftest as live_fixtures
from tests.live._session import SessionPlugin
from tests.tooling import _live_standin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from pathlib import Path

    from tests.live._verification import LiveCase

pytestmark = pytest.mark.usefixtures('block_live_connections')


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_each_case_is_isolated_and_cleaned_on_its_own_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    plugin = SessionPlugin('L1', tmp_path, LiveBudget(session='L1'), _live_standin)
    initial = await plugin.initialize()
    monkeypatch.setitem(live_fixtures._LIVE_SESSION, 'session', plugin)
    request = cast('pytest.FixtureRequest', SimpleNamespace(param=kind))
    factory = cast(
        'Callable[[pytest.FixtureRequest], AsyncGenerator[LiveCase]]',
        inspect.unwrap(live_fixtures.api_case),
    )
    fixture = factory(request)
    case = await anext(fixture)
    try:
        record = await case.create(
            'Shot', {'code': f'{case.namespace}-owned', 'project': case.project}
        )
        assert initial.client is not case.client
        assert (
            cast('_live_standin.Client', initial.client).client
            is not cast('_live_standin.Client', case.client).client
        )
        assert await initial.call('find_one', 'Shot', [['id', 'is', record['id']]]) is None
    finally:
        await fixture.aclose()
    assert await case.call('find_one', 'Shot', [['id', 'is', record['id']]]) is None
    await case.call('close')
    await plugin.finish(initial)
    assert plugin.prefix_after == 0


@pytest.mark.anyio
async def test_session_cases_seed_independent_sites_and_clocks(
    tmp_path: Path,
) -> None:
    first = SessionPlugin('L1', tmp_path, LiveBudget(session='L1'), _live_standin)
    initial = await first.initialize()
    assert initial.target is not None
    initial_client = cast('_live_standin.Client', initial.client).client
    earlier = datetime(2001, 1, 1, tzinfo=UTC)
    initial_client.site.set_time(earlier)
    later = await first.make_case('async')
    other = SessionPlugin('L1', tmp_path, LiveBudget(session='L1'), _live_standin)
    independent = await other.initialize()
    try:
        later_client = cast('_live_standin.Client', later.client).client
        assert later_client.site is not initial_client.site
        assert later_client.site.now() != earlier
        assert await later.call('find', 'Project', [], ['name']) == await initial.call(
            'find', 'Project', [], ['name']
        )
        assert len(cast('list[object]', await later.call('find', 'Project', []))) == 1
        assert len(cast('list[object]', await later.call('find', 'HumanUser', []))) == 1
        record = await later.create(
            'Shot', {'code': f'{later.namespace}-isolated', 'project': later.project}
        )
        assert await initial.call('find_one', 'Shot', [['id', 'is', record['id']]]) is None
        assert await independent.call('find_one', 'Shot', [['id', 'is', record['id']]]) is None
    finally:
        await later.cleanup()
        await later.call('close')
        await first.finish(initial)
        await other.finish(independent)


@pytest.mark.anyio
async def test_schedule_scenario_uses_the_week_checked_at_session_start(tmp_path: Path) -> None:
    plugin = SessionPlugin('L4', tmp_path, LiveBudget(session='L4'), _live_standin)
    initial = await plugin.initialize()
    assert initial.target is not None
    selected_date = date(2026, 7, 15)
    initial.run_date = initial.target.run_date = selected_date
    case = await plugin.make_case('sync')
    assert case.target is not None
    assert case.run_date == case.target.run_date == selected_date
    cast('_live_standin.Client', case.client).client.site.set_time(
        datetime(2026, 7, 31, 0, 1, tzinfo=UTC)
    )
    try:
        await _parity.extended_work_schedule_project_exception(case)
        start = selected_date + timedelta(days=45)
        start -= timedelta(days=start.weekday())
        rows = cast(
            'list[dict[str, object]]',
            await case.call('find', 'WorkDayRule', [], ['start_date'], retired_only=True),
        )
        assert rows
        assert {row['start_date'] for row in rows} == {start.isoformat()}
    finally:
        await case.cleanup()
        await case.call('close')
        await plugin.finish(initial)
