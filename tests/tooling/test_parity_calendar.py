from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from tests import _parity

if TYPE_CHECKING:
    from tests._parity import MockgunCase, Scenario

pytest_plugins = ('tests.lib._parity_fixture',)
pytestmark = pytest.mark.usefixtures('block_live_connections')


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    [
        _parity.plain_upload_attachment_fields,
        _parity.extended_last_accessed_per_actor,
        _parity.week_exact_and_checkbox_group_names,
        _parity.extended_work_schedule_project_exception,
    ],
    ids=lambda scenario: scenario.__name__,
)
async def test_instant_and_explicit_date_scenarios_run_at_month_end_midnight(
    parity_case: MockgunCase, scenario: Scenario
) -> None:
    parity_case.client.site.set_time(datetime(2026, 7, 31, 0, 1, tzinfo=UTC))
    await scenario(parity_case)


@pytest.mark.anyio
async def test_clustered_groups_run_at_month_end(parity_case: MockgunCase) -> None:
    parity_case.client.site.set_time(datetime(2026, 7, 31, 12, tzinfo=UTC))
    await _parity.clustered_date_groups(parity_case)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('scenario', 'instant', 'reason'),
    [
        (scenario, instant, reason)
        for scenario in (_parity.relative_dates_on_date, _parity.relative_dates_on_date_time)
        for instant, reason in (
            (datetime(2026, 7, 31, 12, tzinfo=UTC), 'after day 28'),
            (datetime(2026, 7, 15, 0, 1, tzinfo=UTC), 'day boundary'),
        )
    ]
    + [
        (
            _parity.clustered_date_groups,
            datetime(2026, 7, 31, 23, 59, tzinfo=UTC),
            'day boundary',
        )
    ],
    ids=lambda value: value.__name__ if callable(value) else str(value),
)
async def test_relative_calendar_scenarios_keep_needed_prerequisites(
    parity_case: MockgunCase, scenario: Scenario, instant: datetime, reason: str
) -> None:
    parity_case.client.site.set_time(instant)
    with pytest.raises(pytest.skip.Exception, match=reason):
        await scenario(parity_case)
