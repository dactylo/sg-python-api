from __future__ import annotations

import inspect
from datetime import timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Literal, cast

import pytest

from tests._live_safety import LiveBudget, LiveStop
from tests.live._session import SessionPlugin, session_budget
from tests.live._verification import LiveCase, baseline_equal
from tests.tooling._cleanup_site import CleanupSite, case
from tests.tooling._live_standin import Client, make_case

pytestmark = pytest.mark.usefixtures('block_live_connections')

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def _baseline_site(site: CleanupSite) -> None:
    site.follows = [
        {'type': 'Shot', 'id': number, 'name': f'foreign-{number}'} for number in (98, 99)
    ]
    site.subscriptions = [
        {'human_user_id': number, 'subscription': ['b', 'a']} for number in (2, 3)
    ]
    for number in (98, 99):
        site.rows[('WorkDayRule', number)] = {
            'type': 'WorkDayRule',
            'id': number,
            'project': {'type': 'Project', 'id': 1},
            'description': f'foreign-{number}',
            'working': False,
            'start_date': '2020-01-01',
            'end_date': '2020-01-01',
        }


def _reorder(site: CleanupSite) -> None:
    site.follows.reverse()
    site.subscriptions = [
        {**row, 'subscription': ['a', 'b']} for row in reversed(site.subscriptions)
    ]
    site.rows = dict(reversed(site.rows.items()))


@pytest.mark.parametrize('label', ['F', 'W', 'S'])
def test_canonical_comparison_preserves_container_types(label: str) -> None:
    left: list[dict[str, object]] = [{'type': 'Shot', 'id': 1, 'value': []}]
    right: list[dict[str, object]] = [{'type': 'Shot', 'id': 1, 'value': {}}]
    assert not baseline_equal(label, left, right)


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_reordered_baselines_pass_setup_cleanup_and_finish(
    tmp_path: Path, kind: Literal['sync', 'async']
) -> None:
    sites: list[CleanupSite] = []
    target = ModuleType('canonical_target')

    async def factory(_kind: str, _namespace: str, _budget: LiveBudget) -> LiveCase:
        live_case, site = case(kind)
        _baseline_site(site)
        if sites:
            _reorder(site)
        sites.append(site)
        return live_case

    target.__dict__['make_case'] = factory
    plugin = SessionPlugin('L4', tmp_path, LiveBudget(session='L4'), target)
    initial = await plugin.initialize()
    live_case = await plugin.make_case(kind)
    live_case.flags.update({'follow', 'schedule', 'subscription'})
    await live_case.cleanup()
    assert not any(
        action.startswith(('unfollow', 'delete', 'update')) for action in sites[1].actions
    )
    _reorder(sites[0])
    await plugin.finish(initial)
    assert set(plugin.baseline_labels.values()) == {'equal'}
    await live_case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('label', ['F', 'W', 'S'])
async def test_baseline_field_difference_fails_without_inverse(
    kind: Literal['sync', 'async'], label: str
) -> None:
    live_case, site = case(kind)
    _baseline_site(site)
    assert live_case.target is not None
    live_case.target.baselines = await live_case.read_baselines()
    # Changed fields on an already-followed identity must never trigger unfollow,
    # even if that identity was independently proven owned.
    if label == 'F':
        site.follows = [{**row, 'name': 'changed'} for row in site.follows]
        live_case.discovered.add(('Shot', 98))
    elif label == 'W':
        site.rows[('WorkDayRule', 98)] = {**site.rows[('WorkDayRule', 98)], 'working': True}
    else:
        site.subscriptions = [{**row, 'subscription': ['different']} for row in site.subscriptions]
    live_case.flags.update({'follow', 'schedule', 'subscription'})
    with pytest.raises(RuntimeError, match='live cleanup failed'):
        await live_case.cleanup()
    assert not any(action.startswith(('unfollow', 'delete', 'update')) for action in site.actions)


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('entity_type', ['Note', 'Reply'])
async def test_lost_sudo_response_unfollows_discovered_owned_before_retirement(
    kind: Literal['sync', 'async'],
    entity_type: str,
) -> None:
    live_case, site = case(kind)
    note = await live_case.create(
        'Note', {'project': live_case.project, 'subject': 'owned-script'}
    )
    data = (
        {'subject': 'owned-sudo', 'project': live_case.project}
        if entity_type == 'Note'
        else {'entity': note, 'content': 'owned-reply'}
    )
    site.lose = entity_type
    with pytest.raises(OSError, match='lost'):
        await live_case.sudo_call('create', entity_type, data)
    await live_case.cleanup()
    assert not site.follows
    assert set(site.rows) == {('Project', 1)}
    assert site.actions.index('unfollow') < site.actions.index('delete:Note')


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_lost_project_and_child_are_discovered_and_retired_last(
    kind: Literal['sync', 'async'],
) -> None:
    live_case, site = case(kind)
    site.lose = 'Project'
    with pytest.raises(OSError, match='response lost'):
        await live_case.call('create', 'Project', {'name': 'owned-project', 'archived': True})
    project = {'type': 'Project', 'id': 10}
    site.lose = 'Shot'
    with pytest.raises(OSError, match='response lost'):
        await live_case.call('create', 'Shot', {'project': project, 'code': 'owned-child'})
    await live_case.cleanup()
    assert set(site.rows) == {('Project', 1)}
    assert (
        site.actions.index('update:Project')
        < site.actions.index('delete:Shot')
        < site.actions.index('delete:Project')
    )


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('foreign', ['follow', 'rule'])
async def test_foreign_difference_untouched_and_owned_state_removed(
    kind: Literal['sync', 'async'],
    foreign: str,
) -> None:
    live_case, site = case(kind)
    await live_case.create('Shot', {'project': live_case.project, 'code': 'owned-shot'})
    live_case.flags.update({'follow', 'schedule'})
    live_case.scanned_types.add('WorkDayRule')
    if foreign == 'follow':
        site.follows.append({'type': 'Shot', 'id': 999})
    else:
        site.rows[('WorkDayRule', 999)] = {
            'type': 'WorkDayRule',
            'id': 999,
            'project': live_case.project,
            'description': 'foreign',
        }
    with pytest.raises(RuntimeError, match='live cleanup failed'):
        await live_case.cleanup()
    assert ('Shot', 10) not in site.rows
    assert site.follows == ([{'type': 'Shot', 'id': 999}] if foreign == 'follow' else [])
    assert (('WorkDayRule', 999) in site.rows) == (foreign == 'rule')


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_stop_uses_allowance_and_captures_live_stop(kind: Literal['sync', 'async']) -> None:
    live_case, site = case(kind)
    await live_case.create('Shot', {'project': live_case.project, 'code': 'owned-shot'})
    assert live_case.target is not None
    live_case.target.budget.stopped = 'refused: follow'
    await live_case.cleanup()
    assert set(site.rows) == {('Project', 1)}
    with pytest.raises(LiveStop):
        live_case.target.budget.start_case()


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('operation', ['follow', 'schedule'])
async def test_stopped_state_restores_under_charged_allowance(
    kind: Literal['sync', 'async'],
    operation: str,
) -> None:

    case = await make_case(kind, 'sg-python-api-live-123456789abc', session_budget('L4'))
    assert case.target is not None
    working = False
    if operation == 'schedule':
        client = cast('Client', case.client).client
        write = cast('Callable[..., object]', client.work_schedule_update)
        result = write(
            (case.target.run_date + timedelta(days=80)).isoformat(),
            working,
            'baseline',
            project=case.project,
        )
        if inspect.isawaitable(result):
            await result
    case.target.baselines = await case.read_baselines()
    baseline = case.target.baselines.copy()
    if operation == 'follow':
        shot = await case.create(
            'Shot', {'project': case.project, 'code': f'{case.namespace}-shot'}
        )
        await case.call('follow', case.target.human, shot)
    else:
        await case.call(
            'work_schedule_update',
            (case.target.run_date + timedelta(days=45)).isoformat(),
            working,
            f'{case.namespace}-rule',
            project=case.project,
        )
    with pytest.raises(LiveStop, match='refused'):
        case.target.budget.refuse(operation)
    await case.cleanup()
    assert case.target.budget.cleanup_count > 0
    with case.target.budget.cleanup():
        assert await case.read_baselines() == baseline
