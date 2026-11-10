from datetime import date
from typing import TYPE_CHECKING, Literal

import pytest

from tests._live_safety import LiveBudget, LiveStop
from tests.live import _session, _verification
from tests.live._session import SessionPlugin
from tests.tooling import _live_standin

pytestmark = pytest.mark.usefixtures('block_live_connections')

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


class _Client:
    def __init__(self, kind: str) -> None:
        self._kind = kind
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> 'Callable[..., object]':
        def invoke(*_args: object, **_kwargs: object) -> object:
            self.calls.append(name)
            return {'type': 'Note', 'id': 10}

        async def async_invoke(*args: object, **kwargs: object) -> object:
            return invoke(*args, **kwargs)

        return async_invoke if self._kind == 'async' else invoke

    def factory(self, **_kwargs: object) -> object:
        return self


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_local_client_methods_do_not_authorize_rpc(kind: Literal['sync', 'async']) -> None:
    client = _Client(kind)
    budget = LiveBudget(session='L1')
    case = _verification.LiveCase(
        kind, {}, 'owned', target=_verification.LiveTarget(client.factory, budget)
    )
    for method in ('set_session_uuid', 'add_user_agent', 'reset_user_agent', 'close'):
        await case.call(method)
        with pytest.raises(LiveStop, match='rpc'):
            LiveBudget(session='L1').before(
                body=f'{{"method_name": "{method}", "params": []}}'.encode()
            )
    assert client.calls == ['set_session_uuid', 'add_user_agent', 'reset_user_agent', 'close']


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize(
    ('method', 'args', 'options', 'flag'),
    [
        ('follow', ({'type': 'HumanUser', 'id': 9}, {'type': 'Shot', 'id': 10}), {}, 'follow'),
        ('unfollow', ({'type': 'HumanUser', 'id': 2}, {'type': 'Shot', 'id': 99}), {}, 'follow'),
        ('create', ('Project', {'name': 'foreign'}), {}, 'archive'),
        ('update', ('Project', 1, {'archived': True}), {}, 'archive'),
        ('delete', ('Project', 99), {}, 'archive'),
        ('revive', ('Project', 99), {}, 'archive'),
        (
            'work_schedule_update',
            ('2026-10-21', False, 'owned-rule'),
            {'project': {'type': 'Project', 'id': 1}},
            'schedule',
        ),
        (
            'work_schedule_update',
            ('2026-12-01', False, 'foreign-rule'),
            {'project': {'type': 'Project', 'id': 1}},
            'schedule',
        ),
        (
            'work_schedule_update',
            ('2026-12-01', False, 'owned-rule'),
            {'project': {'type': 'Project', 'id': 99}},
            'schedule',
        ),
        (
            'work_schedule_update',
            ('2026-12-01', False, 'owned-rule'),
            {'project': {'type': 'Project', 'id': 1}, 'user': {'id': 2}},
            'schedule',
        ),
        (
            'work_schedule_update',
            ('2026-12-03', False, 'owned-rule'),
            {'project': {'type': 'Project', 'id': 1}},
            'schedule',
        ),
    ],
)
async def test_guards_refuse_before_transport_and_set_flag(
    kind: Literal['sync', 'async'],
    method: str,
    args: tuple[object, ...],
    options: dict[str, object],
    flag: str,
) -> None:
    client = _Client(kind)
    target = _verification.LiveTarget(client.factory, LiveBudget(session='L4'))
    target.human = {'type': 'HumanUser', 'id': 2}
    target.run_date = date(2026, 10, 20)
    target.baselines['W'] = [{'start_date': '2026-12-02', 'end_date': '2026-12-04'}]
    case = _verification.LiveCase(kind, {'type': 'Project', 'id': 1}, 'owned', target=target)
    case.track('Shot', 10)
    with pytest.raises(LiveStop, match='refused'):
        await case.call(method, *args, **options)
    assert client.calls == []
    assert flag in case.flags


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize(
    ('entity_type', 'data'),
    [
        ('Shot', {'code': 'owned-shot'}),
        ('Note', {'subject': 'foreign', 'project': {'type': 'Project', 'id': 1}}),
        ('Note', {'subject': 'owned-note', 'project': {'type': 'Project', 'id': 99}}),
        ('Reply', {'entity': {'type': 'Note', 'id': 99}, 'content': 'owned-reply'}),
    ],
)
async def test_sudo_create_guards(
    kind: Literal['sync', 'async'],
    entity_type: str,
    data: dict[str, object],
) -> None:
    client = _Client(kind)
    target = _verification.LiveTarget(client.factory, LiveBudget(session='L4'))
    case = _verification.LiveCase(kind, {'type': 'Project', 'id': 1}, 'owned', target=target)
    with pytest.raises(LiveStop, match='refused'):
        await case.sudo_call('create', entity_type, data)
    assert client.calls == []
    assert 'follow' in case.flags


@pytest.mark.anyio
async def test_session_refuses_changed_baseline_before_next_case(tmp_path: 'Path') -> None:
    plugin = SessionPlugin('L4', tmp_path, LiveBudget(session='L4'), _live_standin)
    initial = await plugin.initialize()
    plugin.baselines['S'] = ['original subscription baseline']
    with pytest.raises(LiveStop, match='baseline'):
        await plugin.make_case('sync')
    await initial.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('name', ['C', 'L1', 'L2', 'L3'])
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_standard_case_reuses_resolution_without_baseline_requests(
    tmp_path: 'Path', monkeypatch: pytest.MonkeyPatch, name: str, kind: str
) -> None:
    async def factory(kind: str, namespace: str, budget: LiveBudget) -> _verification.LiveCase:
        case = await _live_standin.make_case(kind, namespace, budget)
        assert case.target is not None
        case.target.project_name = 'standin'
        case.target.human = {}
        case.project = {}
        return case

    monkeypatch.setattr(_session, 'production_case', factory)
    plugin = SessionPlugin(name, tmp_path, LiveBudget(session=name))
    initial = await plugin.initialize()
    before = plugin.budget.counts['requests']
    case = await plugin.make_case(kind)
    assert plugin.budget.counts['requests'] == before
    assert case.project == initial.project
    assert case.target is not None
    assert initial.target is not None
    assert case.target.human == initial.target.human
    assert case.target.human['type'] == 'HumanUser'
    await case.cleanup()
    await case.call('close')
    await initial.call('close')
