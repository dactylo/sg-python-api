import json
from types import SimpleNamespace
from typing import cast

import pytest
import urllib3

from shotgun_api3._transport import (
    TransportRequest,
    Urllib3Transport,
)
from tests._live_safety import LiveBudget, LiveStop
from tests.live._verification import LiveCase
from tests.live.conftest import patch_live_transports
from tests.tooling._required_mode import (
    configure_isolated_environment,
    install_plugin,
)


def test_live_budget_enforces_privileged_call_limits_without_retaining_values() -> None:
    budget = LiveBudget()
    payload = json.dumps(
        {'method_name': 'user_subscriptions_create', 'params': [{'private': 'secret-value'}]}
    ).encode()
    budget.before(body=payload)
    budget.before(body=payload)
    with pytest.raises(LiveStop):
        budget.before(body=payload)
    assert budget.counts['subscription_creates'] == 2
    assert 'secret-value' not in repr(budget.counts)


@pytest.mark.parametrize('extended', [False, True])
@pytest.mark.parametrize('fail_teardown', [False, True])
def test_live_evidence_includes_final_teardown(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
    *,
    extended: bool,
    fail_teardown: bool,
) -> None:
    configure_isolated_environment(monkeypatch)
    pytester.makeconftest("pytest_plugins = ('tests._required_mode', 'tests.live.conftest')")
    pytester.makeini('[pytest]\nmarkers = live: synthetic live-harness case')
    prefix = 'extended_' if extended else ''
    pytester.makepyfile(
        f"""import pytest
pytestmark = pytest.mark.live
@pytest.fixture
def cleanup(request, live_budget):
    live_budget.current_test = request.node.nodeid
    yield
    live_budget.before()
    if {fail_teardown!r} and request.node.name.endswith('last'):
        raise RuntimeError('synthetic cleanup failure')
def test_{prefix}first(cleanup): pass
def test_{prefix}last(cleanup): pass
"""
    )
    evidence_dir = pytester.path / 'evidence'
    args = ['-q', '--live-evidence-dir', str(evidence_dir)]
    if extended:
        args.append('--live-extended')
    result = pytester.runpytest_subprocess(*args)
    result.assert_outcomes(passed=2, errors=int(fail_teardown))
    mode = 'extended' if extended else 'default'
    evidence = json.loads((evidence_dir / f'{mode}.json').read_text())
    outcomes = list(evidence['tests'].values())
    assert len(outcomes) == 2
    assert outcomes[0] == {'setup': 'passed', 'call': 'passed', 'teardown': 'passed'}
    assert outcomes[1]['setup'] == outcomes[1]['call'] == 'passed'
    assert outcomes[1]['teardown'] == ('failed' if fail_teardown else 'passed')
    if fail_teardown:
        assert outcomes[1]['failures']['teardown']['error_type'] == 'RuntimeError'
    assert evidence['stop_reason'] == (
        'live cleanup or restoration failed' if fail_teardown else ''
    )
    assert evidence['counts']['requests'] == 2
    assert list(evidence['requests_by_test'].values()) == [1, 1]
    assert json.loads((evidence_dir / 'budget.json').read_text()) == evidence['total_counts']


@pytest.mark.parametrize('status', [429, 503])
def test_live_budget_stops_before_any_later_attempt(status: int) -> None:
    budget = LiveBudget()
    budget.before()
    with pytest.raises(LiveStop):
        budget.response(status)
    with pytest.raises(LiveStop):
        budget.before()
    assert budget.counts['requests'] == 1


@pytest.mark.async_required
@pytest.mark.parametrize('status', [429, 503])
def test_live_guard_blocks_transport_retry_and_releases_response(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    sent: list[TransportRequest] = []
    closed: list[bool] = []

    def fake_start(
        _transport: Urllib3Transport, outgoing: TransportRequest, *, preload_content: bool
    ) -> urllib3.response.HTTPResponse:
        del preload_content
        sent.append(outgoing)
        response = urllib3.response.HTTPResponse(status=status)
        monkeypatch.setattr(response, 'close', lambda: closed.append(True))
        return response

    monkeypatch.setattr(Urllib3Transport, '_start_request', fake_start)
    budget = LiveBudget()
    patch_live_transports(budget, monkeypatch)
    transport = cast('Urllib3Transport', SimpleNamespace())
    request = cast('TransportRequest', SimpleNamespace(method='POST', body=b'{}', headers={}))
    for _ in range(2):
        with pytest.raises(LiveStop):
            Urllib3Transport._start_request(transport, request, preload_content=True)
    assert sent == [request]
    assert closed == [True]


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'upload'])
async def test_live_cleanup_recovers_create_with_lost_response(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    for name, value in (
        ('SG_TEST_SITE', 'https://live-test.example'),
        ('SG_TEST_SCRIPT', 'test-script'),
        ('SG_TEST_KEY', 'test-key'),
    ):
        monkeypatch.setenv(name, value)
    case = LiveCase('sync', {'type': 'Project', 'id': 1}, 'disposable-namespace')
    active = {10}
    deleted: list[int] = []

    entity_type = 'Asset' if operation == 'create' else 'Attachment'

    def lost_response(*_args: object) -> None:
        raise OSError('response lost after server commit')

    def find(queried_type: str, *_args: object) -> list[dict[str, object]]:
        return [{'id': value} for value in active] if queried_type == entity_type else []

    def find_one(_entity_type: str, filters: list[list[object]]) -> dict[str, object] | None:
        identity = cast('int', filters[0][2])
        return {'id': identity} if identity in active else None

    def delete(_entity_type: str, identity: int) -> bool:
        active.remove(identity)
        deleted.append(identity)
        return True

    monkeypatch.setattr(case.client, operation, lost_response)
    monkeypatch.setattr(case.client, 'find', find)
    monkeypatch.setattr(case.client, 'find_one', find_one)
    monkeypatch.setattr(case.client, 'delete', delete)
    with pytest.raises(OSError, match='response lost'):
        await case.call(operation, 'Asset', {'code': case.namespace})
    await case.cleanup()
    assert deleted == [10]
    assert not active


@pytest.mark.anyio
async def test_live_read_only_cleanup_needs_no_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in (
        ('SG_TEST_SITE', 'https://live-test.example'),
        ('SG_TEST_SCRIPT', 'test-script'),
        ('SG_TEST_KEY', 'test-key'),
    ):
        monkeypatch.setenv(name, value)
    case = LiveCase('sync', {'type': 'Project', 'id': 1}, 'disposable-namespace')
    queried: list[str] = []

    def find(entity_type: str, *_args: object) -> list[dict[str, object]]:
        queried.append(entity_type)
        return []

    monkeypatch.setattr(case.client, 'find', find)
    await case.cleanup()
    assert queried == []


@pytest.mark.parametrize('extended', [False, True])
def test_live_extended_selection_deselects_without_skipping(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, *, extended: bool
) -> None:
    configure_isolated_environment(
        monkeypatch, present=('SG_TEST_KEY', 'SG_TEST_PROJECT', 'SG_TEST_SCRIPT', 'SG_TEST_SITE')
    )
    install_plugin(pytester)
    pytester.makepyfile(
        'import pytest\npytestmark = pytest.mark.live\n'
        'def test_default_read(): pass\n'
        'def test_extended_write(): pass\n'
    )
    args = ['-q', '--require-live']
    if extended:
        args.append('--live-extended')
    result = pytester.runpytest_subprocess(*args)
    result.assert_outcomes(passed=1, deselected=1)
