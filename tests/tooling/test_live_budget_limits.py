import json

import pytest

from tests._live_safety import LiveBudget, LiveStop
from tests._live_vocabulary import SESSION_NAMES


def _body(method: str, entity_type: str = 'Shot') -> bytes:
    return json.dumps({'method_name': method, 'params': [{}, {'type': entity_type}]}).encode()


def test_cleanup_exhaustion_and_case_reserve() -> None:
    budget = LiveBudget(limits={'requests': 20}, cleanup_requests=1, session='C')
    budget.start_case()
    budget.before()
    with pytest.raises(LiveStop, match='reserve'):
        budget.start_case()
    with budget.cleanup():
        budget.before()
        with pytest.raises(LiveStop, match='cleanup'):
            budget.before()


@pytest.mark.parametrize('status', [429, 503])
def test_cleanup_has_separate_allowance_and_latches(status: int) -> None:
    budget = LiveBudget(limits={'requests': 1}, cleanup_requests=2)
    budget.before()
    with pytest.raises(LiveStop):
        budget.response(status)
    with budget.cleanup():
        budget.before()
        with pytest.raises(LiveStop):
            budget.response(status)
        with pytest.raises(LiveStop):
            budget.before()
    assert budget.cleanup_count == 1
    assert budget.counts['requests'] == 2


def test_counters_and_waits_charge_before_send() -> None:
    budget = LiveBudget(limits={'uploads': 1, 'waits': 2}, session='L2')
    budget.before(upload_bytes=40)
    budget.wait(2)
    assert budget.counts['uploads'] == 1
    assert budget.counts['upload_bytes'] == 40
    assert budget.counts['waits'] == 2
    with pytest.raises(LiveStop, match='waits'):
        budget.wait(1)


def test_limits_can_only_lower_and_attempts_remain_charged() -> None:
    with pytest.raises(ValueError, match='lower'):
        LiveBudget(limits={'requests': 2501})
    budget = LiveBudget(limits={'requests': 1})
    budget.before()
    with pytest.raises(LiveStop, match='requests'):
        budget.before()
    assert budget.counts['requests'] == 1


@pytest.mark.parametrize('session', ['C', 'L1', 'L2', 'L3'])
@pytest.mark.parametrize(
    'method',
    [
        'follow',
        'unfollow',
        'work_schedule_update',
        'user_subscriptions_create',
        'update_project_last_accessed',
        'schema_field_create',
        'schema_field_update',
        'schema_field_delete',
        'export_page',
        'unknown_rpc',
    ],
)
def test_session_forbidden_rpcs_refuse_before_transport(session: str, method: str) -> None:
    budget = LiveBudget(session=session)
    with pytest.raises(LiveStop, match='refused'):
        budget.before(body=_body(method))
    assert budget.counts['requests'] == 0


@pytest.mark.parametrize('session', SESSION_NAMES)
@pytest.mark.parametrize(
    'entity_type', ['HumanUser', 'ApiUser', 'PermissionRuleSet', 'WorkDayRule']
)
def test_unowned_entity_writes_refuse(session: str, entity_type: str) -> None:
    budget = LiveBudget(session=session)
    with pytest.raises(LiveStop, match='refused'):
        budget.before(body=_body('create', entity_type))
    assert budget.counts['requests'] == 0


@pytest.mark.parametrize('session', SESSION_NAMES)
def test_nested_batch_member_cannot_bypass_entity_allowlist(session: str) -> None:
    budget = LiveBudget(session=session)
    body = json.dumps(
        {
            'method_name': 'batch',
            'params': [{}, [{'request_type': 'update', 'type': 'HumanUser', 'id': 1}]],
        }
    ).encode()
    with pytest.raises(LiveStop, match='refused'):
        budget.before(body=body)
    assert budget.counts['requests'] == 0
