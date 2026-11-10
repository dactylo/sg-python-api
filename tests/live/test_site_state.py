from __future__ import annotations

import asyncio
import copy
import os
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import ProtocolError, ShotgunError
from tests.live._shapes import dictionary, list_items

if TYPE_CHECKING:
    from pathlib import Path

    from tests.live._verification import LiveCase

pytestmark = [pytest.mark.live, pytest.mark.anyio, pytest.mark.async_required]

_RULE_FIELDS = (
    'start_date',
    'end_date',
    'working',
    'user',
    'description',
    'precedence',
    'day_of_week',
    'project',
)


async def _call(case: LiveCase, method: str, *args: object, **kwargs: object) -> object:
    try:
        return await case.call(method, *args, **kwargs)
    except (ShotgunError, ProtocolError, OSError, ValueError, TypeError):
        pytest.fail(f'live {method} request failed', pytrace=False)


async def _create(case: LiveCase, entity_type: str, data: dict[str, object]) -> dict[str, object]:
    try:
        return await case.create(entity_type, data)
    except (ShotgunError, ProtocolError, OSError, ValueError, TypeError):
        pytest.fail('live disposable record creation failed', pytrace=False)


def _list(value: object, label: str) -> list[object]:
    return list_items(value, f'{label} has the wrong shape', pytrace=False)


def _mapping(value: object, label: str) -> dict[str, object]:
    return dictionary(value, f'{label} has the wrong shape', pytrace=False)


def _ids(rows: object, entity_type: str) -> set[int]:
    result: set[int] = set()
    for item_value in _list(rows, 'live entity-list result'):
        item = _mapping(item_value, 'live entity-list item')
        if item.get('type') != entity_type:
            continue
        entity_id = item.get('id')
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            pytest.fail('live entity-list item has no integer ID', pytrace=False)
        result.add(entity_id)
    return result


async def _project_rules(case: LiveCase) -> dict[int, dict[str, object]]:
    rows = await _call(case, 'find', 'WorkDayRule', [], list(_RULE_FIELDS))
    project_id = case.project['id']
    result: dict[int, dict[str, object]] = {}
    for row_value in _list(rows, 'work rule read'):
        row = _mapping(row_value, 'work rule item')
        project = row.get('project')
        if not isinstance(project, dict):
            continue
        project_mapping = cast('dict[str, object]', project)
        if project_mapping.get('id') != project_id:
            continue
        if row.get('user') is not None:
            continue
        rule_id = row.get('id')
        if not isinstance(rule_id, int) or isinstance(rule_id, bool):
            pytest.fail('work rule item has no integer ID', pytrace=False)
        if any(field not in row for field in _RULE_FIELDS):
            pytest.fail('work rule item lacks restoration fields', pytrace=False)
        result[rule_id] = copy.deepcopy({field: row[field] for field in _RULE_FIELDS})
    return result


async def _restore_call(
    case: LiveCase, method: str, failures: list[str], *args: object
) -> object | None:
    try:
        return await case.call(method, *args)
    except (ShotgunError, ProtocolError, OSError, ValueError, TypeError):
        failures.append(method)
        return None


async def _follow_state(
    case: LiveCase,
    user: dict[str, object],
    asset: dict[str, object],
    *,
    restoration: bool = False,
) -> tuple[set[int], set[int]]:
    if restoration:
        failures: list[str] = []
        followers = await _restore_call(case, 'followers', failures, asset)
        following = await _restore_call(case, 'following', failures, user, case.project, 'Asset')
        if failures or followers is None or following is None:
            pytest.exit('live follow restoration could not be inspected', returncode=2)
    else:
        followers = await _call(case, 'followers', asset)
        following = await _call(case, 'following', user, case.project, 'Asset')
    return _ids(followers, 'HumanUser'), _ids(following, 'Asset')


async def _restore_schedule(
    case: LiveCase,
    before_rules: dict[int, dict[str, object]],
    before_schedule: dict[str, object],
    day_text: str,
) -> None:
    current_rules = await _project_rules(case)
    failures: list[str] = []
    for rule_id in current_rules.keys() - before_rules.keys():
        deleted = await _restore_call(case, 'delete', failures, 'WorkDayRule', rule_id)
        if deleted is not True:
            failures.append('delete result')
    for rule_id, prior in before_rules.items():
        current = current_rules.get(rule_id)
        if current is None:
            revived = await _restore_call(case, 'revive', failures, 'WorkDayRule', rule_id)
            if revived is not True:
                failures.append('revive result')
            current = {}
        changes = {
            field: copy.deepcopy(value)
            for field, value in prior.items()
            if current.get(field) != value
        }
        if changes:
            await _restore_call(case, 'update', failures, 'WorkDayRule', rule_id, changes)
    if await _project_rules(case) != before_rules:
        failures.append('work rule readback')
    after_schedule = await _call(
        case, 'work_schedule_read', day_text, day_text, project=case.project
    )
    if after_schedule != before_schedule:
        failures.append('work schedule readback')
    if failures:
        pytest.exit('live schedule restoration or verification failed', returncode=2)


async def _restore_schema_field(
    case: LiveCase, before: dict[str, object], field_name: str | None
) -> None:
    failures: list[str] = []
    if field_name is None:
        readback = await _restore_call(case, 'schema_field_read', failures, 'Asset')
        if readback is None:
            pytest.exit('live schema field cleanup could not be inspected', returncode=2)
        current = _mapping(readback, 'field cleanup read')
        created_names = set(current) - set(before)
        if len(created_names) == 1:
            field_name = next(iter(created_names))
        elif len(created_names) > 1:
            pytest.exit('live schema field cleanup found multiple new fields', returncode=2)
    if field_name is None:
        return
    deleted = await _restore_call(case, 'schema_field_delete', failures, 'Asset', field_name)
    if deleted is not True:
        pytest.exit('live schema field deletion failed', returncode=2)
    readback = await _restore_call(case, 'schema_field_read', failures, 'Asset')
    if readback is None:
        pytest.exit('live schema field restoration could not be verified', returncode=2)
    remaining = _mapping(readback, 'remaining field schema')
    if field_name in remaining:
        pytest.exit('live schema field remains active', returncode=2)


def _rule_covers(rule: dict[str, object], day: date) -> bool:
    start = rule['start_date']
    end = rule['end_date']
    if start is None and end is None:
        return False
    text = day.isoformat()
    return (start is None or str(start) <= text) and (end is None or text <= str(end))


def _same_state(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        pytest.exit(f'live {label} was not restored', returncode=2)


async def test_activity_stream_contains_disposable_update(api_case: LiveCase) -> None:
    asset = await _create(
        api_case, 'Asset', {'project': api_case.project, 'code': api_case.namespace + '-activity'}
    )
    asset_id = asset['id']
    if not isinstance(asset_id, int):
        pytest.fail('disposable Asset has no integer ID', pytrace=False)
    current = _mapping(
        await _call(api_case, 'find_one', 'Asset', [['id', 'is', asset_id]], ['sg_status_list']),
        'current Asset status',
    )
    schema = _mapping(
        await _call(
            api_case,
            'schema_field_read',
            'Asset',
            'sg_status_list',
            project_entity=api_case.project,
        ),
        'Asset status schema',
    )
    field = _mapping(schema.get('sg_status_list'), 'Asset status field')
    properties = _mapping(field.get('properties'), 'Asset status properties')
    valid_values = _mapping(properties.get('valid_values'), 'Asset status valid values')
    statuses = _list(valid_values.get('value'), 'Asset status values')
    status = next(
        (
            value
            for value in statuses
            if isinstance(value, str) and value != current.get('sg_status_list')
        ),
        None,
    )
    if status is None:
        pytest.fail('Asset status has no different valid value', pytrace=False)
    baseline = _mapping(
        await _call(api_case, 'activity_stream_read', 'Asset', asset_id), 'initial activity'
    )
    initial_updates = _list(baseline.get('updates'), 'initial activity updates')
    updated = _mapping(
        await _call(api_case, 'update', 'Asset', asset_id, {'sg_status_list': status}),
        'asset update',
    )
    if updated.get('sg_status_list') != status:
        pytest.fail('Asset update was not read back', pytrace=False)
    for attempt in range(4):
        stream = _mapping(
            await _call(api_case, 'activity_stream_read', 'Asset', asset_id), 'activity'
        )
        updates = _list(stream.get('updates'), 'activity updates')
        if len(updates) > len(initial_updates) and all(isinstance(item, dict) for item in updates):
            break
        if attempt < 3:
            await asyncio.sleep(0.5)
    else:
        pytest.fail('disposable update is absent from activity stream', pytrace=False)


async def test_extended_multipart_upload_roundtrip(api_case: LiveCase, tmp_path: Path) -> None:
    owner = await _create(
        api_case, 'Shot', {'project': api_case.project, 'code': api_case.namespace + '-multipart'}
    )
    owner_id = owner['id']
    if not isinstance(owner_id, int):
        pytest.fail('disposable Shot has no integer ID', pytrace=False)
    path = tmp_path / (api_case.namespace + '-multipart.bin')
    payload = b'live-multipart-' * 1_333_334
    payload = payload[:20_000_001]
    assert len(payload) == 20_000_001
    path.write_bytes(payload)
    attachment_id = await _call(api_case, 'upload', 'Shot', owner_id, str(path))
    if not isinstance(attachment_id, int) or isinstance(attachment_id, bool):
        pytest.fail('multipart upload returned no attachment ID', pytrace=False)
    api_case.track('Attachment', attachment_id)
    downloaded = await _call(api_case, 'download_attachment', attachment_id)
    if downloaded != payload:
        pytest.fail('multipart download differs from uploaded bytes', pytrace=False)


async def test_extended_schema_field_roundtrip(api_case: LiveCase) -> None:
    label = 'Live Verify ' + api_case.namespace[-12:]
    before = _mapping(await _call(api_case, 'schema_field_read', 'Asset'), 'field snapshot')
    field_name: str | None = None
    try:
        created = await _call(api_case, 'schema_field_create', 'Asset', 'text', label)
        if not isinstance(created, str) or not created.startswith('sg_'):
            pytest.fail('schema creation returned no custom field', pytrace=False)
        field_name = created
        fields = _mapping(await _call(api_case, 'schema_field_read', 'Asset'), 'field schema')
        if field_name not in fields:
            pytest.fail('created schema field is absent', pytrace=False)
        changed = await _call(
            api_case, 'schema_field_update', 'Asset', field_name, {'description': label}
        )
        assert changed is True
        selected = _mapping(
            await _call(api_case, 'schema_field_read', 'Asset', field_name),
            'selected field schema',
        )
        if not selected:
            pytest.fail('updated schema field is absent', pytrace=False)
        detail = _mapping(selected.get(field_name, selected), 'updated field detail')
        description = detail.get('description')
        if isinstance(description, dict):
            description = cast('dict[str, object]', description).get('value')
        if description != label:
            pytest.fail('updated schema field description was not read back', pytrace=False)
    finally:
        await _restore_schema_field(api_case, before, field_name)


async def test_extended_subscription_resend_preserves_full_snapshot(api_case: LiveCase) -> None:
    login = os.environ.get('SG_TEST_HUMAN_LOGIN')
    if not login:
        pytest.fail('SG_TEST_HUMAN_LOGIN is required', pytrace=False)
    snapshot = _list(await _call(api_case, 'user_subscriptions_read'), 'subscription snapshot')
    user = _mapping(
        await _call(api_case, 'find_one', 'HumanUser', [['login', 'is', login]], ['id']),
        'configured human user',
    )
    user_id = user.get('id')
    if not isinstance(user_id, int) or isinstance(user_id, bool):
        pytest.fail('configured human user is unavailable', pytrace=False)
    entries = [
        _mapping(row, 'subscription entry')
        for row in snapshot
        if _mapping(row, 'subscription entry').get('humanUserId') == str(user_id)
    ]
    if len(entries) != 1 or entries[0].get('subscription') in (None, []):
        pytest.fail('configured user has no restorable subscription', pytrace=False)
    assignment = copy.deepcopy(entries[0]['subscription'])
    try:
        result = await _call(
            api_case,
            'user_subscriptions_create',
            [{'humanUserId': str(user_id), 'subscription': assignment}],
        )
        assert result is True
    finally:
        readback = await _call(api_case, 'user_subscriptions_read')
        _same_state(readback, snapshot, 'subscription state')


async def test_extended_work_schedule_restores_exact_state(api_case: LiveCase) -> None:
    before_rules = await _project_rules(api_case)
    start = datetime.now(tz=UTC).date() + timedelta(days=30)
    day = next(
        (
            start + timedelta(days=offset)
            for offset in range(365)
            if not any(
                _rule_covers(rule, start + timedelta(days=offset))
                for rule in before_rules.values()
            )
        ),
        None,
    )
    if day is None:
        pytest.fail('no isolated project schedule date is available', pytrace=False)
    day_text = day.isoformat()
    before_schedule = _mapping(
        await _call(api_case, 'work_schedule_read', day_text, day_text, project=api_case.project),
        'work schedule snapshot',
    )
    prior_entry = _mapping(before_schedule.get(day_text), 'work schedule day snapshot')
    prior_working = prior_entry.get('working')
    if not isinstance(prior_working, bool):
        pytest.fail('work schedule snapshot has no working flag', pytrace=False)
    try:
        changed = _mapping(
            await _call(
                api_case,
                'work_schedule_update',
                day_text,
                not prior_working,
                description=api_case.namespace + '-schedule',
                project=api_case.project,
            ),
            'schedule update',
        )
        if not changed:
            pytest.fail('schedule update returned no data', pytrace=False)
        updated = _mapping(
            await _call(
                api_case, 'work_schedule_read', day_text, day_text, project=api_case.project
            ),
            'updated work schedule',
        )
        entry = _mapping(updated.get(day_text), 'updated work schedule day')
        if entry.get('working') is prior_working:
            pytest.fail('work schedule day did not change', pytrace=False)
    finally:
        await _restore_schedule(api_case, before_rules, before_schedule, day_text)


async def test_follow_state_is_restored_after_roundtrip(api_case: LiveCase) -> None:
    login = os.environ.get('SG_TEST_HUMAN_LOGIN')
    if not login:
        pytest.fail('SG_TEST_HUMAN_LOGIN is required', pytrace=False)
    user_value = await _call(api_case, 'find_one', 'HumanUser', [['login', 'is', login]], ['id'])
    user = _mapping(user_value, 'configured human user')
    if not isinstance(user.get('id'), int):
        pytest.fail('configured human user is unavailable', pytrace=False)
    asset = await _create(
        api_case, 'Asset', {'project': api_case.project, 'code': api_case.namespace + '-follow'}
    )
    user_id = cast('int', user['id'])
    asset_id = asset['id']
    if not isinstance(asset_id, int):
        pytest.fail('disposable Asset has no integer ID', pytrace=False)
    baseline = await _follow_state(api_case, user, asset)
    initially_following = user_id in baseline[0] and asset_id in baseline[1]
    try:
        method = 'unfollow' if initially_following else 'follow'
        _mapping(await _call(api_case, method, user, asset), 'follow change')
        after = await _follow_state(api_case, user, asset)
        if (user_id in after[0]) is initially_following:
            pytest.fail('follower state did not change', pytrace=False)
        if (asset_id in after[1]) is initially_following:
            pytest.fail('following state did not change', pytrace=False)
        _mapping(
            await _call(api_case, 'follow' if initially_following else 'unfollow', user, asset),
            'follow restoration',
        )
    finally:
        current = await _follow_state(api_case, user, asset, restoration=True)
        current_follows = user_id in current[0] or asset_id in current[1]
        if current_follows != initially_following:
            failures: list[str] = []
            result = await _restore_call(
                api_case, 'follow' if initially_following else 'unfollow', failures, user, asset
            )
            if failures or not isinstance(result, dict):
                pytest.exit('live follow restoration failed', returncode=2)
        _same_state(
            await _follow_state(api_case, user, asset, restoration=True), baseline, 'follow state'
        )


async def test_project_access_update_returns_documented_result(api_case: LiveCase) -> None:
    result = await _call(api_case, 'update_project_last_accessed', api_case.project)
    assert result is None
    project = _mapping(
        await _call(
            api_case, 'find_one', 'Project', [['id', 'is', api_case.project['id']]], ['id']
        ),
        'project read',
    )
    if project.get('id') != api_case.project['id']:
        pytest.fail('project read returned a different ID', pytrace=False)


async def test_schema_reads_are_available_with_and_without_project(api_case: LiveCase) -> None:
    for project in (None, api_case.project):
        schema = _mapping(await _call(api_case, 'schema_read', project_entity=project), 'schema')
        entities = _mapping(
            await _call(api_case, 'schema_entity_read', project_entity=project), 'entity schema'
        )
        fields = _mapping(
            await _call(api_case, 'schema_field_read', 'Asset', project_entity=project),
            'field schema',
        )
        selected = _mapping(
            await _call(api_case, 'schema_field_read', 'Asset', 'id', project_entity=project),
            'selected field schema',
        )
        _mapping(schema.get('Asset'), 'Asset schema')
        _mapping(entities.get('Asset'), 'Asset entity schema')
        _mapping(fields.get('id'), 'Asset ID field schema')
        if not selected:
            pytest.fail('selected field schema is empty', pytrace=False)


async def test_site_preferences_and_schedule_have_documented_shapes(api_case: LiveCase) -> None:
    preferences = _mapping(await _call(api_case, 'preferences_read'), 'preferences')
    if not preferences:
        pytest.fail('preferences read is empty', pytrace=False)
    day = datetime.now(tz=UTC).date().isoformat()
    schedule = _mapping(
        await _call(api_case, 'work_schedule_read', day, day, project=api_case.project),
        'work schedule',
    )
    entry = _mapping(schedule.get(day), 'work schedule day')
    if not isinstance(entry.get('working'), bool):
        pytest.fail('work schedule day has no working flag', pytrace=False)
