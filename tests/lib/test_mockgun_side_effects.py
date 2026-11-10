from copy import deepcopy
from datetime import timedelta
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun import _handlers, _retirement
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._sessions import RequestContext
from shotgun_api3.lib.mockgun._store import FieldValue, Row, Store
from tests.conftest import entity_id

from .conftest import QUERY_NOW, QUERY_SCHEMA, FakeSiteCase, raw_call, seed

_LATER = QUERY_NOW + timedelta(hours=1)
_SCRIPT = {'type': 'ApiUser', 'id': 1}


async def _fields(case: FakeSiteCase, link: dict[str, object], *names: str) -> dict[str, object]:
    return cast(
        'dict[str, object]',
        await case.call('find_one', link['type'], [['id', 'is', link['id']]], list(names)),
    )


def _links(value: object) -> object:
    if isinstance(value, list):
        return [_links(item) for item in cast('list[object]', value)]
    if isinstance(value, dict):
        link = cast('dict[str, object]', value)
        return {'type': link['type'], 'id': link['id']}
    return value


async def _scene(
    case: FakeSiteCase,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    sequence = cast('dict[str, object]', await case.call('create', 'Sequence', {'code': 'q1'}))
    asset_a = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'a1'}))
    asset_b = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'b1'}))
    shot = cast(
        'dict[str, object]',
        await case.call(
            'create',
            'Shot',
            {
                'code': 's1',
                'description': 'd',
                'sg_status_list': 'ip',
                'sg_sequence': sequence,
                'assets': [asset_a],
            },
        ),
    )
    case.client.site.set_time(_LATER)
    return sequence, asset_a, asset_b, shot


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update'])
@pytest.mark.parametrize('entity_type', ['Shot', 'Version'])
async def test_failed_write_restores_all_tables_after_event_mutation(
    effects_site: FakeSiteCase,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    entity_type: str,
) -> None:
    await effects_site.call('find', 'Shot', [], ['id'])
    asset = seed(effects_site, 'Asset', {'code': 'old'})
    replacement = seed(effects_site, 'Asset', {'code': 'new'})
    task = seed(effects_site, 'Task', {'content': 'task'})
    playlist = seed(effects_site, 'Playlist', {'code': 'playlist'})
    data = (
        {'assets': [replacement]}
        if entity_type == 'Shot'
        else {'sg_task': task, 'playlists': [playlist]}
    )
    target = seed(
        effects_site,
        entity_type,
        {'code': 'existing', **({'assets': [asset]} if entity_type == 'Shot' else {})},
    )
    seed(effects_site, 'Shot', {'code': 'other connection', 'assets': [asset]})
    store = effects_site.client._server.store
    row = store.require(entity_type, entity_id(target))
    store.mark_unknown(row, 'updated_at', 'previous unknown audit')
    store.mark_unknown(row, 'updated_by', 'previous unknown audit')
    store.mark_unknown(
        row, 'assets' if entity_type == 'Shot' else 'playlists', 'old order', order_only=True
    )
    unrelated = seed(effects_site, 'Asset', {'code': 'unrelated'})
    store.taint(store.require('Asset', entity_id(unrelated)), 'existing taint', None)
    before = deepcopy((store._db, store.events_unknown))
    next_ids = dict(store._next_ids)

    def fail_after_event(mutated_store: Store, *_args: object, **_kwargs: object) -> None:
        assert deepcopy((mutated_store._db, mutated_store.events_unknown)) != before
        mutated_store.append_event({'event_type': 'rollback probe'}, unknown={'meta'})
        mutated_store.taint_events('failed event write')
        assert mutated_store.created_connections
        raise RuntimeError('failure after event mutation')

    monkeypatch.setattr(_handlers, f'record_{operation}', fail_after_event)
    action = (
        effects_site.call('create', entity_type, {'code': 'created', **data})
        if operation == 'create'
        else effects_site.call('update', entity_type, target['id'], data)
    )
    with pytest.raises(RuntimeError, match='failure after event mutation'):
        await action
    assert deepcopy((store._db, store.events_unknown)) == before
    assert {kind: list(table) for kind, table in store._db.items()} == {
        kind: list(table) for kind, table in before[0].items()
    }
    assert store.created_connections == []
    assert store._next_ids['EventLogEntry'] > next_ids['EventLogEntry']
    if operation == 'create':
        assert store._next_ids[entity_type] > next_ids[entity_type]


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['delete', 'revive'])
async def test_failed_lifecycle_restores_all_tables_after_scan(
    effects_site: FakeSiteCase, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    await effects_site.call('find', 'Shot', [], ['id'])
    asset = seed(effects_site, 'Asset', {'code': 'asset'})
    shot = seed(effects_site, 'Shot', {'code': 'shot', 'assets': [asset]})
    task = seed(effects_site, 'Task', {'content': 'task', 'entity': shot})
    seed(effects_site, 'Note', {'subject': 'note', 'tasks': [task]})
    store = effects_site.client._server.store
    if operation == 'revive':
        await effects_site.call('delete', 'Shot', shot['id'])
    before = deepcopy((store._db, store.events_unknown))
    scan_name = '_scan' if operation == 'delete' else '_revive'
    scan = getattr(_retirement, scan_name)

    def fail_after_scan(mutated_store: Store, root: Row, context: RequestContext) -> None:
        scan(mutated_store, root, context)
        assert deepcopy((mutated_store._db, mutated_store.events_unknown)) != before
        mutated_store.taint_events('failed lifecycle scan')
        raise RuntimeError('failure after lifecycle scan')

    monkeypatch.setattr(_retirement, scan_name, fail_after_scan)
    with pytest.raises(RuntimeError, match='failure after lifecycle scan'):
        await effects_site.call(operation, 'Shot', shot['id'])
    assert deepcopy((store._db, store.events_unknown)) == before
    assert {kind: list(table) for kind, table in store._db.items()} == {
        kind: list(table) for kind, table in before[0].items()
    }
    assert store.created_connections == []


@pytest.mark.anyio
async def test_failed_last_batch_request_restores_all_tables(effects_site: FakeSiteCase) -> None:
    await effects_site.call('find', 'Shot', [], ['id'])
    asset = seed(effects_site, 'Asset', {'code': 'asset'})
    replacement = seed(effects_site, 'Asset', {'code': 'replacement'})
    shot = seed(effects_site, 'Shot', {'code': 'shot', 'assets': [asset]})
    task = seed(effects_site, 'Task', {'content': 'task', 'entity': shot})
    store = effects_site.client._server.store
    store.mark_unknown(store.require('Shot', entity_id(shot)), 'updated_at', 'old audit')
    before = deepcopy((store._db, store.events_unknown))
    next_ids = dict(store._next_ids)
    with pytest.raises(Fault):
        await effects_site.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Version',
                    'data': {'code': 'new', 'sg_task': task},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': shot['id'],
                    'data': {'assets': [replacement]},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': shot['id']},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'missing'},
                },
            ],
        )
    assert deepcopy((store._db, store.events_unknown)) == before
    assert {kind: list(table) for kind, table in store._db.items()} == {
        kind: list(table) for kind, table in before[0].items()
    }
    assert store.created_connections == []
    assert store._next_ids['Version'] > next_ids['Version']
    assert store._next_ids['AssetShotConnection'] > next_ids['AssetShotConnection']


def test_followers_are_sorted_and_scoped_to_entity_identity() -> None:
    store = Store(SiteSchema(cast('dict[str, dict[str, object]]', QUERY_SCHEMA), {}))
    for key in ((3, 'Shot', 2), (1, 'Shot', 2), (4, 'Asset', 2), (5, 'Shot', 1)):
        store.side_set('follows', key, value=True)
    assert store.followers('Shot', 2) == [1, 3]
    assert store.followers('Shot', 9) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('data', 'modes', 'expected'),
    [
        ({'description': 'd'}, None, 'd'),
        ({'sg_status_list': 'ip'}, None, 'ip'),
        ({'sg_sequence': 'sequence'}, None, 'sequence'),
        ({'assets': ['asset_a']}, {'assets': 'add'}, ['asset_a']),
        ({'assets': ['asset_a']}, {'assets': 'set'}, ['asset_a']),
        ({'assets': ['asset_b']}, {'assets': 'remove'}, ['asset_a']),
    ],
)
async def test_same_value_update_preserves_audits_and_pairs(
    effects_site: FakeSiteCase,
    data: dict[str, object],
    modes: dict[str, str] | None,
    expected: object,
) -> None:
    sequence, asset_a, asset_b, shot = await _scene(effects_site)
    values = {'sequence': sequence, 'asset_a': asset_a, 'asset_b': asset_b}
    field = next(iter(data))
    value = data[field]
    if isinstance(value, str) and value in values:
        value = values[value]
    elif isinstance(value, list):
        value = [values[cast('str', item)] for item in cast('list[object]', value)]
    result = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Shot', shot['id'], {field: value}, modes),
    )
    expected_value = (
        values[expected] if isinstance(expected, str) and expected in values else expected
    )
    if isinstance(expected, list):
        expected_value = [values[cast('str', item)] for item in cast('list[object]', expected)]
    assert _links(result[field]) == _links(expected_value)
    actual = await _fields(effects_site, shot, field, 'updated_at', 'updated_by')
    assert _links(actual[field]) == _links(expected_value)
    assert actual['updated_at'] == QUERY_NOW
    assert _links(actual['updated_by']) == _SCRIPT
    for target in (sequence, asset_a):
        mirror = await _fields(effects_site, target, 'shots', 'updated_at', 'updated_by')
        assert _links(mirror['shots']) == _links([shot])
        assert mirror['updated_at'] == QUERY_NOW
        assert _links(mirror['updated_by']) == _SCRIPT


@pytest.mark.anyio
async def test_same_value_sudo_update_keeps_original_actor(effects_site: FakeSiteCase) -> None:
    _, _, _, shot = await _scene(effects_site)
    cast(
        'dict[str, object]',
        await effects_site.call('create', 'HumanUser', {'login': 'artist', 'name': 'Artist'}),
    )
    response = raw_call(
        effects_site.client._server,
        'update',
        {
            'type': 'Shot',
            'id': shot['id'],
            'fields': [{'field_name': 'description', 'value': 'd'}],
            'return_fields': ['description'],
        },
        auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
    )
    assert cast('dict[str, object]', response['results'])['description'] == 'd'
    actual = await _fields(effects_site, shot, 'updated_at', 'updated_by')
    assert actual['updated_at'] == QUERY_NOW
    assert _links(actual['updated_by']) == _SCRIPT


@pytest.mark.anyio
async def test_empty_text_normalized_to_existing_null_is_unchanged(
    effects_site: FakeSiteCase,
) -> None:
    shot = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's2'}))
    effects_site.client.site.set_time(_LATER)
    result = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Shot', shot['id'], {'description': ''}),
    )
    assert result['description'] is None
    actual = await _fields(effects_site, shot, 'description', 'updated_at')
    assert actual['description'] is None
    assert actual['updated_at'] == QUERY_NOW


@pytest.mark.anyio
async def test_changed_and_mixed_updates_touch_audit(effects_site: FakeSiteCase) -> None:
    _, _, _, shot = await _scene(effects_site)
    mixed = cast(
        'dict[str, object]',
        await effects_site.call(
            'update', 'Shot', shot['id'], {'description': 'd', 'sg_status_list': 'fin'}
        ),
    )
    assert {field: mixed[field] for field in ('description', 'sg_status_list')} == {
        'description': 'd',
        'sg_status_list': 'fin',
    }
    actual = await _fields(effects_site, shot, 'description', 'sg_status_list', 'updated_at')
    assert (actual['description'], actual['sg_status_list'], actual['updated_at']) == (
        'd',
        'fin',
        _LATER,
    )
    later = _LATER + timedelta(hours=1)
    effects_site.client.site.set_time(later)
    changed = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Shot', shot['id'], {'description': 'e'}),
    )
    assert changed['description'] == 'e'
    assert (await _fields(effects_site, shot, 'updated_at'))['updated_at'] == later


@pytest.mark.anyio
async def test_reordering_same_members_is_a_change(effects_site: FakeSiteCase) -> None:
    _, asset_a, asset_b, shot = await _scene(effects_site)
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [asset_a, asset_b]})
    later = _LATER + timedelta(hours=1)
    effects_site.client.site.set_time(later)
    result = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Shot', shot['id'], {'assets': [asset_b, asset_a]}),
    )
    assert _links(result['assets']) == _links([asset_b, asset_a])
    actual = await _fields(effects_site, shot, 'assets', 'updated_at')
    assert _links(actual['assets']) == _links([asset_b, asset_a])
    assert actual['updated_at'] == later


def test_side_and_record_id_reservations_survive_rollback() -> None:
    store = Store(SiteSchema(cast('dict[str, dict[str, object]]', QUERY_SCHEMA), {}))
    assert store.next_side_id('activity') == 1

    def reserve_then_fail() -> None:
        with store.transaction():
            assert store.next_side_id('activity') == 2
            assert store.reserve_id('Shot') == 1
            raise RuntimeError('rollback')

    with pytest.raises(RuntimeError, match='rollback'):
        reserve_then_fail()
    assert store.next_side_id('activity') == 3
    assert store.next_side_id('follows') == 1
    row = store.create('Shot', {'code': 'next'}, context=RequestContext(None, QUERY_NOW))
    assert row['id'] == 2


def test_side_tables_transaction_rollback_and_snapshot_restore() -> None:
    store = Store(SiteSchema(cast('dict[str, dict[str, object]]', QUERY_SCHEMA), {}))
    before = store.snapshot()

    def insert_then_fail() -> None:
        with store.transaction():
            store.side_set('follows', (1, 'Shot', 2), value=True)
            raise RuntimeError('rollback')

    with pytest.raises(RuntimeError, match='rollback'):
        insert_then_fail()
    assert store.side_get('follows', (1, 'Shot', 2)) is None
    assert store.snapshot() == before
    store.side_set('follows', (1, 'Shot', 2), value=True)
    assert store.side_get('follows', (1, 'Shot', 2)) is True
    assert store.snapshot() != before
    assert store.followers('Shot', 2) == [1]
    saved = store.snapshot()

    def mutate_then_fail() -> None:
        with store.transaction():
            store.side_set('follows', (1, 'Shot', 2), value=False)
            store.side_set('follows', (1, 'Shot', 2), None)
            store.side_set('follows', (3, 'Shot', 2), value=True)
            raise RuntimeError('rollback')

    with pytest.raises(RuntimeError, match='rollback'):
        mutate_then_fail()
    assert store.snapshot() == saved
    store.side_set('follows', (1, 'Shot', 2), None)
    assert store.side_get('follows', (1, 'Shot', 2)) is None
    store.restore(saved)
    assert store.side_get('follows', (1, 'Shot', 2)) is True
    store.restore(before)
    assert store.snapshot() == before


def test_side_values_are_detached_and_items_preserve_insertion_order() -> None:
    store = Store(SiteSchema(cast('dict[str, dict[str, object]]', QUERY_SCHEMA), {}))
    value = {'value': [1]}
    store.side_set('activity', (2,), cast('FieldValue', value))
    value['value'].append(9)
    store.side_set('activity', (1,), {'value': [2]})
    expected = [((2,), {'value': [1]}), ((1,), {'value': [2]})]
    assert store.side_items('activity') == expected
    detached = store.side_get('activity', (2,))
    assert isinstance(detached, dict)
    detached['value'] = [8]
    items = store.side_items('activity')
    items.clear()
    assert store.side_items('activity') == expected


@pytest.mark.anyio
async def test_version_user_defaults_to_actor(effects_site: FakeSiteCase) -> None:
    human = seed(effects_site, 'HumanUser', {'login': 'artist', 'name': 'Artist'})
    version = cast(
        'dict[str, object]', await effects_site.call('create', 'Version', {'code': 'v1'}, ['user'])
    )
    assert version['user'] == {**_SCRIPT, 'name': 'queries'}
    assert (await _fields(effects_site, version, 'user'))['user'] == version['user']
    response = raw_call(
        effects_site.client._server,
        'create',
        {
            'type': 'Version',
            'fields': [{'field_name': 'code', 'value': 'sudo'}],
            'return_fields': ['user'],
        },
        auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
    )
    assert cast('dict[str, object]', response['results'])['user'] == {**human, 'name': 'Artist'}
    supplied = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Version', {'code': 'v2', 'user': human}),
    )
    assert _links(supplied['user']) == human


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'operation'),
    [
        ('color', 'read'),
        ('due_date', 'read'),
        ('due_date', 'filter'),
        ('due_date', 'order'),
        ('content', 'read'),
        ('content', 'filter'),
        ('content', 'order'),
    ],
)
async def test_omitted_task_fields_are_unknown(
    effects_site: FakeSiteCase, field: str, operation: str
) -> None:
    data = {} if field == 'content' else {'content': 't1'}
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', data))
    await effects_site.call('create', 'Task', {} if field == 'content' else {'content': 't2'})
    if operation == 'read':
        action = _fields(effects_site, task, field)
    elif operation == 'filter':
        action = effects_site.call('find', 'Task', [[field, 'is', None]])
    else:
        action = effects_site.call(
            'find', 'Task', [], order=[{'field_name': field, 'direction': 'asc'}]
        )
    with pytest.raises(mockgun.MockgunError, match=rf'Task\.{field}'):
        await action


@pytest.mark.anyio
async def test_supplied_task_values_remain_known(effects_site: FakeSiteCase) -> None:
    task = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Task', {'content': 't3', 'due_date': '2026-07-16'}),
    )
    assert (await _fields(effects_site, task, 'content', 'due_date'))['due_date'] == '2026-07-16'
    assert (await _fields(effects_site, task, 'content'))['content'] == 't3'
    with pytest.raises(mockgun.MockgunError, match=r'Task\.color'):
        await _fields(effects_site, task, 'color')


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['filter', 'link_create', 'link_read', 'create_result'])
async def test_unknown_task_content_rejects_dependent_results(
    effects_site: FakeSiteCase, operation: str
) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {}))
    version = seed(effects_site, 'Version', {'code': 'v7', 'sg_task': task})
    assert (await _fields(effects_site, version, 'sg_task.Task.id'))['sg_task.Task.id'] == task[
        'id'
    ]
    if operation == 'filter':
        action = effects_site.call('find', 'Task', [['content', 'is', 'x']])
    elif operation == 'link_create':
        action = effects_site.call('create', 'Version', {'code': 'v9', 'sg_task': task})
    elif operation == 'link_read':
        action = _fields(effects_site, version, 'sg_task')
    else:
        action = effects_site.call('create', 'Task', {}, ['content'])
    with pytest.raises(mockgun.MockgunError, match=r'Task\.content'):
        await action
    assert len(cast('list[object]', await effects_site.call('find', 'Task', [], ['id']))) == 1
    assert await effects_site.call('find', 'Version', [['code', 'is', 'v9']]) == []


@pytest.mark.anyio
@pytest.mark.parametrize('content', [None, '', 'x'])
async def test_overwriting_unknown_task_content_updates_certainty(
    effects_site: FakeSiteCase, content: str | None
) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {}))
    version = seed(effects_site, 'Version', {'code': 'v7', 'sg_task': task})
    effects_site.client.site.set_time(_LATER)
    await effects_site.call('update', 'Task', task['id'], {'content': content})
    assert (await _fields(effects_site, task, 'content'))['content'] == (content or None)
    if content == 'x':
        with pytest.raises(mockgun.MockgunError, match=r'Task\.updated_at'):
            await _fields(effects_site, task, 'updated_at')
    else:
        assert (await _fields(effects_site, task, 'updated_at'))['updated_at'] == _LATER
        assert (await _fields(effects_site, version, 'sg_task'))['sg_task'] == {
            'type': 'Task',
            'id': task['id'],
            'name': '-',
        }
        created = cast(
            'dict[str, object]',
            await effects_site.call(
                'create', 'Version', {'code': 'v10', 'sg_task': task}, ['sg_task']
            ),
        )
        assert cast('dict[str, object]', created['sg_task'])['name'] == '-'
    await effects_site.call(
        'update', 'Task', task['id'], {'content': 't4', 'start_date': '2026-07-01'}
    )
    assert (await _fields(effects_site, task, 'content', 'start_date', 'updated_at'))[
        'updated_at'
    ] == _LATER


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('entity_type', 'data'), [('Note', {'subject': 'n1'}), ('Reply', {'content': 'r1'})]
)
async def test_omitted_note_reply_user_defaults_to_actor(
    workflow_site: FakeSiteCase, entity_type: str, data: dict[str, object]
) -> None:
    case = workflow_site
    human = cast(
        'dict[str, object]',
        await case.call('create', 'HumanUser', {'login': 'artist', 'name': 'artist'}),
    )
    login_credentials = {'login': 'artist', 'password': 'x'}
    for client, expected in (
        (case, {'type': 'ApiUser', 'id': 1}),
        (
            FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist')),
            {'type': 'HumanUser', 'id': human['id']},
        ),
        (
            FakeSiteCase(case.kind, case.client.new_client(**login_credentials)),
            {'type': 'HumanUser', 'id': human['id']},
        ),
    ):
        row = cast('dict[str, object]', await client.call('create', entity_type, data))
        assert _links((await _fields(client, row, 'user'))['user']) == expected
    supplied = cast(
        'dict[str, object]',
        await case.call('create', entity_type, {**data, 'user': human}),
    )
    assert _links((await _fields(case, supplied, 'user'))['user']) == _links(human)


@pytest.mark.anyio
async def test_observed_task_create_computes_color_and_due_date(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project = cast('dict[str, object]', await case.call('create', 'Project', {'name': 'P'}))
    shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'S'}))
    tasks = [
        cast('dict[str, object]', await case.call('create', 'Task', fields))
        for fields in (
            {'project': project, 'content': 't1'},
            {'project': project, 'content': 't2', 'entity': shot},
            {'entity': shot, 'content': 't3', 'project': project},
        )
    ]
    for task in tasks:
        assert (await _fields(case, task, 'color', 'due_date'))['color'] == 'pipeline_step'
        assert (await _fields(case, task, 'due_date'))['due_date'] is None
    found = cast(
        'list[dict[str, object]]',
        await case.call('find', 'Task', [['color', 'is', 'pipeline_step']], ['id']),
    )
    assert [row['id'] for row in found] == [task['id'] for task in tasks]
    with pytest.raises(mockgun.MockgunError, match=r'Task\.color'):
        await case.call('find', 'Task', [['color', 'is', 'red']], ['id'])


@pytest.mark.anyio
@pytest.mark.parametrize('operands', [[], ['pipeline_step', 'other']])
async def test_unobserved_task_color_filter_arity_remains_unsettled(
    workflow_site: FakeSiteCase, operands: list[str]
) -> None:
    with pytest.raises(mockgun.MockgunError, match=r'Task\.color'):
        await workflow_site.call('find', 'Task', [['color', 'is', *operands]], ['id'])


@pytest.mark.anyio
async def test_other_entity_color_field_remains_unsettled(workflow_site: FakeSiteCase) -> None:
    schema = workflow_site.client._server.schema.fields
    schema['Asset']['color'] = deepcopy(schema['Task']['color'])
    with pytest.raises(mockgun.MockgunError, match=r'Asset\.color'):
        await workflow_site.call('find', 'Asset', [['color', 'is', 'pipeline_step']], ['id'])


@pytest.mark.anyio
@pytest.mark.parametrize('form', ['no_project', 'asset', 'start_date', 'sudo'])
async def test_unobserved_task_create_keeps_computed_fields_unknown(
    workflow_site: FakeSiteCase, form: str
) -> None:
    case = workflow_site
    project = cast('dict[str, object]', await case.call('create', 'Project', {'name': 'P'}))
    asset = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'A'}))
    fields: dict[str, object] = {'project': project, 'content': 't'}
    if form == 'no_project':
        fields.pop('project')
    elif form == 'asset':
        fields['entity'] = asset
    elif form == 'start_date':
        fields['start_date'] = '2026-07-20'
    else:
        await case.call('create', 'HumanUser', {'login': 'artist', 'name': 'artist'})
        case = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    task = cast('dict[str, object]', await case.call('create', 'Task', fields))
    with pytest.raises(mockgun.MockgunError, match=r'Task\.color'):
        await _fields(case, task, 'color')
    with pytest.raises(mockgun.MockgunError, match=r'Task\.due_date'):
        await case.call('find', 'Task', [['id', 'is', task['id']], ['due_date', 'is', None]])


@pytest.mark.anyio
async def test_task_without_content_keeps_content_unknown(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = cast('dict[str, object]', await case.call('create', 'Project', {'name': 'P'}))
    task = cast('dict[str, object]', await case.call('create', 'Task', {'project': project}))
    with pytest.raises(mockgun.MockgunError, match=r'Task\.content'):
        await _fields(case, task, 'content')


@pytest.mark.anyio
async def test_note_reply_content_fails_before_create(effects_site: FakeSiteCase) -> None:
    with pytest.raises(mockgun.MockgunError, match=r'Note\.reply_content'):
        await effects_site.call('create', 'Note', {'subject': 'n3', 'reply_content': 'r'})
    assert await effects_site.call('find', 'Note', [['subject', 'is', 'n3']]) == []


@pytest.mark.anyio
async def test_summary_update_is_read_only(effects_site: FakeSiteCase) -> None:
    shot = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's1'}))
    before = await _fields(effects_site, shot, 'open_notes_count', 'updated_at')
    effects_site.client.site.set_time(_LATER)
    with pytest.raises(Fault, match=r'Shot\.open_notes_count is read only'):
        await effects_site.call('update', 'Shot', shot['id'], {'open_notes_count': 3})
    response = raw_call(
        effects_site.client._server,
        'update',
        {
            'type': 'Shot',
            'id': shot['id'],
            'fields': [{'field_name': 'open_notes_count', 'value': 3}],
            'return_fields': [],
        },
    )
    assert response['exception'] is True
    assert response['error_code'] == 103
    assert (
        response['message'] == 'API update() Shot.open_notes_count is read only:\n'
        '{"field_name" => "open_notes_count", "value" => 3}'
    )
    assert await _fields(effects_site, shot, 'open_notes_count', 'updated_at') == before
    with pytest.raises(mockgun.MockgunError, match=r'Shot\.open_notes_count'):
        await effects_site.call('create', 'Shot', {'code': 'x', 'open_notes_count': 3})


@pytest.mark.anyio
@pytest.mark.parametrize('replace', [False, True])
async def test_version_task_inverse_rejects_tainted_old_task_atomically(
    effects_site: FakeSiteCase, replace: bool
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Archived', 'archived': True})
    old_task = seed(effects_site, 'Task', {'content': 'old', 'project': project})
    new_task = seed(effects_site, 'Task', {'content': 'new'})
    version = seed(effects_site, 'Version', {'code': 'version', 'sg_task': old_task})
    store = effects_site.client._server.store
    await effects_site.call('delete', 'Project', project['id'])
    assert store.tainted(store.require('Task', entity_id(old_task))) is not None
    assert store.tainted(store.require('Version', entity_id(version))) is None
    before = store.snapshot()
    target = new_task if replace else None
    with pytest.raises(mockgun.MockgunError, match=rf'Task {old_task["id"]}.*Project'):
        await effects_site.call('update', 'Version', version['id'], {'sg_task': target})
    assert store.snapshot() == before

    assert await effects_site.call('revive', 'Project', project['id']) is True
    await effects_site.call('update', 'Version', version['id'], {'sg_task': target})
    assert _links((await _fields(effects_site, version, 'sg_task'))['sg_task']) == target
    with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
        await _fields(effects_site, old_task, 'sg_versions')
    if replace:
        with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
            await _fields(effects_site, new_task, 'sg_versions')
    else:
        assert (await _fields(effects_site, new_task, 'sg_versions'))['sg_versions'] == []


@pytest.mark.anyio
async def test_version_task_inverse_becomes_unknown(effects_site: FakeSiteCase) -> None:
    first = cast('dict[str, object]', await effects_site.call('create', 'Task', {'content': 't5'}))
    second = cast(
        'dict[str, object]', await effects_site.call('create', 'Task', {'content': 't6'})
    )
    version = cast(
        'dict[str, object]', await effects_site.call('create', 'Version', {'code': 'v3'})
    )
    await effects_site.call('update', 'Version', version['id'], {'sg_task': first})
    assert (await _fields(effects_site, first, 'sg_versions'))['sg_versions'] == [
        {'type': 'Version', 'id': version['id'], 'name': 'v3'}
    ]
    with pytest.raises(mockgun.MockgunError, match=r'Task\.updated_at'):
        await _fields(effects_site, first, 'updated_at')
    assert (await _fields(effects_site, second, 'sg_versions'))['sg_versions'] == []
    await effects_site.call('update', 'Version', version['id'], {'sg_task': second})
    for task in (first, second):
        with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
            await _fields(effects_site, task, 'sg_versions')
    await effects_site.call('update', 'Task', first['id'], {'sg_versions': []})
    assert (await _fields(effects_site, first, 'sg_versions'))['sg_versions'] == []
    with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
        await effects_site.call(
            'update', 'Task', second['id'], {'sg_versions': [version]}, {'sg_versions': 'add'}
        )
    with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
        await _fields(effects_site, second, 'sg_versions')
    assert _links((await _fields(effects_site, version, 'sg_task'))['sg_task']) == _links(second)


@pytest.mark.anyio
async def test_version_create_with_task_keeps_inverse_unknown(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    task = cast('dict[str, object]', await case.call('create', 'Task', {'content': 't'}))
    await case.call('create', 'Version', {'code': 'v', 'sg_task': task})
    with pytest.raises(mockgun.MockgunError, match=r'Task\.sg_versions'):
        await _fields(case, task, 'sg_versions')


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['read', 'filter'])
async def test_deep_task_unknown_fields_raise(effects_site: FakeSiteCase, operation: str) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {'content': 't1'}))
    version = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Version', {'code': 'v1', 'sg_task': task}),
    )
    field = 'color' if operation == 'read' else 'due_date'
    action = (
        _fields(effects_site, version, 'sg_task.Task.color')
        if operation == 'read'
        else effects_site.call('find', 'Version', [['sg_task.Task.due_date', 'is', None]])
    )
    with pytest.raises(mockgun.MockgunError, match=rf'Task\.{field}'):
        await action


@pytest.mark.anyio
async def test_failed_batch_restores_unknown_markers(effects_site: FakeSiteCase) -> None:
    requests: list[dict[str, object]] = [
        {'request_type': 'create', 'entity_type': 'Task', 'data': {}},
        {
            'request_type': 'update',
            'entity_type': 'Task',
            'entity_id': 9999,
            'data': {'content': 'missing'},
        },
    ]
    with pytest.raises(Fault):
        await effects_site.call('batch', requests)
    assert await effects_site.call('find', 'Task', [], ['content']) == []
    task = seed(effects_site, 'Task', {})
    assert (await _fields(effects_site, task, 'content'))['content'] is None


@pytest.mark.anyio
async def test_partial_schema_marks_only_declared_unknown_fields(query_site: FakeSiteCase) -> None:
    task = cast('dict[str, object]', await query_site.call('create', 'Task', {}))
    with pytest.raises(mockgun.MockgunError, match=r'Task\.content'):
        await _fields(query_site, task, 'content')
    known = cast(
        'dict[str, object]', await query_site.call('create', 'Task', {'content': 'known'})
    )
    assert (await _fields(query_site, known, 'content'))['content'] == 'known'
    version = cast(
        'dict[str, object]',
        await query_site.call('create', 'Version', {'code': 'v', 'sg_task': known}),
    )
    assert cast('dict[str, object]', version['sg_task'])['name'] == 'known'


@pytest.mark.anyio
async def test_unknown_order_preserves_membership_and_clears_on_write(
    effects_site: FakeSiteCase,
) -> None:
    shot = seed(effects_site, 'Shot', {'code': 's'})
    asset = seed(effects_site, 'Asset', {'code': 'a', 'shots': [shot]})
    seed(effects_site, 'Asset', {'code': 'b'})
    store = effects_site.client._server.store
    row = store.require('Asset', entity_id(asset))
    store.mark_unknown(row, 'shots', 'member order unobserved', order_only=True)
    assert _links(await effects_site.call('find', 'Asset', [['shots', 'is', shot]], ['id'])) == [
        asset
    ]
    with pytest.raises(mockgun.MockgunError, match=r'Asset\.shots.*member order unobserved'):
        await _fields(effects_site, asset, 'shots')
    with pytest.raises(mockgun.MockgunError, match=r'Asset\.shots'):
        await effects_site.call(
            'find', 'Asset', [], order=[{'field_name': 'shots', 'direction': 'asc'}]
        )
    snapshot = store.snapshot()
    store.forget_unknown(row, 'shots')
    store.restore(snapshot)
    with pytest.raises(mockgun.MockgunError, match=r'Asset\.shots'):
        await _fields(effects_site, asset, 'shots')
    await effects_site.call('update', 'Asset', asset['id'], {'shots': [shot]})
    assert _links((await _fields(effects_site, asset, 'shots'))['shots']) == [shot]


@pytest.mark.anyio
async def test_unknown_link_update_fails_before_mutation(effects_site: FakeSiteCase) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {}))
    version = seed(effects_site, 'Version', {'code': 'v'})
    before = await _fields(effects_site, version, 'sg_task', 'updated_at')
    effects_site.client.site.set_time(_LATER)
    with pytest.raises(mockgun.MockgunError, match=r'Task\.content'):
        await effects_site.call('update', 'Version', version['id'], {'sg_task': task})
    assert await _fields(effects_site, version, 'sg_task', 'updated_at') == before
    assert (await _fields(effects_site, task, 'sg_versions'))['sg_versions'] == []


@pytest.mark.anyio
async def test_unknown_audits_clear_after_a_known_change(effects_site: FakeSiteCase) -> None:
    task = cast('dict[str, object]', await effects_site.call('create', 'Task', {}))
    effects_site.client.site.set_time(_LATER)
    await effects_site.call('update', 'Task', task['id'], {'content': 'x'})
    with pytest.raises(mockgun.MockgunError, match=r'Task\.updated_by'):
        await _fields(effects_site, task, 'updated_by')
    await effects_site.call('update', 'Task', task['id'], {'content': 'y'})
    actual = await _fields(effects_site, task, 'updated_at', 'updated_by')
    assert actual['updated_at'] == _LATER
    assert _links(actual['updated_by']) == _SCRIPT


@pytest.mark.anyio
async def test_connection_shot_asset_add_remove_and_both_writers(
    effects_site: FakeSiteCase,
) -> None:
    shot = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's1'}))
    a = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a'}))
    b = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'b'}))
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [a, b]}, {'assets': 'add'})
    rows = cast(
        'list[dict[str, object]]',
        await effects_site.call(
            'find', 'AssetShotConnection', [['shot', 'is', shot]], ['asset', 'shot']
        ),
    )
    assert len(rows) == 2
    assert {cast('dict[str, object]', row['asset'])['id'] for row in rows} == {a['id'], b['id']}
    assert all(cast('dict[str, object]', row['shot'])['name'] == 's1' for row in rows)
    assert {cast('dict[str, object]', row['asset'])['name'] for row in rows} == {'a', 'b'}
    removed = next(row for row in rows if cast('dict[str, object]', row['asset'])['id'] == a['id'])
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [a]}, {'assets': 'remove'})
    assert (
        await effects_site.call('find', 'AssetShotConnection', [['id', 'is', removed['id']]]) == []
    )
    assert (
        await effects_site.call(
            'find', 'AssetShotConnection', [['id', 'is', removed['id']]], retired_only=True
        )
        == []
    )
    remaining = cast(
        'list[dict[str, object]]',
        await effects_site.call('find', 'AssetShotConnection', [['shot', 'is', shot]], ['asset']),
    )
    assert len(remaining) == 1
    assert _links(remaining[0]['asset']) == _links(b)
    s2 = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's2'}))
    a2 = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a2'}))
    await effects_site.call('update', 'Asset', a2['id'], {'shots': [s2]})
    assert (
        len(
            cast(
                'list[object]',
                await effects_site.call('find', 'AssetShotConnection', [['asset', 'is', a2]]),
            )
        )
        == 1
    )
    a3 = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a3'}))
    s3 = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Shot', {'code': 's3', 'assets': [a3]}),
    )
    assert (
        len(
            cast(
                'list[object]',
                await effects_site.call('find', 'AssetShotConnection', [['shot', 'is', s3]]),
            )
        )
        == 1
    )


@pytest.mark.anyio
async def test_connection_asset_hierarchy_and_playlist_both_writers(
    effects_site: FakeSiteCase,
) -> None:
    parents = [
        cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': f'p{i}'}))
        for i in range(3)
    ]
    child = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Asset', {'code': 'c', 'parents': parents[:2]}),
    )
    rows = cast(
        'list[dict[str, object]]',
        await effects_site.call(
            'find', 'AssetAssetConnection', [['asset', 'is', child]], ['asset', 'parent']
        ),
    )
    assert len(rows) == 2
    assert {cast('dict[str, object]', _links(row['parent']))['id'] for row in rows} == {
        parents[0]['id'],
        parents[1]['id'],
    }
    assert all(_links(row['asset']) == _links(child) for row in rows)
    await effects_site.call(
        'update', 'Asset', parents[2]['id'], {'assets': [child]}, {'assets': 'add'}
    )
    assert (
        len(
            cast(
                'list[object]',
                await effects_site.call('find', 'AssetAssetConnection', [['asset', 'is', child]]),
            )
        )
        == 3
    )
    versions = [
        cast('dict[str, object]', await effects_site.call('create', 'Version', {'code': f'v{i}'}))
        for i in range(3)
    ]
    playlist = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Playlist', {'code': 'pl', 'versions': versions[:2]}),
    )
    rows = cast(
        'list[dict[str, object]]',
        await effects_site.call(
            'find',
            'PlaylistVersionConnection',
            [['playlist', 'is', playlist]],
            ['playlist', 'version'],
        ),
    )
    assert len(rows) == 2
    assert {cast('dict[str, object]', _links(row['version']))['id'] for row in rows} == {
        versions[0]['id'],
        versions[1]['id'],
    }
    await effects_site.call(
        'update', 'Version', versions[2]['id'], {'playlists': [playlist]}, {'playlists': 'add'}
    )
    assert (
        len(
            cast(
                'list[object]',
                await effects_site.call(
                    'find', 'PlaylistVersionConnection', [['playlist', 'is', playlist]]
                ),
            )
        )
        == 3
    )


@pytest.mark.anyio
async def test_connection_read_rejects_unobserved_fields_and_types(
    effects_site: FakeSiteCase,
) -> None:
    with pytest.raises(mockgun.MockgunError, match='AssetShotConnection'):
        await effects_site.call('find', 'AssetShotConnection', [], ['created_at'])
    with pytest.raises(mockgun.MockgunError, match='AssetShotConnection'):
        await effects_site.call('find', 'AssetShotConnection', [['created_at', 'is', None]])
    with pytest.raises(mockgun.MockgunError, match='AssetShotConnection'):
        await effects_site.call(
            'find',
            'AssetShotConnection',
            [],
            order=[{'field_name': 'created_at', 'direction': 'asc'}],
        )
    with pytest.raises(mockgun.MockgunError, match='AssetSequenceConnection'):
        await effects_site.call('find', 'AssetSequenceConnection', [])


@pytest.mark.anyio
async def test_connection_taint_filter_and_revival(effects_site: FakeSiteCase) -> None:
    a = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a'}))
    b = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'b'}))
    shot = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Shot', {'code': 's', 'assets': [a, b]}),
    )
    await effects_site.call('delete', 'Asset', a['id'])
    with pytest.raises(mockgun.MockgunError, match='AssetShotConnection'):
        await effects_site.call('find', 'AssetShotConnection', [])
    rows = cast(
        'list[dict[str, object]]',
        await effects_site.call('find', 'AssetShotConnection', [['asset', 'is', b]], ['asset']),
    )
    assert len(rows) == 1
    assert _links(rows[0]['asset']) == _links(b)
    await effects_site.call('revive', 'Asset', a['id'])
    assert (
        len(
            cast(
                'list[object]',
                await effects_site.call('find', 'AssetShotConnection', [['shot', 'is', shot]]),
            )
        )
        == 2
    )


@pytest.mark.anyio
async def test_connection_tainted_remove_is_atomic_then_succeeds(
    effects_site: FakeSiteCase,
) -> None:
    s6 = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's6'}))
    s7 = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's7'}))
    a3 = cast(
        'dict[str, object]',
        await effects_site.call('create', 'Asset', {'code': 'a3', 'shots': [s6, s7]}),
    )
    rows = cast(
        'list[dict[str, object]]',
        await effects_site.call('find', 'AssetShotConnection', [['asset', 'is', a3]], ['shot']),
    )
    assert len(rows) == 2
    removed = next(row for row in rows if _links(row['shot']) == _links(s6))
    await effects_site.call('delete', 'Shot', s6['id'])
    before = effects_site.client._server.store.snapshot()
    with pytest.raises(mockgun.MockgunError, match=rf'AssetShotConnection.*{removed["id"]}.*Shot'):
        await effects_site.call('update', 'Asset', a3['id'], {'shots': [s6]}, {'shots': 'remove'})
    assert effects_site.client._server.store.snapshot() == before
    await effects_site.call('revive', 'Shot', s6['id'])
    await effects_site.call('update', 'Asset', a3['id'], {'shots': [s6]}, {'shots': 'remove'})
    assert _links((await _fields(effects_site, a3, 'shots'))['shots']) == [_links(s7)]
    assert (
        await effects_site.call('find', 'AssetShotConnection', [['id', 'is', removed['id']]]) == []
    )
    assert (
        await effects_site.call(
            'find', 'AssetShotConnection', [['id', 'is', removed['id']]], retired_only=True
        )
        == []
    )


@pytest.mark.anyio
async def test_connection_absent_in_partial_schema_control(query_site: FakeSiteCase) -> None:
    s6 = cast('dict[str, object]', await query_site.call('create', 'Shot', {'code': 's6'}))
    s7 = cast('dict[str, object]', await query_site.call('create', 'Shot', {'code': 's7'}))
    a3 = cast(
        'dict[str, object]',
        await query_site.call('create', 'Asset', {'code': 'a3', 'shots': [s6, s7]}),
    )
    await query_site.call('delete', 'Shot', s6['id'])
    await query_site.call('update', 'Asset', a3['id'], {'shots': [s6]}, {'shots': 'remove'})
    assert _links((await _fields(query_site, a3, 'shots'))['shots']) == [_links(s7)]


@pytest.mark.anyio
async def test_connection_no_duplicate_missing_target_and_batch_rollback(
    effects_site: FakeSiteCase,
) -> None:
    shot = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's'}))
    asset = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a'}))
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [asset]}, {'assets': 'add'})
    rows = cast(
        'list[dict[str, object]]', await effects_site.call('find', 'AssetShotConnection', [])
    )
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [asset]}, {'assets': 'add'})
    assert await effects_site.call('find', 'AssetShotConnection', []) == rows
    missing = {'type': 'Asset', 'id': 99999}
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [missing]}, {'assets': 'add'})
    assert await effects_site.call('find', 'AssetShotConnection', []) == rows
    request = {
        'request_type': 'update',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'data': {
            'assets': [
                cast(
                    'dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'b'})
                )
            ]
        },
        'multi_entity_update_modes': {'assets': 'add'},
    }
    with pytest.raises(Fault):
        await effects_site.call(
            'batch',
            [
                request,
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'x'},
                },
            ],
        )
    assert await effects_site.call('find', 'AssetShotConnection', []) == rows


@pytest.mark.anyio
async def test_direct_connection_create_is_ordinary(effects_site: FakeSiteCase) -> None:
    shot = cast('dict[str, object]', await effects_site.call('create', 'Shot', {'code': 's'}))
    asset = cast('dict[str, object]', await effects_site.call('create', 'Asset', {'code': 'a'}))
    row = cast(
        'dict[str, object]',
        await effects_site.call('create', 'AssetShotConnection', {'asset': asset, 'shot': shot}),
    )
    assert _links((await _fields(effects_site, row, 'asset', 'shot'))['asset']) == _links(asset)
    assert (await _fields(effects_site, shot, 'assets'))['assets'] == []
