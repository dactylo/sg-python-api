from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id

from .conftest import QUERY_NOW, FakeSiteCase, raw_call, seed, write_upload

if TYPE_CHECKING:
    from shotgun_api3.lib.mockgun._store import Row

_CREDENTIAL = 'x'
pytestmark = pytest.mark.anyio


async def _create_record(
    case: FakeSiteCase, kind: str, data: dict[str, object]
) -> dict[str, object]:
    return cast('dict[str, object]', await case.call('create', kind, data))


async def _history(
    case: FakeSiteCase,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    project = seed(case, 'Project', {'name': 'P'})
    await _create_record(case, 'Shot', {'project': project, 'code': 's0'})
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=2))
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's1'})
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=4))
    await case.call('update', 'Shot', shot['id'], {'sg_status_list': 'ip'})
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=6))
    note = await _create_record(
        case, 'Note', {'project': project, 'subject': 'n', 'content': 'b', 'note_links': [shot]}
    )
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=8))
    await _create_record(case, 'Reply', {'entity': note, 'content': 'r'})
    return project, shot, note


async def _settled_note(case: FakeSiteCase, data: dict[str, object]) -> dict[str, object]:
    note = await _create_record(case, 'Note', data)
    events = cast(
        'list[dict[str, object]]',
        await case.call('find', 'EventLogEntry', [], ['event_type', 'attribute_name', 'user']),
    )
    assert events
    assert any(event['event_type'] == 'Shotgun_Note_New' for event in events)
    return note


async def _stream(
    case: FakeSiteCase, row: dict[str, object], **options: object
) -> dict[str, object]:
    return cast(
        'dict[str, object]',
        await case.call('activity_stream_read', row['type'], row['id'], **options),
    )


def _updates(response: dict[str, object]) -> list[dict[str, object]]:
    return cast('list[dict[str, object]]', response['updates'])


def _ids(response: dict[str, object]) -> list[object]:
    return [row['id'] for row in _updates(response)]


async def test_create_status_note_and_reply(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    await _create_record(case, 'Shot', {'project': project, 'code': 's0'})
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=2))
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's1'})
    assert await _stream(case, shot) == {
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'latest_update_id': 2,
        'earliest_update_id': 0,
        'updates': [
            {
                'id': 2,
                'update_type': 'create',
                'meta': {'type': 'new_entity', 'entity_type': 'Shot', 'entity_id': shot['id']},
                'created_at': QUERY_NOW + timedelta(seconds=2),
                'read': False,
                'primary_entity': {'type': 'Shot', 'id': shot['id'], 'name': 's1', 'status': None},
                'created_by': {
                    'type': 'ApiUser',
                    'id': 1,
                    'name': 'queries',
                    'status': None,
                    'image': None,
                },
            }
        ],
    }
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=4))
    await case.call('update', 'Shot', shot['id'], {'sg_status_list': 'ip'})
    stream = await _stream(case, shot)
    assert _ids(stream) == [3, 2]
    assert _updates(stream)[0]['meta'] == {
        'type': 'attribute_change',
        'attribute_name': 'sg_status_list',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'field_data_type': 'status_list',
        'old_value': None,
        'new_value': 'ip',
    }
    assert cast('dict[str, object]', _updates(stream)[1]['primary_entity'])['status'] == 'ip'
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=6))
    note = await _create_record(
        case, 'Note', {'project': project, 'subject': 'n', 'content': 'b', 'note_links': [shot]}
    )
    assert _ids(await _stream(case, shot)) == [4, 3, 2]
    assert _ids(await _stream(case, note)) == [4]
    assert _updates(await _stream(case, note))[0]['primary_entity'] == {
        'type': 'Note',
        'id': note['id'],
        'name': 'n - b',
        'status': 'opn',
    }
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=8))
    reply = await _create_record(case, 'Reply', {'entity': note, 'content': 'r'})
    stream = await _stream(case, shot)
    assert _ids(stream) == [5, 3, 2]
    assert _updates(stream)[0]['meta'] == {
        'type': 'new_entity',
        'entity_type': 'Reply',
        'entity_id': reply['id'],
        'content': 'r',
    }
    assert _updates(stream)[0]['update_type'] == 'create_reply'
    assert _ids(await _stream(case, note)) == [5]
    assert stream['latest_update_id'] == 5
    assert stream['earliest_update_id'] == 0


async def test_human_creator_keeps_unknown_write_status(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'ARTIST', 'sg_status_list': 'act'})
    store = case.client._server.store
    row = store.require('HumanUser', entity_id(human))
    store.mark_unknown(row, 'sg_status_list', 'unsettled at write')
    actor = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    try:
        note = await _settled_note(
            actor, {'note_links': [shot], 'subject': 's', 'project': project}
        )
    finally:
        await actor.call('close')
    store.forget_unknown(row, 'sg_status_list')
    with pytest.raises(MockgunError, match='HumanUser'):
        await _stream(case, note)


@pytest.mark.parametrize('session', ['sudo', 'human'])
async def test_human_creator_renders_after_settled_source(
    workflow_site: FakeSiteCase, session: str
) -> None:
    case = workflow_site
    if session == 'human':
        fields = case.client._server.schema.fields['Note']
        fields.pop('user')
        fields['sg_status_list'] = {
            'data_type': {'value': 'status_list'},
            'properties': {'default_value': {'value': None}},
        }
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'ARTIST', 'sg_status_list': 'act'})
    client = (
        case.client.new_client(sudo_as_login='artist')
        if session == 'sudo'
        else case.client.new_client(login='artist', password=_CREDENTIAL)
    )
    actor = FakeSiteCase(case.kind, client)
    try:
        data: dict[str, object] = (
            {'note_links': [shot], 'subject': 's', 'project': project}
            if session == 'sudo'
            else {'subject': 's', 'project': project}
        )
        note = await _settled_note(actor, data)
        for target in [note, shot] if session == 'sudo' else [note]:
            entry = _updates(await _stream(case, target))[0]
            assert entry['update_type'] == 'create'
            assert entry['created_by'] == {
                **human,
                'name': 'ARTIST',
                'status': 'act',
                'image': None,
            }
            meta = cast('dict[str, object]', entry['meta'])
            if session == 'sudo':
                actual = cast('dict[str, object]', meta['sudo_actual_user'])
                assert set(actual) == {'type', 'id', 'name', 'valid', 'uuid'}
                assert {key: actual[key] for key in ('type', 'id', 'name', 'valid')} == {
                    'type': 'ApiUser',
                    'id': 1,
                    'name': 'queries',
                    'valid': 'valid',
                }
            else:
                assert 'sudo_actual_user' not in meta
    finally:
        await actor.call('close')


async def test_human_creator_status_and_entry_roll_back(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'ARTIST', 'sg_status_list': 'act'})
    actor = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    try:
        note = await _settled_note(
            actor, {'note_links': [shot], 'subject': 's', 'project': project}
        )
        before = await _stream(case, shot)
        entries = case.client._server.store.side_items('activity')
        with pytest.raises(Fault):
            await actor.call(
                'batch',
                [
                    {
                        'request_type': 'update',
                        'entity_type': 'HumanUser',
                        'entity_id': human['id'],
                        'data': {'sg_status_list': 'dis'},
                    },
                    {
                        'request_type': 'create',
                        'entity_type': 'Note',
                        'data': {'note_links': [shot], 'subject': 'rollback', 'project': project},
                    },
                    {
                        'request_type': 'update',
                        'entity_type': 'Shot',
                        'entity_id': 99999,
                        'data': {'code': 'missing'},
                    },
                ],
            )
        assert case.client._server.store.side_items('activity') == entries
        assert await case.call(
            'find_one', 'HumanUser', [['id', 'is', human['id']]], ['sg_status_list']
        ) == {**human, 'sg_status_list': 'act'}
        assert await _stream(case, shot) == before
        assert (
            _updates(await _stream(case, note))[0]['created_by']
            == _updates(before)[0]['created_by']
        )
    finally:
        await actor.call('close')


@pytest.mark.parametrize(
    'condition',
    ['image', 'status_changed', 'undeclared_status', 'unknown_status', 'missing', 'absent'],
)
async def test_human_creator_unsettled_guards(workflow_site: FakeSiteCase, condition: str) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'ARTIST', 'sg_status_list': 'act'})
    actor = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    try:
        note = await _settled_note(
            actor, {'note_links': [shot], 'subject': 's', 'project': project}
        )
    finally:
        await actor.call('close')
    store = case.client._server.store
    row = store.require('HumanUser', entity_id(human))
    if condition == 'image':
        row['image'] = 'https://mockgun.example/image'
    elif condition == 'status_changed':
        await case.call('update', 'HumanUser', human['id'], {'sg_status_list': 'dis'})
    elif condition == 'undeclared_status':
        store.schema.fields['HumanUser'].pop('sg_status_list')
    elif condition == 'unknown_status':
        store.mark_unknown(row, 'sg_status_list', 'unsettled test status')
    else:
        key, value = store.side_items('activity')[-1]
        entry = cast('Row', value)
        entry['actor'] = ['HumanUser', 99999] if condition == 'missing' else []
        store.side_set('activity', key, entry)
    with pytest.raises(MockgunError, match=r'activity_stream Note .*entry .*(HumanUser|absent)'):
        await _stream(case, note)


async def test_human_entry_guard_is_view_local(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's4'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    case.client._server.store.require('HumanUser', entity_id(human))['image'] = (
        'https://mockgun.example/image'
    )
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    note = await _create_record(
        sudo, 'Note', {'note_links': [shot], 'subject': 's', 'project': project}
    )
    with pytest.raises(MockgunError, match=r'HumanUser.*image'):
        await _stream(case, shot)
    await _create_record(case, 'Reply', {'entity': note, 'content': 'r'})
    stream = await _stream(case, shot)
    assert _updates(stream)[0]['update_type'] == 'create_reply'
    with pytest.raises(MockgunError, match=r'HumanUser.*image'):
        await _stream(case, shot, max_id=_updates(stream)[0]['id'])
    await sudo.call('close')


async def test_lifecycle_and_rollback(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, shot, note = await _history(case)
    await case.call('delete', 'Shot', shot['id'])
    stream = await _stream(case, shot)
    assert _ids(stream) == []
    assert stream['latest_update_id'] == 5
    assert stream['earliest_update_id'] == 0
    assert _ids(await _stream(case, note)) == []
    await case.call('revive', 'Shot', shot['id'])
    assert _ids(await _stream(case, shot)) == [5, 3, 2]
    second = await _create_record(case, 'Shot', {'project': project, 'code': 's2'})
    assert _ids(await _stream(case, second)) == [6]
    with pytest.raises(Fault):
        await case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {'project': project, 'code': 'rollback'},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'missing'},
                },
            ],
        )
    assert _ids(await _stream(case, project)) == [6, 5, 3, 2, 1]
    third = await _create_record(case, 'Shot', {'project': project, 'code': 's3'})
    assert cast('int', _ids(await _stream(case, third))[0]) > 6


async def test_linked_records_outside_create_audience_are_tainted(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    task = await _create_record(case, 'Task', {'project': project, 'content': 't', 'entity': shot})
    await _create_record(
        case, 'Note', {'project': project, 'subject': 'n', 'content': 'b', 'tasks': [task]}
    )
    with pytest.raises(MockgunError, match='linked activity'):
        await _stream(case, task)


async def test_missing_unreadable_empty_and_invalid_views(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    version = seed(case, 'Version', {'code': 'v'})
    with pytest.raises(MockgunError, match='Version'):
        await _stream(case, version)
    with pytest.raises(MockgunError, match='Shot'):
        await _stream(case, {'type': 'Shot', 'id': 9999})
    empty = seed(case, 'Shot', {'code': 'empty'})
    with pytest.raises(MockgunError, match='candidates'):
        await _stream(case, empty)
    _, shot, _ = await _history(case)
    for options in [
        {'limit': 0},
        {'limit': True},
        {'min_id': 'x'},
        {'entity_fields': {'Shot': ['missing']}},
        {'entity_fields': {'Shot': ['project.Project.name']}},
    ]:
        with pytest.raises(MockgunError, match='activity_stream'):
            await _stream(case, shot, **options)


async def test_project_bounds_and_selected_fields(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, shot, _ = await _history(case)
    stream = await _stream(case, project, min_id=1)
    assert _ids(stream) == [5, 3, 2]
    assert stream['earliest_update_id'] == 2
    with pytest.raises(MockgunError, match='min_id'):
        await _stream(case, project, min_id=3)
    entries = _updates(
        await _stream(case, shot, entity_fields={'Shot': ['code', 'sg_status_list']})
    )
    assert cast('dict[str, object]', entries[1]['primary_entity'])['code'] == 's1'
    assert cast('dict[str, object]', entries[1]['primary_entity'])['sg_status_list'] == 'ip'
    assert list(cast('dict[str, object]', entries[0]['primary_entity'])) == [
        'type',
        'id',
        'name',
        'status',
    ]


async def test_project_create_taints_its_stream(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project = await _create_record(case, 'Project', {'name': 'created'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    assert _ids(await _stream(case, shot)) == [1]
    with pytest.raises(MockgunError, match='create Project'):
        await _stream(case, project)


@pytest.mark.parametrize(
    ('changes', 'fragment'),
    [
        ({'unexpected': None}, 'unexpected'),
        ({'type': 1}, 'type'),
        ({'id': None}, 'id'),
        ({'id': True}, 'id'),
        ({'max_id': False}, 'max_id'),
        ({'max_id': '2'}, 'max_id'),
        ({'entity_fields': []}, 'entity_fields'),
        ({'entity_fields': {'Missing': []}}, 'entity_fields Missing'),
        ({'entity_fields': {'Shot': 'code'}}, 'entity_fields Shot'),
        ({'entity_fields': {'Shot': [1]}}, 'Shot.1'),
    ],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_raw_activity_rejects_malformed_parameters(
    workflow_site: FakeSiteCase, changes: dict[str, object], fragment: str
) -> None:
    case = workflow_site
    shot = await _create_record(case, 'Shot', {'code': 's'})
    before = await _stream(case, shot)
    params: dict[str, object] = {
        'type': 'Shot',
        'id': shot['id'],
        'min_id': None,
        'max_id': None,
        'limit': None,
        'entity_fields': {},
        **changes,
    }
    with pytest.raises(MockgunError, match=f'activity_stream.*{fragment}'):
        raw_call(case.client._server, 'activity_stream', params)
    assert await _stream(case, shot) == before


async def test_retired_primary_and_project_audience_are_guarded(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project, shot, _ = await _history(case)
    await case.call('delete', 'Shot', shot['id'])
    with pytest.raises(MockgunError, match='retired'):
        await _stream(case, project)


async def test_settlement_is_computed_without_event_log(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    case.client._server.schema.fields.pop('EventLogEntry')
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    assert _ids(await _stream(case, shot)) == [1]
    unsettled = await _create_record(
        case, 'Shot', {'project': project, 'code': 'u', 'description': 'd'}
    )
    with pytest.raises(MockgunError, match='source event rows'):
        await _stream(case, unsettled)


async def test_silent_shot_asset_link_change_does_not_taint_asset(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    asset = await _create_record(case, 'Asset', {'project': project, 'code': 'a'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    before = await _stream(case, asset)
    await case.call('update', 'Shot', shot['id'], {'assets': [asset]})
    assert await _stream(case, asset) == before
    await case.call('update', 'Shot', shot['id'], {'assets': []})
    assert await _stream(case, asset) == before


async def test_silent_writes_and_asset_status(workflow_site: FakeSiteCase, tmp_path: Path) -> None:
    case = workflow_site
    project, _, note = await _history(case)
    asset = await _create_record(case, 'Asset', {'project': project, 'code': 'a'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's3'})
    task = await _create_record(
        case, 'Task', {'project': project, 'content': 't3', 'entity': shot}
    )
    shot_ids = _ids(await _stream(case, shot))
    task_ids = _ids(await _stream(case, task))
    assert shot_ids[0] == task_ids[0]
    for field, value in {'code': 'changed', 'sg_cut_in': 10, 'sg_shot_type': 'Feature'}.items():
        await case.call('update', 'Shot', shot['id'], {field: value})
    for field, value in {
        'start_date': '2026-07-16',
        'est_in_mins': 10,
        'sg_status_list': 'ip',
    }.items():
        await case.call('update', 'Task', task['id'], {field: value})
    assert _ids(await _stream(case, shot)) == shot_ids
    assert _ids(await _stream(case, task)) == task_ids
    await case.call('update', 'Asset', asset['id'], {'sg_status_list': 'ip'})
    assert [row['update_type'] for row in _updates(await _stream(case, asset))] == [
        'update',
        'create',
    ]
    before = await _stream(case, note)
    await case.call('upload', 'Note', note['id'], write_upload(tmp_path, 'plain.txt', b'x'))
    assert await _stream(case, note) == before


async def test_status_with_asset_removal_taints_previous_audience(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'shot project'})
    asset_project = seed(case, 'Project', {'name': 'asset project'})
    asset = await _create_record(case, 'Asset', {'project': asset_project, 'code': 'a'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    before = await _stream(case, asset)
    await case.call('update', 'Shot', shot['id'], {'assets': [asset]})
    assert await _stream(case, asset) == before
    await case.call('update', 'Shot', shot['id'], {'assets': [], 'sg_status_list': 'ip'})
    for affected in [asset, asset_project]:
        with pytest.raises(MockgunError, match='linked activity'):
            await _stream(case, affected)


async def test_taints_include_old_and_new_links_and_rollback(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, shot, note = await _history(case)
    other_project = seed(case, 'Project', {'name': 'other'})
    other = await _create_record(case, 'Shot', {'project': other_project, 'code': 'other'})
    before = await _stream(case, shot)
    with pytest.raises(Fault):
        await case.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Note',
                    'entity_id': note['id'],
                    'data': {'note_links': [other]},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'bad'},
                },
            ],
        )
    assert await _stream(case, shot) == before
    assert _ids(await _stream(case, other)) == [6]
    await case.call('update', 'Note', note['id'], {'note_links': [other]})
    for row in [note, shot, other, project, other_project]:
        with pytest.raises(MockgunError, match='note_links'):
            await _stream(case, row)


async def test_unsettled_entry_outside_view_does_not_poison_later_entry(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(
        case, 'Shot', {'project': project, 'code': 's', 'description': 'd'}
    )
    with pytest.raises(MockgunError, match='source event rows'):
        await _stream(case, shot)
    await case.call('update', 'Shot', shot['id'], {'sg_status_list': 'ip'})
    assert _ids(await _stream(case, shot, limit=1)) == [2]
    with pytest.raises(MockgunError, match='source event rows'):
        await _stream(case, shot)


@pytest.mark.parametrize(
    ('options', 'ids', 'latest', 'earliest'),
    [
        ({'limit': 1}, [5], 5, 5),
        ({'limit': 2}, [5, 3], 5, 3),
        ({'min_id': 2}, [5, 3], 5, 3),
        ({'max_id': 5}, [4, 3, 2], 4, 0),
    ],
)
async def test_views(
    workflow_site: FakeSiteCase,
    options: dict[str, object],
    ids: list[int],
    latest: int,
    earliest: int,
) -> None:
    _, shot, _ = await _history(workflow_site)
    stream = await _stream(workflow_site, shot, **options)
    assert _ids(stream) == ids
    assert stream['latest_update_id'] == latest
    assert stream['earliest_update_id'] == earliest


@pytest.mark.parametrize(
    'write', ['version', 'subject', 'delete_asset', 'thumbnail', 'description']
)
async def test_unsettled_writes_taint_affected_streams(
    workflow_site: FakeSiteCase, tmp_path: Path, write: str
) -> None:
    case = workflow_site
    project, shot, note = await _history(case)
    if write == 'version':
        await _create_record(case, 'Version', {'project': project, 'code': 'v', 'entity': shot})
        affected, reason = [shot, project], 'Version'
    elif write == 'subject':
        await case.call('update', 'Note', note['id'], {'subject': 'x'})
        affected, reason = [note, shot], 'subject'
    elif write == 'description':
        new = await _create_record(
            case, 'Shot', {'project': project, 'code': 'x', 'description': 'd'}
        )
        affected, reason = [new], 'unsettled'
    else:
        asset = await _create_record(case, 'Asset', {'project': project, 'code': 'a'})
        if write == 'delete_asset':
            await case.call('delete', 'Asset', asset['id'])
            reason = 'retirement'
        else:
            await case.call(
                'upload_thumbnail', 'Asset', asset['id'], write_upload(tmp_path, 'a.jpg', b'image')
            )
            reason = 'image'
        affected = [asset, project]
    for row in affected:
        with pytest.raises(MockgunError, match=reason):
            await _stream(case, row)


@pytest.mark.parametrize('operation', ['share', 'clear', 'filmstrip', 'plain_upload'])
async def test_image_and_upload_taints_reach_links_and_projects(
    workflow_site: FakeSiteCase, tmp_path: Path, operation: str
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    note = await _create_record(
        case, 'Note', {'project': project, 'subject': 'n', 'content': 'b', 'note_links': [shot]}
    )
    asset = await _create_record(case, 'Asset', {'project': project, 'code': 'a'})
    if operation == 'share':
        source = seed(case, 'Asset', {'code': 'source'})
        await case.call(
            'upload_thumbnail', 'Asset', source['id'], write_upload(tmp_path, 's.jpg', b'image')
        )
        await case.call('share_thumbnail', [asset], source_entity=source)
        affected, reason = [asset, project], 'share_thumbnail'
    elif operation == 'clear':
        await case.call('update', 'Shot', shot['id'], {'image': None})
        affected, reason = [shot, project], 'image'
    elif operation == 'filmstrip':
        await case.call(
            'upload_filmstrip_thumbnail',
            'Asset',
            asset['id'],
            write_upload(tmp_path, 'f.jpg', b'image'),
        )
        affected, reason = [asset, project], 'filmstrip_image'
    else:
        reply = await _create_record(case, 'Reply', {'entity': note, 'content': 'r'})
        await case.call('upload', 'Reply', reply['id'], write_upload(tmp_path, 'r.txt', b'body'))
        affected, reason = [note, shot, project], 'upload'
    for row in affected:
        with pytest.raises(MockgunError, match=reason):
            await _stream(case, row)


@pytest.mark.parametrize('unknown', ['status', 'name', 'undeclared_status'])
async def test_current_primary_rendering_guards(workflow_site: FakeSiteCase, unknown: str) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': 'P'})
    shot = await _create_record(case, 'Shot', {'project': project, 'code': 's'})
    store = case.client._server.store
    row = store.require('Shot', entity_id(shot))
    if unknown == 'undeclared_status':
        store.schema.fields['Shot'].pop('sg_status_list')
    else:
        store.mark_unknown(
            row, 'sg_status_list' if unknown == 'status' else 'code', 'unsettled test value'
        )
    with pytest.raises(MockgunError, match='activity_stream'):
        await _stream(case, shot)
