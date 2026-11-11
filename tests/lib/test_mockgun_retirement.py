from datetime import datetime, timedelta
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id

from .conftest import QUERY_NOW, FakeSiteCase, raw_call, seed

_LATER = QUERY_NOW + timedelta(hours=1)
_LATEST = [{'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'}]


async def _create_record_from_fields(
    case: FakeSiteCase, kind: str, **fields: object
) -> dict[str, object]:
    row = cast('dict[str, object]', await case.call('create', kind, fields))
    return {'type': kind, 'id': row['id']}


async def _read(
    case: FakeSiteCase, row: dict[str, object], *fields: str, retired: bool = False
) -> dict[str, object]:
    return cast(
        'dict[str, object]',
        await case.call(
            'find_one', row['type'], [['id', 'is', row['id']]], list(fields), retired_only=retired
        ),
    )


async def _lifecycle(
    case: FakeSiteCase, method: str, row: dict[str, object], *, sudo: bool = False
) -> None:
    if sudo:
        result = raw_call(
            case.client._server,
            method,
            row,
            auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
        )
        assert result['results'] is True
    else:
        assert await case.call(method, row['type'], row['id']) is True


async def _parent_scene(
    case: FakeSiteCase, parent_type: str
) -> tuple[
    dict[str, object],
    tuple[dict[str, object], dict[str, object], dict[str, object]],
    list[tuple[dict[str, object], str, bool]],
    list[dict[str, object]],
]:
    ancestor = (
        await _create_record_from_fields(case, 'Asset', code='ancestor')
        if parent_type == 'Asset'
        else None
    )
    parent = await _create_record_from_fields(
        case,
        parent_type,
        code='parent',
        **({'parents': [ancestor]} if ancestor is not None else {}),
    )
    task = await _create_record_from_fields(case, 'Task', content='task', entity=parent)
    version = await _create_record_from_fields(case, 'Version', code='version', entity=parent)
    note = await _create_record_from_fields(case, 'Note', subject='note', note_links=[parent])
    holders: list[tuple[dict[str, object], str, bool]] = []
    if parent_type in {'Shot', 'Asset'}:
        holders.append(
            (
                await _create_record_from_fields(case, 'PublishedFile', code='pf', entity=parent),
                'entity',
                False,
            )
        )
    if parent_type == 'Asset':
        assert ancestor is not None
        holders += [
            (ancestor, 'assets', True),
            (
                await _create_record_from_fields(case, 'Shot', code='shot', assets=[parent]),
                'assets',
                True,
            ),
        ]
    if parent_type == 'Sequence':
        holders += [
            (
                await _create_record_from_fields(case, 'Shot', code='shot', sg_sequence=parent),
                'sg_sequence',
                False,
            ),
            (
                await _create_record_from_fields(case, 'Cut', code='cut', entity=parent),
                'entity',
                False,
            ),
        ]
    before = [await _read(case, holder, 'updated_at', 'updated_by') for holder, _, _ in holders]
    return parent, (task, version, note), holders, before


async def _touched_audit(
    case: FakeSiteCase,
    row: dict[str, object],
    actor: str,
    when: datetime,
    *,
    retired: bool = False,
) -> None:
    result = await _read(case, row, 'updated_by', 'updated_at', retired=retired)
    assert cast('dict[str, object]', result['updated_by'])['type'] == actor
    assert result['updated_at'] == when


@pytest.mark.anyio
async def test_shot_cascade_and_connection_holder_take_operation_time(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    shot = await _create_record_from_fields(case, 'Shot', code='s')
    version = await _create_record_from_fields(case, 'Version', code='v', entity=shot)
    task = await _create_record_from_fields(case, 'Task', content='t', entity=shot)
    asset = await _create_record_from_fields(case, 'Asset', code='a', shots=[shot])
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', shot)
    for row in (shot, version, task):
        await _touched_audit(case, row, 'ApiUser', _LATER, retired=True)
    await _touched_audit(case, asset, 'ApiUser', _LATER)
    later = _LATER + timedelta(hours=1)
    case.client.site.set_time(later)
    await _lifecycle(case, 'revive', shot)
    for row in (shot, version, task, asset):
        await _touched_audit(case, row, 'ApiUser', later)


@pytest.mark.parametrize('parent_type', ['Shot', 'Asset', 'Sequence'])
@pytest.mark.anyio
async def test_retire_parent_cascades_children_and_preserves_hidden_holder_audits(
    effects_site: FakeSiteCase, parent_type: str
) -> None:
    case = effects_site
    parent, (task, version, note), holders, before = await _parent_scene(case, parent_type)
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', parent, sudo=True)
    assert (await _read(case, parent, 'updated_at', retired=True))['updated_at'] == _LATER
    assert (
        cast(
            'dict[str, object]',
            (await _read(case, parent, 'updated_by', retired=True))['updated_by'],
        )['type']
        == 'HumanUser'
    )
    for child in (task, version, note):
        assert await case.call('find', child['type'], []) == []
        assert await case.call('find', child['type'], [], retired_only=True) == [child]
        await _touched_audit(case, child, 'HumanUser', _LATER, retired=True)
    assert (await _read(case, task, 'entity', retired=True))['entity'] is None
    assert (await _read(case, version, 'entity', retired=True))['entity'] is None
    assert (await _read(case, note, 'note_links', retired=True))['note_links'] == []
    assert await case.call(
        'find', 'Task', [['updated_at', 'greater_than', QUERY_NOW]], retired_only=True
    ) == [task]
    for (holder, field, touched), original in zip(holders, before, strict=True):
        assert (await _read(case, holder, field))[field] in (None, [])
        if touched:
            await _touched_audit(case, holder, 'HumanUser', _LATER)
        else:
            assert await _read(case, holder, 'updated_at', 'updated_by') == original


@pytest.mark.parametrize('parent_type', ['Shot', 'Asset', 'Sequence'])
@pytest.mark.anyio
async def test_revive_parent_restores_children_and_preserves_hidden_holder_audits(
    effects_site: FakeSiteCase, parent_type: str
) -> None:
    case = effects_site
    parent, (task, version, note), holders, before = await _parent_scene(case, parent_type)
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', parent, sudo=True)
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', parent)
    assert (await _read(case, parent, 'updated_at'))['updated_at'] == _LATER + timedelta(hours=1)
    assert (
        cast('dict[str, object]', (await _read(case, parent, 'updated_by'))['updated_by'])['type']
        == 'ApiUser'
    )
    expected_link = {**parent, 'name': 'parent'}
    assert (await _read(case, task, 'entity'))['entity'] == expected_link
    assert (await _read(case, version, 'entity'))['entity'] == expected_link
    assert (await _read(case, note, 'note_links'))['note_links'] == [expected_link]
    for child in (task, version, note):
        assert await case.call('find', child['type'], []) == [child]
        assert await case.call('find', child['type'], [], retired_only=True) == []
        assert await _read(case, child) == child
        await _touched_audit(case, child, 'ApiUser', _LATER + timedelta(hours=1))
    assert (
        cast('dict[str, object]', (await _read(case, version, 'entity'))['entity'])['id']
        == parent['id']
    )
    for (holder, field, touched), original in zip(holders, before, strict=True):
        declaration = cast(
            'dict[str, object]',
            case.client._server.schema.fields[cast('str', holder['type'])][field],
        )
        data_type = cast('dict[str, object]', declaration['data_type'])['value']
        assert (await _read(case, holder, field))[field] == (
            [expected_link] if data_type == 'multi_entity' else expected_link
        )
        if touched:
            await _touched_audit(case, holder, 'ApiUser', _LATER + timedelta(hours=1))
        else:
            assert await _read(case, holder, 'updated_at', 'updated_by') == original
    await case.call('update', 'Task', task['id'], {'content': 'changed'})
    assert (await _read(case, task, 'updated_at'))['updated_at'] == _LATER + timedelta(hours=1)


@pytest.mark.anyio
async def test_task_note_and_reply_cascades(effects_site: FakeSiteCase) -> None:
    case = effects_site
    task = await _create_record_from_fields(case, 'Task', content='task')
    note = await _create_record_from_fields(case, 'Note', subject='note', tasks=[task])
    version = seed(case, 'Version', {'code': 'version', 'sg_task': task})
    file = await _create_record_from_fields(case, 'PublishedFile', code='file', task=task)
    originals = [
        await _read(case, holder, 'updated_at', 'updated_by') for holder in (version, file)
    ]
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', task, sudo=True)
    assert await _read(case, note, retired=True) == note
    await _touched_audit(case, note, 'HumanUser', _LATER, retired=True)
    for holder, field, original in zip(
        (version, file), ('sg_task', 'task'), originals, strict=True
    ):
        assert (await _read(case, holder, field))[field] is None
        assert await _read(case, holder, 'updated_at', 'updated_by') == original
    await _lifecycle(case, 'revive', task)
    for holder, field in ((version, 'sg_task'), (file, 'task')):
        assert (
            cast('dict[str, object]', (await _read(case, holder, field))[field])['id']
            == task['id']
        )
    other = await _create_record_from_fields(case, 'Note', subject='other')
    reply = await _create_record_from_fields(case, 'Reply', content='reply', entity=other)
    await _lifecycle(case, 'delete', other)
    assert await _read(case, reply, retired=True) == reply
    await _lifecycle(case, 'revive', other)
    assert await _read(case, reply) == reply


@pytest.mark.anyio
async def test_shot_task_note_chain_and_two_member_note(effects_site: FakeSiteCase) -> None:
    case = effects_site
    sequence = await _create_record_from_fields(case, 'Sequence', code='sequence')
    shot = await _create_record_from_fields(case, 'Shot', code='shot', sg_sequence=sequence)
    task = await _create_record_from_fields(case, 'Task', content='task', entity=shot)
    note = await _create_record_from_fields(case, 'Note', subject='note', tasks=[task])
    asset = await _create_record_from_fields(case, 'Asset', code='asset')
    mixed = await _create_record_from_fields(
        case, 'Note', subject='mixed', note_links=[shot, asset]
    )
    originals = [await _read(case, row, 'updated_at', 'updated_by') for row in (sequence, mixed)]
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', shot, sudo=True)
    for child in (task, note):
        assert await _read(case, child, retired=True) == child
        await _touched_audit(case, child, 'HumanUser', _LATER, retired=True)
    assert (await _read(case, sequence, 'shots'))['shots'] == []
    assert [
        member['id']
        for member in cast(
            'list[dict[str, object]]', (await _read(case, mixed, 'note_links'))['note_links']
        )
    ] == [asset['id']]
    assert await case.call('find', 'Note', [['note_links', 'is', asset]]) == [mixed]
    assert await case.call('find', 'Sequence', [['shots', 'is', shot]]) == []
    for row, original in zip((sequence, mixed), originals, strict=True):
        assert await _read(case, row, 'updated_at', 'updated_by') == original
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', shot)
    for child in (task, note):
        await _touched_audit(case, child, 'ApiUser', _LATER + timedelta(hours=1))
    assert (await _read(case, sequence, 'shots'))['shots'] == [{**shot, 'name': 'shot'}]
    assert (await _read(case, mixed, 'note_links'))['note_links'] == [
        {**asset, 'name': 'asset'},
        {**shot, 'name': 'shot'},
    ]
    assert await case.call('find', 'Task', []) == [task]
    assert await case.call('find', 'Note', []) == [note, mixed]


@pytest.mark.parametrize(
    ('parent_type', 'holder_type', 'field', 'touched'),
    [
        ('Version', 'Playlist', 'versions', True),
        ('Shot', 'Asset', 'shots', True),
        ('Asset', 'Asset', 'parents', True),
        ('Note', 'Shot', 'notes', False),
        ('PublishedFile', 'Version', 'published_files', False),
        ('CutItem', 'Cut', 'cut_items', False),
        ('Playlist', 'Version', 'playlists', True),
        ('Version', 'PublishedFile', 'version', False),
    ],
)
@pytest.mark.anyio
async def test_hidden_holders_and_audits(
    effects_site: FakeSiteCase, parent_type: str, holder_type: str, field: str, touched: bool
) -> None:
    case = effects_site
    parent = await _create_record_from_fields(
        case, parent_type, **{'subject' if parent_type == 'Note' else 'code': 'parent'}
    )
    multi = field != 'version'
    holder = await _create_record_from_fields(
        case, holder_type, code='holder', **{field: [parent] if multi else parent}
    )
    original = await _read(case, holder, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', parent, sudo=True)
    assert (await _read(case, holder, field))[field] == ([] if multi else None)
    if touched:
        await _touched_audit(case, holder, 'HumanUser', _LATER)
    else:
        assert await _read(case, holder, 'updated_at', 'updated_by') == original
    await _lifecycle(case, 'revive', parent)
    assert (await _read(case, holder, field))[field]
    if touched:
        await _touched_audit(case, holder, 'ApiUser', _LATER)
    else:
        assert await _read(case, holder, 'updated_at', 'updated_by') == original


@pytest.mark.anyio
async def test_shot_children_revived_alone_hide_the_shot_until_it_returns(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    old = await _create_record_from_fields(case, 'Version', code='old', entity=shot)
    version = await _create_record_from_fields(case, 'Version', code='version', entity=shot)
    await _lifecycle(case, 'delete', old)
    await _lifecycle(case, 'delete', shot)
    await _lifecycle(case, 'revive', shot)
    assert await _read(case, old, retired=True) == old
    assert await _read(case, version) == version
    task = await _create_record_from_fields(case, 'Task', content='task', entity=shot)
    note = await _create_record_from_fields(case, 'Note', subject='note', note_links=[shot])
    await _lifecycle(case, 'delete', shot)
    case.client.site.set_time(_LATER)
    for child in (version, task, note):
        await _lifecycle(case, 'revive', child)
        await _touched_audit(case, child, 'ApiUser', _LATER)
    assert (await _read(case, version, 'entity'))['entity'] is None
    assert (await _read(case, task, 'entity'))['entity'] is None
    assert (await _read(case, note, 'note_links'))['note_links'] == []
    assert await case.call('find', 'Task', [['entity', 'is', shot]]) == []
    assert (await _read(case, shot, 'tasks', retired=True))['tasks'] == [{**task, 'name': 'task'}]
    assert (await _read(case, shot, 'notes', retired=True))['notes'] == []
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', shot)
    linked = {**shot, 'name': 'shot'}
    assert (await _read(case, version, 'entity'))['entity'] == linked
    assert (await _read(case, task, 'entity'))['entity'] == linked
    assert (await _read(case, note, 'note_links'))['note_links'] == [linked]
    for child in (version, task, note):
        await _touched_audit(case, child, 'ApiUser', _LATER)


@pytest.mark.anyio
async def test_project_task_revived_alone_stays_unsettled_until_a_change(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='project')
    task = await _create_record_from_fields(case, 'Task', content='task', project=project)
    await _lifecycle(case, 'delete', project)
    await _lifecycle(case, 'revive', task)
    assert await case.call('find', 'Task', []) == [task]
    assert await case.call('find', 'Task', [['content', 'is', 'task']]) == [task]
    for field in ('project', 'updated_at'):
        with pytest.raises(
            MockgunError, match=f'(?=.*{field})Task.*{task["id"]}.*Project.*{project["id"]}'
        ):
            await _read(case, task, field)
    with pytest.raises(MockgunError, match=f'Task.*{task["id"]}.*Project.*{project["id"]}'):
        await case.call('find', 'Task', [['project', 'is', project]])
    await _lifecycle(case, 'revive', project)
    assert (
        cast('dict[str, object]', (await _read(case, task, 'project'))['project'])['id']
        == project['id']
    )
    with pytest.raises(MockgunError, match=r'updated_at'):
        await _read(case, task, 'updated_at')
    await case.call('update', 'Task', task['id'], {'content': 'changed'})
    assert (await _read(case, task, 'updated_at'))['updated_at'] == QUERY_NOW


@pytest.mark.anyio
async def test_project_shot_revived_alone_hides_the_project_until_it_returns(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='project')
    shot = await _create_record_from_fields(case, 'Shot', code='shot', project=project)
    await _lifecycle(case, 'delete', project)
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'revive', shot)
    assert (await _read(case, shot, 'project'))['project'] is None
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', project)
    assert (await _read(case, shot, 'project'))['project'] == {**project, 'name': 'project'}
    await _touched_audit(case, shot, 'ApiUser', _LATER)


@pytest.mark.parametrize('parent_type', ['Asset', 'Sequence'])
@pytest.mark.anyio
async def test_children_of_other_parents_revived_alone_are_settled(
    effects_site: FakeSiteCase, parent_type: str
) -> None:
    case = effects_site
    parent = await _create_record_from_fields(case, parent_type, code='parent')
    task = await _create_record_from_fields(case, 'Task', content='task', entity=parent)
    version = await _create_record_from_fields(case, 'Version', code='version', entity=parent)
    note = await _create_record_from_fields(case, 'Note', subject='note', note_links=[parent])
    await _lifecycle(case, 'delete', parent)
    case.client.site.set_time(_LATER)
    for child in (task, version, note):
        await _lifecycle(case, 'revive', child)
    assert (await _read(case, task, 'entity'))['entity'] is None
    assert (await _read(case, note, 'note_links'))['note_links'] == []
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', parent)
    linked = {**parent, 'name': 'parent'}
    assert (await _read(case, version, 'entity'))['entity'] == linked
    assert (await _read(case, note, 'note_links'))['note_links'] == [linked]
    for child in (task, version, note):
        await _touched_audit(case, child, 'ApiUser', _LATER)


@pytest.mark.anyio
async def test_hidden_asset_members_survive_visible_edits(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shots = [
        await _create_record_from_fields(case, 'Shot', code=f'shot{index}') for index in range(3)
    ]
    asset = await _create_record_from_fields(case, 'Asset', code='asset', shots=shots[:2])
    await _lifecycle(case, 'delete', shots[0])
    await case.call('update', 'Asset', asset['id'], {'shots': [shots[1]]})
    await case.call(
        'update',
        'Asset',
        asset['id'],
        {'shots': [shots[2]]},
        multi_entity_update_modes={'shots': 'add'},
    )
    await _lifecycle(case, 'revive', shots[0])
    for shot in shots:
        assert await case.call('find', 'Asset', [['shots', 'is', shot]]) == [asset]
    assert [
        row['id']
        for row in cast('list[dict[str, object]]', (await _read(case, asset, 'shots'))['shots'])
    ] == [shot['id'] for shot in shots]
    await case.call('update', 'Asset', asset['id'], {'shots': [shots[1], shots[0]]})
    assert [
        row['id']
        for row in cast('list[dict[str, object]]', (await _read(case, asset, 'shots'))['shots'])
    ] == [shots[0]['id'], shots[1]['id']]


@pytest.mark.parametrize(
    ('method', 'entity_type', 'field', 'mode'),
    [
        ('update', 'Asset', 'shots', 'remove'),
        ('update', 'Asset', 'shots', 'add'),
        ('update', 'Asset', 'shots', 'set'),
        ('update', 'Task', 'entity', None),
        ('create', 'Asset', 'shots', None),
        ('create', 'Task', 'entity', None),
    ],
)
@pytest.mark.anyio
async def test_write_naming_a_retired_record_is_refused(
    effects_site: FakeSiteCase, method: str, entity_type: str, field: str, mode: str | None
) -> None:
    case = effects_site
    retired = await _create_record_from_fields(case, 'Shot', code='retired')
    live = await _create_record_from_fields(case, 'Shot', code='live')
    asset = await _create_record_from_fields(case, 'Asset', code='asset', shots=[retired, live])
    task = await _create_record_from_fields(case, 'Task', content='task', entity=live)
    await _lifecycle(case, 'delete', retired)
    if field == 'shots':
        value: object = [retired, live] if mode in {'set', None} else [retired]
    else:
        value = retired
    holder = asset if entity_type == 'Asset' else task
    if method == 'create':
        name = 'code' if entity_type == 'Asset' else 'content'
        write = case.call('create', entity_type, {name: 'new', field: value})
    else:
        modes = {field: mode} if mode in {'add', 'remove'} else None
        write = case.call(
            'update', entity_type, holder['id'], {field: value}, multi_entity_update_modes=modes
        )
    pattern = (
        rf'API {method}\(\) CRUD ERROR #\d+: .*'
        rf'Update failed for \[{entity_type}\.{field}\]: Value is not legal\.'
    )
    with pytest.raises(Fault, match=pattern):
        await write
    assert await case.call('find', entity_type, []) == [holder]
    await _lifecycle(case, 'revive', retired)
    assert [
        row['id']
        for row in cast('list[dict[str, object]]', (await _read(case, asset, 'shots'))['shots'])
    ] == [live['id'], retired['id']]
    assert (await _read(case, task, 'entity'))['entity'] == {**live, 'name': 'live'}


@pytest.mark.anyio
async def test_refused_retired_link_write_has_fault_code_104(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task')
    await _lifecycle(case, 'delete', shot)
    response = raw_call(
        case.client._server,
        'update',
        {'type': 'Task', 'id': task['id'], 'fields': [{'field_name': 'entity', 'value': shot}]},
    )
    assert response == {
        'exception': True,
        'message': 'API update() CRUD ERROR #5: Update failed for [Task.entity]: '
        'Value is not legal.',
        'error_code': 104,
    }


@pytest.mark.anyio
async def test_project_cascades_supported_children_and_taints_other_holders(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='project')
    children = [
        await _create_record_from_fields(case, kind, project=project, **{field: kind})
        for kind, field in [
            ('Shot', 'code'),
            ('Asset', 'code'),
            ('Sequence', 'code'),
            ('Task', 'content'),
            ('Version', 'code'),
            ('Note', 'subject'),
        ]
    ]
    playlist = await _create_record_from_fields(case, 'Playlist', code='playlist', project=project)
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', project)
    for child in children:
        assert await _read(case, child, retired=True) == child
        assert await _read(case, child) is None
        await _touched_audit(case, child, 'ApiUser', _LATER, retired=True)
    with pytest.raises(MockgunError, match=f'Playlist.*{playlist["id"]}.*Project'):
        await case.call('find', 'Playlist', [])
    assert await case.call('find', 'Playlist', [['code', 'is', 'other']]) == []
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', project)
    for child in children:
        assert await _read(case, child) == child
        assert (await _read(case, child, 'project'))['project'] == {**project, 'name': 'project'}
        await _touched_audit(case, child, 'ApiUser', _LATER + timedelta(hours=1))
    assert await _read(case, playlist) == playlist
    with pytest.raises(MockgunError, match=r'updated_at'):
        await _read(case, playlist, 'updated_at')


@pytest.mark.anyio
async def test_archived_project_retirement_cascades_like_any_project(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='archived', archived=True)
    shot = await _create_record_from_fields(case, 'Shot', code='shot', project=project)
    task = await _create_record_from_fields(case, 'Task', content='task', project=project)
    file = await _create_record_from_fields(case, 'PublishedFile', code='file', entity=shot)
    await _lifecycle(case, 'delete', project)
    assert await case.call('find', 'Shot', [], retired_only=True) == [shot]
    assert await case.call('find', 'Task', [], retired_only=True) == [task]
    assert (await _read(case, shot, 'project', retired=True))['project'] is None
    assert (await _read(case, file, 'entity'))['entity'] is None
    await _lifecycle(case, 'revive', project)
    assert await case.call('find', 'Shot', [['project', 'is', project]]) == [shot]
    assert await case.call('find', 'Task', [['project', 'is', project]]) == [task]


@pytest.mark.anyio
async def test_unobserved_edges_taint_transitively_and_reject_writes_atomically(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task')
    note = await _create_record_from_fields(
        case, 'Note', subject='note', note_links=[shot], tasks=[task]
    )
    other = await _create_record_from_fields(case, 'Note', subject='other')
    reply = await _create_record_from_fields(case, 'Reply', content='reply', entity=note)
    await _lifecycle(case, 'delete', shot)
    for kind, filters, retired in [
        ('Note', [], False),
        ('Note', [], True),
        ('Note', [['tasks', 'is', task]], False),
        ('Reply', [], False),
    ]:
        with pytest.raises(
            MockgunError,
            match=f'{kind}.*{(reply if kind == "Reply" else note)["id"]}.*Shot.*{shot["id"]}',
        ):
            await case.call('find', kind, filters, retired_only=retired)
    assert await case.call('find', 'Note', [['subject', 'is', 'other']]) == [other]
    assert await case.call('find', 'Note', [['id', 'is', other['id']]], retired_only=True) == []
    assert await case.call('find', 'Reply', [['content', 'is', 'other']]) == []
    assert await case.call('find', 'Task', []) == [task]
    assert await case.call('find', 'Shot', [], retired_only=True) == [shot]
    store = case.client._server.store
    before = store.snapshot()
    for method, args in [
        ('update', ('Note', note['id'], {'subject': 'changed'})),
        ('delete', ('Note', note['id'])),
        ('create', ('Reply', {'content': 'new', 'entity': note})),
        ('update', ('Reply', reply['id'], {'content': 'changed'})),
        ('delete', ('Task', task['id'])),
    ]:
        with pytest.raises(MockgunError):
            await case.call(method, *args)
        assert store.snapshot() == before
    await _lifecycle(case, 'revive', shot)
    assert await case.call('find', 'Note', []) == [note, other]
    assert await case.call('find', 'Reply', []) == [reply]
    with pytest.raises(MockgunError, match=r'updated_at'):
        await _read(case, note, 'updated_at')
    edited = await _create_record_from_fields(case, 'Shot', code='edited')
    cut = await _create_record_from_fields(case, 'Cut', code='cut', entity=edited)
    item = await _create_record_from_fields(case, 'CutItem', code='item', cut=cut)
    await _lifecycle(case, 'delete', edited)
    with pytest.raises(MockgunError, match=f'Cut.*{cut["id"]}.*Shot'):
        await case.call('find', 'Cut', [])
    assert await case.call('find', 'Cut', [['code', 'is', 'other']]) == []
    with pytest.raises(MockgunError, match=f'CutItem.*{item["id"]}.*cut.*Shot'):
        await _read(case, item, 'cut')


@pytest.mark.anyio
async def test_latest_reductions_dominance_and_limit(effects_site: FakeSiteCase) -> None:
    case = effects_site
    reviewer = await _create_record_from_fields(case, 'HumanUser', login='reviewer', name='r')
    active = await _create_record_from_fields(case, 'Project', name='active')
    shots = [
        await _create_record_from_fields(case, 'Shot', code=f'shot{index}', project=active)
        for index in range(3)
    ]
    winners: list[dict[str, object]] = []
    uncertain: list[list[dict[str, object]]] = []
    for group, offsets in enumerate((range(8, 1, -1), (0,), range(7, 0, -1))):
        uncertain.append([])
        for offset in offsets:
            candidate = await _create_record_from_fields(
                case,
                'Version',
                code=f'uncertain{group}x{offset}',
                entity=shots[group],
                project=active,
                user=reviewer,
                created_at=QUERY_NOW - timedelta(hours=offset),
            )
            uncertain[-1].append(candidate)
        winners.append(
            await _create_record_from_fields(
                case,
                'Version',
                code=f'winner{group}',
                entity=shots[group],
                project=active,
                created_at=QUERY_NOW - timedelta(hours=(0, 1, 8)[group]),
            )
        )
    await _lifecycle(case, 'delete', reviewer)
    result = cast(
        'list[dict[str, object]]',
        await case.call(
            'find',
            'Version',
            [['entity', 'is', shots[0]]],
            ['code'],
            additional_filter_presets=_LATEST,
        ),
    )
    assert [row['id'] for row in result] == [winners[0]['id']]
    with pytest.raises(
        MockgunError, match=f'Version.*{uncertain[0][0]["id"]}.*HumanUser.*{reviewer["id"]}'
    ):
        await case.call('find', 'Version', [['entity', 'is', shots[0]]])
    with pytest.raises(
        MockgunError, match=f'Version.*{uncertain[1][0]["id"]}.*HumanUser.*{reviewer["id"]}'
    ):
        await case.call(
            'find', 'Version', [['entity', 'is', shots[1]]], additional_filter_presets=_LATEST
        )
    with pytest.raises(MockgunError, match=r'128.*64'):
        await case.call(
            'find', 'Version', [['entity', 'is', shots[2]]], additional_filter_presets=_LATEST
        )


@pytest.mark.anyio
async def test_cascade_of_cascade_and_cut_item_link(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    note = await _create_record_from_fields(case, 'Note', subject='note', note_links=[shot])
    reply = await _create_record_from_fields(case, 'Reply', content='reply', entity=note)
    await _lifecycle(case, 'delete', shot)
    assert await _read(case, note, retired=True) == note
    with pytest.raises(MockgunError, match=f'Reply.*{reply["id"]}.*Shot.*{shot["id"]}'):
        await case.call('find', 'Reply', [])
    await _lifecycle(case, 'revive', shot)
    assert await _read(case, reply) == reply
    assert await _read(case, note) == note
    cuts = [
        await _create_record_from_fields(case, 'Cut', code=f'cut{index}') for index in range(2)
    ]
    items = [
        await _create_record_from_fields(case, 'CutItem', code=f'item{index}', cut=cut)
        for index, cut in enumerate(cuts)
    ]
    original = await _read(case, items[0], 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', cuts[0])
    assert await case.call('find', 'CutItem', [], ['code']) == [
        {**item, 'code': f'item{index}'} for index, item in enumerate(items)
    ]
    assert (await _read(case, items[0], 'cut'))['cut'] is None
    assert await _read(case, items[0], 'updated_at', 'updated_by') == original
    assert await case.call('find', 'CutItem', [['cut', 'is', cuts[0]]]) == []
    assert await case.call('find', 'CutItem', [['cut', 'is', cuts[1]]]) == [items[1]]
    await _lifecycle(case, 'revive', cuts[0])
    assert await case.call('find', 'CutItem', [['cut', 'is', cuts[0]]]) == [items[0]]
    assert (
        cast('dict[str, object]', (await _read(case, items[0], 'cut'))['cut'])['id']
        == cuts[0]['id']
    )


@pytest.mark.anyio
async def test_batch_restores_retirement_effects(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    for kind, field in [('Task', 'content'), ('Version', 'code'), ('Note', 'subject')]:
        await _create_record_from_fields(
            case,
            kind,
            **{
                field: kind,
                'note_links' if kind == 'Note' else 'entity': [shot] if kind == 'Note' else shot,
            },
        )
    active_before = {
        kind: await case.call('find', kind, []) for kind in ('Shot', 'Task', 'Version', 'Note')
    }
    before = case.client._server.store.snapshot()
    with pytest.raises(Fault, match=r'No entity'):
        await case.call(
            'batch',
            [
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': shot['id']},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'missing'},
                },
            ],
        )
    assert case.client._server.store.snapshot() == before
    for kind, rows in active_before.items():
        assert await case.call('find', kind, []) == rows


@pytest.mark.anyio
async def test_retired_holder_hides_its_link_while_the_target_is_retired(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    file = await _create_record_from_fields(case, 'PublishedFile', code='file', entity=shot)
    linked = {**shot, 'name': 'shot'}
    await _lifecycle(case, 'delete', file)
    assert (await _read(case, file, 'entity', retired=True))['entity'] == linked
    await _lifecycle(case, 'delete', shot)
    assert (await _read(case, file, 'entity', retired=True))['entity'] is None
    assert (
        await case.call('find', 'PublishedFile', [['entity', 'is', shot]], retired_only=True) == []
    )
    await _lifecycle(case, 'revive', file)
    assert (await _read(case, file, 'entity'))['entity'] is None
    await _lifecycle(case, 'revive', shot)
    assert (await _read(case, file, 'entity'))['entity'] == linked


@pytest.mark.anyio
async def test_retired_record_keeps_its_link_to_itself(effects_site: FakeSiteCase) -> None:
    case = effects_site
    row = await _create_record_from_fields(case, 'PublishedFile', code='self')
    await case.call('update', 'PublishedFile', row['id'], {'entity': row})
    await _lifecycle(case, 'delete', row)
    assert (await _read(case, row, 'entity', retired=True))['entity'] == {**row, 'name': 'self'}


@pytest.mark.anyio
async def test_holder_revived_before_its_target_hides_the_link_until_the_target_returns(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task', entity=shot)
    await _lifecycle(case, 'delete', task)
    await _lifecycle(case, 'delete', shot)
    assert (await _read(case, task, 'entity', retired=True))['entity'] is None
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'revive', task)
    assert (await _read(case, task, 'entity'))['entity'] is None
    assert await case.call('find', 'Task', [['entity', 'is', shot]]) == []
    revived = await _read(case, task, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', shot)
    assert (await _read(case, task, 'entity'))['entity'] == {**shot, 'name': 'shot'}
    assert await _read(case, task, 'updated_at', 'updated_by') == revived


@pytest.mark.parametrize(
    'spec',
    [
        ('Shot', 'assets', 'Asset', 'shots', 'AssetShotConnection', ('shot', 'asset')),
        ('Asset', 'shots', 'Shot', 'assets', 'AssetShotConnection', ('asset', 'shot')),
        (
            'Version',
            'playlists',
            'Playlist',
            'versions',
            'PlaylistVersionConnection',
            ('version', 'playlist'),
        ),
        (
            'Playlist',
            'versions',
            'Version',
            'playlists',
            'PlaylistVersionConnection',
            ('playlist', 'version'),
        ),
        ('Asset', 'assets', 'Asset', 'parents', 'AssetAssetConnection', ('parent', 'asset')),
        ('Asset', 'parents', 'Asset', 'assets', 'AssetAssetConnection', ('asset', 'parent')),
    ],
)
@pytest.mark.anyio
async def test_retiring_either_end_retires_the_connection_record(
    effects_site: FakeSiteCase, spec: tuple[str, str, str, str, str, tuple[str, str]]
) -> None:
    case = effects_site
    root_type, root_field, other_type, other_field, connection, sides = spec
    root_side, other_side = sides
    other = await _create_record_from_fields(case, other_type, code='other')
    root = await _create_record_from_fields(case, root_type, code='root', **{root_field: [other]})
    named_other = {**other, 'name': 'other'}
    named_root = {**root, 'name': 'root'}
    filters = [[other_side, 'is', other]]
    [record] = cast(
        'list[dict[str, object]]', await case.call('find', connection, filters, list(sides))
    )
    await _lifecycle(case, 'delete', root)
    assert await case.call('find', connection, filters, list(sides)) == []
    assert await case.call('find', connection, filters, list(sides), retired_only=True) == [
        {**record, root_side: None, other_side: named_other}
    ]
    assert (await _read(case, other, other_field))[other_field] == []
    assert (await _read(case, root, root_field, retired=True))[root_field] == []
    await _lifecycle(case, 'revive', root)
    assert await case.call('find', connection, filters, list(sides)) == [
        {**record, root_side: named_root, other_side: named_other}
    ]
    assert (await _read(case, other, other_field))[other_field] == [named_root]
    assert (await _read(case, root, root_field))[root_field] == [named_other]


@pytest.mark.anyio
async def test_connection_revives_with_the_end_that_retired_it(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    asset = await _create_record_from_fields(case, 'Asset', code='asset', shots=[shot])
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', shot)
    await _lifecycle(case, 'delete', asset)
    retired_asset = await _read(case, asset, 'updated_at', retired=True)
    await _lifecycle(case, 'revive', shot)
    assert (await _read(case, asset, 'shots', retired=True))['shots'] == [{**shot, 'name': 'shot'}]
    assert (await _read(case, shot, 'assets'))['assets'] == []
    assert (await _read(case, asset, 'updated_at', retired=True)) == retired_asset
    await _lifecycle(case, 'revive', asset)
    assert (await _read(case, shot, 'assets'))['assets'] == [{**asset, 'name': 'asset'}]
    other_shot = await _create_record_from_fields(case, 'Shot', code='other shot')
    other = await _create_record_from_fields(case, 'Asset', code='other', shots=[other_shot])
    await _lifecycle(case, 'delete', other)
    await _lifecycle(case, 'delete', other_shot)
    await _lifecycle(case, 'revive', other_shot)
    assert (await _read(case, other_shot, 'assets'))['assets'] == []
    assert (await _read(case, other, 'shots', retired=True))['shots'] == []
    await _lifecycle(case, 'revive', other)
    assert (await _read(case, other_shot, 'assets'))['assets'] == [{**other, 'name': 'other'}]


@pytest.mark.anyio
async def test_note_retired_after_its_shot_follows_the_observed_revival(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    asset = await _create_record_from_fields(case, 'Asset', code='asset')
    pair = await _create_record_from_fields(case, 'Note', subject='pair', note_links=[shot, asset])
    single = await _create_record_from_fields(case, 'Note', subject='single', note_links=[shot])
    await _lifecycle(case, 'delete', shot)
    await _lifecycle(case, 'revive', single)
    await _lifecycle(case, 'delete', single)
    await _lifecycle(case, 'delete', pair)
    assert (await _read(case, pair, 'note_links', retired=True))['note_links'] == []
    await _lifecycle(case, 'revive', shot)
    assert (await _read(case, pair, 'note_links'))['note_links'] == [
        {**asset, 'name': 'asset'},
        {**shot, 'name': 'shot'},
    ]
    assert (await _read(case, single, 'note_links', retired=True))['note_links'] == [
        {**shot, 'name': 'shot'}
    ]


@pytest.mark.anyio
async def test_note_retired_after_its_task_stays_retired(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task')
    note = await _create_record_from_fields(
        case, 'Note', subject='note', tasks=[task], note_links=[shot]
    )
    await _lifecycle(case, 'delete', task)
    await _lifecycle(case, 'delete', note)
    await _lifecycle(case, 'revive', task)
    retired = await _read(case, note, 'tasks', 'note_links', retired=True)
    assert retired['tasks'] == [{**task, 'name': 'task'}]
    assert retired['note_links'] == []


@pytest.mark.anyio
async def test_unobserved_note_shape_retired_after_its_target_is_unsettled(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    other = await _create_record_from_fields(case, 'Shot', code='other')
    note = await _create_record_from_fields(case, 'Note', subject='note', note_links=[shot, other])
    await _lifecycle(case, 'delete', shot)
    assert (await _read(case, note, 'note_links'))['note_links'] == [{**other, 'name': 'other'}]
    await _lifecycle(case, 'delete', note)
    await _lifecycle(case, 'revive', shot)
    with pytest.raises(MockgunError, match=rf'Note.*{note["id"]}.*Shot {shot["id"]}'):
        await case.call('find', 'Note', [], retired_only=True)


@pytest.mark.parametrize('parent_type', ['Shot', 'Task'])
@pytest.mark.anyio
async def test_time_log_survives_its_entity_with_the_link_hidden(
    effects_site: FakeSiteCase, parent_type: str
) -> None:
    case = effects_site
    name = 'code' if parent_type == 'Shot' else 'content'
    parent = await _create_record_from_fields(case, parent_type, **{name: 'parent'})
    log = await _create_record_from_fields(case, 'TimeLog', entity=parent, duration=60)
    original = await _read(case, log, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', parent)
    assert await case.call('find', 'TimeLog', []) == [log]
    assert (await _read(case, log, 'entity'))['entity'] is None
    assert await _read(case, log, 'updated_at', 'updated_by') == original
    await _lifecycle(case, 'revive', parent)
    assert (await _read(case, log, 'entity'))['entity'] == {**parent, 'name': 'parent'}
    assert await _read(case, log, 'updated_at', 'updated_by') == original


@pytest.mark.anyio
async def test_parent_revival_leaves_children_whose_link_changed_while_retired(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    other = await _create_record_from_fields(case, 'Shot', code='other')
    kept, cleared, moved = [
        await _create_record_from_fields(case, 'Version', code=code, entity=shot)
        for code in ('kept', 'cleared', 'moved')
    ]
    await _lifecycle(case, 'delete', shot)
    await case.call('update', 'Version', cleared['id'], {'entity': None})
    await case.call('update', 'Version', moved['id'], {'entity': other})
    await _lifecycle(case, 'revive', shot)
    assert await case.call('find', 'Version', []) == [kept]
    assert await case.call('find', 'Version', [], retired_only=True) == [cleared, moved]
    assert (await _read(case, moved, 'entity', retired=True))['entity'] == {
        **other,
        'name': 'other',
    }


@pytest.mark.anyio
async def test_note_created_for_a_task_also_links_the_task_entity(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task', entity=shot)
    bare = await _create_record_from_fields(case, 'Task', content='bare')
    note = await _create_record_from_fields(case, 'Note', subject='note', tasks=[task])
    plain = await _create_record_from_fields(case, 'Note', subject='plain', tasks=[bare])
    assert (await _read(case, note, 'note_links'))['note_links'] == [{**shot, 'name': 'shot'}]
    assert (await _read(case, shot, 'notes'))['notes'] == [{**note, 'name': 'note'}]
    assert (await _read(case, plain, 'note_links'))['note_links'] == []
    original = await _read(case, note, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', task)
    assert await case.call('find', 'Note', []) == [note, plain]
    assert (await _read(case, note, 'tasks'))['tasks'] == []
    assert (await _read(case, note, 'note_links'))['note_links'] == [{**shot, 'name': 'shot'}]
    assert await _read(case, note, 'updated_at', 'updated_by') == original
    await _lifecycle(case, 'revive', task)
    assert (await _read(case, note, 'tasks'))['tasks'] == [{**task, 'name': 'task'}]


@pytest.mark.anyio
async def test_note_links_gain_the_entities_of_every_new_task(effects_site: FakeSiteCase) -> None:
    case = effects_site
    shot_a = await _create_record_from_fields(case, 'Shot', code='a')
    shot_b = await _create_record_from_fields(case, 'Shot', code='b')
    task_a = await _create_record_from_fields(case, 'Task', content='ta', entity=shot_a)
    task_a2 = await _create_record_from_fields(case, 'Task', content='ta2', entity=shot_a)
    task_b = await _create_record_from_fields(case, 'Task', content='tb', entity=shot_b)

    async def links(note: dict[str, object]) -> list[object]:
        rows = cast(
            'list[dict[str, object]]', (await _read(case, note, 'note_links'))['note_links']
        )
        return [row['id'] for row in rows]

    both = await _create_record_from_fields(case, 'Note', subject='both', tasks=[task_b, task_a])
    assert await links(both) == [shot_a['id'], shot_b['id']]
    same = await _create_record_from_fields(case, 'Note', subject='same', tasks=[task_a, task_a2])
    assert await links(same) == [shot_a['id']]
    union = await _create_record_from_fields(
        case, 'Note', subject='union', tasks=[task_a], note_links=[shot_b]
    )
    assert await links(union) == [shot_a['id'], shot_b['id']]
    later = await _create_record_from_fields(case, 'Note', subject='later', note_links=[shot_b])
    await case.call('update', 'Note', later['id'], {'tasks': [task_a]})
    assert await links(later) == [shot_a['id'], shot_b['id']]
    assert (await _read(case, shot_a, 'notes'))['notes'] == [
        {**both, 'name': 'both'},
        {**later, 'name': 'later'},
        {**same, 'name': 'same'},
        {**union, 'name': 'union'},
    ]
    await case.call('update', 'Note', later['id'], {'tasks': []})
    assert await links(later) == [shot_a['id'], shot_b['id']]
    await case.call('update', 'Note', both['id'], {'tasks': [task_b], 'note_links': []})
    assert await links(both) == []


@pytest.mark.anyio
async def test_note_link_from_a_task_with_a_retired_entity_is_unsettled(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    task = await _create_record_from_fields(case, 'Task', content='task', entity=shot)
    await _lifecycle(case, 'delete', task)
    await _lifecycle(case, 'delete', shot)
    await _lifecycle(case, 'revive', task)
    create = case.call('create', 'Note', {'subject': 'note', 'tasks': [task]})
    with pytest.raises(MockgunError, match=rf'Task {task["id"]}.*retired Shot {shot["id"]}'):
        await create
    assert await case.call('find', 'Note', []) == []


@pytest.mark.anyio
@pytest.mark.parametrize('method', ['delete', 'revive'])
async def test_missing_retirement_id_has_fault_code_104(
    effects_site: FakeSiteCase, method: str
) -> None:
    shot = seed(effects_site, 'Shot', {'code': 's1'})
    with pytest.raises(Fault, match='No entity of type Shot exists with id 999'):
        await effects_site.call(method, 'Shot', 999)

    response = raw_call(effects_site.client._server, method, {'type': 'Shot', 'id': 999})
    assert response == {
        'exception': True,
        'message': 'No entity of type Shot exists with id 999',
        'error_code': 104,
    }
    assert await effects_site.call('find_one', 'Shot', [['id', 'is', shot['id']]], ['code']) == {
        **shot,
        'code': 's1',
    }


@pytest.mark.anyio
async def test_missing_update_id_has_fault_code_104(effects_site: FakeSiteCase) -> None:
    shot = seed(effects_site, 'Shot', {'code': 's1'})
    with pytest.raises(Fault, match='No entity of type Shot exists with id 999'):
        await effects_site.call('update', 'Shot', 999, {'code': 'missing'})

    response = raw_call(
        effects_site.client._server,
        'update',
        {
            'type': 'Shot',
            'id': 999,
            'fields': [{'field_name': 'code', 'value': 'missing'}],
        },
    )
    assert response == {
        'exception': True,
        'message': 'No entity of type Shot exists with id 999',
        'error_code': 104,
    }
    assert await effects_site.call('find_one', 'Shot', [['id', 'is', shot['id']]], ['code']) == {
        **shot,
        'code': 's1',
    }


@pytest.mark.anyio
async def test_retirement_results_and_unknown_type_stay_stable(
    effects_site: FakeSiteCase,
) -> None:
    shot = seed(effects_site, 'Shot', {'code': 's1'})
    shot_id = shot['id']
    assert await effects_site.call('delete', 'Shot', shot_id) is True
    assert await effects_site.call('delete', 'Shot', shot_id) is False
    assert await effects_site.call('revive', 'Shot', shot_id) is True
    assert await effects_site.call('revive', 'Shot', shot_id) is False

    with pytest.raises(Fault, match='NotAnEntity is not a valid entity'):
        await effects_site.call('delete', 'NotAnEntity', 1)
    assert raw_call(effects_site.client._server, 'delete', {'type': 'NotAnEntity', 'id': 1}) == {
        'exception': True,
        'message': 'NotAnEntity is not a valid entity',
        'error_code': None,
    }


@pytest.mark.anyio
async def test_client_task_version_and_note_retirement(effects_site: FakeSiteCase) -> None:
    case = effects_site
    task = await _create_record_from_fields(case, 'Task', content='t2')
    note = await _create_record_from_fields(case, 'Note', subject='n3', tasks=[task])
    version = await _create_record_from_fields(case, 'Version', code='v2', sg_task=task)
    await _lifecycle(case, 'delete', task)
    assert await case.call('find', 'Note', []) == []
    assert await case.call('find', 'Note', [], retired_only=True) == [note]
    assert await case.call('find', 'Version', [], ['sg_task']) == [{**version, 'sg_task': None}]
    await _lifecycle(case, 'revive', task)
    assert (await _read(case, note, 'tasks'))['tasks'] == [{**task, 'name': 't2'}]
    assert (await _read(case, version, 'sg_task'))['sg_task'] == {**task, 'name': 't2'}


@pytest.mark.anyio
async def test_version_note_and_file_lifecycle(effects_site: FakeSiteCase) -> None:
    case = effects_site
    version = await _create_record_from_fields(case, 'Version', code='v10')
    note = await _create_record_from_fields(case, 'Note', subject='n12', note_links=[version])
    file = await _create_record_from_fields(case, 'PublishedFile', code='pf10', version=version)
    original = await _read(case, file, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', version, sudo=True)
    assert await case.call('find', 'Note', []) == []
    assert await case.call('find', 'Note', [], retired_only=True) == [note]
    await _touched_audit(case, note, 'HumanUser', _LATER, retired=True)
    assert (await _read(case, file, 'version'))['version'] is None
    assert await _read(case, file, 'updated_at', 'updated_by') == original
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', version)
    assert await case.call('find', 'Note', []) == [note]
    assert (await _read(case, note, 'note_links'))['note_links'] == [{**version, 'name': 'v10'}]
    assert (await _read(case, file, 'version'))['version'] == {**version, 'name': 'v10'}
    await _touched_audit(case, note, 'ApiUser', _LATER + timedelta(hours=1))
    assert await _read(case, file, 'updated_at', 'updated_by') == original


@pytest.mark.anyio
async def test_task_retirement_hides_inverse_and_seeded_holders(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='s19')
    task = await _create_record_from_fields(case, 'Task', content='t11', entity=shot)
    file = await _create_record_from_fields(case, 'PublishedFile', code='pf11', task=task)
    version = seed(case, 'Version', {'code': 'v11', 'sg_task': task})
    holders = ((shot, 'tasks'), (file, 'task'), (version, 'sg_task'))
    originals = [await _read(case, row, 'updated_at', 'updated_by') for row, _ in holders]
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', task, sudo=True)
    for (row, field), original in zip(holders, originals, strict=True):
        assert (await _read(case, row, field))[field] == ([] if field == 'tasks' else None)
        assert await _read(case, row, 'updated_at', 'updated_by') == original
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', task)
    for (row, field), original in zip(holders, originals, strict=True):
        named = {**task, 'name': 't11'}
        assert (await _read(case, row, field))[field] == ([named] if field == 'tasks' else named)
        assert await _read(case, row, 'updated_at', 'updated_by') == original


@pytest.mark.anyio
@pytest.mark.parametrize(
    'edge',
    [
        ('Shot', 'Note', 'note_links', 'notes', False),
        ('Version', 'PublishedFile', 'version', 'published_files', False),
        ('Cut', 'CutItem', 'cut', 'cut_items', False),
        ('Version', 'Playlist', 'versions', 'playlists', True),
    ],
)
async def test_forward_link_retirement_hides_inverse_members(
    effects_site: FakeSiteCase, edge: tuple[str, str, str, str, bool]
) -> None:
    holder_type, parent_type, forward, inverse, touched = edge
    case = effects_site
    holder = await _create_record_from_fields(case, holder_type, code='holder')
    parent = await _create_record_from_fields(
        case,
        parent_type,
        **{
            'subject' if parent_type == 'Note' else 'code': 'parent',
            forward: [holder] if forward in {'note_links', 'versions'} else holder,
        },
    )
    original = await _read(case, holder, 'updated_at', 'updated_by')
    case.client.site.set_time(_LATER)
    await _lifecycle(case, 'delete', parent, sudo=True)
    assert (await _read(case, holder, inverse))[inverse] == []
    if touched:
        await _touched_audit(case, holder, 'HumanUser', _LATER)
    else:
        assert await _read(case, holder, 'updated_at', 'updated_by') == original
    case.client.site.set_time(_LATER + timedelta(hours=1))
    await _lifecycle(case, 'revive', parent)
    assert (await _read(case, holder, inverse))[inverse] == [{**parent, 'name': 'parent'}]
    if touched:
        await _touched_audit(case, holder, 'ApiUser', _LATER + timedelta(hours=1))
    else:
        assert await _read(case, holder, 'updated_at', 'updated_by') == original


@pytest.mark.anyio
async def test_effects_hidden_member_visible_remove(effects_site: FakeSiteCase) -> None:
    case = effects_site
    hidden_shot = await _create_record_from_fields(case, 'Shot', code='s9')
    visible_shot = await _create_record_from_fields(case, 'Shot', code='s10')
    asset = await _create_record_from_fields(
        case, 'Asset', code='a4', shots=[hidden_shot, visible_shot]
    )
    await _lifecycle(case, 'delete', hidden_shot)
    await case.call(
        'update',
        'Asset',
        asset['id'],
        {'shots': [visible_shot]},
        multi_entity_update_modes={'shots': 'remove'},
    )
    await _lifecycle(case, 'revive', hidden_shot)
    assert (await _read(case, asset, 'shots'))['shots'] == [{**hidden_shot, 'name': 's9'}]


@pytest.mark.anyio
async def test_retained_set_without_membership_change_touches_nothing(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shots = [
        await _create_record_from_fields(case, 'Shot', code=f's{index}') for index in range(2)
    ]
    asset = await _create_record_from_fields(case, 'Asset', code='asset', shots=shots)
    await _lifecycle(case, 'delete', shots[0])
    before = case.client._server.store.require('Asset', entity_id(asset)).copy()
    await case.call('create', 'HumanUser', {'login': 'artist', 'name': 'Artist'})
    case.client.site.set_time(_LATER)
    response = raw_call(
        case.client._server,
        'update',
        {
            'type': 'Asset',
            'id': asset['id'],
            'fields': [{'field_name': 'shots', 'value': [shots[1]]}],
        },
        auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
    )
    assert response.get('exception') is not True
    stored = case.client._server.store.require('Asset', entity_id(asset))
    assert stored['updated_at'] == before['updated_at']
    assert stored['updated_by'] == before['updated_by']
    await _lifecycle(case, 'revive', shots[0])
    assert [
        row['id']
        for row in cast('list[dict[str, object]]', (await _read(case, asset, 'shots'))['shots'])
    ] == [shot['id'] for shot in shots]


@pytest.mark.anyio
async def test_known_active_taint_is_rejected_before_retired_filters(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='project')
    task = await _create_record_from_fields(case, 'Task', content='task', project=project)
    await _lifecycle(case, 'delete', project)
    await _lifecycle(case, 'revive', task)
    assert (
        await case.call(
            'find', 'Task', [['updated_at', 'greater_than', QUERY_NOW]], retired_only=True
        )
        == []
    )


@pytest.mark.anyio
async def test_candidacy_reduction_precedes_combination_limit(effects_site: FakeSiteCase) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='archived', archived=True)
    shots = [
        await _create_record_from_fields(case, 'Shot', code=f'shot{index}', project=project)
        for index in range(7)
    ]
    await _create_record_from_fields(case, 'Note', subject='note', note_links=shots)
    await _lifecycle(case, 'delete', project)
    filters = {
        'filter_operator': 'all',
        'filters': [
            {'filter_operator': 'any', 'filters': [['note_links', 'is', shot] for shot in shots]},
            ['subject', 'is', 'absent'],
        ],
    }
    assert await case.call('find', 'Note', filters) == []


@pytest.mark.anyio
async def test_deep_nonmatching_condition_drops_visibility_dimensions(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='archived', archived=True)
    sequence = await _create_record_from_fields(case, 'Sequence', code='sequence')
    for index in range(7):
        await _create_record_from_fields(
            case, 'Shot', code=f'shot{index}', project=project, sg_sequence=sequence
        )
    await _lifecycle(case, 'delete', project)
    assert await case.call('find', 'Sequence', [['shots.Shot.code', 'is', 'absent']]) == []


@pytest.mark.anyio
async def test_unrelated_uncertainty_preserves_latest_archive_guard(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    shot = await _create_record_from_fields(case, 'Shot', code='shot')
    for index, archived in enumerate((False, True)):
        project = await _create_record_from_fields(
            case, 'Project', name=f'p{index}', archived=archived
        )
        await _create_record_from_fields(
            case,
            'Version',
            code=f'v{index}',
            project=project,
            entity=shot,
            created_at=QUERY_NOW - timedelta(hours=index),
        )
    cut = await _create_record_from_fields(case, 'Cut', code='unrelated')
    for retire_cut in (False, True):
        if retire_cut:
            await _lifecycle(case, 'delete', cut)
        with pytest.raises(MockgunError, match=r'LATEST.*archived.*active'):
            await case.call(
                'find',
                'Version',
                [],
                additional_filter_presets=_LATEST,
                include_archived_projects=False,
            )


@pytest.mark.anyio
async def test_dominance_reduction_precedes_large_condition_space(
    effects_site: FakeSiteCase,
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='archived', archived=True)
    shots = [
        await _create_record_from_fields(case, 'Shot', code=f'shot{index}', project=project)
        for index in range(7)
    ]
    sequence = await _create_record_from_fields(case, 'Sequence', code='sequence', shots=shots)
    await _create_record_from_fields(
        case, 'Version', code='older', entity=sequence, created_at=QUERY_NOW - timedelta(hours=1)
    )
    winner = await _create_record_from_fields(
        case, 'Version', code='winner', created_at=QUERY_NOW, entity=sequence
    )
    await _lifecycle(case, 'delete', project)
    filters = {
        'filter_operator': 'any',
        'filters': [
            ['code', 'is', 'winner'],
            {
                'filter_operator': 'any',
                'filters': [
                    ['entity.Sequence.shots.Shot.code', 'is', f'shot{index}'] for index in range(7)
                ],
            },
        ],
    }
    assert await case.call('find', 'Version', filters, additional_filter_presets=_LATEST) == [
        winner
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('unrelated_version', [False, True])
async def test_cut_preset_reads_external_shot_without_matching_versions(
    effects_site: FakeSiteCase, unrelated_version: bool
) -> None:
    case = effects_site
    project = await _create_record_from_fields(case, 'Project', name='archived', archived=True)
    shot = await _create_record_from_fields(case, 'Shot', code='uncertain', project=project)
    other = await _create_record_from_fields(case, 'Shot', code='other')
    cut = await _create_record_from_fields(case, 'Cut', code='cut')
    await _create_record_from_fields(case, 'CutItem', code='item', cut=cut, shot=shot)
    if unrelated_version:
        await _create_record_from_fields(case, 'Version', code='unrelated', entity=other)
    await _lifecycle(case, 'delete', project)
    with pytest.raises(MockgunError, match=r'CUT_SHOT_VERSIONS.*CutItem.shot'):
        await case.call(
            'find',
            'Version',
            [],
            additional_filter_presets=[{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': cut['id']}],
        )
