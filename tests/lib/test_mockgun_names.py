from typing import cast

import pytest

from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id
from tests.lib._records import create_link

from .conftest import FakeSiteCase, raw_call

_NOTE_NAMES = [
    ({'subject': 'Sub'}, 'Sub'),
    ({'subject': '', 'content': 'Body'}, 'Body'),
    ({'subject': 'Sub', 'content': 'Body'}, 'Sub - Body'),
    ({'subject': 'a' * 28, 'content': 'b' * 28}, 'a' * 28 + ' - ' + 'b' * 28),
    ({'subject': 'a' * 120}, 'a' * 120),
    ({'subject': 'a' * 40, 'content': 'b' * 40}, 'a' * 40 + ' - ' + 'b' * 40),
    ({'content': 'c' * 300}, 'c' * 255),
    ({'subject': 's1\ns2'}, 's1\ns2'),
    ({'content': 'c1\nc2'}, 'c1\nc2'),
    ({'subject': 's\n1', 'content': 'c\n2'}, 's\n1 - c\n2'),
    ({'content': 'c' * 255}, 'c' * 255),
]
_UNSETTLED_NOTES = [
    {},
    {'subject': 'a' * 121},
    {'subject': 'a' * 41, 'content': 'b' * 40},
    {'content': 'c' * 200 + ' ' + 'c' * 99},
    {'subject': 'line\rbreak'},
    {'subject': '\u4e2d' * 86},
]


async def _read(case: FakeSiteCase, link: dict[str, object], field: str) -> object:
    row = cast(
        'dict[str, object]',
        await case.call('find_one', link['type'], [['id', 'is', link['id']]], [field]),
    )
    return row[field]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'data', 'name'),
    [
        ('Project', {'name': 'Alpha'}, 'Alpha'),
        ('Sequence', {'code': 'seq-a'}, 'seq-a'),
        ('Shot', {'code': 'S010'}, 'S010'),
        ('Asset', {'code': 'asset'}, 'asset'),
        ('Step', {'code': 'Comp', 'list_order': 10}, 'Comp'),
        ('Task', {'content': 'Layout'}, 'Layout'),
        ('Version', {'code': 'v001'}, 'v001'),
        ('PublishedFileType', {'code': 'Maya Scene'}, 'Maya Scene'),
        ('PublishedFile', {'code': 'pf-code', 'name': 'pf-name'}, 'pf-code'),
        ('Playlist', {'code': 'pl-a'}, 'pl-a'),
        ('Cut', {'code': 'cut-a'}, 'cut-a'),
        ('CutItem', {'code': 'ci-1'}, 'ci-1'),
        ('HumanUser', {'login': 'artist1', 'name': 'Artist One'}, 'Artist One'),
        ('TaskTemplate', {'code': 'tt-1'}, 'tt-1'),
        ('Reply', {'content': 'Thanks'}, 'Thanks'),
        ('ApiUser', {'firstname': 'Ada', 'lastname': 'Lovelace'}, 'Ada Lovelace'),
        ('Shot', {'code': 'x' * 255}, 'x' * 255),
    ],
)
async def test_display_rules(
    query_site: FakeSiteCase, kind: str, data: dict[str, object], name: str
) -> None:
    target = await create_link(query_site, kind, data)
    scalar = await create_link(query_site, 'Scalar', {'sg_link': target})
    assert await _read(query_site, scalar, 'sg_link') == {**target, 'name': name}


@pytest.mark.anyio
@pytest.mark.parametrize(('data', 'name'), _NOTE_NAMES)
@pytest.mark.parametrize('surface', ['cache', 'link'])
async def test_note_names(
    query_site: FakeSiteCase, data: dict[str, object], name: str, surface: str
) -> None:
    note = await create_link(query_site, 'Note', data)
    if surface == 'cache':
        assert await _read(query_site, note, 'cached_display_name') == name
    else:
        scalar = await create_link(query_site, 'Scalar', {'sg_link': note})
        assert await _read(query_site, scalar, 'sg_link') == {**note, 'name': name}
    assert await _read(query_site, note, 'content') == (data.get('content') or None)


@pytest.mark.anyio
async def test_attachment_display_name_is_cached_and_named_in_note(
    workflow_site: FakeSiteCase,
) -> None:
    attachment = await create_link(
        workflow_site, 'Attachment', {'display_name': 'Shown', 'filename': 'file.txt'}
    )
    assert await _read(workflow_site, attachment, 'cached_display_name') == 'Shown'
    note = await create_link(workflow_site, 'Note', {'subject': 'n', 'attachments': [attachment]})
    assert await _read(workflow_site, note, 'attachments') == [{**attachment, 'name': 'Shown'}]


@pytest.mark.anyio
async def test_attachment_without_display_name_has_unsettled_note_link(
    workflow_site: FakeSiteCase,
) -> None:
    attachment = await create_link(workflow_site, 'Attachment', {'display_name': None})
    with pytest.raises(MockgunError, match=r'Attachment\.display_name'):
        await create_link(workflow_site, 'Note', {'subject': 'n', 'attachments': [attachment]})


@pytest.mark.anyio
@pytest.mark.parametrize('data', _UNSETTLED_NOTES)
@pytest.mark.parametrize('surface', ['cache', 'link'])
async def test_unsettled_note_names(
    query_site: FakeSiteCase, data: dict[str, object], surface: str
) -> None:
    note = await create_link(query_site, 'Note', data)
    operation = (
        _read(query_site, note, 'cached_display_name')
        if surface == 'cache'
        else create_link(query_site, 'Scalar', {'sg_link': note})
    )
    with pytest.raises(MockgunError, match='Note'):
        await operation
    assert await query_site.call('find', 'Scalar', []) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'data', 'name'),
    [
        ('Department', {'code': 'dept-code', 'name': 'Dept Name'}, 'Dept Name'),
        ('Group', {'code': 'group-code'}, 'group-code'),
        ('CustomEntity03', {'code': 'deliverable'}, 'deliverable'),
    ],
)
async def test_other_observed_types_name_links_and_cache(
    query_site: FakeSiteCase, kind: str, data: dict[str, object], name: str
) -> None:
    target = await create_link(query_site, kind, data)
    holder = await create_link(query_site, 'Scalar', {'sg_link': target})
    assert await _read(query_site, holder, 'sg_link') == {**target, 'name': name}
    assert await _read(query_site, target, 'cached_display_name') == name


@pytest.mark.anyio
async def test_ticket_names_links_by_id_and_title(query_site: FakeSiteCase) -> None:
    ticket = await create_link(query_site, 'Ticket', {'title': 'Broken render'})
    holder = await create_link(query_site, 'Scalar', {'sg_link': ticket})
    name = f'#{ticket["id"]}: Broken render'
    assert await _read(query_site, holder, 'sg_link') == {**ticket, 'name': name}
    assert await _read(query_site, ticket, 'cached_display_name') == name


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'data'),
    [
        ('Shot', {'code': 'x' * 256}),
        ('Shot', {'code': None}),
        ('ApiUser', {'firstname': 'Solo'}),
        ('CustomEntity01', {'code': 'c1'}),
        ('Department', {'code': 'd1'}),
        ('Ticket', {}),
    ],
)
@pytest.mark.parametrize('operation', ['create', 'update'])
async def test_unsettled_link_write_is_atomic(
    query_site: FakeSiteCase, kind: str, data: dict[str, object], operation: str
) -> None:
    target = await create_link(query_site, kind, data)
    scalar = await create_link(query_site, 'Scalar', {}) if operation == 'update' else None
    action = (
        create_link(query_site, 'Scalar', {'sg_link': target})
        if scalar is None
        else query_site.call('update', 'Scalar', scalar['id'], {'sg_link': target})
    )
    with pytest.raises(MockgunError, match=kind):
        await action
    if scalar is None:
        assert await query_site.call('find', 'Scalar', []) == []
    else:
        assert await _read(query_site, scalar, 'sg_link') is None


@pytest.mark.anyio
@pytest.mark.parametrize('value', [None, ''])
@pytest.mark.parametrize('surface', ['read', 'create'])
async def test_task_null_name(query_site: FakeSiteCase, value: str | None, surface: str) -> None:
    task = await create_link(query_site, 'Task', {'content': 'Paint'})
    version = await create_link(query_site, 'Version', {'code': 'v5', 'sg_task': task})
    await query_site.call('update', 'Task', task['id'], {'content': value})
    if surface == 'create':
        version = await create_link(query_site, 'Version', {'code': 'v6', 'sg_task': task})
    assert await _read(query_site, version, 'sg_task') == {**task, 'name': '-'}
    assert await _read(query_site, version, 'sg_task.Task.content') is None


@pytest.mark.anyio
@pytest.mark.parametrize('value', [None, ''])
async def test_link_name_becomes_unsettled(query_site: FakeSiteCase, value: str | None) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S010'})
    version = await create_link(query_site, 'Version', {'code': 'v1', 'entity': shot})
    await query_site.call('update', 'Shot', shot['id'], {'code': value})
    with pytest.raises(MockgunError, match=r'Shot\.code'):
        await _read(query_site, version, 'entity')
    assert await _read(query_site, version, 'entity.Shot.id') == shot['id']


@pytest.mark.anyio
@pytest.mark.parametrize(
    'surface', ['read', 'create', 'update', 'batch', 'deep', 'reverse', 'multi']
)
async def test_named_link_surfaces(query_site: FakeSiteCase, surface: str) -> None:
    project = await create_link(query_site, 'Project', {'name': 'Alpha'})
    sequence = await create_link(query_site, 'Sequence', {'code': 'seq-a', 'project': project})
    shot = await create_link(
        query_site, 'Shot', {'code': 'S010', 'project': project, 'sg_sequence': sequence}
    )
    version = await create_link(query_site, 'Version', {'code': 'v001', 'entity': shot})
    playlist = await create_link(query_site, 'Playlist', {'code': 'pl-a', 'versions': [version]})
    expected: object = {**shot, 'name': 'S010'}
    if surface == 'read':
        await query_site.call('update', 'Shot', shot['id'], {'code': 'S020'})
        actual = await _read(query_site, version, 'entity')
        expected = {**shot, 'name': 'S020'}
    elif surface == 'create':
        actual = cast(
            'dict[str, object]',
            await query_site.call('create', 'Version', {'code': 'v4', 'entity': shot}, ['entity']),
        )['entity']
    elif surface == 'update':
        actual = cast(
            'dict[str, object]',
            await query_site.call('update', 'Version', version['id'], {'entity': shot}),
        )['entity']
    elif surface == 'batch':
        results = cast(
            'list[dict[str, object]]',
            await query_site.call(
                'batch',
                [
                    {
                        'request_type': 'create',
                        'entity_type': 'Version',
                        'data': {'code': 'v4', 'entity': shot},
                    },
                    {
                        'request_type': 'update',
                        'entity_type': 'Playlist',
                        'entity_id': playlist['id'],
                        'data': {'versions': [version]},
                    },
                ],
            ),
        )
        actual = [results[0]['entity'], results[1]['versions']]
        expected = [expected, [{**version, 'name': 'v001'}]]
    elif surface == 'deep':
        actual = await _read(query_site, version, 'entity.Shot.sg_sequence')
        expected = {**sequence, 'name': 'seq-a'}
    elif surface == 'reverse':
        published = await create_link(
            query_site, 'PublishedFile', {'code': 'pf-code', 'version': version}
        )
        actual = await _read(query_site, version, 'published_files')
        expected = [{**published, 'name': 'pf-code'}]
    else:
        actual = await _read(query_site, playlist, 'versions')
        expected = [{**version, 'name': 'v001'}]
    assert actual == expected


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update', 'read'])
async def test_cache_ignores_supplied_values(query_site: FakeSiteCase, operation: str) -> None:
    result = cast(
        'dict[str, object]',
        await query_site.call(
            'create',
            'Shot',
            {'code': 'S1', 'cached_display_name': 'other'},
            ['cached_display_name'],
        ),
    )
    if operation == 'update':
        result = cast(
            'dict[str, object]',
            await query_site.call(
                'update', 'Shot', result['id'], {'cached_display_name': 'again'}
            ),
        )
    elif operation == 'read':
        await query_site.call('update', 'Shot', result['id'], {'code': 'S2'})
        result['cached_display_name'] = await _read(query_site, result, 'cached_display_name')
    assert result['cached_display_name'] == ('S2' if operation == 'read' else 'S1')
    stored = query_site.client._server.store.row('Shot', entity_id(result))
    assert stored is not None
    assert stored['cached_display_name'] is None


@pytest.mark.anyio
@pytest.mark.parametrize('sudo', [False, True])
async def test_actor_names(query_site: FakeSiteCase, sudo: bool) -> None:
    if sudo:
        raw_call(
            query_site.client._server,
            'create',
            {
                'type': 'Shot',
                'fields': [{'field_name': 'code', 'value': 'sudo'}],
                'return_fields': [],
            },
            auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
        )
        expected = 'artist'
    else:
        await create_link(query_site, 'Shot', {'code': 'script'})
        expected = 'queries'
    row = cast('dict[str, object]', await query_site.call('find_one', 'Shot', [], ['created_by']))
    assert cast('dict[str, object]', row['created_by'])['name'] == expected


@pytest.mark.anyio
@pytest.mark.parametrize('target', ['human', 'missing', 'project'])
async def test_unmodeled_links(fake_site: FakeSiteCase, target: str) -> None:
    if target == 'human':
        link = await create_link(fake_site, 'HumanUser', {'login': 'human'})
        shot = await create_link(fake_site, 'Shot', {'reviewers': [link]})
        assert await _read(fake_site, shot, 'reviewers') == [link]
    else:
        link = (
            {'type': 'Project', 'id': 99}
            if target == 'missing'
            else await create_link(fake_site, 'Project', {'name': 'P'})
        )
        shot = await create_link(fake_site, 'Shot', {'project': link})
        assert await _read(fake_site, shot, 'project') == (
            link if target == 'missing' else {**link, 'name': 'P'}
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('owner', 'field', 'target_data', 'name'),
    [
        ('Shot', 'project', ('Project', {'name': 'Alpha'}), 'Alpha'),
        ('Shot', 'sg_sequence', ('Sequence', {'code': 'seq-a'}), 'seq-a'),
        ('Version', 'sg_task', ('Task', {'content': 'Layout'}), 'Layout'),
        ('PublishedFile', 'entity', ('Shot', {'code': 'S010'}), 'S010'),
        ('PublishedFile', 'version', ('Version', {'code': 'v001'}), 'v001'),
        (
            'PublishedFile',
            'published_file_type',
            ('PublishedFileType', {'code': 'Maya Scene'}),
            'Maya Scene',
        ),
        ('CutItem', 'cut', ('Cut', {'code': 'cut-a'}), 'cut-a'),
        ('CutItem', 'shot', ('Shot', {'code': 'S010'}), 'S010'),
        ('Task', 'step', ('Step', {'code': 'Comp', 'list_order': 10}), 'Comp'),
        (
            'Reply',
            'entity',
            ('Note', {'subject': 'Review', 'content': 'Looks good'}),
            'Review - Looks good',
        ),
    ],
)
async def test_declared_named_edges(
    query_site: FakeSiteCase,
    owner: str,
    field: str,
    target_data: tuple[str, dict[str, object]],
    name: str,
) -> None:
    target, data = target_data
    link = await create_link(query_site, target, data)
    display = 'content' if owner in {'Task', 'Reply'} else 'code'
    record = await create_link(query_site, owner, {display: 'owner', field: link})
    assert await _read(query_site, record, field) == {**link, 'name': name}


@pytest.mark.anyio
async def test_reverse_cut_item_name(query_site: FakeSiteCase) -> None:
    cut = await create_link(query_site, 'Cut', {'code': 'cut-a'})
    item = await create_link(query_site, 'CutItem', {'code': 'ci-1', 'cut': cut})
    assert await _read(query_site, cut, 'cut_items') == [{**item, 'name': 'ci-1'}]


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update', 'batch'])
async def test_unsettled_cached_name_write_is_atomic(
    query_site: FakeSiteCase, operation: str
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S010'})
    if operation == 'create':
        action = query_site.call('create', 'Shot', {'code': None}, ['cached_display_name'])
    elif operation == 'update':
        action = query_site.call(
            'update', 'Shot', shot['id'], {'code': None, 'cached_display_name': 'ignored'}
        )
    else:
        action = query_site.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': shot['id'],
                    'data': {'code': 'changed'},
                },
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {'code': None, 'cached_display_name': 'ignored'},
                },
            ],
        )
    with pytest.raises(MockgunError, match=r'Shot\.code'):
        await action
    assert await query_site.call('find', 'Shot', [], ['code']) == [{**shot, 'code': 'S010'}]
