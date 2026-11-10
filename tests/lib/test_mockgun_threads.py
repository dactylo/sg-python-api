from datetime import timedelta
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id

from .conftest import QUERY_NOW, FakeSiteCase, raw_call, seed, write_upload

pytestmark = pytest.mark.anyio


async def _note(case: FakeSiteCase) -> tuple[dict[str, object], dict[str, object]]:
    project = cast('dict[str, object]', await case.call('create', 'Project', {'name': 'P'}))
    note = cast(
        'dict[str, object]',
        await case.call('create', 'Note', {'project': project, 'subject': 's', 'content': 'c'}),
    )
    return project, note


async def test_ascii_text_attachment_thread_file_selection(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    case = workflow_site
    _, note = await _note(case)
    identity = await case.call('upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a'))
    attachment = await case.call('find_one', 'Attachment', [['id', 'is', identity]], ['this_file'])
    assert isinstance(attachment, dict)
    rows = cast(
        'list[dict[str, object]]',
        await case.call('note_thread_read', note['id'], {'Attachment': ['this_file']}),
    )
    selected = [row for row in rows if row['type'] == 'Attachment' and row['id'] == identity]
    assert len(selected) == 1
    file_value = cast('dict[str, object]', selected[0]['this_file'])
    direct = cast('dict[str, object]', attachment['this_file'])
    assert (
        set(file_value)
        == set(direct)
        == {'url', 'name', 'content_type', 'link_type', 'type', 'id'}
    )
    for field in ('name', 'content_type', 'link_type', 'type', 'id'):
        assert file_value[field] == direct[field]
    url = urlsplit(cast('str', file_value['url']))
    assert url.scheme == 'https'
    assert url.hostname == 'mockgun-storage.invalid'
    assert str(identity) not in url.path


async def test_attachment_membership_and_retirement(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    case = workflow_site
    _, note = await _note(case)
    reply = cast(
        'dict[str, object]', await case.call('create', 'Reply', {'entity': note, 'content': 'r'})
    )
    to_reply = await case.call(
        'upload', 'Reply', reply['id'], write_upload(tmp_path, 'reply.txt', b'r')
    )
    rows = cast('list[dict[str, object]]', await case.call('note_thread_read', note['id']))
    assert to_reply not in [row['id'] for row in rows if row['type'] == 'Attachment']
    to_note = await case.call(
        'upload', 'Note', note['id'], write_upload(tmp_path, 'note.txt', b'n')
    )
    note2 = cast('dict[str, object]', await case.call('create', 'Note', {'subject': 'another'}))
    await case.call(
        'update', 'Note', note2['id'], {'attachments': [{'type': 'Attachment', 'id': to_note}]}
    )
    rows2 = cast('list[dict[str, object]]', await case.call('note_thread_read', note2['id']))
    assert [row['id'] for row in rows2 if row['type'] == 'Attachment'] == [to_note]
    await case.call('delete', 'Reply', reply['id'])
    assert all(
        row['id'] != reply['id']
        for row in cast('list[dict[str, object]]', await case.call('note_thread_read', note['id']))
        if row['type'] == 'Reply'
    )
    await case.call('delete', 'Attachment', to_note)
    assert all(
        row['type'] != 'Attachment'
        for row in cast(
            'list[dict[str, object]]', await case.call('note_thread_read', note2['id'])
        )
    )
    await case.call('delete', 'Note', note['id'])
    for identity in (note['id'], entity_id(note) + 1000):
        with pytest.raises(Fault, match='Note'):
            await case.call('note_thread_read', identity)
        raw = raw_call(case.client._server, 'note_thread_contents', {'note_id': identity})
        assert raw['error_code'] == 103


async def test_empty_type_accepts_declared_selection_and_reply_audit_omission(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    _, note = await _note(case)
    rows = cast(
        'list[dict[str, object]]',
        await case.call(
            'note_thread_read',
            note['id'],
            {'Attachment': ['filename'], 'Reply': ['content', 'created_by']},
        ),
    )
    assert [row['type'] for row in rows] == ['Note']


async def test_image_attachment_thread_file_selection_stays_unknown(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    case = workflow_site
    _, note = await _note(case)
    await case.call('upload', 'Note', note['id'], write_upload(tmp_path, 'a.png', b'png'))
    with pytest.raises(MockgunError, match=r'Attachment\.this_file'):
        await case.call('note_thread_read', note['id'], {'Attachment': ['this_file']})


async def test_mixed_type_tie_and_non_null_reply_image_guard(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    case = workflow_site
    _, note = await _note(case)
    reply = cast(
        'dict[str, object]', await case.call('create', 'Reply', {'entity': note, 'content': 'r'})
    )
    await case.call('upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a'))
    with pytest.raises(MockgunError, match='note_thread_contents'):
        await case.call('note_thread_read', note['id'])
    await case.call('delete', 'Reply', reply['id'])
    artist = seed(case, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=1))
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    await sudo.call('create', 'Reply', {'entity': note, 'content': 'r2'})
    case.client._server.store.require('HumanUser', entity_id(artist))['image'] = 'image.jpg'
    with pytest.raises(MockgunError, match=r'HumanUser\.image'):
        await case.call('note_thread_read', note['id'])
    await sudo.call('close')


@pytest.mark.parametrize(
    ('changes', 'fragment'),
    [
        ({'note_id': None}, 'note_id'),
        ({'note_id': True}, 'note_id'),
        ({'note_id': '1'}, 'note_id'),
        ({'unexpected': None}, 'unexpected'),
        ({'entity_fields': []}, 'entity_fields'),
        ({'entity_fields': {'Shot': []}}, 'entity_fields Shot'),
        ({'entity_fields': {'Note': 'content'}}, 'entity_fields Note'),
        ({'entity_fields': {'Note': [1]}}, 'entity_fields Note'),
    ],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_raw_thread_rejects_malformed_parameters(
    workflow_site: FakeSiteCase, changes: dict[str, object], fragment: str
) -> None:
    case = workflow_site
    _, note = await _note(case)
    before = await case.call('note_thread_read', note['id'])
    with pytest.raises(MockgunError, match=f'note_thread_contents.*{fragment}'):
        raw_call(case.client._server, 'note_thread_contents', {'note_id': note['id'], **changes})
    assert await case.call('note_thread_read', note['id']) == before


async def test_same_instant_replies_use_id_order(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    _, note = await _note(case)
    requests = [
        {
            'request_type': 'create',
            'entity_type': 'Reply',
            'data': {'entity': note, 'content': text},
        }
        for text in ('first', 'second')
    ]
    replies = cast('list[dict[str, object]]', await case.call('batch', requests))
    rows = cast('list[dict[str, object]]', await case.call('note_thread_read', note['id']))
    assert [row['id'] for row in rows[1:]] == [replies[0]['id'], replies[1]['id']]


async def test_sudo_reply_user(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    _, note = await _note(case)
    artist = seed(case, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    reply = cast(
        'dict[str, object]', await sudo.call('create', 'Reply', {'entity': note, 'content': 'r'})
    )
    rows = cast('list[dict[str, object]]', await case.call('note_thread_read', note['id']))
    assert rows[1]['id'] == reply['id']
    assert rows[1]['user'] == {
        'id': artist['id'],
        'name': 'artist',
        'type': 'HumanUser',
        'image': None,
    }
    await sudo.call('close')


async def test_thread_order_keys_and_selected_fields(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    case = workflow_site
    _, note = await _note(case)
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=2))
    reply1 = cast(
        'dict[str, object]', await case.call('create', 'Reply', {'entity': note, 'content': 'r1'})
    )
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=4))
    attachment = await case.call(
        'upload',
        'Note',
        note['id'],
        write_upload(tmp_path, 'mgc-plain.txt', b'x'),
        display_name='Shown',
    )
    case.client.site.set_time(QUERY_NOW + timedelta(seconds=6))
    reply2 = cast(
        'dict[str, object]', await case.call('create', 'Reply', {'entity': note, 'content': 'r2'})
    )
    rows = cast('list[dict[str, object]]', await case.call('note_thread_read', note['id']))
    assert [list(row) for row in rows] == [
        ['type', 'id', 'content', 'created_at', 'created_by'],
        ['type', 'id', 'content', 'user', 'created_at'],
        ['type', 'id', 'created_by', 'created_at'],
        ['type', 'id', 'content', 'user', 'created_at'],
    ]
    assert rows == [
        {
            'type': 'Note',
            'id': note['id'],
            'content': 'c',
            'created_at': QUERY_NOW,
            'created_by': {'id': 1, 'name': 'queries', 'type': 'ApiUser'},
        },
        {
            'type': 'Reply',
            'id': reply1['id'],
            'content': 'r1',
            'user': {'id': 1, 'name': 'queries', 'type': 'ApiUser', 'image': None},
            'created_at': QUERY_NOW + timedelta(seconds=2),
        },
        {
            'type': 'Attachment',
            'id': attachment,
            'created_by': {'id': 1, 'name': 'queries', 'type': 'ApiUser'},
            'created_at': QUERY_NOW + timedelta(seconds=4),
        },
        {
            'type': 'Reply',
            'id': reply2['id'],
            'content': 'r2',
            'user': {'id': 1, 'name': 'queries', 'type': 'ApiUser', 'image': None},
            'created_at': QUERY_NOW + timedelta(seconds=6),
        },
    ]
    selected = cast(
        'list[dict[str, object]]',
        await case.call(
            'note_thread_read',
            note['id'],
            {
                'Note': ['subject', 'user'],
                'Reply': ['content', 'user', 'created_by'],
                'Attachment': ['filename'],
            },
        ),
    )
    assert list(selected[0]) == [*list(rows[0]), 'subject', 'user']
    assert selected[0]['subject'] == 's'
    assert selected[0]['user'] == {'id': 1, 'name': 'queries', 'type': 'ApiUser'}
    assert selected[1] == rows[1]
    assert list(selected[2]) == [*list(rows[2]), 'filename']
    assert selected[2]['filename'] == 'mgc-plain.txt'


@pytest.mark.parametrize(
    ('fields', 'fragment'),
    [
        ({'Note': ['note_links']}, 'Note.note_links'),
        ({'Note': ['updated_by']}, 'Note.updated_by'),
        ({'Shot': ['code']}, 'Shot'),
        ({'Note': ['missing']}, 'Note.missing'),
    ],
)
async def test_unsettled_selected_fields(
    workflow_site: FakeSiteCase,
    tmp_path: Path,
    fields: dict[str, list[str]],
    fragment: str,
) -> None:
    case = workflow_site
    _, note = await _note(case)
    await case.call('upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a'))
    with pytest.raises(MockgunError, match=fragment):
        await case.call('note_thread_read', note['id'], fields)


@pytest.mark.parametrize(
    ('fields', 'fragment'),
    [
        ({'Attachment': ['missing']}, r'Attachment\.missing'),
        ({'Reply': ['content.deep']}, r'Reply\.content\.deep'),
        ({'Reply': ['updated_by']}, r'Reply\.updated_by'),
        ({'Attachment': ['attachment_links']}, r'Attachment\.attachment_links'),
    ],
)
async def test_empty_type_rejects_unsettled_selected_field(
    workflow_site: FakeSiteCase, fields: dict[str, list[str]], fragment: str
) -> None:
    case = workflow_site
    _, note = await _note(case)
    with pytest.raises(MockgunError, match=fragment):
        await case.call('note_thread_read', note['id'], fields)


@pytest.mark.parametrize('selected', [['entity'], ['entity', 'created_by']])
async def test_reply_entity_thread_selection_is_omitted(
    workflow_site: FakeSiteCase, selected: list[str]
) -> None:
    case = workflow_site
    _, note = await _note(case)
    reply = cast(
        'dict[str, object]', await case.call('create', 'Reply', {'entity': note, 'content': 'r'})
    )
    direct = cast(
        'dict[str, object]',
        await case.call('find_one', 'Reply', [['id', 'is', reply['id']]], ['entity']),
    )
    assert cast('dict[str, object]', direct['entity'])['id'] == note['id']
    rows = cast(
        'list[dict[str, object]]',
        await case.call('note_thread_read', note['id'], {'Reply': selected}),
    )
    assert rows[1]['id'] == reply['id']
    assert 'entity' not in rows[1]
    assert 'created_by' not in rows[1]


@pytest.mark.parametrize('human_reply', [False, True])
async def test_reply_entity_selection_preserves_unobserved_actor_forms(
    workflow_site: FakeSiteCase, human_reply: bool
) -> None:
    case = workflow_site
    _, note = await _note(case)
    seed(case, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    try:
        creator = sudo if human_reply else case
        reader = case if human_reply else sudo
        await creator.call('create', 'Reply', {'entity': note, 'content': 'r'})
        rows = cast(
            'list[dict[str, object]]',
            await reader.call('note_thread_read', note['id'], {'Reply': ['entity']}),
        )
        assert cast('dict[str, object]', rows[1]['entity'])['id'] == note['id']
    finally:
        await sudo.call('close')
