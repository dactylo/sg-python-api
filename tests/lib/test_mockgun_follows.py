from typing import cast
from uuid import UUID

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.lib._events import cursor, delta

from .conftest import FakeSiteCase, raw_call, seed

_CREDENTIAL = 'x'
pytestmark = pytest.mark.anyio


async def _follower(
    case: FakeSiteCase, target: dict[str, object], human: dict[str, object]
) -> dict[str, object]:
    rows = cast('list[dict[str, object]]', await case.call('followers', target))
    assert len(rows) == 1
    row = rows[0]
    assert list(row) == ['name', 'type', 'id', 'valid', 'status', 'uuid']
    assert {key: row[key] for key in ('name', 'type', 'id', 'valid', 'status')} == {
        'name': 'artist',
        'type': 'HumanUser',
        'id': human['id'],
        'valid': 'valid',
        'status': 'act',
    }
    UUID(cast('str', row['uuid']))
    return row


def _scene(case: FakeSiteCase) -> tuple[dict[str, object], dict[str, object]]:
    project = seed(case, 'Project', {'name': 'P'})
    human = seed(case, 'HumanUser', {'login': 'artist', 'name': 'artist', 'sg_status_list': 'act'})
    return project, human


def _sudo(case: FakeSiteCase) -> FakeSiteCase:
    return FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))


async def test_auto_follow_and_follow_writes_do_not_emit_events(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project, human = _scene(case)
    shot = seed(case, 'Shot', {'project': project, 'code': 's'})
    quiet_shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'quiet'}))
    script_note = cast(
        'dict[str, object]',
        await case.call(
            'create',
            'Note',
            {'project': project, 'subject': 'script', 'content': 'b', 'note_links': [shot]},
        ),
    )
    assert await case.call('followers', script_note) == []
    sudo = _sudo(case)
    try:
        sudo_note = cast(
            'dict[str, object]',
            await sudo.call(
                'create', 'Note', {'note_links': [shot], 'subject': 'sudo', 'project': project}
            ),
        )
        await _follower(case, sudo_note, human)
        before = await cursor(case)
        stream = await case.call('activity_stream_read', 'Shot', quiet_shot['id'])
        await case.call('follow', human, quiet_shot)
        await case.call('unfollow', human, quiet_shot)
        assert await delta(case, before, ['event_type']) == []
        assert await case.call('activity_stream_read', 'Shot', quiet_shot['id']) == stream
        await sudo.call('create', 'Reply', {'entity': script_note, 'content': 'r'})
        await _follower(case, script_note, human)
    finally:
        await sudo.call('close')


async def test_auto_follow_enables_script_reply_read_event(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, human = _scene(case)
    shot = seed(case, 'Shot', {'project': project, 'code': 's'})
    sudo = _sudo(case)
    try:
        note = cast(
            'dict[str, object]',
            await sudo.call(
                'create', 'Note', {'note_links': [shot], 'subject': 'n', 'project': project}
            ),
        )
        await _follower(case, note, human)
        before = await cursor(case)
        await case.call('create', 'Reply', {'entity': note, 'content': 'r'})
        rows = await delta(case, before, ['event_type', 'attribute_name'])
        assert [row['attribute_name'] for row in rows] == [
            'read_by_current_user',
            None,
            'user',
            'entity',
            'replies',
            'content',
        ]
        assert [row['event_type'] for row in rows] == [
            'Shotgun_Reading_Change',
            'Shotgun_Reply_New',
            'Shotgun_Reply_Change',
            'Shotgun_Reply_Change',
            'Shotgun_Note_Change',
            'Shotgun_Reply_Change',
        ]
    finally:
        await sudo.call('close')


async def test_follow_guards_and_failed_batch_rollback(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, human = _scene(case)
    asset = seed(case, 'Asset', {'project': project, 'code': 'F'})
    other = seed(case, 'HumanUser', {'login': 'other', 'name': 'other', 'sg_status_list': 'act'})
    api = seed(case, 'ApiUser', {'firstname': 'script'})
    with pytest.raises(MockgunError, match=r'follow.*HumanUser'):
        await case.call('follow', api, asset)
    with pytest.raises(MockgunError, match=r'follow.*Asset'):
        await case.call('follow', human, {'type': 'Asset', 'id': 999})
    await case.call('follow', human, asset)
    await case.call('follow', other, asset)
    with pytest.raises(MockgunError, match=r'followers.*Asset'):
        await case.call('followers', asset)
    await case.call('delete', 'Asset', asset['id'])
    with pytest.raises(MockgunError, match=r'followers.*Asset'):
        await case.call('followers', asset)
    with pytest.raises(MockgunError, match=r'following.*Asset'):
        await case.call('following', human, entity_type='Asset')
    with pytest.raises(MockgunError, match=r'follow.*Asset'):
        await case.call('follow', human, asset)
    sudo = _sudo(case)
    try:
        with pytest.raises(Fault):
            await sudo.call(
                'batch',
                [
                    {
                        'request_type': 'create',
                        'entity_type': 'Note',
                        'data': {'project': project, 'subject': 'rolled back', 'content': 'b'},
                    },
                    {
                        'request_type': 'update',
                        'entity_type': 'Asset',
                        'entity_id': 999,
                        'data': {'code': 'missing'},
                    },
                ],
            )
        assert await case.call('following', human, entity_type='Note') == []
    finally:
        await sudo.call('close')


async def test_followers_reject_multiple_humans(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, first = _scene(case)
    second = seed(
        case, 'HumanUser', {'login': 'second', 'name': 'second', 'sg_status_list': 'act'}
    )
    asset = seed(case, 'Asset', {'project': project, 'code': 'a'})
    await case.call('follow', first, asset)
    await _follower(case, asset, first)
    await case.call('follow', second, asset)
    with pytest.raises(MockgunError, match=r'followers Asset .*two followers'):
        await case.call('followers', asset)
    await case.call('unfollow', second, asset)
    await _follower(case, asset, first)


async def test_following_filters_sort_and_reject_mixed_types(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, human = _scene(case)
    first = seed(case, 'Asset', {'project': project, 'code': 'F1'})
    second = seed(case, 'Asset', {'project': project, 'code': 'F2'})
    other_project = seed(case, 'Project', {'name': 'Other'})
    foreign = seed(case, 'Asset', {'project': other_project, 'code': 'F4'})
    shot = seed(case, 'Shot', {'project': project, 'code': 'F3'})
    for target in (second, first, foreign, shot):
        await case.call('follow', human, target)
    assert await case.call('following', human, entity_type='Asset') == [first, second, foreign]
    assert await case.call('following', human, entity_type='Shot') == [shot]
    assert await case.call('following', human, project=project, entity_type='Asset') == [
        first,
        second,
    ]
    with pytest.raises(MockgunError, match=r'following.*types'):
        await case.call('following', human)


@pytest.mark.parametrize('kind', ['Note', 'Reply'])
async def test_human_session_creation_follows_note(workflow_site: FakeSiteCase, kind: str) -> None:
    case = workflow_site
    _, human = _scene(case)
    actor = FakeSiteCase(case.kind, case.client.new_client(login='artist', password=_CREDENTIAL))
    try:
        if kind == 'Reply':
            note = cast('dict[str, object]', await case.call('create', 'Note', {'subject': 'n'}))
            assert await case.call('followers', note) == []
            await actor.call('create', 'Reply', {'entity': note, 'content': 'r'})
        else:
            note = cast('dict[str, object]', await actor.call('create', 'Note', {'subject': 'n'}))
        await _follower(case, note, human)
    finally:
        await actor.call('close')


@pytest.mark.parametrize('method', ['follow', 'unfollow', 'followers', 'following'])
@pytest.mark.parametrize('variant', ['non-dict', 'missing', 'extra', 'wrong-type'])
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_raw_follow_rpc_rejects_unsettled_parameter_shape_without_writes(
    workflow_site: FakeSiteCase, method: str, variant: str
) -> None:
    case = workflow_site
    project, human = _scene(case)
    asset = seed(case, 'Asset', {'project': project, 'code': 'F'})
    await case.call('follow', human, asset)
    params: dict[str, object] = {'entity': asset} if method == 'followers' else {'user': human}
    if method in {'follow', 'unfollow'}:
        params['entity'] = asset
    key = 'entity' if method != 'following' else 'user'
    if variant == 'non-dict':
        request: object = []
        fragment = 'parameter params'
    elif variant == 'missing':
        params.pop(key)
        request = params
        fragment = key
    elif variant == 'extra':
        params['unexpected'] = 1
        request = params
        fragment = 'unexpected'
    else:
        params[key] = 1
        request = params
        fragment = key
    store = case.client._server.store
    before = store.side_items('follows')
    with pytest.raises(MockgunError, match=rf'{method}.*{fragment}'):
        raw_call(case.client._server, method, request)
    assert store.side_items('follows') == before


@pytest.mark.parametrize(
    ('key', 'value'),
    [('project', []), ('entity_type', 1), ('project', None), ('entity_type', None)],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_raw_following_rejects_wrong_optional_types(
    workflow_site: FakeSiteCase, key: str, value: object
) -> None:
    case = workflow_site
    _, human = _scene(case)
    with pytest.raises(MockgunError, match=rf'following.*{key}'):
        raw_call(case.client._server, 'following', {'user': human, key: value})
    assert case.client._server.store.side_items('follows') == []


async def test_roundtrip_is_idempotent_and_has_stable_follower_identity(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project, human = _scene(case)
    asset = seed(case, 'Asset', {'project': project, 'code': 'F'})
    expected_follow = {'followed': True, 'user': human, 'entity': asset}
    assert await case.call('follow', human, asset) == expected_follow
    assert await case.call('follow', human, asset) == expected_follow
    first = await _follower(case, asset, human)
    assert await _follower(case, asset, human) == first
    assert await case.call('following', human) == [asset]
    expected_unfollow = {'unfollowed': True, 'user': human, 'entity': asset}
    assert await case.call('unfollow', human, asset) == expected_unfollow
    assert await case.call('unfollow', human, asset) == expected_unfollow
    assert await case.call('followers', asset) == []
