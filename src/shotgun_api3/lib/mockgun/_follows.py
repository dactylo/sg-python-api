"""Emulate observed follow operations and Note auto-follow side effects."""

from collections.abc import Mapping
from copy import deepcopy
from typing import cast

from ._errors import MockgunError
from ._links import display_name
from ._sessions import RequestContext
from ._store import Row, Store


def _reference(store: Store, operation: str, value: object, role: str) -> tuple[str, int, Row]:
    """Require an active declared record for a follow argument."""
    if not isinstance(value, dict):
        raise MockgunError(f'mockgun cannot {operation} {role}: the entity link is unsettled')
    link = cast('dict[str, object]', value)
    kind = link.get('type')
    identity = link.get('id')
    if not isinstance(kind, str) or not isinstance(identity, int) or isinstance(identity, bool):
        raise MockgunError(
            f'mockgun cannot {operation} {role} {kind} {identity}: invalid identity'
        )
    if role == 'user' and kind != 'HumanUser':
        raise MockgunError(
            f'mockgun cannot {operation} user {kind} {identity}: expected HumanUser'
        )
    if not store.schema.declares(kind):
        raise MockgunError(f'mockgun cannot {operation} {kind} {identity}: undeclared type')
    row = store.row(kind, identity)
    if row is None or row['__retired']:
        raise MockgunError(f'mockgun cannot {operation} {kind} {identity}: missing or retired')
    return kind, identity, row


def _pair(store: Store, operation: str, params: Mapping[str, object]) -> tuple[int, str, int]:
    """Validate the user and target of a follow write."""
    _, human_id, _ = _reference(store, operation, params.get('user'), 'user')
    kind, identity, _ = _reference(store, operation, params.get('entity'), 'entity')
    return human_id, kind, identity


def _project(store: Store, params: Mapping[str, object]) -> dict[str, object] | None:
    """Validate an optional Project restriction for a following query."""
    project = params.get('project')
    if project is None:
        return None
    kind, identity, _ = _reference(store, 'following', project, 'project')
    if kind != 'Project':
        raise MockgunError(f'mockgun cannot following project {kind} {identity}: expected Project')
    return {'type': 'Project', 'id': identity}


def follow(store: Store, params: Mapping[str, object]) -> dict[str, object]:
    """Record an idempotent HumanUser follow and echo the supplied links.

    Args:
        store: Site records and journaled side tables.
        params: RPC user and entity links.

    Returns:
        The observed follow result with the caller's link shapes.

    Raises:
        MockgunError: If the user or target cannot be resolved to an active,
            declared record in the supported follow form.
    """
    followed = True
    store.side_set('follows', _pair(store, 'follow', params), followed)
    return {
        'followed': True,
        'user': deepcopy(params['user']),
        'entity': deepcopy(params['entity']),
    }


def follow_created_note(store: Store, row: Row, *, context: RequestContext) -> None:
    """Follow a created Note or a Reply's Note for its HumanUser actor.

    Args:
        store: Site records and journaled side tables.
        row: Newly created Note or Reply.
        context: Actor for the enclosing create transaction.
    """
    actor = context.actor
    if actor is None or actor.entity_type != 'HumanUser':
        return
    if row['type'] == 'Note':
        note_id = cast('int', row['id'])
    elif row['type'] == 'Reply':
        entity = row.get('entity')
        if not isinstance(entity, dict) or entity.get('type') != 'Note':
            return
        note_id = cast('int', entity['id'])
    else:
        return
    followed = True
    store.side_set('follows', (actor.entity_id, 'Note', note_id), followed)


def followers(store: Store, params: Mapping[str, object]) -> list[dict[str, object]]:
    """List the observed zero or one HumanUser follower of an active record.

    Args:
        store: Site records and journaled side tables.
        params: RPC entity link.

    Returns:
        An empty list or a one-element list containing a follower dictionary
        with stable identity and observed key order.

    Raises:
        MockgunError: If the target or follower is unavailable, more than one
            follower exists, or required follower metadata is undeclared or
            unsettled.
    """
    kind, identity, _ = _reference(store, 'followers', params.get('entity'), 'entity')
    humans = store.followers(kind, identity)
    if len(humans) > 1:
        raise MockgunError(
            f'mockgun cannot followers {kind} {identity}: order of two followers is unsettled'
        )
    if not humans:
        return []
    if not store.schema.declares('HumanUser', 'sg_status_list'):
        raise MockgunError(
            f'mockgun cannot followers {kind} {identity}: HumanUser.sg_status_list is undeclared'
        )
    _, human_id, human = _reference(
        store, 'followers', {'type': 'HumanUser', 'id': humans[0]}, 'user'
    )
    store.check_known(human, 'sg_status_list')
    status = human.get('sg_status_list')
    name = display_name(store, 'HumanUser', human_id)
    if name is None or not isinstance(status, str):
        raise MockgunError(
            f'mockgun cannot followers {kind} {identity}: '
            f'HumanUser {human_id} name or status is unsettled'
        )
    return [
        {
            'name': name,
            'type': 'HumanUser',
            'id': human_id,
            'valid': 'valid',
            'status': status,
            'uuid': store.link_uuid('HumanUser', human_id),
        }
    ]


def following(store: Store, params: Mapping[str, object]) -> list[dict[str, object]]:
    """List active follows after observed type and project restrictions.

    Args:
        store: Site records and journaled side tables.
        params: RPC user link and optional project and entity type.

    Returns:
        Identity links in ascending ID for a single resulting type.

    Raises:
        MockgunError: If the user, project, or a reached followed record is
            unavailable, a required project field is undeclared or unknown, or
            the result contains multiple entity types with unsettled order.
    """
    _, human_id, _ = _reference(store, 'following', params.get('user'), 'user')
    entity_type = params.get('entity_type')
    if entity_type is not None and not isinstance(entity_type, str):
        raise MockgunError(f'mockgun cannot following entity_type {entity_type}: invalid type')
    project_link = _project(store, params)
    result: list[dict[str, object]] = []
    for key, _ in store.side_items('follows'):
        if key[0] != human_id:
            continue
        kind = cast('str', key[1])
        identity = cast('int', key[2])
        if entity_type is not None and kind != entity_type:
            continue
        _, _, row = _reference(store, 'following', {'type': kind, 'id': identity}, 'entity')
        if project_link is not None:
            if not store.schema.declares(kind, 'project'):
                raise MockgunError(
                    f'mockgun cannot following {kind} {identity}: project field is undeclared'
                )
            store.check_known(row, 'project')
            if row.get('project') != project_link:
                continue
        result.append({'type': kind, 'id': identity})
    if len({item['type'] for item in result}) > 1:
        raise MockgunError(
            f'mockgun cannot following HumanUser {human_id}: order across types is unsettled'
        )
    result.sort(key=lambda item: cast('int', item['id']))
    return result


def unfollow(store: Store, params: Mapping[str, object]) -> dict[str, object]:
    """Remove an idempotent follow and echo the supplied links.

    Args:
        store: Site records and journaled side tables.
        params: RPC user and entity links.

    Returns:
        The observed unfollow result with the caller's link shapes.

    Raises:
        MockgunError: If the user or target cannot be resolved to an active,
            declared record in the supported follow form.
    """
    store.side_set('follows', _pair(store, 'unfollow', params), None)
    return {
        'unfollowed': True,
        'user': deepcopy(params['user']),
        'entity': deepcopy(params['entity']),
    }
