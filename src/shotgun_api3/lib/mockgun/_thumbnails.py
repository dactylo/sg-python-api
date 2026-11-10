"""Apply observed thumbnail identities, sharing and owner side effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit

from ._activity import taint_activity
from ._errors import MockgunError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._server import FakeServer
    from ._sessions import RequestContext
    from ._storage import Allocation
    from ._store import Row


def _owner(server: FakeServer, kind: str, identity: int, field: str, route: str) -> Row:
    """Require an active writable owner with the selected image declaration."""
    row = server.store.row(kind, identity) if server.schema.declares(kind) else None
    if row is None or row['__retired'] or not server.schema.declares(kind, field):
        raise MockgunError(
            f'mockgun POST {route} {kind} {identity} field {field}: '
            'missing or retired owner or image declaration is unsettled'
        )
    server.store.check_writable(row)
    return row


def _reference(server: FakeServer, value: str) -> Row:
    """Resolve a share form reference to an active image owner."""
    kind, identity = value.rsplit('_', 1)
    return _owner(server, kind, int(identity), 'image', '/upload/share_thumbnail')


def _set_image(server: FakeServer, row: Row, field: str, key: str | None, route: str) -> None:
    """Replace an image while invalidating its previous identity and side effects."""
    store = server.store
    kind, identity = cast('str', row['type']), cast('int', row['id'])
    store.remember(row)
    row[field] = server.storage.image_url(key) if key is not None else None
    store.forget_unknown(row, field)
    store.side_set('image_identity', (kind, identity, field), None)
    reason = (
        f'POST {route} {kind} {identity} field {field}: audit, events and activity are unsettled'
    )
    for name in ('updated_at', 'updated_by'):
        if server.schema.declares(kind, name):
            store.mark_unknown(row, name, reason)
    store.taint_events(reason)
    taint_activity(store, [row], reason)


def link_thumbnail(
    server: FakeServer, allocation: Allocation, form: Mapping[str, str], context: RequestContext
) -> str:
    """Link uploaded thumbnail bytes using an unreadable Attachment identity.

    Args:
        server: Site whose storage allocation and record are being linked.
        allocation: Issued Thumbnail allocation with completed bytes.
        form: Link form identifying an owner and optional filmstrip selection.
        context: Authenticated request actor and instant.

    Returns:
        The client's successful link grammar containing the reserved identity.

    Raises:
        MockgunError: If owner, field or optional metadata behavior is
            unsettled.
    """
    del context
    route = '/upload/api_link_file'
    kind, identity = form['entity_type'], int(form['entity_id'])
    if form.keys() - {'entity_type', 'entity_id', 'filmstrip', 'upload_link_info'} or (
        'filmstrip' in form and form['filmstrip'] != 'true'
    ):
        raise MockgunError(
            f'mockgun POST {route} {kind} {identity} thumbnail fields: metadata is unsettled'
        )
    field = 'filmstrip_image' if form.get('filmstrip') == 'true' else 'image'
    row = _owner(server, kind, identity, field, route)
    if not server.schema.declares('Attachment'):
        raise MockgunError(
            f'mockgun POST {route} {kind} {identity} field {field}: Attachment schema is missing'
        )
    # C6/C7 P4-14: the shared counter advances without creating an Attachment row.
    with server.store.transaction():
        thumbnail_id = server.store.reserve_id('Attachment')
        _set_image(server, row, field, allocation.key, route)
        server.store.side_set('image_identity', (kind, identity, field), thumbnail_id)
    return f'1:{thumbnail_id}'


def share_thumbnail(server: FakeServer, form: Mapping[str, str], context: RequestContext) -> str:
    """Share an uploaded source image and return its stable thumbnail identity.

    Args:
        server: Site containing source and target owners and stored bytes.
        form: Entity references and filmstrip flag from the source-share route.
        context: Authenticated request actor and instant.

    Returns:
        Success with the source identity, or the observed null-source failure.

    Raises:
        MockgunError: If filmstrip, owner or source identity behavior is
            unsettled.
    """
    del context
    route = '/upload/share_thumbnail'
    if form['filmstrip_thumbnail'] != 'False':
        raise MockgunError(
            f'mockgun POST {route} {form["source_entity"]} filmstrip_thumbnail: '
            'share identity is unsettled'
        )
    source = _reference(server, form['source_entity'])
    targets = [_reference(server, value) for value in form['entities'].split(',')]
    server.store.check_known(source, 'image')
    kind, identity = cast('str', source['type']), cast('int', source['id'])
    image = source.get('image')
    # C8 Q4: the empty Asset source clears one populated, non-filmstrip Asset.
    if image is None:
        if (
            kind == 'Asset'
            and len(targets) == 1
            and targets[0]['type'] == 'Asset'
            and isinstance(targets[0].get('image'), str)
            and targets[0].get('filmstrip_image') is None
        ):
            with server.store.transaction():
                _set_image(server, targets[0], 'image', None, route)
        return f'0:mockgun share failed: {kind} {identity} has no thumbnail'
    thumbnail_id = server.store.side_get('image_identity', (kind, identity, 'image'))
    if not isinstance(thumbnail_id, int) or not isinstance(image, str):
        raise MockgunError(
            f'mockgun POST {route} {kind} {identity} image: source identity is unsettled'
        )
    key = urlsplit(image).path.lstrip('/')
    # C7 P4-19: every share retains the path and source ID, with fresh URL tokens.
    with server.store.transaction():
        for target in targets:
            _set_image(server, target, 'image', key, route)
    return f'1:{thumbnail_id}'
