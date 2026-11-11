"""Read observed Note threads from the fake site's records."""

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from itertools import pairwise
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._links import named_link
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store
from ._wire import format_wire_datetime

_DEFAULT_FIELDS = {
    'Attachment': ('type', 'id', 'created_by', 'created_at'),
    'Note': ('type', 'id', 'content', 'created_at', 'created_by'),
    'Reply': ('type', 'id', 'content', 'user', 'created_at'),
}


def _field_type(store: Store, kind: str, field: str) -> str:
    """Reject structurally unsettled selections even without rows."""
    prefix = f'mockgun cannot note_thread_contents {kind}.{field}'
    if field == 'updated_by':
        raise MockgunError(f'{prefix}: the audit link is unsettled')
    if '.' in field or not store.schema.declares(kind, field):
        raise MockgunError(f'{prefix}: the field is undeclared or deep')
    data_type = store.schema.field_type(kind, field)
    if data_type == 'multi_entity':
        raise MockgunError(f'{prefix}: multi-entity selection is unsettled')
    return data_type


def _fields(store: Store, params: Mapping[str, object]) -> dict[str, list[str]]:
    """Validate requested thread selections before reading any row."""
    extra = set(params) - {'note_id', 'entity_fields'}
    if extra:
        raise MockgunError(f'mockgun cannot note_thread_contents parameter {min(extra)}')
    raw = params.get('entity_fields', {})
    if not isinstance(raw, dict):
        raise MockgunError('mockgun cannot note_thread_contents parameter entity_fields')
    result: dict[str, list[str]] = {}
    for kind, names in cast('dict[object, object]', raw).items():
        if (
            kind not in _DEFAULT_FIELDS
            or not isinstance(names, list)
            or not all(isinstance(name, str) for name in cast('list[object]', names))
        ):
            raise MockgunError(f'mockgun cannot note_thread_contents entity_fields {kind}')
        entity_type = cast('str', kind)
        selected = cast('list[str]', names)
        for field in selected:
            if field not in _DEFAULT_FIELDS[entity_type] and not (
                entity_type == 'Reply' and field == 'created_by'
            ):
                _field_type(store, entity_type, field)
        result[entity_type] = selected
    return result


def _unsettled(row: Row, field: str, reason: str) -> MockgunError:
    """Name the thread field and record whose value cannot be reproduced."""
    return MockgunError(
        f'mockgun cannot note_thread_contents {row["type"]}.{field} '
        f'on record {row["id"]}: {reason}'
    )


def _known(store: Store, row: Row, field: str) -> None:
    """Include the thread RPC in a stored unknown marker's diagnostic."""
    try:
        store.check_known(row, field)
    except MockgunError as exc:
        raise _unsettled(row, field, str(exc)) from exc


def _entries(store: Store, note: Row) -> list[Row]:
    """Collect active Replies and Note-member Attachments."""
    identity = note['id']
    _known(store, note, 'attachments')
    members = cast('list[dict[str, FieldValue]]', note.get('attachments', []))
    entries: list[Row] = []
    if store.schema.declares('Reply'):
        for row in store.rows('Reply', retired=False):
            _known(store, row, 'entity')
            if row.get('entity') == {'type': 'Note', 'id': identity}:
                entries.append(row)
    if store.schema.declares('Attachment'):
        for row in store.rows('Attachment', retired=False):
            _known(store, row, 'attachment_links')
            linked = {'type': 'Note', 'id': identity} in cast(
                'list[dict[str, FieldValue]]', row.get('attachment_links', [])
            )
            member = {'type': 'Attachment', 'id': row['id']} in members
            if linked and not member:
                raise _unsettled(row, 'attachment_links', 'the Note does not list this attachment')
            if member:
                entries.append(row)
    return entries


def _named(store: Store, row: Row, field: str) -> dict[str, object] | None:
    """Return an observed named single link in thread key order."""
    _known(store, row, field)
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _unsettled(row, field, 'the link shape is unsettled')
    try:
        link = named_link(store, cast('dict[str, FieldValue]', value))
    except (MockgunError, ServerFaultError) as exc:
        raise _unsettled(row, field, str(exc)) from exc
    if 'name' not in link:
        raise _unsettled(row, field, 'the linked name is unsettled')
    return {'id': link['id'], 'name': link['name'], 'type': link['type']}


def _ordered(store: Store, entries: list[Row]) -> list[Row]:
    """Order thread entries while rejecting unobserved timestamp forms."""
    for row in entries:
        if not store.schema.declares(cast('str', row['type']), 'created_at') and len(entries) > 1:
            raise _unsettled(row, 'created_at', 'the timestamp is undeclared')
        _known(store, row, 'created_at')
        if not isinstance(row.get('created_at'), datetime):
            raise _unsettled(row, 'created_at', 'the timestamp is unsettled')
    entries.sort(key=lambda row: (cast('datetime', row['created_at']), cast('int', row['id'])))
    for earlier, later in pairwise(entries):
        if earlier['created_at'] == later['created_at'] and earlier['type'] != later['type']:
            raise MockgunError(
                f'mockgun cannot note_thread_contents {earlier["type"]} {earlier["id"]} '
                f'and {later["type"]} {later["id"]}: mixed-type timestamp tie is unsettled'
            )
    return entries


def _reply_user(store: Store, row: Row) -> dict[str, object]:
    """Include the observed null image on a Reply actor link."""
    link = _named(store, row, 'user')
    if link is None:
        raise _unsettled(row, 'user', 'the null actor is unsettled')
    user_type = cast('str', link['type'])
    user_id = cast('int', link['id'])
    user = store.row(user_type, user_id)
    if user is not None and store.schema.declares(user_type, 'image'):
        _known(store, user, 'image')
        if user.get('image') is not None:
            raise _unsettled(row, 'user', f'{user_type}.image is non-null')
    return {**link, 'image': None}


def _value(store: Store, row: Row, field: str) -> object:
    """Project one declared thread field without exposing unknown state."""
    kind = cast('str', row['type'])
    data_type = _field_type(store, kind, field)
    if kind == 'Reply' and field == 'user':
        return _reply_user(store, row)
    if data_type == 'entity':
        link = _named(store, row, field)
        if field == 'created_by' and link is None:
            raise _unsettled(row, field, 'the null actor is unsettled')
        return link
    _known(store, row, field)
    value = row.get(field)
    if data_type == 'date_time' and value is not None:
        if not isinstance(value, datetime):
            raise _unsettled(row, field, 'the timestamp is unsettled')
        return format_wire_datetime(value)
    return deepcopy(value)


def _row(store: Store, row: Row, fields: list[str], *, script_reader: bool) -> dict[str, object]:
    """Build default thread keys followed by requested fields."""
    kind = cast('str', row['type'])
    result: dict[str, object] = {}
    for field in _DEFAULT_FIELDS[kind]:
        result[field] = row[field] if field in {'type', 'id'} else _value(store, row, field)
    for field in fields:
        if field in result or (kind == 'Reply' and field == 'created_by'):
            continue
        user = row.get('user')
        if (
            kind == 'Reply'
            and field == 'entity'
            and script_reader
            and isinstance(user, dict)
            and user.get('type') == 'ApiUser'
        ):
            continue
        result[field] = _value(store, row, field)
    return result


def read_thread(
    store: Store, params: Mapping[str, object], *, context: RequestContext
) -> list[dict[str, object]]:
    """Return one Note followed by its active, time-ordered thread entries.

    Args:
        store: Site records and schema for the current request.
        params: Wire `note_id` and optional per-type `entity_fields`.
        context: Request context captured for this RPC.

    Returns:
        Detached rows in the observed Note thread wire shape.

    Raises:
        MockgunError: If a requested value or ordering is unsettled.
        ServerFaultError: If the Note is absent or retired.
    """
    script_reader = context.session_kind == 'script'
    if not store.schema.declares('Note'):
        raise MockgunError('mockgun cannot note_thread_contents Note: schema is undeclared')
    identity = params.get('note_id')
    if not isinstance(identity, int) or isinstance(identity, bool):
        raise MockgunError('mockgun cannot note_thread_contents parameter note_id')
    selections = _fields(store, params)
    note = store.row('Note', identity)
    if note is None or note['__retired']:
        raise ServerFaultError(f'Note {identity} not found', error_code=103)
    entries = _ordered(store, _entries(store, note))
    return [_row(store, note, selections.get('Note', []), script_reader=script_reader)] + [
        _row(store, row, selections.get(cast('str', row['type']), []), script_reader=script_reader)
        for row in entries
    ]
