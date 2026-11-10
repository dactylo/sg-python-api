"""Record and render the observed activity stream forms."""

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, cast

from ._errors import MockgunError
from ._links import display_name
from ._query import project_rows
from ._retirement import RetirementEffects
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store
from ._wire import format_wire_datetime

if TYPE_CHECKING:
    from datetime import datetime

_READABLE_TYPES = frozenset({'Asset', 'Note', 'Project', 'Shot', 'Task'})
_ACTOR_PARTS = 2
_AUDIT_LINKS = frozenset({'created_by', 'updated_by', 'user'})
_SILENT_FIELDS = {
    'Shot': frozenset(
        {'description', 'code', 'sg_cut_in', 'sg_shot_type', 'sg_sequence', 'assets'}
    ),
    'Task': frozenset({'start_date', 'est_in_mins', 'sg_status_list'}),
}


def _error(row: Row, reason: str) -> MockgunError:
    """Name the stream record and the unobserved answer."""
    return MockgunError(f'mockgun cannot activity_stream {row["type"]} {row["id"]}: {reason}')


def _earliest(
    row: Row, entries: list[Row], kept: list[Row], shown: list[Row], minimum: int | None
) -> int:
    """Resolve only the observed lower-bound and limit header cases."""
    if not shown:
        return 0
    lowest = cast('int', shown[-1]['id'])
    if minimum is not None and any(cast('int', entry['id']) <= minimum for entry in entries):
        if lowest != minimum + 1:
            raise _error(row, 'min_id cut the scan with an unsettled earliest_update_id')
        return lowest
    return lowest if len(shown) < len(kept) else 0


def _human_status(store: Store, entry: Row, identity: int) -> FieldValue:
    """Require O2 S1's known, unchanged creator status and null image."""
    label = f'HumanUser {identity}'
    if not store.schema.declares('HumanUser'):
        raise MockgunError(f'{label} creator record is missing')
    human = store.row('HumanUser', identity)
    if human is None:
        raise MockgunError(f'{label} creator record is missing')
    store.check_known(human, 'image')
    if human.get('image') is not None:
        raise MockgunError(f'{label} image is unsettled')
    if not store.schema.declares('HumanUser', 'sg_status_list'):
        raise MockgunError(f'{label} sg_status_list is undeclared')
    store.check_known(human, 'sg_status_list')
    if 'actor_status' not in entry:
        raise MockgunError(f'{label} sg_status_list at the write is unknown')
    status = human.get('sg_status_list')
    if status != entry['actor_status']:
        raise MockgunError(f'{label} sg_status_list changed since the write')
    return status


def _creator(store: Store, entry: Row, stream: Row) -> dict[str, object]:
    """Render ApiUser creators and HumanUser creators with unchanged status and null image."""
    actor = cast('list[FieldValue]', entry['actor'])
    if len(actor) != _ACTOR_PARTS or actor[0] not in {'ApiUser', 'HumanUser'}:
        raise _error(stream, f'entry {entry["id"]} absent created_by is unsettled')
    kind, identity = cast('str', actor[0]), cast('int', actor[1])
    try:
        status = _human_status(store, entry, identity) if kind == 'HumanUser' else None
        name = display_name(store, kind, identity)
    except MockgunError as exc:
        raise _error(stream, f'entry {entry["id"]} {exc}') from exc
    if name is None:
        raise _error(stream, f'entry {entry["id"]} {kind} {identity} display name is unmodeled')
    return {'type': kind, 'id': identity, 'name': name, 'status': status, 'image': None}


def _entry_row(
    store: Store, entry: Row, stream: Row, fields: dict[str, list[str]]
) -> dict[str, object]:
    """Render an entry only when its actor, source write and current primary are settled."""
    if not entry['settled']:
        raise _error(stream, f'entry {entry["id"]} has unsettled source event rows')
    creator = _creator(store, entry, stream)
    primary = cast('list[FieldValue]', entry['primary'])
    kind, identity = cast('str', primary[0]), cast('int', primary[1])
    row = store.require(kind, identity)
    if row['__retired']:
        raise _error(stream, f'entry {entry["id"]} primary {kind} {identity} is retired')
    if stream['type'] == 'Project':
        for link in cast('list[list[FieldValue]]', entry['streams']):
            linked = store.require(cast('str', link[0]), cast('int', link[1]))
            if linked['__retired']:
                raise _error(stream, f'entry {entry["id"]} stream record {link} is retired')
    if not store.schema.declares(kind, 'sg_status_list'):
        raise _error(stream, f'{kind}.sg_status_list is undeclared')
    try:
        store.check_known(row, 'sg_status_list')
        name = display_name(store, kind, identity)
        selected = project_rows(store, store.schema, [row], fields.get(kind, []))[0]
    except MockgunError as exc:
        raise _error(stream, str(exc)) from exc
    if name is None:
        raise _error(stream, f'entry {entry["id"]} display name is unmodeled')
    primary_entity: dict[str, object] = {
        'type': kind,
        'id': identity,
        'name': name,
        'status': row.get('sg_status_list'),
    }
    primary_entity.update({field: selected[field] for field in fields.get(kind, [])})
    return {
        'id': entry['id'],
        'update_type': entry['update_type'],
        'meta': deepcopy(entry['meta']),
        'created_at': format_wire_datetime(cast('datetime', entry['created_at'])),
        'read': False,
        'primary_entity': primary_entity,
        'created_by': creator,
    }


def _kept(candidates: list[Row]) -> list[Row]:
    """Keep only the newest bounded candidate for each Note."""
    result: list[Row] = []
    seen: set[int] = set()
    for entry in candidates:
        note = cast('int | None', entry['note'])
        if note is not None and note in seen:
            continue
        if note is not None:
            seen.add(note)
        result.append(entry)
    return result


def _kept_actor_status(store: Store, context: RequestContext) -> Row:
    """Keep only known HumanUser status in the journaled entry without blocking writes."""
    actor = context.actor
    if (
        actor is None
        or actor.entity_type != 'HumanUser'
        or not store.schema.declares('HumanUser', 'sg_status_list')
    ):
        return {}
    human = store.row('HumanUser', actor.entity_id)
    if human is None:
        return {}
    try:
        store.check_known(human, 'sg_status_list')
    except MockgunError:
        return {}
    return {'actor_status': human.get('sg_status_list')}


def _key(row: Row) -> tuple[str, int]:
    """Return a record's stream identity."""
    return cast('str', row['type']), cast('int', row['id'])


def _selections(store: Store, raw: object) -> dict[str, list[str]]:
    """Validate direct declared fields for each requested primary type."""
    if not isinstance(raw, dict):
        raise MockgunError('mockgun cannot activity_stream parameter entity_fields')
    result: dict[str, list[str]] = {}
    for kind, names in cast('dict[object, object]', raw).items():
        if (
            not isinstance(kind, str)
            or not store.schema.declares(kind)
            or not isinstance(names, list)
        ):
            raise MockgunError(f'mockgun cannot activity_stream entity_fields {kind}')
        selected: list[str] = []
        for field in cast('list[object]', names):
            if (
                not isinstance(field, str)
                or '.' in field
                or not store.schema.declares(kind, field)
            ):
                raise MockgunError(
                    f'mockgun cannot activity_stream {kind}.{field}: undeclared or deep field'
                )
            selected.append(field)
        result[kind] = selected
    return result


def _parameters(store: Store, params: Mapping[str, object]) -> dict[str, list[str]]:
    """Require the exact client parameter shape and direct declared selections."""
    required = {'type', 'id', 'max_id', 'min_id', 'limit', 'entity_fields'}
    mismatch = required.symmetric_difference(params)
    if mismatch:
        raise MockgunError(f'mockgun cannot activity_stream parameter {min(mismatch)}')
    for name in ('id', 'min_id', 'max_id', 'limit'):
        value = params[name]
        if value is None and name != 'id':
            continue
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or (name == 'limit' and value < 1)
        ):
            raise MockgunError(f'mockgun cannot activity_stream parameter {name}')
    if not isinstance(params['type'], str):
        raise MockgunError('mockgun cannot activity_stream parameter type')
    return _selections(store, params['entity_fields'])


def _target(store: Store, link: dict[str, FieldValue]) -> Row | None:
    """Resolve only declared targets so activity hooks preserve partial-schema writes."""
    kind = cast('str', link['type'])
    if not store.schema.declares(kind):
        return None
    return store.row(kind, cast('int', link['id']))


def _linked(store: Store, row: Row) -> list[Row]:
    """Collect direct entity links except the three excluded actor fields."""
    result: list[Row] = []
    kind = cast('str', row['type'])
    for field in store.schema.fields[kind]:
        if field in _AUDIT_LINKS:
            continue
        data_type = store.schema.field_type(kind, field)
        value = row.get(field)
        values = value if data_type == 'multi_entity' and isinstance(value, list) else [value]
        if data_type not in {'entity', 'multi_entity'}:
            continue
        for link in values:
            if isinstance(link, dict):
                target = _target(store, link)
                if target is not None:
                    result.append(target)
    return result


def _primary(store: Store, row: Row) -> Row:
    """Map a Reply write onto its Note stream record."""
    parent = row.get('entity')
    if row['type'] == 'Reply' and isinstance(parent, dict) and parent.get('type') == 'Note':
        note = _target(store, parent)
        if note is not None:
            return note
    return row


def _with_projects(store: Store, rows: list[Row]) -> dict[tuple[str, int], Row]:
    """Include each record's current Project without traversing other links."""
    result = {_key(row): row for row in rows}
    for row in rows:
        project = row.get('project')
        if isinstance(project, dict) and project.get('type') == 'Project':
            target = _target(store, project)
            if target is not None:
                result[_key(target)] = target
    return result


def _affected(store: Store, row: Row) -> dict[tuple[str, int], Row]:
    """Collect a write's primary, direct links, and their current Projects."""
    primary = _primary(store, row)
    rows = [primary, *_linked(store, row)]
    if primary is not row:
        rows.extend(_linked(store, primary))
    return {
        key: target
        for key, target in _with_projects(store, rows).items()
        if key[0] in _READABLE_TYPES
    }


def _streams(store: Store, row: Row) -> list[list[FieldValue]]:
    """Capture the observed create audience and its Projects at write time."""
    rows = [row]
    if row['type'] == 'Task':
        links = [row.get('entity')]
    elif row['type'] == 'Note':
        links = cast('list[FieldValue]', row.get('note_links', []))
    else:
        links = []
    for link in links:
        if isinstance(link, dict) and link.get('type') in {'Shot', 'Asset'}:
            target = _target(store, link)
            if target is not None:
                rows.append(target)
    return [[kind, identity] for kind, identity in _with_projects(store, rows)]


def _write_entry(
    store: Store, row: Row, entry: Row, context: RequestContext, *, before: Row | None = None
) -> None:
    """Journal an entry and taint linked readable records outside its observed audience."""
    primary = _primary(store, row)
    streams = _streams(store, primary)
    actor = context.actor
    identity = store.next_side_id('activity')
    store.side_set(
        'activity',
        (identity,),
        {
            'id': identity,
            **entry,
            **_kept_actor_status(store, context),
            'created_at': context.now,
            'actor': [actor.entity_type, actor.entity_id] if actor else [],
            'primary': [primary['type'], primary['id']],
            'streams': cast('list[FieldValue]', streams),
            'note': primary['id'] if primary['type'] == 'Note' else None,
        },
    )
    included = {tuple(link) for link in streams}
    affected = _affected(store, row)
    if before is not None:
        affected.update(_affected(store, before))
    for key in affected:
        if key not in included:
            store.side_set(
                'activity_taint',
                key,
                f'{row["type"]} {row["id"]} {entry["update_type"]}: linked activity is unsettled',
            )


def read_stream(
    store: Store, params: Mapping[str, object], *, context: RequestContext
) -> dict[str, object]:
    """Read the bounded activity view for a declared readable record.

    Args:
        store: Site records and journaled activity history.
        params: Exact client wire parameters, including bounds and field
            selections.
        context: Request context captured for this RPC.

    Returns:
        An unwrapped stream envelope with newest-first updates and observed
        headers.

    Raises:
        MockgunError: If the record, view, source entry or rendering is
            unsettled.
    """
    del context
    fields = _parameters(store, params)
    kind, identity = cast('str', params['type']), cast('int', params['id'])
    if not store.schema.declares(kind) or kind not in _READABLE_TYPES:
        raise MockgunError(f'mockgun cannot activity_stream {kind} {identity}: unreadable type')
    row = store.row(kind, identity)
    if row is None:
        raise MockgunError(f'mockgun cannot activity_stream {kind} {identity}: missing record')
    taint = store.side_get('activity_taint', (kind, identity))
    if taint is not None:
        raise _error(row, str(taint))
    entries = [
        cast('Row', value)
        for _, value in store.side_items('activity')
        if [kind, identity] in cast('list[list[FieldValue]]', cast('Row', value)['streams'])
    ]
    minimum, maximum = cast('int | None', params['min_id']), cast('int | None', params['max_id'])
    candidates = [
        entry
        for entry in entries
        if (minimum is None or cast('int', entry['id']) > minimum)
        and (maximum is None or cast('int', entry['id']) < maximum)
    ]
    candidates.sort(key=lambda entry: cast('int', entry['id']), reverse=True)
    if not candidates:
        raise _error(row, 'no candidates: activity headers are unsettled')
    kept = _kept(candidates)
    shown = [] if row['__retired'] else kept[: cast('int | None', params['limit'])]
    return {
        'entity_type': kind,
        'entity_id': identity,
        'latest_update_id': candidates[0]['id'],
        'earliest_update_id': _earliest(row, entries, kept, shown, minimum),
        'updates': [_entry_row(store, entry, row, fields) for entry in shown],
    }


def taint_activity(store: Store, records: Iterable[Row], reason: str) -> None:
    """Mark every affected readable stream as unsettled.

    Args:
        store: Site records and journaled side tables.
        records: Written records, including before and after snapshots when
            applicable.
        reason: Diagnostic naming the write whose activity is unobserved.
    """
    for row in records:
        for key in _affected(store, row):
            store.side_set('activity_taint', key, reason)


def record_create_activity(
    store: Store, row: Row, *, settled: bool, context: RequestContext
) -> None:
    """Record observed entity or Reply creates and taint every other create.

    Args:
        store: Site records and journaled side tables.
        row: Newly created record.
        settled: Whether this operation's event planner returned a complete
            plan.
        context: Actor and instant captured for the write.
    """
    kind = row['type']
    if kind not in {'Asset', 'Shot', 'Task', 'Note'} and not (
        kind == 'Reply' and _primary(store, row)['type'] == 'Note'
    ):
        taint_activity(store, [row], f'create {kind} {row["id"]}: activity is unsettled')
        return
    meta: Row = {'type': 'new_entity', 'entity_type': kind, 'entity_id': row['id']}
    if kind == 'Reply':
        meta['content'] = row.get('content')
    elif context.sudo and context.actual is not None:
        actual = context.actual
        meta['sudo_actual_user'] = {
            'type': actual.entity_type,
            'id': actual.entity_id,
            'name': display_name(store, actual.entity_type, actual.entity_id),
            'valid': 'valid',
            'uuid': store.link_uuid(actual.entity_type, actual.entity_id),
        }
    _write_entry(
        store,
        row,
        {
            'update_type': 'create_reply' if kind == 'Reply' else 'create',
            'meta': meta,
            'settled': settled,
        },
        context,
    )


def record_lifecycle_activity(
    store: Store, effects: RetirementEffects, *, context: RequestContext
) -> None:
    """Keep Shot lifecycles silent and taint other lifecycle audiences.

    Args:
        store: Site records and journaled side tables.
        effects: Root, cascaded, touched and tainted rows returned by the
            lifecycle.
        context: Request context captured for the write.
    """
    del context
    if not effects.changed or effects.root['type'] == 'Shot':
        return
    reason = (
        f'retirement or revival {effects.root["type"]} {effects.root["id"]}: activity is unsettled'
    )
    taint_activity(
        store, [effects.root, *effects.cascaded, *effects.touched, *effects.tainted], reason
    )


def record_update_activity(
    store: Store, before: Row, after: Row, *, settled: bool, context: RequestContext
) -> None:
    """Record Shot and Asset status changes while guarding unobserved changed fields.

    Args:
        store: Site records and journaled side tables.
        before: Detached record before this operation.
        after: Updated record including side effects.
        settled: Whether this operation's event planner returned a complete
            plan.
        context: Actor and instant captured for the write.
    """
    kind = cast('str', after['type'])
    changed = {
        field for field in store.schema.fields[kind] if before.get(field) != after.get(field)
    } - {'updated_at', 'updated_by'}
    status = kind in {'Shot', 'Asset'} and 'sg_status_list' in changed
    if status:
        meta: Row = {
            'type': 'attribute_change',
            'attribute_name': 'sg_status_list',
            'entity_type': kind,
            'entity_id': after['id'],
            'field_data_type': 'status_list',
            'old_value': before.get('sg_status_list'),
            'new_value': after.get('sg_status_list'),
        }
        _write_entry(
            store,
            after,
            {'update_type': 'update', 'meta': meta, 'settled': settled},
            context,
            before=before,
        )
    other = (
        changed - _SILENT_FIELDS.get(kind, frozenset()) - ({'sg_status_list'} if status else set())
    )
    if other:
        taint_activity(
            store,
            [before, after],
            f'update {kind} {after["id"]} fields {", ".join(sorted(other))}: '
            'activity is unsettled',
        )
