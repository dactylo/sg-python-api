"""Apply observed lifecycle cascades and preserve unsettled retirement effects."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from ._links import (
    RetirementEdge,
    hidden,
    implicit_connection_fields,
    link_target,
    retirement_edge,
)
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store

# A cascaded child revived while its parent stays retired hides its link to the
# parent and is untouched by the parent's later revival (a Shot's Version
# earlier; the rest on 2026-11-10). Each child type maps to its parent field and
# the parent types observed.
_SETTLED_ALONE = {
    'Note': ('note_links', frozenset({'Asset', 'Sequence', 'Shot', 'Version'})),
    'Shot': ('project', frozenset({'Project'})),
    'Task': ('entity', frozenset({'Asset', 'Sequence', 'Shot'})),
    'Version': ('entity', frozenset({'Asset', 'Sequence', 'Shot'})),
}


@dataclass(frozen=True)
class RetirementEffects:
    """Describe a lifecycle change and the dependent rows it affected.

    Field bindings are frozen, but contained rows remain live mutable store
    rows.

    Attributes:
        changed: Whether the root changed retirement state.
        root: Root record after the operation.
        cascaded: Dependent records retired or revived with the root.
        tainted: Records tainted by retirement, or records whose source-owned
            taints were cleared during revival.
        touched: Records whose update audit fields were refreshed.
        settled: Whether the operation's complete lifecycle-event delta is
            modeled.
        retired_at: Retirement instant for a retirement operation, the root's
            prior retirement instant for revival, or `None` when unavailable or
            no state change occurred.
    """

    changed: bool
    root: Row
    cascaded: tuple[Row, ...]
    tainted: tuple[Row, ...]
    touched: tuple[Row, ...]
    settled: bool
    retired_at: datetime | None = None


def _cascade_allowed(
    root: Row, target: Row, entry: tuple[Row, list[tuple[str, RetirementEdge]]]
) -> bool:
    """Allow direct cascades and only the observed Shot-Task-Note chain."""
    holder, edges = entry
    if len(edges) != 1 or edges[0][1].effect != 'cascade' or target.get('__taint'):
        return False
    return target is root or (
        target['type'] == 'Task'
        and root['type'] == 'Shot'
        and target.get('entity') == {'type': root['type'], 'id': root['id']}
        and holder['type'] == 'Note'
        and edges[0][0] == 'tasks'
    )


def _clear_source(store: Store, root: Row) -> list[Row]:
    """Clear source-owned lifecycle taints while preserving unknown audits."""
    cleared: list[Row] = []
    for kind in store.schema.fields:
        for row in store.tainted_rows(kind):
            taint = cast('dict[str, FieldValue]', row['__taint'])
            if taint['source'] == [root['type'], root['id']]:
                store.remember(row)
                row.pop('__taint')
                cleared.append(row)
    return cleared


def _identity(row: Row) -> tuple[str, int]:
    """Return a stored record's identity."""
    return cast('str', row['type']), cast('int', row['id'])


def _holder_groups(
    store: Store, target: Row
) -> list[tuple[Row, list[tuple[str, RetirementEdge]]]]:
    """Group incoming fields before deciding whether their rules agree."""
    groups: dict[tuple[str, int], tuple[Row, list[tuple[str, RetirementEdge]]]] = {}
    for holder, field in store.linking(*_identity(target)):
        groups.setdefault(_identity(holder), (holder, []))[1].append(
            (field, retirement_edge(store, holder, field, target))
        )
    return list(groups.values())


def _mark_hidden_links(store: Store, rows: list[Row]) -> None:
    """Record the NoteLink and NoteTask members already hidden when these rows retired."""
    operation = {_identity(row) for row in rows}
    for row in rows:
        members: list[FieldValue] = []
        for field in implicit_connection_fields(cast('str', row['type'])):
            value = row.get(field)
            for link in cast(
                'list[dict[str, FieldValue]]', value if isinstance(value, list) else []
            ):
                target = link_target(store, link)
                if (
                    target is not None
                    and target['__retired']
                    and _identity(target) not in operation
                ):
                    members.append([field, target['type'], target['id']])
        if members:
            store.remember(row)
            row['__retired_hidden'] = members


def _names(value: FieldValue, target: Row) -> bool:
    """Return whether a single or multi-entity value names the target."""
    identity: FieldValue = {'type': target['type'], 'id': target['id']}
    return value == identity or (isinstance(value, list) and identity in value)


def _settled_alone(root: Row, source: Row) -> bool:
    """Identify the observed standalone revivals of cascaded children."""
    entry = _SETTLED_ALONE.get(cast('str', root['type']))
    if entry is None or source['type'] not in entry[1]:
        return False
    if root['type'] == 'Note' and root.get('tasks'):
        return False
    return _names(root.get(entry[0]), source)


def _revived_alone(store: Store, roots: tuple[Row, Row | None], children: list[Row]) -> None:
    """Taint unobserved standalone revivals, keeping the observed ones settled."""
    root, source = roots
    if source is None or not source['__retired']:
        return
    reason = f'unsettled revival while {source["type"]} {source["id"]} remains retired'
    if not _settled_alone(root, source):
        for holder, field in store.linking(*_identity(source)):
            if holder is root:
                store.taint(root, reason, source, link=(field, source), active=True)
    for row in children:
        store.taint(row, reason, source)


def _touch(store: Store, row: Row, context: RequestContext) -> None:
    """Set the observed actor and operation time on a lifecycle effect."""
    kind = cast('str', row['type'])
    store.remember(row)
    if store.schema.declares(kind, 'updated_by'):
        actor = context.actor
        row['updated_by'] = {'type': actor.entity_type, 'id': actor.entity_id} if actor else None
        store.forget_unknown(row, 'updated_by')
    if store.schema.declares(kind, 'updated_at'):
        row['updated_at'] = context.now
        store.forget_unknown(row, 'updated_at')


def _holder_effect(
    store: Store,
    roots: tuple[Row, Row],
    entry: tuple[Row, list[tuple[str, RetirementEdge]]],
    context: RequestContext,
) -> str:
    """Apply one holder's effect and identify the next scan or touch category."""
    root, target = roots
    holder, edges = entry
    if _cascade_allowed(root, target, entry):
        store.retire(*_identity(holder))
        store.remember(holder)
        holder['__retired_at'] = context.now
        holder['__cascaded_by'] = [target['type'], target['id']]
        _touch(store, holder, context)
        return 'cascade'
    reason = f'unsettled retirement effect from {root["type"]} {root["id"]}'
    effect = edges[0][1].effect if len(edges) == 1 else 'taint'
    if effect == 'hide':
        if edges[0][1].touched:
            if store.tainted(target):
                for field in ('updated_at', 'updated_by'):
                    if store.schema.declares(cast('str', holder['type']), field):
                        store.mark_unknown(holder, field, reason)
            else:
                _touch(store, holder, context)
                return 'touch'
        return 'hide'
    for field, _ in edges:
        store.taint(holder, reason, root, link=(field, target))
    return 'taint'


def _restore_children(store: Store, parent: Row, context: RequestContext) -> list[Row]:
    """Revive the children this cascade retired that still link the parent."""
    children: list[Row] = []
    for kind in store.schema.fields:
        for row in store.rows(kind, retired=True):
            if row.get('__cascaded_by') == [parent['type'], parent['id']]:
                store.check_writable(row)
                store.remember(row)
                row.pop('__cascaded_by', None)
                # 2026-11-10: a child relinked or cleared while retired stays retired.
                if not any(_names(row.get(field), parent) for field in store.schema.fields[kind]):
                    continue
                store.revive(*_identity(row))
                row.pop('__retired_hidden', None)
                _touch(store, row, context)
                children.append(row)
    return children


def _restore_touches(store: Store, parent: Row, context: RequestContext) -> list[Row]:
    """Repeat the audit touches recorded by the retirement operation."""
    holders: list[Row] = []
    store.remember(parent)
    for link in cast('list[list[FieldValue]]', parent.pop('__retirement_touched', [])):
        holder = store.require(cast('str', link[0]), cast('int', link[1]))
        store.check_writable(holder)
        # 2026-11-10: a holder retired meanwhile keeps its audit fields.
        if holder['__retired']:
            continue
        _touch(store, holder, context)
        holders.append(holder)
    return holders


def _revive_marked_notes(store: Store, parent: Row, context: RequestContext) -> list[Row]:
    """Revive the observed Notes retired after this parent, or taint other shapes.

    A Note linking one Shot and one Asset, retired while the Shot was retired,
    revives with the Shot; a one-link Note, or one retired after its Task,
    stays retired (2026-11-10).
    """
    revived: list[Row] = []
    if not store.schema.declares('Note', 'note_links'):
        return revived
    for note in store.rows('Note', retired=True):
        markers = cast('list[list[FieldValue]]', note.get('__retired_hidden', []))
        fields = {
            cast('str', marker[0])
            for marker in markers
            if marker[1:] == [parent['type'], parent['id']]
        }
        if not fields:
            continue
        links = cast('list[dict[str, FieldValue]]', note.get('note_links', []))
        # 2026-11-10: a one-link Note, or one retired after its Task, stays retired.
        if fields == {'tasks'} or (
            fields == {'note_links'} and len(links) == 1 and not note.get('tasks')
        ):
            continue
        store.check_writable(note)
        if (
            fields == {'note_links'}
            and parent['type'] == 'Shot'
            and not note.get('tasks')
            and sorted(cast('str', link['type']) for link in links) == ['Asset', 'Shot']
        ):
            store.revive(*_identity(note))
            store.remember(note)
            note.pop('__retired_hidden', None)
            _touch(store, note, context)
            revived.append(note)
        else:
            reason = (
                f'unsettled revival of {parent["type"]} {parent["id"]} for a Note retired after it'
            )
            store.taint(note, reason, None)
    return revived


def _revive(store: Store, root: Row, context: RequestContext) -> RetirementEffects:
    """Restore marked children and touches, then resolve source-owned taints."""
    store.remember(root)
    source_link = root.pop('__cascaded_by', None)
    source = (
        store.row(cast('str', source_link[0]), cast('int', source_link[1]))
        if isinstance(source_link, list)
        else None
    )
    retired_at = cast('datetime | None', root.get('__retired_at'))
    store.revive(*_identity(root), context=context)
    root.pop('__retired_hidden', None)
    cascaded: list[Row] = []
    touched: list[Row] = []
    queue = [root]
    settled = True
    while queue:
        parent = queue.pop(0)
        for holder, field in store.linking(*_identity(parent)):
            taint = cast('dict[str, FieldValue]', holder.get('__taint', {}))
            if taint and taint.get('source') != [root['type'], root['id']]:
                store.check_writable(holder)
            if not taint:
                settled = settled and retirement_edge(store, holder, field, parent).settled
        children = _restore_children(store, parent, context)
        cascaded.extend(children)
        queue.extend(children)
        touched.extend(_restore_touches(store, parent, context))
    cascaded.extend(_revive_marked_notes(store, root, context))
    cleared = _clear_source(store, root)
    _revived_alone(store, (root, source), cascaded)
    return RetirementEffects(
        changed=True,
        root=root,
        cascaded=tuple(cascaded),
        tainted=tuple(cleared),
        touched=tuple(touched),
        settled=settled and not (cascaded or touched or cleared or store.tainted(root)),
        retired_at=retired_at,
    )


def _scan(store: Store, root: Row, context: RequestContext) -> RetirementEffects:
    """Scan each lifecycle wave against the state left by its predecessor."""
    cascaded: list[Row] = []
    tainted: list[Row] = []
    touched: list[Row] = []
    buckets = {'cascade': cascaded, 'taint': tainted, 'touch': touched}
    initial_taints = {
        _identity(row) for kind in store.schema.fields for row in store.tainted_rows(kind)
    }
    queue = [root]
    visited: set[tuple[str, int]] = set()
    settled = True
    while queue:
        target = queue.pop(0)
        key = _identity(target)
        if key in visited:
            continue
        visited.add(key)
        for entry in _holder_groups(store, target):
            holder, edges = entry
            if holder is root or holder in cascaded:
                continue
            if _identity(holder) in initial_taints:
                store.check_writable(holder)
            settled = settled and all(edge.settled for _, edge in edges)
            effect = _holder_effect(store, (root, target), entry, context)
            bucket = buckets.get(effect)
            if bucket is not None and holder not in bucket:
                bucket.append(holder)
                if effect != 'touch':
                    queue.append(holder)
    store.remember(root)
    root['__retirement_touched'] = [[row['type'], row['id']] for row in touched]
    _mark_hidden_links(store, [root, *cascaded])
    return RetirementEffects(
        changed=True,
        root=root,
        cascaded=tuple(cascaded),
        tainted=tuple(tainted),
        touched=tuple(touched),
        settled=settled and not (cascaded or tainted or touched),
        retired_at=context.now,
    )


def kept_members(
    store: Store,
    entity_type: str,
    entity_id: int,
    fields: Mapping[str, object],
) -> dict[str, list[dict[str, FieldValue]]]:
    """Keep hidden Asset.shots members through an edit of the visible members.

    The edit cannot name a hidden member: the site refuses links to retired
    records before this runs.

    Args:
        store: Site records used to inspect existing shot links.
        entity_type: Entity type being updated.
        entity_id: Existing entity identifier.
        fields: Incoming field values.

    Returns:
        A `shots` entry containing preserved stored link dictionaries, or an
        empty mapping when no preservation applies.

    Raises:
        ServerFaultError: If the Asset record cannot be resolved.
        MockgunError: If an existing shot link's visibility is unsettled.
    """
    if entity_type != 'Asset' or not isinstance(fields.get('shots'), list):
        return {}
    row = store.require(entity_type, entity_id)
    if row['__retired']:
        return {}
    members = cast('list[dict[str, FieldValue]]', row.get('shots', []))
    kept = [link for link in members if hidden(store, row, 'shots', link)]
    return {'shots': kept} if kept else {}


def retire(
    store: Store, entity_type: str, entity_id: int, *, context: RequestContext
) -> RetirementEffects:
    """Retire a root and apply its observed dependent effects atomically.

    Args:
        store: Site records and transaction journal.
        entity_type: Root entity type.
        entity_id: Root entity identifier.
        context: Request actor and timestamp used for lifecycle audit fields.

    Returns:
        Effects containing live stored rows. `changed` is `False` when the root
        was already retired.

    Raises:
        ServerFaultError: If a required entity type or record is missing.
        MockgunError: If the root or a dependent record has an unsettled
            lifecycle state that prevents the operation.
    """
    root = store.require(entity_type, entity_id)
    store.check_writable(root)
    if root['__retired']:
        return RetirementEffects(
            changed=False, root=root, cascaded=(), tainted=(), touched=(), settled=True
        )
    with store.transaction():
        store.retire(entity_type, entity_id, context=context)
        return _scan(store, root, context)


def revive(
    store: Store, entity_type: str, entity_id: int, *, context: RequestContext
) -> RetirementEffects:
    """Restore a root and its recorded dependent effects atomically.

    Args:
        store: Site records and transaction journal.
        entity_type: Root entity type.
        entity_id: Root entity identifier.
        context: Request actor and timestamp used for lifecycle audit fields.

    Returns:
        Effects containing live stored rows. `changed` is `False` when the root
        was already active.

    Raises:
        ServerFaultError: If a required entity type or record is missing.
        MockgunError: If the root or a dependent record has an unsettled
            lifecycle state that prevents restoration.
    """
    root = store.require(entity_type, entity_id)
    store.check_writable(root)
    if not root['__retired']:
        return RetirementEffects(
            changed=False, root=root, cascaded=(), tainted=(), touched=(), settled=True
        )
    with store.transaction():
        return _revive(store, root, context)
