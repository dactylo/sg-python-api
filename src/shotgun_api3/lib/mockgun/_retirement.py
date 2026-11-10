"""Apply observed lifecycle cascades and preserve unsettled retirement effects."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from ._links import RetirementEdge, hidden, retirement_edge
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store


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


def _revived_alone(store: Store, roots: tuple[Row, Row | None], children: list[Row]) -> None:
    """Taint the unobserved standalone revival while preserving the Version exception."""
    root, source = roots
    if source is None or not source['__retired']:
        return
    if (
        root['type'] == 'Version'
        and source['type'] == 'Shot'
        and root.get('entity') == {'type': 'Shot', 'id': source['id']}
    ):
        return
    reason = f'unsettled revival while {source["type"]} {source["id"]} remains retired'
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
    if effect == 'keep' and not store.tainted(target):
        return 'keep'
    for field, _ in edges:
        store.taint(holder, reason, root, link=(field, target))
    return 'taint'


def _restore_children(store: Store, parent: Row, context: RequestContext) -> list[Row]:
    """Revive only still-retired children owned by this cascade marker."""
    children: list[Row] = []
    for kind in store.schema.fields:
        for row in store.rows(kind, retired=True):
            if row.get('__cascaded_by') == [parent['type'], parent['id']]:
                store.check_writable(row)
                store.revive(*_identity(row))
                store.remember(row)
                row.pop('__cascaded_by', None)
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
        _touch(store, holder, context)
        holders.append(holder)
    return holders


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
    """Keep hidden Asset.shots members when an edit names visible members only.

    Args:
        store: Site records used to inspect existing shot links.
        entity_type: Entity type being updated.
        entity_id: Existing entity identifier.
        fields: Incoming field values. Naming any hidden shot explicitly
            disables automatic preservation.

    Returns:
        A `shots` entry containing preserved stored link dictionaries, or an
        empty mapping when no preservation applies.

    Raises:
        ServerFaultError: If the Asset record cannot be resolved.
        MockgunError: If an existing shot link's visibility is unsettled.
    """
    if entity_type != 'Asset' or 'shots' not in fields:
        return {}
    row = store.require(entity_type, entity_id)
    if row['__retired']:
        return {}
    members = cast('list[dict[str, FieldValue]]', row.get('shots', []))
    kept = [link for link in members if hidden(store, row, 'shots', link)]
    incoming = fields['shots']
    if not isinstance(incoming, list):
        return {}
    identities = {
        (cast('dict[str, object]', item).get('type'), cast('dict[str, object]', item).get('id'))
        for item in cast('list[object]', incoming)
        if isinstance(item, dict)
    }
    if any((link['type'], link['id']) in identities for link in kept):
        return {}
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
