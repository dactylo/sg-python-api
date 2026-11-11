"""Compute modeled display names and detached named entity links."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ._errors import MockgunError
from ._store import CONNECTION_FIELDS, FieldValue, Row, Store


@dataclass(frozen=True)
class _RetirementRule:
    """Describe one observed retirement rule and its holder condition."""

    effect: str
    condition: str
    touched: bool
    settled: bool


_DISPLAY_FIELDS = {
    'Attachment': ('display_name',),
    'Project': ('name',),
    'HumanUser': ('name',),
    'Department': ('name',),
    'Shot': ('code',),
    'Asset': ('code',),
    'Sequence': ('code',),
    'Version': ('code',),
    'PublishedFile': ('code',),
    'Cut': ('code',),
    'Playlist': ('code',),
    'CutItem': ('code',),
    'Step': ('code',),
    'PublishedFileType': ('code',),
    'TaskTemplate': ('code',),
    'Group': ('code',),
    'CustomEntity03': ('code',),
    'Task': ('content',),
    'Ticket': ('title',),
    'Reply': ('content',),
    'Note': ('subject', 'content'),
    'ApiUser': ('firstname', 'lastname'),
}
_DISPLAY_LIMIT = 255
# Fields the site keeps in NoteLink and NoteTask records, which mockgun does not
# store; a retired holder hides their members (2026-11-10 live comparison).
_IMPLICIT_CONNECTIONS = {
    'Note': frozenset({'note_links', 'tasks'}),
    'Shot': frozenset({'notes'}),
}
_RETIREMENT_EDGES = {
    # 2026-11-10: Attachment and Note links hide; neither end is touched or logged.
    ('Attachment', 'attachment_links', 'Note'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('Attachment', 'attachment_links', 'Version'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('Note', 'attachments', 'Attachment'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    # 2026-11-10: a live Task whose Shot is revived gains no event rows.
    ('Task', 'entity', 'Shot'): _RetirementRule('cascade', 'always', touched=False, settled=True),
    # 2026-11-10: a live Version whose Shot is revived gains no event rows.
    ('Version', 'entity', 'Shot'): _RetirementRule(
        'cascade', 'always', touched=False, settled=True
    ),
    ('Task', 'entity', 'Asset'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('Task', 'entity', 'Sequence'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('Version', 'entity', 'Asset'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('Version', 'entity', 'Sequence'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('Note', 'note_links', 'Shot'): _RetirementRule(
        'cascade', 'note_links', touched=False, settled=False
    ),
    ('Note', 'note_links', 'Asset'): _RetirementRule(
        'cascade', 'note_links', touched=False, settled=False
    ),
    ('Note', 'note_links', 'Sequence'): _RetirementRule(
        'cascade', 'note_links', touched=False, settled=False
    ),
    ('Note', 'note_links', 'Version'): _RetirementRule(
        'cascade', 'note_links', touched=False, settled=False
    ),
    ('Note', 'tasks', 'Task'): _RetirementRule('cascade', 'tasks', touched=False, settled=False),
    ('Reply', 'entity', 'Note'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('Shot', 'project', 'Project'): _RetirementRule(
        'cascade', 'project', touched=False, settled=False
    ),
    ('Asset', 'project', 'Project'): _RetirementRule(
        'cascade', 'project', touched=False, settled=False
    ),
    ('Sequence', 'project', 'Project'): _RetirementRule(
        'cascade', 'project', touched=False, settled=False
    ),
    ('Task', 'project', 'Project'): _RetirementRule(
        'cascade', 'project', touched=False, settled=False
    ),
    ('Version', 'project', 'Project'): _RetirementRule(
        'cascade', 'project', touched=False, settled=False
    ),
    ('Note', 'project', 'Project'): _RetirementRule(
        'cascade', 'note_project', touched=False, settled=False
    ),
    ('PublishedFile', 'entity', 'Shot'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('CutItem', 'shot', 'Shot'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('PublishedFile', 'entity', 'Asset'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('PublishedFile', 'entity', 'Sequence'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('TimeLog', 'entity', 'Shot'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('TimeLog', 'entity', 'Task'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('Shot', 'sg_sequence', 'Sequence'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('Cut', 'entity', 'Sequence'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('PublishedFile', 'version', 'Version'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('CutItem', 'version', 'Version'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('Version', 'sg_task', 'Task'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('PublishedFile', 'task', 'Task'): _RetirementRule(
        'hide', 'always', touched=False, settled=True
    ),
    ('Sequence', 'shots', 'Shot'): _RetirementRule('hide', 'always', touched=False, settled=False),
    # 2026-11-10: retiring or reviving a Task alone adds no row for its Shot.
    ('Shot', 'tasks', 'Task'): _RetirementRule('hide', 'always', touched=False, settled=True),
    ('Shot', 'notes', 'Note'): _RetirementRule('hide', 'always', touched=False, settled=False),
    ('Version', 'published_files', 'PublishedFile'): _RetirementRule(
        'hide', 'always', touched=False, settled=False
    ),
    ('Cut', 'cut_items', 'CutItem'): _RetirementRule(
        'hide', 'always', touched=False, settled=False
    ),
    ('Playlist', 'versions', 'Version'): _RetirementRule(
        'hide', 'always', touched=True, settled=False
    ),
    ('Version', 'playlists', 'Playlist'): _RetirementRule(
        'hide', 'always', touched=True, settled=False
    ),
    ('Asset', 'parents', 'Asset'): _RetirementRule('hide', 'always', touched=True, settled=False),
    ('Asset', 'assets', 'Asset'): _RetirementRule('hide', 'always', touched=True, settled=False),
    ('Shot', 'assets', 'Asset'): _RetirementRule('hide', 'always', touched=True, settled=False),
    ('Asset', 'shots', 'Shot'): _RetirementRule('hide', 'always', touched=True, settled=False),
    # 2026-11-10: a CutItem outlives its Cut with the link hidden and no rows.
    ('CutItem', 'cut', 'Cut'): _RetirementRule('hide', 'always', touched=False, settled=True),
    # 2026-11-10: a connection record retires with either end and revives with it.
    ('AssetShotConnection', 'shot', 'Shot'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('AssetShotConnection', 'asset', 'Asset'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('AssetAssetConnection', 'parent', 'Asset'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('AssetAssetConnection', 'asset', 'Asset'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('PlaylistVersionConnection', 'version', 'Version'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
    ('PlaylistVersionConnection', 'playlist', 'Playlist'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
    ),
}
_NOTE_COMBINED_LIMIT = 83
_NOTE_SUBJECT_LIMIT = 120


@dataclass(frozen=True)
class RetirementEdge:
    """Describe an immutable link retirement effect and its completeness.

    Attributes:
        effect: `hide`, `cascade`, or `taint`, describing the effect on the
            holder when the target is retired.
        touched: Whether hiding the link affects the holder's update audit
            fields.
        settled: Whether the edge's lifecycle-event effects are fully modeled.
    """

    effect: str
    touched: bool = False
    settled: bool = False


@dataclass(frozen=True)
class RecordState:
    """Describe an immutable possible retirement state and hidden outgoing links.

    Attributes:
        retired: Whether the record is retired in this assumed state.
        hidden: Hidden outgoing links as `(field_name, target_type, target_id)`
            tuples.
    """

    retired: bool
    hidden: frozenset[tuple[str, str, int]] = frozenset()


def _display_values(store: Store, row: Row, fields: tuple[str, ...]) -> list[FieldValue]:
    """Read display inputs only after all their values are known."""
    for field in fields:
        store.check_known(row, field)
    return [row.get(field) for field in fields]


def _listed(holder: Row, field_name: str, link: Mapping[str, FieldValue]) -> bool:
    """Identify an outgoing link listed by the holder's taint."""
    taint = cast('dict[str, FieldValue]', holder.get('__taint', {}))
    return [field_name, link['type'], link['id']] in cast(
        'list[FieldValue]', taint.get('links', [])
    )


def _retired_target(target: Row, assume: Mapping[tuple[str, int], RecordState] | None) -> bool:
    """Return a target's assumed or settled retirement, rejecting an unsettled one."""
    state = (
        assume.get((cast('str', target['type']), cast('int', target['id']))) if assume else None
    )
    if state is not None:
        return state.retired
    taint = cast('dict[str, FieldValue]', target.get('__taint', {}))
    if taint and not taint.get('active'):
        raise MockgunError(f'{target["type"]} {target["id"]}: {taint["reason"]}')
    return bool(target['__retired'])


def _uncertain(row: Row) -> bool:
    """Return whether a record's own retirement state is unsettled."""
    taint = cast('dict[str, FieldValue]', row.get('__taint', {}))
    return bool(taint) and not taint.get('active')


def _unsettled(entity_type: str, fields: tuple[str, ...]) -> MockgunError:
    """Identify the unresolved display fields in a naming failure."""
    paths = ', '.join(f'{entity_type}.{field}' for field in fields)
    return MockgunError(f'{paths} text display name is not modeled for these values')


def _note_name(row: Row) -> tuple[str, bool]:
    """Return the observed whole or cut Note name, rejecting unsettled forms."""
    subject = cast('str | None', row.get('subject'))
    content = cast('str | None', row.get('content'))
    error = _unsettled('Note', ('subject', 'content'))
    if not (subject or content) or any('\r' in value for value in (subject, content) if value):
        raise error
    if subject and content:
        name = f'{subject} - {content}'
        if len(name) > _NOTE_COMBINED_LIMIT:
            raise error
    elif subject:
        name = subject
        if len(name) > _NOTE_SUBJECT_LIMIT:
            raise error
    else:
        name = cast('str', content)
        if len(name) > _DISPLAY_LIMIT:
            prefix = name[:_DISPLAY_LIMIT]
            if not all(
                character.isascii() and (character.isalnum() or character == '-')
                for character in prefix
            ):
                raise error
            return prefix, True
    return name, False


def _field_name(
    store: Store, entity_type: str, entity_id: int, row: Row, fields: tuple[str, ...]
) -> tuple[str, bool]:
    """Name a record from its display fields in its type's observed form."""
    values = _display_values(store, row, fields)
    if entity_type == 'Note':
        return _note_name(row)
    if entity_type == 'Task' and row.get('content') is None:
        return '-', True
    if any(value is None for value in values):
        raise _unsettled(entity_type, fields)
    name = ' '.join(cast('list[str]', values))
    # C9: a Ticket is named `#<id>: <title>`.
    return (f'#{entity_id}: {name}' if entity_type == 'Ticket' else name), False


def _display(store: Store, entity_type: str, entity_id: int) -> tuple[str | None, bool]:
    """Resolve naming precedence against the current stored or candidate row."""
    if not store.schema.declares(entity_type):
        return None, False
    row = store.row(entity_type, entity_id)
    if row is None:
        return None, False
    actor = store.actor_key(entity_type, entity_id)
    if actor is not None and (
        entity_type == 'ApiUser' or not store.schema.declares(entity_type, 'name')
    ):
        name, derived = actor, False
    else:
        fields = _DISPLAY_FIELDS.get(entity_type)
        if fields is None:
            raise _unsettled(entity_type, ('cached_display_name',))
        if not all(store.schema.declares(entity_type, field) for field in fields):
            return None, False
        name, derived = _field_name(store, entity_type, entity_id, row, fields)
    if len(name) > _DISPLAY_LIMIT or (
        not name.isascii() and len(name.encode('utf-8')) > _DISPLAY_LIMIT
    ):
        raise _unsettled(entity_type, _DISPLAY_FIELDS[entity_type])
    return name, derived


def connection_backed(holder: Row, field_name: str, target: Row) -> bool:
    """Return whether the site keeps a link's membership in a connection record.

    A retired holder hides such a member while its connection record is
    retired, even when the target is active.
    """
    kind = cast('str', holder['type'])
    return (kind, field_name, cast('str', target['type'])) in CONNECTION_FIELDS or (
        field_name in _IMPLICIT_CONNECTIONS.get(kind, frozenset())
    )


def connection_record(store: Store, holder: Row, field_name: str, target: Row) -> Row | None:
    """Return the stored connection record behind one member, retired or not.

    Returns:
        The live stored connection row, or `None` when the schema declares no
        such record or none joins the two records.
    """
    definition = CONNECTION_FIELDS.get(
        (cast('str', holder['type']), field_name, cast('str', target['type']))
    )
    if definition is None:
        return None
    connection_type, holder_side, target_side = definition
    if not (
        store.schema.declares(connection_type, holder_side)
        and store.schema.declares(connection_type, target_side)
    ):
        return None
    return store.connection_row(
        connection_type,
        {
            holder_side: {'type': holder['type'], 'id': holder['id']},
            target_side: {'type': target['type'], 'id': target['id']},
        },
    )


def derived_name(store: Store, entity_type: str, entity_id: int) -> bool:
    """Return whether a modeled name uses Task fallback or Note truncation.

    Raises:
        MockgunError: If the target's display name is unsettled.
    """
    return _display(store, entity_type, entity_id)[1]


def display_name(store: Store, entity_type: str, entity_id: int) -> str | None:
    """Return the current display name, or `None` when its inputs are unmodeled.

    Raises:
        MockgunError: If the target's display name is unsettled.
    """
    return _display(store, entity_type, entity_id)[0]


def implicit_connection_fields(entity_type: str) -> frozenset[str]:
    """Return a type's connection-backed fields whose records mockgun does not store."""
    return _IMPLICIT_CONNECTIONS.get(entity_type, frozenset())


def implicit_link_hidden(holder: Row, field_name: str, target: Row) -> bool:
    """Return whether a retired holder hides an active NoteLink or NoteTask member.

    The internal record stays retired with the holder, unless the target
    retired first and so revived it on its own revival (2026-11-10).
    """
    member: FieldValue = [field_name, target['type'], target['id']]
    return member not in cast('list[FieldValue]', holder.get('__retired_hidden', []))


def _retired_holder_hidden(
    store: Store,
    holder: Row,
    field_name: str,
    target: Row,
    assume: Mapping[tuple[str, int], RecordState] | None,
) -> bool:
    """Resolve a retired holder's link from its target and any connection record."""
    if _retired_target(target, assume):
        return True
    if not connection_backed(holder, field_name, target):
        return False
    record = connection_record(store, holder, field_name, target)
    if record is not None:
        return _retired_target(record, assume)
    return implicit_link_hidden(holder, field_name, target)


def link_target(store: Store, link: Mapping[str, FieldValue]) -> Row | None:
    """Return the stored record a link names, including a retired one.

    Returns:
        The live stored row, or `None` when the link is malformed, names an
        undeclared entity type, or names a missing record.
    """
    kind, identity = link.get('type'), link.get('id')
    if (
        not isinstance(kind, str)
        or not isinstance(identity, int)
        or not store.schema.declares(kind)
    ):
        return None
    return store.row(kind, identity)


def _live(store: Store, link: Mapping[str, FieldValue]) -> bool:
    """Return whether a link names an existing active record with settled state."""
    row = link_target(store, link)
    return row is not None and not row['__retired'] and not store.tainted(row)


def _chained_task(store: Store, holder: Row, target: Row) -> bool:
    """Identify a Note linked to a Shot and to one live Task of that Shot."""
    links = cast('list[dict[str, FieldValue]]', holder.get('note_links', []))
    tasks = cast('list[dict[str, FieldValue]]', holder.get('tasks', []))
    identity = {'type': target['type'], 'id': target['id']}
    if target['type'] != 'Shot' or len(tasks) != 1 or links != [identity]:
        return False
    task = link_target(store, tasks[0])
    return task is not None and task.get('entity') == identity and _live(store, tasks[0])


def _note_edge(store: Store, holder: Row, target: Row) -> RetirementEdge:
    """Resolve the observed Note.note_links shapes, including the Shot-Task chain."""
    links = cast('list[dict[str, FieldValue]]', holder.get('note_links', []))
    if holder.get('tasks'):
        # A Note created for a Task also links the Task's Shot; both retire with the Shot.
        return RetirementEdge('cascade' if _chained_task(store, holder, target) else 'taint')
    if len(links) == 1:
        return RetirementEdge('cascade')
    others = [
        link for link in links if (link['type'], link['id']) != (target['type'], target['id'])
    ]
    # 2026-11-10: a two-link Note survives either link's retirement while the other is live.
    if len(others) == 1 and len(links) == len(others) + 1 and _live(store, others[0]):
        return RetirementEdge('hide')
    return RetirementEdge('taint')


def _task_note_edge(store: Store, holder: Row, rule: _RetirementRule) -> RetirementEdge:
    """Resolve a Note's single Task link by whether a live note_links target remains."""
    links = cast('list[dict[str, FieldValue]]', holder.get('note_links', []))
    if len(cast('list[FieldValue]', holder.get('tasks', []))) != 1:
        return RetirementEdge('taint')
    if not links:
        return RetirementEdge(rule.effect, rule.touched, rule.settled)
    # 2026-11-10: a Note keeping a live note_links target survives its Task.
    return RetirementEdge('hide' if all(_live(store, link) for link in links) else 'taint')


def deciding_record(
    store: Store, holder: Row, field_name: str, link: Mapping[str, FieldValue]
) -> Row | None:
    """Return the record whose possible states decide an uncertain link.

    Args:
        store: Site records used to resolve the link target.
        holder: Stored record containing the outgoing link.
        field_name: Holder field containing the link.
        link: Target identity with `type` and `id`.

    Returns:
        The holder, target or connection row whose state decides link
        visibility, or `None` when no modeled uncertainty requires a deciding
        record.
    """
    target = link_target(store, link)
    if target is None or (target['type'], target['id']) == (holder['type'], holder['id']):
        return None
    if _listed(holder, field_name, link):
        return holder
    if _uncertain(target):
        return target
    if holder['__retired'] and connection_backed(holder, field_name, target):
        record = connection_record(store, holder, field_name, target)
        if record is not None and _uncertain(record):
            return record
    return None


def hidden(
    store: Store,
    holder: Row,
    field_name: str,
    link: Mapping[str, FieldValue],
    *,
    assume: Mapping[tuple[str, int], RecordState] | None = None,
) -> bool:
    """Resolve observed link hiding under optional record-state assumptions.

    A link is hidden while its target is retired, whether the holder is active
    or retired. A retired holder also hides a connection-backed member while
    the connection record is retired. A link to the holder itself stays
    visible.

    Args:
        store: Site records used to resolve link targets.
        holder: Stored record containing the outgoing link.
        field_name: Holder field containing the link.
        link: Target identity with `type` and `id`.
        assume: Optional states keyed by `(entity_type, entity_id)` for records
            whose visibility is uncertain.

    Returns:
        Whether the link is hidden under the observed rules and supplied
        assumptions.

    Raises:
        MockgunError: If visibility depends on an uncertain record for which no
            explicit state was supplied.
    """
    target = link_target(store, link)
    if target is None or (target['type'], target['id']) == (holder['type'], holder['id']):
        return False
    identity = (cast('str', holder['type']), cast('int', holder['id']))
    holder_state = assume.get(identity) if assume is not None else None
    retired = holder_state.retired if holder_state is not None else bool(holder['__retired'])
    deciding = deciding_record(store, holder, field_name, link)
    if deciding is not None:
        key = (cast('str', deciding['type']), cast('int', deciding['id']))
        state = assume.get(key) if assume is not None else None
        if state is None:
            raise MockgunError(
                f'{holder["type"]} {holder["id"]}.{field_name}: {store.tainted(deciding)}'
            )
        if deciding is holder:
            if not state.retired:
                return (
                    field_name,
                    cast('str', link['type']),
                    cast('int', link['id']),
                ) in state.hidden
            retired = True
        elif not retired:
            return state.retired
    if retired:
        return _retired_holder_hidden(store, holder, field_name, target, assume)
    return _retired_target(target, assume)


def named_link(store: Store, link: Mapping[str, FieldValue]) -> dict[str, FieldValue]:
    """Return a detached identity with a name when the target is modeled.

    Raises:
        MockgunError: If the target's display name is unsettled.
    """
    result = {'type': link['type'], 'id': link['id']}
    name = display_name(store, cast('str', link['type']), cast('int', link['id']))
    if name is not None:
        result['name'] = name
    return result


def retirement_edge(store: Store, holder: Row, field_name: str, target: Row) -> RetirementEdge:
    """Resolve a retirement edge, including its observed holder conditions."""
    kind = cast('str', holder['type'])
    target_type = cast('str', target['type'])
    if kind == 'EventLogEntry' and field_name == 'entity':
        return RetirementEdge('hide')
    rule = _RETIREMENT_EDGES.get((kind, field_name, target_type))
    if rule is None:
        return RetirementEdge('taint')
    if rule.condition == 'note_links':
        return _note_edge(store, holder, target)
    if rule.condition == 'tasks':
        return _task_note_edge(store, holder, rule)
    if rule.condition == 'note_project' and (holder.get('note_links') or holder.get('tasks')):
        return RetirementEdge('taint')
    return RetirementEdge(rule.effect, rule.touched, rule.settled)
