"""Compute modeled display names and detached named entity links."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ._errors import MockgunError
from ._store import FieldValue, Row, Store


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
_RETIREMENT_EDGES = {
    ('Attachment', 'attachment_links', 'Note'): _RetirementRule(
        'keep', 'always', touched=False, settled=False
    ),
    ('Attachment', 'attachment_links', 'Version'): _RetirementRule(
        'keep', 'always', touched=False, settled=False
    ),
    ('Note', 'attachments', 'Attachment'): _RetirementRule(
        'keep', 'always', touched=False, settled=False
    ),
    ('Task', 'entity', 'Shot'): _RetirementRule('cascade', 'always', touched=False, settled=False),
    ('Version', 'entity', 'Shot'): _RetirementRule(
        'cascade', 'always', touched=False, settled=False
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
        'hide', 'always', touched=False, settled=False
    ),
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
        'hide', 'always', touched=False, settled=False
    ),
    ('Sequence', 'shots', 'Shot'): _RetirementRule('hide', 'always', touched=False, settled=False),
    ('Shot', 'tasks', 'Task'): _RetirementRule('hide', 'always', touched=False, settled=False),
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
    ('CutItem', 'cut', 'Cut'): _RetirementRule('keep', 'always', touched=False, settled=False),
}
_NOTE_COMBINED_LIMIT = 83
_NOTE_SUBJECT_LIMIT = 120


@dataclass(frozen=True)
class RetirementEdge:
    """Describe an immutable link retirement effect and its completeness.

    Attributes:
        effect: `hide`, `keep`, `cascade`, or `taint`, describing the effect on
            the holder when the target is retired.
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


def _note_edge(store: Store, holder: Row, target: Row) -> RetirementEdge:
    """Resolve the two observed Note.note_links shapes."""
    links = cast('list[dict[str, FieldValue]]', holder.get('note_links', []))
    if holder.get('tasks'):
        return RetirementEdge('taint')
    if len(links) == 1:
        return RetirementEdge('cascade')
    others = [
        link for link in links if (link['type'], link['id']) != (target['type'], target['id'])
    ]
    if (
        len(others) == 1
        and len(links) == len(others) + 1
        and target['type'] == 'Shot'
        and others[0]['type'] == 'Asset'
    ):
        other = store.row('Asset', cast('int', others[0]['id']))
        if other is not None and not other['__retired'] and not store.tainted(other):
            return RetirementEdge('hide')
    return RetirementEdge('taint')


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
    if rule is None or (target_type == 'Project' and target.get('archived')):
        return RetirementEdge('taint')
    if rule.condition == 'note_links':
        return _note_edge(store, holder, target)
    if rule.condition == 'tasks' and (
        holder.get('note_links') or len(cast('list[FieldValue]', holder.get('tasks', []))) != 1
    ):
        return RetirementEdge('taint')
    if rule.condition == 'note_project' and (holder.get('note_links') or holder.get('tasks')):
        return RetirementEdge('taint')
    return RetirementEdge(rule.effect, rule.touched, rule.settled)


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
        The live holder or target row whose state decides link visibility, or
        `None` when no modeled uncertainty requires a deciding record.
    """
    if (
        not isinstance(link.get('type'), str)
        or not isinstance(link.get('id'), int)
        or not store.schema.declares(cast('str', link['type']))
    ):
        return None
    if _listed(holder, field_name, link):
        return holder
    target = store.row(cast('str', link['type']), cast('int', link['id']))
    if target is None:
        return None
    edge = retirement_edge(store, holder, field_name, target)
    if edge.effect == 'keep' and target['__retired']:
        return holder
    if (
        edge.effect == 'hide'
        and store.tainted(target)
        and not cast('dict[str, FieldValue]', target['__taint']).get('active')
    ):
        return target
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

    Args:
        store: Site records used to resolve link targets.
        holder: Stored record containing the outgoing link.
        field_name: Holder field containing the link.
        link: Target identity with `type` and `id`.
        assume: Optional states keyed by `(entity_type, entity_id)` for records
            whose visibility is uncertain.

    Returns:
        Whether the link is hidden under the observed rules and supplied
        assumptions. Retired holders retain their links.

    Raises:
        MockgunError: If visibility depends on an uncertain record for which no
            explicit state was supplied.
    """
    identity = (cast('str', holder['type']), cast('int', holder['id']))
    holder_state = assume.get(identity) if assume is not None else None
    if holder_state.retired if holder_state is not None else holder['__retired']:
        return False
    deciding = deciding_record(store, holder, field_name, link)
    if deciding is not None:
        key = (cast('str', deciding['type']), cast('int', deciding['id']))
        state = assume.get(key) if assume is not None else None
        if state is None:
            reason = store.tainted(deciding) or f'retired {link["type"]} {link["id"]} keep link'
            raise MockgunError(f'{holder["type"]} {holder["id"]}.{field_name}: {reason}')
        if deciding is holder:
            return (field_name, cast('str', link['type']), cast('int', link['id'])) in state.hidden
        return state.retired
    if (
        not isinstance(link.get('type'), str)
        or not isinstance(link.get('id'), int)
        or not store.schema.declares(cast('str', link['type']))
    ):
        return False
    target = store.row(cast('str', link['type']), cast('int', link['id']))
    if target is None:
        return False
    edge = retirement_edge(store, holder, field_name, target)
    hides = edge.effect == 'hide' or (holder['type'], field_name, target['type']) == (
        'Version',
        'entity',
        'Shot',
    )
    return hides and bool(target['__retired'])
