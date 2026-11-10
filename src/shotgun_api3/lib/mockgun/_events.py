"""Record settled event rows and reject reads of unsettled event metadata."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from ._errors import MockgunError
from ._links import display_name
from ._retirement import RetirementEffects
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store


@dataclass(frozen=True)
class _CreateForm:
    """Describe one observed create form in request and event field order."""

    kind: str
    session: str
    required: tuple[str, ...]
    optional: tuple[str, ...]
    computed: tuple[str, ...]
    prefix: tuple[str, ...]


# C6 P6-03 B1-B3 and C7 Q1 E6-E17: exact observed create forms.
_CREATE_FORMS = (
    _CreateForm(
        'Task',
        'script',
        ('project', 'content'),
        ('entity',),
        ('color', 'sg_status_list'),
        ('color', 'content', 'sg_status_list'),
    ),
    _CreateForm(
        'Note',
        'script',
        ('project', 'subject', 'content'),
        ('note_links',),
        ('user', 'sg_status_list'),
        ('user', 'sg_status_list'),
    ),
    _CreateForm(
        'Note',
        'sudo',
        ('note_links', 'subject', 'project'),
        (),
        ('user', 'sg_status_list'),
        ('user', 'sg_status_list'),
    ),
    _CreateForm('Reply', 'script', ('entity', 'content'), (), ('user',), ('user',)),
    _CreateForm(
        'Sequence',
        'script',
        ('project', 'code'),
        ('description',),
        ('sg_status_list',),
        ('code', 'sg_status_list'),
    ),
    _CreateForm(
        'Cut', 'script', ('project', 'code'), (), ('sg_status_list',), ('code', 'sg_status_list')
    ),
    _CreateForm('CutItem', 'script', ('project', 'code', 'cut'), (), (), ('code',)),
    _CreateForm(
        'CutItem', 'script', ('project', 'code', 'cut', 'shot', 'version'), (), (), ('code',)
    ),
    _CreateForm(
        'Playlist', 'script', ('project', 'code', 'description', 'versions'), (), (), ('code',)
    ),
)
_CREATE_IGNORED = frozenset(
    {
        'id',
        'type',
        'project',
        'created_at',
        'updated_at',
        'created_by',
        'updated_by',
        'cached_display_name',
    }
)
_EVENT_KINDS = {
    'new_entity': 'New',
    'entity_retirement': 'Retirement',
    'entity_revival': 'Revival',
}
_INVERSE_FIELDS = {
    ('Asset', 'parents'): 'assets',
    ('Asset', 'shots'): 'assets',
    ('CutItem', 'cut'): 'cut_items',
    ('Playlist', 'versions'): 'playlists',
    ('Reply', 'entity'): 'replies',
    ('Sequence', 'shots'): 'sg_sequence',
    ('Shot', 'assets'): 'shots',
    ('Shot', 'sg_sequence'): 'shots',
    ('Task', 'entity'): 'tasks',
    ('Version', 'playlists'): 'versions',
    ('Version', 'sg_task'): 'sg_versions',
}
_LIFECYCLE_TYPES = frozenset(
    {
        'Shot',
        'Task',
        'Note',
        'Version',
        'Playlist',
        'Sequence',
        'Asset',
        'PublishedFile',
        'CutItem',
    }
)
_LINK_TARGETS = {
    ('Asset', 'parents'): 'Asset',
    ('Asset', 'shots'): 'Shot',
    ('CutItem', 'cut'): 'Cut',
    ('CutItem', 'shot'): 'Shot',
    ('CutItem', 'version'): 'Version',
    ('Note', 'note_links'): 'Shot',
    ('Playlist', 'versions'): 'Version',
    ('Reply', 'entity'): 'Note',
    ('Sequence', 'shots'): 'Shot',
    ('Shot', 'assets'): 'Asset',
    ('Shot', 'sg_sequence'): 'Sequence',
    ('Task', 'entity'): 'Shot',
    ('Version', 'playlists'): 'Playlist',
    ('Version', 'sg_task'): 'Task',
}
# C6 P6-04 P1/P2: (added count, removed count), also for single links.
_LINK_UPDATE_FORMS = {
    ('Asset', 'parents'): {(1, 0), (1, 1)},
    ('Asset', 'shots'): {(1, 0)},
    ('CutItem', 'cut'): {(1, 1)},
    ('Note', 'note_links'): {(2, 0), (1, 2)},
    ('Playlist', 'versions'): {(1, 0), (0, 2)},
    ('Sequence', 'shots'): {(1, 0)},
    ('Shot', 'sg_sequence'): {(1, 0), (1, 1)},
    ('Task', 'entity'): {(1, 0), (1, 1)},
    ('Version', 'playlists'): {(2, 0)},
    ('Version', 'sg_task'): {(1, 0)},
}
_SCALAR_ENCODINGS = frozenset(
    {'number', 'duration', 'checkbox', 'text', 'list', 'status_list', 'date', 'date_time', 'float'}
)
_WRITE_TYPES = frozenset(
    {
        'Shot',
        'Asset',
        'Sequence',
        'Version',
        'Task',
        'Note',
        'Reply',
        'Playlist',
        'Cut',
        'CutItem',
        'PublishedFile',
    }
)


@dataclass(frozen=True)
class _EventPlan:
    """One ordered event independent of the log's availability."""

    kind: str
    row: Row
    meta: Row
    unknown: frozenset[str]
    forward: int | None = None


def _check_element(store: Store, value: FieldValue) -> None:
    """Check historical link names and statuses recursively within metadata."""
    if isinstance(value, list):
        for item in value:
            _check_element(store, item)
    elif isinstance(value, dict):
        if {'id', 'type', 'name', 'valid', 'status', 'uuid'} <= value.keys():
            kind = cast('str', value['type'])
            identity = cast('int', value['id'])
            target = store.row(kind, identity) if store.schema.declares(kind) else None
            if target is None or target['__retired'] or store.tainted(target):
                raise MockgunError(f'event meta link {kind} {identity} is no longer active')
            store.check_known(target, 'sg_status_list')
            if (
                display_name(store, kind, identity) != value['name']
                or target.get('sg_status_list') != value['status']
            ):
                raise MockgunError(f'event meta link {kind} {identity} name or status changed')
        else:
            for item in value.values():
                _check_element(store, item)


def _context_plans(plans: list[_EventPlan], context: RequestContext) -> list[_EventPlan]:
    """Keep sudo metadata uncertainty in the reusable plans themselves."""
    if not context.sudo:
        return plans
    return [
        _EventPlan(plan.kind, plan.row, plan.meta, plan.unknown | {'meta'}, plan.forward)
        for plan in plans
    ]


def _create_fields(store: Store, row: Row, supplied: Sequence[str]) -> list[str] | None:
    """Resolve only observed populated create fields and their event order."""
    kind = cast('str', row['type'])
    # An omitted checkbox left False adds no row (C4 EV-04 with C5-04's readback).
    populated = [
        field
        for field in store.schema.fields[kind]
        if field not in _CREATE_IGNORED
        and row.get(field) is not None
        and row.get(field) != []
        and not (
            field not in supplied
            and row.get(field) is False
            and store.schema.field_type(kind, field) == 'checkbox'
        )
    ]
    computed_user = kind == 'Version' and 'user' not in supplied
    defaults = set(populated) - set(supplied) - ({'user'} if computed_user else set())
    if (
        row.get('__unknown')
        or any(
            store.schema.field_type(kind, field) not in {'status_list', 'list'}
            for field in defaults
        )
        or any(
            store.schema.field_type(kind, field) in {'entity', 'multi_entity'}
            and not (computed_user and field == 'user')
            for field in populated
        )
    ):
        return None
    if len(populated) <= 1:
        return populated
    if (
        kind == 'Shot'
        and set(populated) == {'code', 'sg_status_list', 'sg_shot_type'}
        and defaults == {'sg_status_list', 'sg_shot_type'}
    ):
        return ['code', 'sg_status_list', 'sg_shot_type']
    if (
        kind == 'Asset'
        and set(populated) == {'code', 'sg_status_list'}
        and defaults == {'sg_status_list'}
    ):
        return ['code', 'sg_status_list']
    if (
        kind == 'Version'
        and set(populated) == {'code', 'sg_status_list', 'user', 'description', 'sg_first_frame'}
        and defaults == {'sg_status_list'}
        and computed_user
    ):
        return [
            'code',
            'sg_status_list',
            'user',
            *[field for field in supplied if field in {'description', 'sg_first_frame'}],
        ]
    return None


def _default_matches(store: Store, row: Row, field: str, context: RequestContext) -> bool:
    """Require the precise schema default or observed computed value."""
    if field == 'user':
        actor = context.actor
        return actor is not None and row[field] == {
            'type': actor.entity_type,
            'id': actor.entity_id,
        }
    if field == 'color':
        return row[field] == 'pipeline_step'
    entry = cast('dict[str, object]', store.schema.fields[cast('str', row['type'])][field])
    properties = cast('dict[str, FieldValue]', entry.get('properties', {}))
    default = cast('dict[str, FieldValue]', properties.get('default_value', {}))
    return row[field] == default.get('value')


def _element(store: Store, link: Row) -> Row:
    """Encode an Asset link with a stable site identity and observed status shape."""
    kind, identity = cast('str', link['type']), cast('int', link['id'])
    if kind != 'Asset' or not store.schema.declares(kind, 'sg_status_list'):
        raise MockgunError('unobserved event link element')
    target = store.require(kind, identity)
    store.check_known(target, 'sg_status_list')
    if target['__retired'] or store.tainted(target):
        raise MockgunError('event link target is not known active')
    return {
        'type': kind,
        'id': identity,
        'name': display_name(store, kind, identity),
        'valid': 'valid',
        'status': target.get('sg_status_list'),
        'uuid': store.link_uuid(kind, identity),
    }


def _encode(value: FieldValue, field_type: str) -> FieldValue:
    """Encode one observed scalar value, raising for unsettled float representations."""
    if field_type not in _SCALAR_ENCODINGS:
        raise MockgunError('unobserved scalar event encoding')
    if value is None:
        return None
    if field_type in {'number', 'duration'}:
        return int(cast('int', value))
    if field_type == 'date_time':
        return cast('datetime', value).astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    if field_type == 'float':
        value = cast('float', value)
        if 'e' in repr(value).lower():
            raise MockgunError('exponent float event meta is unsettled')
        numerator, denominator = value.as_integer_ratio()
        if (abs(numerator) * 1_000_000) % denominator * 2 == denominator:
            raise MockgunError('tie float event meta is unsettled')
        return repr(round(value, 6))
    return value


def _change_meta(row: Row, field: str, field_type: str, old: FieldValue) -> tuple[Row, set[str]]:
    """Build scalar change metadata while retaining unobserved encodings as unknown."""
    meta: Row = {
        'type': 'attribute_change',
        'attribute_name': field,
        'entity_type': row['type'],
        'entity_id': row['id'],
        'field_data_type': field_type,
    }
    unknown: set[str] = set()
    try:
        meta['old_value'] = _encode(old, field_type)
        meta['new_value'] = _encode(row.get(field), field_type)
    except (MockgunError, OverflowError, ValueError):
        unknown.add('meta')
    return meta, unknown


def _known_inverse(store: Store, row: Row, field: str, links: list[Row]) -> bool:
    """Require inverse targets and declarations before planning their rows."""
    inverse = _INVERSE_FIELDS.get((cast('str', row['type']), field))
    if inverse is None:
        return True
    return all(
        store.schema.declares(cast('str', link['type']), inverse)
        and store.row(cast('str', link['type']), cast('int', link['id'])) is not None
        for link in links
    )


def _link_meta(
    store: Store, row: Row, field: str, delta: tuple[list[Row], list[Row]]
) -> tuple[Row, set[str]]:
    """Build one forward or inverse link delta, marking unsupported targets unknown."""
    meta: Row = {
        'type': 'attribute_change',
        'attribute_name': field,
        'entity_type': row['type'],
        'entity_id': row['id'],
        'field_data_type': 'multi_entity',
    }
    try:
        meta['added'] = [_element(store, link) for link in delta[0]]
        meta['removed'] = [_element(store, link) for link in delta[1]]
    except MockgunError:
        return meta, {'meta'}
    return meta, set()


def _members(value: FieldValue) -> list[Row]:
    """Normalize single and multiple links for ordered delta calculations."""
    if isinstance(value, dict):
        return [value]
    return cast('list[Row]', value or [])


def _new_plan(row: Row, *, connection: bool = False) -> _EventPlan:
    """Build an entity or connection creation plan."""
    return _EventPlan(
        'connection' if connection else 'new',
        row,
        {'type': 'new_entity', 'entity_type': row['type'], 'entity_id': row['id']},
        frozenset({'created_at', 'project'} if connection else ()),
    )


def _project(store: Store, row: Row) -> FieldValue:
    """Resolve the write-time Project, following a Reply's Note when applicable."""
    if row['type'] != 'Reply':
        store.check_known(row, 'project')
        return row.get('project')
    store.check_known(row, 'entity')
    link = row.get('entity')
    if isinstance(link, dict) and link.get('type') == 'Note' and store.schema.declares('Note'):
        note = store.row('Note', cast('int', link['id']))
        if note is not None and not store.tainted(note):
            store.check_known(note, 'project')
            return note.get('project')
    return None


def _append(
    store: Store,
    row: Row,
    meta: Row,
    *,
    context: RequestContext,
    unknown: set[str],
) -> Row:
    """Append one event with common fields and row-specific uncertainty."""
    unknown = unknown | {'description'}
    if context.sudo:
        unknown.add('meta')
    try:
        project = _project(store, row)
    except MockgunError:
        project = None
    if project is None:
        unknown.add('project')
    actor = context.actor
    if actor is None:
        raise MockgunError('cannot construct an event without an actor')
    if meta['type'] == 'reading':
        return store.append_event(
            {
                'event_type': 'Shotgun_Reading_Change',
                'attribute_name': 'read_by_current_user',
                'user': {'type': actor.entity_type, 'id': actor.entity_id},
            },
            unknown=unknown,
        )
    kind = _EVENT_KINDS.get(cast('str', meta['type']), 'Change')
    return store.append_event(
        {
            'event_type': f'Shotgun_{row["type"]}_{kind}',
            'attribute_name': meta.get('attribute_name'),
            'meta': meta,
            'entity': {'type': row['type'], 'id': row['id']},
            'user': {'type': actor.entity_type, 'id': actor.entity_id},
            'project': project,
            'created_at': context.now,
            'session_uuid': context.session_uuid,
            'cached_display_name': None,
            'image': None,
            'filmstrip_image': None,
            'image_blur_hash': None,
            'image_source_entity': None,
        },
        unknown=unknown,
    )


def _reading_plan(row: Row, *, follower: bool = False) -> _EventPlan:
    """Represent a human Note read or the opaque followed-Reply read."""
    unknown = {
        'entity',
        'project',
        'meta',
        'created_at',
        'session_uuid',
        'cached_display_name',
        'image',
        'filmstrip_image',
        'image_blur_hash',
        'image_source_entity',
    }
    if follower:
        unknown.add('user')
    return _EventPlan(
        'reading',
        row,
        {'type': 'reading', 'attribute_name': 'read_by_current_user'},
        frozenset(unknown),
    )


def _settled_link_update(
    store: Store, before: Row, after: Row, field: str, context: RequestContext
) -> bool:
    """Recognize supported link changes, including single Shot asset and Note link changes."""
    key = (cast('str', before['type']), field)
    old, new = _members(before.get(field)), _members(after.get(field))
    added, removed = (
        [link for link in new if link not in old],
        [link for link in old if link not in new],
    )
    if key in {('Shot', 'assets'), ('Note', 'note_links')} and len(added) + len(removed) == 1:
        target = (added or removed)[0]
        return key[0] == 'Note' or (
            target['type'] == 'Asset' and store.row('Asset', cast('int', target['id'])) is not None
        )
    if not _known_inverse(store, after, field, [*added, *removed]):
        return False
    if context.session_kind != 'script' or (
        len(added),
        len(removed),
    ) not in _LINK_UPDATE_FORMS.get(key, set()):
        return False
    if key == ('Note', 'note_links'):
        return {cast('str', link['type']) for link in (removed or added)} == {'Shot', 'Asset'}
    if key == ('Sequence', 'shots') and after.get('__sequence_shots_from_null') is not True:
        return False
    return all(link['type'] == _LINK_TARGETS[key] for link in [*added, *removed])


def _single_link_meta(store: Store, row: Row, field: str, old: FieldValue) -> tuple[Row, set[str]]:
    """Encode a single link's old and new values using observed elements."""
    meta: Row = {
        'type': 'attribute_change',
        'attribute_name': field,
        'entity_type': row['type'],
        'entity_id': row['id'],
        'field_data_type': 'entity',
    }
    try:
        meta['old_value'] = _element(store, old) if isinstance(old, dict) else None
        value = row.get(field)
        meta['new_value'] = _element(store, value) if isinstance(value, dict) else None
    except MockgunError:
        return meta, {'meta'}
    return meta, set()


def _field_plan(store: Store, row: Row, field: str, old: FieldValue) -> _EventPlan:
    """Build a scalar or link change with the appropriate metadata encoding."""
    field_type = store.schema.field_type(cast('str', row['type']), field)
    if field_type == 'entity':
        meta, unknown = _single_link_meta(store, row, field, old)
    elif field_type == 'multi_entity':
        before, after = _members(old), _members(row.get(field))
        meta, unknown = _link_meta(
            store,
            row,
            field,
            (
                [link for link in after if link not in before],
                [link for link in before if link not in after],
            ),
        )
    else:
        meta, unknown = _change_meta(row, field, field_type, old)
    return _EventPlan('change', row, meta, frozenset(unknown))


def _inverse_plans(
    store: Store, row: Row, field: str, delta: tuple[list[Row], list[Row]], forward: int
) -> list[_EventPlan]:
    """Build removed targets before added targets, citing the forward plan."""
    inverse = _INVERSE_FIELDS.get((cast('str', row['type']), field))
    if inverse is None:
        return []
    writer: Row = {'type': row['type'], 'id': row['id']}
    plans: list[_EventPlan] = []
    added, removed = delta
    for link in [*removed, *added]:
        target = store.require(cast('str', link['type']), cast('int', link['id']))
        if (row['type'], field) != ('Shot', 'assets') and store.schema.field_type(
            cast('str', target['type']), inverse
        ) == 'entity':
            snapshot: Row = {**target, inverse: writer if link in added else None}
            meta, unknown = _single_link_meta(
                store, snapshot, inverse, None if link in added else writer
            )
        else:
            meta, unknown = _link_meta(
                store, target, inverse, ([writer], []) if link in added else ([], [writer])
            )
        meta['actual_attribute_changed'] = f'{row["type"]}.{field}'
        if row['type'] == 'Shot' and field == 'assets':
            unknown.add('project')
        plans.append(_EventPlan('inverse', target, meta, frozenset(unknown), forward))
    return plans


def _create_plans(
    store: Store, row: Row, fields: list[str], context: RequestContext
) -> list[_EventPlan]:
    """Assemble new, connection and change rows in the observed order."""
    plans: list[_EventPlan] = []
    if (
        row['type'] == 'Note'
        and context.actor is not None
        and context.actor.entity_type == 'HumanUser'
    ):
        plans.append(_reading_plan(row))
    if row['type'] == 'Reply':
        note = _members(row.get('entity'))
        if note and store.followers('Note', cast('int', note[0]['id'])):
            plans.append(_reading_plan(row, follower=True))
    plans.append(_new_plan(row))
    if row['type'] == 'Playlist':
        plans.extend(
            _new_plan(connection, connection=True) for connection in store.created_connections
        )
    for field in fields:
        plan = _field_plan(store, row, field, None)
        plan.meta['in_create'] = True
        unknown = set(plan.unknown)
        if store.schema.field_type(cast('str', row['type']), field) == 'checkbox' or (
            row['type'] == 'CutItem' and field == 'code' and 'cut' in fields
        ):
            unknown.add('meta')
        forward = len(plans)
        plans.append(_EventPlan(plan.kind, plan.row, plan.meta, frozenset(unknown)))
        if (cast('str', row['type']), field) in _INVERSE_FIELDS:
            inverses = _inverse_plans(store, row, field, (_members(row.get(field)), []), forward)
            for inverse in inverses:
                inverse.meta['in_create'] = True
            plans.extend(inverses)
    return plans


def _plan_link_update(store: Store, before: Row, after: Row, field: str) -> list[_EventPlan]:
    """Place created connections before the forward row and ordered inverses."""
    old, new = _members(before.get(field)), _members(after.get(field))
    plans = [_new_plan(connection, connection=True) for connection in store.created_connections]
    forward = len(plans)
    plans.append(_field_plan(store, after, field, before.get(field)))
    plans.extend(
        _inverse_plans(
            store,
            after,
            field,
            ([link for link in new if link not in old], [link for link in old if link not in new]),
            forward,
        )
    )
    return plans


def _plan_update(
    store: Store, before: Row, after: Row, supplied: Mapping[str, str], *, context: RequestContext
) -> list[_EventPlan] | None:
    """Classify update events independently of event declaration or readability."""
    # C7 P4-17 establishes clearing, but not its event rows, including no-ops.
    image_fields = bool({'image', 'filmstrip_image'} & supplied.keys())
    kind = cast('str', before['type'])
    unknown = cast('dict[str, FieldValue]', before.get('__unknown', {}))
    changed = [field for field in supplied if before.get(field) != after.get(field)]
    uncertain = any(field in unknown for field in supplied)
    if kind != 'EventLogEntry' and not changed and not uncertain and not image_fields:
        return []
    actor = context.actor
    if (
        kind not in _WRITE_TYPES
        or image_fields
        or actor is None
        or context.session_kind == 'human'
        or uncertain
    ):
        return None
    if (kind == 'Version' and 'sg_task' in supplied and 'sg_task' not in changed) or (
        kind == 'Task'
        and (
            'due_date' in supplied
            or any(
                field in supplied and after.get(field) is not None
                for field in ('start_date', 'duration')
            )
        )
    ):
        return None
    if any(
        store.schema.field_type(kind, field) in {'entity', 'multi_entity'} for field in changed
    ):
        if len(changed) != 1 or not _settled_link_update(
            store, before, after, changed[0], context
        ):
            return None
        return _context_plans(_plan_link_update(store, before, after, changed[0]), context)
    return _context_plans(
        [_field_plan(store, after, field, before.get(field)) for field in changed], context
    )


def _unsettled(store: Store, reason: str) -> None:
    """Keep the first unsettled API write as the event log's permanent read barrier."""
    store.taint_events(reason)


def _valid_create_links(store: Store, row: Row, supplied: list[str]) -> bool:
    """Restrict populated links to each recorded create form's target and count."""
    kind = cast('str', row['type'])
    for field in supplied:
        if field == 'project':
            continue
        target = _LINK_TARGETS.get((kind, field))
        if target is not None:
            members = _members(row.get(field))
            if (
                len(members) != 1
                or members[0]['type'] != target
                or not _known_inverse(store, row, field, members)
            ):
                return False
    if kind == 'Playlist' and not store.schema.declares('PlaylistVersionConnection'):
        return False
    if kind == 'Reply':
        note = _members(row['entity'])[0]
        followers = store.followers('Note', cast('int', note['id']))
        return not followers or (len(followers) == 1 and supplied == ['entity', 'content'])
    return True


def _observed_create_fields(
    store: Store, row: Row, supplied: list[str], context: RequestContext
) -> list[str] | None:
    """Match exact observed create forms and leave other forms for fallback."""
    actor = context.actor
    if actor is None or row.get('__unknown'):
        return None
    session = context.session_kind
    kind = cast('str', row['type'])
    populated = {
        field
        for field in store.schema.fields[kind]
        if field not in _CREATE_IGNORED and row.get(field) is not None and row.get(field) != []
    }
    for form in _CREATE_FORMS:
        if (kind, session) != (form.kind, form.session):
            continue
        if not set(form.required) <= set(supplied) <= set(form.required) | set(form.optional):
            continue
        if session == 'sudo' and supplied != list(form.required):
            continue
        expected = (set(supplied) - {'project'}) | set(form.computed)
        if populated != expected or not all(
            _default_matches(store, row, field, context) for field in form.computed
        ):
            continue
        if not _valid_create_links(store, row, supplied):
            continue
        return [
            *form.prefix,
            *[field for field in supplied if field != 'project' and field not in form.prefix],
        ]
    return None


def _plan_create(
    store: Store, row: Row, supplied: Sequence[str], *, context: RequestContext
) -> list[_EventPlan] | None:
    """Classify create events without depending on the event log's state."""
    actor = context.actor
    if (
        row['type'] not in _WRITE_TYPES
        or actor is None
        or (context.session_kind == 'human' and row['type'] != 'Note')
    ):
        return None
    effective = [
        field for field in supplied if row.get(field) is not None and row.get(field) != []
    ]
    fields = _observed_create_fields(store, row, effective, context)
    if fields is None:
        fields = _create_fields(store, row, effective)
    if fields is None:
        return None
    plans = _create_plans(store, row, fields, context)
    if any(
        field in supplied and row.get(field) != context.now
        for field in ('created_at', 'updated_at')
    ):
        plans = [
            _EventPlan(plan.kind, plan.row, plan.meta, plan.unknown | {'created_at'}, plan.forward)
            for plan in plans
        ]
    return _context_plans(plans, context)


def _write_plans(store: Store, plans: list[_EventPlan], context: RequestContext) -> None:
    """Append every plan once, resolving inverse references to written IDs."""
    written: list[Row] = []
    for plan in plans:
        meta = dict(plan.meta)
        if plan.forward is not None:
            meta['original_event_log_entry_id'] = written[plan.forward]['id']
        written.append(_append(store, plan.row, meta, context=context, unknown=set(plan.unknown)))


def check_meta(store: Store, row: Row) -> None:
    """Reject unknown metadata or historical links whose current identity state differs.

    Args:
        store: Site records used to resolve historical links.
        row: Event row whose metadata is being read.

    Raises:
        MockgunError: If metadata is unknown or a historical link's current
            identity, display state, or lifecycle cannot support the read.
    """
    store.check_known(row, 'meta')
    _check_element(store, row.get('meta'))


def record_attachment_upload(
    store: Store,
    attachment: Row,
    owner: Row,
    *,
    context: RequestContext,
    display_name_changed: bool = True,
) -> None:
    """Write the observed rows for a plain script upload to a Note.

    Args:
        store: Store whose event log receives the rows.
        attachment: Newly linked Attachment.
        owner: Note receiving the plain upload.
        context: Script actor and request instant captured by the HTTP route.
        display_name_changed: Whether to include the display-name event; C8
            omits it for observed text files retaining their filename.
    """
    if not store.schema.declares('EventLogEntry') or store.events_unknown is not None:
        return
    # C7 P4-07: the New row preceded the Project assignment; every Project
    # value and each Change row's metadata remain unsettled.
    new = _new_plan(attachment)
    plans = [_EventPlan(new.kind, new.row, new.meta, frozenset({'project'}))]
    for row, field in (
        (attachment, 'attachment_links'),
        (owner, 'attachments'),
        (attachment, 'display_name'),
        (attachment, 'project'),
    ):
        if field == 'display_name' and not display_name_changed:
            continue
        plans.append(
            _EventPlan(
                'change',
                row,
                {
                    'type': 'attribute_change',
                    'attribute_name': field,
                    'entity_type': row['type'],
                    'entity_id': row['id'],
                },
                frozenset({'meta', 'project'}),
            )
        )
    _write_plans(store, plans, context)


def record_create(
    store: Store, row: Row, supplied: Sequence[str], *, context: RequestContext
) -> bool:
    """Classify a create and write its events when the log is readable.

    Args:
        store: Site records and event log.
        row: Newly stored record including defaults and computed fields.
        supplied: Field names in request order.
        context: Request actor, time and session.

    Returns:
        Whether the write has a settled event plan, even without an event log.
    """
    plans = _plan_create(store, row, supplied, context=context)
    if store.schema.declares('EventLogEntry') and store.events_unknown is None:
        if plans is None:
            _unsettled(
                store,
                f'{row["type"]} {row["id"]} create has unsettled event rows',
            )
        else:
            _write_plans(store, plans, context)
    return plans is not None


def record_lifecycle(
    store: Store, effects: RetirementEffects, *, revival: bool, context: RequestContext
) -> None:
    """Write settled lifecycle rows using retained retirement timestamps.

    Args:
        store: Site records and event declarations.
        effects: Completed lifecycle root, dependent changes and settlement
            state.
        revival: Whether the operation restored rather than retired the root.
        context: Request actor, timestamp, session UUID and sudo flag.
    """
    if not store.schema.declares('EventLogEntry') or store.events_unknown is not None:
        return
    row = effects.root
    kind = cast('str', row['type'])
    if not effects.changed and kind != 'EventLogEntry':
        return
    actor = context.actor
    if (
        kind not in _LIFECYCLE_TYPES
        or not effects.settled
        or actor is None
        or context.session_kind == 'human'
    ):
        operation = 'revive' if revival else 'delete'
        _unsettled(store, f'{kind} {row["id"]} {operation} has unsettled event rows')
        return
    instant = effects.retired_at
    stamp = (
        instant.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S UTC') if instant is not None else None
    )
    _append(
        store,
        row,
        {
            'type': 'attribute_change',
            'attribute_name': 'retirement_date',
            'entity_type': kind,
            'entity_id': row['id'],
            'old_value': stamp if revival else None,
            'new_value': None if revival else stamp,
        },
        context=context,
        unknown={'meta'} if stamp is None else set(),
    )
    meta: Row = {
        'class_name': kind,
        'id': row['id'],
        'entity_type': kind,
        'entity_id': row['id'],
        'type': 'entity_revival' if revival else 'entity_retirement',
    }
    unknown: set[str] = set()
    if not revival:
        meta['retirement_date'] = stamp
        if stamp is None:
            unknown.add('meta')
    try:
        name = display_name(store, kind, cast('int', row['id']))
        if name is None:
            unknown.add('meta')
        else:
            meta['display_name'] = name
    except MockgunError:
        unknown.add('meta')
    _append(store, row, meta, context=context, unknown=unknown)


def record_schema_write(
    store: Store,
    operation: str,
    data_type: str,
    properties: Sequence[Mapping[str, object]],
    *,
    context: RequestContext,
) -> None:
    """Append the observed text-field schema rows or taint the event log.

    Args:
        store: Site store receiving events or an event-log uncertainty marker.
        operation: Schema operation: `create`, `update`, or `delete`.
        data_type: Data type of the affected schema field.
        properties: Wire property entries containing `property_name` and
            `value`.
        context: Validated request context with a resolved actor for an
            event-producing operation.
    """
    if not store.schema.declares('EventLogEntry') or store.events_unknown is not None:
        return
    settled = (
        data_type == 'text'
        and context.session_kind == 'script'
        and (
            operation != 'update'
            or [item.get('property_name') for item in properties] == ['description']
        )
    )
    if not settled:
        store.taint_events(f'schema_field_{operation} has unsettled event rows')
        return
    actor = context.actor
    if actor is None:
        raise MockgunError('schema event has no actor after session validation')
    rows = {
        'create': [('Shotgun_DisplayColumn_New', None)],
        'update': [
            ('Shotgun_DisplayColumn_Change', 'description'),
            ('Shotgun_DisplayColumn_Change', 'properties'),
        ],
        'delete': [
            ('Shotgun_DisplayColumn_Change', 'retirement_date'),
            ('Shotgun_DisplayColumn_Retirement', None),
        ],
    }[operation]
    for event_type, attribute_name in rows:
        store.append_event(
            {
                'event_type': event_type,
                'attribute_name': attribute_name,
                'user': {'type': actor.entity_type, 'id': actor.entity_id},
                'project': None,
                'session_uuid': context.session_uuid,
                'cached_display_name': None,
                'image': None,
                'filmstrip_image': None,
                'image_blur_hash': None,
                'image_source_entity': None,
            },
            unknown={'entity', 'meta', 'created_at'},
        )


def record_update(
    store: Store, before: Row, after: Row, supplied: Mapping[str, str], *, context: RequestContext
) -> bool:
    """Classify an update and write its events when the log is readable.

    Args:
        store: Site records and event log.
        before: Detached record before the write.
        after: Record after the write.
        supplied: Fields mapped to their effective update modes.
        context: Request actor, time and session.

    Returns:
        Whether the write has a settled event plan, even without an event log.
    """
    plans = _plan_update(store, before, after, supplied, context=context)
    if store.schema.declares('EventLogEntry') and store.events_unknown is None:
        if plans is None:
            _unsettled(
                store,
                f'{after["type"]} {after["id"]} update has unsettled event rows',
            )
        else:
            _write_plans(store, plans, context)
    return plans is not None
