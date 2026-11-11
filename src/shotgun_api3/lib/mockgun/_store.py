"""Store fake-site records with the existing validation and normalization rules."""

from __future__ import annotations

import copy
import datetime
import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, TypeAlias, cast
from uuid import uuid4

from ._errors import MockgunError, ServerFaultError
from ._wire import parse_wire_datetime

UPLOAD_LINK_REASON_PREFIX = 'POST /upload/api_link_file '

if TYPE_CHECKING:
    from collections.abc import Collection, Generator, Mapping, Sequence

    from ._schema import SiteSchema
    from ._sessions import RequestContext

FieldValue: TypeAlias = (
    bool
    | int
    | float
    | str
    | datetime.date
    | datetime.datetime
    | dict[str, 'FieldValue']
    | list['FieldValue']
    | None
)
Row: TypeAlias = dict[str, FieldValue]
_SideKey: TypeAlias = tuple[str | int, ...]
_EntityLink: TypeAlias = dict[str, FieldValue]
_Schema: TypeAlias = dict[str, FieldValue]

_FIELD_PYTHON_TYPES: dict[str, type[object]] = {
    'checkbox': bool,
    'date': str,
    'date_time': datetime.datetime,
    'duration': int,
    'entity_type': str,
    'float': float,
    'list': str,
    'number': int,
    'percent': int,
    'serializable': dict,
    'status_list': str,
    'text': str,
    'url': dict,
}

_PAIRED_FIELDS = {
    ('Shot', 'assets', 'Asset'): ('Asset', 'shots'),
    ('Asset', 'shots', 'Shot'): ('Shot', 'assets'),
    ('Shot', 'sg_sequence', 'Sequence'): ('Sequence', 'shots'),
    ('Sequence', 'shots', 'Shot'): ('Shot', 'sg_sequence'),
    ('Asset', 'parents', 'Asset'): ('Asset', 'assets'),
    ('Asset', 'assets', 'Asset'): ('Asset', 'parents'),
    ('Version', 'playlists', 'Playlist'): ('Playlist', 'versions'),
    ('Playlist', 'versions', 'Version'): ('Version', 'playlists'),
    ('PublishedFile', 'version', 'Version'): ('Version', 'published_files'),
    ('Task', 'entity', 'Shot'): ('Shot', 'tasks'),
    ('Note', 'note_links', 'Shot'): ('Shot', 'notes'),
    ('CutItem', 'cut', 'Cut'): ('Cut', 'cut_items'),
}

CONNECTION_FIELDS = {
    ('Shot', 'assets', 'Asset'): ('AssetShotConnection', 'shot', 'asset'),
    ('Asset', 'shots', 'Shot'): ('AssetShotConnection', 'asset', 'shot'),
    ('Asset', 'parents', 'Asset'): ('AssetAssetConnection', 'asset', 'parent'),
    ('Asset', 'assets', 'Asset'): ('AssetAssetConnection', 'parent', 'asset'),
    ('Version', 'playlists', 'Playlist'): ('PlaylistVersionConnection', 'version', 'playlist'),
    ('Playlist', 'versions', 'Version'): ('PlaylistVersionConnection', 'playlist', 'version'),
}

_UNKNOWN_CREATE_FIELDS = {
    'Task': ('color', 'due_date', 'content'),
}


class Store:
    """Records and retirement state owned by one serialized fake server.

    Rows returned by this internal interface are live store rows. The server
    holds its request lock while using them; RPC handlers project detached
    values.

    Attributes:
        created_connections: Connection rows created by the latest create or
            update, for immediate event construction under the request lock.
            Reset at each create/update and on transactional restoration.
        context: Latest request actor and time, or `None` before any request.
        schema: Field declarations shared with query handlers.
        events_unknown: First unsettled event write, or `None` while the log
            remains readable.
    """

    def __init__(self, schema: SiteSchema) -> None:
        """Create empty entity tables and seed the declared startup event fields."""
        self._actors: dict[tuple[str, int], str] = {}
        self._candidate: Row | None = None
        self._link_uuids: dict[tuple[str, int], str] = {}
        self._undo: dict[tuple[str, int], Row | None] | None = None
        self._side_tables: dict[str, dict[_SideKey, FieldValue]] = {
            name: {}
            for name in ('activity', 'activity_taint', 'follows', 'image_identity', 'last_access')
        }
        self._side_next_ids = dict.fromkeys(self._side_tables, 1)
        self._side_undo: dict[tuple[str, _SideKey], FieldValue] | None = None
        self.context: RequestContext | None = None
        self.created_connections: list[Row] = []
        self.schema = schema
        self.events_unknown: str | None = None
        self._schema = schema.fields
        self._db: dict[str, dict[int, Row]] = {entity_type: {} for entity_type in self._schema}
        if schema.declares('EventLogEntry'):
            event = self._new_row('EventLogEntry')
            for name, value in {
                'description': 'Mockgun was born. Yay.',
                'event_type': 'Hello_Mockgun_World',
            }.items():
                if schema.declares('EventLogEntry', name):
                    event[name] = value
            event['id'] = 1
            self._db['EventLogEntry'][1] = event
        self._next_ids = {
            entity_type: max(table, default=0) + 1 for entity_type, table in self._db.items()
        }

    @staticmethod
    def _expand_local_paths(data: dict[str, object]) -> dict[str, object]:
        """Copy data and add the file-link values synthesized by the service."""
        expanded = copy.deepcopy(data)
        for value in expanded.values():
            if not isinstance(value, dict) or 'local_path' not in value:
                continue
            path_value = cast('dict[str, object]', value)
            path_value.setdefault(
                'local_storage',
                {
                    'id': 0,
                    'name': 'auto_generated_by_mockgun',
                    'type': 'LocalStorage',
                },
            )
            path_value.setdefault('local_path_linux', path_value['local_path'])
            path_value.setdefault('local_path_mac', path_value['local_path'])
            path_value.setdefault('local_path_windows', path_value['local_path'])
        return expanded

    @staticmethod
    def _links(value: FieldValue, target_type: str) -> list[_EntityLink]:
        """Extract distinct relation identities for one observed target type."""
        values = value if isinstance(value, list) else [value]
        links: list[_EntityLink] = []
        for item in values:
            if isinstance(item, dict) and item.get('type') == target_type and item not in links:
                links.append(item)
        return links

    @staticmethod
    def _normalize_field_value(field_type: str, value: FieldValue) -> FieldValue:
        """Detach a field value and reduce entity links to identity keys."""
        if value is None:
            return None
        if field_type == 'text' and value == '':
            return None
        if field_type == 'entity':
            link = cast('_EntityLink', value)
            return {'type': link['type'], 'id': link['id']}
        if field_type == 'multi_entity':
            links = cast('list[_EntityLink]', value)
            return [{'type': item['type'], 'id': item['id']} for item in links]
        return copy.deepcopy(value)

    def _remember_identity(self, entity_type: str, entity_id: int) -> None:
        """Save an identity's original row once before mutation or insertion."""
        undo = self._undo
        key = (entity_type, entity_id)
        if undo is not None and key not in undo:
            undo[key] = copy.deepcopy(self._db[entity_type].get(entity_id))

    def _schema_field_type(self, entity_type: str, field_name: str) -> str:
        """Return the data type of a field already validated against the schema."""
        entity_schema = cast('_Schema', self._schema[entity_type])
        field_schema = cast('_Schema', entity_schema[field_name])
        data_type = cast('_Schema', field_schema['data_type'])
        return cast('str', data_type['value'])

    def _new_row(self, entity_type: str) -> Row:
        """Build an active row populated with independent schema defaults."""
        row: Row = {'__retired': False, 'type': entity_type}
        entity_schema = cast('_Schema', self._schema[entity_type])
        for field_name, field_schema_value in entity_schema.items():
            field_schema = cast('_Schema', field_schema_value)
            data_type = self._schema_field_type(entity_type, field_name)
            if data_type == 'multi_entity':
                row[field_name] = []
            else:
                properties = cast('_Schema', field_schema['properties'])
                default_value = cast('_Schema', properties['default_value'])
                row[field_name] = _row_default(data_type, default_value['value'])
        return row

    def _set_created_audit(
        self, entity_type: str, data: dict[str, object], context: RequestContext
    ) -> None:
        """Add declared creation fields while preserving supported supplied values."""
        actor = context.actor
        for name in ('created_at', 'updated_at'):
            if self.schema.declares(entity_type, name):
                if actor is None:
                    data[name] = context.now
                else:
                    data.setdefault(name, context.now)
        for name in ('created_by', 'updated_by'):
            if not self.schema.declares(entity_type, name):
                continue
            link = (
                {'type': actor.entity_type, 'id': actor.entity_id} if actor is not None else None
            )
            if actor is None or name == 'updated_by':
                data[name] = link
            else:
                data.setdefault(name, link)

    @staticmethod
    def _update_multi_entity(
        current: list[_EntityLink],
        incoming: list[_EntityLink],
        mode: object,
    ) -> list[_EntityLink]:
        """Apply multi-entity modes while retaining order and identity semantics."""
        normalized = [{'type': item['type'], 'id': item['id']} for item in incoming]
        if mode == 'set':
            return normalized
        if mode == 'add':
            result = copy.deepcopy(current)
            for item in normalized:
                if item not in result:
                    result.append(item)
            return result
        if mode == 'remove':
            return [copy.deepcopy(item) for item in current if item not in normalized]
        return copy.deepcopy(current)

    @staticmethod
    def _validate_direct_fields(data: dict[str, object]) -> None:
        """Reject invalid names and fields controlled by mockgun."""
        if any(
            not isinstance(field, str) or not field for field in cast('Collection[object]', data)
        ):
            raise ServerFaultError('field names must be nonempty strings')
        if 'id' in data or 'type' in data:
            raise ServerFaultError("Can't set id or type on create or update")
        internal_fields = sorted(field for field in data if field.startswith('_'))
        if internal_fields:
            raise ServerFaultError(f'fields cannot be set directly: {", ".join(internal_fields)}')

    @staticmethod
    def _validate_entity_link(entity_type: str, field_name: str, value: object) -> None:
        """Validate the structural requirements for a single link."""
        if not isinstance(value, dict):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type entity, '
                f'but data {value!r} is not a dictionary'
            )
        if 'type' not in value or 'id' not in value:
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type entity, '
                f"but data {value!r} does not contain 'type' and 'id'"
            )

    def _validate_entity_type(self, entity_type: str) -> None:
        """Reject entity types absent from the loaded schema."""
        if entity_type not in self._schema:
            raise ServerFaultError(f'{entity_type} is not a valid entity')

    def _validate_entity_fields(
        self,
        entity_type: str,
        fields: Collection[str] | None,
    ) -> None:
        """Reject fields absent from the selected entity schema."""
        self._validate_entity_type(entity_type)
        if fields is None:
            return
        valid_fields = cast('_Schema', self._schema[entity_type])
        for field_name in fields:
            if field_name not in valid_fields and field_name not in {'id', 'type'}:
                raise ServerFaultError(
                    f'{field_name} is not a valid field for entity {entity_type}',
                    error_code=103,
                )

    def _parse_fields(self, entity_type: str, fields: dict[str, object]) -> dict[str, object]:
        """Convert declared datetime field values from their wire representation."""
        if entity_type == 'Project' and 'last_accessed_by_current_user' in fields:
            raise MockgunError(
                'mockgun cannot create/update Project.last_accessed_by_current_user: '
                'per-user field is not writable'
            )
        self._validate_entity_type(entity_type)
        self._validate_direct_fields(fields)
        self._validate_entity_fields(entity_type, fields.keys())
        result = copy.deepcopy(fields)
        for name, value in result.items():
            if self._schema_field_type(entity_type, name) == 'date_time' and isinstance(
                value, str
            ):
                try:
                    result[name] = parse_wire_datetime(value)
                except ValueError:
                    # Leave malformed strings for the existing scalar validation error.
                    continue
        return result

    @staticmethod
    def _validate_multi_entity_links(
        entity_type: str,
        field_name: str,
        value: object,
        valid_types: Collection[str],
    ) -> None:
        """Validate the structure and permitted types for a link list."""
        if not isinstance(value, list):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but data {value!r} is not a list',
                error_code=103 if value is None else None,
            )
        items = cast('list[object]', value)
        if any(not isinstance(item, dict) for item in items):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but data {value!r} contains a non-dictionary'
            )
        links = cast('list[_EntityLink]', items)
        if any('type' not in item or 'id' not in item for item in links):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f"but an item in data {value!r} does not contain 'type' and 'id'"
            )
        if any(item['type'] not in valid_types for item in links):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but an item in data {value!r} has an invalid type '
                f'(expected one of {valid_types!r})'
            )

    @staticmethod
    def _validate_scalar_value(
        entity_type: str,
        field_name: str,
        field_type: str,
        value: object,
    ) -> None:
        """Validate one non-relation field value."""
        python_type = _FIELD_PYTHON_TYPES.get(field_type)
        if python_type is None:
            raise MockgunError(
                f'Field {entity_type}.{field_name}: handling for schema type '
                f'{field_type} is not implemented'
            )
        if not isinstance(value, python_type):
            raise ServerFaultError(
                f'{entity_type}.{field_name} is of schema type {field_type}, '
                f'but data {value!r} is not a {python_type.__name__}',
                error_code=103 if field_type == 'checkbox' and value is None else None,
            )

    @staticmethod
    def _validate_valid_value(
        field_path: str,
        field_type: str,
        value: object,
        field_schema: _Schema,
        operation: str,
    ) -> None:
        """Reject a status or list value outside its declared choices."""
        if field_type not in {'status_list', 'list'}:
            return
        properties = cast('_Schema', field_schema['properties'])
        valid_values = properties.get('valid_values')
        if valid_values is None:
            return
        values = cast('list[str]', cast('_Schema', valid_values)['value'])
        if value in values:
            return
        kind = 'status' if field_type == 'status_list' else 'list value'
        label = 'statuses' if field_type == 'status_list' else 'list values'
        raise ServerFaultError(
            f'API {operation}() Update failed for [{field_path}]: '
            f'{value} is not a valid {kind}. Valid {label}: {", ".join(values)}.',
            error_code=104,
        )

    def _validate_entity_data(
        self, entity_type: str, data: dict[str, object], operation: str
    ) -> None:
        """Validate field values against the hand-loaded site schema."""
        self._validate_direct_fields(data)
        self._validate_entity_fields(entity_type, data.keys())
        for field_name, value in data.items():
            entity_schema = cast('_Schema', self._schema[entity_type])
            field_schema = cast('_Schema', entity_schema[field_name])
            field_type = self._schema_field_type(entity_type, field_name)
            if field_type == 'summary' and operation == 'update':
                encoded = 'nil' if value is None else json.dumps(value)
                raise ServerFaultError(
                    f'API update() {entity_type}.{field_name} is read only:\n'
                    f'{{"field_name" => "{field_name}", "value" => {encoded}}}',
                    error_code=103,
                )
            if (entity_type, field_name, value) == ('Version', 'code', ''):
                if operation == 'create':
                    raise ServerFaultError(
                        'API create() Create failed for [Version]: '
                        'Cannot set identifier field to empty. (Version)',
                        error_code=104,
                    )
                raise MockgunError('Version.code text update to empty is not modeled')
            if value is None and field_type not in {'multi_entity', 'checkbox'}:
                continue
            if field_type == 'entity':
                self._validate_entity_link(entity_type, field_name, value)
                continue

            if field_type == 'multi_entity':
                properties = cast('_Schema', field_schema['properties'])
                valid_types = cast('_Schema', properties['valid_types'])
                self._validate_multi_entity_links(
                    entity_type,
                    field_name,
                    value,
                    cast('Collection[str]', valid_types['value']),
                )
                continue

            self._validate_scalar_value(entity_type, field_name, field_type, value)
            self._validate_valid_value(
                f'{entity_type}.{field_name}', field_type, value, field_schema, operation
            )

    def actor_key(self, entity_type: str, entity_id: int) -> str | None:
        """Return the credential key only for a site-created actor record."""
        return self._actors.get((entity_type, entity_id))

    @staticmethod
    def check_known(row: Row, field_name: str) -> None:
        """Reject a read of an unknown field value.

        Args:
            row: Stored or detached record whose field is being read.
            field_name: Direct field name.

        Raises:
            MockgunError: If the requested value is unknown.
        """
        markers = cast('dict[str, FieldValue]', row.get('__unknown', {}))
        if field_name in markers:
            raise MockgunError(
                f'{row["type"]}.{field_name} on record {row["id"]} is unknown: '
                f'{markers[field_name]}'
            )

    def connection_row(
        self, connection_type: str, endpoints: Mapping[str, _EntityLink]
    ) -> Row | None:
        """Return the stored connection record joining both endpoints, retired or not.

        Args:
            connection_type: Declared connection entity type.
            endpoints: Endpoint field names mapped to their `type` and `id`
                links.
        """
        for row in self._db[connection_type].values():
            if all(row.get(name) == link for name, link in endpoints.items()):
                return row
        return None

    def followers(self, entity_type: str, entity_id: int) -> list[int]:
        """Return following HumanUser IDs in ascending order for one record."""
        return sorted(
            cast('int', key[0])
            for key in self._side_tables['follows']
            if key[1:] == (entity_type, entity_id)
        )

    def link_uuid(self, entity_type: str, entity_id: int) -> str:
        """Return a random canonical identity kept outside transactional snapshots."""
        key = (entity_type, entity_id)
        if key not in self._link_uuids:
            self._link_uuids[key] = str(uuid4())
        return self._link_uuids[key]

    def next_side_id(self, table: str) -> int:
        """Consume the next ID for a declared side table without rollback."""
        identity = self._side_next_ids[table]
        self._side_next_ids[table] += 1
        return identity

    def preview(self, row: Row) -> Store:
        """Return a read view substituting one detached candidate row.

        Args:
            row: Candidate to expose without changing tables or ID allocation.
        """
        preview = copy.copy(self)
        preview._candidate = row  # noqa: SLF001
        return preview

    def record_actor(self, entity_type: str, entity_id: int, key: str) -> None:
        """Register a record created by the site for a script name or login."""
        self._actors[(entity_type, entity_id)] = key

    def remember(self, row: Row) -> None:
        """Journal a live row before mutation; ignore detached candidates."""
        if self._undo is None:
            return
        entity_type = cast('str', row['type'])
        entity_id = cast('int', row.get('id'))
        if self._db[entity_type].get(entity_id) is row:
            self._remember_identity(entity_type, entity_id)

    def forget_unknown(self, row: Row, field_name: str) -> None:
        """Clear the unknown marker for a field made known by a write."""
        self.remember(row)
        markers = cast('dict[str, FieldValue]', row.get('__unknown', {}))
        markers.pop(field_name, None)
        if not markers:
            row.pop('__unknown', None)

    def _set_updated_audit(self, entity_type: str, row: Row, context: RequestContext) -> None:
        """Record the actor and time of a successful change when declared."""
        self.remember(row)
        for field_name in ('updated_at', 'updated_by'):
            self.forget_unknown(row, field_name)
        if self.schema.declares(entity_type, 'updated_at'):
            row['updated_at'] = context.now
        if self.schema.declares(entity_type, 'updated_by'):
            actor = context.actor
            row['updated_by'] = (
                {'type': actor.entity_type, 'id': actor.entity_id} if actor is not None else None
            )

    def drop_field(self, entity_type: str, field_name: str) -> None:
        """Remove a deleted schema field and its markers from every record."""
        for row in self._db[entity_type].values():
            self.remember(row)
            row.pop(field_name, None)
            self.forget_unknown(row, field_name)

    def mark_unknown(self, row: Row, field_name: str, reason: str) -> None:
        """Record an unsettled value alongside its stored row.

        Args:
            row: Record to mark; snapshots include these markers.
            field_name: Declared field whose value is unsettled.
            reason: Evidence-specific explanation returned by read failures.
        """
        self.remember(row)
        markers = cast('dict[str, FieldValue]', row.setdefault('__unknown', {}))
        markers[field_name] = reason

    def _apply_create_computed(
        self, row: Row, fields: Mapping[str, object], context: RequestContext
    ) -> None:
        """Fill observed actor and Task values or retain unknown markers."""
        actor = context.actor
        if actor is None:
            return
        entity_type = cast('str', row['type'])
        if (
            entity_type in {'Version', 'Note', 'Reply'}
            and 'user' not in fields
            and self.schema.declares(entity_type, 'user')
        ):
            row['user'] = {'type': actor.entity_type, 'id': actor.entity_id}
        observed_task = entity_type == 'Task' and _observed_task_form(fields, context)
        if observed_task:
            if self.schema.declares(entity_type, 'color'):
                row['color'] = 'pipeline_step'
            if self.schema.declares(entity_type, 'due_date'):
                row['due_date'] = None
        for name in _UNKNOWN_CREATE_FIELDS.get(entity_type, ()):
            if name in fields or not self.schema.declares(entity_type, name):
                continue
            if observed_task and name in {'color', 'due_date'}:
                continue
            self.mark_unknown(row, name, 'server-computed value on create is unobserved')
        if entity_type == 'Note' and 'reply_content' in fields:
            self.mark_unknown(row, 'reply_content', 'create does not retain the supplied value')

    def _mirror_first_task_inverse(self, task: Row, version: Row) -> None:
        """Append the first observed Version assignment and unsettle Task audit."""
        unknown = cast('dict[str, FieldValue]', task.get('__unknown', {}))
        if 'sg_versions' not in unknown:
            self.remember(task)
            members = cast('list[FieldValue]', task['sg_versions'])
            link: FieldValue = {'type': 'Version', 'id': version['id']}
            if link not in members:
                task['sg_versions'] = [*members, link]
        for name in ('updated_at', 'updated_by'):
            if self.schema.declares('Task', name):
                self.mark_unknown(task, name, 'Version.sg_task inverse audit is unobserved')

    def _update_certainty(
        self,
        row: Row,
        stored: Row,
        fields: dict[str, object],
        modes: dict[str, str],
        context: RequestContext,
    ) -> None:
        """Clear written markers and distinguish certain changes from unknown ones."""
        entity_type = cast('str', row['type'])
        changed = False
        uncertain = False
        for field_name in fields:
            if field_name == 'cached_display_name':
                continue
            field_type = self._schema_field_type(entity_type, field_name)
            unknown = field_name in cast('dict[str, FieldValue]', stored.get('__unknown', {}))
            if unknown:
                uncertain = True
                changed = changed or (
                    entity_type == 'Task' and field_name == 'content' and row[field_name] is None
                )
                if field_type != 'multi_entity' or modes.get(field_name, 'set') == 'set':
                    self.forget_unknown(row, field_name)
            else:
                changed = changed or row[field_name] != stored[field_name]
        if changed:
            self._set_updated_audit(entity_type, row, context)
        elif uncertain:
            for name in ('updated_at', 'updated_by'):
                if self.schema.declares(entity_type, name):
                    self.mark_unknown(
                        row, name, 'whether the update changed an unknown value is unsettled'
                    )

    def add_field(self, entity_type: str, field_name: str) -> None:
        """Populate a new schema field on existing active and retired records."""
        field_type = self._schema_field_type(entity_type, field_name)
        for row in self._db[entity_type].values():
            self.remember(row)
            row[field_name] = [] if field_type == 'multi_entity' else None
            if field_type == 'checkbox':
                self.mark_unknown(
                    row,
                    field_name,
                    'value of a checkbox field created after this record is unsettled',
                )

    def append_event(self, fields: Mapping[str, FieldValue], *, unknown: Collection[str]) -> Row:
        """Append declared event fields and mark all unsettled declared values unknown.

        Args:
            fields: Known common event values; undeclared fields are ignored.
            unknown: Fields whose supplied or default value is unsettled.

        Returns:
            The stored event row with the next EventLogEntry ID.
        """
        row = self._new_row('EventLogEntry')
        for name in self.schema.fields['EventLogEntry']:
            if name in {'id', 'type'}:
                continue
            if name in fields:
                row[name] = copy.deepcopy(fields[name])
            if name in unknown or name not in fields:
                self.mark_unknown(row, name, 'event field was not observed')
        identity = self._next_ids['EventLogEntry']
        self._next_ids['EventLogEntry'] += 1
        row['id'] = identity
        row['type'] = 'EventLogEntry'
        self._remember_identity('EventLogEntry', identity)
        self._db['EventLogEntry'][identity] = row
        return row

    def require(self, entity_type: str, entity_id: int) -> Row:
        """Return a live stored row, including retired records.

        Raises:
            ServerFaultError: If the entity type is undeclared or the ID is
                missing. A missing ID uses error code 104.
        """
        self._validate_entity_type(entity_type)
        row = self._db[entity_type].get(entity_id)
        if row is None:
            raise ServerFaultError(
                f'No entity of type {entity_type} exists with id {entity_id}', error_code=104
            )
        return row

    def reserve_id(self, entity_type: str) -> int:
        """Consume the next ID of a declared entity type without creating a row.

        Raises:
            ServerFaultError: If the entity type is not declared.
        """
        self._validate_entity_type(entity_type)
        identity = self._next_ids[entity_type]
        self._next_ids[entity_type] += 1
        return identity

    def restore(self, snapshot: object) -> None:
        """Restore table contents in place without rewinding ID allocation.

        Args:
            snapshot: An opaque snapshot previously returned by this store.
        """
        self.created_connections = []
        tables, self.events_unknown, side_tables = cast(
            'tuple[dict[str, dict[int, Row]], str | None, dict[str, dict[_SideKey, FieldValue]]]',
            snapshot,
        )
        for entity_type, saved in tables.items():
            table = self._db[entity_type]
            table.clear()
            table.update(copy.deepcopy(saved))
        self._side_tables = copy.deepcopy(side_tables)

    def row(self, entity_type: str, entity_id: int) -> Row | None:
        """Return a live row, including retired rows, or `None` for a missing ID.

        Raises:
            ServerFaultError: If the entity type is absent from the schema.
        """
        self._validate_entity_type(entity_type)
        candidate = self._candidate
        if candidate is not None and (candidate['type'], candidate['id']) == (
            entity_type,
            entity_id,
        ):
            return candidate
        return self._db[entity_type].get(entity_id)

    def _link_task_entities(self, row: Row, added: Sequence[_EntityLink]) -> None:
        """Add the entities of a Note's newly linked Tasks to its note_links, as the site does.

        Raises:
            MockgunError: If a Task's entity is unknown, or is retired, whose
                effect is unobserved.
        """
        if (
            row['type'] != 'Note'
            or not self.schema.declares('Note', 'note_links')
            or not self.schema.declares('Task', 'entity')
        ):
            return
        members = list(cast('list[_EntityLink]', row.get('note_links') or []))
        for link in added:
            task = self.row('Task', cast('int', link['id']))
            if task is None:
                continue
            self.check_known(task, 'entity')
            entity = task.get('entity')
            if not isinstance(entity, dict):
                continue
            target = self.row(cast('str', entity['type']), cast('int', entity['id']))
            if target is None:
                continue
            if target['__retired']:
                raise MockgunError(
                    f'Note.note_links from Task {task["id"]}: its retired '
                    f'{target["type"]} {target["id"]} has an unobserved effect'
                )
            identity: _EntityLink = {'type': target['type'], 'id': target['id']}
            if identity not in members:
                members.append(identity)
        # 2026-11-10: creates and updates add each new Task's entity; removing a Task
        # leaves note_links alone.
        row['note_links'] = cast('FieldValue', members)

    def check_link_targets(
        self, entity_type: str, fields: Mapping[str, object], operation: str
    ) -> None:
        """Refuse a write naming a retired record in a link field, in any update mode.

        Args:
            entity_type: Entity type being written.
            fields: Supplied field values; undeclared fields are left to
                validation.
            operation: `create` or `update`, selecting the site's message.

        Raises:
            ServerFaultError: If a supplied link names a retired record, with
                error code 104 (live comparison, 2026-11-10).
        """
        for field_name, value in fields.items():
            if not self.schema.declares(entity_type, field_name) or self._schema_field_type(
                entity_type, field_name
            ) not in {'entity', 'multi_entity'}:
                continue
            for item in cast('list[object]', value) if isinstance(value, list) else [value]:
                if not isinstance(item, dict):
                    continue
                link = cast('dict[str, object]', item)
                kind, identity = link.get('type'), link.get('id')
                if not isinstance(kind, str) or not isinstance(identity, int):
                    continue
                target = self.row(kind, identity) if self.schema.declares(kind) else None
                if target is None or not target['__retired']:
                    continue
                failure = f'Update failed for [{entity_type}.{field_name}]: Value is not legal.'
                message = (
                    f'API update() CRUD ERROR #5: {failure}'
                    if operation == 'update'
                    else f'API create() CRUD ERROR #6: Invalid field value, update failed '
                    f'[5 - {failure}]'
                )
                raise ServerFaultError(message, error_code=104)

    def rows(self, entity_type: str, *, retired: bool) -> list[Row]:
        """Return live rows of the requested retirement state in ascending ID order.

        Raises:
            ServerFaultError: If the entity type is absent from the schema.
        """
        self._validate_entity_type(entity_type)
        return [row for row in self._db[entity_type].values() if bool(row['__retired']) is retired]

    def linking(self, entity_type: str, entity_id: int) -> list[tuple[Row, str]]:
        """Return active non-event holders and declared fields linking the identity.

        Returns:
            Pairs of live stored holder rows and the names of their linking
            fields.
        """
        result: list[tuple[Row, str]] = []
        for kind in self._db:
            if kind == 'EventLogEntry':
                continue
            for row in self.rows(kind, retired=False):
                for field in self.schema.fields[kind]:
                    if self._schema_field_type(kind, field) not in {'entity', 'multi_entity'}:
                        continue
                    if any(
                        link['id'] == entity_id
                        for link in self._links(row.get(field), entity_type)
                    ):
                        result.append((row, field))
        return result

    def side_get(self, table: str, key: _SideKey) -> FieldValue:
        """Return a detached side value, or `None` when the key is absent.

        Args:
            table: One of the store's declared side tables.
            key: Identity within that table.
        """
        return copy.deepcopy(self._side_tables[table].get(key))

    def side_items(self, table: str) -> list[tuple[_SideKey, FieldValue]]:
        """Return detached side-table entries in insertion order.

        Args:
            table: One of the store's declared side tables.
        """
        return copy.deepcopy(list(self._side_tables[table].items()))

    def side_set(self, table: str, key: _SideKey, value: FieldValue) -> None:
        """Store a detached value, or delete its key when the value is `None`.

        Mutations participate in the enclosing transaction, if any.

        Args:
            table: One of the store's declared side tables.
            key: Identity within that table.
            value: Value to copy, or `None` to delete the entry.
        """
        entries = self._side_tables[table]
        undo = self._side_undo
        identity = (table, key)
        if undo is not None and identity not in undo:
            undo[identity] = copy.deepcopy(entries.get(key))
        if value is None:
            entries.pop(key, None)
        else:
            entries[key] = copy.deepcopy(value)

    def snapshot(self) -> object:
        """Return detached record and side tables, excluding all ID counters."""
        return copy.deepcopy(self._db), self.events_unknown, copy.deepcopy(self._side_tables)

    def taint_events(self, reason: str) -> None:
        """Preserve the first unsettled write when the site declares an event log."""
        if self.schema.declares('EventLogEntry') and self.events_unknown is None:
            self.events_unknown = reason

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Roll back record and side-table writes, joining any enclosing journal.

        All ID counters, link UUIDs and recorded actors remain outside
        rollback.
        Failures restore event certainty and clear transient connection rows.
        """
        if self._undo is not None:
            yield
            return
        undo: dict[tuple[str, int], Row | None] = {}
        self._undo = undo
        side_undo: dict[tuple[str, _SideKey], FieldValue] = {}
        self._side_undo = side_undo
        events_unknown = self.events_unknown
        try:
            yield
        except BaseException:
            reordered: set[str] = set()
            for (entity_type, entity_id), original in reversed(undo.items()):
                table = self._db[entity_type]
                if original is None:
                    table.pop(entity_id, None)
                else:
                    if entity_id not in table:
                        reordered.add(entity_type)
                    table[entity_id] = original
            # IDs are inserted in ascending order. Reinserted deleted rows must
            # regain that order; only failed deletions need this shallow rebuild.
            for entity_type in reordered:
                table = self._db[entity_type]
                ordered = sorted(table.items())
                table.clear()
                table.update(ordered)
            for (table_name, key), value in reversed(side_undo.items()):
                entries = self._side_tables[table_name]
                if value is None:
                    entries.pop(key, None)
                else:
                    entries[key] = value
            self.events_unknown = events_unknown
            self.created_connections = []
            raise
        finally:
            self._undo = None
            self._side_undo = None

    def taint(
        self,
        row: Row,
        reason: str,
        source: Row | None,
        *,
        link: tuple[str, Row] | None = None,
        active: bool = False,
    ) -> None:
        """Mark unsettled lifecycle state, affected outgoing links and audit values.

        Args:
            row: Live row whose state becomes unsettled.
            reason: Explanation retained when the row is first tainted.
            source: Row responsible for the uncertainty, or `None`.
            link: Optional pair of outgoing field name and affected target row.
            active: Whether the row is known to remain active despite its
                unsettled effects.
        """
        self.remember(row)
        taint = cast(
            'dict[str, FieldValue]',
            row.setdefault(
                '__taint',
                {
                    'reason': reason,
                    'source': [source['type'], source['id']] if source is not None else None,
                    'links': [],
                    'active': active,
                },
            ),
        )
        if link is not None:
            field, target = link
            member: FieldValue = [field, target['type'], target['id']]
            members = cast('list[FieldValue]', taint['links'])
            if member not in members:
                members.append(member)
        for field in ('updated_at', 'updated_by'):
            if self.schema.declares(cast('str', row['type']), field):
                self.mark_unknown(row, field, reason)

    @staticmethod
    def tainted(row: Row) -> str | None:
        """Return the stored lifecycle uncertainty reason, or `None` when untainted."""
        taint = cast('dict[str, FieldValue]', row.get('__taint', {}))
        return cast('str', taint['reason']) if taint else None

    def _connection_delta(
        self, source: tuple[_EntityLink, str], target: _EntityLink, *, added: bool
    ) -> None:
        """Mirror a supported pair delta into its declared connection table."""
        writer, field = source
        writer_type = cast('str', writer['type'])
        target_type = cast('str', target['type'])
        definition = CONNECTION_FIELDS.get((writer_type, field, target_type))
        if definition is None:
            return
        connection_type, writer_field, target_field = definition
        if not (
            self.schema.declares(connection_type, writer_field)
            and self.schema.declares(connection_type, target_field)
        ):
            return
        target_id = target['id']
        if not isinstance(target_id, int) or self.row(target_type, target_id) is None:
            return
        endpoints = {writer_field: writer, target_field: target}
        matches = [
            row
            for row in self._db[connection_type].values()
            if all(row[name] == link for name, link in endpoints.items())
        ]
        if added:
            if matches:
                return
            row = self._new_row(connection_type)
            row.update(copy.deepcopy(endpoints))
            connection_id = self._next_ids[connection_type]
            row['id'] = connection_id
            self._next_ids[connection_type] += 1
            self._remember_identity(connection_type, connection_id)
            self._db[connection_type][connection_id] = row
            self.created_connections.append(row)
        else:
            for row in matches:
                reason = self.tainted(row)
                if reason is not None:
                    raise MockgunError(
                        f'{connection_type} {row["id"]} {target_type} {target_id}: {reason}'
                    )
                self.remember(row)
                del self._db[connection_type][cast('int', row['id'])]

    def _sequence_shots_provenance(self, before: Row, after: Row, data: dict[str, object]) -> None:
        """Retain the observed from-null boundary before inverse writes erase it."""
        after.pop('__sequence_shots_from_null', None)
        if (
            after['type'] != 'Sequence'
            or 'shots' not in data
            or self.schema.field_type('Sequence', 'shots') != 'multi_entity'
        ):
            return
        old = cast('list[Row]', before.get('shots', []))
        new = cast('list[Row]', after.get('shots', []))
        added = [link for link in new if link not in old]
        if (
            len(added) != 1
            or added[0]['type'] != 'Shot'
            or not self.schema.declares('Shot', 'sg_sequence')
            or self.schema.field_type('Shot', 'sg_sequence') != 'entity'
        ):
            return
        shot = self.row('Shot', cast('int', added[0]['id']))
        if shot is None:
            return
        unknown = cast('dict[str, FieldValue]', shot.get('__unknown', {}))
        after['__sequence_shots_from_null'] = (
            shot.get('sg_sequence') is None
            and 'sg_sequence' not in unknown
            and not self.tainted(shot)
        )

    def check_writable(self, row: Row) -> None:
        """Reject a mutation of a record whose lifecycle effect is unsettled.

        Raises:
            MockgunError: If the record carries an unsettled lifecycle marker.
        """
        reason = self.tainted(row)
        if reason is not None:
            raise MockgunError(f'{row["type"]} {row["id"]}: {reason}')

    def _check_link_writes(self, entity_type: str, data: dict[str, object]) -> None:
        """Reject supplied links to records already tainted by an earlier write."""
        for field_name, value in data.items():
            if self._schema_field_type(entity_type, field_name) not in {'entity', 'multi_entity'}:
                continue
            links = cast('list[object]', value) if isinstance(value, list) else [value]
            for value_link in links:
                if isinstance(value_link, dict):
                    link = cast('dict[str, object]', value_link)
                    if not self.schema.declares(cast('str', link['type'])):
                        continue
                    target = self.row(cast('str', link['type']), cast('int', link['id']))
                    if target is not None:
                        self.check_writable(target)

    def _mark_task_inverse(self, before: Row, after: Row) -> None:
        """Mirror the observed first assignment and mark other inverses unsettled."""
        if after['type'] != 'Version' or before.get('sg_task') == after.get('sg_task'):
            return
        if (
            not self.schema.declares('Task', 'sg_versions')
            or self._schema_field_type('Task', 'sg_versions') != 'multi_entity'
        ):
            return
        for link in (before.get('sg_task'), after.get('sg_task')):
            if isinstance(link, dict) and link.get('type') == 'Task':
                task = self.row('Task', cast('int', link['id']))
                if task is not None:
                    self.check_writable(task)
                    if before and before.get('sg_task') is None and link == after.get('sg_task'):
                        self._mirror_first_task_inverse(task, after)
                        continue
                    self.mark_unknown(
                        task, 'sg_versions', 'Version.sg_task inverse value is unobserved'
                    )

    def _remove_previous_pair(
        self, previous: FieldValue, writer_type: str, field: str, target: _EntityLink
    ) -> None:
        """Remove a reparented target from its previous owner's multi-entity field."""
        if not isinstance(previous, dict) or previous.get('type') != writer_type:
            return
        previous_id = previous.get('id')
        if not isinstance(previous_id, int):
            return
        previous_row = self._db[writer_type].get(previous_id)
        if previous_row is None or self._schema_field_type(writer_type, field) != 'multi_entity':
            return
        self.check_writable(previous_row)
        self.remember(previous_row)
        # W17: a Sequence losing a Shot through another Sequence keeps its audit.
        previous_row[field] = [
            item for item in cast('list[FieldValue]', previous_row[field]) if item != target
        ]

    def _mirror_pair(
        self,
        source: tuple[_EntityLink, str],
        target: _EntityLink,
        mirror: tuple[str, str],
        *,
        added: bool,
        context: RequestContext,
    ) -> None:
        """Apply one observed pair delta and its side-specific audit policy."""
        writer, field = source
        target_type, mirror_field = mirror
        target_id = target['id']
        if not isinstance(target_id, int):
            return
        target_row = self._db[target_type].get(target_id)
        if target_row is None:
            return
        self.check_writable(target_row)
        old_value = target_row[mirror_field]
        writer_type = cast('str', writer['type'])
        if self._schema_field_type(target_type, mirror_field) == 'multi_entity':
            members = cast('list[FieldValue]', old_value)
            value: FieldValue
            if added:
                value = [*members, writer] if writer not in members else members
            else:
                value = [item for item in members if item != writer]
        elif added:
            value = writer
            if old_value != writer:
                self._remove_previous_pair(old_value, writer_type, field, target)
        else:
            value = None if old_value == writer else old_value
        if value == old_value:
            return
        self.remember(target_row)
        target_row[mirror_field] = copy.deepcopy(value)
        preserve = (writer_type, field) == ('Note', 'note_links') or (
            (writer_type, field) == ('Sequence', 'shots') and not added
        )
        if not preserve:
            self._set_updated_audit(target_type, target_row, context)

    def _apply_pairs(
        self,
        entity_type: str,
        before: Row,
        after: Row,
        fields: Collection[str],
        context: RequestContext,
    ) -> None:
        """Mirror changed identities only where both declared fields are relations."""
        writer: _EntityLink = {'type': entity_type, 'id': after['id']}
        for (source_type, field, target_type), mirror in _PAIRED_FIELDS.items():
            if source_type != entity_type or field not in fields:
                continue
            if not self.schema.declares(*mirror):
                continue
            if self._schema_field_type(entity_type, field) not in {'entity', 'multi_entity'}:
                continue
            if self._schema_field_type(*mirror) not in {'entity', 'multi_entity'}:
                continue
            old_links = self._links(before.get(field), target_type)
            new_links = self._links(after[field], target_type)
            for target in old_links:
                if target not in new_links:
                    self._connection_delta((writer, field), target, added=False)
                    self._mirror_pair(
                        (writer, field), target, mirror, added=False, context=context
                    )
            for target in new_links:
                if target not in old_links:
                    self._mirror_pair((writer, field), target, mirror, added=True, context=context)
                    self._connection_delta((writer, field), target, added=True)

    @staticmethod
    def kept_fields(
        row: Row,
        fields: Mapping[str, object],
        modes: Mapping[str, str],
        kept: Mapping[str, Sequence[Mapping[str, FieldValue]]],
    ) -> dict[str, object]:
        """Apply visible-member modes and prepend the retained hidden members.

        Args:
            row: Stored row supplying the current multi-entity members.
            fields: Incoming field values to copy and adjust.
            modes: Per-field `set`, `add`, or `remove` modes; omitted modes use
                `set`.
            kept: Hidden member dictionaries to retain, keyed by field name.

        Returns:
            A new field mapping whose adjusted multi-entity values contain
            copied hidden members followed by the updated visible members.
            Values for unadjusted fields retain their original references.
        """
        result = dict(fields)
        for field, members in kept.items():
            visible = [
                item for item in cast('list[_EntityLink]', row[field]) if item not in members
            ]
            result[field] = [
                *[dict(member) for member in members],
                *Store._update_multi_entity(
                    visible, cast('list[_EntityLink]', fields[field]), modes.get(field, 'set')
                ),
            ]
        return result

    def prepare_create(
        self,
        entity_type: str,
        fields: dict[str, object],
        *,
        context: RequestContext,
    ) -> Row:
        """Build a validated candidate without changing records or allocating an ID.

        Args:
            entity_type: Schema entity type to create.
            fields: Field values for the new row.
            context: Request actor and time; a null actor marks bootstrap.

        Returns:
            A detached candidate with the next available ID and schema
            defaults.

        Raises:
            ServerFaultError: If entity, field, or value validation has a
                modeled server fault.
            MockgunError: If a field type, linked state, or computed result is
                unsupported or unsettled.
            OverflowError: If converting a timestamp field to UTC exceeds the
                supported datetime range.
        """
        data = self._expand_local_paths(self._parse_fields(entity_type, fields))
        self._set_created_audit(entity_type, data, context)
        self._validate_entity_data(entity_type, data, 'create')
        self._check_link_writes(entity_type, data)
        self.check_link_targets(entity_type, data, 'create')
        row = self._new_row(entity_type)
        for field_name, value in data.items():
            if field_name == 'cached_display_name':
                continue
            field_type = self._schema_field_type(entity_type, field_name)
            row[field_name] = self._normalize_field_value(field_type, cast('FieldValue', value))
        row['id'] = self._next_ids[entity_type]
        self._apply_create_computed(row, fields, context)
        if 'tasks' in fields:
            self._link_task_entities(row, self._links(row.get('tasks'), 'Task'))
        return row

    def create(
        self,
        entity_type: str,
        fields: dict[str, object],
        *,
        context: RequestContext,
    ) -> Row:
        """Create a validated row with increasing per-entity IDs and schema defaults.

        Use an enclosing `transaction()` when a failure must roll back the row
        and dependent updates.

        Args:
            entity_type: Schema entity type to create.
            fields: Field values for the new row.
            context: Request actor and time; a null actor marks bootstrap.

        Returns:
            The live stored row, including internal retirement state.

        Raises:
            ServerFaultError: If entity, field, or value validation has a
                modeled server fault.
            MockgunError: If a field type, linked state, or dependent effect is
                unsupported or unsettled.
            OverflowError: If converting a timestamp field to UTC exceeds the
                supported datetime range.
        """
        self.created_connections = []
        row = self.prepare_create(entity_type, fields, context=context)
        self._next_ids[entity_type] += 1
        self._remember_identity(entity_type, cast('int', row['id']))
        self._db[entity_type][cast('int', row['id'])] = row
        mirrored = [
            *fields,
            *(['note_links'] if entity_type == 'Note' and 'tasks' in fields else []),
        ]
        self._apply_pairs(entity_type, {}, row, mirrored, context)
        if context.actor is not None:
            self._mark_task_inverse({}, row)
        return row

    def prepare_update(
        self,
        entity_type: str,
        entity_id: int,
        fields: dict[str, object],
        modes: dict[str, str],
        *,
        context: RequestContext,
    ) -> Row:
        """Build a validated replacement without changing the stored record.

        Args:
            entity_type: Schema entity type to update.
            entity_id: Existing row ID.
            fields: Replacement field values from the wire.
            modes: Per-field multi-entity `set`, `add`, or `remove` modes.
            context: Request actor and time for updated audit fields.

        Returns:
            A detached replacement row with validated field changes and audit
            values, ready to be stored.

        Raises:
            ServerFaultError: If the record is absent or entity, field, or
                value validation has a modeled server fault.
            MockgunError: If a field type, linked state, or record lifecycle is
                unsupported or unsettled.
            OverflowError: If converting a timestamp field to UTC exceeds the
                supported datetime range.
        """
        data = self._parse_fields(entity_type, fields)
        # C7 P4-17: clearing the image also clears the declared filmstrip.
        if (
            'image' in data
            and data['image'] is None
            and self.schema.declares(entity_type, 'filmstrip_image')
        ):
            data['filmstrip_image'] = None
        for field_name in data:
            if field_name in {'created_at', 'created_by', 'updated_at', 'updated_by'}:
                raise ServerFaultError(
                    f'API update() {entity_type}.{field_name} is editable on create only',
                    error_code=103,
                )
        self._validate_entity_data(entity_type, data, 'update')
        self._check_link_writes(entity_type, data)
        stored = self.require(entity_type, entity_id)
        self.check_writable(stored)
        row = copy.deepcopy(stored)
        for field_name, value in data.items():
            if field_name == 'cached_display_name':
                continue
            field_type = self._schema_field_type(entity_type, field_name)
            if field_type == 'multi_entity':
                members = cast('list[_EntityLink]', row[field_name])
                updated = self._update_multi_entity(
                    members, cast('list[_EntityLink]', value), modes.get(field_name, 'set')
                )
                # 2026-11-10: the site keeps no member order, so a reordering set changes
                # nothing, including audit fields and event rows.
                if _identities(updated) != _identities(members):
                    row[field_name] = cast('FieldValue', updated)
            else:
                row[field_name] = self._normalize_field_value(
                    field_type, cast('FieldValue', value)
                )
        if 'tasks' in data:
            old = self._links(stored.get('tasks'), 'Task')
            self._link_task_entities(
                row, [link for link in self._links(row.get('tasks'), 'Task') if link not in old]
            )
        self._update_certainty(row, stored, data, modes, context)
        self._sequence_shots_provenance(stored, row, data)
        return row

    def retire(
        self, entity_type: str, entity_id: int, *, context: RequestContext | None = None
    ) -> bool:
        """Retire an active entity.

        Args:
            entity_type: Schema entity type to retire.
            entity_id: ID of the existing entity.
            context: Request actor and time, when called through a server
                request.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ServerFaultError: If the entity type or ID is unknown.
        """
        row = self.require(entity_type, entity_id)
        if row['__retired']:
            return False
        self.remember(row)
        row['__retired'] = True
        if context is not None:
            row['__retired_at'] = context.now
            self._set_updated_audit(entity_type, row, context)
        return True

    def revive(
        self, entity_type: str, entity_id: int, *, context: RequestContext | None = None
    ) -> bool:
        """Restore a retired entity to active state.

        Args:
            entity_type: Schema entity type to restore.
            entity_id: ID of the existing entity.
            context: Request actor and time, when called through a server
                request.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ServerFaultError: If the entity type or ID is unknown.
        """
        row = self.require(entity_type, entity_id)
        if not row['__retired']:
            return False
        self.remember(row)
        row['__retired'] = False
        row.pop('__retired_at', None)
        if context is not None:
            self._set_updated_audit(entity_type, row, context)
        return True

    def tainted_rows(self, entity_type: str) -> list[Row]:
        """Return live tainted rows of one type from both retirement scopes.

        Raises:
            ServerFaultError: If the entity type is absent from the schema.
        """
        self._validate_entity_type(entity_type)
        return [
            row
            for row in self._db[entity_type].values()
            if row.get('__taint') and self.tainted(row)
        ]

    def update(
        self,
        entity_type: str,
        entity_id: int,
        fields: dict[str, object],
        modes: dict[str, str],
        *,
        context: RequestContext,
    ) -> Row:
        """Update validated fields and return the live stored row.

        Use an enclosing `transaction()` when a failure must roll back the row
        and dependent updates.

        Args:
            entity_type: Schema entity type to update.
            entity_id: Existing row ID.
            fields: Replacement field values from the wire.
            modes: Per-field multi-entity `set`, `add`, or `remove` modes.
            context: Request actor and time for updated audit fields.

        Raises:
            ServerFaultError: If the record is absent or entity, field, or
                value validation has a modeled server fault.
            MockgunError: If a field type, linked state, record lifecycle, or
                dependent effect is unsupported or unsettled.
            OverflowError: If converting a timestamp field to UTC exceeds the
                supported datetime range.
        """
        self.created_connections = []
        row = self.prepare_update(entity_type, entity_id, fields, modes, context=context)
        stored = self.require(entity_type, entity_id)
        if row != stored:
            before = copy.deepcopy(stored)
            self.remember(stored)
            stored.clear()
            stored.update(row)
            mirrored = [
                *fields,
                *(['note_links'] if entity_type == 'Note' and 'tasks' in fields else []),
            ]
            self._apply_pairs(entity_type, before, stored, mirrored, context)
            self._mark_task_inverse(before, stored)
        image_fields = {'image', 'filmstrip_image'} & fields.keys()
        if 'image' in image_fields and fields['image'] is None:
            image_fields.add('filmstrip_image')
        for field in image_fields:
            self.side_set('image_identity', (entity_type, entity_id, field), None)
        if image_fields:
            self.side_set(
                'activity_taint',
                (entity_type, entity_id),
                f'update {entity_type} {entity_id} image fields: activity is unsettled',
            )
        return stored


def _identities(links: Sequence[_EntityLink]) -> list[tuple[str, int]]:
    """Return validated member identities in a stable order for membership comparison."""
    return sorted((cast('str', link['type']), cast('int', link['id'])) for link in links)


def _observed_task_form(fields: Mapping[str, object], context: RequestContext) -> bool:
    """Identify the script Task create forms with observed computed values."""
    entity = fields.get('entity')
    content = fields.get('content')
    return (
        context.session_kind == 'script'
        and set(fields) <= {'project', 'content', 'entity'}
        and {'project', 'content'} <= set(fields)
        and isinstance(content, str)
        and content != ''
        and (
            entity is None
            or (
                isinstance(entity, dict)
                and cast('dict[str, object]', entity).get('type') == 'Shot'
            )
        )
    )


def _row_default(data_type: str, value: FieldValue) -> FieldValue:
    """Copy a schema default, reading a generated schema's integer text as an integer."""
    # 2026-11-10 trial capture: generated schemas give number, percent and duration defaults
    # as text such as '0' and '100', and the site reads those fields as integers.
    if _FIELD_PYTHON_TYPES.get(data_type) is int and isinstance(value, str):
        digits = value.removeprefix('-')
        if digits.isascii() and digits.isdigit():
            return int(value)
    return copy.deepcopy(value)
