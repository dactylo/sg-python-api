"""Evaluate existing mockgun filters, ordering, projection, and server paging."""

import copy
import datetime
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations, groupby, product
from math import prod
from string import ascii_letters, ascii_lowercase, ascii_uppercase, digits
from typing import Protocol, Self, TypeAlias, cast

from ._dates import in_window, validate_relative
from ._errors import MockgunError, ServerFaultError
from ._events import check_meta
from ._links import (
    RecordState,
    deciding_record,
    derived_name,
    display_name,
    hidden,
    named_link,
)
from ._presets import (
    Preset,
    apply_presets,
    assuming,
    latest_groups,
    latest_rank_key,
    preset_dependencies,
    validate_presets,
)
from ._schema import SiteSchema
from ._sessions import RequestContext
from ._store import UPLOAD_LINK_REASON_PREFIX, FieldValue, Row, Store
from ._wire import format_wire_datetime, parse_wire_datetime

_Bit: TypeAlias = tuple[str, int, str, str, int]
_Decision: TypeAlias = 'bool | _Failure | _Fork'
_StateReads: TypeAlias = dict[tuple[str, int], set[tuple[str, str, int] | None]]
_STATE_LIMIT = 64
_ASCII_LOWER = str.maketrans(ascii_uppercase, ascii_lowercase)
_COLLATION_CHARACTERS = frozenset(ascii_letters + digits)
_COLLATION_IGNORED = str.maketrans('', '', '_-. ')
_EntityLink: TypeAlias = dict[str, FieldValue]
_LINK_PATH_PARTS = 3
_NOTE_LINK_PAIR = {'Asset', 'Shot'}
_NUMERIC_TYPES = frozenset({'date', 'date_time', 'duration', 'float', 'number', 'percent'})
_ORDER_TYPES = _NUMERIC_TYPES | {
    'checkbox',
    'entity',
    'entity_type',
    'image',
    'list',
    'multi_entity',
    'status_list',
    'text',
    'url',
}
_RELATIVE_RELATIONS = frozenset(
    {
        'in_last',
        'in_next',
        'not_in_last',
        'not_in_next',
        'in_calendar_day',
        'in_calendar_week',
        'in_calendar_month',
        'in_calendar_year',
    }
)
_UNFILTERABLE_TYPES = frozenset({'calculated', 'serializable', 'summary', 'url'})
_VALID_RELATIONS: dict[str, tuple[str, ...]] = {
    'checkbox': ('is', 'is_not'),
    'color': ('is', 'is_not', 'in', 'not_in'),
    'date': (
        'is',
        'is_not',
        'greater_than',
        'less_than',
        'in_last',
        'not_in_last',
        'in_next',
        'not_in_next',
        'in_calendar_week',
        'in_calendar_month',
        'in_calendar_day',
        'in_calendar_year',
        'between',
        'in',
        'not_in',
    ),
    'date_time': (
        'is',
        'is_not',
        'greater_than',
        'less_than',
        'in_last',
        'not_in_last',
        'in_next',
        'not_in_next',
        'in_calendar_week',
        'in_calendar_month',
        'in_calendar_day',
        'in_calendar_year',
        'between',
        'in',
        'not_in',
    ),
    'duration': ('is', 'is_not', 'greater_than', 'less_than', 'between', 'in', 'not_in'),
    'entity': (
        'is',
        'is_not',
        'name_contains',
        'name_not_contains',
        'name_is',
        'type_is',
        'type_is_not',
        'in',
        'not_in',
    ),
    'float': ('is', 'is_not', 'greater_than', 'less_than', 'between', 'in', 'not_in'),
    'image': ('is', 'is_not'),
    'list': ('is', 'is_not', 'in', 'not_in'),
    'multi_entity': (
        'is',
        'is_not',
        'name_contains',
        'name_not_contains',
        'name_is',
        'type_is',
        'type_is_not',
        'in',
        'not_in',
    ),
    'number': ('is', 'is_not', 'greater_than', 'less_than', 'between', 'in', 'not_in'),
    'percent': ('is', 'is_not', 'greater_than', 'less_than', 'between', 'in', 'not_in'),
    'pivot_column': ('is', 'is_not'),
    'status_list': ('is', 'is_not', 'in', 'not_in'),
    'text': (
        'contains',
        'not_contains',
        'is',
        'is_not',
        'starts_with',
        'ends_with',
        'in',
        'not_in',
    ),
}


class _ComparableFieldValue(Protocol):
    """Describe values with ordered comparisons."""

    def __gt__(self, other: Self, /) -> bool:
        """Return whether this value sorts after another value."""
        ...

    def __le__(self, other: Self, /) -> bool:
        """Return whether this value does not sort after another value."""
        ...

    def __lt__(self, other: Self, /) -> bool:
        """Return whether this value sorts before another value."""
        ...


@dataclass(frozen=True)
class ReadScope:
    """Immutable selection context for retirement and archive scope.

    Attributes:
        retired: Select retired records instead of active records.
        context: Actor and timestamp captured for the request.
        include_archived_projects: Whether archived Projects and their records
            remain visible.
        presets: Additional filter presets to validate and apply before
            ordering.
    """

    retired: bool
    context: RequestContext
    include_archived_projects: bool = True
    presets: object = None


@dataclass(frozen=True)
class _Failure:
    """Keep a reachable evaluation error while reducing symbolic conditions."""

    message: str


@dataclass(frozen=True)
class _Fork:
    """Share the two outcomes of one retirement or outgoing visibility bit."""

    bit: _Bit
    off: _Decision
    on: _Decision


@dataclass
class _Candidate:
    """Keep scope, group admission, and selection separate until reduction ends."""

    row: Row
    member: _Decision
    archive: _Decision
    selected: _Decision


class _RowReader:
    """Read stored fields and link visibility for selection or projection."""

    def __init__(self, store: Store, schema: SiteSchema) -> None:
        """Bind row sources without requiring a selection scope."""
        self._store = store
        self._schema = schema
        self._assume: dict[tuple[str, int], RecordState] | None = None

    def _direct_field_value(
        self, entity_type: str, row: Row, field_name: str, *, membership: bool = False
    ) -> FieldValue:
        """Read a stored field or compute its non-retained display cache."""
        if entity_type == 'Project' and field_name == 'last_accessed_by_current_user':
            context = self._store.context
            actor = context.actor if context is not None else None
            value = (
                self._store.side_get(
                    'last_access', (cast('int', row['id']), actor.entity_type, actor.entity_id)
                )
                if actor is not None
                else None
            )
            if value is None:
                raise MockgunError(
                    'mockgun cannot read Project.last_accessed_by_current_user '
                    f'on record {row["id"]}: '
                    'actor has never accessed the Project'
                )
            return value
        if '__unknown' in row or (not membership and '__unordered' in row):
            self._store.check_known(row, field_name, membership=membership)
        if entity_type == 'EventLogEntry' and field_name == 'meta':
            check_meta(self._store, row)
        if field_name == 'cached_display_name' and entity_type != 'EventLogEntry':
            return display_name(self._store, entity_type, cast('int', row['id']))
        return row.get(field_name)

    def _hidden(self, row: Row, field: str, link: Mapping[str, FieldValue]) -> bool:
        """Resolve link visibility under the reduced unit's concrete assumption."""
        return hidden(self._store, row, field, link, assume=self._assume)

    def _visible_value(self, row: Row, field_name: str, value: FieldValue) -> FieldValue:
        """Suppress listed retired links while preserving stored values."""
        if isinstance(value, dict):
            return None if self._hidden(row, field_name, value) else value
        if isinstance(value, list):
            return [
                member
                for member in value
                if not isinstance(member, dict) or not self._hidden(row, field_name, member)
            ]
        return value


class _Projection(_RowReader):
    """Project detached fields with explicit context for session-specific ordering."""

    def __init__(self, store: Store, schema: SiteSchema, context: RequestContext | None) -> None:
        """Bind projection with its explicit context for session-specific ordering."""
        super().__init__(store, schema)
        self._projection_context = context

    def _project_value(self, entity_type: str, row: Row, path: str) -> tuple[bool, FieldValue]:
        """Follow one valid single-link path and distinguish omission from null."""
        if entity_type == 'EventLogEntry' and path == 'audit_trail':
            return False, None
        parts = path.split('.', 2)
        if len(parts) != _LINK_PATH_PARTS:
            value = self._direct_field_value(entity_type, row, path)
            visible = self._visible_value(row, path, value)
            return True, _ordered_note_links(row, path, value, visible, self._projection_context)
        field, linked_type, rest = parts
        link_value = self._direct_field_value(entity_type, row, field)
        if self._visible_value(row, field, link_value) != link_value:
            raise MockgunError(
                f'mockgun cannot project {entity_type}.{path}: hidden {entity_type}.{field} link'
            )
        if link_value is None:
            if not self._schema.declares(linked_type):
                raise MockgunError(
                    f'mockgun cannot project {entity_type}.{path}: '
                    f'{linked_type} is not a declared entity type'
                )
            return True, None
        if not isinstance(link_value, dict):
            raise MockgunError(f'mockgun cannot project {entity_type}.{path}: invalid link')
        if link_value['type'] != linked_type:
            return False, None
        if not self._schema.declares(linked_type):
            return False, None
        linked_row = self._store.row(linked_type, cast('int', link_value['id']))
        if linked_row is None:
            raise MockgunError(
                f'mockgun cannot project {entity_type}.{path}: missing {linked_type} link'
            )
        return self._project_value(linked_type, linked_row, rest)

    def _return_field_type(self, entity_type: str, path: str) -> str | None:
        """Resolve a readable path, omitting statically invalid positions."""
        parts = path.split('.', 2)
        field = parts[0] if len(parts) == _LINK_PATH_PARTS else path
        if field not in {'id', 'type'} and not self._schema.declares(entity_type, field):
            return None
        field_type = self._schema.field_type(entity_type, field)
        if len(parts) != _LINK_PATH_PARTS:
            return field_type
        if field_type != 'entity':
            return None
        linked_type, rest = parts[1:]
        if not self._schema.declares(linked_type):
            return 'unknown'
        return self._return_field_type(linked_type, rest)

    def project(self, rows: list[Row], return_fields: list[str]) -> list[dict[str, object]]:
        """Project detached output fields and format declared datetime values.

        Args:
            rows: Stored rows in the desired result order.
            return_fields: Direct or linked field paths to project.

        Returns:
            Detached dictionaries in input order, always containing `type` and
            `id`. Unsupported projection paths are omitted when the projection
            rules permit omission.

        Raises:
            ServerFaultError: If resolving a declared field or linked entity
                encounters a modeled schema fault.
            MockgunError: If projected values or linked identity state are
                unknown or unsupported.
            OverflowError: If converting a projected datetime to UTC exceeds
                the supported datetime range.
            ValueError: If the platform cannot convert a naive datetime.
            OSError: If local timezone conversion fails.
        """
        result: list[dict[str, object]] = []
        field_types = (
            {
                name: self._return_field_type(cast('str', rows[0]['type']), name)
                for name in return_fields
            }
            if rows
            else {}
        )
        for row in rows:
            entity_type = cast('str', row['type'])
            item: dict[str, object] = {}
            for name in return_fields:
                field_type = field_types[name]
                if field_type is None:
                    continue
                present, value = self._project_value(entity_type, row, name)
                if not present:
                    continue
                if field_type in {'entity', 'multi_entity'}:
                    if isinstance(value, list):
                        value = [
                            named_link(self._store, cast('_EntityLink', item)) for item in value
                        ]
                    elif isinstance(value, dict):
                        value = named_link(self._store, value)
                elif field_type == 'date_time':
                    value = _format_datetimes(value)
                item[name] = copy.deepcopy(value)
            item['type'] = entity_type
            item['id'] = row['id']
            result.append(item)
        return result


class _Query(_RowReader):
    """Evaluate a query against one schema and store snapshot."""

    def __init__(
        self,
        store: Store,
        schema: SiteSchema,
        scope: ReadScope,
    ) -> None:
        """Bind the store and schema for one serialized operation."""
        super().__init__(store, schema)
        self._scope = scope
        self._external: _Decision = True
        self._nodes: dict[tuple[_Bit, _Decision, _Decision], _Fork] = {}
        self._choices: dict[tuple[_Decision, _Decision, _Decision], _Decision] = {}

    def _compare_value(
        self, field_type: str, actual: FieldValue, relation: str, values: list[FieldValue]
    ) -> bool:
        """Use request time for relative filters and scalar comparison otherwise."""
        if field_type in {'date', 'date_time'} and relation in _RELATIVE_RELATIONS:
            return in_window(field_type, relation, values, actual, now=self._scope.context.now)
        return _compare(field_type, actual, relation, values)

    def _entity_order_keys(self, values: list[FieldValue]) -> list[FieldValue]:
        """Read names only for distinct non-null targets that must be compared."""
        identities = {
            (cast('str', value['type']), cast('int', value['id']))
            for value in values
            if isinstance(value, dict)
        }
        if len(identities) <= 1:
            return [None if value is None else 0 for value in values]
        names: dict[tuple[str, int], str] = {}
        for identity in identities:
            name = display_name(self._store, *identity)
            if name is None or derived_name(self._store, *identity):
                raise MockgunError('entity order needs a modeled, non-derived name')
            names[identity] = name
        if len({name.translate(_ASCII_LOWER) for name in names.values()}) != len(names):
            raise MockgunError('distinct entity targets have equal display names')
        return [
            None
            if value is None
            else names[
                (
                    cast('str', cast('_EntityLink', value)['type']),
                    cast('int', cast('_EntityLink', value)['id']),
                )
            ]
            for value in values
        ]

    def _filter_field_type(self, entity_type: str, path: str) -> str:
        """Resolve filter fields with the server's missing-field fault."""
        if entity_type == 'Project' and path == 'last_accessed_by_current_user':
            raise MockgunError(
                'mockgun cannot read Project.last_accessed_by_current_user: '
                'filtering and ordering are unsettled'
            )
        parts = path.split('.', 2)
        field = parts[0] if len(parts) == _LINK_PATH_PARTS else path
        if (
            self._schema.declares(entity_type)
            and field not in {'id', 'type'}
            and not self._schema.declares(entity_type, field)
        ):
            raise ServerFaultError(
                f"API read() {entity_type}.{field} doesn't exist", error_code=103
            )
        field_type = self._schema.field_type(entity_type, field)
        if len(parts) == _LINK_PATH_PARTS:
            if field_type not in {'entity', 'multi_entity'}:
                return self._schema.field_type(entity_type, path)
            return self._filter_field_type(parts[1], parts[2])
        return field_type

    def _fork(self, bit: _Bit, off: _Decision, on: _Decision) -> _Decision:
        """Discard a bit when its two outcomes are identical."""
        if off == on:
            return off
        return self._nodes.setdefault((bit, off, on), _Fork(bit, off, on))

    def _choose(self, condition: _Decision, yes: _Decision, no: _Decision) -> _Decision:
        """Preserve short circuits and errors while composing ordered branches."""
        if isinstance(condition, bool):
            return yes if condition else no
        if isinstance(condition, _Failure):
            return condition
        key = (condition, yes, no)
        if key in self._choices:
            return self._choices[key]
        bit = min(item.bit for item in key if isinstance(item, _Fork))
        branches: list[_Decision] = []
        for enabled in (False, True):
            parts = [
                (item.on if enabled else item.off)
                if isinstance(item, _Fork) and item.bit == bit
                else item
                for item in key
            ]
            branches.append(self._choose(parts[0], yes=parts[1], no=parts[2]))
        result = self._fork(bit, off=branches[0], on=branches[1])
        self._choices[key] = result
        return result

    def _all(self, left: _Decision, right: _Decision) -> _Decision:
        """Compose ordered conjunction without suppressing an earlier error."""
        return self._choose(left, yes=right, no=False)

    def _any(self, left: _Decision, right: _Decision) -> _Decision:
        """Compose ordered disjunction without suppressing an earlier error."""
        return self._choose(left, yes=True, no=right)

    def _archive_guard(self, left: _Candidate, right: _Candidate) -> _Decision:
        """Reject LATEST groups mixing archived and active Projects before filtering."""
        different = self._choose(
            left.archive, yes=self._choose(right.archive, yes=False, no=True), no=right.archive
        )
        together = self._all(left.member, right.member)
        return self._choose(
            self._all(together, different),
            yes=_Failure('LATEST group mixes archived and active Projects'),
            no=True,
        )

    def _errors(self, value: _Decision) -> _Decision:
        """Retain only branches capable of raising, with successful leaves equal."""
        if isinstance(value, bool):
            return True
        if isinstance(value, _Failure):
            return value
        return self._fork(value.bit, off=self._errors(value.off), on=self._errors(value.on))

    def _guard_uploaded_file(self, row: Row, path: str) -> None:
        """Reject only upload-marked Attachment file dictionaries along this path."""
        if path != 'this_file' and not path.endswith('.Attachment.this_file'):
            return
        if row['type'] == 'Attachment' and path == 'this_file':
            unknown = cast('dict[str, FieldValue]', row.get('__unknown', {}))
            reason = unknown.get('this_file')
            if isinstance(reason, str) and reason.startswith(UPLOAD_LINK_REASON_PREFIX):
                self._store.check_known(row, 'this_file')
            return
        parts = path.split('.', 2)
        if len(parts) != _LINK_PATH_PARTS:
            return
        value = row.get(parts[0])
        links = value if isinstance(value, list) else [value]
        for link in links:
            if isinstance(link, dict) and link.get('type') == parts[1]:
                target = self._store.row(parts[1], cast('int', link['id']))
                if target is not None:
                    self._guard_uploaded_file(target, parts[2])

    def _has_uncertainty(self) -> bool:
        """Check whether taints or a retired Cut can affect any query path."""
        if any(self._store.tainted_rows(kind) for kind in self._schema.fields):
            return True
        return self._schema.declares('Cut') and bool(self._store.rows('Cut', retired=True))

    def _hidden_filter_value(
        self,
        row: Row,
        field_name: str,
        value: FieldValue,
        relation: str,
        values: list[FieldValue],
    ) -> FieldValue:
        """Apply the narrow modeled filter rule for hidden links."""
        visible = self._visible_value(row, field_name, value)
        if visible != value and (relation != 'is' or not values or values[0] is None):
            raise MockgunError(f'hidden {row["type"]}.{field_name} link has unmodeled {relation}')
        return visible

    def _in_archive_scope(self, entity_type: str, row: Row, scope: ReadScope) -> bool:
        """Keep rows outside an archived Project when the flag excludes archives."""
        if scope.include_archived_projects or not self._schema.declares('Project', 'archived'):
            return True
        if entity_type == 'Project':
            return row.get('archived') is not True
        if not self._schema.declares(entity_type, 'project') or (
            self._schema.field_type(entity_type, 'project') != 'entity'
        ):
            return True
        project = self._visible_value(row, 'project', row.get('project'))
        if isinstance(project, dict) and project.get('type') == 'Project':
            project_id = project.get('id')
            if isinstance(project_id, int):
                project_row = self._store.row('Project', project_id)
                return project_row is None or project_row.get('archived') is not True
        return True

    def _linked_row(self, link: _EntityLink) -> Row:
        """Resolve a stored link, rejecting a target type absent from the schema."""
        entity_id = cast('int', link['id'])
        try:
            row = self._store.row(cast('str', link['type']), entity_id)
        except ServerFaultError as exc:
            raise MockgunError(f'stored linked target is outside the schema: {link!r}') from exc
        if row is None:
            raise KeyError(entity_id)
        return row

    def _members(self, left: _Decision, right: _Decision) -> _Decision:
        """Evaluate every multi-link member, as the ordinary deep filter does."""
        return self._all(self._errors(left), self._choose(right, yes=True, no=left))

    def _name_relation_value(self, value: FieldValue) -> FieldValue:
        """Resolve every required name and reject unresolved comparison semantics."""
        if value is None:
            return None
        if isinstance(value, list):
            return [self._name_relation_value(member) for member in value]
        link = cast('_EntityLink', value)
        kind, entity_id = cast('str', link['type']), cast('int', link['id'])
        name = display_name(self._store, kind, entity_id)
        if name is None or derived_name(self._store, kind, entity_id):
            raise MockgunError(f'{kind} display-name comparison is not modeled')
        return {'type': kind, 'id': entity_id, 'name': name}

    def _matches_direct(
        self, entity_type: str, row: Row, path: str, relation: str, values: list[FieldValue]
    ) -> bool:
        """Reduce identity conditions before reading link visibility."""
        field_type = self._schema.field_type(entity_type, path)
        actual = self._direct_field_value(entity_type, row, path, membership=True)
        if (
            field_type in {'entity', 'multi_entity'}
            and relation == 'is'
            and values
            and values[0] is not None
        ):
            # A hidden nonmatching identity cannot change this condition.
            if not self._compare_value(field_type, actual, relation, values):
                return False
            if isinstance(actual, list):
                actual = [
                    member
                    for member in actual
                    if self._compare_value('entity', member, relation, values)
                ]
        if field_type in {'entity', 'multi_entity'}:
            actual = self._hidden_filter_value(row, path, actual, relation, values)
        if field_type == 'checkbox' and actual is None:
            raise MockgunError('stored checkbox is null')
        if field_type in {'entity', 'multi_entity'} and relation.startswith('name_'):
            actual = self._name_relation_value(actual)
        return self._compare_value(field_type, actual, relation, values)

    def _matches_field(
        self,
        entity_type: str,
        row: Row,
        path: str,
        relation: str,
        values: list[FieldValue],
    ) -> bool:
        """Traverse filter links, rejecting a wrong-type path only if null matches."""
        field_type = self._schema.field_type(entity_type, path)
        parts = path.split('.', 2)
        if len(parts) != _LINK_PATH_PARTS:
            return self._matches_direct(entity_type, row, path, relation, values)
        field, linked_type, rest = parts
        actual = self._direct_field_value(entity_type, row, field, membership=True)
        actual = self._hidden_filter_value(row, field, actual, relation, values)
        links = actual if isinstance(actual, list) else [actual]
        matches: list[bool] = []
        for link in links:
            if link is None:
                matches.append(self._compare_value(field_type, None, relation, values))
                continue
            if not isinstance(link, dict):
                raise ServerFaultError(
                    f'linked field path {entity_type}.{path} contains a non-entity value'
                )
            linked_row = self._linked_row(link)
            if link['type'] != linked_type:
                if self._compare_value(field_type, None, relation, values):
                    raise MockgunError('a different linked type matches as null')
                matches.append(False)
                continue
            matches.append(self._matches_field(linked_type, linked_row, rest, relation, values))
        return any(matches)

    def _matches_tree(
        self,
        entity_type: str,
        row: Row,
        tree: dict[str, object],
    ) -> bool:
        """Evaluate a compiled logical condition tree for one row."""
        conditions = cast('list[dict[str, object]]', tree['conditions'])
        for condition in conditions:
            if 'logical_operator' in condition:
                matches = self._matches_tree(entity_type, row, condition)
            else:
                field_name = cast('str', condition['path'])
                relation = cast('str', condition['relation'])
                values = cast('list[FieldValue]', condition['values'])
                field_type = self._schema.field_type(entity_type, field_name)
                try:
                    matches = self._matches_field(entity_type, row, field_name, relation, values)
                except (MockgunError, TypeError, AttributeError, KeyError) as exc:
                    raise MockgunError(
                        f'mockgun cannot evaluate {entity_type}.{field_name} '
                        f'{field_type} {relation} {values!r}: {exc}'
                    ) from exc
            if tree['logical_operator'] == 'and' and not matches:
                return False
            if tree['logical_operator'] == 'or' and matches:
                return True
        return tree['logical_operator'] == 'and'

    def _order_field(self, entity_type: str, path: str) -> tuple[str, str, str]:
        """Validate a complete order path and locate its terminal schema field."""
        if entity_type == 'Project' and path == 'last_accessed_by_current_user':
            raise MockgunError(
                'mockgun cannot read Project.last_accessed_by_current_user: '
                'filtering and ordering are unsettled'
            )
        parts = path.split('.', 2)
        field_name = parts[0]
        field_type = self._schema.field_type(entity_type, field_name)
        if len(parts) == 1:
            return entity_type, field_name, field_type
        if len(parts) != _LINK_PATH_PARTS or field_type != 'entity':
            raise MockgunError('order paths must traverse single-entity fields')
        return self._order_field(parts[1], parts[2])

    def _order_keys(
        self, values: list[FieldValue], field: tuple[str, str, str]
    ) -> list[FieldValue]:
        """Apply the terminal data type's key and uncertainty rules to a tie group."""
        entity_type, field_name, field_type = field
        if field_type not in {'multi_entity', 'url', 'image', 'text'} and all(
            value == values[0] for value in values
        ):
            return [0] * len(values)
        if field_type in {'multi_entity', 'url', 'image'}:
            occupied = [
                bool(value) if field_type == 'multi_entity' else value is not None
                for value in values
            ]
            if sum(occupied) > 1:
                raise MockgunError('multiple opaque values have unsettled order')
            return [0 if present else None for present in occupied]
        if field_type == 'checkbox' and None in values:
            raise MockgunError('null checkbox has unsettled order')
        if field_type == 'status_list':
            metadata = cast('dict[str, object]', self._schema.fields[entity_type][field_name])
            properties = cast('dict[str, object]', metadata.get('properties', {}))
            valid = cast('dict[str, object]', properties.get('valid_values', {})).get('value')
            if isinstance(valid, list):
                allowed = cast('list[FieldValue]', valid)
                if any(value is not None and value not in allowed for value in values):
                    raise MockgunError('status value is outside valid_values')
                return [None if value is None else allowed.index(value) for value in values]
        if field_type == 'entity':
            values = self._entity_order_keys(values)
        if field_type in {'text', 'entity'}:
            names = {value for value in values if isinstance(value, str)}
            check_collation(names, entity_type=entity_type, field_name=field_name)
            return [collation_key(value) if isinstance(value, str) else value for value in values]
        return values

    def _order_value(self, entity_type: str, row: Row, path: str) -> FieldValue:
        """Resolve an order value, rejecting hidden and invalid intermediate links."""
        parts = path.split('.', 2)
        field_name = parts[0]
        value = self._direct_field_value(entity_type, row, field_name)
        if self._visible_value(row, field_name, value) != value:
            raise MockgunError('hidden link has unmodeled order')
        if len(parts) == 1 or value is None:
            return value
        if not isinstance(value, dict) or value.get('type') != parts[1]:
            raise MockgunError('order path has a different linked type')
        return self._order_value(parts[1], self._linked_row(value), parts[2])

    def _order_group(
        self, rows: list[Row], entity_type: str, entry: dict[str, str]
    ) -> list[list[Row]]:
        """Refine one earlier-key tie group while keeping nulls and final IDs stable."""
        if len(rows) <= 1:
            return [rows]
        path = entry['field_name']
        field = self._order_field(entity_type, path)
        try:
            values = [self._order_value(entity_type, row, path) for row in rows]
            keys = self._order_keys(values, field)
            populated = [
                (key, row) for key, row in zip(keys, rows, strict=True) if key is not None
            ]
            nulls = [row for key, row in zip(keys, rows, strict=True) if key is None]
            populated.sort(
                key=lambda pair: cast('_ComparableFieldValue', pair[0]),
                reverse=entry['direction'] == 'desc',
            )
            groups = [
                [row for _, row in group]
                for _, group in groupby(populated, key=lambda pair: pair[0])
            ]
            if nulls:
                if field[2] == 'image' and entry['direction'] == 'asc':
                    groups.insert(0, nulls)
                else:
                    groups.append(nulls)
        except (MockgunError, TypeError, AttributeError, KeyError) as exc:
            raise MockgunError(
                f'mockgun cannot order {entity_type}.{path} {field[2]}: {exc}'
            ) from exc
        return groups

    def _ordinary_rows(
        self,
        entity_type: str,
        adapted: dict[str, object],
        presets: tuple[Preset, ...],
        scope: ReadScope,
    ) -> list[Row]:
        """Keep the established evaluator when no uncertain state is present."""
        source_rows = self._store.rows(entity_type, retired=scope.retired)
        latest = any(preset.name == 'LATEST' for preset in presets)
        if not latest:
            source_rows = [
                row for row in source_rows if self._in_archive_scope(entity_type, row, scope)
            ]
        rows = [row for row in source_rows if self._matches_tree(entity_type, row, adapted)]
        if not scope.include_archived_projects and latest:
            with assuming(None):
                for group in latest_groups(self._store, entity_type, rows):
                    if len({self._in_archive_scope(entity_type, row, scope) for row in group}) > 1:
                        raise MockgunError(
                            f'mockgun cannot select {entity_type} LATEST preset: '
                            'project entity group mixes archived and active Projects'
                        )
        rows = [row for row in rows if self._in_archive_scope(entity_type, row, scope)]
        with assuming(None):
            return apply_presets(self._store, entity_type, rows, presets, retired=scope.retired)

    def _preset_paths(
        self, row: Row, presets: tuple[Preset, ...], *, group_only: bool
    ) -> list[tuple[Row, str]]:
        """List guarded link reads in preset order, respecting membership exits."""
        if not presets:
            return []
        kind = cast('str', row['type'])
        paths = [(row, 'entity')]
        if kind != 'Cut' and row.get('entity') is None:
            return paths
        if kind == 'PublishedFile':
            paths.append((row, 'published_file_type'))
            if row.get('published_file_type') is None:
                return paths
            paths.append((row, 'task'))
        if not group_only and any(
            preset.latest_by == 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'
            for preset in presets
        ):
            paths.append((row, 'sg_task'))
            task = row.get('sg_task')
            if isinstance(task, dict):
                paths.append((self._linked_row(task), 'step'))
        return paths

    @staticmethod
    def _reads(value: _Decision) -> _StateReads:
        """Collect reachable reduced bits without generating any possible states."""
        reads: _StateReads = {}
        pending = [value]
        visited: set[_Fork] = set()
        while pending:
            item = pending.pop()
            if not isinstance(item, _Fork) or item in visited:
                continue
            visited.add(item)
            kind, entity_id, field, target, target_id = item.bit
            token = (field, target, target_id) if field else None
            reads.setdefault((kind, entity_id), set()).add(token)
            pending.extend((item.off, item.on))
        return reads

    def _select_assumed(
        self,
        entity_type: str,
        rows: list[Row],
        adapted: dict[str, object],
        presets: tuple[Preset, ...],
    ) -> list[Row]:
        """Evaluate one final reduced state, keeping the ordinary guard order."""
        scope = self._scope
        assume = self._assume or {}
        latest = any(preset.name == 'LATEST' for preset in presets)
        selected: list[Row] = []
        for row in rows:
            identity = (entity_type, cast('int', row['id']))
            state = assume.get(identity)
            if (state.retired if state is not None else bool(row['__retired'])) != scope.retired:
                continue
            if (latest or self._in_archive_scope(entity_type, row, scope)) and self._matches_tree(
                entity_type, row, adapted
            ):
                selected.append(row)
        if not scope.include_archived_projects and latest:
            with assuming(assume):
                for group in latest_groups(self._store, entity_type, selected):
                    if len({self._in_archive_scope(entity_type, row, scope) for row in group}) > 1:
                        raise MockgunError(
                            f'{entity_type} LATEST group mixes archived and active Projects'
                        )
        selected = [row for row in selected if self._in_archive_scope(entity_type, row, scope)]
        with assuming(assume):
            return apply_presets(
                self._store, entity_type, selected, presets, retired=scope.retired
            )

    def _state_counts(self, reads: _StateReads) -> dict[tuple[str, int], int]:
        """Count reduced states without materializing unread link combinations."""
        counts: dict[tuple[str, int], int] = {}
        for identity, tokens in reads.items():
            row = self._store.require(*identity)
            taint = cast('dict[str, FieldValue]', row.get('__taint', {}))
            varies = bool(taint) and not taint.get('active')
            if row['__retired'] and not varies:
                counts[identity] = 1
            else:
                counts[identity] = 2 ** len(tokens - {None}) + int(varies and None in tokens)
        return counts

    def _states(self, reads: _StateReads) -> dict[tuple[str, int], tuple[RecordState, ...]]:
        """Build reduced states from only the retirement and link bits read."""
        dimensions: dict[tuple[str, int], tuple[RecordState, ...]] = {}
        for identity, count in self._state_counts(reads).items():
            if count <= 1:
                continue
            tokens = reads[identity]
            links = [token for token in tokens if token is not None]
            states = [
                RecordState(
                    retired=False,
                    hidden=frozenset(
                        link for link, hide in zip(links, flags, strict=True) if hide
                    ),
                )
                for flags in product((False, True), repeat=len(links))
            ]
            row = self._store.require(*identity)
            taint = cast('dict[str, FieldValue]', row.get('__taint', {}))
            if None in tokens and taint and not taint.get('active'):
                states.append(RecordState(retired=True))
            dimensions[identity] = tuple(states)
        return dimensions

    def _combinations(
        self, reads: _StateReads, label: str
    ) -> list[dict[tuple[str, int], RecordState]]:
        """Check reduced cardinality before materializing any state combinations."""
        count = prod(self._state_counts(reads).values())
        if count > _STATE_LIMIT:
            raise MockgunError(f'{label}: {count} combinations exceeds limit of {_STATE_LIMIT}')
        dimensions = self._states(reads)
        return [
            dict(zip(dimensions, states, strict=True)) for states in product(*dimensions.values())
        ]

    def _symbolic_retired(self, row: Row) -> _Decision:
        """Represent only an unsettled retirement, keeping known scope constant."""
        taint = cast('dict[str, FieldValue]', row.get('__taint', {}))
        if taint and not taint.get('active'):
            return self._fork(
                (cast('str', row['type']), cast('int', row['id']), '', '', 0), off=False, on=True
            )
        return bool(row['__retired'])

    def _symbolic_hidden(self, row: Row, field: str, link: _EntityLink) -> _Decision:
        """Express hiding through the deciding record and the holder's scope."""
        deciding = deciding_record(self._store, row, field, link)
        if deciding is row:
            outcome = self._fork(
                (
                    cast('str', row['type']),
                    cast('int', row['id']),
                    field,
                    cast('str', link['type']),
                    cast('int', link['id']),
                ),
                off=False,
                on=True,
            )
        elif deciding is not None:
            outcome = self._symbolic_retired(deciding)
        else:
            active = {
                (cast('str', row['type']), cast('int', row['id'])): RecordState(retired=False)
            }
            outcome = hidden(self._store, row, field, link, assume=active)
        return self._choose(self._symbolic_retired(row), yes=False, no=outcome)

    def _symbolic_archive(self, row: Row, scope: ReadScope) -> _Decision:
        """Retain a project visibility bit only when its archive status matters."""
        if scope.include_archived_projects or not self._schema.declares('Project', 'archived'):
            return True
        kind = cast('str', row['type'])
        if kind == 'Project':
            return row.get('archived') is not True
        project = row.get('project')
        if not self._schema.declares(kind, 'project') or not isinstance(project, dict):
            return True
        if project.get('type') != 'Project' or not isinstance(project.get('id'), int):
            return True
        target = self._store.row('Project', cast('int', project['id']))
        if target is None or target.get('archived') is not True:
            return True
        return self._symbolic_hidden(row, 'project', project)

    def _symbolic_direct(self, row: Row, condition: dict[str, object]) -> _Decision:
        """Reduce a direct condition before consulting matching link visibility."""
        kind = cast('str', row['type'])
        path = cast('str', condition['path'])
        relation = cast('str', condition['relation'])
        values = cast('list[FieldValue]', condition['values'])
        field_type = self._schema.field_type(kind, path)
        actual = self._direct_field_value(kind, row, path, membership=True)
        if field_type not in {'entity', 'multi_entity'}:
            return self._matches_direct(kind, row, path, relation, values)
        links = actual if isinstance(actual, list) else [actual]
        result: _Decision = False
        hides: _Decision = False
        for link in links:
            if not isinstance(link, dict):
                continue
            visibility = self._symbolic_hidden(row, path, link)
            if relation == 'is' and values and values[0] is not None:
                if self._compare_value('entity', link, relation, values):
                    result = self._any(result, self._choose(visibility, yes=False, no=True))
            else:
                hides = self._any(hides, visibility)
        if relation == 'is' and values and values[0] is not None:
            return result
        if relation.startswith('name_'):
            actual = self._name_relation_value(actual)
        result = self._compare_value(field_type, actual, relation, values)
        return self._choose(
            hides,
            yes=_Failure(f'hidden {kind} {row["id"]}.{path} link has unmodeled {relation}'),
            no=result,
        )

    def _symbolic_field(self, row: Row, condition: dict[str, object]) -> _Decision:
        """Build one condition's graph, preserving terminal reads and failure leaves."""
        try:
            path = cast('str', condition['path'])
            if len(path.split('.', 2)) != _LINK_PATH_PARTS:
                return self._symbolic_direct(row, condition)
            field = path.split('.', 1)[0]
            actual = self._direct_field_value(
                cast('str', row['type']), row, field, membership=True
            )
            links = actual if isinstance(actual, list) else [actual]
            result: _Decision = False
            for link in links:
                result = self._members(result, self._symbolic_link(row, link, condition))
        except (MockgunError, ServerFaultError, TypeError, AttributeError, KeyError) as exc:
            return _Failure(str(exc))
        else:
            return result

    def _symbolic_link(
        self, row: Row, link: FieldValue, condition: dict[str, object]
    ) -> _Decision:
        """Resolve one deep member and conditionally retain errors from its target."""
        path = cast('str', condition['path'])
        field, linked_type, rest = path.split('.', 2)
        relation = cast('str', condition['relation'])
        values = cast('list[FieldValue]', condition['values'])
        field_type = self._schema.field_type(cast('str', row['type']), path)
        if link is None:
            return self._compare_value(field_type, None, relation, values)
        if not isinstance(link, dict):
            return _Failure(f'linked field path {row["type"]}.{path} contains a non-entity value')
        visibility = self._symbolic_hidden(row, field, link)
        try:
            linked = self._linked_row(link)
            if link['type'] != linked_type:
                outcome: _Decision = (
                    _Failure('a different linked type matches as null')
                    if self._compare_value(field_type, None, relation, values)
                    else False
                )
            else:
                outcome = self._symbolic_field(linked, {**condition, 'path': rest})
        except (MockgunError, ServerFaultError) as exc:
            outcome = _Failure(str(exc))
        hidden_outcome: _Decision = (
            False
            if relation == 'is' and values and values[0] is not None
            else _Failure(
                f'hidden {row["type"]} {row["id"]}.{field} link has unmodeled {relation}'
            )
        )
        return self._choose(visibility, yes=hidden_outcome, no=outcome)

    def _symbolic_paths(self, paths: list[tuple[Row, str]], outcome: _Decision) -> _Decision:
        """Retain possible hidden-link failures for every reached preset dependency."""
        for holder, field in reversed(paths):
            value = holder.get(field)
            if isinstance(value, dict):
                outcome = self._choose(
                    self._symbolic_hidden(holder, field, value),
                    yes=_Failure(f'{holder["type"]} {holder["id"]}.{field}: hidden preset link'),
                    no=outcome,
                )
        return outcome

    def _symbolic_tree(self, row: Row, tree: dict[str, object]) -> _Decision:
        """Apply condition order and short circuits before discovering dimensions."""
        conjunction = tree['logical_operator'] == 'and'
        result: _Decision = conjunction
        for condition in cast('list[dict[str, object]]', tree['conditions']):
            if result is (not conjunction):
                break
            outcome = (
                self._symbolic_tree(row, condition)
                if 'logical_operator' in condition
                else self._symbolic_field(row, condition)
            )
            result = self._all(result, outcome) if conjunction else self._any(result, outcome)
        return result

    @staticmethod
    def _validate_relation(
        entity_type: str,
        field_name: str,
        field_type: str,
        relation: str,
        values: Sequence[object],
    ) -> bool:
        """Validate vocabulary and known arity, returning evaluator availability."""
        arities: dict[str, dict[str, int | None]] = {
            'checkbox': {'is': 1, 'is_not': 1},
            'entity': {
                'in': None,
                'not_in': None,
                'is': 1,
                'is_not': 1,
                'name_is': 1,
                'name_contains': 1,
                'name_ends_with': 1,
                'name_not_contains': 1,
                'name_starts_with': 1,
                'type_is': 1,
                'type_is_not': 1,
            },
            'entity_type': {'is': 1},
            'list': {'in': None, 'is': 1, 'is_not': 1, 'not_in': None},
            'multi_entity': {
                'in': None,
                'is': 1,
                'is_not': 1,
                'name_contains': 1,
                'name_is': 1,
                'name_not_contains': 1,
                'not_in': None,
                'type_is': 1,
                'type_is_not': 1,
            },
            'status_list': {'in': None, 'is': 1, 'is_not': 1, 'not_in': None},
            'text': {
                'contains': 1,
                'ends_with': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'not_contains': 1,
                'not_in': None,
                'starts_with': 1,
            },
        }
        for numeric_type in _NUMERIC_TYPES:
            arities[numeric_type] = {
                'between': 2,
                'greater_than': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'less_than': 1,
                'not_in': None,
            }
        for null_type in ('currency', 'image', 'timecode'):
            arities[null_type] = {'is': 1, 'is_not': 1}
        if field_type in {'currency', 'timecode'} and relation == 'not_between':
            raise ServerFaultError(
                f'API read() {entity_type}.{field_name} {field_type} does not support not_between',
                error_code=103,
            )
        if field_type in _UNFILTERABLE_TYPES:
            raise ServerFaultError(
                f'API read() {entity_type}.{field_name} {field_type} '
                'data type cannot be used in a filter',
                error_code=103,
            )
        valid = _VALID_RELATIONS.get(field_type)
        if valid is not None and relation not in valid:
            raise ServerFaultError(
                f'API read() {entity_type}.{field_name} {field_type} '
                f'does not support {relation}. Valid relations: {", ".join(valid)}',
                error_code=103,
            )
        if field_type == 'color':
            return (
                entity_type == 'Task'
                and field_name == 'color'
                and relation == 'is'
                and list(values) == ['pipeline_step']
            )
        field_relations = arities.get(field_type, {})
        expected = field_relations.get(relation)
        if expected is not None and len(values) != expected:
            if field_type == 'multi_entity' and relation == 'is':
                raise ServerFaultError(
                    "API read() 'is' relation expects a single value for "
                    f'{entity_type}.{field_name}',
                    error_code=103,
                )
            raise ServerFaultError(f'{relation} requires exactly {expected} value(s)')
        return relation in field_relations

    def _validate_tree(self, entity_type: str, tree: dict[str, object]) -> None:
        """Validate all filter relations before evaluating any database row."""
        conditions = cast('list[dict[str, object]]', tree['conditions'])
        for condition in conditions:
            if 'logical_operator' in condition:
                self._validate_tree(entity_type, condition)
                continue
            field_name = cast('str', condition['path'])
            field_type = self._filter_field_type(entity_type, field_name)
            relation = cast('str', condition['relation'])
            if field_type == 'url':
                for row in self._store.rows(entity_type, retired=self._scope.retired):
                    self._guard_uploaded_file(row, field_name)
            if field_type in {'date', 'date_time'} and relation in _RELATIVE_RELATIONS:
                validate_relative(
                    f'{entity_type}.{field_name}',
                    field_type,
                    relation,
                    cast('list[FieldValue]', condition['values']),
                    context=self._scope.context,
                )
                continue
            implemented = self._validate_relation(
                entity_type,
                field_name,
                field_type,
                cast('str', condition['relation']),
                cast('list[FieldValue]', condition['values']),
            )
            _validate_operands(
                entity_type,
                field_name,
                field_type,
                cast('str', condition['relation']),
                cast('list[FieldValue]', condition['values']),
            )
            if not implemented:
                raise MockgunError(
                    f'mockgun does not implement {entity_type}.{field_name} '
                    f'{field_type} relation {condition["relation"]}'
                )
            if field_type == 'date_time':
                try:
                    condition['values'] = [
                        parse_wire_datetime(value) if isinstance(value, str) else value
                        for value in cast('list[object]', condition['values'])
                    ]
                except (ValueError, OverflowError) as exc:
                    raise MockgunError(
                        f'mockgun cannot parse {entity_type}.{field_name} '
                        f'{field_type} operand: {exc}'
                    ) from exc

    def _validate_selection(
        self, entity_type: str, tree: dict[str, object], scope: ReadScope
    ) -> tuple[dict[str, object], tuple[Preset, ...]]:
        """Validate filters and presets together before candidate selection."""
        preset_error: MockgunError | ServerFaultError | None = None
        try:
            presets = validate_presets(
                self._schema, entity_type, [] if scope.presets is None else scope.presets
            )
        except (MockgunError, ServerFaultError) as exc:
            preset_error = exc
            presets = ()
        try:
            adapted = _adapt_condition_tree(tree)
            self._validate_tree(entity_type, adapted)
        except (MockgunError, ServerFaultError) as exc:
            if preset_error is not None:
                raise MockgunError(
                    f'mockgun cannot select {entity_type}: invalid filters and presets'
                ) from exc
            raise
        if preset_error is not None:
            raise preset_error
        return adapted, presets

    def _visible_assumption(self) -> dict[tuple[str, int], RecordState]:
        """Supply one canonical visible state without generating state combinations."""
        return {
            (kind, cast('int', row['id'])): RecordState(retired=retired)
            for kind in self._schema.fields
            for retired in (False, True)
            for row in self._store.rows(kind, retired=retired)
        }

    def _dominance_safe(
        self, candidate: _Candidate, group: list[_Candidate], presets: tuple[Preset, ...]
    ) -> bool:
        """Prove group guards unchanged using their pairwise witnesses and certain rows."""
        if self._errors(candidate.selected) is not True:
            return False
        if any(
            self._archive_guard(candidate, other) is not True
            for other in group
            if other is not candidate
        ):
            return False
        certain = [other.row for other in group if other.selected is True]
        peers = [
            certain,
            *[
                [*certain, other.row]
                for other in group
                if other is not candidate
                and other.selected is not True
                and other.selected is not False
            ],
        ]
        kind = cast('str', candidate.row['type'])
        assume = self._visible_assumption()
        for rows in peers:
            errors: list[bool] = []
            for comparison in (rows, [*rows, candidate.row]):
                try:
                    with assuming(assume):
                        apply_presets(self._store, kind, comparison, presets, retired=False)
                except MockgunError:
                    errors.append(True)
                else:
                    errors.append(False)
            if errors[0] != errors[1]:
                return False
        return True

    def _dominance(
        self, entity_type: str, group: list[_Candidate], presets: tuple[Preset, ...]
    ) -> list[_Candidate]:
        """Drop strictly losing candidates before counting any state product."""
        mode = cast('str', next(preset for preset in presets if preset.name == 'LATEST').latest_by)
        certain = [candidate.row for candidate in group if candidate.selected is True]
        best = max(
            (latest_rank_key(self._store, entity_type, row, mode) for row in certain), default=None
        )
        retained: list[_Candidate] = []
        for candidate in group:
            if candidate.selected is not True and best is not None:
                try:
                    loses = latest_rank_key(self._store, entity_type, candidate.row, mode) < best
                except MockgunError:
                    loses = False
                if loses and self._dominance_safe(candidate, group, presets):
                    continue
            retained.append(candidate)
        return retained

    def _symbolic_external(self, entity_type: str, presets: tuple[Preset, ...]) -> _Decision:
        """Keep external preset reads independent of candidate and dominance rejection."""
        try:
            paths = preset_dependencies(self._store, presets)
            with assuming(self._visible_assumption()):
                apply_presets(
                    self._store,
                    entity_type,
                    [],
                    presets,
                    retired=self._scope.retired,
                )
        except (MockgunError, ServerFaultError) as exc:
            return _Failure(str(exc))
        return self._symbolic_paths(paths, outcome=True)

    def _symbolic_preset(
        self, row: Row, presets: tuple[Preset, ...], *, group_only: bool = False
    ) -> _Decision:
        """Keep preset failures conditional, without enumerating a candidate's states."""
        if not presets:
            return True
        kind = cast('str', row['type'])
        assume = self._visible_assumption()
        try:
            paths = self._preset_paths(row, presets, group_only=group_only)
            with assuming(assume):
                outcome: _Decision = bool(
                    latest_groups(self._store, kind, [row])
                    if group_only
                    else apply_presets(
                        self._store,
                        kind,
                        [row],
                        presets,
                        retired=self._scope.retired,
                    )
                )
        except (MockgunError, ServerFaultError) as exc:
            return _Failure(str(exc))
        return self._symbolic_paths(paths, outcome)

    def _candidate(
        self, row: Row, adapted: dict[str, object], presets: tuple[Preset, ...]
    ) -> _Candidate | None:
        """Reject known scope and constant candidacy before retaining any dimensions."""
        scope = self._scope
        admitted = self._choose(
            self._symbolic_retired(row), yes=scope.retired, no=not scope.retired
        )
        if admitted is False:
            return None
        latest = any(preset.name == 'LATEST' for preset in presets)
        archive = self._symbolic_archive(row, scope)
        if not latest:
            admitted = self._all(admitted, archive)
        if admitted is False:
            return None
        member = self._all(admitted, self._symbolic_tree(row, adapted))
        if member is False:
            return None
        if latest:
            member = self._all(member, self._symbolic_preset(row, presets, group_only=True))
        selected = self._all(member, archive)
        selected = self._all(selected, self._symbolic_preset(row, presets))
        if selected is False and (not latest or member is False):
            return None
        return _Candidate(row, member, archive, selected)

    def _uncertainty_error(self, reads: _StateReads, detail: str = '') -> MockgunError:
        """Name deciding identities, fields and taint sources for a state-dependent answer."""
        reasons: list[str] = []
        for identity, tokens in reads.items():
            row = self._store.require(*identity)
            reason = self._store.tainted(row)
            links = [token for token in tokens if token is not None]
            fields = ','.join(token[0] for token in links) or 'retirement'
            if reason is None:
                reason = ', '.join(
                    f'retired {kind} {entity_id} keep link' for _, kind, entity_id in links
                )
            reasons.append(f'{identity[0]} {identity[1]}.{fields}: {reason}')
        return MockgunError('; '.join([*reasons, detail] if detail else reasons))

    def _select_unit(
        self,
        entity_type: str,
        rows: list[Row],
        reads: _StateReads,
        adapted: dict[str, object],
        presets: tuple[Preset, ...],
    ) -> list[Row]:
        """Count only reduced dimensions, then require identical outcomes in every state."""
        for identity, tokens in self._reads(self._external).items():
            reads.setdefault(identity, set()).update(tokens)
        answer: list[Row] | None = None
        for combination in self._combinations(
            reads, f'{entity_type} group {[row["id"] for row in rows]}'
        ):
            self._assume = {**self._visible_assumption(), **combination}
            try:
                selected = self._select_assumed(entity_type, rows, adapted, presets)
            except MockgunError as exc:
                if reads:
                    raise self._uncertainty_error(reads, str(exc)) from exc
                raise
            if answer is not None and [row['id'] for row in selected] != [
                row['id'] for row in answer
            ]:
                raise self._uncertainty_error(reads)
            answer = selected
        return answer or []

    @staticmethod
    def _stored_groups(entity_type: str, candidates: list[_Candidate]) -> list[list[_Candidate]]:
        """Partition possible candidates by stored preset group keys."""
        groups: dict[tuple[object, ...], list[_Candidate]] = {}
        for candidate in candidates:
            row = candidate.row
            entity = row.get('entity')
            identity = (entity.get('type'), entity.get('id')) if isinstance(entity, dict) else None
            task = row.get('task')
            task_key = (task.get('type'), task.get('id')) if isinstance(task, dict) else None
            key = (
                (identity, task_key, row.get('name'))
                if entity_type == 'PublishedFile'
                else (row.get('code'), identity)
                if entity_type == 'Cut'
                else (identity,)
            )
            groups.setdefault(key, []).append(candidate)
        return list(groups.values())

    def _latest_units(
        self,
        entity_type: str,
        candidates: list[_Candidate],
        adapted: dict[str, object],
        presets: tuple[Preset, ...],
    ) -> list[Row]:
        """Apply all reductions before collecting each group's final state dimensions."""
        result: list[Row] = []
        for group in self._stored_groups(entity_type, candidates):
            retained = self._dominance(entity_type, group, presets)
            decisions = [candidate.selected for candidate in retained]
            decisions.extend(self._errors(candidate.member) for candidate in retained)
            decisions.extend(
                self._archive_guard(left, right) for left, right in combinations(retained, 2)
            )
            reads: _StateReads = {}
            for decision in decisions:
                for key, tokens in self._reads(decision).items():
                    reads.setdefault(key, set()).update(tokens)
            result.extend(
                self._select_unit(
                    entity_type, [candidate.row for candidate in retained], reads, adapted, presets
                )
            )
        return result

    def _preset_rows(
        self,
        entity_type: str,
        adapted: dict[str, object],
        presets: tuple[Preset, ...],
        scope: ReadScope,
    ) -> list[Row]:
        """Reduce symbolic candidate outcomes, then perform bounded concrete evaluation."""
        if not self._has_uncertainty():
            return self._ordinary_rows(entity_type, adapted, presets, scope)
        source_rows = self._store.rows(entity_type, retired=scope.retired)
        for row in self._store.tainted_rows(entity_type):
            if row not in source_rows:
                source_rows.append(row)
        try:
            self._external = self._symbolic_external(entity_type, presets)
            candidates = [
                candidate
                for row in source_rows
                if (candidate := self._candidate(row, adapted, presets)) is not None
            ]
            if not candidates:
                return self._select_unit(entity_type, [], {}, adapted, presets)
            if not any(preset.name == 'LATEST' for preset in presets):
                return [
                    selected
                    for candidate in candidates
                    for selected in self._select_unit(
                        entity_type,
                        [candidate.row],
                        self._reads(candidate.selected),
                        adapted,
                        presets,
                    )
                ]
            return self._latest_units(entity_type, candidates, adapted, presets)
        finally:
            self._assume = None

    def select(
        self,
        entity_type: str,
        tree: dict[str, object],
        order: list[dict[str, str]],
    ) -> list[Row]:
        """Validate and order candidates by clauses, then ascending record ID.

        Args:
            entity_type: Entity type to query.
            tree: Wire filter tree with `logical_operator` and `conditions`;
                leaf conditions contain `path`, `relation`, and `values`.
            order: Ordered mappings containing `field_name` and an `asc` or
                `desc` direction.

        Returns:
            Live stored rows in the requested order, with ascending ID as the
            final tie breaker.

        Raises:
            ServerFaultError: If selection has a modeled entity, filter, or
                preset fault.
            MockgunError: If selection, linked state, event state, or ordering
                is unsupported or unsettled.
        """
        if entity_type == 'EventLogEntry' and self._store.events_unknown is not None:
            raise MockgunError(f'EventLogEntry is unknown: {self._store.events_unknown}')
        adapted, presets = self._validate_selection(entity_type, tree, self._scope)
        clauses: list[tuple[dict[str, str], str]] = []
        for entry in order:
            path = entry['field_name']
            if entry['direction'] not in {'asc', 'desc'}:
                raise ValueError(f'unsupported ordering direction: {entry["direction"]}')
            if (
                '.' not in path
                and path not in {'id', 'type'}
                and not self._schema.declares(entity_type, path)
            ):
                continue
            try:
                field_type = self._order_field(entity_type, path)[2]
            except (MockgunError, ServerFaultError) as exc:
                raise MockgunError(f'mockgun cannot order {entity_type}.{path}: {exc}') from exc
            clauses.append((entry, field_type))
        rows = self._preset_rows(entity_type, adapted, presets, self._scope)
        rows.sort(key=lambda row: cast('int', row['id']))
        groups = [rows]
        for entry, field_type in clauses:
            if field_type == 'url':
                for row in rows:
                    self._guard_uploaded_file(row, entry['field_name'])
            if len(rows) > 1 and field_type not in _ORDER_TYPES:
                raise MockgunError(
                    f'mockgun cannot order {entity_type}.{entry["field_name"]} {field_type}'
                )
            groups = [
                part for group in groups for part in self._order_group(group, entity_type, entry)
            ]
        return [row for group in groups for row in group]


def _adapt_condition_tree(tree: dict[str, object]) -> dict[str, object]:
    """Copy a source-form condition tree into the evaluator representation."""
    operator = tree.get('logical_operator')
    if not isinstance(operator, str) or operator not in {'and', 'or'}:
        raise ServerFaultError('condition-tree logical_operator must be "and" or "or"')
    source_conditions = tree.get('conditions')
    if not isinstance(source_conditions, Sequence) or isinstance(
        source_conditions,
        (str, bytes, bytearray),
    ):
        raise ServerFaultError('condition-tree conditions must be a sequence')

    conditions: list[dict[str, object]] = []
    for source_condition in cast('Sequence[object]', source_conditions):
        if not isinstance(source_condition, dict):
            raise ServerFaultError('condition-tree nodes must be dictionaries')
        condition = cast('dict[str, object]', source_condition)
        if 'logical_operator' in condition or 'conditions' in condition:
            conditions.append(_adapt_condition_tree(condition))
            continue
        path = condition.get('path')
        if not isinstance(path, str):
            raise ServerFaultError('condition-tree leaf path must be a string')
        relation = condition.get('relation')
        if not isinstance(relation, str):
            raise ServerFaultError('condition-tree leaf relation must be a string')
        values = condition.get('values')
        if not isinstance(values, list):
            raise ServerFaultError('condition-tree leaf values must be a list')
        values = cast('list[object]', values)
        if path.startswith('$FROM$'):
            continue
        conditions.append(
            {
                'path': path,
                'relation': relation,
                'values': copy.deepcopy(values),
            }
        )
    return {'logical_operator': operator, 'conditions': conditions}


def _compare_entity_metadata(actual: _EntityLink | None, relation: str, expected: str) -> bool:
    """Compare nullable link types or case-insensitive modeled display names."""
    if actual is None:
        return relation in {'name_not_contains', 'type_is_not'}
    if relation == 'type_is':
        return actual['type'] == expected
    if relation == 'type_is_not':
        return actual['type'] != expected
    name = cast('str', actual['name']).lower()
    expected = expected.lower()
    if relation == 'name_is':
        return name == expected
    contains = expected in name
    return not contains if relation == 'name_not_contains' else contains


def _compare_equality(actual: FieldValue, relation: str, values: list[FieldValue]) -> bool:
    """Evaluate a validated checkbox or null-only scalar relation."""
    return actual == values[0] if relation == 'is' else actual != values[0]


def _compare_numeric(
    actual: _ComparableFieldValue | None,
    relation: str,
    values: list[_ComparableFieldValue | None],
) -> bool:
    """Evaluate a validated numeric, date, or datetime relation."""
    first = values[0] if values else None
    if relation == 'is':
        result = actual == first
    elif relation == 'is_not':
        result = actual != first
    elif relation == 'in':
        result = actual in values
    elif relation == 'not_in':
        result = actual not in values
    elif actual is None or first is None:
        result = False
    elif relation == 'less_than':
        result = actual < first
    elif relation == 'greater_than':
        result = actual > first
    else:
        second = values[1]
        if second is None:
            return False
        lower, upper = (second, first) if second < first else (first, second)
        result = lower <= actual <= upper
    return result


def _compare_selection(actual: str | None, relation: str, values: list[str | None]) -> bool:
    """Evaluate a validated list or status-list relation."""
    actual = actual.lower() if isinstance(actual, str) else actual
    values = [value.lower() if isinstance(value, str) else value for value in values]
    first = values[0] if values else None
    if relation == 'is':
        result = actual == first
    elif relation == 'is_not':
        result = actual != first
    elif relation == 'in':
        result = actual in values
    else:
        result = actual not in values
    return result


def _compare_text_fragment(actual: str | None, relation: str, expected: str | None) -> bool:
    """Evaluate a validated case-insensitive text fragment relation."""
    normalized_actual = (actual or '').lower()
    normalized_expected = (expected or '').lower()
    contains = normalized_expected in normalized_actual and (
        actual is not None or not isinstance(expected, str)
    )
    if relation == 'contains':
        result = contains
    elif relation == 'not_contains':
        result = not contains
    elif relation == 'starts_with':
        result = normalized_actual.startswith(normalized_expected)
    else:
        result = normalized_actual.endswith(normalized_expected)
    return result


def _compare_text_membership(
    actual: str | None,
    relation: str,
    values: list[str | list[str | None] | None],
) -> bool:
    """Evaluate a validated case-insensitive text membership relation."""
    normalized_actual = actual.lower() if actual is not None else None
    normalized_values: list[str | list[str | None] | None] = []
    for value in values:
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            items = cast('Sequence[str | None]', value)
            normalized_values.append(
                [item.lower() if item is not None else None for item in items]
            )
        else:
            normalized_values.append(value.lower() if value is not None else None)
    first = normalized_values[0] if normalized_values else None
    if first == '' and relation in {'is', 'is_not'}:
        first = None
    if relation == 'is':
        result = normalized_actual == first
    elif relation == 'is_not':
        result = normalized_actual != first
    elif relation == 'in':
        result = normalized_actual in normalized_values
    else:
        result = normalized_actual not in normalized_values
    return result


def _entity_identity(value: Mapping[str, FieldValue]) -> tuple[FieldValue, FieldValue]:
    """Return the source-observed identity pair for an entity value."""
    return value['type'], value['id']


def _compare_entity_identity(
    actual: _EntityLink | None,
    relation: str,
    values: list[_EntityLink | None],
) -> bool:
    """Evaluate validated entity identity relations."""
    first = values[0] if values else None
    if relation == 'is':
        if actual is None or first is None:
            result = actual == first
        else:
            result = _entity_identity(actual) == _entity_identity(first)
    elif relation == 'is_not':
        if actual is None or first is None:
            result = actual != first
        else:
            result = _entity_identity(actual) != _entity_identity(first)
    else:
        actual_identity = _entity_identity(actual) if actual is not None else None
        contains = any(
            actual_identity == (_entity_identity(value) if value is not None else None)
            for value in values
        )
        result = not contains if relation == 'not_in' else contains
    return result


def _compare_multi_entity(
    actual: list[_EntityLink] | None,
    relation: str,
    values: list[FieldValue],
) -> bool:
    """Match multi-entity members by identity or metadata, including complements."""
    if relation in {'is', 'is_not'} and values[0] is None:
        return not actual if relation == 'is' else bool(actual)
    positive = {
        'is_not': 'is',
        'not_in': 'in',
        'name_not_contains': 'name_contains',
        'type_is_not': 'type_is',
    }.get(relation, relation)
    if positive in {'is', 'in'}:
        matches = any(
            _compare_entity_identity(member, positive, cast('list[_EntityLink | None]', values))
            for member in actual or []
        )
    else:
        matches = any(
            _compare_entity_metadata(member, positive, cast('str', values[0]))
            for member in actual or []
        )
    return not matches if relation != positive else matches


def _compare_scalar(
    field_type: str,
    actual: FieldValue,
    relation: str,
    values: list[FieldValue],
) -> bool:
    """Dispatch one non-list field value to its type-specific comparator."""
    if field_type in {'checkbox', 'color', 'currency', 'image', 'timecode'}:
        result = _compare_equality(actual, relation, values)
    elif field_type in _NUMERIC_TYPES:
        result = _compare_numeric(
            cast('_ComparableFieldValue | None', actual),
            relation,
            cast('list[_ComparableFieldValue | None]', values),
        )
    elif field_type in {'list', 'status_list'}:
        result = _compare_selection(
            cast('str | None', actual),
            relation,
            cast('list[str | None]', values),
        )
    elif field_type == 'entity_type':
        result = actual == values[0]
    elif field_type == 'text':
        if relation in {'contains', 'ends_with', 'not_contains', 'starts_with'}:
            result = _compare_text_fragment(
                cast('str | None', actual),
                relation,
                cast('str | None', values[0]),
            )
        else:
            result = _compare_text_membership(
                cast('str | None', actual),
                relation,
                cast('list[str | list[str | None] | None]', values),
            )
    elif field_type == 'entity':
        if relation in {'in', 'is', 'is_not', 'not_in'}:
            result = _compare_entity_identity(
                cast('_EntityLink | None', actual),
                relation,
                cast('list[_EntityLink | None]', values),
            )
        else:
            result = _compare_entity_metadata(
                cast('_EntityLink', actual),
                relation,
                cast('str', values[0]),
            )
    elif field_type == 'multi_entity':
        result = _compare_multi_entity(
            cast('list[_EntityLink] | None', actual),
            relation,
            values,
        )
    else:
        raise ServerFaultError(f'relation {relation} is unsupported for field type {field_type}')
    return result


def _compare(
    field_type: str,
    actual: FieldValue,
    relation: str,
    values: list[FieldValue],
) -> bool:
    """Evaluate one source-supported field relation."""
    if isinstance(actual, list) and field_type != 'multi_entity':
        items = cast('list[FieldValue]', actual)
        return any(_compare(field_type, item, relation, values) for item in items)
    return _compare_scalar(field_type, actual, relation, values)


def _entity_operand_fault(value: FieldValue) -> str | None:
    """Describe the observed string-ID entity hash fault."""
    if isinstance(value, dict) and isinstance(value.get('id'), str):
        return (
            "API read() invalid/missing entity hash integer 'id':\n"
            f'{{"type" => "{value.get("type")}", "id" => "{value["id"]}"}}'
        )
    return None


def _format_datetimes(value: FieldValue) -> FieldValue:
    """Format datetime leaves while preserving nested link lists and null values."""
    if isinstance(value, datetime.datetime):
        return format_wire_datetime(value)
    if isinstance(value, list):
        return [_format_datetimes(member) for member in value]
    return value


def _operand_fault(field_type: str, relation: str, value: object) -> str | None:
    """Describe only operand errors settled by the scalar characterizations."""
    wrong_type = (
        (field_type == 'number' and type(value) in {float, str})
        or (field_type == 'float' and type(value) is int)
        or (field_type == 'text' and type(value) in {int, float})
        or (
            field_type == 'date'
            and isinstance(value, str)
            and bool(re.search(r'\d{2}:\d{2}', value))
        )
        or (
            field_type == 'date_time'
            and isinstance(value, str)
            and bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', value))
        )
    )
    if wrong_type:
        return f'{field_type} operand has the wrong type: {value!r}'
    if field_type == 'checkbox':
        prefix = 'expected [FalseClass, TrueClass] data type(s) but got '
        if isinstance(value, str):
            return f'{prefix}String:\n"{value}"'
        if value is None and relation == 'is':
            return f'{prefix}NilClass:\nnil'
    return None


def _ordered_note_links(
    row: Row, field: str, stored: FieldValue, visible: FieldValue, context: RequestContext | None
) -> FieldValue:
    """Return Asset before Shot for script reads of a script-authored Note's two links."""
    creator = row.get('created_by')
    if (
        row['type'] == 'Note'
        and context is not None
        and context.session_kind == 'script'
        and isinstance(creator, dict)
        and creator.get('type') == 'ApiUser'
        and field == 'note_links'
        and isinstance(stored, list)
        and len(stored) == len(_NOTE_LINK_PAIR)
        and {cast('str', cast('_EntityLink', member)['type']) for member in stored}
        == _NOTE_LINK_PAIR
        and isinstance(visible, list)
    ):
        return sorted(visible, key=lambda member: cast('str', cast('_EntityLink', member)['type']))
    return visible


def _validate_operands(
    entity_type: str,
    field_name: str,
    field_type: str,
    relation: str,
    values: list[FieldValue],
) -> None:
    """Validate settled scalar and entity operands independently of candidate rows."""
    qualified = f'{entity_type}.{field_name}'
    faults = [_operand_fault(field_type, relation, value) for value in values]
    entity_faults = (
        [_entity_operand_fault(value) for value in values]
        if (
            field_type in {'entity', 'multi_entity'}
            and relation in {'is', 'is_not', 'in', 'not_in'}
        )
        else []
    )
    unsettled = (
        field_type in {'duration', 'percent'}
        and any(value is not None and type(value) is not int for value in values)
    ) or (
        field_type in {'currency', 'image', 'timecode'}
        and any(value is not None for value in values)
    )
    has_null = (
        relation in {'in', 'not_in'}
        and relation in _VALID_RELATIONS.get(field_type, ())
        and None in values
    )
    if unsettled or (has_null and (any(faults) or any(entity_faults))):
        raise MockgunError(
            f'mockgun cannot evaluate {qualified} {field_type} {relation} {values!r}'
        )
    if has_null:
        raise ServerFaultError(
            f'API read() Read failed for entity type [{entity_type}]', error_code=104
        )
    for fault in entity_faults:
        if fault is not None:
            raise ServerFaultError(fault, error_code=103)
    for fault in faults:
        if fault is not None:
            raise ServerFaultError(f'API read() {qualified} {fault}', error_code=103)
    if not values and relation in {'in', 'not_in'}:
        if field_type == 'number':
            raise ServerFaultError(
                f'API read() {qualified} {relation} requires at least one value', error_code=103
            )
        if field_type in {'duration', 'entity', 'multi_entity', 'percent'} or (
            field_type in {'date', 'date_time', 'float'} and relation == 'not_in'
        ):
            raise MockgunError(
                f'mockgun cannot evaluate {qualified} {field_type} {relation} {values!r}'
            )


def collation_key(value: str) -> str:
    """Return the ASCII letter-and-digit key used by modeled text order."""
    return ''.join(
        character
        for character in value.translate(_ASCII_LOWER)
        if character in _COLLATION_CHARACTERS
    )


def check_collation(values: Collection[str], *, entity_type: str, field_name: str) -> None:
    """Reject unsettled comparisons within one preceding-key tie group.

    Args:
        values: Distinct non-null text values or names from one tie group only.
        entity_type: Entity owning the ordered field.
        field_name: Ordered field or path, used in the error message.

    Raises:
        MockgunError: If distinct ASCII-lowered values have equal keys, or an
            unobserved character decides their comparison. Equal lowered values
            tie.
    """
    lowered = {value.translate(_ASCII_LOWER) for value in values}
    if len({collation_key(value) for value in lowered}) != len(lowered):
        raise MockgunError(
            f'mockgun cannot order {entity_type}.{field_name}: equal collation keys'
        )
    stripped = [value.translate(_COLLATION_IGNORED) for value in lowered]
    if all(set(value) <= _COLLATION_CHARACTERS for value in stripped):
        return
    for left, right in combinations(stripped, 2):
        for first, second in zip(left, right, strict=False):
            if first != second:
                if first not in _COLLATION_CHARACTERS or second not in _COLLATION_CHARACTERS:
                    raise MockgunError(
                        f'mockgun cannot order {entity_type}.{field_name}: unsettled collation'
                    )
                break


def page(
    rows: list[Row], *, entities_per_page: int, current_page: int, max_page_size: int
) -> tuple[list[Row], dict[str, int]]:
    """Return the requested capped page and counts for all matching rows.

    Args:
        rows: All matching rows in query order.
        entities_per_page: Requested positive page width.
        current_page: One-based page number.
        max_page_size: Site's positive maximum page width.

    Returns:
        A new list retaining the selected live row references, followed by a
        paging dictionary containing `entity_count`, `current_page`,
        `entities_per_page`, and `page_count`. The effective width is the
        smaller of the requested width and site maximum.
    """
    width = min(entities_per_page, max_page_size)
    offset = (current_page - 1) * width
    count = len(rows)
    return rows[offset : offset + width], {
        'entity_count': count,
        'current_page': current_page,
        'entities_per_page': width,
        'page_count': (count + width - 1) // width,
    }


def project_rows(
    store: Store,
    schema: SiteSchema,
    rows: list[Row],
    return_fields: list[str],
    *,
    context: RequestContext | None = None,
) -> list[dict[str, object]]:
    """Return detached selected fields plus identity for each row in order.

    Args:
        store: Site records used to resolve projected links.
        schema: Field declarations for the requested projection.
        rows: Selected stored rows, in return order.
        return_fields: Fields to select alongside type and identity.
        context: Read actor for evidence bounded by session kind. When absent,
            preserve projection behavior outside observed read forms.

    Raises:
        ServerFaultError: If resolving a declared field or linked entity
            encounters a modeled schema fault.
        MockgunError: If projected values or linked identity state are unknown
            or unsupported.
        OverflowError: If converting a projected datetime to UTC exceeds the
            supported datetime range.
        ValueError: If the platform cannot convert a naive datetime.
        OSError: If local timezone conversion fails.
    """
    return _Projection(store, schema, context).project(rows, return_fields)


def select(
    store: Store,
    entity_type: str,
    tree: dict[str, object],
    order: list[dict[str, str]],
    *,
    scope: ReadScope,
) -> list[Row]:
    """Select matching rows with the current filter and stable ordering semantics.

    Args:
        store: Site records and schema to query.
        entity_type: Entity type to query.
        tree: Wire filter tree with `logical_operator` and `conditions`;
            leaf conditions contain `path`, `relation`, and `values`.
        order: Ordered mappings containing `field_name` and an `asc` or `desc`
            direction.
        scope: Retirement, archive, preset, and request-context selection
            scope.

    Returns:
        Live stored rows in the requested order, with ascending ID as the final
        tie breaker.

    Raises:
        ServerFaultError: If selection has a modeled entity, filter, or preset
            fault.
        MockgunError: If selection, linked state, event state, or ordering is
            unsupported or unsettled.
    """
    return _Query(store, store.schema, scope).select(entity_type, tree, order)
