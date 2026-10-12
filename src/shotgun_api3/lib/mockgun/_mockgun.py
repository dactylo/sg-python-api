"""In-memory implementation of the mockgun compatibility surface."""

import copy
import datetime
import pickle
import urllib.parse
from collections.abc import Callable, Collection, Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Never, Protocol, Self, TypeAlias, TypeVar, cast, overload

from shotgun_api3 import _crud, _protocol
from shotgun_api3._client import (
    CONSTRUCTOR_SIGNATURE,
    bind_constructor,
    expose_constructor_signature,
)
from shotgun_api3._config import parse_server_params
from shotgun_api3._errors import Fault, ShotgunError

_DocumentationFunction = TypeVar('_DocumentationFunction', bound=Callable[..., object])

_FieldValue: TypeAlias = (
    bool
    | int
    | float
    | str
    | datetime.date
    | datetime.datetime
    | dict[str, '_FieldValue']
    | list['_FieldValue']
    | None
)
_EntityLink: TypeAlias = dict[str, _FieldValue]
_Row: TypeAlias = dict[str, _FieldValue]
_Schema: TypeAlias = dict[str, _FieldValue]
_BatchResult: TypeAlias = bool | dict[str, object]

_DEFERRED_TRADITIONAL_FILTER_ERROR = 'deferred_traditional_filter_error'
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
_FIND_DOCSTRING = """Find entities matching the supplied in-memory filters.

Args:
    entity_type: Schema entity type to query.
    filters: Traditional filters or a logical condition tree.
    fields: Additional result fields; `id` and `type` are always included.
    order: Clauses containing `field_name` and `direction` (`asc` or `desc`).
        Sorts are stable, with the last clause taking precedence. Without
        clauses, results retain insertion order.
    filter_operator: `all` or `any` for traditional filters; `None` uses
        `all`. Ignored for a logical condition tree.
    limit: Accepted for client compatibility and ignored.
    retired_only: Whether to return retired rows instead of active
        rows.
    page: Accepted for client compatibility and ignored.
    include_archived_projects: Accepted for client compatibility and ignored.
    additional_filter_presets: Accepted for client compatibility and ignored.

Returns:
    Detached matching entities in the requested order.

Raises:
    ShotgunError: If the entity type, fields, or filters are invalid.
    ValueError: If an order clause lacks `field_name` or has an unsupported
        direction.
    KeyError: If an order clause lacks `direction`.
    TypeError: If arguments do not match the signature or ordered values
        cannot be compared.
"""
_FIND_ONE_DOCSTRING = """Return the first entity matching the supplied filters.

Args:
    entity_type: Schema entity type to query.
    filters: Traditional filters or a logical condition tree.
    fields: Additional result fields; `id` and `type` are always included.
    order: Clauses containing `field_name` and `direction` (`asc` or `desc`).
        Sorts are stable, with the last clause taking precedence. Without
        clauses, results retain insertion order.
    filter_operator: `all` or `any` for traditional filters; `None` uses
        `all`. Ignored for a logical condition tree.
    retired_only: Whether to return retired rows instead of active
        rows.
    include_archived_projects: Accepted for client compatibility and ignored.
    additional_filter_presets: Accepted for client compatibility and ignored.

Returns:
    The first detached matching entity, or `None` when none match.

Raises:
    ShotgunError: If the entity type, fields, or filters are invalid.
    ValueError: If an order clause lacks `field_name` or has an unsupported
        direction.
    KeyError: If an order clause lacks `direction`.
    TypeError: If arguments do not match the signature or ordered values
        cannot be compared.
"""
_MIN_DATETIME = datetime.datetime.fromisoformat('0001-01-01T00:00:00')
_MIN_TRADITIONAL_CONDITION_PARTS = 3
_TRADITIONAL_SEQUENCE_RELATIONS = frozenset({'between', 'in', 'not_between', 'not_in'})
_VALID_TRADITIONAL_FILTER_OPERATORS = frozenset({'all', 'any'})


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


class _MockConfig:
    """Expose the minimal mutable configuration used by mockgun clients."""

    def __init__(self, base_url: str) -> None:
        """Parse a site URL into mock configuration defaults."""
        self.scheme, _server, self.api_path = parse_server_params(base_url)
        parsed = urllib.parse.urlsplit(base_url)
        self.server = parsed.netloc
        self.records_per_page = 500


class _SchemaReader(Protocol):
    """Describe the schema export client surface."""

    def schema_entity_read(self) -> Mapping[str, object]:
        """Return entity schema metadata.

        Returns:
            Entity schema metadata keyed by entity type.
        """
        ...

    def schema_read(self) -> Mapping[str, object]:
        """Return field schema metadata.

        Returns:
            Field schema metadata keyed by entity type.
        """
        ...


class MockgunError(Exception):
    """Report mockgun setup and initialization failures."""


def _adapt_condition_tree(tree: dict[str, object]) -> dict[str, object]:
    """Copy a source-form condition tree into the evaluator representation."""
    operator = tree.get('logical_operator')
    if not isinstance(operator, str) or operator not in {'and', 'or'}:
        raise ShotgunError('condition-tree logical_operator must be "and" or "or"')
    source_conditions = tree.get('conditions')
    if not isinstance(source_conditions, Sequence) or isinstance(
        source_conditions,
        (str, bytes, bytearray),
    ):
        raise ShotgunError('condition-tree conditions must be a sequence')

    conditions: list[dict[str, object]] = []
    for source_condition in cast('Sequence[object]', source_conditions):
        if not isinstance(source_condition, dict):
            raise ShotgunError('condition-tree nodes must be dictionaries')
        condition = cast('dict[str, object]', source_condition)
        if 'logical_operator' in condition or 'conditions' in condition:
            conditions.append(_adapt_condition_tree(condition))
            continue
        path = condition.get('path')
        if not isinstance(path, str):
            raise ShotgunError('condition-tree leaf path must be a string')
        relation = condition.get('relation')
        if not isinstance(relation, str):
            raise ShotgunError('condition-tree leaf relation must be a string')
        values = condition.get('values')
        if not isinstance(values, list):
            raise ShotgunError('condition-tree leaf values must be a list')
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


def _adapt_traditional_filter_tree(
    filters: list[object] | tuple[object, ...] | dict[str, object],
    tree: dict[str, object],
) -> dict[str, object]:
    """Restore mockgun operand semantics on a shared compiled filter tree."""
    source_conditions = cast(
        'Sequence[object]',
        filters.get('filters', []) if isinstance(filters, dict) else filters,
    )
    compiled_conditions = cast('list[dict[str, object]]', tree['conditions'])
    for source_condition, compiled_condition in zip(
        source_conditions,
        compiled_conditions,
        strict=True,
    ):
        if isinstance(source_condition, dict):
            nested_filters = cast('dict[str, object]', source_condition)
            _adapt_traditional_filter_tree(nested_filters, compiled_condition)
            continue
        condition = cast('Sequence[object]', source_condition)
        if len(condition) < _MIN_TRADITIONAL_CONDITION_PARTS:
            raise ShotgunError('Filters must be in the form [lval, operator, rval]')
        if (
            len(condition) == _MIN_TRADITIONAL_CONDITION_PARTS
            and condition[1] not in _TRADITIONAL_SEQUENCE_RELATIONS
        ):
            compiled_condition['values'] = [copy.deepcopy(condition[2])]
    return tree


def _compare_checkbox(actual: bool | None, relation: str, values: list[_FieldValue]) -> bool:
    """Evaluate a validated checkbox relation."""
    return actual == values[0] if relation == 'is' else actual != values[0]


def _compare_entity_metadata(actual: _EntityLink, relation: str, expected: str) -> bool:
    """Evaluate validated entity type and display-name relations."""
    if relation == 'type_is':
        result = actual['type'] == expected
    elif relation == 'type_is_not':
        result = actual['type'] != expected
    elif relation == 'name_contains':
        result = expected in cast('str', actual['name'])
    elif relation == 'name_not_contains':
        result = expected not in cast('str', actual['name'])
    elif relation == 'name_starts_with':
        result = cast('str', actual['name']).startswith(expected)
    else:
        result = cast('str', actual['name']).endswith(expected)
    return result


def _compare_multi_entity(
    actual: list[_EntityLink] | None,
    relation: str,
    expected: _EntityLink | None,
) -> bool:
    """Evaluate validated multi-entity relations with source identity quirks."""
    if expected is None:
        result = not actual if relation == 'is' else bool(actual)
    else:
        contains_id = expected['id'] in (item['id'] for item in cast('list[_EntityLink]', actual))
        result = contains_id if relation == 'is' else not contains_id
    return result


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
    elif relation == 'less_than':
        result = cast('_ComparableFieldValue', actual) < cast('_ComparableFieldValue', first)
    elif relation == 'greater_than':
        result = cast('_ComparableFieldValue', actual) > cast('_ComparableFieldValue', first)
    elif relation == 'between':
        result = (
            cast('_ComparableFieldValue', values[0])
            <= cast('_ComparableFieldValue', actual)
            <= cast('_ComparableFieldValue', values[1])
        )
    elif relation == 'not_between':
        result = cast('_ComparableFieldValue', actual) < cast(
            '_ComparableFieldValue', values[0]
        ) or cast('_ComparableFieldValue', actual) > cast('_ComparableFieldValue', values[1])
    else:
        result = actual in values
    return result


def _compare_selection(actual: str | None, relation: str, values: list[str | None]) -> bool:
    """Evaluate a validated list or status-list relation."""
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
    if relation == 'contains':
        result = normalized_expected in normalized_actual
    elif relation == 'not_contains':
        result = normalized_expected not in normalized_actual
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
    if relation == 'is':
        result = normalized_actual == first
    elif relation == 'is_not':
        result = normalized_actual != first
    elif relation == 'in':
        result = normalized_actual in normalized_values
    else:
        result = normalized_actual not in normalized_values
    return result


def _deferred_traditional_filter_tree(message: str) -> dict[str, object]:
    """Build an evaluator tree that raises only when its node is reached."""
    return {
        'logical_operator': 'and',
        'conditions': [{_DEFERRED_TRADITIONAL_FILTER_ERROR: message}],
    }


def _deferred_traditional_group_error(filters: dict[object, object]) -> str | None:
    """Return a deferred error without invoking hooks on malformed group keys."""
    keys: list[str] = []
    has_filter_operator = False
    has_filters = False
    for key in filters:
        if type(key) is not str:
            return 'invalid traditional filter structure'
        keys.append(key)
        if key == 'filter_operator':
            has_filter_operator = True
        elif key == 'filters':
            has_filters = True
    if has_filter_operator and has_filters:
        return None
    found = ', '.join(keys)
    return f"Bad filter operator, requires keys 'filter_operator' and 'filters', found {found}"


def _deferred_traditional_operator_error(operator: object) -> str | None:
    """Return a deferred error without invoking hooks on an invalid operator."""
    if operator is None:
        return None
    if type(operator) is not str:
        return 'invalid traditional filter structure'
    if operator in _VALID_TRADITIONAL_FILTER_OPERATORS:
        return None
    return f'Unknown filter_operator type: {operator}'


def _deferred_traditional_tuple_is_short(value: object) -> bool:
    """Return whether a tuple condition lacks its right-hand operand."""
    return (
        isinstance(value, tuple)
        and len(cast('tuple[object, ...]', value)) < _MIN_TRADITIONAL_CONDITION_PARTS
    )


def _compile_deferred_traditional_condition(
    source_condition: object,
) -> list[dict[str, object]]:
    """Compile one condition or return its deferred structural error."""
    if _deferred_traditional_tuple_is_short(source_condition):
        return [{_DEFERRED_TRADITIONAL_FILTER_ERROR: 'invalid traditional filter structure'}]
    try:
        condition_tree = _adapt_traditional_filter_tree(
            [source_condition],
            _protocol.translate_filters(
                [source_condition],
                optimize=False,
            ),
        )
    except Fault:
        message = 'invalid traditional filter structure'
        if not isinstance(source_condition, (list, tuple, dict)):
            message = (
                'Filters can only be lists or dictionaries, not '
                f'{type(source_condition).__name__}.'
            )
        return [{_DEFERRED_TRADITIONAL_FILTER_ERROR: message}]
    return cast('list[dict[str, object]]', condition_tree['conditions'])


def _compile_deferred_traditional_filter_tree(
    filters: object,
    filter_operator: str | None,
) -> dict[str, object]:
    """Compile valid nodes while preserving malformed nodes for evaluation."""
    if isinstance(filters, dict):
        group_error = _deferred_traditional_group_error(cast('dict[object, object]', filters))
        if group_error is not None:
            return _deferred_traditional_filter_tree(group_error)
        typed_filters = cast('dict[str, object]', filters)
        source_conditions: object = typed_filters.get('filters', [])
        operator: object = typed_filters.get('filter_operator')
        shell: list[object] | dict[str, object] = {
            'filter_operator': operator,
            'filters': [],
        }
        shell_operator = None
    else:
        source_conditions = filters
        operator = filter_operator
        shell = []
        shell_operator = filter_operator
    operator_error = _deferred_traditional_operator_error(operator)
    if operator_error is not None:
        return _deferred_traditional_filter_tree(operator_error)
    try:
        tree = _protocol.translate_filters(shell, shell_operator, optimize=False)
    except Fault:
        return _deferred_traditional_filter_tree('invalid traditional filter structure')
    if not isinstance(source_conditions, (list, tuple)):
        return _deferred_traditional_filter_tree('invalid traditional filter structure')

    compiled_conditions = cast('list[dict[str, object]]', tree['conditions'])
    for source_condition in cast('list[object] | tuple[object, ...]', source_conditions):
        if isinstance(source_condition, dict):
            nested_filters = cast('dict[str, object]', source_condition)
            compiled_conditions.append(
                _compile_deferred_traditional_filter_tree(nested_filters, None)
            )
            continue
        compiled_conditions.extend(_compile_deferred_traditional_condition(source_condition))
    return tree


def _deferred_traditional_tree_requires_fallback(
    filters: object,
    filter_operator: object,
) -> bool:
    """Return whether a filter tree contains a node requiring deferment."""
    if isinstance(filters, dict):
        if _deferred_traditional_group_error(cast('dict[object, object]', filters)) is not None:
            return True
        typed_filters = cast('dict[str, object]', filters)
        operator = typed_filters.get('filter_operator')
        source_conditions: object = typed_filters.get('filters', [])
    else:
        operator = filter_operator
        source_conditions = filters
    if _deferred_traditional_operator_error(operator) is not None:
        return True
    if not isinstance(source_conditions, (list, tuple)):
        return False
    return any(
        _deferred_traditional_tuple_is_short(source_condition)
        or (
            isinstance(source_condition, dict)
            and _deferred_traditional_tree_requires_fallback(
                cast('dict[str, object]', source_condition),
                None,
            )
        )
        for source_condition in cast('list[object] | tuple[object, ...]', source_conditions)
    )


def _compile_traditional_filter_tree(
    filters: object,
    filter_operator: str | None,
) -> dict[str, object]:
    """Compile a traditional tree, deferring only structural compiler faults."""
    if not isinstance(filters, (list, tuple, dict)):
        return _compile_deferred_traditional_filter_tree(filters, filter_operator)
    filters = cast('list[object] | tuple[object, ...] | dict[str, object]', filters)
    if _deferred_traditional_tree_requires_fallback(filters, filter_operator):
        return _compile_deferred_traditional_filter_tree(filters, filter_operator)
    try:
        return _adapt_traditional_filter_tree(
            filters,
            _protocol.translate_filters(filters, filter_operator, optimize=False),
        )
    except Fault:
        return _compile_deferred_traditional_filter_tree(filters, filter_operator)


def _entity_identity(value: Mapping[str, _FieldValue]) -> tuple[_FieldValue, _FieldValue]:
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
        actual_identity = _entity_identity(cast('_EntityLink', actual))
        result = all(
            actual_identity == _entity_identity(cast('_EntityLink', value)) for value in values
        )
    return result


def _compare_scalar(
    field_type: str,
    actual: _FieldValue,
    relation: str,
    values: list[_FieldValue],
) -> bool:
    """Dispatch one non-list field value to its type-specific comparator."""
    if field_type == 'checkbox':
        result = _compare_checkbox(cast('bool | None', actual), relation, values)
    elif field_type in {'date', 'date_time', 'float', 'number'}:
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
        if relation in {'in', 'is', 'is_not'}:
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
            cast('_EntityLink | None', values[0]),
        )
    else:
        raise ShotgunError(f'relation {relation} is unsupported for field type {field_type}')
    return result


def _compare(
    field_type: str,
    actual: _FieldValue,
    relation: str,
    values: list[_FieldValue],
) -> bool:
    """Evaluate one source-supported field relation."""
    if isinstance(actual, list) and field_type != 'multi_entity':
        items = cast('list[_FieldValue]', actual)
        return any(_compare(field_type, item, relation, values) for item in items)
    return _compare_scalar(field_type, actual, relation, values)


def expose_find_documentation(function: _DocumentationFunction) -> _DocumentationFunction:
    """Attach mockgun's public `find` documentation.

    Args:
        function: Sync or async mockgun implementation.

    Returns:
        The same function with mockgun's runtime documentation attached.
    """
    object.__setattr__(function, '__doc__', _FIND_DOCSTRING)
    return function


def expose_find_one_documentation(function: _DocumentationFunction) -> _DocumentationFunction:
    """Attach mockgun's public `find_one` documentation.

    Args:
        function: Sync or async mockgun implementation.

    Returns:
        The same function with mockgun's runtime documentation attached.
    """
    object.__setattr__(function, '__doc__', _FIND_ONE_DOCSTRING)
    return function


class Shotgun:
    """Provide an in-memory stand-in for `shotgun_api3.Shotgun`.

    Each instance owns independent entity data and loaded schema objects.
    Operations do not contact the configured site. Schema read methods expose
    live schema values, so mutations to those values affect the instance.

    Attributes:
        base_url: Constructor-supplied site URL, preserved unchanged.
        config: Mutable mock configuration.
        finds: Number of find operations performed.
    """

    __signature__ = CONSTRUCTOR_SIGNATURE
    _schema_entity_path: ClassVar[str | None] = None
    _schema_path: ClassVar[str | None] = None

    @overload
    def __init__(self, base_url: Never) -> None: ...

    @overload
    def __init__(
        self,
        base_url: str,
        script_name: str | None = None,
        api_key: str | None = None,
        convert_datetimes_to_utc: bool = True,
        http_proxy: str | None = None,
        connect: bool = True,
        ca_certs: str | None = None,
        login: str | None = None,
        password: str | None = None,
        sudo_as_login: str | None = None,
        session_token: str | None = None,
        auth_token: str | None = None,
    ) -> None: ...

    @expose_constructor_signature
    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize an isolated client from the configured schema files.

        Args:
            *args: Positional values from the compatibility signature.
            **kwargs: Keyword values from the compatibility signature.

        Raises:
            MockgunError: If schema paths are unset or files are missing.
            TypeError: If arguments do not match the compatibility signature.
            ValueError: If `base_url` has an unsupported or invalid origin.
        """
        options = bind_constructor(args, kwargs, 'Mockgun Shotgun')
        self._initialize(options.base_url)

    def _entity_display_value(self, value: _EntityLink) -> _EntityLink:
        """Return a detached entity link enriched with its stored display name."""
        result = copy.deepcopy(value)
        linked_row = self._db[cast('str', result['type'])][cast('int', result['id'])]
        if 'name' in linked_row:
            result['name'] = linked_row['name']
        elif 'code' in linked_row:
            result['name'] = linked_row['code']
        return result

    @staticmethod
    def _expand_local_paths(data: dict[str, Any]) -> dict[str, Any]:
        """Copy data and add the file-link values synthesized by the service."""
        expanded = copy.deepcopy(data)
        for value in expanded.values():
            if not isinstance(value, dict) or 'local_path' not in value:
                continue
            path_value = cast('dict[str, Any]', value)
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

    def _field_value(
        self,
        entity_type: str,
        row: _Row,
        field_name: str,
    ) -> _FieldValue:
        """Resolve a direct or recursively linked field without mutating links."""
        try:
            link_field, linked_type, subfield = field_name.split('.', 2)
        except ValueError:
            return row.get(field_name)

        if link_field not in row:
            return None
        link_value = row[link_field]
        if isinstance(link_value, list):
            values: list[_FieldValue] = []
            for item in cast('list[_FieldValue]', link_value):
                if not isinstance(item, dict):
                    raise ShotgunError(
                        f'linked field path {entity_type}.{field_name} contains a non-entity value'
                    )
                link = cast('_EntityLink', item)
                if link['type'] != linked_type:
                    continue
                linked_row = self._db[cast('str', link['type'])][cast('int', link['id'])]
                values.append(self._field_value(linked_type, linked_row, subfield))
            return values
        if link_value is None:
            return None
        if not isinstance(link_value, dict):
            raise ShotgunError(
                f'linked field path {entity_type}.{field_name} contains a non-entity value'
            )
        if link_value['type'] != linked_type:
            raise ShotgunError(
                f'linked field path {entity_type}.{field_name} requests {linked_type}, '
                f'but the stored link is {link_value!r}'
            )
        linked_row = self._db[cast('str', link_value['type'])][cast('int', link_value['id'])]
        return self._field_value(linked_type, linked_row, subfield)

    @staticmethod
    def _normalize_field_value(field_type: str, value: _FieldValue) -> _FieldValue:
        """Detach a field value and reduce entity links to identity keys."""
        if value is None:
            return None
        if field_type == 'entity':
            link = cast('_EntityLink', value)
            return {'type': link['type'], 'id': link['id']}
        if field_type == 'multi_entity':
            links = cast('list[_EntityLink]', value)
            return [{'type': item['type'], 'id': item['id']} for item in links]
        return copy.deepcopy(value)

    @staticmethod
    def _require_dictionary(value: object, message: str) -> dict[str, Any]:
        """Return a dynamically checked dictionary or raise `ShotgunError`."""
        if not isinstance(value, dict):
            raise ShotgunError(message)
        return cast('dict[str, Any]', value)

    @staticmethod
    def _require_list(value: object, message: str) -> list[object]:
        """Return a dynamically checked list or raise `ShotgunError`."""
        if not isinstance(value, list):
            raise ShotgunError(message)
        return cast('list[object]', value)

    @staticmethod
    def _require_string_list(value: object, message: str) -> list[str]:
        """Return a list of strings or raise `ShotgunError`."""
        items = Shotgun._require_list(value, message)
        if any(not isinstance(item, str) for item in items):
            raise ShotgunError(message)
        return cast('list[str]', items)

    def _schema_field_type(self, entity_type: str, field_name: str) -> str:
        """Return the data type of a field already validated against the schema."""
        entity_schema = cast('_Schema', self._schema[entity_type])
        field_schema = cast('_Schema', entity_schema[field_name])
        data_type = cast('_Schema', field_schema['data_type'])
        return cast('str', data_type['value'])

    def _new_row(self, entity_type: str) -> _Row:
        """Build an active row populated with independent schema defaults."""
        row: _Row = {'__retired': False, 'type': entity_type}
        entity_schema = cast('_Schema', self._schema[entity_type])
        for field_name, field_schema_value in entity_schema.items():
            field_schema = cast('_Schema', field_schema_value)
            if self._schema_field_type(entity_type, field_name) == 'multi_entity':
                row[field_name] = []
            else:
                properties = cast('_Schema', field_schema['properties'])
                default_value = cast('_Schema', properties['default_value'])
                row[field_name] = copy.deepcopy(default_value['value'])
        return row

    @staticmethod
    def _update_multi_entity(
        current: list[_EntityLink],
        incoming: list[_EntityLink],
        mode: object,
    ) -> list[_EntityLink]:
        """Apply source-compatible multi-entity replacement modes."""
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
            return [
                copy.deepcopy(item)
                for item in current
                for removal in normalized
                if item['id'] != removal['id'] or item['type'] != removal['type']
            ]
        return copy.deepcopy(current)

    @staticmethod
    def _validate_direct_fields(data: dict[str, Any]) -> None:
        """Reject invalid names and fields controlled by mockgun."""
        if any(
            not isinstance(field, str) or not field for field in cast('Collection[object]', data)
        ):
            raise ShotgunError('field names must be nonempty strings')
        if 'id' in data or 'type' in data:
            raise ShotgunError("Can't set id or type on create or update")
        internal_fields = sorted(field for field in data if field.startswith('_'))
        if internal_fields:
            raise ShotgunError(f'fields cannot be set directly: {", ".join(internal_fields)}')

    @staticmethod
    def _validate_entity_link(entity_type: str, field_name: str, value: object) -> None:
        """Validate the structural requirements for a single link."""
        if not isinstance(value, dict):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type entity, '
                f'but data {value!r} is not a dictionary'
            )
        if 'type' not in value or 'id' not in value:
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type entity, '
                f"but data {value!r} does not contain 'type' and 'id'"
            )

    def _validate_entity_type(self, entity_type: str) -> None:
        """Reject entity types absent from the loaded schema."""
        if entity_type not in self._schema:
            raise ShotgunError(f'{entity_type} is not a valid entity')

    def _require_entity(self, entity_type: str, entity_id: int) -> _Row:
        """Return an existing row or report its missing identity."""
        self._validate_entity_type(entity_type)
        row = self._db[entity_type].get(entity_id)
        if row is None:
            raise ShotgunError(f'No entity of type {entity_type} exists with id {entity_id}')
        return row

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
                raise ShotgunError(f'{field_name} is not a valid field for entity {entity_type}')

    def _field_type(self, entity_type: str, field_name: str) -> str:
        """Return the schema type for a validated direct or linked field path."""
        try:
            link_field, linked_type, subfield = field_name.split('.', 2)
        except ValueError:
            self._validate_entity_fields(entity_type, [field_name])
        else:
            return self._validate_link_path(entity_type, link_field, linked_type, subfield)
        if field_name == 'id':
            return 'number'
        if field_name == 'type':
            return 'entity_type'
        return self._schema_field_type(entity_type, field_name)

    def _matches_tree(
        self,
        entity_type: str,
        row: _Row,
        tree: dict[str, object],
    ) -> bool:
        """Evaluate a compiled logical condition tree for one row."""
        conditions = cast('list[dict[str, object]]', tree['conditions'])
        for condition in conditions:
            if _DEFERRED_TRADITIONAL_FILTER_ERROR in condition:
                message = cast('str', condition[_DEFERRED_TRADITIONAL_FILTER_ERROR])
                raise ShotgunError(message) from None
            if 'logical_operator' in condition:
                matches = self._matches_tree(entity_type, row, condition)
            else:
                field_name = cast('str', condition['path'])
                relation = cast('str', condition['relation'])
                values = cast('list[_FieldValue]', condition['values'])
                field_type = self._field_type(entity_type, field_name)
                actual = self._field_value(entity_type, row, field_name)
                if field_type == 'entity' and actual is not None:
                    actual = self._entity_display_value(cast('_EntityLink', actual))
                matches = _compare(
                    field_type,
                    actual,
                    relation,
                    values,
                )
            if tree['logical_operator'] == 'and' and not matches:
                return False
            if tree['logical_operator'] == 'or' and matches:
                return True
        return tree['logical_operator'] == 'and'

    def _sort_value(
        self,
        entity_type: str,
        row: _Row,
        field_name: str,
    ) -> _FieldValue:
        """Return a source-compatible ordering value including `None` sentinels."""
        value = self._field_value(entity_type, row, field_name)
        if value is not None:
            return value
        field_type = self._field_type(entity_type, field_name)
        if field_type in {'duration', 'number', 'percent'}:
            return 0
        if field_type == 'float':
            return 0.0
        if field_type in {'date', 'entity_type', 'list', 'status_list', 'text'}:
            return ''
        if field_type == 'date_time':
            return _MIN_DATETIME
        return None

    def _validate_link_path(
        self,
        entity_type: str,
        link_field: str,
        linked_type: str,
        subfield: str,
    ) -> str:
        """Validate one linked path level and return its resolved leaf type."""
        self._validate_entity_fields(entity_type, [link_field])
        field_type = self._schema_field_type(entity_type, link_field)
        if field_type not in {'entity', 'multi_entity'}:
            raise ShotgunError(
                f'{link_field} is not a valid linked field for entity {entity_type}'
            )
        self._validate_entity_type(linked_type)
        return self._field_type(linked_type, subfield)

    @staticmethod
    def _validate_multi_entity_links(
        entity_type: str,
        field_name: str,
        value: object,
        valid_types: Collection[str],
    ) -> None:
        """Validate the structure and permitted types for a link list."""
        if not isinstance(value, list):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but data {value!r} is not a list'
            )
        items = cast('list[object]', value)
        if any(not isinstance(item, dict) for item in items):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but data {value!r} contains a non-dictionary'
            )
        links = cast('list[_EntityLink]', items)
        if any('type' not in item or 'id' not in item for item in links):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f"but an item in data {value!r} does not contain 'type' and 'id'"
            )
        if any(item['type'] not in valid_types for item in links):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of type multi_entity, '
                f'but an item in data {value!r} has an invalid type '
                f'(expected one of {valid_types!r})'
            )

    @staticmethod
    def _validate_relation(field_type: str, relation: str, values: Sequence[object]) -> None:
        """Validate a source-supported field/relation combination and its arity."""
        arities: dict[str, dict[str, int | None]] = {
            'checkbox': {'is': 1, 'is_not': 1},
            'date': {
                'between': 2,
                'greater_than': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'less_than': 1,
                'not_between': 2,
            },
            'date_time': {
                'between': 2,
                'greater_than': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'less_than': 1,
                'not_between': 2,
            },
            'entity': {
                'in': None,
                'is': 1,
                'is_not': 1,
                'name_contains': 1,
                'name_ends_with': 1,
                'name_not_contains': 1,
                'name_starts_with': 1,
                'type_is': 1,
                'type_is_not': 1,
            },
            'entity_type': {'is': 1},
            'float': {
                'between': 2,
                'greater_than': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'less_than': 1,
                'not_between': 2,
            },
            'list': {'in': None, 'is': 1, 'is_not': 1, 'not_in': None},
            'multi_entity': {'is': 1, 'is_not': 1},
            'number': {
                'between': 2,
                'greater_than': 1,
                'in': None,
                'is': 1,
                'is_not': 1,
                'less_than': 1,
                'not_between': 2,
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
        field_relations = arities.get(field_type, {})
        if relation not in field_relations:
            raise ShotgunError(f'relation {relation} is unsupported for field type {field_type}')
        expected = field_relations[relation]
        if expected is not None and len(values) != expected:
            raise ShotgunError(f'{relation} requires exactly {expected} value(s)')

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
            raise ShotgunError(
                f'Field {entity_type}.{field_name}: handling for schema type '
                f'{field_type} is not implemented'
            )
        if not isinstance(value, python_type):
            raise ShotgunError(
                f'{entity_type}.{field_name} is of schema type {field_type}, '
                f'but data {value!r} is not a {python_type.__name__}'
            )

    def _validate_entity_data(self, entity_type: str, data: dict[str, Any]) -> None:
        """Validate field values against the hand-loaded site schema."""
        self._validate_direct_fields(data)
        self._validate_entity_fields(entity_type, data.keys())
        for field_name, value in data.items():
            entity_schema = cast('_Schema', self._schema[entity_type])
            field_schema = cast('_Schema', entity_schema[field_name])
            field_type = self._schema_field_type(entity_type, field_name)
            if value is None and field_type != 'multi_entity':
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

    def _validate_tree(self, entity_type: str, tree: dict[str, object]) -> None:
        """Validate all filter relations before evaluating any database row."""
        conditions = cast('list[dict[str, object]]', tree['conditions'])
        for condition in conditions:
            if _DEFERRED_TRADITIONAL_FILTER_ERROR in condition:
                continue
            if 'logical_operator' in condition:
                self._validate_tree(entity_type, condition)
                continue
            field_type = self._field_type(entity_type, cast('str', condition['path']))
            self._validate_relation(
                field_type,
                cast('str', condition['relation']),
                cast('list[_FieldValue]', condition['values']),
            )

    def _validated_order(
        self,
        entity_type: str,
        order: list[dict[str, str]] | None,
    ) -> list[dict[str, str]]:
        """Validate order clauses before any row evaluation."""
        if order is None:
            return []
        entries = self._require_list(order, 'find order must be a list')
        validated: list[dict[str, str]] = []
        for entry in entries:
            if not isinstance(entry, dict) or 'field_name' not in entry:
                raise ValueError('order clauses require field_name and direction keys')
            typed_entry = cast('dict[str, str]', entry)
            field_name = typed_entry['field_name']
            direction = typed_entry['direction']
            self._field_type(entity_type, field_name)
            if direction not in {'asc', 'desc'}:
                raise ValueError(f'unsupported ordering direction: {direction}')
            validated.append(typed_entry)
        return validated

    def _find(self, options: _crud.FindOptions) -> list[dict[str, object]]:
        """Execute one bound in-memory find request."""
        self.finds += 1
        self._validate_entity_type(options.entity_type)
        fields = (
            []
            if options.fields is None
            else self._require_string_list(
                options.fields,
                'find fields must be a list of strings',
            )
        )
        for field_name in fields:
            self._field_type(options.entity_type, field_name)
        order = self._validated_order(options.entity_type, options.order)
        filters = options.filters
        if (
            isinstance(filters, dict)
            and all(type(key) is str for key in cast('dict[object, object]', filters))
            and any(
                key in {'conditions', 'logical_operator'}
                for key in cast('dict[str, object]', filters)
            )
        ):
            tree = _adapt_condition_tree(cast('dict[str, object]', filters))
        else:
            tree = _compile_traditional_filter_tree(options.filters, options.filter_operator)
        source_rows = [
            row
            for row in self._db[options.entity_type].values()
            if bool(row['__retired']) is options.retired_only
        ]
        self._validate_tree(options.entity_type, tree)
        rows = [row for row in source_rows if self._matches_tree(options.entity_type, row, tree)]
        for order_entry in order:
            order_field = order_entry['field_name']
            rows = sorted(
                rows,
                key=lambda row, field=order_field: cast(
                    '_ComparableFieldValue',
                    self._sort_value(
                        options.entity_type,
                        row,
                        field,
                    ),
                ),
                reverse=order_entry['direction'] == 'desc',
            )
        result: list[dict[str, object]] = []
        for row in rows:
            item: dict[str, object] = {
                field_name: copy.deepcopy(self._field_value(options.entity_type, row, field_name))
                for field_name in fields
            }
            item['type'] = row['type']
            item['id'] = row['id']
            result.append(item)
        return result

    def create(
        self,
        entity_type: str,
        data: dict[str, Any],
        return_fields: list[str] | None = None,
    ) -> dict[str, object]:
        """Create and return an entity in the in-memory database.

        Args:
            entity_type: Schema entity type to create.
            data: Field values for the new entity.
            return_fields: Fields to return instead of the fields in `data`.

        Returns:
            Created entity identity and the selected field values.

        Raises:
            ShotgunError: If the entity, fields, or values are invalid.
        """
        validated_data = self._require_dictionary(
            data,
            'create data must be a dictionary',
        )
        validated_return_fields = (
            None
            if return_fields is None
            else self._require_string_list(
                return_fields,
                'create return_fields must be a list of strings',
            )
        )

        created_data = self._expand_local_paths(validated_data)
        self._validate_entity_type(entity_type)
        self._validate_entity_data(entity_type, created_data)
        self._validate_entity_fields(entity_type, validated_return_fields)

        table = self._db[entity_type]
        entity_id = max(table, default=0) + 1
        row = self._new_row(entity_type)
        for field_name, value in created_data.items():
            field_type = self._schema_field_type(entity_type, field_name)
            row[field_name] = self._normalize_field_value(field_type, value)
        row['id'] = entity_id
        table[entity_id] = row

        fields = (
            validated_data.keys() if validated_return_fields is None else validated_return_fields
        )
        result: dict[str, object] = {
            field_name: copy.deepcopy(row[field_name]) for field_name in fields
        }
        result['type'] = entity_type
        result['id'] = entity_id
        return result

    def _batch_create(self, index: int, request: dict[str, Any]) -> dict[str, object]:
        """Validate and dispatch one batch create request."""
        if 'data' not in request:
            raise ShotgunError(f"batch create request {index} is missing 'data'")
        return self.create(
            request['entity_type'],
            request['data'],
            request.get('return_fields'),
        )

    def delete(self, entity_type: str, entity_id: int) -> bool:
        """Retire an active entity.

        Args:
            entity_type: Schema entity type to retire.
            entity_id: ID of the existing entity.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ShotgunError: If the entity type or ID is unknown.
        """
        row = self._require_entity(entity_type, entity_id)
        if row['__retired']:
            return False
        row['__retired'] = True
        return True

    def _batch_delete(self, index: int, request: dict[str, Any]) -> bool:
        """Validate and dispatch one batch delete request."""
        if 'entity_id' not in request:
            raise ShotgunError(f"batch delete request {index} is missing 'entity_id'")
        return self.delete(request['entity_type'], request['entity_id'])

    @overload
    def find(self, entity_type: Never) -> list[dict[str, object]]: ...

    @overload
    def find(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        limit: int = 0,
        retired_only: bool = False,
        page: int = 0,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> list[dict[str, object]]: ...

    @expose_find_documentation
    @_crud.expose_find_signature
    def find(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
        return self._find(_crud.bind_find(args, kwargs, 'Shotgun'))

    @overload
    def find_one(self, entity_type: Never) -> dict[str, object] | None: ...

    @overload
    def find_one(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        retired_only: bool = False,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> dict[str, object] | None: ...

    @expose_find_one_documentation
    @_crud.expose_find_one_signature
    def find_one(self, *args: object, **kwargs: object) -> dict[str, object] | None:
        options = _crud.bind_find_one(args, kwargs, 'Shotgun')
        results = self.find(
            options.entity_type,
            options.filters,
            fields=options.fields,
            order=options.order,
            filter_operator=options.filter_operator,
            limit=1,
            retired_only=options.retired_only,
            include_archived_projects=options.include_archived_projects,
            additional_filter_presets=options.additional_filter_presets,
        )
        return results[0] if results else None

    @classmethod
    def get_schema_paths(cls) -> tuple[str | None, str | None]:
        """Return the schema paths configured for this class.

        Returns:
            Field-schema and entity-schema paths, in that order.
        """
        return cls._schema_path, cls._schema_entity_path

    def _initialize(self, base_url: str) -> None:
        """Load schema state and seed the in-memory database."""
        schema_path, schema_entity_path = self.get_schema_paths()
        if schema_path is None or schema_entity_path is None:
            raise MockgunError(
                'mockgun schema paths are not configured; call Shotgun.set_schema_paths() first'
            )
        if not Path(schema_path).is_file():
            raise MockgunError(f'mockgun schema file does not exist: {schema_path}')
        if not Path(schema_entity_path).is_file():
            raise MockgunError(f'mockgun schema_entity file does not exist: {schema_entity_path}')

        with Path(schema_path).open('rb') as schema_file:
            self._schema = cast('_Schema', pickle.load(schema_file))  # noqa: S301
        with Path(schema_entity_path).open('rb') as schema_entity_file:
            self._schema_entity = cast(
                '_Schema',
                pickle.load(schema_entity_file),  # noqa: S301
            )

        self.base_url = base_url
        self.config = _MockConfig(base_url)
        self._db: dict[str, dict[int, _Row]] = {entity_type: {} for entity_type in self._schema}

        event = self._new_row('EventLogEntry')
        event['description'] = 'Mockgun was born. Yay.'
        event['event_type'] = 'Hello_Mockgun_World'
        event['id'] = 1
        self._db['EventLogEntry'][1] = event
        self.finds = 0

    def revive(self, entity_type: str, entity_id: int) -> bool:
        """Restore a retired entity to active state.

        Args:
            entity_type: Schema entity type to restore.
            entity_id: ID of the existing entity.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ShotgunError: If the entity type or ID is unknown.
        """
        row = self._require_entity(entity_type, entity_id)
        if not row['__retired']:
            return False
        row['__retired'] = False
        return True

    def schema_entity_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded entity schema.

        Args:
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live entity-schema dictionary. Mutations affect this client.
        """
        del project_entity
        return cast('dict[str, object]', self._schema_entity)

    def schema_field_read(
        self,
        entity_type: str,
        field_name: str | None = None,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded field schema for an entity or one field.

        Args:
            entity_type: Entity type whose fields to return.
            field_name: Optional field name to select.
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live entity field-schema dictionary when no field is selected.
            Otherwise, a new dictionary containing the selected field and its
            shared schema value, or an empty dictionary for an unknown field.
            Mutating shared schema values affects this client.

        Raises:
            KeyError: If `entity_type` is absent from the loaded schema.
        """
        del project_entity
        fields = cast('dict[str, object]', self._schema[entity_type])
        if field_name is None:
            return fields
        return {name: value for name, value in fields.items() if name == field_name}

    def schema_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded field schema.

        Args:
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live field-schema dictionary. Mutations affect this client.
        """
        del project_entity
        return cast('dict[str, object]', self._schema)

    @classmethod
    def set_schema_paths(cls, schema_path: str, schema_entity_path: str) -> None:
        """Configure the schema files loaded by future instances.

        Args:
            schema_path: Path to the field-schema pickle.
            schema_entity_path: Path to the entity-schema pickle.
        """
        cls._schema_path = schema_path
        cls._schema_entity_path = schema_entity_path

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, Any],
        multi_entity_update_modes: dict[str, str] | None = None,
    ) -> list[dict[str, object]]:
        """Update an entity and return the official one-item result list.

        Args:
            entity_type: Schema entity type to update.
            entity_id: ID of the existing entity.
            data: Replacement field values.
            multi_entity_update_modes: Per-field `set`, `add`, or `remove`
                modes.

        Returns:
            One updated entity containing identity and submitted fields.

        Raises:
            ShotgunError: If the entity, fields, or values are invalid.
        """
        validated_data = self._require_dictionary(
            data,
            'update data must be a dictionary',
        )
        validated_modes = (
            None
            if multi_entity_update_modes is None
            else self._require_dictionary(
                multi_entity_update_modes,
                'multi_entity_update_modes must be a dictionary',
            )
        )

        self._validate_entity_type(entity_type)
        self._validate_entity_data(entity_type, validated_data)
        row = self._require_entity(entity_type, entity_id)

        for field_name, value in validated_data.items():
            field_type = self._schema_field_type(entity_type, field_name)
            if field_type == 'multi_entity':
                mode = validated_modes.get(field_name, 'set') if validated_modes else 'set'
                row[field_name] = cast(
                    '_FieldValue',
                    self._update_multi_entity(
                        cast('list[_EntityLink]', row[field_name]),
                        cast('list[_EntityLink]', value),
                        mode,
                    ),
                )
            else:
                row[field_name] = self._normalize_field_value(field_type, value)

        result: dict[str, object] = {
            field_name: copy.deepcopy(row[field_name]) for field_name in validated_data
        }
        result['type'] = entity_type
        result['id'] = entity_id
        return [result]

    def _batch_update(self, index: int, request: dict[str, Any]) -> dict[str, object]:
        """Validate and dispatch one batch update request."""
        if 'entity_id' not in request:
            raise ShotgunError(f"batch update request {index} is missing 'entity_id'")
        if 'data' not in request:
            raise ShotgunError(f"batch update request {index} is missing 'data'")
        return self.update(
            request['entity_type'],
            request['entity_id'],
            request['data'],
            request.get('multi_entity_update_modes'),
        )[0]

    def _batch_request(self, index: int, request: object) -> _BatchResult:
        """Validate shared batch keys and dispatch one supported request."""
        typed_request = self._require_dictionary(
            request, f'batch request {index} must be a dictionary'
        )
        if 'request_type' not in typed_request:
            raise ShotgunError(f"batch request {index} is missing 'request_type'")
        if 'entity_type' not in typed_request:
            raise ShotgunError(f"batch request {index} is missing 'entity_type'")

        request_type = typed_request['request_type']
        if request_type == 'create':
            return self._batch_create(index, typed_request)
        if request_type == 'update':
            return self._batch_update(index, typed_request)
        if request_type == 'delete':
            return self._batch_delete(index, typed_request)
        raise ShotgunError(f'Invalid request type {request_type} in request {request!r}')

    def batch(self, requests: list[dict[str, Any]]) -> list[object]:
        """Execute supported requests in order and return their result values.

        Requests apply sequentially. If a later request fails, earlier
        mutations remain applied.

        Args:
            requests: Create, update, and delete request dictionaries.

        Returns:
            Per-request results in input order.

        Raises:
            ShotgunError: If a request or delegated operation is invalid.
        """
        validated_requests = self._require_list(
            requests,
            'batch requests must be a list',
        )

        results: list[object] = []
        for index, request in enumerate(validated_requests):
            results.append(self._batch_request(index, request))

        return results


def generate_schema(
    sg: _SchemaReader,
    schema_file_path: str,
    schema_entity_file_path: str,
) -> None:
    """Write the schema dictionaries needed to initialize mockgun.

    Args:
        sg: Client that supplies the schema dictionaries.
        schema_file_path: Destination for the field schema.
        schema_entity_file_path: Destination for the entity schema.
    """
    with Path(schema_file_path).open('wb') as schema_file:
        pickle.dump(sg.schema_read(), schema_file, protocol=2)
    with Path(schema_entity_file_path).open('wb') as schema_entity_file:
        pickle.dump(sg.schema_entity_read(), schema_entity_file, protocol=2)
