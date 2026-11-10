"""Validate summary requests and calculate supported values over selected rows."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._links import derived_name, display_name, hidden
from ._query import ReadScope, check_collation, collation_key, project_rows, select
from ._schema import SiteSchema
from ._sessions import RequestContext
from ._store import FieldValue, Row, Store
from ._wire import parse_wire_datetime

_ASCII_LOWER = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
_BINARY_UNIT = Fraction(1, 2**53)
_BUCKET_SIZES = {
    'tens': 10,
    'hundreds': 100,
    'thousands': 1000,
    'tensofthousands': 10000,
    'hundredsofthousands': 100000,
    'millions': 1000000,
}
_CLUSTERED_ORDER = (
    'last_few_months',
    'last_week',
    'yesterday',
    'today',
    'tomorrow',
    'later_this_week',
    'next_week',
    'next_few_weeks',
    'next_few_months',
)
_DATE_GROUP_TYPES = ('exact', 'day', 'week', 'month', 'quarter', 'year', 'clustered_date')
_DECIMAL_PLACES = 4
_EXACT_INTEGER_LIMIT = 2**53
_FLOAT_GROUP_MAX = 1000
_FLOAT_GROUP_MIN = 0.0001
_GROUP_ENTITY_TYPES = {'Asset', 'Sequence', 'Shot'}
_GROUP_TYPES = {
    'checkbox': {'exact'},
    'date': set(_DATE_GROUP_TYPES),
    'date_time': {'exact', 'day', 'week', 'month', 'quarter', 'year'},
    'entity': {'exact', 'entitytype'},
    'float': {'exact'},
    'list': {'exact'},
    'multi_entity': {'exact'},
    'number': {
        'exact',
        'tens',
        'hundreds',
        'thousands',
        'tensofthousands',
        'hundredsofthousands',
        'millions',
    },
    'status_list': {'exact'},
    'text': {'exact', 'firstletter'},
}
_HALF = Fraction(1, 2)
_MAX_GROUP_LEVELS = 3
_MIN_DEEP_PATH_PARTS = 3
_MONTHS = (
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December',
)
_QUARTER_MONTHS = 3
_SIGNIFICANT_DIGITS = 15
_SUMMARY_TYPES = {
    'calculated': ('record_count', 'count', 'sum', 'minimum', 'maximum', 'average'),
    'checkbox': ('record_count', 'checked', 'unchecked'),
    'date': ('record_count', 'count', 'earliest', 'latest'),
    'date_time': ('record_count', 'count', 'earliest', 'latest'),
    'duration': ('record_count', 'count', 'sum', 'minimum', 'maximum', 'average'),
    'entity': ('record_count', 'count'),
    'float': ('record_count', 'count', 'sum', 'maximum', 'minimum', 'average'),
    'list': ('record_count', 'count', 'percentage'),
    'multi_entity': ('record_count', 'count'),
    'number': ('record_count', 'count', 'sum', 'maximum', 'minimum', 'average'),
    'percent': ('record_count', 'count', 'sum', 'maximum', 'minimum', 'average'),
    'status_list': (
        'record_count',
        'status_list',
        'status_percentage',
        'status_percentage_as_float',
    ),
    'text': ('record_count', 'count'),
    'url': ('record_count',),
    'pivot_column': (),
    'image': (),
    'summary': (),
    'serializable': (),
    'color': (),
}


@dataclass
class _Issues:
    """Collect independent request errors and server faults."""

    errors: list[str] = field(default_factory=list[str])
    faults: list[str] = field(default_factory=list[str])


@dataclass
class _Bucket:
    """Keep a group's identity, response values, sort key and candidate rows."""

    key: str | int | float | tuple[str, int] | None
    name: str | int
    value: object
    order: tuple[str, str, int | float] = ('', '', 0)
    rows: list[Row] = field(default_factory=list[Row])


@dataclass
class _Grouping:
    """Carry the read interfaces and summary entries through nested groups."""

    store: Store
    schema: SiteSchema
    entity_type: str
    scope: ReadScope
    summaries: list[dict[str, object]]


def _check_archive_targets(ctx: _Grouping, targets: set[tuple[str, int]], message: str) -> None:
    """Resolve linked target archive state through selection and projection only."""
    if ctx.scope.include_archived_projects or not ctx.schema.declares('Project', 'archived'):
        return
    for kind in sorted({kind for kind, _ in targets}):
        ids = {entity_id for target_type, entity_id in targets if target_type == kind}
        tree: dict[str, object] = {
            'logical_operator': 'and',
            'conditions': [{'path': 'id', 'relation': 'in', 'values': sorted(ids)}],
        }
        field_name = 'archived' if kind == 'Project' else 'project.Project.archived'
        fields = [field_name] if kind == 'Project' or ctx.schema.declares(kind, 'project') else []
        found: set[int] = set()
        for retired in (False, True):
            rows = select(
                ctx.store,
                kind,
                tree,
                [],
                scope=ReadScope(
                    retired=retired, context=ctx.scope.context, include_archived_projects=True
                ),
            )
            projected = project_rows(ctx.store, ctx.schema, rows, fields)
            found.update(cast('int', row['id']) for row in projected)
            if any(row.get(field_name) is True for row in projected):
                raise MockgunError(message)
        if found != ids:
            raise MockgunError(message)


def _check_group_names(ctx: _Grouping, path: str, names: list[str]) -> None:
    """Reject ambiguous names within a single preceding-key tie group."""
    check_collation(set(names), entity_type=ctx.entity_type, field_name=path)
    if len({name.translate(_ASCII_LOWER) for name in names}) != len(names):
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: equal names')


def _check_hidden_groups(ctx: _Grouping, rows: list[Row], path: str, message: str) -> None:
    """Use raw candidate links only to reject hidden group members."""
    for row in rows:
        value = row.get(path)
        links = value if isinstance(value, list) else [value]
        for link in links:
            if isinstance(link, dict) and hidden(ctx.store, row, path, link):
                raise MockgunError(message)


def _clustered_bucket(day: date, today: date, message: str) -> _Bucket:
    """Name only measured UTC date clusters with an unambiguous label."""
    start = today - timedelta(days=(today.weekday() + 1) % 7)
    day_start = day - timedelta(days=(day.weekday() + 1) % 7)
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    if day == yesterday and day_start == start - timedelta(days=7):
        raise MockgunError(message)
    if day == tomorrow and day_start == start + timedelta(days=7):
        raise MockgunError(message)
    if day == yesterday:
        name = 'yesterday'
    elif day == today:
        name = 'today'
    elif day == tomorrow:
        name = 'tomorrow'
    elif day_start == start - timedelta(days=7):
        name = 'last_week'
    elif day_start == start and day > tomorrow:
        name = 'later_this_week'
    elif day_start == start + timedelta(days=7):
        name = 'next_week'
    else:
        raise MockgunError(message)
    return _Bucket(name, name, name, (f'{_CLUSTERED_ORDER.index(name):02}', '', 0))


def _date_bucket(value: str, data_type: str, kind: str, message: str, *, today: date) -> _Bucket:
    """Format supported UTC dates using locale-independent English month names."""
    if kind == 'exact' and data_type == 'date_time':
        text = parse_wire_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
        return _Bucket(text, text, text, (text, '', 0))
    day = date.fromisoformat(value) if data_type == 'date' else parse_wire_datetime(value).date()
    if kind == 'clustered_date':
        return _clustered_bucket(day, today, message)
    if kind == 'week':
        day -= timedelta(days=(day.weekday() + 1) % 7)
        end = day + timedelta(days=6)
        name = (
            f'{_MONTHS[day.month - 1]} {day.day:02}, {day.year:04} to '
            f'{_MONTHS[end.month - 1]} {end.day:02}, {end.year:04}'
        )
    elif kind == 'exact':
        name = f'{_MONTHS[day.month - 1][:3]} {day.day:02} {day.year % 100:02}'
    elif kind == 'day':
        name = f'{_MONTHS[day.month - 1]} {day.day:02}, {day.year:04}'
    elif kind == 'month':
        day = day.replace(day=1)
        name = f'{_MONTHS[day.month - 1]} {day.year:04}'
    elif kind == 'quarter':
        quarter = (day.month - 1) // _QUARTER_MONTHS + 1
        day = day.replace(month=(quarter - 1) * _QUARTER_MONTHS + 1, day=1)
        name = f'Q{quarter} {day.year:04}'
    else:
        day = day.replace(month=1, day=1)
        name = f'{day.year:04}'
    text = day.isoformat()
    return _Bucket(text, name, text, (text, '', 0))


def _date_value(
    values: list[object], summary_type: str, data_type: str, message: str
) -> str | None:
    """Return supported date extrema in the summary wire format."""
    if not values:
        return None
    present = cast('list[str]', [value for value in values if value is not None])
    if not present or (data_type == 'date_time' and len(present) != len(values)):
        raise MockgunError(message)
    result = min(present) if summary_type == 'earliest' else max(present)
    if data_type == 'date_time':
        return parse_wire_datetime(result).strftime('%Y-%m-%d %H:%M:%S UTC')
    return result


def _declared_field_type(
    schema: SiteSchema, entity_type: str, path: str, issues: _Issues
) -> str | None:
    """Resolve a summary field while separating direct faults from unsettled paths."""
    parts = path.split('.')
    if len(parts) == 1:
        if path not in {'id', 'type'} and not schema.declares(entity_type, path):
            issues.faults.append(f"API summarize() {entity_type}.{path} doesn't exist")
            return None
        return schema.field_type(entity_type, path)
    if len(parts) < _MIN_DEEP_PATH_PARTS or len(parts) % 2 == 0:
        issues.errors.append(f'summarize field {path} has an unsupported path')
        return None
    current_type = entity_type
    invalid_reason = None
    for index in range(0, len(parts) - 1, 2):
        link, target = parts[index : index + 2]
        if not schema.declares(current_type, link):
            invalid_reason = f'undeclared link {link}'
        elif schema.field_type(current_type, link) != 'entity':
            invalid_reason = f'cannot traverse {link}'
        elif not schema.declares(target):
            invalid_reason = f'undeclared type {target}'
        if invalid_reason is not None:
            issues.errors.append(f'summarize field {path} has {invalid_reason}')
            return None
        current_type = target
    terminal = parts[-1]
    if terminal not in {'id', 'type'} and not schema.declares(current_type, terminal):
        issues.errors.append(f'summarize field {path} has undeclared terminal {terminal}')
        return None
    return schema.field_type(current_type, terminal)


def _field_type(schema: SiteSchema, entity_type: str, path: str, issues: _Issues) -> str | None:
    """Reject per-user access summaries before resolving declared field types."""
    if (entity_type == 'Project' and path == 'last_accessed_by_current_user') or path.endswith(
        '.Project.last_accessed_by_current_user'
    ):
        issues.errors.append(
            f'summarize Project.last_accessed_by_current_user via {path} is unsettled'
        )
        return None
    return _declared_field_type(schema, entity_type, path, issues)


def _group_properties(schema: SiteSchema, entity_type: str, path: str) -> dict[str, object]:
    """Read enum metadata for a direct or deep terminal field."""
    parts = path.split('.')
    terminal_type = parts[-2] if len(parts) > 1 else entity_type
    metadata = cast('dict[str, object]', schema.fields[terminal_type][parts[-1]])
    return cast('dict[str, object]', metadata.get('properties', {}))


def _check_enum_order(ctx: _Grouping, path: str, values: list[str], data_type: str) -> None:
    """Accept enum orders only when code, rank and display candidates agree."""
    _check_group_names(ctx, path, values)
    ordered = sorted(values)
    if sorted(values, key=collation_key) != ordered:
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: enum order')
    properties = _group_properties(ctx.schema, ctx.entity_type, path)
    valid = cast('dict[str, object]', properties.get('valid_values', {})).get('value')
    if isinstance(valid, list):
        valid = cast('list[str]', valid)
        if (
            any(value not in valid for value in values)
            or sorted(values, key=valid.index) != ordered
        ):
            raise MockgunError(f'mockgun cannot summarize grouping field {path}: enum rank')
    display = cast('dict[str, object]', properties.get('display_values', {})).get('value')
    if data_type != 'status_list' or not isinstance(display, dict):
        return
    display = cast('dict[str, str]', display)
    if any(value not in display for value in values):
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: enum display')
    _check_group_names(ctx, path, [display[value] for value in values])
    if (
        sorted(values, key=display.__getitem__) != ordered
        or sorted(values, key=lambda value: collation_key(display[value])) != ordered
    ):
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: enum display order')


def _number_bucket(value: float, kind: str, message: str) -> _Bucket:
    """Keep numeric ordering separate from the observed response string forms."""
    if kind in _BUCKET_SIZES:
        if value < 0:
            raise MockgunError(message)
        bucket = int(value) // _BUCKET_SIZES[kind] * _BUCKET_SIZES[kind]
        return _Bucket(bucket, bucket, bucket, ('', '', bucket))
    if isinstance(value, float) and (
        repr(value) == '-0.0'
        or (value != 0.0 and not _FLOAT_GROUP_MIN <= abs(value) < _FLOAT_GROUP_MAX)
    ):
        raise MockgunError(message)
    text = str(value)
    return _Bucket(value, text, text, ('', '', value))


def _raise_summary_fault(
    store: Store, entity_type: str, tree: dict[str, object], scope: ReadScope, message: str
) -> None:
    """Check filter faults before returning a single summary fault."""
    try:
        select(store, entity_type, tree, [], scope=scope)
    except ServerFaultError as error:
        raise MockgunError(f'mockgun cannot summarize: {message}; {error}') from error
    code = (
        None
        if message.startswith(
            ('Uncaught exception!', "API summarize() invalid/missing string 'direction'")
        )
        else 103
    )
    raise ServerFaultError(message, error_code=code)


def _round_fraction(value: Fraction, places: int, *, half_even: bool) -> Fraction:
    """Round exactly to a decimal place, with either observed tie convention."""
    unit = Fraction(10) ** -places
    scaled = abs(value) / unit
    whole, remainder = divmod(scaled.numerator, scaled.denominator)
    part = Fraction(remainder, scaled.denominator)
    if part > _HALF or (part == _HALF and (not half_even or whole % 2)):
        whole += 1
    return whole * unit if value >= 0 else -whole * unit


def _percentage_value(
    values: list[object], summary_type: str, selector: str, valid: object, message: str
) -> int:
    """Count exact enum matches with the supported denominator and rounding rules."""
    if isinstance(valid, list) and selector not in valid:
        raise MockgunError(message)
    for value in values:
        if value is None and summary_type != 'percentage':
            raise MockgunError(message)
        if isinstance(value, str) and value != selector and value.lower() == selector.lower():
            raise MockgunError(message)
    if not values:
        return -1 if summary_type == 'status_percentage_as_float' else 0
    percentage = Fraction(100 * values.count(selector), len(values))
    if summary_type == 'status_percentage_as_float':
        return percentage.numerator // percentage.denominator
    if summary_type == 'percentage' and percentage % 1 == _HALF:
        raise MockgunError(message)
    return int(_round_fraction(percentage, 0, half_even=False))


def _scalar_bucket(
    value: object, data_type: str, kind: str, message: str, *, today: date
) -> _Bucket:
    """Build a supported scalar bucket without inspecting any record storage."""
    if data_type == 'checkbox':
        if value is True:
            return _Bucket('t', '[x]', 't', ('t', '', 0))
        if value is not False:
            raise MockgunError(message)
        return _Bucket('f', '[ ]', 'f', ('f', '', 0))
    if value is None:
        return _Bucket(None, '', None)
    if data_type in {'number', 'float'}:
        return _number_bucket(cast('int | float', value), kind, message)
    text = cast('str', value)
    if data_type in {'date', 'date_time'}:
        return _date_bucket(text, data_type, kind, message, today=today)
    if kind == 'firstletter':
        if (
            not text
            or text[0] not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_'
        ):
            raise MockgunError(message)
        text = text[0].upper()
    key = text.translate(_ASCII_LOWER) if data_type == 'text' else text
    return _Bucket(key, text, text, (collation_key(text) if data_type == 'text' else text, '', 0))


def _significant_places(value: Fraction) -> int:
    """Find the decimal place for fifteen significant digits without floating arithmetic."""
    if value == 0:
        return _SIGNIFICANT_DIGITS - 1
    value = abs(value)
    exponent = len(str(value.numerator)) - len(str(value.denominator))
    if value < Fraction(10) ** exponent:
        exponent -= 1
    return _SIGNIFICANT_DIGITS - 1 - exponent


def _arithmetic_value(values: list[int | float], summary_type: str, message: str) -> float:
    """Accept an arithmetic result only when all exact uncertainty bounds agree."""
    decimal_sum = sum((Fraction(Decimal(repr(value))) for value in values), Fraction())
    binary_sum = sum((Fraction(value) for value in values), Fraction())
    absolute_sum = sum((abs(Fraction(value)) for value in values), Fraction())
    count = len(values)
    factor = (count - 1) * _BINARY_UNIT
    if factor >= 1:
        raise MockgunError(message)
    error = factor / (1 - factor) * absolute_sum
    if all(isinstance(value, int) for value in values) and absolute_sum <= _EXACT_INTEGER_LIMIT:
        error = Fraction()
    lower, upper = binary_sum - error, binary_sum + error
    if summary_type == 'average':
        candidate = decimal_sum / count
        lower /= count
        upper /= count
        lower -= abs(lower) * _BINARY_UNIT
        upper += abs(upper) * _BINARY_UNIT
        candidates = {
            _round_fraction(value, _DECIMAL_PLACES, half_even=half_even)
            for value in (candidate, lower, upper)
            for half_even in (False, True)
        }
    else:
        candidates = {decimal_sum}
        candidates.update(
            _round_fraction(value, places, half_even=half_even)
            for value in (lower, upper)
            for places in (_DECIMAL_PLACES, _significant_places(value))
            for half_even in (False, True)
        )
    if len(candidates) != 1:
        raise MockgunError(message)
    return float(candidates.pop())


def _numeric_value(
    values: list[object], summary_type: str, data_type: str, message: str
) -> int | float:
    """Calculate supported numeric summaries, distinguishing empty and all-null rows."""
    if data_type == 'percent' and (summary_type != 'average' or not values):
        raise MockgunError(message)
    present = cast('list[int | float]', [value for value in values if value is not None])
    if not present:
        if values:
            raise MockgunError(message)
        return 0.0 if data_type == 'float' or summary_type == 'average' else 0
    if summary_type == 'average' or (summary_type == 'sum' and data_type == 'float'):
        return _arithmetic_value(present, summary_type, message)
    if summary_type == 'sum':
        result = sum(present)
    elif summary_type == 'maximum':
        result = max(present)
    else:
        result = min(present)
    return float(result) if data_type == 'float' else int(result)


def _summary_value(
    schema: SiteSchema, entity_type: str, entry: dict[str, object], values: list[object]
) -> object:
    """Apply a validated summary to projected field values."""
    name = cast('str', entry['field'])
    summary_type = cast('str', entry['type'])
    data_type = schema.field_type(entity_type, name)
    message = f'mockgun cannot summarize summary type {summary_type!r} field {name} {data_type}'
    if summary_type == 'count':
        return sum(value is not None for value in values)
    if summary_type in {'sum', 'maximum', 'minimum', 'average'}:
        return _numeric_value(values, summary_type, data_type, message)
    if summary_type in {'earliest', 'latest'}:
        return _date_value(values, summary_type, data_type, message)
    if summary_type in {'checked', 'unchecked'}:
        if None in values:
            raise MockgunError(message)
        return values.count(summary_type == 'checked')
    if summary_type == 'status_list':
        if not values or values[0] is None or any(value != values[0] for value in values):
            raise MockgunError(message)
        return values[0]
    parts = name.split('.')
    terminal_type = parts[-2] if len(parts) > 1 else entity_type
    metadata = cast('dict[str, object]', schema.fields[terminal_type][parts[-1]])
    properties = cast('dict[str, object]', metadata.get('properties', {}))
    valid = cast('dict[str, object]', properties.get('valid_values', {})).get('value')
    return _percentage_value(values, summary_type, cast('str', entry['value']), valid, message)


def _summary_values(
    store: Store,
    schema: SiteSchema,
    entity_type: str,
    rows: list[Row],
    entries: list[dict[str, object]],
) -> dict[str, object]:
    """Project only value-reading summaries and preserve the request's result order."""
    fields = [cast('str', entry['field']) for entry in entries if entry['type'] != 'record_count']
    projected = project_rows(store, schema, rows, fields) if fields else []
    result: dict[str, object] = {}
    for entry in entries:
        name = cast('str', entry['field'])
        if entry['type'] == 'record_count':
            result[name] = len(rows)
            continue
        if any(name not in row for row in projected):
            raise MockgunError(
                f'mockgun cannot summarize summary type {entry["type"]!r} field '
                f'{name} {schema.field_type(entity_type, name)}: projection omits field'
            )
        result[name] = _summary_value(schema, entity_type, entry, [row[name] for row in projected])
    return result


def _type_display(schema: SiteSchema, entity_type: str) -> str | None:
    """Read a declared entity display name without supplying a fallback."""
    metadata = schema.entities.get(entity_type, {})
    name = cast('dict[str, object]', metadata.get('name', {})).get('value')
    return name if isinstance(name, str) else None


def _check_group_types(ctx: _Grouping, path: str, types: set[str]) -> None:
    """Require the observed type order to agree with display-name candidates."""
    if len(types) <= 1:
        return
    if not types <= _GROUP_ENTITY_TYPES:
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: mixed types')
    names = [_type_display(ctx.schema, kind) for kind in sorted(types)]
    if any(name is None for name in names):
        raise MockgunError(f'mockgun cannot summarize grouping field {path}: unnamed types')
    present = cast('list[str]', names)
    _check_group_names(ctx, path, present)
    for keys in (
        present,
        [name.translate(_ASCII_LOWER) for name in present],
        [collation_key(name) for name in present],
    ):
        if keys != sorted(set(keys)):
            raise MockgunError(f'mockgun cannot summarize grouping field {path}: type order')


def _entity_bucket(
    ctx: _Grouping, link: dict[str, FieldValue], kind: str, message: str
) -> _Bucket:
    """Build one target or type bucket from a projected entity identity."""
    target_type, target_id = cast('str', link['type']), cast('int', link['id'])
    if kind == 'entitytype':
        if _type_display(ctx.schema, target_type) != target_type:
            raise MockgunError(message)
        return _Bucket(target_type, target_type, target_type, (target_type, '', 0))
    name = display_name(ctx.store, target_type, target_id)
    if name is None or derived_name(ctx.store, target_type, target_id):
        raise MockgunError(message)
    value = {'type': target_type, 'id': target_id, 'name': name, 'valid': 'valid'}
    return _Bucket((target_type, target_id), name, value, (target_type, collation_key(name), 0))


def _group_order(ctx: _Grouping, option: dict[str, str], buckets: list[_Bucket]) -> list[_Bucket]:
    """Check only sibling groups and reverse non-null order for descending requests."""
    path = option['field']
    data_type = ctx.schema.field_type(ctx.entity_type, path)
    present = [bucket for bucket in buckets if bucket.key is not None]
    names = [cast('str', bucket.name) for bucket in present]
    if data_type == 'entity':
        types = {bucket.order[0] for bucket in present}
        _check_group_types(ctx, path, types)
        if option['type'] == 'exact':
            for target_type in types:
                _check_group_names(
                    ctx,
                    path,
                    [
                        cast('str', bucket.name)
                        for bucket in present
                        if bucket.order[0] == target_type
                    ],
                )
    elif data_type == 'multi_entity':
        if len({bucket.order[0] for bucket in present}) > 1:
            raise MockgunError(f'mockgun cannot summarize grouping field {path}: mixed members')
        _check_group_names(ctx, path, names)
    elif data_type == 'text':
        _check_group_names(ctx, path, names)
    elif data_type in {'list', 'status_list'}:
        _check_enum_order(ctx, path, names, data_type)
    return sorted(
        present, key=lambda bucket: bucket.order, reverse=option['direction'] == 'desc'
    ) + [bucket for bucket in buckets if bucket.key is None]


def _group_buckets(ctx: _Grouping, rows: list[Row], option: dict[str, str]) -> list[_Bucket]:
    """Partition candidate rows by projected scalar values or individual link members."""
    path, kind = option['field'], option['type']
    data_type = ctx.schema.field_type(ctx.entity_type, path)
    message = (
        f'mockgun cannot summarize grouping option {kind!r} field '
        f'{ctx.entity_type}.{path} {data_type}: bucket is unsettled'
    )
    is_link = data_type in {'entity', 'multi_entity'}
    if is_link:
        if '.' in path:
            raise MockgunError(message)
        _check_hidden_groups(ctx, rows, path, message)
    projected = project_rows(ctx.store, ctx.schema, rows, [path])
    buckets: dict[str | int | float | tuple[str, int] | None, _Bucket] = {}
    targets: set[tuple[str, int]] = set()
    for row, projection in zip(rows, projected, strict=True):
        value = projection.get(path)
        if path not in projection or (
            value is not None
            and ctx.scope.context.session_kind != 'script'
            and (data_type == 'date_time' or kind == 'clustered_date')
        ):
            raise MockgunError(message)
        values = (
            (cast('list[FieldValue]', value) or [None]) if data_type == 'multi_entity' else [value]
        )
        for member in values:
            if is_link and isinstance(member, dict):
                member = cast('dict[str, FieldValue]', member)
                targets.add((cast('str', member['type']), cast('int', member['id'])))
                bucket = _entity_bucket(ctx, member, kind, message)
            else:
                bucket = _scalar_bucket(
                    member, data_type, kind, message, today=ctx.scope.context.now.date()
                )
            if data_type == 'multi_entity':
                bucket.value = [] if member is None else [bucket.value]
            existing = buckets.setdefault(bucket.key, bucket)
            if existing.name != bucket.name:
                raise MockgunError(message)
            if not existing.rows or existing.rows[-1] is not row:
                existing.rows.append(row)
    _check_archive_targets(ctx, targets, message)
    return _group_order(ctx, option, list(buckets.values()))


def _group_values(
    ctx: _Grouping, rows: list[Row], options: list[dict[str, str]]
) -> list[dict[str, object]]:
    """Calculate each sibling's summaries and recurse only for non-leaf groups."""
    result: list[dict[str, object]] = []
    option = options[0]
    path = option['field']
    try:
        buckets = _group_buckets(ctx, rows, option)
    except MockgunError as error:
        raise MockgunError(
            f'mockgun cannot summarize grouping option {option["type"]!r} field '
            f'{path} {ctx.schema.field_type(ctx.entity_type, path)}: {error}'
        ) from error
    for bucket in buckets:
        group: dict[str, object] = {
            'group_name': bucket.name,
            'group_value': bucket.value,
            'summaries': _summary_values(
                ctx.store, ctx.schema, ctx.entity_type, bucket.rows, ctx.summaries
            ),
        }
        if len(options) > 1:
            group['groups'] = _group_values(ctx, bucket.rows, options[1:])
        result.append(group)
    return result


def _validate_summary_value(
    entry: dict[str, object],
    field: str,
    summary_type: str,
    data_type: str,
    issues: _Issues,
) -> None:
    """Check percentage selectors and reject unsupported value keys."""
    if (
        summary_type in {'status_percentage', 'status_percentage_as_float'}
        and data_type == 'status_list'
    ):
        if 'values' in entry:
            issues.faults.append(f'API summarize() unrecognized values key: {entry}')
        if ('value' not in entry and 'values' not in entry) or isinstance(
            entry.get('value'), list
        ):
            issues.faults.append(
                'API summarize() invalid/missing key value. Percentage-based summaries '
                f'require a valid value key in the summary: {entry}'
            )
        elif 'value' in entry and not isinstance(entry['value'], str):
            issues.errors.append(
                f'summarize summary type {summary_type!r} field {field} {data_type}'
            )
    elif summary_type == 'percentage' and data_type == 'list':
        if 'value' not in entry:
            issues.faults.append(
                'API summarize() invalid/missing key value. Percentage-based summaries '
                f'require a valid value key in the summary: {entry}'
            )
        elif not isinstance(entry['value'], str):
            issues.errors.append(f'summarize summary type percentage field {field} {data_type}')
        if 'values' in entry:
            issues.errors.append(f'summarize summary type percentage field {field} {data_type}')
    elif 'value' in entry or 'values' in entry:
        issues.errors.append(f'summarize summary type {summary_type!r} field {field} {data_type}')


def _vocabulary(names: tuple[str, ...]) -> str:
    """Render a server vocabulary in its observed order."""
    return '[' + ', '.join(f':{name}' for name in names) + ']'


def _validate_group_option(
    entity_type: str, field: str, grouping_type: str, data_type: str, issues: _Issues
) -> None:
    """Check a resolved grouping option against its observed vocabulary."""
    if data_type == 'date' and '.' in field and grouping_type == 'clustered_date':
        issues.errors.append(
            f'summarize grouping option {grouping_type!r} field {field} {data_type}'
        )
    elif data_type == 'date' and '.' not in field and grouping_type not in _DATE_GROUP_TYPES:
        issues.faults.append(
            f"API summarize() grouping option '{grouping_type}' not valid for field "
            f"{entity_type}.{field} with data type 'date'\n"
            f'Valid grouping types:\n{_vocabulary(_DATE_GROUP_TYPES)}'
        )
    elif grouping_type not in _GROUP_TYPES.get(data_type, set()):
        issues.errors.append(
            f'summarize grouping option {grouping_type!r} field {field} {data_type}'
        )


def _validate_grouping(
    schema: SiteSchema,
    entity_type: str,
    entries: list[object],
    issues: _Issues,
) -> None:
    """Validate grouping options before selecting rows."""
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            issues.errors.append(f'summarize grouping entry {entry!r} has invalid shape')
            continue
        entry = cast('dict[str, object]', entry)
        field = entry.get('field')
        grouping_type = entry.get('type')
        direction = entry.get('direction')
        if not isinstance(field, str) or entry.keys() - {'field', 'type', 'direction'}:
            issues.errors.append(f'summarize grouping entry {entry!r} has invalid shape')
            continue
        if field in seen:
            data_type = _field_type(schema, entity_type, field, issues)
            issues.errors.append(f'summarize grouping field {field} {data_type} repeats')
        seen.add(field)
        if 'type' not in entry:
            issues.faults.append(
                f"API summarize() invalid/missing string 'type' in grouping: {entry}"
            )
        elif not isinstance(grouping_type, str):
            issues.errors.append(
                f'summarize grouping field {field} has invalid type {grouping_type!r}'
            )
        if 'direction' not in entry or (
            isinstance(direction, str) and direction not in {'asc', 'desc'}
        ):
            issues.faults.append(
                f"API summarize() invalid/missing string 'direction' in grouping: {entry}. "
                'Valid direction values: asc, desc'
            )
        elif not isinstance(direction, str):
            issues.errors.append(
                f'summarize grouping field {field} has invalid direction {direction!r}'
            )
        data_type = _field_type(schema, entity_type, field, issues)
        if data_type is None or not isinstance(grouping_type, str):
            continue
        _validate_group_option(entity_type, field, grouping_type, data_type, issues)


def _validate_summary_type(
    entity_type: str,
    field: str,
    summary_type: str,
    data_type: str,
    issues: _Issues,
) -> None:
    """Check the type vocabulary and direct link count faults."""
    if data_type not in _SUMMARY_TYPES:
        issues.errors.append(f'summarize summary type {summary_type!r} field {field} {data_type}')
    elif summary_type not in _SUMMARY_TYPES[data_type]:
        issues.faults.append(
            f"API summarize() summary type '{summary_type}' not valid for field "
            f"{entity_type}.{field} with data type '{data_type}'\n"
            f'Valid summary types:\n{_vocabulary(_SUMMARY_TYPES[data_type])}'
        )
    elif summary_type == 'count' and data_type in {'entity', 'multi_entity'}:
        if '.' in field:
            issues.errors.append(f'summarize summary type count field {field} {data_type}')
        else:
            issues.faults.append(f'Uncaught exception! Summary type not supported: {field} count')
    if data_type == 'calculated' and summary_type != 'record_count':
        issues.errors.append(f'summarize summary type {summary_type!r} field {field} calculated')


def _validate_summaries(
    schema: SiteSchema,
    entity_type: str,
    entries: list[object],
    issues: _Issues,
) -> None:
    """Validate every summary entry before selection or value calculation."""
    seen: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            issues.errors.append(f'summarize summary entry {entry!r} has invalid shape')
            continue
        entry = cast('dict[str, object]', entry)
        field = entry.get('field')
        summary_type = entry.get('type')
        if (
            not isinstance(field, str)
            or not isinstance(summary_type, str)
            or entry.keys() - {'field', 'type', 'value', 'values'}
        ):
            issues.errors.append(f'summarize summary entry {entry!r} has invalid shape')
            continue
        data_type = _field_type(schema, entity_type, field, issues)
        if data_type is None:
            continue
        _validate_summary_type(entity_type, field, summary_type, data_type, issues)
        _validate_summary_value(entry, field, summary_type, data_type, issues)
        if field in seen:
            if seen[field] == summary_type:
                issues.errors.append(
                    f'summarize summary type {summary_type!r} field {field} {data_type}'
                )
            else:
                issues.faults.append(
                    f'API summarize() field {entity_type}.{field} has more than one summary type'
                )
        seen[field] = summary_type


def summarize(
    store: Store, schema: SiteSchema, params: Mapping[str, object], *, context: RequestContext
) -> dict[str, object]:
    """Calculate supported summaries and nested groups over selected rows.

    Args:
        store: Records to select and summarize.
        schema: Entity and field definitions used to validate the request.
        params: Wire entity type, filter tree, summary entries, and optional
            grouping and archived-project controls.
        context: Request identity and timestamp used during row selection.

    Returns:
        A mapping with field-keyed `summaries` and nested `groups`; `groups` is
        empty when no grouping was requested.

    Raises:
        MockgunError: If the request needs summary behavior not yet emulated.
        ServerFaultError: If supported validation or row selection produces a
            server fault.
    """
    if params.keys() - {'type', 'filters', 'summaries', 'grouping', 'include_archived_projects'}:
        raise MockgunError('mockgun cannot summarize unknown parameters')
    entity_type = params.get('type')
    tree = params.get('filters')
    entries = params.get('summaries')
    archived = params.get('include_archived_projects', True)
    grouping = params.get('grouping')
    if (
        not isinstance(entity_type, str)
        or not schema.declares(entity_type)
        or not isinstance(tree, dict)
        or not isinstance(entries, list)
        or not isinstance(archived, bool)
        or not entries
        or (
            'grouping' in params
            and (
                not isinstance(grouping, list)
                or not 1 <= len(cast('list[object]', grouping)) <= _MAX_GROUP_LEVELS
            )
        )
    ):
        raise MockgunError('mockgun cannot summarize this request yet')

    issues = _Issues()
    _validate_summaries(schema, entity_type, cast('list[object]', entries), issues)
    if isinstance(grouping, list):
        _validate_grouping(schema, entity_type, cast('list[object]', grouping), issues)
    if issues.errors:
        raise MockgunError(
            'mockgun cannot summarize: ' + '; '.join([*issues.errors, *issues.faults])
        )
    if len(issues.faults) > 1:
        raise MockgunError('mockgun cannot summarize: ' + '; '.join(issues.faults))
    scope = ReadScope(retired=False, context=context, include_archived_projects=archived)
    if issues.faults:
        _raise_summary_fault(
            store, entity_type, cast('dict[str, object]', tree), scope, issues.faults[0]
        )
    rows = select(store, entity_type, cast('dict[str, object]', tree), [], scope=scope)
    summary_entries = cast('list[dict[str, object]]', entries)
    ctx = _Grouping(store, schema, entity_type, scope, summary_entries)
    return {
        'summaries': _summary_values(
            store, schema, entity_type, rows, cast('list[dict[str, object]]', entries)
        ),
        'groups': _group_values(ctx, rows, cast('list[dict[str, str]]', grouping))
        if grouping is not None
        else [],
    }
