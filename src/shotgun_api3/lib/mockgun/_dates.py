"""Validate and evaluate settled relative-date windows in request time."""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._sessions import RequestContext
from ._store import FieldValue

_ROLLING_ARITY = 2
_UNITS = ('HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR')


def _shift(now: datetime, amount: int, unit: str) -> datetime:
    """Shift by elapsed time or by the same calendar day without clamping."""
    if unit == 'MONTH':
        year, month = divmod(now.year * 12 + now.month - 1 + amount, 12)
        return now.replace(year=year, month=month + 1)
    if unit == 'YEAR':
        return now.replace(year=now.year + amount)
    return now + timedelta(hours=amount * {'HOUR': 1, 'DAY': 24, 'WEEK': 168}[unit])


def _valid_operands(field_type: str, relation: str, values: Sequence[object], prefix: str) -> bool:
    """Distinguish characterized validation faults from unsettled operand forms."""
    calendar = relation.startswith('in_calendar_')
    if field_type == 'date_time' and relation == 'in_last':
        if len(values) != _ROLLING_ARITY:
            raise ServerFaultError(f'{prefix} expects [amount, unit]', error_code=103)
        amount, unit = values
        if type(amount) is not int:
            raise ServerFaultError(f'{prefix} requires an Integer amount', error_code=103)
        if amount < 0:
            raise ServerFaultError(f'{prefix} requires a positive Integer amount', error_code=103)
        if unit not in _UNITS:
            raise ServerFaultError(
                f'{prefix} unit {unit} is not supported. Valid units: {", ".join(_UNITS)}',
                error_code=103,
            )
        valid = amount > 0 or unit == 'DAY'
    elif field_type == 'date_time' and relation == 'in_calendar_day':
        if len(values) != 1 or type(values[0]) is not int:
            raise ServerFaultError(f'{prefix} requires an Integer offset', error_code=103)
        valid = True
    elif calendar:
        valid = len(values) == 1 and type(values[0]) is int
    else:
        valid = (
            len(values) == _ROLLING_ARITY
            and type(values[0]) is int
            and values[0] > 0
            and values[1] in _UNITS
        )
    return valid


def _window(
    field_type: str, relation: str, values: Sequence[object], now: datetime
) -> tuple[datetime, datetime]:
    """Build rolling or calendar bounds after operand validation."""
    amount = cast('int', values[0])
    if relation.startswith('in_calendar_'):
        unit = relation.removeprefix('in_calendar_').upper()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if unit == 'WEEK':
            start -= timedelta(days=(start.weekday() + 1) % 7)
        elif unit == 'MONTH':
            start = start.replace(day=1)
        elif unit == 'YEAR':
            start = start.replace(month=1, day=1)
        lower = amount if amount < 0 else (1 if amount > 0 else 0)
        upper = 0 if amount < 0 else amount + 1
        return _shift(start, lower, unit), _shift(start, upper, unit)
    unit = cast('str', values[1])
    if field_type == 'date':
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if unit == 'HOUR':
            amount //= 24
            unit = 'DAY'
    if relation.endswith('last'):
        return _shift(now, -amount, unit), now
    return now, _shift(now, amount, unit)


def in_window(
    field_type: str, relation: str, values: Sequence[object], value: FieldValue, *, now: datetime
) -> bool:
    """Match a validated relative filter, rejecting unsettled datetime endpoints.

    Args:
        field_type: Either `date` or `date_time`.
        relation: Validated rolling or calendar relation.
        values: Validated amount and unit, or calendar offset.
        value: Stored date string, datetime, or null.
        now: Aware UTC request timestamp.

    Returns:
        Whether the stored value matches the relation. A null value matches
        only `not_in_last` and `not_in_next`.

    Raises:
        MockgunError: A stored date cannot be parsed or a datetime equals a
            rolling endpoint.
    """
    negative = relation in {'not_in_last', 'not_in_next'}
    if value is None:
        return negative
    lower, upper = _window(field_type, relation, values, now)
    if field_type == 'date':
        try:
            actual_date = datetime.fromisoformat(cast('str', value)).date()
        except ValueError as exc:
            raise MockgunError(f'stored date cannot be parsed: {value!r}') from exc
        matches = lower.date() <= actual_date <= upper.date()
        if relation.startswith('in_calendar_'):
            matches = lower.date() <= actual_date < upper.date()
    else:
        actual = cast('datetime', value)
        if relation.startswith('in_calendar_'):
            matches = lower <= actual < upper
        else:
            if actual in (lower, upper):
                raise MockgunError('relative datetime endpoint is not settled')
            matches = lower < actual < upper
    return not matches if negative else matches


def validate_relative(
    path: str,
    field_type: str,
    relation: str,
    values: Sequence[object],
    *,
    context: RequestContext,
) -> None:
    """Validate relative operands and row-independent timezone and shift constraints.

    Args:
        path: Root entity type and filter path, as `Entity.field`, for
            qualified errors.
        field_type: Either `date` or `date_time`.
        relation: Supported relative relation.
        values: Amount and unit, or calendar offset.
        context: Request whose UTC time and session classification apply.

    Raises:
        ServerFaultError: A characterized invalid operand has fault code 103.
        MockgunError: Operand, timezone, or calendar shift behavior is
            unsettled.
    """
    prefix = f'API read() {path} {relation}'
    calendar = relation.startswith('in_calendar_')
    valid = _valid_operands(field_type, relation, values, prefix)
    prefix = f'mockgun cannot evaluate {path} {field_type} {relation}'
    if not valid:
        raise MockgunError(f'{prefix} operands are not settled: {values!r}')
    unknown_zone = context.session_kind != 'script'
    if unknown_zone and (calendar or field_type == 'date' or values[1] in {'MONTH', 'YEAR'}):
        raise MockgunError(f'{prefix} requires a known session timezone')
    try:
        _window(field_type, relation, values, context.now)
    except (ValueError, OverflowError) as exc:
        raise MockgunError(f'{prefix} calendar shift is not settled: {exc}') from exc
