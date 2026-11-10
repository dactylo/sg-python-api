"""Emulate explicitly configured preferences, schedules, subscriptions and navigation."""

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, cast

from ._activity import taint_activity
from ._errors import MockgunError, ServerFaultError
from ._links import display_name
from ._sessions import RequestContext
from ._store import Row, Store

if TYPE_CHECKING:
    from ._server import FakeServer

_RECORDED_PREFERENCES = (
    'format_date_fields',
    'date_component_order',
    'format_time_hour_fields',
    'format_currency_fields_display_dollar_sign',
    'format_currency_fields_decimal_options',
    'format_currency_fields_negative_options',
    'format_number_fields',
    'format_float_fields',
    'format_float_fields_rounding',
    'format_footage_fields',
    'support_local_storage',
    'view_master_settings',
    'duration_units',
    'hours_per_day',
    'enable_rv_integration',
    'enable_shotgun_review_for_rv',
    'creative_review_settings',
)

_CANNED_METHODS = {'nav_expand', 'nav_search_string', 'nav_search_entity', 'export_page'}
_RULE_FIELDS = ('start_date', 'end_date', 'working', 'description', 'project', 'user')


class SiteData:
    """Explicit site settings, independent of record transaction rollback.

    Attributes:
        preferences: Configured values; absent recorded preferences remain
            unknown.
        subscriptions: HumanUser IDs and string subscriptions in insertion
            order.
        work_week: Seven working flags from Monday to Sunday.
    """

    def __init__(self) -> None:
        """Start with no configured values and the observed weekday work week."""
        self.preferences: dict[str, object] = {}
        self.subscriptions: dict[int, str] = {}
        self.work_week = [True] * 5 + [False] * 2
        self._canned: list[_Canned] = []

    def canned(self, method: str, params: dict[str, object]) -> object:
        """Return a detached configured answer for exactly matching RPC parameters.

        Args:
            method: Wire RPC name, including the shared `nav_search` name.
            params: Validated wire parameters.

        Raises:
            MockgunError: If no configured entry matches.
            ServerFaultError: If the matching entry specifies a fault.
        """
        for entry in self._canned:
            if entry.method == method and entry.params == params:
                if entry.fault is not None:
                    raise entry.fault
                return deepcopy(entry.response)
        raise MockgunError(f'mockgun cannot {method} {params!r}: no canned response is configured')

    def set_canned(
        self,
        method: str,
        params: Mapping[str, object],
        response: object,
        fault: ServerFaultError | None = None,
    ) -> None:
        """Replace a canned entry after detaching its exact wire parameters.

        Args:
            method: One of the four supported public navigation/export methods.
            params: Exact RPC parameters, compared without regard to key order.
            response: JSON response value, unused for a fault.
            fault: Optional wire fault in place of a response.

        Raises:
            ValueError: If the public method is unsupported.
            TypeError: If parameters or response are not JSON-serializable.
        """
        if method not in _CANNED_METHODS:
            raise ValueError(f'unsupported canned method {method!r}')
        rpc = 'nav_search' if method.startswith('nav_search_') else method
        entry = _Canned(rpc, json_copy(dict(params)), json_copy(response), fault)
        self._canned = [
            old for old in self._canned if (old.method, old.params) != (rpc, entry.params)
        ]
        self._canned.append(entry)


@dataclass
class _Canned:
    """A detached response or fault matched by method and parameter equality."""

    method: str
    params: object
    response: object
    fault: ServerFaultError | None


def _active(store: Store, rpc: str, kind: str, identity: int) -> Row:
    """Require an active declared target for a site operation."""
    row = store.row(kind, identity) if store.schema.declares(kind) else None
    if row is None or row['__retired']:
        raise MockgunError(f'mockgun cannot {rpc} {kind} {identity}: missing or retired record')
    return row


def _params(rpc: str, value: object, required: set[str], optional: set[str]) -> dict[str, object]:
    """Require an exact parameter dictionary for a site RPC."""
    if not isinstance(value, dict):
        raise MockgunError(f'mockgun cannot {rpc} params: expected a dictionary')
    params = cast('dict[str, object]', value)
    missing = required - params.keys()
    extra = params.keys() - required - optional
    if missing or extra:
        key = min(missing or extra)
        raise MockgunError(f'mockgun cannot {rpc} {key}: missing or extra parameter')
    return params


def _rules(store: Store) -> list[Row]:
    """Read rules only from the complete declared scheduling schema."""
    if not all(store.schema.declares('WorkDayRule', field) for field in _RULE_FIELDS):
        return []
    return store.rows('WorkDayRule', retired=False)


def _schedule_day(store: Store, day: str, project: object) -> Row | None:
    """Resolve a single supported exception, rejecting ambiguous covering rules."""
    matches: list[Row] = []
    for row in _rules(store):
        for field in _RULE_FIELDS:
            store.check_known(row, field)
        start, end = row.get('start_date'), row.get('end_date')
        if not isinstance(start, str) or not isinstance(end, str):
            raise MockgunError(
                f'mockgun cannot work_schedule_read WorkDayRule {row["id"]}: '
                'date coverage is unsettled'
            )
        if not start <= day <= end:
            continue
        if row.get('user') is not None or row.get('project') is None:
            raise MockgunError(
                f'mockgun cannot work_schedule_read WorkDayRule {row["id"]}: '
                'user or studio rule is unsettled'
            )
        if row.get('project') == project:
            if start != day or end != day:
                raise MockgunError(
                    f'mockgun cannot work_schedule_read WorkDayRule {row["id"]}: '
                    'spanning rule is unsettled'
                )
            matches.append(row)
    if len(matches) > 1:
        raise MockgunError(
            f'mockgun cannot work_schedule_read WorkDayRule {day}: '
            'multiple matching rules are unsettled'
        )
    return matches[0] if matches else None


def _typed(rpc: str, params: dict[str, object], key: str, kind: type) -> object:
    """Require the exact JSON type of a present parameter."""
    value = params[key]
    if type(value) is not kind:
        raise MockgunError(f'mockgun cannot {rpc} {key}: invalid parameter type')
    return value


def _day(rpc: str, params: dict[str, object], key: str) -> date:
    """Require a canonical calendar date string."""
    value = cast('str', _typed(rpc, params, key, str))
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise MockgunError(f'mockgun cannot {rpc} {key}: invalid date {value!r}') from exc
    if result.isoformat() != value:
        raise MockgunError(f'mockgun cannot {rpc} {key}: expected YYYY-MM-DD')
    return result


def _link(rpc: str, params: dict[str, object], key: str, kind: str) -> dict[str, object]:
    """Require a typed identity link in an optional restriction."""
    link = cast('dict[str, object]', _typed(rpc, params, key, dict))
    if link.keys() != {'type', 'id'} or link['type'] != kind or type(link['id']) is not int:
        raise MockgunError(f'mockgun cannot {rpc} {key}: expected a {kind} identity link')
    return link


def _navigation_params(rpc: str, value: object) -> dict[str, object]:
    """Validate navigation and export parameters before canned lookup."""
    if rpc == 'export_page':
        params = _params(rpc, value, {'page_id', 'format'}, {'layout_name'})
        _typed(rpc, params, 'page_id', int)
        _typed(rpc, params, 'format', str)
        if 'layout_name' in params:
            _typed(rpc, params, 'layout_name', str)
        return params
    required = {'path'} if rpc == 'nav_expand' else {'root_path', 'search_criteria'}
    optional = (
        {'seed_entity_field', 'entity_fields'} if rpc == 'nav_expand' else {'seed_entity_field'}
    )
    params = _params(rpc, value, required, optional)
    _typed(rpc, params, 'path' if rpc == 'nav_expand' else 'root_path', str)
    if 'seed_entity_field' in params:
        _typed(rpc, params, 'seed_entity_field', str)
    if 'entity_fields' in params:
        fields = cast('dict[str, object]', _typed(rpc, params, 'entity_fields', dict))
        if any(
            not isinstance(values, list)
            or any(not isinstance(item, str) for item in cast('list[object]', values))
            for values in fields.values()
        ):
            raise MockgunError(f'mockgun cannot {rpc} entity_fields: expected lists of names')
    if rpc == 'nav_search':
        criteria = cast('dict[str, object]', _typed(rpc, params, 'search_criteria', dict))
        if criteria.keys() == {'search_string'}:
            _typed(rpc, criteria, 'search_string', str)
        elif criteria.keys() == {'entity'}:
            entity = cast('dict[str, object]', _typed(rpc, criteria, 'entity', dict))
            if (
                entity.keys() != {'type', 'id'}
                or not isinstance(entity['type'], str)
                or type(entity['id']) is not int
            ):
                raise MockgunError(
                    f'mockgun cannot {rpc} search_criteria.entity: invalid identity link'
                )
        else:
            raise MockgunError(
                f'mockgun cannot {rpc} search_criteria: expected search_string or entity'
            )
    return params


def _schedule_params(rpc: str, value: object, *, update: bool) -> dict[str, object]:
    """Validate schedule restrictions before inspecting records."""
    required = {'date', 'working'} if update else {'start_date', 'end_date'}
    optional = (
        {'project', 'user', 'description', 'recalculate_field'} if update else {'project', 'user'}
    )
    params = _params(rpc, value, required, optional)
    for key, kind in (('project', 'Project'), ('user', 'HumanUser')):
        if key in params:
            _link(rpc, params, key, kind)
    if 'user' in params or 'recalculate_field' in params:
        key = 'user' if 'user' in params else 'recalculate_field'
        raise MockgunError(f'mockgun cannot {rpc} {key}: this scheduling scope is unsettled')
    return params


def export_page(server: 'FakeServer', params: object, *, context: RequestContext) -> object:
    """Return the exact configured export result or fault.

    Args:
        server: Site state and explicit canned responses.
        params: Wire page ID, format and optional layout name.
        context: Request identity and timestamp.

    Raises:
        MockgunError: If no configured response matches the request.
        ServerFaultError: If the matching entry specifies a canned fault.
    """
    del context
    return server.site_data.canned('export_page', _navigation_params('export_page', params))


def json_copy(value: object) -> object:
    """Detach a site control value through its JSON representation.

    Args:
        value: A JSON-serializable control value.

    Returns:
        The decoded, detached JSON value.

    Raises:
        TypeError: If the value cannot be represented as JSON.
    """
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (ValueError, TypeError) as exc:
        raise TypeError('site control values must be JSON-serializable') from exc


def nav_expand(server: 'FakeServer', params: object, *, context: RequestContext) -> object:
    """Return an explicitly configured navigation expansion.

    Args:
        server: Site state and explicit canned responses.
        params: Wire navigation path and optional field selections.
        context: Request identity and timestamp.

    Raises:
        MockgunError: If no configured response matches the request.
        ServerFaultError: If the matching entry specifies a canned fault.
    """
    del context
    return server.site_data.canned('nav_expand', _navigation_params('nav_expand', params))


def nav_search(server: 'FakeServer', params: object, *, context: RequestContext) -> object:
    """Return an explicitly configured entity or string navigation search.

    Args:
        server: Site state and explicit canned responses.
        params: Wire root path, search criteria and optional seed field.
        context: Request identity and timestamp.

    Raises:
        MockgunError: If no configured response matches the request.
        ServerFaultError: If the matching entry specifies a canned fault.
    """
    del context
    return server.site_data.canned('nav_search', _navigation_params('nav_search', params))


def preferences_read(server: 'FakeServer', params: object, *, context: RequestContext) -> object:
    """Read configured preferences in recorded or requested order (C6 P4-56).

    Args:
        server: Site state and explicit preference values.
        params: A `prefs` list; empty requests all recorded keys.
        context: Request identity and timestamp.

    Returns:
        A mapping of requested or recorded names to detached configured values,
        omitting unconfigured names outside the recorded keys.

    Raises:
        MockgunError: If a recorded key has no configured value.
    """
    del context
    values = _params('preferences_read', params, {'prefs'}, set())
    names = cast('list[object]', _typed('preferences_read', values, 'prefs', list))
    if any(not isinstance(name, str) for name in names):
        raise MockgunError('mockgun cannot preferences_read prefs: expected string names')
    result: dict[str, object] = {}
    for name in cast('list[str]', names) or _RECORDED_PREFERENCES:
        if name in server.site_data.preferences:
            result[name] = deepcopy(server.site_data.preferences[name])
        elif name in _RECORDED_PREFERENCES:
            raise MockgunError(
                f'mockgun cannot preferences_read {name}: site value is unconfigured'
            )
    return result


def update_project_last_accessed_by_current_user(
    server: 'FakeServer', params: object, *, context: RequestContext
) -> object:
    """Record an actor's Project access instant and taint its side effects.

    Args:
        server: Site records and journaled per-actor access times.
        params: Project ID and optional attributed HumanUser ID.
        context: Request identity and timestamp.

    Returns:
        `True` after recording the timestamp.

    Raises:
        MockgunError: If the Project or attributed HumanUser is unavailable, or
            no request actor is available.
    """
    rpc = 'update_project_last_accessed_by_current_user'
    values = _params(rpc, params, {'project_id'}, {'user_id'})
    identity = cast('int', _typed(rpc, values, 'project_id', int))
    project = _active(server.store, rpc, 'Project', identity)
    actor = context.actor
    if 'user_id' in values:
        user_id = cast('int', _typed(rpc, values, 'user_id', int))
        _active(server.store, rpc, 'HumanUser', user_id)
        kind = 'HumanUser'
    elif actor is not None:
        kind, user_id = actor.entity_type, actor.entity_id
    else:
        raise MockgunError(f'mockgun cannot {rpc} Project {identity}: actor is unavailable')
    with server.store.transaction():
        server.store.side_set('last_access', (identity, kind, user_id), context.now)
        server.store.taint_events(f'{rpc} Project {identity}: event rows are unsettled')
        taint_activity(server.store, [project], f'{rpc} Project {identity}: activity is unsettled')
    return True


def user_subscriptions_create(
    server: 'FakeServer', params: object, *, context: RequestContext
) -> object:
    """Accept only observed no-op subscription writes.

    Args:
        server: Site state and explicit subscriptions.
        params: Mapping of decimal HumanUser ID strings to subscription values.
            Only values equal to the configured subscriptions are accepted as
            modeled writes.
        context: Request identity and timestamp.

    Returns:
        A mapping of unchanged IDs to `None`.

    Raises:
        ServerFaultError: If a single requested HumanUser does not exist.
        MockgunError: If a requested HumanUser is otherwise unavailable, or a
            subscription is unconfigured or would change.
    """
    del context
    rpc = 'user_subscriptions_create'
    if not isinstance(params, dict):
        raise MockgunError(f'mockgun cannot {rpc} params: expected a dictionary')
    values = cast('dict[str, object]', params)
    result: dict[str, None] = {}
    for key, value in values.items():
        if not key.isascii() or not key.isdecimal():
            raise MockgunError(f'mockgun cannot {rpc} {key}: expected a decimal HumanUser ID')
        if (
            value is not None
            and not isinstance(value, str)
            and not (
                isinstance(value, list)
                and all(isinstance(item, str) for item in cast('list[object]', value))
            )
        ):
            raise MockgunError(f'mockgun cannot {rpc} {key}: invalid subscription type')
        identity = int(key)
        row = (
            server.store.row('HumanUser', identity)
            if server.schema.declares('HumanUser')
            else None
        )
        if row is None and len(values) == 1:
            raise ServerFaultError(
                f'API user_subscriptions_create() failed! Invalid humanUserId {key}'
            )
        _active(server.store, rpc, 'HumanUser', identity)
        if (
            identity not in server.site_data.subscriptions
            or value != server.site_data.subscriptions[identity]
        ):
            raise MockgunError(
                f'mockgun cannot {rpc} HumanUser {identity}: changed subscription is unsettled'
            )
        result[key] = None
    return result


def user_subscriptions_read(
    server: 'FakeServer', params: object, *, context: RequestContext
) -> object:
    """Read configured active HumanUser subscriptions in insertion order.

    Args:
        server: Site state and explicit subscriptions.
        params: Absent wire parameters, represented by `None`.
        context: Request identity and timestamp.

    Returns:
        Subscription strings keyed by string HumanUser IDs, in configured
        insertion order.

    Raises:
        MockgunError: If a configured HumanUser is missing, retired, or
            undeclared.
    """
    del context
    if params is not None:
        raise MockgunError(
            'mockgun cannot user_subscriptions_read params: expected absent parameters'
        )
    result: dict[str, str] = {}
    for identity, value in server.site_data.subscriptions.items():
        _active(server.store, 'user_subscriptions_read', 'HumanUser', identity)
        result[str(identity)] = value
    return result


def work_schedule_read(server: 'FakeServer', params: object, *, context: RequestContext) -> object:
    """Read the studio week and supported single-date Project exceptions.

    Args:
        server: Site settings and WorkDayRule records.
        params: Inclusive start and end dates, with an optional Project
            restriction.
        context: Request identity and timestamp.

    Returns:
        Date-keyed working flags, descriptions and observed reason names, in
        chronological order.

    Raises:
        MockgunError: If the requested scheduling scope is unsupported, the
            date range is invalid, or relevant rules have unknown, overlapping,
            spanning, or otherwise unsettled coverage.
    """
    del context
    rpc = 'work_schedule_read'
    values = _schedule_params(rpc, params, update=False)
    start, end = _day(rpc, values, 'start_date'), _day(rpc, values, 'end_date')
    if start > end:
        raise MockgunError(f'mockgun cannot {rpc} start_date: later than end_date')
    result: dict[str, object] = {}
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        rule = _schedule_day(server.store, day.isoformat(), values.get('project'))
        result[day.isoformat()] = {
            'working': rule['working'] if rule else server.site_data.work_week[day.weekday()],
            'description': rule['description'] if rule else None,
            'reason': 'PROJECT_EXCEPTION' if rule else 'STUDIO_WORK_WEEK',
        }
    return result


def work_schedule_update(
    server: 'FakeServer', params: object, *, context: RequestContext
) -> object:
    """Replace a Project's single-date exception transactionally (C6 P4-60/61).

    Args:
        server: Site settings and WorkDayRule records.
        params: Date, working flag, optional description and required Project.
        context: Request identity and timestamp.

    Returns:
        The supplied date and values with a named Project link and null user.
    """
    rpc = 'work_schedule_update'
    values = _schedule_params(rpc, params, update=True)
    day = _day(rpc, values, 'date').isoformat()
    working = _typed(rpc, values, 'working', bool)
    if 'description' in values:
        _typed(rpc, values, 'description', str)
    if 'project' not in values:
        raise MockgunError(f'mockgun cannot {rpc} project: studio-wide updates are unsettled')
    project = cast('dict[str, object]', values['project'])
    identity = cast('int', project['id'])
    target = _active(server.store, rpc, 'Project', identity)
    if not all(server.schema.declares('WorkDayRule', field) for field in _RULE_FIELDS):
        raise MockgunError(f'mockgun cannot {rpc} WorkDayRule: scheduling schema is undeclared')
    fields = {
        'start_date': day,
        'end_date': day,
        'working': working,
        'description': values.get('description'),
        'project': project,
        'user': None,
    }
    project_name = display_name(server.store, 'Project', identity)
    with server.store.transaction():
        for rule in _rules(server.store):
            if (
                rule.get('project') == project
                and rule.get('start_date') == rule.get('end_date') == day
            ):
                server.store.retire('WorkDayRule', cast('int', rule['id']), context=context)
        row = server.store.create('WorkDayRule', fields, context=context)
        for field in ('precedence', 'day_of_week'):
            if server.schema.declares('WorkDayRule', field):
                server.store.mark_unknown(row, field, f'{rpc} server-computed value is unsettled')
        server.store.taint_events(f'{rpc} WorkDayRule {row["id"]}: event rows are unsettled')
        taint_activity(server.store, [target], f'{rpc} Project {identity}: activity is unsettled')
    return {
        'date': day,
        'working': working,
        'description': values.get('description'),
        'project': {**project, 'name': project_name},
        'user': None,
    }
