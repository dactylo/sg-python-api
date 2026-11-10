#!/usr/bin/env python
"""Restore private site state around the pinned upstream conformance tests."""

from __future__ import annotations

import configparser
import copy
import datetime as dt
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

import pytest

from shotgun_api3 import Shotgun

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType
    from typing import Protocol

    class _ConformanceClient(Protocol):
        """Expose the client operations needed to restore conformance state."""

        def create(self, entity: str, data: dict[str, object], /) -> dict[str, object]:
            """Create one record for conformance setup."""
            ...

        def delete(self, entity: str, entity_id: int, /) -> bool:
            """Delete one record during conformance restoration."""
            ...

        def find(
            self, entity: str, filters: list[list[object]], fields: list[str], /
        ) -> list[dict[str, object]]:
            """Read matching records with the requested fields."""
            ...

        def find_one(
            self,
            entity: str,
            filters: list[list[object]],
            fields: list[str],
            /,
            *,
            order: list[dict[str, str]] | None = None,
        ) -> dict[str, object] | None:
            """Read the first matching record with the requested fields."""
            ...

        def revive(self, entity: str, entity_id: int, /) -> bool:
            """Restore one retired record."""
            ...

        def schema_field_delete(self, entity: str, field: str, /) -> bool:
            """Remove one conformance schema field."""
            ...

        def schema_field_read(
            self,
            entity: str,
            field: str | None = None,
            project: dict[str, object] | None = None,
            /,
        ) -> dict[str, object]:
            """Read field metadata in the optional project scope."""
            ...

        def schema_field_update(
            self,
            entity: str,
            field: str,
            data: dict[str, object],
            project: dict[str, object],
            /,
        ) -> bool:
            """Update field properties for one project."""
            ...

        def update(
            self, entity: str, entity_id: int, data: dict[str, object], /
        ) -> dict[str, object]:
            """Update one record and return its resulting fields."""
            ...

    class _Node(Protocol):
        """Describe the collected pytest test node."""

        name: str
        path: Path

    class _Request(Protocol):
        """Expose the current pytest instance and node."""

        instance: object | None
        node: _Node

    class _UpstreamCase(Protocol):
        """Expose the upstream case asset for restoration."""

        asset: dict[str, object]


_RULE_FIELDS = (
    'start_date',
    'end_date',
    'working',
    'user',
    'description',
    'precedence',
    'day_of_week',
    'project',
)
_RULE_DATES = ('2012-01-02', '2012-01-03', '2012-01-04')
_ISO_DATE_LENGTH = len('YYYY-MM-DD')
_TEST_FIELDS = {
    'TestDataTypes.test_set_checkbox': 'email_notes',
    'TestDataTypes.test_set_date_time': 'locked_until',
    'TestErrors.test_bad_auth': 'locked_until',
    'TestUtc.test_convert_to_utc': 'locked_until',
    'TestUtc.test_no_convert_to_utc': 'locked_until',
    'TestScriptUserSudoAuth.test_user_is_creator': 'projects',
}
_T = TypeVar('_T')
_Restoration = list[tuple[str, Callable[[], None]]]


class _PrivateFailure:
    """Suppress private API errors for a sanitized caller-owned failure."""

    def __init__(self) -> None:
        """Start with no failure recorded."""
        self.failed = False
        self.restoration_failure: _RestorationError | None = None

    def __enter__(self) -> None:
        """Enter a private API operation."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Suppress ordinary failures without swallowing process control."""
        del exc_type, traceback
        self.failed = exc is not None
        if isinstance(exc, _RestorationError):
            self.restoration_failure = exc
        return isinstance(exc, Exception)


class _RestorationError(RuntimeError):
    """Indicate a sanitized cleanup failure across setup boundaries."""


def _date_string(value: object) -> str | None:
    """Normalize API date values for comparison with the test dates."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, str) and len(value) == _ISO_DATE_LENGTH:
        dt.date.fromisoformat(value)
        return value
    raise ValueError('Unexpected conformance rule date')


def _entity_id(value: object, expected_type: str) -> int | None:
    """Extract an entity ID without retaining its display name."""
    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or cast('dict[str, object]', value).get('type') != expected_type
    ):
        raise ValueError('Unexpected conformance entity reference')
    entity_id = cast('dict[str, object]', value).get('id')
    if not isinstance(entity_id, int) or isinstance(entity_id, bool):
        raise TypeError('Unexpected conformance entity ID')
    return entity_id


def _project_ids(links: object) -> list[int]:
    """Validate a complete list of user project links."""
    if not isinstance(links, list):
        raise TypeError('Conformance user projects are unavailable')
    ids: list[int] = []
    for link in cast('list[object]', links):
        project_id = _entity_id(link, 'Project')
        if project_id is None:
            raise TypeError('Conformance user project link is unavailable')
        ids.append(project_id)
    return ids


def _read_config(config_path: Path) -> configparser.ConfigParser:
    """Read private upstream settings without exposing their values."""
    config = configparser.ConfigParser()
    if not config.read(config_path, encoding='utf-8'):
        raise ValueError('Conformance config is missing')
    return config


def _record(
    client: _ConformanceClient, entity: str, entity_id: int, fields: list[str]
) -> dict[str, object]:
    """Read one existing record with every requested field present."""
    result = client.find_one(entity, [['id', 'is', entity_id]], fields)
    if not isinstance(result, dict) or any(field not in result for field in fields):
        raise ValueError('Conformance record is missing')
    return result


def _capture_schema_field(
    client: _ConformanceClient, field: str, asset_id: int | None, restoration: _Restoration
) -> bool:
    """Save field presence and, when it exists, the shared asset value."""
    existed = field in client.schema_field_read('Asset')
    if existed and asset_id is not None:
        original = copy.deepcopy(_record(client, 'Asset', asset_id, [field])[field])

        def restore_value() -> None:
            current = _record(client, 'Asset', asset_id, [field])[field]
            if current != original:
                client.update('Asset', asset_id, {field: copy.deepcopy(original)})
            if _record(client, 'Asset', asset_id, [field])[field] != original:
                raise ValueError('Conformance asset field was not restored')

        restoration.append(('asset value', restore_value))
    if not existed:

        def delete_new_field() -> None:
            if (
                field in client.schema_field_read('Asset')
                and client.schema_field_delete('Asset', field) is not True
            ):
                raise ValueError('Conformance field deletion failed')
            if field in client.schema_field_read('Asset'):
                raise ValueError('Conformance field remains')

        restoration.append(('new schema field', delete_new_field))
    return existed


def _capture_user_field(
    client: _ConformanceClient, user_id: int, field: str, restoration: _Restoration
) -> None:
    """Save a user field and register exact-value restoration."""
    original = copy.deepcopy(_record(client, 'HumanUser', user_id, [field])[field])

    def restore() -> None:
        current = _record(client, 'HumanUser', user_id, [field])[field]
        if current != original:
            client.update('HumanUser', user_id, {field: copy.deepcopy(original)})
        if _record(client, 'HumanUser', user_id, [field])[field] != original:
            raise ValueError('Conformance user field was not restored')

    restoration.append(('user field', restore))


def _capture_visibility(
    client: _ConformanceClient, field: str, project: dict[str, object], restoration: _Restoration
) -> None:
    """Save visibility on both projects, or arrange to delete the new field."""
    if not _capture_schema_field(client, field, None, restoration):
        return
    other = client.find_one(
        'Project',
        [['id', 'is_not', project['id']]],
        ['id'],
        order=[{'field_name': 'id', 'direction': 'asc'}],
    )
    if not isinstance(other, dict) or not isinstance(other.get('id'), int):
        raise TypeError('A second conformance project is required')
    for target in (project, other):
        schema = client.schema_field_read('Asset', field, target)
        visibility = cast('dict[str, object]', schema.get(field, {})).get('visible')
        if not isinstance(visibility, dict):
            raise TypeError('Conformance visibility is missing')
        original = cast('dict[str, object]', visibility).get('value')
        if not isinstance(original, bool):
            raise TypeError('Conformance visibility is missing')

        def restore(target: dict[str, object] = target, original: bool = original) -> None:
            schema = client.schema_field_read('Asset', field, target)
            field_schema = cast('dict[str, object]', schema[field])
            current = cast('dict[str, object]', field_schema['visible'])['value']
            if current != original:
                client.schema_field_update('Asset', field, {'visible': original}, target)
            schema = client.schema_field_read('Asset', field, target)
            field_schema = cast('dict[str, object]', schema[field])
            actual = cast('dict[str, object]', field_schema['visible'])['value']
            if actual != original:
                raise ValueError('Conformance visibility was not restored')

        restoration.append(('project visibility', restore))


def _register_access_restore(
    client: _ConformanceClient, user: dict[str, object]
) -> tuple[_Restoration, list[int]]:
    """Save all prior user project links before access changes."""
    original_value = user['projects']
    if not isinstance(original_value, list):
        raise TypeError('Conformance user projects are unavailable')
    original = copy.deepcopy(cast('list[object]', original_value))
    original_ids = _project_ids(original)
    if len(original_ids) != len(set(original_ids)):
        raise ValueError('Conformance user project links are ambiguous')
    restoration: _Restoration = []
    user_id = user['id']
    if not isinstance(user_id, int):
        raise TypeError('Conformance user ID is unavailable')

    def restore() -> None:
        current = _record(client, 'HumanUser', user_id, ['projects'])['projects']
        if current != original:
            client.update('HumanUser', user_id, {'projects': copy.deepcopy(original)})
        actual = _record(client, 'HumanUser', user_id, ['projects'])['projects']
        if _project_ids(actual) != original_ids:
            raise ValueError('Conformance project access was not restored')

    restoration.append(('user project access', restore))
    return restoration, original_ids


def _resolve_access_entities(
    client: _ConformanceClient, config: configparser.ConfigParser
) -> tuple[dict[str, object], dict[str, object]]:
    """Find the configured user and stable project unambiguously."""
    login = config['TEST_DATA']['human_login']
    project_name = config['TEST_DATA']['project_name']
    users = client.find('HumanUser', [['login', 'is', login]], ['projects'])
    if len(users) != 1 or not isinstance(users[0].get('projects'), list):
        raise ValueError('Configured conformance user is unavailable')
    projects = client.find('Project', [['name', 'is', project_name]], ['name'])
    if len(projects) > 1:
        raise ValueError('Conformance project name is ambiguous')
    project = projects[0] if projects else client.create('Project', {'name': project_name})
    if not isinstance(project.get('id'), int):
        raise TypeError('Conformance project is unavailable')
    return users[0], project


def _restore_prior_rule(
    client: _ConformanceClient, rule_id: int, prior: dict[str, object]
) -> bool:
    """Revive or update an original rule, reporting a failed API operation."""
    failed = False
    existing = client.find_one('WorkDayRule', [['id', 'is', rule_id]], list(_RULE_FIELDS))
    if existing is None:
        failure = _PrivateFailure()
        with failure:
            if client.revive('WorkDayRule', rule_id) is not True:
                failed = True
        if failure.failed or failed:
            return False
        existing = client.find_one('WorkDayRule', [['id', 'is', rule_id]], list(_RULE_FIELDS))
    if existing is None:
        return False
    changes = {
        field: copy.deepcopy(prior[field])
        for field in _RULE_FIELDS
        if existing[field] != prior[field]
    }
    if changes:
        failure = _PrivateFailure()
        with failure:
            client.update('WorkDayRule', rule_id, changes)
        if failure.failed:
            return False
    return True


def _rule_applies(rule: dict[str, object], date: str, scope: dict[str, object] | None) -> bool:
    """Identify only the dated exception at the expected scope."""
    if scope is None:
        if rule['project'] is not None or rule['user'] is not None:
            return False
    elif scope['type'] == 'Project':
        if _entity_id(rule['project'], 'Project') != scope['id'] or rule['user'] is not None:
            return False
    elif scope['type'] == 'HumanUser':
        if _entity_id(rule['user'], 'HumanUser') != scope['id'] or rule['project'] is not None:
            return False
    else:
        raise ValueError('Unexpected conformance rule scope')
    start = _date_string(rule['start_date'])
    end = _date_string(rule['end_date'])
    if start is None and end is None:
        return False
    return (start is None or start <= date) and (end is None or date <= end)


def _dated_rules(
    client: _ConformanceClient, date: str, scope: dict[str, object] | None
) -> dict[int, dict[str, object]]:
    """Read full writable state for exceptions affecting one tested date."""
    rows = client.find('WorkDayRule', [], list(_RULE_FIELDS))
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        if any(field not in row for field in _RULE_FIELDS):
            raise ValueError('Unexpected conformance rule shape')
        if _rule_applies(row, date, scope):
            rule_id = row.get('id')
            if not isinstance(rule_id, int) or isinstance(rule_id, bool) or rule_id in result:
                raise ValueError('Unexpected conformance rule ID')
            result[rule_id] = copy.deepcopy(row)
    return result


def _run_restore(restoration: _Restoration) -> None:
    """Attempt every cleanup and surface static labels for failures."""
    failures: list[str] = []
    for label, action in reversed(restoration):
        failure = _PrivateFailure()
        with failure:
            action()
        if failure.failed:
            failures.append(label)
    if failures:
        raise _RestorationError('Conformance restoration failed: ' + ', '.join(failures)) from None


def _prepare_access(
    client: _ConformanceClient, config_path: Path
) -> tuple[dict[str, object], dict[str, object], _Restoration]:
    """Resolve fixtures and give the configured user temporary project access."""
    user, project = _resolve_access_entities(client, _read_config(config_path))
    restoration, original_ids = _register_access_restore(client, user)
    if project['id'] not in original_ids:
        original = cast('list[object]', user['projects'])
        user_id = cast('int', user['id'])
        verified = False
        try:
            failure = _PrivateFailure()
            with failure:
                client.update('HumanUser', user_id, {'projects': [*original, project]})
                active = _record(client, 'HumanUser', user_id, ['projects'])['projects']
                if project['id'] not in _project_ids(active):
                    raise ValueError('Conformance project access was not added')
            if failure.failed:
                raise ValueError('Conformance project access was not added')
            verified = True
        finally:
            if not verified:
                _run_restore(restoration)
    return user, project, restoration


def _sanitized(action: Callable[[], _T], message: str) -> _T:
    """Replace private API errors with a static public failure."""
    result: list[_T] = []
    failure = _PrivateFailure()
    with failure:
        result.append(action())
    if failure.restoration_failure is not None:
        raise failure.restoration_failure from None
    if failure.failed:
        raise RuntimeError(message) from None
    return result[0]


def _single_day_rule(rule: dict[str, object], date: str) -> bool:
    """Accept only an exception wholly owned by one tested date."""
    return _date_string(rule['start_date']) == date == _date_string(rule['end_date'])


def _restore_schedule_date(
    client: _ConformanceClient,
    date: str,
    scope: dict[str, object] | None,
    original: dict[int, dict[str, object]],
) -> None:
    """Restore one exact date and scope while leaving other rules untouched."""
    current = _dated_rules(client, date, scope)
    failed = False
    for rule_id in current.keys() - original.keys():
        if not _single_day_rule(current[rule_id], date):
            failed = True
            continue
        failure = _PrivateFailure()
        with failure:
            if client.delete('WorkDayRule', rule_id) is not True:
                failed = True
        failed |= failure.failed
    for rule_id, prior in original.items():
        failure = _PrivateFailure()
        with failure:
            if not _restore_prior_rule(client, rule_id, prior):
                failed = True
        failed |= failure.failed
    if failed or _dated_rules(client, date, scope) != original:
        raise ValueError('Conformance schedule was not restored')


def _capture_schedule(
    client: _ConformanceClient,
    project: dict[str, object],
    user: dict[str, object],
    restoration: _Restoration,
) -> None:
    """Save dated rules so new exceptions can be removed after the test."""
    for date, scope in zip(_RULE_DATES, (None, project, user), strict=True):
        original = _dated_rules(client, date, scope)
        if any(not _single_day_rule(rule, date) for rule in original.values()):
            raise ValueError('Conformance schedule has an overlapping range')

        def restore(
            date: str = date,
            scope: dict[str, object] | None = scope,
            original: dict[int, dict[str, object]] = original,
        ) -> None:
            _restore_schedule_date(client, date, scope, original)

        restoration.append(('work schedule', restore))


def _capture_test(
    client: _ConformanceClient,
    case: object,
    test_name: str,
    user: dict[str, object],
    project: dict[str, object],
) -> _Restoration:
    """Register the state touched by one pinned upstream test."""
    restoration: _Restoration = []
    match test_name:
        case 'TestFind.test_zero_is_not_none':
            field = 'sg_cf_number_' + uuid.uuid4().hex
            vars(case)['_conformance_number_field'] = field
            asset = cast('_UpstreamCase', case).asset
            _capture_schema_field(client, field, cast('int', asset['id']), restoration)
        case 'TestReadAdditionalFilterPresets.test_modify_visibility':
            label = 'CF Visibility ' + uuid.uuid4().hex
            vars(case)['_conformance_visibility_label'] = label
            field = 'sg_' + label.lower().replace(' ', '_')
            _capture_visibility(client, field, project, restoration)
        case 'TestShotgunApi.test_work_schedule':
            _capture_schedule(client, project, user, restoration)
        case _ if test_name in _TEST_FIELDS:
            _capture_user_field(
                client, cast('int', user['id']), _TEST_FIELDS[test_name], restoration
            )
        case _:
            pass
    return restoration


@pytest.fixture(scope='session', autouse=True)
def conformance_access() -> Iterator[tuple[Shotgun, dict[str, object], dict[str, object]]]:
    """Provide temporary project access before upstream class setup.

    Restore the temporary access changes and close the client when the session
    fixture finishes.

    Yields:
        The client, human-user record, and project record, in that order.

    Raises:
        RuntimeError: If connection setup, access preparation, restoration, or
            client cleanup fails.
    """
    config_path = Path(__file__).with_name('config')

    def connect() -> Shotgun:
        config = _read_config(config_path)
        server = config['SERVER_INFO']
        return Shotgun(
            server['server_url'],
            script_name=server['script_name'],
            api_key=server['api_key'],
            connect=False,
        )

    client = _sanitized(connect, 'Conformance access setup failed')
    try:
        user, project, restoration = _sanitized(
            lambda: _prepare_access(client, config_path), 'Conformance access setup failed'
        )
        try:
            yield client, user, project
        finally:
            _run_restore(restoration)
    finally:
        _sanitized(client.close, 'Conformance client close failed')


@pytest.fixture(autouse=True)
def conformance_test_restore(
    request: _Request, conformance_access: tuple[Shotgun, dict[str, object], dict[str, object]]
) -> Iterator[None]:
    """Restore pinned test mutations even when its unittest body fails.

    Tests outside `test_api.py`, and tests without a unittest instance, run
    without a captured restoration.

    Args:
        request: Pytest request identifying the test and unittest instance.
        conformance_access: Session client, human-user record, and project
            record, in that order.

    Yields:
        Control to the test body while any captured restoration is pending.

    Raises:
        RuntimeError: If capturing or restoring the test state fails.
    """
    instance = request.instance
    node = request.node
    if node.path.name != 'test_api.py' or instance is None:
        yield
        return
    name = f'{type(instance).__name__}.{node.name}'
    client, user, project = conformance_access
    restoration = _sanitized(
        lambda: _capture_test(client, instance, name, user, project),
        'Conformance test snapshot failed',
    )
    try:
        yield
    finally:
        _run_restore(restoration)
