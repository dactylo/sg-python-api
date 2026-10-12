from __future__ import annotations

import configparser
import copy
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from scripts import _conformance_runtime as runtime

if TYPE_CHECKING:
    from shotgun_api3 import Shotgun


class _Site:
    def __init__(self) -> None:
        self.user: dict[str, Any] = {
            'type': 'HumanUser',
            'id': 11,
            'login': 'fixture-user',
            'projects': [{'type': 'Project', 'id': 7, 'name': 'existing'}],
            'email_notes': True,
            'locked_until': '2030-01-01T00:00:00Z',
        }
        self.project = {'type': 'Project', 'id': 9, 'name': 'conformance'}
        self.other_project = {'type': 'Project', 'id': 7, 'name': 'existing'}
        self.asset = {'type': 'Asset', 'id': 17, 'sg_api_tests_number_field': 42}
        self.fields = {'sg_api_tests_number_field', 'sg_project_visibility_test'}
        self.retired_fields: set[str] = set()
        self.visibility = {7: True, 9: False}
        self.rules: dict[int, dict[str, Any]] = {}
        self.fail_user_update_after_mutation = False
        self.fail_user_restore = False
        self.interrupt_user_update_after_mutation = False
        self.fail_delete_rule_id: int | None = None
        self.fail_visibility_project: int | None = None
        self.delete_attempts: list[int] = []

    def create(self, entity: str, data: dict[str, Any]) -> dict[str, Any]:
        assert entity == 'Project'
        self.project = {'type': 'Project', 'id': 9, **data}
        return copy.deepcopy(self.project)

    def delete(self, entity: str, entity_id: int) -> bool:
        assert entity == 'WorkDayRule'
        self.delete_attempts.append(entity_id)
        if self.fail_delete_rule_id == entity_id:
            raise RuntimeError('private server detail')
        self.rules.pop(entity_id)
        return True

    def find(
        self,
        entity: str,
        filters: list[list[object]],
        fields: list[str],
        **kwargs: object,
    ) -> list[dict[str, Any]]:
        del fields, kwargs
        if entity == 'HumanUser':
            rows = [self.user]
        elif entity == 'Project':
            rows = [self.other_project, self.project]
        elif entity == 'Asset':
            rows = [self.asset]
        elif entity == 'WorkDayRule':
            rows = list(self.rules.values())
        else:
            raise AssertionError(entity)
        for key, operator, value in filters:
            assert isinstance(key, str)
            if operator == 'is':
                rows = [row for row in rows if row[key] == value]
            elif operator == 'is_not':
                rows = [row for row in rows if row[key] != value]
            else:
                raise AssertionError(operator)
        return copy.deepcopy(rows)

    def find_one(
        self,
        entity: str,
        filters: list[list[object]],
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> dict[str, Any] | None:
        rows = self.find(entity, filters, fields or [], **kwargs)
        return rows[0] if rows else None

    def revive(self, entity: str, entity_id: int) -> bool:
        assert entity == 'WorkDayRule'
        assert entity_id in self.rules
        return True

    def schema_field_create(self, entity: str, field_type: str, label: str) -> str:
        assert entity == 'Asset'
        assert field_type in {'number', 'text'}
        field = 'sg_' + label.lower().replace(' ', '_')
        if field in self.fields or field in self.retired_fields:
            raise ValueError('retired schema name cannot be reused')
        self.fields.add(field)
        return field

    def schema_field_delete(self, entity: str, field: str) -> bool:
        assert entity == 'Asset'
        self.fields.remove(field)
        self.retired_fields.add(field)
        return True

    def schema_field_read(
        self, entity: str, field: str | None = None, project: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        assert entity == 'Asset'
        if field is None:
            return {name: {} for name in self.fields}
        if field not in self.fields:
            return {}
        project_id = project['id'] if project else 9
        return {field: {'visible': {'value': self.visibility[project_id], 'editable': True}}}

    def schema_field_update(
        self, entity: str, field: str, data: dict[str, Any], project: dict[str, Any]
    ) -> bool:
        assert entity == 'Asset'
        assert field in self.fields
        if project['id'] == self.fail_visibility_project:
            raise RuntimeError('private server detail')
        self.visibility[project['id']] = data['visible']
        return True

    def update(self, entity: str, entity_id: int, data: dict[str, Any]) -> None:
        if entity == 'HumanUser':
            assert entity_id == self.user['id']
            if self.fail_user_restore and [link['id'] for link in data.get('projects', [])] == [7]:
                raise RuntimeError('private server detail')
            self.user.update(copy.deepcopy(data))
            if self.fail_user_update_after_mutation:
                self.fail_user_update_after_mutation = False
                raise RuntimeError('private server detail')
            if self.interrupt_user_update_after_mutation:
                self.interrupt_user_update_after_mutation = False
                raise KeyboardInterrupt
        elif entity == 'Asset':
            assert entity_id == self.asset['id']
            self.asset.update(copy.deepcopy(data))
        elif entity == 'WorkDayRule':
            self.rules[entity_id].update(copy.deepcopy(data))
        else:
            raise AssertionError(entity)


def _client(site: _Site) -> Shotgun:
    return cast('Shotgun', site)


def _config(path: Path) -> None:
    config = configparser.ConfigParser()
    config['TEST_DATA'] = {'human_login': 'fixture-user', 'project_name': 'conformance'}
    with path.open('w', encoding='utf-8') as stream:
        config.write(stream)


def _rule(
    rule_id: int,
    date: str,
    scope: dict[str, Any] | None = None,
    description: str = 'before',
) -> dict[str, Any]:
    return {
        'type': 'WorkDayRule',
        'id': rule_id,
        'start_date': date,
        'end_date': date,
        'working': True,
        'user': scope if scope and scope['type'] == 'HumanUser' else None,
        'description': description,
        'precedence': 2,
        'day_of_week': None,
        'project': scope if scope and scope['type'] == 'Project' else None,
    }


def test_access_interrupt_after_mutation_restores_prior_links(tmp_path: Path) -> None:
    site = _Site()
    path = tmp_path / 'config'
    _config(path)
    site.interrupt_user_update_after_mutation = True
    with pytest.raises(KeyboardInterrupt):
        runtime._prepare_access(_client(site), path)
    assert [link['id'] for link in site.user['projects']] == [7]


def test_access_restores_prior_project_links_after_sudo_test(tmp_path: Path) -> None:
    site = _Site()
    path = tmp_path / 'config'
    _config(path)
    user, project, session_restore = runtime._prepare_access(_client(site), path)
    assert [link['id'] for link in site.user['projects']] == [7, 9]
    per_test = runtime._capture_test(
        _client(site),
        SimpleNamespace(),
        'TestScriptUserSudoAuth.test_user_is_creator',
        user,
        project,
    )
    site.update('HumanUser', 11, {'projects': [project]})
    runtime._run_restore(per_test)
    assert [link['id'] for link in site.user['projects']] == [7, 9]
    runtime._run_restore(session_restore)
    assert site.user['projects'] == [{'type': 'Project', 'id': 7, 'name': 'existing'}]


def test_access_setup_failure_after_mutation_restores_prior_links(tmp_path: Path) -> None:
    site = _Site()
    path = tmp_path / 'config'
    _config(path)
    site.fail_user_update_after_mutation = True
    with pytest.raises(ValueError, match='access was not added'):
        runtime._prepare_access(_client(site), path)
    assert [link['id'] for link in site.user['projects']] == [7]


def test_access_setup_reports_cleanup_failure_without_private_details(tmp_path: Path) -> None:
    site = _Site()
    path = tmp_path / 'config'
    _config(path)
    site.fail_user_update_after_mutation = True
    site.fail_user_restore = True
    with pytest.raises(RuntimeError, match='Conformance restoration failed') as exc:
        runtime._sanitized(
            lambda: runtime._prepare_access(_client(site), path),
            'Conformance access setup failed',
        )
    assert 'private server detail' not in str(exc.value)
    assert [link['id'] for link in site.user['projects']] == [7, 9]


def test_pytest_fixtures_wrap_unittest_setup_and_failed_test(tmp_path: Path) -> None:
    source = Path(runtime.__file__).read_text(encoding='utf-8')
    (tmp_path / 'conftest.py').write_text(source, encoding='utf-8')
    (tmp_path / 'config').write_text(
        '[SERVER_INFO]\nserver_url = https://example.invalid\nscript_name = fixture\n'
        'api_key = fixture\n[TEST_DATA]\nhuman_login = fixture\n'
        'project_name = conformance\n',
        encoding='utf-8',
    )
    (tmp_path / 'shotgun_api3.py').write_text(
        textwrap.dedent(
            """\
            USER = {'id': 11, 'type': 'HumanUser', 'login': 'fixture',
                    'projects': [{'id': 7, 'type': 'Project'}], 'email_notes': True}
            PROJECT = {'id': 9, 'type': 'Project', 'name': 'conformance'}
            FIELDS = set()
            RETIRED = set()

            class Shotgun:
                def __init__(self, *args, **kwargs):
                    pass

                def find(self, entity, filters, fields):
                    if entity == 'HumanUser':
                        return [dict(USER)]
                    if entity == 'Project':
                        return [dict(PROJECT)]
                    raise AssertionError(entity)

                def find_one(self, entity, filters, fields):
                    assert entity == 'HumanUser'
                    return dict(USER)

                def update(self, entity, entity_id, data):
                    assert entity == 'HumanUser' and entity_id == 11
                    USER.update(data)

                def schema_field_read(self, entity, field=None, project=None):
                    assert entity == 'Asset'
                    return {name: {} for name in FIELDS if field is None or name == field}

                def schema_field_delete(self, entity, field):
                    assert entity == 'Asset' and field in FIELDS
                    FIELDS.remove(field)
                    RETIRED.add(field)
                    return True

                def close(self):
                    assert [link['id'] for link in USER['projects']] == [7]
                    assert not FIELDS
            """
        ),
        encoding='utf-8',
    )
    (tmp_path / 'test_api.py').write_text(
        textwrap.dedent(
            """\
            import unittest
            from shotgun_api3 import FIELDS, RETIRED, USER

            class TestDataTypes(unittest.TestCase):
                @classmethod
                def setUpClass(cls):
                    assert [link['id'] for link in USER['projects']] == [7, 9]

                def setUp(self):
                    if self._testMethodName == 'test_set_checkbox':
                        USER['email_notes'] = False

                @unittest.expectedFailure
                def test_set_checkbox(self):
                    assert USER['email_notes'] is False
                    raise AssertionError('injected test failure')

                def test_verify_restored(self):
                    assert USER['email_notes'] is True

            class TestFind(unittest.TestCase):
                asset = {'id': 17, 'type': 'Asset'}

                def setUp(self):
                    assert self._conformance_number_field.startswith('sg_cf_number_')

                def test_zero_is_not_none(self):
                    field = self._conformance_number_field
                    assert field not in FIELDS and field not in RETIRED
                    FIELDS.add(field)

            class TestReadAdditionalFilterPresets(unittest.TestCase):
                def setUp(self):
                    if self._testMethodName == 'test_modify_visibility':
                        assert self._conformance_visibility_label.startswith('CF Visibility ')

                def test_modify_visibility(self):
                    label = self._conformance_visibility_label
                    field = 'sg_' + label.lower().replace(' ', '_')
                    assert field not in FIELDS and field not in RETIRED
                    FIELDS.add(field)

                def test_verify_schema_restored(self):
                    assert not FIELDS
                    assert len(RETIRED) == 2
            """
        ),
        encoding='utf-8',
    )
    environment = {
        **os.environ,
        'PYTHONPATH': str(tmp_path),
        'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1',
    }
    result = subprocess.run(
        [sys.executable, '-m', 'pytest', '-q', 'test_api.py'],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert '4 passed, 1 xfailed' in result.stdout

    parts = source.rsplit('_run_restore(restoration)', 1)
    assert len(parts) == 2
    (tmp_path / 'conftest.py').write_text(parts[0] + 'pass' + parts[1], encoding='utf-8')
    broken = subprocess.run(
        [sys.executable, '-m', 'pytest', '-q', 'test_api.py'],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert broken.returncode != 0
    assert 'test_verify_restored' in broken.stdout


def test_schedule_cleanup_failure_is_sanitized_and_other_dates_are_restored() -> None:
    site = _Site()
    restoration = runtime._capture_test(
        _client(site),
        SimpleNamespace(),
        'TestShotgunApi.test_work_schedule',
        site.user,
        site.project,
    )
    site.rules[4] = _rule(4, '2012-01-02')
    site.rules[5] = _rule(5, '2012-01-04', site.user)
    site.fail_delete_rule_id = 5
    with pytest.raises(RuntimeError, match='Conformance restoration failed') as exc:
        runtime._run_restore(restoration)
    assert 'private server detail' not in str(exc.value)
    assert set(site.rules) == {5}
    assert site.delete_attempts == [5, 4]


def test_schedule_does_not_delete_new_range_outside_owned_day() -> None:
    site = _Site()
    restoration = runtime._capture_test(
        _client(site),
        SimpleNamespace(),
        'TestShotgunApi.test_work_schedule',
        site.user,
        site.project,
    )
    site.rules[4] = _rule(4, '2012-01-01')
    site.rules[4]['end_date'] = '2012-01-03'
    with pytest.raises(RuntimeError, match='Conformance restoration failed'):
        runtime._run_restore(restoration)
    assert site.delete_attempts == []
    assert 4 in site.rules


def test_schedule_refuses_existing_range_before_test() -> None:
    site = _Site()
    site.rules[1] = _rule(1, '2012-01-01')
    site.rules[1]['end_date'] = '2012-01-04'
    with pytest.raises(ValueError, match='overlapping range'):
        runtime._capture_test(
            _client(site),
            SimpleNamespace(),
            'TestShotgunApi.test_work_schedule',
            site.user,
            site.project,
        )
    assert site.delete_attempts == []


def test_schedule_restores_modified_rules_and_deletes_only_new_target_exceptions() -> None:
    site = _Site()
    site.rules = {
        1: _rule(1, '2012-01-02'),
        2: _rule(2, '2010-01-01'),
        3: _rule(3, '2012-01-03', site.other_project),
    }
    original = copy.deepcopy(site.rules)
    restoration = runtime._capture_test(
        _client(site),
        SimpleNamespace(),
        'TestShotgunApi.test_work_schedule',
        site.user,
        site.project,
    )
    site.rules[1]['description'] = 'changed'
    site.rules[1]['start_date'] = '2013-01-02'
    site.rules[4] = _rule(4, '2012-01-03', site.project)
    site.rules[5] = _rule(5, '2012-01-04', site.user)
    runtime._run_restore(restoration)
    assert site.rules == original


@pytest.mark.parametrize(
    ('name', 'field', 'mutated'),
    [
        ('TestDataTypes.test_set_checkbox', 'email_notes', False),
        ('TestDataTypes.test_set_date_time', 'locked_until', None),
        ('TestErrors.test_bad_auth', 'locked_until', None),
        ('TestUtc.test_convert_to_utc', 'locked_until', None),
        ('TestUtc.test_no_convert_to_utc', 'locked_until', None),
    ],
)
def test_user_fields_restore_nondefault_values(name: str, field: str, mutated: object) -> None:
    site = _Site()
    original = site.user[field]
    restoration = runtime._capture_test(
        _client(site), SimpleNamespace(), name, site.user, site.project
    )
    site.update('HumanUser', 11, {field: mutated})
    assert site.user[field] == mutated
    runtime._run_restore(restoration)
    assert site.user[field] == original


@pytest.mark.parametrize('already_exists', [False, True])
def test_number_field_restores_presence_and_prior_asset_value(already_exists: bool) -> None:
    site = _Site()
    field = 'sg_api_tests_number_field'
    if not already_exists:
        site.fields.remove(field)
        site.asset.pop(field)
    restoration: runtime._Restoration = []
    runtime._capture_schema_field(_client(site), field, 17, restoration)
    site.fields.add(field)
    site.update('Asset', 17, {field: 1})
    runtime._run_restore(restoration)
    assert (field in site.fields) is already_exists
    if already_exists:
        assert site.asset[field] == 42


@pytest.mark.parametrize(
    ('test_name', 'field_type'),
    [
        ('TestFind.test_zero_is_not_none', 'number'),
        ('TestReadAdditionalFilterPresets.test_modify_visibility', 'text'),
    ],
)
def test_fresh_schema_name_survives_two_retired_name_lifecycles(
    test_name: str, field_type: str
) -> None:
    site = _Site()
    original_fields = site.fields.copy()
    names: list[str] = []
    labels: list[str] = []
    for _ in range(2):
        case = SimpleNamespace(asset=site.asset)
        restoration = runtime._capture_test(
            _client(site), case, test_name, site.user, site.project
        )
        if field_type == 'number':
            field = case._conformance_number_field
            label = field.removeprefix('sg_').replace('_', ' ')
        else:
            label = case._conformance_visibility_label
            field = 'sg_' + label.lower().replace(' ', '_')
        assert site.schema_field_create('Asset', field_type, label) == field
        if field_type == 'number':
            site.update('Asset', 17, {field: 1})
        else:
            site.visibility.update({7: False, 9: True})
        runtime._run_restore(restoration)
        names.append(field)
        labels.append(label)
        assert site.fields == original_fields
    assert names[0] != names[1]
    assert site.retired_fields == set(names)
    with pytest.raises(ValueError, match='retired schema name'):
        site.schema_field_create('Asset', field_type, labels[-1])


def test_visibility_cleanup_attempts_both_projects_and_reports_failure() -> None:
    site = _Site()
    restoration: runtime._Restoration = []
    runtime._capture_visibility(
        _client(site), 'sg_project_visibility_test', site.project, restoration
    )
    site.visibility.update({7: False, 9: True})
    site.fail_visibility_project = 9
    with pytest.raises(
        RuntimeError, match='Conformance restoration failed: project visibility'
    ) as exc:
        runtime._run_restore(restoration)
    assert 'private server detail' not in str(exc.value)
    assert site.visibility[7] is True


@pytest.mark.parametrize('already_exists', [False, True])
def test_visibility_restores_both_projects_or_deletes_new_field(already_exists: bool) -> None:
    site = _Site()
    field = 'sg_project_visibility_test'
    if not already_exists:
        site.fields.remove(field)
    restoration: runtime._Restoration = []
    runtime._capture_visibility(_client(site), field, site.project, restoration)
    site.fields.add(field)
    site.visibility.update({7: False, 9: True})
    runtime._run_restore(restoration)
    assert (field in site.fields) is already_exists
    if already_exists:
        assert site.visibility == {7: True, 9: False}
