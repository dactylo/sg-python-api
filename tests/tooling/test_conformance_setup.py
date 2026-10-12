import ast
import configparser
import hashlib
import os
import runpy
import subprocess
import sys
import unittest
import warnings
from pathlib import Path
from typing import cast

import pytest

from scripts import _conformance_setup as setup
from shotgun_api3 import ShotgunError

_API_SOURCE = (
    'import unittest\nimport shotgun_api3\n'
    'class TestShotgunApi(unittest.TestCase):\n'
    '    def test_deprecated_functions(self):\n'
    '        raise AssertionError("unadapted alias expectation")\n'
    'class TestExportPage(unittest.TestCase):\n'
    '    def test_export_page_format_missing(self):\n'
    '        raise AssertionError("unadapted message expectation")\n'
    '    def test_export_page_missing_page_id(self):\n'
    '        raise AssertionError("unadapted message expectation")\n'
    'class TestFind(unittest.TestCase):\n'
    '    def test_zero_is_not_none(self):\n'
    '        field = "sg_api_tests_number_field"\n'
    '        self.assertEqual(field, self.expected_field)\n'
    'class TestReadAdditionalFilterPresets(unittest.TestCase):\n'
    '    def schema_field_create(self, entity, kind, label):\n'
    '        return label\n'
    '    def test_modify_visibility(self):\n'
    '        label = "Project Visibility Test"\n'
    '        created = self.schema_field_create("Asset", "text", "Project Visibility Test")\n'
    '        self.assertEqual(created, label)\n'
    '        self.assertEqual(label, self.expected_label)\n'
)


class _ContractClient:
    def __init__(self, defect: str) -> None:
        self.defect = defect
        self.calls: list[str] = []

    def export_page(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.calls.append('export_page')
        if self.defect == 'missing_error':
            return
        if self.defect == 'wrong_error':
            raise ValueError('invalid export')
        raise ShotgunError('candidate-specific validation text')

    def schema_entity_read(self) -> dict[str, object]:
        return {'Asset': {}}

    def entity_types(self) -> dict[str, object]:
        self.calls.append('entity_types')
        if self.defect != 'missing_warning':
            warnings.warn('deprecated alias', DeprecationWarning, stacklevel=2)
        return {} if self.defect == 'wrong_alias' else self.schema_entity_read()

    def schema_field_read(self, entity: str) -> dict[str, object]:
        assert entity == 'Asset'
        return {'code': {}}

    def schema(self, entity: str) -> dict[str, object]:
        self.calls.append('schema')
        warnings.warn('deprecated alias', DeprecationWarning, stacklevel=2)
        return self.schema_field_read(entity)


class _ContractCase(unittest.TestCase):
    sg: _ContractClient


class _UserClient:
    def __init__(self, users: list[dict[str, object]]) -> None:
        self.users = users
        self.calls: list[tuple[str, list[list[object]], list[str]]] = []

    def find(
        self, entity_type: str, filters: list[list[object]], fields: list[str]
    ) -> list[dict[str, object]]:
        self.calls.append((entity_type, filters, fields))
        return self.users


def test_configuration_uses_existing_user_and_preserves_literal_secrets(tmp_path: Path) -> None:
    client = _UserClient([{'id': 123, 'name': 'private-person@example.invalid'}])
    environment = {
        'SG_TEST_SITE': 'https://example.invalid',
        'SG_TEST_SCRIPT': 'fixture-script',
        'SG_TEST_KEY': 'script%secret',
        'SG_TEST_HUMAN_LOGIN': 'fixture-login',
        'SG_TEST_HUMAN_PASSWORD': 'human%secret\nwith a second line',
    }
    path = tmp_path / 'config'

    setup._configure_user(client, path, environment)

    assert client.calls == [('HumanUser', [['login', 'is', 'fixture-login']], ['name'])]
    parser = configparser.ConfigParser()
    parser.read(path)
    assert parser['TEST_DATA']['human_name'] == 'private-person@example.invalid'
    assert parser['TEST_DATA']['human_login'] == 'fixture-login'
    assert parser['TEST_DATA']['human_password'] == environment['SG_TEST_HUMAN_PASSWORD']
    assert parser['SERVER_INFO']['api_key'] == environment['SG_TEST_KEY']
    if os.name == 'posix':
        assert path.stat().st_mode & 0o777 == 0o600


def test_contract_adaptations_reject_a_missing_test() -> None:
    with pytest.raises(RuntimeError, match='expectation'):
        setup._adapt_expectations(ast.parse('class TestMissing: pass'))


@pytest.mark.parametrize(
    'defect', ['', 'missing_warning', 'wrong_alias', 'missing_error', 'wrong_error']
)
def test_contract_adaptations_still_detect_behavior_failures(tmp_path: Path, defect: str) -> None:
    tree = ast.parse(_API_SOURCE)
    setup._adapt_expectations(tree)
    path = tmp_path / 'adapted.py'
    path.write_text(ast.unparse(tree))
    namespace = runpy.run_path(str(path))
    client = _ContractClient(defect)
    result = unittest.TestResult()
    for name in ('TestShotgunApi', 'TestExportPage'):
        case_type = cast('type[unittest.TestCase]', namespace[name])
        for case in unittest.defaultTestLoader.loadTestsFromTestCase(case_type):
            assert isinstance(case, unittest.TestCase)
            cast('_ContractCase', case).sg = client
            case.run(result)
    assert result.testsRun == 3
    assert result.wasSuccessful() is (not defect)
    assert not result.skipped
    assert client.calls.count('export_page') >= 2


def test_live_fixture_reuses_login_without_creating_a_user(tmp_path: Path) -> None:
    tree = ast.parse(
        'class LiveTestBase:\n'
        '    @classmethod\n'
        '    def _setup_db(cls, config, sg):\n'
        "        cls.human_user = sg.create('HumanUser', {})\n"
    )
    setup._adapt_fixtures('base.py', tree)
    path = tmp_path / 'base.py'
    path.write_text(ast.unparse(tree))
    path.write_text(
        path.read_text() + '\nfrom types import SimpleNamespace\n'
        'class Client:\n'
        '    def find_one(self, entity, filters, fields):\n'
        "        assert entity == 'HumanUser'\n"
        "        assert filters == [['login', 'is', 'fixture-login']]\n"
        "        return {'id': 1, 'name': 'private-name@example.invalid'}\n"
        "LiveTestBase.config = SimpleNamespace(human_login='fixture-login')\n"
        'LiveTestBase._setup_db(None, Client())\n'
        "assert LiveTestBase.human_user['id'] == 1\n",
    )
    runpy.run_path(str(path))


@pytest.mark.parametrize(
    'users',
    [[], [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}], [{'id': 1, 'name': None}]],
)
def test_missing_or_ambiguous_user_fails_without_creating_config(
    tmp_path: Path, users: list[dict[str, object]]
) -> None:
    client = _UserClient(users)
    path = tmp_path / 'config'

    with pytest.raises(RuntimeError, match='configured test user'):
        setup._configure_user(client, path, {'SG_TEST_HUMAN_LOGIN': 'private@example.invalid'})

    assert not path.exists()


def test_preparation_installs_restoration_and_contract_adaptations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = tmp_path / 'test_api.py'
    api.write_text(_API_SOURCE)
    monkeypatch.setattr(setup, '_EXCLUSIONS', {})
    monkeypatch.setattr(
        setup, '_UPSTREAM_HASHES', {'test_api.py': hashlib.sha256(api.read_bytes()).hexdigest()}
    )

    setup._prepare_sources(tmp_path)

    runtime = Path(setup.__file__).with_name('_conformance_runtime.py')
    assert (tmp_path / 'conftest.py').read_bytes() == runtime.read_bytes()
    assert 'self.assertWarns(DeprecationWarning)' in api.read_text()
    assert 'unadapted' not in api.read_text()
    assert 'self._conformance_number_field' in api.read_text()
    assert 'self._conformance_visibility_label' in api.read_text()
    assert 'sg_api_tests_number_field' not in api.read_text()
    assert 'Project Visibility Test' not in api.read_text()


def test_preparation_keeps_public_test_and_skips_only_named_internal_test(tmp_path: Path) -> None:
    source = (
        'import unittest\n'
        'from shotgun_api3.lib.httplib2 import Http\n'
        'class TestExample(unittest.TestCase):\n'
        '    def test_public(self):\n'
        '        self.assertEqual(2 + 2, 4)\n'
        '    def test_internal(self):\n'
        '        raise AssertionError("must not run")\n'
    )
    tree = setup._adapt_module(source, {'TestExample.test_internal': 'legacy transport internals'})
    path = tmp_path / 'example.py'
    path.write_text(ast.unparse(tree))
    namespace = runpy.run_path(str(path))
    case = cast('type[unittest.TestCase]', namespace['TestExample'])
    result = unittest.TestResult()
    case('test_public').run(result)
    assert result.wasSuccessful()
    assert not result.skipped
    result = unittest.TestResult()
    case('test_internal').run(result)
    assert result.skipped[0][1] == 'legacy transport internals'
    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == 'shotgun_api3.lib.httplib2'
        for node in ast.walk(tree)
    )


def test_preparation_rejects_unexpected_existing_fixtures(tmp_path: Path) -> None:
    original = 'existing fixture must survive\n'
    conftest = tmp_path / 'conftest.py'
    conftest.write_text(original)

    with pytest.raises(RuntimeError, match='Unexpected upstream conformance fixtures'):
        setup._prepare_sources(tmp_path)

    assert conftest.read_text() == original


@pytest.mark.parametrize('wrong_input', [False, True])
def test_schema_adaptation_uses_fixture_names_and_preserves_assertions(
    tmp_path: Path, wrong_input: bool
) -> None:
    tree = ast.parse(_API_SOURCE)
    setup._adapt_schema_fixtures(tree)
    path = tmp_path / 'schema.py'
    path.write_text(ast.unparse(tree))
    namespace = runpy.run_path(str(path))
    result = unittest.TestResult()
    cases = (
        ('TestFind', 'test_zero_is_not_none', '_conformance_number_field', 'expected_field'),
        (
            'TestReadAdditionalFilterPresets',
            'test_modify_visibility',
            '_conformance_visibility_label',
            'expected_label',
        ),
    )
    for class_name, method, attribute, expected in cases:
        case_type = cast('type[unittest.TestCase]', namespace[class_name])
        case = case_type(method)
        setattr(case, attribute, 'fixture chosen name')
        setattr(case, expected, 'wrong name' if wrong_input else 'fixture chosen name')
        case.run(result)
    assert result.testsRun == 2
    assert result.wasSuccessful() is (not wrong_input)
    assert not result.skipped
    if wrong_input:
        assert len(result.failures) == 2


@pytest.mark.parametrize(
    'source',
    ['class TestMissing: pass', _API_SOURCE.replace('Project Visibility Test', 'Changed Label')],
    ids=['missing-test', 'changed-input'],
)
def test_schema_adaptation_rejects_drift(source: str) -> None:
    with pytest.raises(RuntimeError, match='schema fixture'):
        setup._adapt_schema_fixtures(ast.parse(source))


def test_setup_failure_hides_raw_client_details(tmp_path: Path) -> None:
    # Fail before credentials are read or any site operation can occur.
    site = tmp_path / 'site-packages'
    package = site / 'shotgun_api3'
    package.mkdir(parents=True)
    (package / '__init__.py').write_text(
        "raise RuntimeError('private-person@example.invalid secret-key')\n",
    )
    result = subprocess.run(
        [sys.executable, '-I', str(Path(setup.__file__)), str(site), str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert 'Conformance setup failed' in result.stderr
    assert 'private-person' not in result.stderr + result.stdout
    assert 'secret-key' not in result.stderr + result.stdout


def test_source_drift_fails_before_rewriting_any_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / 'first.py'
    first.write_bytes(b'import os\n')
    second = tmp_path / 'second.py'
    second.write_bytes(b'changed\n')
    monkeypatch.setattr(
        setup,
        '_UPSTREAM_HASHES',
        {'first.py': hashlib.sha256(first.read_bytes()).hexdigest(), 'second.py': '0' * 64},
    )

    with pytest.raises(RuntimeError, match='digest mismatch'):
        setup._prepare_sources(tmp_path)

    assert first.read_bytes() == b'import os\n'
    assert second.read_bytes() == b'changed\n'


def test_stale_exclusion_is_rejected() -> None:
    with pytest.raises(RuntimeError, match='exclusion'):
        setup._adapt_module('class TestExample:\n    pass\n', {'TestExample.test_gone': 'reason'})


def test_sudo_version_is_deleted_even_after_a_failed_assertion(tmp_path: Path) -> None:
    source = (
        'import unittest\n'
        'class Client:\n'
        '    def create(self, *args, **kwargs):\n'
        '        self.created = True\n'
        "        return {'id': 17}\n"
        '    def delete(self, entity, identifier):\n'
        "        assert self.created and (entity, identifier) == ('Version', 17)\n"
        '        self.deleted = True\n'
        '    def close(self):\n'
        '        self.closed = True\n'
        'client = Client()\n'
        'class TestScriptUserSudoAuth(unittest.TestCase):\n'
        '    sg = client\n'
        '    def test_user_is_creator(self):\n'
        '        x = client\n'
        "        version = x.create('Version', {})\n"
        "        self.fail('injected assertion failure')\n"
    )
    tree = ast.parse(source)
    setup._adapt_fixtures('test_api.py', tree)
    path = tmp_path / 'sudo.py'
    path.write_text(ast.unparse(tree))
    namespace = runpy.run_path(str(path))
    case = cast('type[unittest.TestCase]', namespace['TestScriptUserSudoAuth'])
    result = unittest.TestResult()
    case('test_user_is_creator').run(result)
    assert len(result.failures) == 1
    assert getattr(namespace['client'], 'deleted', False)
    assert getattr(namespace['client'], 'closed', False)


def test_unknown_import_remains_a_collection_error(tmp_path: Path) -> None:
    tree = setup._adapt_module('import shotgun_api3.lib.unexpected\n', {})
    path = tmp_path / 'example.py'
    path.write_text(ast.unparse(tree))
    with pytest.raises(ModuleNotFoundError):
        runpy.run_path(str(path))
