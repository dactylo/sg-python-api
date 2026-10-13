#!/usr/bin/env python
"""Prepare private credentials and adapt the pinned upstream conformance tests."""

from __future__ import annotations

import ast
import configparser
import hashlib
import importlib
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from types import TracebackType

    from shotgun_api3 import Shotgun

_EXCLUSIONS: dict[str, dict[str, str]] = {
    'test_api.py': {
        'TestErrors.test_make_call_retry': 'patch unsupported shotgun.Http.request',
        'TestErrors.test_sanitized_auth_params': 'patch legacy urllib opener',
        'TestErrors.test_status_not_200': 'patch unsupported shotgun.Http.request',
        'TestExportPage.test_export_page_without_layout_name': (
            'patch unsupported shotgun.Http.request'
        ),
        'TestHumanUserAuth.test_humanuser_upload_thumbnail_for_version': (
            'direct unsupported httplib2.Http'
        ),
        'TestLibImports.test_import_httplib': 'asserts unsupported httplib2 import',
        'TestSessionTokenAuth.test_humanuser_upload_thumbnail_for_version': (
            'direct unsupported httplib2.Http'
        ),
        'TestShotgunApi.test_get_attachment_upload_info_thumbnail_with_created_at': (
            'private upload or routing method'
        ),
        'TestShotgunApi.test_get_attachment_upload_info_with_created_at': (
            'private upload or routing method'
        ),
        'TestShotgunApi.test_get_attachment_upload_info_without_created_at': (
            'private upload or routing method'
        ),
        'TestShotgunApi.test_json_dumps_default_ensure_ascii_disabled': 'private _http_request',
        'TestShotgunApi.test_linked_thumbnail_url': 'direct unsupported httplib2.Http',
        'TestShotgunApi.test_requires_direct_s3_upload': 'private upload or routing method',
        'TestShotgunApi.test_share_thumbnail_not_ready': 'private upload or routing method',
        'TestShotgunApi.test_share_thumbnail_returns_error': 'private upload or routing method',
        'TestShotgunApi.test_upload_thumbnail_for_task': 'direct unsupported httplib2.Http',
        'TestShotgunApi.test_upload_thumbnail_for_version': 'direct unsupported httplib2.Http',
        'TestShotgunApi.test_upload_thumbnail_in_create': 'direct unsupported httplib2.Http',
        'TestShotgunApi.test_upload_to_sg': 'private upload or routing method',
        'TestShotgunApi.test_upload_to_sg_with_created_at': 'private upload or routing method',
        'TestShotgunApi.test_upload_to_sg_without_created_at': 'private upload or routing method',
        'TestShotgunApi.test_upload_to_storage_with_created_at': (
            'private upload or routing method'
        ),
        'TestShotgunApi.test_upload_to_storage_without_created_at': (
            'private upload or routing method'
        ),
    },
    'test_client.py': {
        'TestShotgunClient.test_authorization': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_call_rpc': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_connect_close': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_decode_response_ascii': (
            'private protocol method plus legacy fixture'
        ),
        'TestShotgunClient.test_decode_response_unicode': (
            'private protocol method plus legacy fixture'
        ),
        'TestShotgunClient.test_detect_client_caps': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_detect_server_caps': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_encode_payload': 'private protocol method plus legacy fixture',
        'TestShotgunClient.test_extra_auth_params': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_http_error': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_localization_header_default': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_localization_header_when_localized': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_network_retry': 'httplib2.HttpLib2Error and private _http_request',
        'TestShotgunClient.test_parse_records': 'private protocol method plus legacy fixture',
        'TestShotgunClient.test_rpc_error': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_server_version_json': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_session_uuid': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_set_retry_interval': (
            'httplib2.HttpLib2Error and private _http_request'
        ),
        'TestShotgunClient.test_split_url': 'private protocol method plus legacy fixture',
        'TestShotgunClient.test_thumb_url': 'private protocol method plus legacy fixture',
        'TestShotgunClient.test_transform_data': 'private protocol method plus legacy fixture',
        'TestShotgunClient.test_upload_s3_500': 'private upload methods and retry constants',
        'TestShotgunClient.test_upload_s3_503': 'private upload methods and retry constants',
        'TestShotgunClient.test_upload_s3_urlerror__get_attachment_upload_info': (
            'private upload methods and retry constants'
        ),
        'TestShotgunClient.test_upload_s3_urlerror__upload_to_storage': (
            'private upload methods and retry constants'
        ),
        'TestShotgunClient.test_url': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
        'TestShotgunClient.test_user_agent': (
            'MockTestBase.setUp requires unsupported httplib2.Http and private connection mocks'
        ),
    },
    'test_unit.py': {
        'TestCerts.test_found_correct_cert': 'bundled certifi, httplib2, or private opener',
        'TestCerts.test_httplib': 'bundled certifi, httplib2, or private opener',
        'TestCerts.test_urlib': 'bundled certifi, httplib2, or private opener',
        'TestClientCapabilities.test_darwin': (
            'patches shotgun.sys implementation module; candidate capability class lives elsewhere'
        ),
        'TestClientCapabilities.test_linux': (
            'patches shotgun.sys implementation module; candidate capability class lives elsewhere'
        ),
        'TestClientCapabilities.test_no_platform': (
            'patches shotgun.sys implementation module; candidate capability class lives elsewhere'
        ),
        'TestClientCapabilities.test_py_version': (
            'patches shotgun.sys implementation module; candidate capability class lives elsewhere'
        ),
        'TestClientCapabilities.test_windows': (
            'patches shotgun.sys implementation module; candidate capability class lives elsewhere'
        ),
        'TestFilters.test_arrays': 'private filter and update translation methods',
        'TestFilters.test_empty': 'private filter and update translation methods',
        'TestFilters.test_invalid': 'private filter and update translation methods',
        'TestFilters.test_nested': 'private filter and update translation methods',
        'TestFilters.test_related_object': 'private filter and update translation methods',
        'TestFilters.test_related_object_entity_optimization_in': (
            'private filter and update translation methods'
        ),
        'TestFilters.test_related_object_entity_optimization_is': (
            'private filter and update translation methods'
        ),
        'TestFilters.test_related_object_update_entity': (
            'private filter and update translation methods'
        ),
        'TestFilters.test_related_object_update_optimization_entity': (
            'private filter and update translation methods'
        ),
        'TestFilters.test_related_object_update_optimization_entity_multi': (
            'private filter and update translation methods'
        ),
        'TestFilters.test_simple': 'private filter and update translation methods',
        'TestServerCapabilities.test_dev_version': (
            'uses server versions below documented 7.10.0 floor'
        ),
        'TestShotgunSummarize.test_filter_operator_all': (
            'private Shotgun._call_rpc patch helper; candidate has no such method'
        ),
        'TestShotgunSummarize.test_filter_operator_none': (
            'private Shotgun._call_rpc patch helper; candidate has no such method'
        ),
        'TestShotgunSummarize.test_filter_operator_or': (
            'private Shotgun._call_rpc patch helper; candidate has no such method'
        ),
        'TestShotgunSummarize.test_filters': (
            'private Shotgun._call_rpc patch helper; candidate has no such method'
        ),
        'TestShotgunSummarize.test_grouping': (
            'private Shotgun._call_rpc patch helper; candidate has no such method'
        ),
    },
}
_EXPECTATIONS = {
    'TestExportPage.test_export_page_format_missing': (
        'self.assertRaises(shotgun_api3.ShotgunError, self.sg.export_page, 11, None)\n'
        'self.assertRaises(shotgun_api3.ShotgunError, self.sg.export_page, 11, None, '
        "layout_name='Conformance')\n"
    ),
    'TestExportPage.test_export_page_missing_page_id': (
        "self.assertRaises(shotgun_api3.ShotgunError, self.sg.export_page, None, 'csv')\n"
        "self.assertRaises(shotgun_api3.ShotgunError, self.sg.export_page, None, 'csv', "
        "layout_name='Conformance')\n"
    ),
}
_SCHEMA_INPUTS = {
    'TestFind.test_zero_is_not_none': (
        'sg_api_tests_number_field',
        '_conformance_number_field',
        1,
    ),
    'TestReadAdditionalFilterPresets.test_modify_visibility': (
        'Project Visibility Test',
        '_conformance_visibility_label',
        2,
    ),
}
_UPSTREAM_HASHES = {
    'base.py': 'deffb01512c86df313b4ba82f63e3409b6ff024b11dc0607ecf17c4257b7d949',
    'test_api.py': '29aa14a0fff6f1d3e858f7ed9309d9085e61cceb34f7f9b7feea897d4bc1cc1c',
    'test_client.py': '61feca77a92245d7ac96265e5f52e9ea971d94d7b9ae0e71c9e56d0cacc2ffc3',
    'test_unit.py': 'ed35e0ff5bd20e32b8d19227159a453fa23428b0c0e10546ea3c9e5b3e3a732c',
}


class _SetupFailure:
    """Contain errors that may carry private site data or credentials."""

    def __init__(self) -> None:
        """Initialize the failure flag."""
        self.failed = False

    def __enter__(self) -> None:
        """Enter the private setup boundary."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Suppress ordinary exceptions while preserving process control."""
        del exc_type, traceback
        self.failed = exc is not None
        return isinstance(exc, Exception)


class _UserClient(Protocol):
    """Describe the read-only lookup needed to reuse a test user."""

    def find(
        self, entity_type: str, filters: list[list[object]], fields: list[str]
    ) -> list[dict[str, object]]:
        """Read matching records with the requested fields."""
        ...


def _adapt_expectations(tree: ast.Module) -> None:
    """Exercise documented aliases and error types without upstream message text."""
    remaining = set(_EXPECTATIONS)
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for method in node.body:
            if not isinstance(method, ast.FunctionDef):
                continue
            name = f'{node.name}.{method.name}'
            if name in _EXPECTATIONS:
                method.body = ast.parse(_EXPECTATIONS[name]).body
                remaining.remove(name)
    if remaining:
        raise RuntimeError('Pinned conformance expectation no longer matches a test')
    ast.fix_missing_locations(tree)


def _adapt_module(source: str, exclusions: dict[str, str]) -> ast.Module:
    """Remove exact legacy imports and mark enumerated tests before setup."""
    tree = ast.parse(source)
    remaining = set(exclusions)
    body: list[ast.stmt] = []
    for node in tree.body:
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == 'shotgun_api3.lib.httplib2'
            and [(name.name, name.asname) for name in node.names] == [('Http', None)]
        ) or (
            isinstance(node, ast.Import)
            and [(name.name, name.asname) for name in node.names]
            == [('shotgun_api3.lib.httplib2', 'httplib2')]
        ):
            continue
        if isinstance(node, ast.ClassDef):
            for method in node.body:
                if not isinstance(method, ast.FunctionDef):
                    continue
                name = f'{node.name}.{method.name}'
                if name in exclusions:
                    method.decorator_list.insert(
                        0,
                        ast.Call(
                            func=ast.Name(id='_sg_conformance_skip', ctx=ast.Load()),
                            args=[ast.Constant(value=exclusions[name])],
                            keywords=[],
                        ),
                    )
                    remaining.remove(name)
        body.append(node)
    if remaining:
        raise RuntimeError('Pinned conformance exclusion no longer matches a test')
    # Insert after any future imports and module docstring.
    index = 0
    for statement in body:
        if isinstance(statement, ast.Expr) or (
            isinstance(statement, ast.ImportFrom) and statement.module == '__future__'
        ):
            index += 1
        else:
            break
    body.insert(index, ast.parse('from unittest import skip as _sg_conformance_skip').body[0])
    tree.body = body
    return ast.fix_missing_locations(tree)


def _add_sudo_cleanup(method: ast.FunctionDef) -> None:
    """Register owned client and Version cleanup before upstream assertions."""
    remaining = {'x', 'version'}
    body: list[ast.stmt] = []
    for statement in method.body:
        body.append(statement)
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id == 'x':
            body.extend(ast.parse('self.addCleanup(x.close)').body)
            remaining.remove('x')
        elif target.id == 'version':
            body.extend(
                ast.parse("self.addCleanup(self.sg.delete, 'Version', version['id'])").body
            )
            remaining.remove('version')
    if remaining:
        raise RuntimeError('Pinned sudo fixture no longer matches')
    method.body = body


def _configure_user(client: _UserClient, path: Path, environment: dict[str, str]) -> None:
    """Resolve a unique existing login and write a private upstream config."""
    users = client.find(
        'HumanUser', [['login', 'is', environment['SG_TEST_HUMAN_LOGIN']]], ['name']
    )
    if len(users) != 1 or not isinstance(users[0].get('name'), str) or not users[0]['name']:
        raise RuntimeError('Could not resolve one configured test user')
    config = configparser.ConfigParser()
    sections = {
        'SERVER_INFO': {
            'server_url': environment['SG_TEST_SITE'],
            'script_name': environment['SG_TEST_SCRIPT'],
            'api_key': environment['SG_TEST_KEY'],
            'http_proxy': '',
            'session_uuid': '',
        },
        'TEST_DATA': {
            'project_name': 'sg-python-api conformance project',
            'human_name': str(users[0]['name']),
            'human_login': environment['SG_TEST_HUMAN_LOGIN'],
            'human_password': environment['SG_TEST_HUMAN_PASSWORD'],
            'asset_code': 'conformance asset',
            'version_code': 'conformance version',
            'shot_code': 'conformance shot',
            'task_content': 'conformance task',
            'playlist_code': 'conformance playlist',
        },
    }
    for section, values in sections.items():
        config[section] = {key: value.replace('%', '%%') for key, value in values.items()}
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
        config.write(stream)


def _replace_schema_inputs(method: ast.FunctionDef, literal: str, attribute: str) -> int:
    """Replace pinned assignment and call inputs with fixture-owned names."""
    count = 0

    def replace(value: ast.expr) -> ast.expr:
        nonlocal count
        if not isinstance(value, ast.Constant) or value.value != literal:
            return value
        count += 1
        return ast.Attribute(
            value=ast.Name(id='self', ctx=ast.Load()), attr=attribute, ctx=ast.Load()
        )

    for node in ast.walk(method):
        if isinstance(node, ast.Assign):
            node.value = replace(node.value)
        elif isinstance(node, ast.Call):
            node.args = [replace(value) for value in node.args]
    return count


def _adapt_schema_fixtures(tree: ast.Module) -> None:
    """Retain schema assertions while using fresh field names for each run."""
    remaining = set(_SCHEMA_INPUTS)
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for method in node.body:
            if not isinstance(method, ast.FunctionDef):
                continue
            name = f'{node.name}.{method.name}'
            if name not in _SCHEMA_INPUTS:
                continue
            literal, attribute, expected = _SCHEMA_INPUTS[name]
            if _replace_schema_inputs(method, literal, attribute) != expected:
                raise RuntimeError('Pinned conformance schema fixture inputs changed')
            remaining.remove(name)
    if remaining:
        raise RuntimeError('Pinned conformance schema fixture no longer matches a test')
    ast.fix_missing_locations(tree)


def _reuse_user_fixture(method: ast.FunctionDef) -> None:
    """Replace the pinned name-based user creation with a login lookup."""
    for index, statement in enumerate(method.body):
        if (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Attribute)
            and statement.targets[0].attr == 'human_user'
        ):
            method.body[index : index + 1] = ast.parse(
                "cls.human_user = sg.find_one('HumanUser', "
                "[['login', 'is', cls.config.human_login]], ['name'])\n"
                'if cls.human_user is None:\n'
                "    raise RuntimeError('Configured test user disappeared')\n"
            ).body
            return
    raise RuntimeError('Pinned HumanUser fixture no longer matches')


def _adapt_fixtures(module: str, tree: ast.Module) -> None:
    """Replace only the pinned user lookup and mock-only setup barriers."""
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if module == 'test_client.py' and node.name == 'TestShotgunClientInterface':
            node.bases = [
                ast.Attribute(
                    value=ast.Name(id='base', ctx=ast.Load()), attr='TestBase', ctx=ast.Load()
                )
            ]
        for method in node.body:
            if not isinstance(method, ast.FunctionDef):
                continue
            if (
                module == 'test_api.py'
                and node.name == 'TestScriptUserSudoAuth'
                and method.name == 'test_user_is_creator'
            ):
                _add_sudo_cleanup(method)
            if module == 'base.py' and node.name == 'LiveTestBase' and method.name == '_setup_db':
                _reuse_user_fixture(method)
            if (
                module == 'test_client.py'
                and node.name == 'TestShotgunClient'
                and method.name == 'setUp'
            ):
                method.body[:0] = ast.parse(
                    "if self._testMethodName in ('test_b64encode', 'test_read_config'):\n"
                    '    return\n'
                ).body
    ast.fix_missing_locations(tree)


def _prepare_sources(tests_root: Path) -> None:
    """Verify pinned sources before adapting disposable test files."""
    if (tests_root / 'conftest.py').exists():
        raise RuntimeError('Unexpected upstream conformance fixtures')
    sources: dict[str, str] = {}
    for module, digest in _UPSTREAM_HASHES.items():
        content = (tests_root / module).read_bytes()
        if hashlib.sha256(content).hexdigest() != digest:
            raise RuntimeError('Pinned conformance source digest mismatch')
        sources[module] = content.decode('utf-8')
    for module, source in sources.items():
        tree = _adapt_module(source, _EXCLUSIONS.get(module, {}))
        _adapt_fixtures(module, tree)
        if module == 'test_api.py':
            _adapt_expectations(tree)
            _adapt_schema_fixtures(tree)
        (tests_root / module).write_text(ast.unparse(tree) + '\n', encoding='utf-8')

    runtime = Path(__file__).with_name('_conformance_runtime.py')
    (tests_root / 'conftest.py').write_bytes(runtime.read_bytes())


def main() -> int:
    """Prepare the isolated suite without printing user data or server errors.

    Returns:
        Zero after setup, or one with a sanitized failure message.
    """
    site_packages = Path(sys.argv[1]).resolve()
    tests_root = Path(sys.argv[2]).resolve()
    sys.path.insert(0, str(site_packages))
    failure = _SetupFailure()
    with failure:
        module = importlib.import_module('shotgun_api3')
        origin = module.__file__
        if origin is None or not Path(origin).resolve().is_relative_to(site_packages):
            raise RuntimeError('Conformance setup imported a non-candidate client')
        _prepare_sources(tests_root)
        client_class = cast('type[Shotgun]', module.Shotgun)
        client = client_class(
            os.environ['SG_TEST_SITE'],
            script_name=os.environ['SG_TEST_SCRIPT'],
            api_key=os.environ['SG_TEST_KEY'],
            connect=False,
        )
        try:
            _configure_user(client, tests_root / 'config', dict(os.environ))
        finally:
            client.close()
    if failure.failed:
        print(
            'Conformance setup failed: check pinned tests and the configured existing user',
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
