from __future__ import annotations

import ast
import importlib
import sys
from email.message import Message
from importlib.metadata import PackagePath
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest

from scripts import run_installed_tests as _runner
from shotgun_api3._version import COMPATIBILITY_VERSION, PACKAGE_VERSION

if TYPE_CHECKING:
    from collections.abc import Iterator
    from importlib.metadata import Distribution, PackageMetadata

    from scripts.run_installed_tests import _Arguments, _Mode


class _DistributionOriginStub:
    def __init__(self, origin: Path) -> None:
        self.files = [
            PackagePath('shotgun_api3/py.typed'),
            PackagePath('sg_python_api-9.9.9.dist-info/METADATA'),
        ]
        self._origin = origin

    def locate_file(self, path: object) -> Path:
        return self._origin / str(path)


class _MainStageRecorder:
    def __init__(
        self,
        arguments: _Arguments,
        checkout: Path,
        purelib: Path,
    ) -> None:
        self._arguments = arguments
        self._checkout = checkout
        self.events: list[str] = []
        self._package = ModuleType('shotgun_api3')
        self._purelib = purelib

    def assert_async_import(self, package: ModuleType, purelib: Path) -> None:
        assert package is self._package
        assert purelib == self._purelib
        self.events.append('async-import')

    def assert_base_import(self, purelib: Path) -> ModuleType:
        assert purelib == self._purelib
        self.events.append('base-import')
        return self._package

    def assert_metadata(self, purelib: Path) -> None:
        assert purelib == self._purelib
        self.events.append('metadata')

    def assert_public_imports(self, package: ModuleType, purelib: Path) -> None:
        assert package is self._package
        assert purelib == self._purelib
        self.events.append('public-imports')

    def configure_import_path(self, purelib: Path, checkout: Path) -> None:
        assert purelib == self._purelib
        assert checkout == self._checkout
        self.events.append('configure-import-path')

    def import_module(self, name: str) -> ModuleType:
        if name == 'urllib3':
            self.events.append('import-urllib3')
            return ModuleType(name)
        if name == 'shotgun_api3':
            self.events.append('import-package')
            return self._package
        raise AssertionError(f'unexpected import: {name}')

    def purge_modules(self, root: str) -> None:
        assert root == 'shotgun_api3'
        self.events.append('purge-package')

    def run_pytest(
        self,
        tests: Path,
        mode: _Mode,
        checkout: Path,
        purelib: Path,
    ) -> None:
        assert tests == self._arguments.tests
        assert mode == self._arguments.mode
        assert checkout == self._checkout
        assert purelib == self._purelib
        self.events.append('pytest')

    def validate_paths(self, arguments: _Arguments, purelib: Path) -> Path:
        assert arguments == self._arguments
        assert purelib == self._purelib
        self.events.append('validate-paths')
        return self._checkout


def _arguments(mode: _Mode, target: Path, tests: Path) -> _Arguments:
    return _runner._Arguments(mode=mode, target=target, tests=tests)


def _package_metadata(
    *,
    extras: tuple[str, ...] = ('async',),
    requirements: tuple[str, ...] = (
        'urllib3<3,>=2',
        "httpx2<3,>=2; extra == 'async'",
    ),
) -> PackageMetadata:
    metadata = Message()
    for extra in extras:
        metadata['Provides-Extra'] = extra
    for requirement in requirements:
        metadata['Requires-Dist'] = requirement
    return cast('PackageMetadata', metadata)


def _write_synthetic_package(purelib: Path, package_source: str) -> None:
    package = purelib / 'shotgun_api3'
    mockgun_package = package / 'lib'
    mockgun_package.mkdir(parents=True)
    (package / '__init__.py').write_text(package_source, encoding='utf-8')
    (mockgun_package / '__init__.py').write_text('', encoding='utf-8')
    (mockgun_package / 'mockgun.py').write_text(
        "__all__ = ['AsyncShotgun', 'MockgunError', 'Shotgun', 'generate_schema']\n",
        encoding='utf-8',
    )


@pytest.fixture
def isolated_package_import_state() -> Iterator[None]:
    prefixes = ('httpx2', 'shotgun_api3')
    saved_modules = {
        name: module
        for name, module in sys.modules.items()
        if name in prefixes or name.startswith(('httpx2.', 'shotgun_api3.'))
    }
    saved_path = sys.path[:]
    saved_meta_path = sys.meta_path[:]
    try:
        yield
    finally:
        for name in tuple(sys.modules):
            if name in prefixes or name.startswith(('httpx2.', 'shotgun_api3.')):
                del sys.modules[name]
        sys.modules.update(saved_modules)
        sys.path[:] = saved_path
        sys.meta_path[:] = saved_meta_path


@pytest.fixture
def nested_interpreter_sites(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, Path]:
    environment = tmp_path / 'checkout' / '.venv'
    sites = {
        'purelib': environment / 'lib' / 'python' / 'site-packages',
        'platlib': environment / 'lib64' / 'python' / 'site-packages',
    }
    for path in sites.values():
        path.mkdir(parents=True)

    def interpreter_site_path(name: str) -> str:
        return str(sites[name])

    monkeypatch.setattr(_runner.sysconfig, 'get_path', interpreter_site_path)
    return sites


def test_async_validation_requires_explicit_async_export(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    purelib = tmp_path / 'target-purelib'
    purelib.mkdir()
    package = ModuleType('shotgun_api3')
    imported_names: list[str] = []

    def import_module(name: str) -> ModuleType:
        imported_names.append(name)
        return ModuleType(name)

    monkeypatch.setattr(importlib, 'import_module', import_module)

    with pytest.raises(AttributeError, match='AsyncShotgun'):
        _runner._assert_async_import(package, purelib)

    assert imported_names == ['anyio', 'httpx2']


@pytest.mark.parametrize(
    'extra_path',
    [None, '.', 'src', 'vendor/site-packages', '.venv/lib/python/site-packages/child'],
)
def test_checkout_assertion_allows_only_active_interpreter_sites(
    monkeypatch: pytest.MonkeyPatch,
    nested_interpreter_sites: dict[str, Path],
    tmp_path: Path,
    extra_path: str | None,
) -> None:
    checkout = tmp_path / 'checkout'
    paths = [str(path) for path in nested_interpreter_sites.values()]
    if extra_path is not None:
        paths.append(str(checkout / extra_path))
    monkeypatch.setattr(sys, 'path', paths)

    if extra_path is None:
        _runner._assert_checkout_absent(checkout)
    else:
        with pytest.raises(AssertionError, match='checkout paths remain'):
            _runner._assert_checkout_absent(checkout)


@pytest.mark.parametrize('node', _runner._CHECKOUT_TEST_EXCLUSIONS)
def test_checkout_exclusions_resolve_to_existing_tests(node: str) -> None:
    relative_path, separator, name = node.partition('::')
    path = Path(__file__).parents[1] / relative_path

    assert path.is_file(), node
    if separator:
        definitions = ast.parse(path.read_text(encoding='utf-8')).body
        assert any(
            isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef))
            and definition.name == name
            for definition in definitions
        ), node


@pytest.mark.parametrize('site_kind', ['purelib', 'platlib'])
def test_configure_import_path_preserves_nested_dependencies_without_source(
    isolated_package_import_state: None,
    monkeypatch: pytest.MonkeyPatch,
    nested_interpreter_sites: dict[str, Path],
    tmp_path: Path,
    site_kind: str,
) -> None:
    del isolated_package_import_state
    checkout = tmp_path / 'checkout'
    purelib = tmp_path / 'target-purelib'
    dependency_site = nested_interpreter_sites[site_kind]
    _write_synthetic_package(purelib, 'import httpx2\n')
    dependency = dependency_site / 'httpx2.py'
    dependency.write_text('', encoding='utf-8')
    forbidden_paths = [checkout, checkout / 'src', checkout / 'vendor' / 'site-packages']
    for path in forbidden_paths:
        _write_synthetic_package(path, "raise AssertionError('checkout package imported')\n")
    _write_synthetic_package(
        dependency_site, "raise AssertionError('tooling package masked target')\n"
    )
    monkeypatch.setattr(
        sys,
        'path',
        [
            *(str(path) for path in forbidden_paths),
            *(str(path) for path in nested_interpreter_sites.values()),
            str(purelib),
        ],
    )
    _runner._purge_modules('httpx2')
    _runner._purge_modules('shotgun_api3')

    _runner._configure_import_path(purelib, checkout)
    package = importlib.import_module('shotgun_api3')
    imported_dependency = importlib.import_module('httpx2')

    assert package.__file__ == str(purelib / 'shotgun_api3' / '__init__.py')
    assert imported_dependency.__file__ == str(dependency)
    assert not any(str(path) in sys.path for path in forbidden_paths)
    _runner._assert_checkout_absent(checkout)
    _runner._assert_import_origins(purelib)


def test_configure_import_path_removes_checkout_before_target_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout = tmp_path / 'checkout'
    checkout_source = checkout / 'src'
    purelib = tmp_path / 'target-purelib'
    system_path = tmp_path / 'system-path'
    checkout_source.mkdir(parents=True)
    purelib.mkdir()
    system_path.mkdir()
    # The real entry point initializes sysconfig before changing sys.path.
    _runner.sysconfig.get_path('purelib')
    monkeypatch.setattr(
        sys,
        'path',
        [str(checkout), str(system_path), str(purelib), str(checkout_source)],
    )

    _runner._configure_import_path(purelib, checkout)

    assert sys.path == [str(purelib), str(system_path)]


def test_distribution_origin_outside_target_purelib_is_rejected(tmp_path: Path) -> None:
    purelib = tmp_path / 'target-purelib'
    purelib.mkdir()
    outside = tmp_path / 'outside'
    distribution = cast('Distribution', _DistributionOriginStub(outside))

    with pytest.raises(AssertionError, match='distribution metadata is outside target'):
        _runner._assert_distribution_file_origin(distribution, purelib)


@pytest.mark.parametrize(
    ('module_name', 'expected'),
    [
        ('shotgun_api3', _runner._EXPECTED_PACKAGE_EXPORTS),
        ('shotgun_api3.shotgun', _runner._EXPECTED_SHOTGUN_EXPORTS),
        ('shotgun_api3.lib.mockgun', _runner._EXPECTED_MOCKGUN_EXPORTS),
        ('shotgun_api3.lib.mockgun.mockgun', _runner._EXPECTED_MOCKGUN_EXPORTS),
    ],
)
def test_expected_exports_match_checkout_modules(module_name: str, expected: list[str]) -> None:
    # The installed-wheel gate runs only at release; catch drift in ordinary checks.
    exports = importlib.import_module(module_name).__all__
    assert len(exports) == len(set(exports))
    assert set(exports) == set(expected)


def test_export_validation_accepts_reordering_but_rejects_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType('synthetic_exports')
    monkeypatch.setattr(module, '__all__', ['beta', 'alpha'], raising=False)
    monkeypatch.setattr(module, 'alpha', object(), raising=False)
    monkeypatch.setattr(module, 'beta', object(), raising=False)

    _runner._assert_exports(module, ['alpha', 'beta'])
    monkeypatch.setattr(module, '__all__', ['beta', 'alpha', 'beta'])
    with pytest.raises(AssertionError, match='public exports differ'):
        _runner._assert_exports(module, ['alpha', 'beta'])


def test_installed_dependency_contract_accepts_current_public_metadata() -> None:
    _runner._assert_declared_dependencies(_package_metadata())


def test_installed_dependency_contract_rejects_dev_extra() -> None:
    metadata = _package_metadata(
        extras=('async', 'dev'),
        requirements=(
            'urllib3<3,>=2',
            "httpx2<3,>=2; extra == 'async'",
            "pytest; extra == 'dev'",
        ),
    )

    with pytest.raises(AssertionError, match='declared extras differ'):
        _runner._assert_declared_dependencies(metadata)


def test_installed_project_url_parser_preserves_labels_and_values() -> None:
    metadata = Message()
    for label, url in _runner._EXPECTED_URLS.items():
        metadata['Project-URL'] = f'{label}, {url}'
    package_metadata = cast('PackageMetadata', metadata)

    assert _runner._project_urls(package_metadata) == _runner._EXPECTED_URLS


def test_package_origin_outside_target_purelib_is_rejected(
    isolated_package_import_state: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    del isolated_package_import_state
    purelib = tmp_path / 'target-purelib'
    purelib.mkdir()
    outside_package = tmp_path / 'checkout' / 'shotgun_api3' / '__init__.py'
    outside_package.parent.mkdir(parents=True)
    outside_package.write_text('', encoding='utf-8')
    for name in tuple(sys.modules):
        if name == 'shotgun_api3' or name.startswith('shotgun_api3.'):
            monkeypatch.delitem(sys.modules, name)
    package = ModuleType('shotgun_api3')
    package.__file__ = str(outside_package)
    monkeypatch.setitem(sys.modules, 'shotgun_api3', package)

    with pytest.raises(AssertionError, match='shotgun_api3 imported outside target'):
        _runner._assert_import_origins(purelib)


def test_public_import_validation_rejects_missing_typing_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    purelib = tmp_path / 'target-purelib'
    (purelib / 'shotgun_api3').mkdir(parents=True)
    package = ModuleType('shotgun_api3')
    monkeypatch.setattr(package, '__version__', COMPATIBILITY_VERSION, raising=False)
    imported_module = ModuleType('installed-public-module')

    def ignore_exports(*_args: object) -> None:
        return None

    def import_public_module(_name: str) -> ModuleType:
        return imported_module

    monkeypatch.setattr(_runner, '_assert_exports', ignore_exports)
    monkeypatch.setattr(importlib, 'import_module', import_public_module)

    with pytest.raises(AssertionError, match='installed typing marker is missing'):
        _runner._assert_public_imports(package, purelib)


def test_public_import_validation_requires_compatibility_version(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # The package version belongs to the distribution metadata, not __version__.
    package = ModuleType('shotgun_api3')
    monkeypatch.setattr(package, '__version__', PACKAGE_VERSION, raising=False)

    with pytest.raises(AssertionError, match='import version differs'):
        _runner._assert_public_imports(package, tmp_path)


def test_tests_importing_checkout_scripts_have_whole_file_exclusions() -> None:
    tests = Path(__file__).parents[1]
    excluded_files = {node for node in _runner._CHECKOUT_TEST_EXCLUSIONS if '::' not in node}
    missing: list[str] = []
    for path in tests.rglob('test_*.py'):
        tree = ast.parse(path.read_text(encoding='utf-8'))
        imports_scripts = any(
            (
                isinstance(node, ast.ImportFrom)
                and node.module is not None
                and (node.module == 'scripts' or node.module.startswith('scripts.'))
            )
            or (
                isinstance(node, ast.Import)
                and any(
                    alias.name == 'scripts' or alias.name.startswith('scripts.')
                    for alias in node.names
                )
            )
            for node in ast.walk(tree)
        )
        relative_path = path.relative_to(tests).as_posix()
        if imports_scripts and relative_path not in excluded_files:
            missing.append(relative_path)

    assert not missing, (
        f'tests importing checkout scripts need whole-file exclusions: {sorted(missing)}'
    )


@pytest.mark.parametrize(
    ('missing', 'expected_error'),
    [
        pytest.param('target', 'target prefix does not exist', id='target'),
        pytest.param('tests', 'tests directory does not exist', id='tests'),
        pytest.param('purelib', 'target purelib does not exist', id='purelib'),
        pytest.param('package', 'installed package does not exist', id='package'),
    ],
)
def test_validate_paths_rejects_missing_installed_layout_component(
    tmp_path: Path,
    missing: str,
    expected_error: str,
) -> None:
    target = tmp_path / 'target'
    tests = tmp_path / 'checkout' / 'tests'
    purelib = tmp_path / 'target-purelib'
    package = purelib / 'shotgun_api3'
    if missing != 'target':
        target.mkdir()
    if missing != 'tests':
        tests.mkdir(parents=True)
    if missing != 'purelib':
        purelib.mkdir()
    if missing not in {'package', 'purelib'}:
        package.mkdir()
    arguments = _arguments('base', target, tests)

    with pytest.raises(AssertionError, match=expected_error):
        _runner._validate_paths(arguments, purelib)


@pytest.mark.parametrize(
    ('package_source', 'expected_error'),
    [
        pytest.param(
            'try:\n    import httpx2\nexcept ModuleNotFoundError:\n    pass\n',
            'base import discovered httpx2',
            id='async-dependency-discovery',
        ),
        pytest.param(
            "import sys\nimport types\nsys.modules['shotgun_api3._async_transport'] = "
            "types.ModuleType('shotgun_api3._async_transport')\n",
            'base import loaded async modules',
            id='loaded-async-module',
        ),
    ],
)
def test_base_import_rejects_async_runtime_activity(
    isolated_package_import_state: None,
    tmp_path: Path,
    package_source: str,
    expected_error: str,
) -> None:
    del isolated_package_import_state
    purelib = tmp_path / 'target-purelib'
    _write_synthetic_package(purelib, package_source)
    sys.path.insert(0, str(purelib))

    with pytest.raises(AssertionError, match=expected_error):
        _runner._assert_base_import(purelib)


@pytest.mark.parametrize(
    ('mode', 'expected_events'),
    [
        pytest.param(
            'base',
            [
                'validate-paths',
                'configure-import-path',
                'metadata',
                'import-urllib3',
                'base-import',
                'public-imports',
                'pytest',
            ],
            id='base',
        ),
        pytest.param(
            'async',
            [
                'validate-paths',
                'configure-import-path',
                'metadata',
                'import-urllib3',
                'purge-package',
                'import-package',
                'async-import',
                'public-imports',
                'pytest',
            ],
            id='async',
        ),
    ],
)
def test_main_validates_isolation_imports_and_mode_before_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: _Mode,
    expected_events: list[str],
) -> None:
    target = tmp_path / 'target'
    checkout = tmp_path / 'checkout'
    tests = checkout / 'tests'
    purelib = tmp_path / 'target-purelib'
    arguments = _arguments(mode, target, tests)
    recorder = _MainStageRecorder(arguments, checkout, purelib)

    def purelib_for_prefix(_target: Path) -> Path:
        return purelib

    monkeypatch.setattr(_runner, '_parse_args', lambda: arguments)
    monkeypatch.setattr(_runner, '_purelib_for_prefix', purelib_for_prefix)
    monkeypatch.setattr(_runner, '_validate_paths', recorder.validate_paths)
    monkeypatch.setattr(_runner, '_configure_import_path', recorder.configure_import_path)
    monkeypatch.setattr(_runner, '_assert_metadata', recorder.assert_metadata)
    monkeypatch.setattr(importlib, 'import_module', recorder.import_module)
    monkeypatch.setattr(_runner, '_assert_base_import', recorder.assert_base_import)
    monkeypatch.setattr(_runner, '_purge_modules', recorder.purge_modules)
    monkeypatch.setattr(_runner, '_assert_async_import', recorder.assert_async_import)
    monkeypatch.setattr(_runner, '_assert_public_imports', recorder.assert_public_imports)
    monkeypatch.setattr(_runner, '_run_pytest', recorder.run_pytest)

    assert _runner._main() == 0
    assert recorder.events == expected_events


@pytest.mark.parametrize(
    'requirements',
    [
        pytest.param(
            ('urllib3<3,>=2,!=2.5', "httpx2<3,>=2; extra == 'async'"),
            id='added-constraint',
        ),
        pytest.param(
            (
                'urllib3<3,>=2',
                "httpx2<3,>=2; extra == 'async' and python_version >= '3.11'",
            ),
            id='compound-marker',
        ),
    ],
)
def test_installed_dependency_contract_rejects_altered_requirements(
    requirements: tuple[str, ...],
) -> None:
    with pytest.raises(AssertionError, match='declared dependencies differ'):
        _runner._assert_declared_dependencies(_package_metadata(requirements=requirements))


@pytest.mark.parametrize(
    ('mode', 'expression', 'require_no_skips'),
    [
        pytest.param(
            'async',
            'async_required and not live',
            True,
            id='async',
        ),
        pytest.param(
            'base',
            'not async_required and not live',
            False,
            id='base',
        ),
    ],
)
def test_pytest_arguments_preserve_installed_suite_boundary(
    tmp_path: Path,
    mode: _Mode,
    expression: str,
    *,
    require_no_skips: bool,
) -> None:
    tests = tmp_path / 'tests'
    config = tmp_path / 'pytest.ini'

    arguments = _runner._pytest_arguments(tests, config, mode)

    ignores = {argument for argument in arguments if argument.startswith('--ignore=')}
    assert {
        f'--ignore={tests / "tooling/test_artifact_evidence.py"}',
        f'--ignore={tests / "tooling/test_benchmark.py"}',
        f'--ignore={tests / "test_examples.py"}',
        f'--ignore={tests / "tooling/test_run_conformance.py"}',
        f'--ignore={tests / "tooling/test_run_installed_tests.py"}',
        f'--ignore={tests / "tooling/test_bootstrap_test_site.py"}',
        f'--ignore={tests / "tooling/test_live_standin.py"}',
        f'--ignore={tests / "tooling/test_parity_calendar.py"}',
        f'--ignore={tests / "tooling/test_sdist_contract.py"}',
        f'--ignore={tests / "tooling/test_validate_artifacts.py"}',
    } <= ignores
    deselections = {argument for argument in arguments if argument.startswith('--deselect=')}
    assert {
        '--deselect=tests/lib/test_mockgun_async.py::test_async_mockgun_finds_is_statically_integer',
        '--deselect=tests/test_signature_parity.py::test_result_annotations_preserve_only_validated_topology',
        '--deselect=tests/files/test_uploads.py::test_thumbnail_forwarding_static_contract',
        '--deselect=tests/test_config.py::test_config_static_surface_rejects_invalid_fields',
        '--deselect=tests/test_datetimes.py::test_transform_annotations_preserve_only_container_topology',
        '--deselect=tests/test_parity.py::test_client_method_static_surface_accepts_complete_contract',
        '--deselect=tests/test_parity.py::test_client_method_static_surface_rejects_invalid_calls',
        '--deselect=tests/test_parity.py::test_crud_static_surface_rejects_invalid_calls',
        '--deselect=tests/test_parity.py::test_supported_query_attachment_and_schedule_types',
        '--deselect=tests/test_signature_parity.py::test_single_any_argument_call_is_rejected_by_type_checker',
    } <= deselections
    assert arguments[arguments.index('-m') + 1] == expression
    assert ('--require-no-skips' in arguments) is require_no_skips
    assert arguments[-1] == str(tests)


@pytest.mark.parametrize('pytest_status', [0, 5])
def test_run_pytest_propagates_actual_pytest_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pytest_status: int
) -> None:
    fake_pytest = ModuleType('pytest')
    calls: list[list[str]] = []

    def main(arguments: list[str]) -> int:
        calls.append(arguments)
        return pytest_status

    monkeypatch.setattr(fake_pytest, 'main', main, raising=False)
    original_import = importlib.import_module

    def import_module(name: str) -> ModuleType:
        return fake_pytest if name == 'pytest' else original_import(name)

    def check_origin(_path: Path) -> None:
        pass

    monkeypatch.setattr(importlib, 'import_module', import_module)
    monkeypatch.setattr(_runner, '_assert_checkout_absent', check_origin)
    monkeypatch.setattr(_runner, '_assert_import_origins', check_origin)
    tests = tmp_path / 'tests'
    checkout = tmp_path / 'checkout'
    purelib = tmp_path / 'purelib'

    if pytest_status:
        with pytest.raises(SystemExit) as captured:
            _runner._run_pytest(tests, 'base', checkout, purelib)
        assert captured.value.code == pytest_status
    else:
        assert _runner._run_pytest(tests, 'base', checkout, purelib) is None
    assert len(calls) == 1
    assert calls[0][-1] == str(tests)
