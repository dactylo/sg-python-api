#!/usr/bin/env python
"""Validate and test an isolated installed `shotgun_api3` package.

Run this script with positional arguments `TARGET_PREFIX TESTS_DIR MODE`, where
`MODE` is `base` or `async`. The target prefix must already contain the
installed package and dependencies.

The entry point removes checkout source paths from import resolution, verifies
installed metadata and public import origins, and runs the applicable tier-1
tests. Live tests and declared checkout-only tests are excluded.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.abc
import importlib.metadata
import sys
import sysconfig
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Sequence
    from importlib.machinery import ModuleSpec
    from types import ModuleType

_CHECKOUT_TEST_EXCLUSIONS = {
    'lib/test_mockgun_async.py::test_async_mockgun_finds_is_statically_integer': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'lib/test_mockgun_async.py::test_async_mockgun_import_needs_no_httpx2': (
        'launches a subprocess against the checkout src directory'
    ),
    'client/test_async.py::test_constructor_static_contract_rejects_invalid_arguments': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'transport/test_async_httpx2.py::test_httpx2_is_imported_only_by_adapter': (
        'scans Python sources in the checkout src directory'
    ),
    'tooling/test_artifact_evidence.py': 'imports the checkout artifact-evidence helper',
    'tooling/test_conformance_runtime.py': 'imports the checkout conformance runtime helper',
    'tooling/test_conformance_setup.py': 'imports the checkout conformance setup helper',
    'tooling/test_benchmark.py': 'tests checkout benchmark script and evidence paths only',
    'client/test_crud.py::test_result_annotations_preserve_only_validated_topology': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'files/test_uploads.py::test_thumbnail_forwarding_static_contract': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_config.py::test_config_static_surface_rejects_invalid_fields': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_datetimes.py::test_transform_annotations_preserve_only_container_topology': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'transport/test_driver.py::test_only_attempt_boundaries_call_transport_send_in_production': (
        'scans Python sources in the checkout src directory'
    ),
    'test_examples.py': 'tests checkout-only public example programs',
    'client/test_filters.py::test_entity_optimization_flag_initializes_from_environment': (
        'launches a subprocess with the checkout src directory on PYTHONPATH'
    ),
    'test_init.py::test_async_shotgun_missing_extra_error_is_specific': (
        'launches a subprocess with the checkout src directory on PYTHONPATH'
    ),
    'test_init.py::test_async_shotgun_unrelated_import_failure_propagates': (
        'launches a subprocess with the checkout src directory on PYTHONPATH'
    ),
    'test_init.py::test_base_import_does_not_load_async_modules_without_extra': (
        'launches a subprocess with the checkout src directory on PYTHONPATH'
    ),
    'test_init.py::test_project_metadata': 'reads checkout pyproject metadata',
    'test_parity.py::test_client_method_static_surface_accepts_complete_contract': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_parity.py::test_client_method_static_surface_rejects_invalid_calls': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_parity.py::test_crud_static_surface_rejects_invalid_calls': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_parity.py::test_supported_query_attachment_and_schedule_types': (
        'runs pyright with the checkout pyproject configuration'
    ),
    'test_project_metadata.py': 'reads checkout pyproject metadata only',
    'tooling/test_required_mode.py': 'tests checkout-local test plugins and live-site scripts',
    'tooling/test_run_conformance.py': (
        'tests checkout conformance runner and archived artifact paths only'
    ),
    'tooling/test_run_installed_tests.py': 'imports and tests the checkout installed-test runner',
    'tooling/test_validate_artifacts.py': 'tests checkout artifact-validation scripts only',
    'tooling/test_wire_fixtures.py': 'imports and tests the checkout wire-probe script',
}
_DIST_NAME = 'sg-python-api'
_EXPECTED_DISTRIBUTION_FILES = {'shotgun_api3/py.typed'}
_EXPECTED_EXTRAS = {'async'}
_EXPECTED_MOCKGUN_EXPORTS = ['AsyncShotgun', 'MockgunError', 'Shotgun', 'generate_schema']
_EXPECTED_PACKAGE_EXPORTS = [
    'AuthenticationFault',
    'Error',
    'Fault',
    'MissingTwoFactorAuthenticationFault',
    'ProtocolError',
    'ResponseError',
    'Shotgun',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
    '__version__',
    'sg_timezone',
]
_EXPECTED_REQUIREMENTS = {
    "httpx2<3,>=2;extra=='async'",
    'urllib3<3,>=2',
}
_EXPECTED_SHOTGUN_EXPORTS = [
    'LOG',
    'SG_TIMEZONE',
    'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION',
    'AuthenticationFault',
    'ClientCapabilities',
    'Error',
    'Fault',
    'MissingTwoFactorAuthenticationFault',
    'ProtocolError',
    'ResponseError',
    'ServerCapabilities',
    'SgTimezone',
    'Shotgun',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForOxygenAuthenticationFault',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
    '__version__',
]
_EXPECTED_URLS = {
    'Documentation': 'https://github.com/dactylo/sg-python-api/tree/main/docs',
    'Homepage': 'https://github.com/dactylo/sg-python-api',
    'Issues': 'https://github.com/dactylo/sg-python-api/issues',
    'Repository': 'https://github.com/dactylo/sg-python-api',
}
_EXPECTED_VERSION = '0.1.2'
_PACKAGE_NAME = 'shotgun_api3'
_Mode = Literal['async', 'base']


class _Arguments(NamedTuple):
    """Hold validated command-line arguments."""

    mode: _Mode
    target: Path
    tests: Path


class _Httpx2Blocker(importlib.abc.MetaPathFinder):
    """Reject and record every attempt to discover `httpx2`."""

    def __init__(self) -> None:
        """Initialize a module blocker for the `httpx2` namespace."""
        self.lookups: list[str] = []

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        """Reject `httpx2` discovery and ignore all other modules.

        Returns:
            `None` for every unblocked module.

        Raises:
            ModuleNotFoundError: If `fullname` names `httpx2` or a submodule.
        """
        del path, target
        if fullname == 'httpx2' or fullname.startswith('httpx2.'):
            self.lookups.append(fullname)
            raise ModuleNotFoundError(
                'httpx2 blocked during base installed-package validation',
                name=fullname,
            )
        return None


class _PytestModule(Protocol):
    """Describe the pytest entry point used by this runner."""

    def main(self, args: Sequence[str]) -> int:
        """Run pytest with command-line arguments.

        Returns:
            Pytest process exit status.
        """
        ...


def _assert_declared_dependencies(metadata: importlib.metadata.PackageMetadata) -> None:
    """Require exact declared dependencies and extras."""
    extras = set(metadata.get_all('Provides-Extra') or [])
    if extras != _EXPECTED_EXTRAS:
        raise AssertionError(f'declared extras differ: {sorted(extras)}')
    requirements = {
        requirement.replace(' ', '').replace('"', "'")
        for requirement in metadata.get_all('Requires-Dist') or []
    }
    if requirements != _EXPECTED_REQUIREMENTS:
        raise AssertionError(f'declared dependencies differ: {sorted(requirements)}')


def _assert_distribution_file_origin(
    distribution: importlib.metadata.Distribution,
    purelib: Path,
) -> None:
    """Require a typing marker in the inventory and metadata under target purelib."""
    files = distribution.files
    if files is None:
        raise AssertionError('installed distribution has no file inventory')
    inventory = {str(path) for path in files}
    missing = _EXPECTED_DISTRIBUTION_FILES - inventory
    if missing:
        raise AssertionError(f'installed distribution lacks files: {sorted(missing)}')
    metadata_files = [
        path
        for path in files
        if path.name == 'METADATA' and path.parent.name.endswith('.dist-info')
    ]
    if len(metadata_files) != 1:
        raise AssertionError(f'expected one installed METADATA file: {metadata_files}')
    metadata_path = Path(str(distribution.locate_file(metadata_files[0]))).resolve()
    if not metadata_path.is_relative_to(purelib):
        raise AssertionError(f'distribution metadata is outside target: {metadata_path}')


def _assert_exports(
    module: ModuleType,
    expected: list[str],
) -> None:
    """Require one module's exact export list and importable public names."""
    if getattr(module, '__all__', None) != expected:
        raise AssertionError(f'{module.__name__} public exports differ')
    for name in expected:
        if not hasattr(module, name):
            raise AssertionError(f'{module.__name__} public import is missing: {name}')


def _assert_import_origins(purelib: Path) -> None:
    """Require all loaded `shotgun_api3` modules to come from target."""
    for name, module in tuple(sys.modules.items()):
        if name != _PACKAGE_NAME and not name.startswith(f'{_PACKAGE_NAME}.'):
            continue
        module_file = getattr(module, '__file__', None)
        if module_file is None:
            continue
        resolved = Path(module_file).resolve()
        if not resolved.is_relative_to(purelib):
            raise AssertionError(f'{name} imported outside target: {resolved}')


def _assert_async_import(package: ModuleType, purelib: Path) -> None:
    """Validate the installed async extra and public client export."""
    importlib.import_module('anyio')
    importlib.import_module('httpx2')
    async_client = cast('object', package.AsyncShotgun)
    if getattr(async_client, '__module__', None) != 'shotgun_api3._async_client':
        raise AssertionError('AsyncShotgun is not the installed public async client')
    importlib.import_module('shotgun_api3._async_httpx2')
    _assert_import_origins(purelib)


def _assert_public_imports(
    package: ModuleType,
    distribution: importlib.metadata.Distribution,
    purelib: Path,
) -> None:
    """Validate version, typing marker, and expected public imports."""
    if getattr(package, '__version__', None) != _EXPECTED_VERSION:
        raise AssertionError(f'import version differs: {getattr(package, "__version__", None)}')
    if getattr(package, '__version__', None) != distribution.version:
        raise AssertionError('import and distribution versions differ')
    _assert_exports(package, _EXPECTED_PACKAGE_EXPORTS)
    shotgun = importlib.import_module('shotgun_api3.shotgun')
    _assert_exports(shotgun, _EXPECTED_SHOTGUN_EXPORTS)
    mockgun = importlib.import_module('shotgun_api3.lib.mockgun')
    _assert_exports(mockgun, _EXPECTED_MOCKGUN_EXPORTS)
    typing_marker = purelib / _PACKAGE_NAME / 'py.typed'
    if not typing_marker.is_file():
        raise AssertionError(f'installed typing marker is missing: {typing_marker}')
    _assert_import_origins(purelib)


def _parse_args() -> _Arguments:
    """Parse the fixed installed-test runner interface."""
    parser = argparse.ArgumentParser()
    parser.add_argument('target', type=Path, metavar='TARGET_PREFIX')
    parser.add_argument('tests', type=Path, metavar='TESTS_DIR')
    parser.add_argument('mode', choices=('base', 'async'), metavar='MODE')
    namespace = parser.parse_args()
    return _Arguments(
        mode=cast('_Mode', namespace.mode),
        target=cast('Path', namespace.target).resolve(),
        tests=cast('Path', namespace.tests).resolve(),
    )


def _project_urls(metadata: importlib.metadata.PackageMetadata) -> dict[str, str]:
    """Parse and return labeled project URLs from distribution metadata."""
    urls: dict[str, str] = {}
    for entry in metadata.get_all('Project-URL') or []:
        label, separator, url = entry.partition(',')
        if not separator:
            raise AssertionError(f'invalid Project-URL metadata: {entry}')
        urls[label.strip()] = url.strip()
    return urls


def _assert_metadata(purelib: Path) -> importlib.metadata.Distribution:
    """Validate installed distribution metadata and its target origin."""
    candidates = [
        distribution
        for distribution in importlib.metadata.distributions(path=[str(purelib)])
        if distribution.metadata['Name'] == _DIST_NAME
    ]
    if len(candidates) != 1:
        raise AssertionError(f'expected one {_DIST_NAME} distribution, found {len(candidates)}')
    distribution = candidates[0]
    metadata = distribution.metadata
    if distribution.version != _EXPECTED_VERSION:
        raise AssertionError(f'distribution version differs: {distribution.version}')
    if metadata['Name'] != _DIST_NAME:
        raise AssertionError(f'distribution name differs: {metadata["Name"]}')
    if metadata['Requires-Python'] != '>=3.11':
        raise AssertionError(f'Python requirement differs: {metadata["Requires-Python"]}')
    if metadata['License-Expression'] != 'MIT' and metadata['License'] != 'MIT':
        raise AssertionError('license metadata is not MIT')
    urls = _project_urls(metadata)
    if urls != _EXPECTED_URLS:
        raise AssertionError(f'project URLs differ: {urls}')
    _assert_declared_dependencies(metadata)
    _assert_distribution_file_origin(distribution, purelib)
    return distribution


def _purelib_for_prefix(target: Path) -> Path:
    """Compute the target prefix's interpreter-specific purelib path."""
    return Path(
        sysconfig.get_path(
            'purelib',
            vars={'base': str(target), 'platbase': str(target)},
        )
    ).resolve()


def _purge_modules(root: str) -> None:
    """Remove a package and its child modules from the import cache."""
    for name in tuple(sys.modules):
        if name == root or name.startswith(f'{root}.'):
            del sys.modules[name]


def _assert_base_import(purelib: Path) -> ModuleType:
    """Prove base import neither discovers nor loads `httpx2`."""
    _purge_modules('httpx2')
    _purge_modules(_PACKAGE_NAME)
    blocker = _Httpx2Blocker()
    sys.meta_path.insert(0, blocker)
    try:
        package = importlib.import_module(_PACKAGE_NAME)
        if blocker.lookups:
            raise AssertionError(f'base import discovered httpx2: {blocker.lookups}')
        loaded_httpx2 = sorted(
            name for name in sys.modules if name == 'httpx2' or name.startswith('httpx2.')
        )
        if loaded_httpx2:
            raise AssertionError(f'base import loaded httpx2: {loaded_httpx2}')
        loaded_async = sorted(
            name for name in sys.modules if name.startswith('shotgun_api3._async')
        )
        if loaded_async:
            raise AssertionError(f'base import loaded async modules: {loaded_async}')
        mockgun = importlib.import_module('shotgun_api3.lib.mockgun')
        if getattr(mockgun, '__all__', None) != _EXPECTED_MOCKGUN_EXPORTS:
            raise AssertionError('mockgun public exports differ')
        if blocker.lookups:
            raise AssertionError(f'base public imports discovered httpx2: {blocker.lookups}')
    finally:
        sys.meta_path.remove(blocker)
    _assert_import_origins(purelib)
    return package


def _pytest_arguments(tests: Path, config: Path, mode: _Mode) -> list[str]:
    """Build the installed-artifact pytest command."""
    expression = (
        'async_required and not live' if mode == 'async' else 'not async_required and not live'
    )
    arguments = [
        '-c',
        str(config),
        '--rootdir',
        str(tests.parent),
        '--import-mode=importlib',
        '-m',
        expression,
    ]
    if mode == 'async':
        arguments.append('--require-no-skips')
    for node in _CHECKOUT_TEST_EXCLUSIONS:
        path, separator, test_name = node.partition('::')
        if separator:
            arguments.append(f'--deselect={tests.name}/{path}::{test_name}')
        else:
            arguments.append(f'--ignore={tests / path}')
    arguments.append(str(tests))
    return arguments


def _resolved_search_path(entry: str) -> Path:
    """Resolve one interpreter search path, including the empty cwd entry."""
    return Path(entry or '.').resolve()


def _checkout_source_paths(checkout: Path) -> list[str]:
    """Find checkout paths other than the active interpreter's dependency sites."""
    interpreter_sites = {
        Path(sysconfig.get_path(name)).resolve() for name in ('purelib', 'platlib')
    }
    return [
        entry
        for entry in sys.path
        if (path := _resolved_search_path(entry)).is_relative_to(checkout)
        and path not in interpreter_sites
    ]


def _assert_checkout_absent(checkout: Path) -> None:
    """Reject checkout source paths left on the interpreter search path."""
    checkout_paths = _checkout_source_paths(checkout)
    if checkout_paths:
        raise AssertionError(f'checkout paths remain on sys.path: {checkout_paths}')


def _configure_import_path(purelib: Path, checkout: Path) -> None:
    """Put target purelib before dependency paths and remove checkout source paths."""
    if purelib.is_relative_to(checkout):
        raise AssertionError(f'target purelib is inside checkout: {purelib}')
    checkout_paths = _checkout_source_paths(checkout)
    system_paths = [
        entry
        for entry in sys.path
        if _resolved_search_path(entry) != purelib and entry not in checkout_paths
    ]
    sys.path[:] = [str(purelib), *system_paths]
    _assert_checkout_absent(checkout)


def _run_pytest(tests: Path, mode: _Mode, checkout: Path, purelib: Path) -> None:
    """Run applicable tier-1 tests with checkout-only tests excluded."""
    with tempfile.TemporaryDirectory(prefix='sg-python-api-pytest-') as temp_dir:
        config = Path(temp_dir) / 'pytest.ini'
        config.write_text(
            '[pytest]\n'
            'addopts = -q --strict-markers --tb=short\n'
            'markers =\n'
            '    async_required: tests requiring the installed async extra\n'
            '    live: tests requiring a live test site\n'
            '    slow: tests that are slow\n',
            encoding='utf-8',
        )
        pytest = cast('_PytestModule', importlib.import_module('pytest'))
        result = pytest.main(_pytest_arguments(tests, config, mode))
    _assert_checkout_absent(checkout)
    _assert_import_origins(purelib)
    if result != 0:
        raise SystemExit(result)


def _validate_paths(args: _Arguments, purelib: Path) -> Path:
    """Validate the target, tests, and installed package directories."""
    if not args.target.is_dir():
        raise AssertionError(f'target prefix does not exist: {args.target}')
    if not args.tests.is_dir():
        raise AssertionError(f'tests directory does not exist: {args.tests}')
    if not purelib.is_dir():
        raise AssertionError(f'target purelib does not exist: {purelib}')
    package = purelib / _PACKAGE_NAME
    if not package.is_dir():
        raise AssertionError(f'installed package does not exist: {package}')
    return args.tests.parent


def main() -> int:
    """Validate an installed artifact and run its applicable tier-1 tests.

    Returns:
        Zero when metadata, imports, and tests pass.
    """
    args = _parse_args()
    purelib = _purelib_for_prefix(args.target)
    checkout = _validate_paths(args, purelib)
    _configure_import_path(purelib, checkout)
    distribution = _assert_metadata(purelib)
    importlib.import_module('urllib3')
    if args.mode == 'base':
        package = _assert_base_import(purelib)
    else:
        _purge_modules(_PACKAGE_NAME)
        package = importlib.import_module(_PACKAGE_NAME)
        _assert_async_import(package, purelib)
    _assert_public_imports(package, distribution, purelib)
    for node, reason in _CHECKOUT_TEST_EXCLUSIONS.items():
        print(f'excluding checkout-only test {node}: {reason}')
    _run_pytest(args.tests, args.mode, checkout, purelib)
    print(f'installed artifact validation OK ({args.mode})')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
