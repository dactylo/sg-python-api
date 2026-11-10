import importlib
import importlib.metadata
import os
import subprocess
import sys
import tomllib
import xmlrpc.client
from pathlib import Path

import pytest

import shotgun_api3
import shotgun_api3.shotgun as shotgun_module
from shotgun_api3 import AsyncShotgun as ExportedAsyncShotgun
from shotgun_api3 import _capabilities, _client, _errors, _protocol, _version
from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._version import COMPATIBILITY_VERSION, PACKAGE_VERSION
from shotgun_api3.lib.sgtimezone import SgTimezone

_BASE_IMPORT_SCRIPT = """
import importlib.abc
import importlib.util
import sys


class BlockHttpx2(importlib.abc.MetaPathFinder):
    def __init__(self):
        self.lookups = []

    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'httpx2' or fullname.startswith('httpx2.'):
            self.lookups.append(fullname)
            raise ModuleNotFoundError(
                'httpx2 blocked by base-import isolation test',
                name='httpx2',
            )
        return None


blocker = BlockHttpx2()
sys.meta_path.insert(0, blocker)
try:
    importlib.util.find_spec('httpx2')
except ModuleNotFoundError:
    pass
else:
    raise AssertionError('httpx2 discovery was not blocked')
if blocker.lookups != ['httpx2']:
    raise RuntimeError(f'httpx2 blocker recorded unexpected lookups: {blocker.lookups}')
blocker.lookups.clear()

import shotgun_api3

if blocker.lookups:
    raise RuntimeError(f'base import attempted httpx2 lookup: {blocker.lookups}')

star_namespace = {}
exec('from shotgun_api3 import *', star_namespace)
if 'AsyncShotgun' in star_namespace:
    raise RuntimeError('star import exposed the optional async class')
if blocker.lookups:
    raise RuntimeError(f'star import attempted httpx2 lookup: {blocker.lookups}')

loaded_async_modules = sorted(
    name for name in sys.modules if name.startswith('shotgun_api3._async')
)
if loaded_async_modules:
    raise RuntimeError(f'base import loaded async modules: {loaded_async_modules}')
"""
_PACKAGE_EXPORTS = [
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
_REPOSITORY_URL = 'https://github.com/dactylo/sg-python-api'
_SHOTGUN_MODULE_EXPORTS = [
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


def _run_base_import_script(
    *,
    cwd: Path,
    pythonpath: Path,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    pythonpath_value = str(pythonpath)
    if inherited_pythonpath := environment.get('PYTHONPATH'):
        pythonpath_value = os.pathsep.join((pythonpath_value, inherited_pythonpath))
    environment['PYTHONPATH'] = pythonpath_value
    return subprocess.run(
        [sys.executable, '-c', _BASE_IMPORT_SCRIPT],
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def test_async_shotgun_explicit_import() -> None:
    assert ExportedAsyncShotgun is AsyncShotgun


def test_async_shotgun_missing_extra_error_is_specific(tmp_path: Path) -> None:
    script = (
        _BASE_IMPORT_SCRIPT
        + """
try:
    shotgun_api3.AsyncShotgun
except ImportError as error:
    message = str(error)
    if 'httpx2' not in message or 'async' not in message:
        raise RuntimeError(f'unhelpful async-extra error: {message}') from error
else:
    raise RuntimeError('missing async extra did not fail')
"""
    )
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(Path(__file__).parents[1] / 'src')
    result = subprocess.run(
        [sys.executable, '-c', script],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_async_shotgun_unrelated_import_failure_propagates(tmp_path: Path) -> None:
    (tmp_path / 'httpx2.py').write_text("raise RuntimeError('broken dependency')\n")
    repository = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment['PYTHONPATH'] = os.pathsep.join((str(tmp_path), str(repository / 'src')))
    script = """
import shotgun_api3
try:
    shotgun_api3.AsyncShotgun
except RuntimeError as error:
    if str(error) != 'broken dependency':
        raise
else:
    raise RuntimeError('unrelated dependency failure was swallowed')
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_base_import_does_not_load_async_modules_without_extra() -> None:
    repository = Path(__file__).parents[1]
    result = _run_base_import_script(cwd=repository, pythonpath=repository / 'src')

    assert result.returncode == 0, result.stderr


def test_entity_optimization_flag_initializes_from_environment() -> None:
    repository = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(repository / 'src')
    environment['SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION'] = '1'
    source = (
        'from pathlib import Path; '
        'import shotgun_api3.shotgun as module; '
        "assert Path(module.__file__).resolve() == Path('src/shotgun_api3/shotgun.py').resolve(); "
        'assert module.SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION is True'
    )

    subprocess.run(
        [sys.executable, '-c', source],
        check=True,
        cwd=repository,
        env=environment,
    )


def test_exported_objects_preserve_identity() -> None:
    assert shotgun_api3.Shotgun is shotgun_module.Shotgun is _client.Shotgun
    assert shotgun_api3.sg_timezone is shotgun_module.SG_TIMEZONE is _protocol.SG_TIMEZONE
    assert isinstance(shotgun_api3.sg_timezone, SgTimezone)
    assert shotgun_module.ClientCapabilities is _capabilities.ClientCapabilities
    assert shotgun_module.ServerCapabilities is _capabilities.ServerCapabilities
    assert shotgun_module.SgTimezone is SgTimezone
    for name in (
        'AuthenticationFault',
        'Fault',
        'MissingTwoFactorAuthenticationFault',
        'ShotgunError',
        'ShotgunFileDownloadError',
        'ShotgunThumbnailNotReady',
        'UserCredentialsNotAllowedForSSOAuthenticationFault',
    ):
        assert getattr(shotgun_api3, name) is getattr(shotgun_module, name)
        assert getattr(shotgun_module, name) is getattr(_errors, name)
    assert (
        shotgun_module.UserCredentialsNotAllowedForOxygenAuthenticationFault
        is _errors.UserCredentialsNotAllowedForOxygenAuthenticationFault
    )


def test_logger_name() -> None:
    assert shotgun_module.LOG.name == 'shotgun_api3'


def test_package_level_exports() -> None:
    assert len(shotgun_api3.__all__) == len(set(shotgun_api3.__all__))
    assert set(shotgun_api3.__all__) == set(_PACKAGE_EXPORTS)
    for name in _PACKAGE_EXPORTS:
        assert hasattr(shotgun_api3, name), name
    for name in (
        'ClientCapabilities',
        'LOG',
        'SG_TIMEZONE',
        'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION',
        'ServerCapabilities',
        'SgTimezone',
        'UserCredentialsNotAllowedForOxygenAuthenticationFault',
    ):
        assert not hasattr(shotgun_api3, name), name


def test_package_version_without_installed_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_distributions(**_: object) -> list[importlib.metadata.Distribution]:
        return []

    with monkeypatch.context() as patch:
        patch.setattr(importlib.metadata, 'distributions', no_distributions)
        assert importlib.reload(_version).PACKAGE_VERSION == '0+unknown'
    assert importlib.reload(_version).PACKAGE_VERSION == PACKAGE_VERSION


def test_project_metadata() -> None:
    pyproject_path = Path(__file__).parents[1] / 'pyproject.toml'
    with pyproject_path.open('rb') as pyproject_file:
        project = tomllib.load(pyproject_file)['project']

    # Code written for Autodesk's client reads __version__ to learn which API it has.
    assert shotgun_api3.__version__ == shotgun_module.__version__ == COMPATIBILITY_VERSION
    # The manifest is the only copy; the runtime reads it from installed metadata.
    assert project['version'] == PACKAGE_VERSION
    assert 'version' not in project['dynamic']
    assert project['license'] == 'MIT'
    assert project['urls'] == {
        'Documentation': f'{_REPOSITORY_URL}/tree/main/docs',
        'Homepage': _REPOSITORY_URL,
        'Issues': f'{_REPOSITORY_URL}/issues',
        'Repository': _REPOSITORY_URL,
    }


def test_shotgun_module_exports() -> None:
    assert len(shotgun_module.__all__) == len(set(shotgun_module.__all__))
    assert set(shotgun_module.__all__) == set(_SHOTGUN_MODULE_EXPORTS)
    for name in _SHOTGUN_MODULE_EXPORTS:
        assert hasattr(shotgun_module, name), name
    assert not hasattr(shotgun_module, 'sg_timezone')


def test_xmlrpc_aliases_preserve_identity() -> None:
    for name in ('Error', 'ProtocolError', 'ResponseError'):
        expected = getattr(xmlrpc.client, name)
        assert getattr(shotgun_api3, name) is expected
        assert getattr(shotgun_module, name) is expected
