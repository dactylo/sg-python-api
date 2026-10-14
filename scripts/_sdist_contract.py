#!/usr/bin/env python
"""Validate the source distribution contract."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import stat
import sys
import tarfile
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from email.message import Message

_ARTIFACT_ARGUMENT_COUNT = 6
_COMMAND_ARGUMENT_COUNT = 3
_DIRECT_DOC_PART_COUNT = 2
_EXPECTED_VERSION = '0.1.2'
_REGULAR_MEMBER_TYPES = frozenset({tarfile.AREGTYPE, tarfile.REGTYPE})
_ROOT_SOURCE_FILES = frozenset(
    {
        '.gitignore',
        'COMPATIBILITY.md',
        'LICENSE',
        'Makefile',
        'README.md',
        'pyproject.toml',
    }
)
_SDIST_ROOT_NAME = f'sg_python_api-{_EXPECTED_VERSION}'
_TREE_SOURCE_ROOTS = frozenset({'examples', 'scripts', 'src', 'tests'})
_EXPECTED_CLASSIFIERS = {
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
    'Programming Language :: Python :: 3.13',
    'Programming Language :: Python :: 3.14',
    'Programming Language :: Python :: 3.15',
    'Typing :: Typed',
}
_EXPECTED_EXTRAS = {'async'}
_EXPECTED_PACKAGE_FILES = {
    '__init__.py',
    '_async_client.py',
    '_async_driver.py',
    '_async_httpx2.py',
    '_async_transport.py',
    '_capabilities.py',
    '_client.py',
    '_config.py',
    '_crud.py',
    '_driver.py',
    '_errors.py',
    '_execution.py',
    '_files.py',
    '_methods.py',
    '_protocol.py',
    '_sentinel.py',
    '_sg_timezone.py',
    '_signatures.py',
    '_transport.py',
    '_version.py',
    'lib/__init__.py',
    'lib/mockgun/__init__.py',
    'lib/mockgun/_async.py',
    'lib/mockgun/_mockgun.py',
    'lib/sgtimezone.py',
    'py.typed',
    'shotgun.py',
}
_EXPECTED_REQUIREMENTS = {
    "httpx2<3,>=2;extra=='async'",
    'urllib3<3,>=2',
}
_EXPECTED_URLS = {
    'Documentation': 'https://github.com/dactylo/sg-python-api/tree/main/docs',
    'Homepage': 'https://github.com/dactylo/sg-python-api',
    'Issues': 'https://github.com/dactylo/sg-python-api/issues',
    'Repository': 'https://github.com/dactylo/sg-python-api',
}
_WHEEL_DIST_INFO = f'sg_python_api-{_EXPECTED_VERSION}.dist-info'


def _assert_archive_path(name: str, label: str) -> None:
    """Reject an unsafe archive member path."""
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts:
        raise AssertionError(f'{label} contains an unsafe path: {name}')


def _is_expected_sdist_source(name: str) -> bool:
    """Return whether one exported file belongs in the sdist."""
    path = PurePosixPath(name)
    if name in _ROOT_SOURCE_FILES:
        return True
    if (
        len(path.parts) == _DIRECT_DOC_PART_COUNT
        and path.parts[0] == 'docs'
        and path.suffix == '.md'
    ):
        return True
    return len(path.parts) > 1 and path.parts[0] in _TREE_SOURCE_ROOTS


def _assert_sdist_paths(names: set[str], label: str) -> None:
    """Reject paths outside the source distribution contract."""
    unexpected = sorted(
        name for name in names if name != 'PKG-INFO' and not _is_expected_sdist_source(name)
    )
    if unexpected:
        raise AssertionError(f'{label} contains unexpected paths: {unexpected}')


def _metadata_signature(metadata: Message, label: str) -> tuple[object, ...]:
    """Check metadata against independent release literals."""
    expected_fields = {
        'License-Expression': 'MIT',
        'Name': 'sg-python-api',
        'Requires-Python': '>=3.11',
        'Version': _EXPECTED_VERSION,
    }
    for field, expected in expected_fields.items():
        if metadata[field] != expected:
            raise AssertionError(
                f'{label} {field} differs: expected {expected!r}, got {metadata[field]!r}'
            )
    if set(metadata.get_all('Classifier') or []) != _EXPECTED_CLASSIFIERS:
        raise AssertionError(f'{label} classifiers differ')
    if set(metadata.get_all('Provides-Extra') or []) != _EXPECTED_EXTRAS:
        raise AssertionError(f'{label} extras differ')
    if set(metadata.get_all('License-File') or []) != {'LICENSE'}:
        raise AssertionError(f'{label} license files differ')

    requirements = {
        requirement.replace(' ', '').replace('"', "'")
        for requirement in metadata.get_all('Requires-Dist') or []
    }
    if requirements != _EXPECTED_REQUIREMENTS:
        raise AssertionError(f'{label} requirements differ: {sorted(requirements)}')

    project_urls: dict[str, str] = {}
    for entry in metadata.get_all('Project-URL') or []:
        project, separator, url = entry.partition(',')
        if not separator:
            raise AssertionError(f'{label} has invalid Project-URL: {entry}')
        project_urls[project.strip()] = url.strip()
    if project_urls != _EXPECTED_URLS:
        raise AssertionError(f'{label} project URLs differ: {project_urls}')

    return (
        tuple(sorted(expected_fields.items())),
        tuple(sorted(_EXPECTED_CLASSIFIERS)),
        tuple(sorted(_EXPECTED_REQUIREMENTS)),
        tuple(sorted(_EXPECTED_URLS.items())),
        tuple(sorted(_EXPECTED_EXTRAS)),
        ('LICENSE',),
    )


def _parse_metadata(contents: bytes, label: str) -> Message:
    """Parse metadata without tolerating parser defects."""
    metadata = BytesParser(policy=default).parsebytes(contents)
    if metadata.defects:
        raise AssertionError(f'{label} metadata defects: {metadata.defects}')
    return metadata


def _read_exported_files(root: Path) -> dict[str, bytes]:
    """Read every regular candidate-export file."""
    files: dict[str, bytes] = {}
    for path in root.rglob('*'):
        name = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            if _is_expected_sdist_source(name):
                raise AssertionError(f'candidate export contains a symlink: {name}')
            continue
        if stat.S_ISREG(mode):
            files[name] = path.read_bytes()
    return files


def _read_sdist(sdist_path: Path) -> dict[str, bytes]:
    """Read one structurally valid raw sdist."""
    with tarfile.open(sdist_path, mode='r:*') as archive:
        file_members: dict[str, tarfile.TarInfo] = {}
        for member in archive.getmembers():
            _assert_archive_path(member.name, sdist_path.name)
            parts = PurePosixPath(member.name).parts
            if not parts or parts[0] != _SDIST_ROOT_NAME:
                raise AssertionError(
                    f'{sdist_path.name} has unexpected archive root: {member.name}'
                )
            if member.issym() or member.islnk():
                raise AssertionError(f'{sdist_path.name} contains a link: {member.name}')
            if member.type == tarfile.DIRTYPE:
                continue
            if member.type not in _REGULAR_MEMBER_TYPES:
                raise AssertionError(
                    f'{sdist_path.name} contains unsupported member: {member.name}'
                )
            relative = PurePosixPath(*parts[1:]).as_posix()
            if relative in file_members:
                raise AssertionError(f'{sdist_path.name} contains duplicate member: {relative}')
            file_members[relative] = member
        files: dict[str, bytes] = {}
        for relative, member in file_members.items():
            extracted = archive.extractfile(member)
            if extracted is None:
                raise AssertionError(f'{sdist_path.name} member cannot be read: {member.name}')
            files[relative] = extracted.read()
    return files


def _read_wheel(path: Path) -> dict[str, bytes]:
    """Read one wheel and reject duplicate or unsafe members."""
    with zipfile.ZipFile(path) as archive:
        files: dict[str, bytes] = {}
        for member in archive.infolist():
            _assert_archive_path(member.filename, path.name)
            if member.is_dir():
                continue
            if member.filename in files:
                raise AssertionError(f'{path.name} contains duplicate member: {member.filename}')
            files[member.filename] = archive.read(member)
    return files


def _validate_record(files: dict[str, bytes], record_name: str, label: str) -> None:
    """Check RECORD membership, hashes, and sizes."""
    rows = list(csv.reader(io.StringIO(files[record_name].decode('utf-8'))))
    record_paths = {row[0] for row in rows}
    if record_paths != set(files):
        raise AssertionError(f'{label} RECORD paths differ from wheel members')
    for name, digest, size in rows:
        if name == record_name:
            if digest or size:
                raise AssertionError(f'{label} RECORD self-entry has a hash or size')
            continue
        expected_digest = base64.urlsafe_b64encode(hashlib.sha256(files[name]).digest())
        expected_hash = f'sha256={expected_digest.rstrip(b"=").decode("ascii")}'
        if digest != expected_hash or size != str(len(files[name])):
            raise AssertionError(f'{label} RECORD entry differs: {name}')


def _validate_sdist_sources(export_root: Path, sdist_path: Path) -> None:
    """Compare one raw sdist with the positive source contract."""
    exported_files = _read_exported_files(export_root)
    expected_files = {
        name: contents
        for name, contents in exported_files.items()
        if _is_expected_sdist_source(name)
    }
    actual_files = _read_sdist(sdist_path)
    actual_files.pop('PKG-INFO', None)
    if set(actual_files) != set(expected_files):
        missing = sorted(set(expected_files) - set(actual_files))
        unexpected = sorted(set(actual_files) - set(expected_files))
        raise AssertionError(f'sdist contents differ; missing={missing}, unexpected={unexpected}')
    for name, contents in expected_files.items():
        if actual_files[name] != contents:
            raise AssertionError(f'sdist member differs from HEAD: {name}')


def _validate_wheel(
    path: Path,
    files: dict[str, bytes],
    exported_files: dict[str, bytes],
) -> tuple[object, ...]:
    """Check wheel inventory, exported sources, and metadata."""
    metadata_name = f'{_WHEEL_DIST_INFO}/METADATA'
    record_name = f'{_WHEEL_DIST_INFO}/RECORD'
    wheel_name = f'{_WHEEL_DIST_INFO}/WHEEL'
    license_name = f'{_WHEEL_DIST_INFO}/licenses/LICENSE'
    expected_members = {
        *(f'shotgun_api3/{name}' for name in _EXPECTED_PACKAGE_FILES),
        metadata_name,
        record_name,
        wheel_name,
        license_name,
    }
    if set(files) != expected_members:
        missing = sorted(expected_members - set(files))
        unexpected = sorted(set(files) - expected_members)
        raise AssertionError(
            f'{path.name} contents differ; missing={missing}, unexpected={unexpected}'
        )
    for name in _EXPECTED_PACKAGE_FILES:
        source_name = f'src/shotgun_api3/{name}'
        wheel_name_for_source = f'shotgun_api3/{name}'
        if source_name not in exported_files:
            raise AssertionError(f'{path.name} package source missing from HEAD: {name}')
        if files[wheel_name_for_source] != exported_files[source_name]:
            raise AssertionError(f'{path.name} package member differs from HEAD: {name}')
    if files[license_name] != exported_files['LICENSE']:
        raise AssertionError(f'{path.name} license differs from HEAD')

    wheel_metadata = _parse_metadata(files[wheel_name], f'{path.name} WHEEL')
    expected_wheel_fields = {
        'Root-Is-Purelib': 'true',
        'Tag': 'py3-none-any',
        'Wheel-Version': '1.0',
    }
    for field, expected in expected_wheel_fields.items():
        if wheel_metadata[field] != expected:
            raise AssertionError(f'{path.name} WHEEL {field} differs')
    _validate_record(files, record_name, path.name)
    return _metadata_signature(_parse_metadata(files[metadata_name], path.name), path.name)


def _validate_artifacts(
    source_wheel: Path, sdist: Path, derived_wheel: Path, export_root: Path
) -> None:
    """Validate archive names, contents, RECORD, metadata, and byte parity."""
    if source_wheel.name != f'sg_python_api-{_EXPECTED_VERSION}-py3-none-any.whl':
        raise AssertionError(f'unexpected source wheel name: {source_wheel.name}')
    if sdist.name != f'sg_python_api-{_EXPECTED_VERSION}.tar.gz':
        raise AssertionError(f'unexpected sdist name: {sdist.name}')
    if derived_wheel.name != source_wheel.name:
        raise AssertionError(f'sdist-derived wheel name differs: {derived_wheel.name}')

    exported_files = {
        path.relative_to(export_root).as_posix(): path.read_bytes()
        for path in export_root.rglob('*')
        if path.is_file()
    }
    source_files = _read_wheel(source_wheel)
    derived_files = _read_wheel(derived_wheel)
    sdist_files = _read_sdist(sdist)

    _assert_sdist_paths(set(sdist_files), sdist.name)

    source_signature = _validate_wheel(source_wheel, source_files, exported_files)
    derived_signature = _validate_wheel(derived_wheel, derived_files, exported_files)
    sdist_signature = _metadata_signature(
        _parse_metadata(sdist_files['PKG-INFO'], sdist.name), sdist.name
    )
    if source_signature != derived_signature or source_signature != sdist_signature:
        raise AssertionError('wheel and sdist metadata differ')
    if source_files != derived_files:
        raise AssertionError('source and sdist-derived wheel member hashes differ')
    if (
        hashlib.sha256(source_wheel.read_bytes()).digest()
        != hashlib.sha256(derived_wheel.read_bytes()).digest()
    ):
        raise AssertionError('source and sdist-derived wheel archive hashes differ')

    print('artifact archive validation OK')


def _main() -> None:
    """Validate source contents or the completed artifact set."""
    if len(sys.argv) == _COMMAND_ARGUMENT_COUNT:
        _validate_sdist_sources(Path(sys.argv[1]), Path(sys.argv[2]))
    elif len(sys.argv) == _ARTIFACT_ARGUMENT_COUNT and sys.argv[1] == '--artifacts':
        _validate_artifacts(*(Path(argument) for argument in sys.argv[2:]))
    else:
        raise SystemExit(
            'usage: _sdist_contract.py EXPORT_ROOT SDIST_PATH | '
            '--artifacts SOURCE_WHEEL SDIST_PATH DERIVED_WHEEL EXPORT_ROOT'
        )


if __name__ == '__main__':
    _main()
