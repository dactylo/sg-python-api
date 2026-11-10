from __future__ import annotations

import base64
import csv
import hashlib
import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Never

import pytest

from scripts import _sdist_contract as contract
from shotgun_api3._version import PACKAGE_VERSION
from tests.tooling._wheels import write_wheel

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SDIST_CONTRACT = _REPO_ROOT / 'scripts' / '_sdist_contract.py'
_SDIST_NAME = f'sg_python_api-{PACKAGE_VERSION}.tar.gz'
_WHEEL_NAME = f'sg_python_api-{PACKAGE_VERSION}-py3-none-any.whl'

_SDIST_METADATA = f"""Metadata-Version: 2.4
Version: {PACKAGE_VERSION}
Name: sg-python-api
Requires-Python: >=3.11
License-Expression: MIT
License-File: LICENSE
Classifier: Programming Language :: Python :: 3 :: Only
Classifier: Programming Language :: Python :: 3.11
Classifier: Programming Language :: Python :: 3.12
Classifier: Programming Language :: Python :: 3.13
Classifier: Programming Language :: Python :: 3.14
Classifier: Programming Language :: Python :: 3.15
Classifier: Typing :: Typed
Requires-Dist: urllib3<3,>=2
Provides-Extra: async
Requires-Dist: httpx2<3,>=2; extra == 'async'
Project-URL: Documentation, https://github.com/dactylo/sg-python-api/tree/main/docs
Project-URL: Homepage, https://github.com/dactylo/sg-python-api
Project-URL: Issues, https://github.com/dactylo/sg-python-api/issues
Project-URL: Repository, https://github.com/dactylo/sg-python-api

""".encode('ascii')
_WHEEL_METADATA = f"""Metadata-Version: 2.4
Name: sg-python-api
Version: {PACKAGE_VERSION}
License-Expression: MIT
License-File: LICENSE
Requires-Python: >=3.11
Classifier: Programming Language :: Python :: 3 :: Only
Classifier: Programming Language :: Python :: 3.11
Classifier: Programming Language :: Python :: 3.12
Classifier: Programming Language :: Python :: 3.13
Classifier: Programming Language :: Python :: 3.14
Classifier: Programming Language :: Python :: 3.15
Classifier: Typing :: Typed
Provides-Extra: async
Requires-Dist: urllib3<3,>=2
Requires-Dist: httpx2<3,>=2; extra == "async"
Project-URL: Documentation, https://github.com/dactylo/sg-python-api/tree/main/docs
Project-URL: Homepage, https://github.com/dactylo/sg-python-api
Project-URL: Issues, https://github.com/dactylo/sg-python-api/issues
Project-URL: Repository, https://github.com/dactylo/sg-python-api

""".encode('ascii')


def _replace_wheel_member(path: Path, name: str, contents: bytes) -> None:
    with zipfile.ZipFile(path) as archive:
        files = {member.filename: archive.read(member) for member in archive.infolist()}
    files[name] = contents
    write_wheel(path, files, fixed_timestamp=True)


def _run_metadata_validation(
    contents: bytes,
    label: str,
) -> tuple[object, ...]:
    return contract._metadata_signature(contract._parse_metadata(contents, label), label)


def _run_sdist_contract(
    export_root: Path,
    sdist_path: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            '-I',
            str(_SDIST_CONTRACT),
            str(export_root),
            str(sdist_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _sdist_file(name: str, contents: bytes) -> tuple[tarfile.TarInfo, bytes]:
    member = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/{name}')
    member.size = len(contents)
    return member, contents


def _write_sdist(
    path: Path,
    members: list[tuple[tarfile.TarInfo, bytes | None]],
) -> None:
    with tarfile.open(path, mode='w:gz') as archive:
        for member, contents in members:
            archive.addfile(
                member,
                io.BytesIO(contents) if contents is not None else None,
            )


def _write_tree(root: Path, files: dict[str, bytes]) -> None:
    for name, contents in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)


def _valid_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    export_root = tmp_path / 'export'
    source_wheel = tmp_path / 'source' / _WHEEL_NAME
    derived_wheel = tmp_path / 'derived' / _WHEEL_NAME
    sdist = tmp_path / _SDIST_NAME
    source_wheel.parent.mkdir()
    derived_wheel.parent.mkdir()
    package_files = {
        f'shotgun_api3/{name}': f'{name}\n'.encode('ascii')
        for name in contract._EXPECTED_PACKAGE_FILES
    }
    _write_tree(
        export_root,
        {
            **{f'src/{name}': contents for name, contents in package_files.items()},
            'LICENSE': b'license\n',
        },
    )
    _write_sdist(
        sdist,
        [
            _sdist_file('PKG-INFO', _SDIST_METADATA),
            _sdist_file('LICENSE', b'license\n'),
            *(_sdist_file(f'src/{name}', contents) for name, contents in package_files.items()),
        ],
    )
    dist_info = f'sg_python_api-{PACKAGE_VERSION}.dist-info'
    wheel_files = {
        **package_files,
        f'{dist_info}/METADATA': _WHEEL_METADATA,
        f'{dist_info}/WHEEL': b'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n',
        f'{dist_info}/licenses/LICENSE': b'license\n',
    }
    record_name = f'{dist_info}/RECORD'
    rows = [
        [
            name,
            'sha256='
            + base64.urlsafe_b64encode(hashlib.sha256(contents).digest())
            .rstrip(b'=')
            .decode('ascii'),
            str(len(contents)),
        ]
        for name, contents in wheel_files.items()
    ]
    rows.append([record_name, '', ''])
    buffer = io.StringIO()
    csv.writer(buffer).writerows(rows)
    wheel_files[record_name] = buffer.getvalue().encode('ascii')
    write_wheel(source_wheel, wheel_files, fixed_timestamp=True)
    write_wheel(derived_wheel, wheel_files, fixed_timestamp=True)
    return source_wheel, sdist, derived_wheel, export_root


def test_artifact_contract_accepts_synthetic_archives_and_isolated_cli(tmp_path: Path) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)

    contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)
    result = subprocess.run(
        [
            sys.executable,
            '-I',
            str(_SDIST_CONTRACT),
            '--artifacts',
            str(source_wheel),
            str(sdist),
            str(derived_wheel),
            str(export_root),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert 'artifact archive validation OK' in result.stdout


def test_artifact_contract_rejects_differing_wheel_archive_bytes(tmp_path: Path) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    with zipfile.ZipFile(derived_wheel, mode='a') as archive:
        archive.comment = b'different archive bytes'

    with pytest.raises(AssertionError, match='archive hashes differ'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


@pytest.mark.parametrize(
    ('member', 'expected'),
    [
        pytest.param('shotgun_api3/../escape.py', 'contains an unsafe path', id='unsafe-wheel'),
        pytest.param(
            'shotgun_api3/__init__.py', 'contains duplicate member', id='duplicate-wheel'
        ),
    ],
)
def test_artifact_contract_rejects_invalid_wheel_members(
    tmp_path: Path, member: str, expected: str
) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    with zipfile.ZipFile(source_wheel, mode='a') as archive:
        if member == 'shotgun_api3/__init__.py':
            with pytest.warns(UserWarning, match='Duplicate name'):
                archive.writestr(member, b'corrupt\n')
        else:
            archive.writestr(member, b'corrupt\n')

    with pytest.raises(AssertionError, match=expected):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


def test_artifact_contract_rejects_metadata_mismatch(tmp_path: Path) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    _write_sdist(
        sdist,
        [
            _sdist_file(
                'PKG-INFO',
                _SDIST_METADATA.replace(b'Name: sg-python-api', b'Name: other-package'),
            ),
            _sdist_file('LICENSE', b'license\n'),
            *(
                _sdist_file(
                    f'src/shotgun_api3/{name}',
                    (export_root / 'src' / 'shotgun_api3' / name).read_bytes(),
                )
                for name in contract._EXPECTED_PACKAGE_FILES
            ),
        ],
    )

    with pytest.raises(AssertionError, match='Name differs'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


@pytest.mark.parametrize('name', ['.scratch/notes.md', 'notes/private.md', 'docs/drafts/notes.md'])
@pytest.mark.parametrize('archive_kind', ['sdist', 'wheel'])
def test_artifact_contract_rejects_nonpublic_members(
    tmp_path: Path, name: str, archive_kind: str
) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    if archive_kind == 'sdist':
        files = contract._read_sdist(sdist)
        files[name] = b'private\n'
        _write_sdist(sdist, [_sdist_file(path, content) for path, content in files.items()])
    else:
        with zipfile.ZipFile(source_wheel, mode='a') as archive:
            archive.writestr(name, b'private\n')

    with pytest.raises(AssertionError, match='unexpected'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


@pytest.mark.parametrize('field', ['hash', 'size'])
def test_artifact_contract_rejects_record_mismatch(tmp_path: Path, field: str) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    with zipfile.ZipFile(source_wheel) as archive:
        record = archive.read(f'sg_python_api-{PACKAGE_VERSION}.dist-info/RECORD')
    if field == 'hash':
        damaged = record.replace(b'sha256=', b'sha256=x', 1)
    else:
        damaged = record.replace(b',12\r\n', b',999\r\n', 1)
        assert damaged != record
    _replace_wheel_member(
        source_wheel, f'sg_python_api-{PACKAGE_VERSION}.dist-info/RECORD', damaged
    )

    with pytest.raises(AssertionError, match='RECORD entry differs'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


@pytest.mark.parametrize('missing', [False, True], ids=['altered', 'missing'])
def test_artifact_contract_rejects_source_drift(tmp_path: Path, *, missing: bool) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    source = export_root / 'src' / 'shotgun_api3' / '__init__.py'
    if missing:
        source.unlink()
    else:
        source.write_bytes(b'altered\n')

    with pytest.raises(AssertionError, match=r'__init__\.py'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


def test_artifact_contract_rejects_unsupported_sdist_member(tmp_path: Path) -> None:
    source_wheel, sdist, derived_wheel, export_root = _valid_artifacts(tmp_path)
    fifo = tarfile.TarInfo(f'sg_python_api-{PACKAGE_VERSION}/src/fifo')
    fifo.type = tarfile.FIFOTYPE
    _write_sdist(sdist, [(fifo, None)])

    with pytest.raises(AssertionError, match='contains unsupported member'):
        contract._validate_artifacts(source_wheel, sdist, derived_wheel, export_root)


@pytest.mark.parametrize(
    ('label', 'contents'),
    [
        pytest.param('source wheel', _WHEEL_METADATA, id='wheel'),
        pytest.param('source distribution', _SDIST_METADATA, id='sdist'),
    ],
)
def test_artifact_validator_accepts_project_metadata(label: str, contents: bytes) -> None:
    assert _run_metadata_validation(contents, label)


@pytest.mark.parametrize(
    ('case', 'contents', 'expected_error'),
    [
        pytest.param(
            'missing extra',
            _WHEEL_METADATA.replace(b'Provides-Extra: async\n', b''),
            'missing extra extras differ',
            id='missing-extra',
        ),
        pytest.param(
            'unexpected extra',
            _WHEEL_METADATA.replace(
                b'Provides-Extra: async\n',
                b'Provides-Extra: async\nProvides-Extra: sync\n',
            ),
            'unexpected extra extras differ',
            id='unexpected-extra',
        ),
        pytest.param(
            'altered required field',
            _WHEEL_METADATA.replace(b'Name: sg-python-api\n', b'Name: other-package\n'),
            "altered required field Name differs: expected 'sg-python-api', got 'other-package'",
            id='required-field',
        ),
        pytest.param(
            'altered dependency',
            _WHEEL_METADATA.replace(
                b'Requires-Dist: urllib3<3,>=2\n', b'Requires-Dist: urllib3<4,>=2\n'
            ),
            'altered dependency requirements differ',
            id='dependency',
        ),
        pytest.param(
            'altered URL',
            _WHEEL_METADATA.replace(
                b'Project-URL: Homepage, https://github.com/dactylo/sg-python-api\n',
                b'Project-URL: Homepage, https://example.invalid/sg-python-api\n',
            ),
            'altered URL project URLs differ',
            id='url',
        ),
    ],
)
def test_artifact_validator_rejects_mismatched_metadata(
    case: str,
    contents: bytes,
    expected_error: str,
) -> None:
    with pytest.raises(AssertionError, match=expected_error):
        _run_metadata_validation(contents, case)


def test_sdist_contract_accepts_exact_positive_sources(tmp_path: Path) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    exported = {
        '.editorconfig': b'editor\n',
        '.gitattributes': b'attributes\n',
        '.github/workflows/ci.yml': b'ci\n',
        '.gitignore': b'ignore\n',
        'COMPATIBILITY.md': b'compatibility\n',
        'LICENSE': b'license\n',
        'Makefile': b'make\n',
        'README.md': b'readme\n',
        'docs/guide.md': b'guide\n',
        'docs/drafts/private.md': b'private\n',
        'examples/nested/example.py': b'example\n',
        'pyproject.toml': b'project\n',
        'scripts/check.sh': b'check\n',
        'src/shotgun_api3/__init__.py': b'package\n',
        'tests/test_package.py': b'test\n',
    }
    expected_names = {
        '.gitignore',
        'LICENSE',
        'README.md',
        'pyproject.toml',
        'src/shotgun_api3/__init__.py',
    }
    _write_tree(export_root, exported)
    _write_sdist(
        sdist_path,
        [
            *(_sdist_file(name, exported[name]) for name in sorted(expected_names)),
            _sdist_file('PKG-INFO', b'metadata\n'),
        ],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode == 0, result.stderr


def test_sdist_contract_rejects_changed_positive_source(tmp_path: Path) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/package.py': b'from HEAD\n'})
    _write_sdist(
        sdist_path,
        [
            _sdist_file('PKG-INFO', b'metadata\n'),
            _sdist_file('src/package.py', b'changed\n'),
        ],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert 'sdist member differs from HEAD: src/package.py' in result.stderr


def test_sdist_contract_rejects_duplicate_before_positive_comparison(
    tmp_path: Path,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    duplicate = _sdist_file('src/duplicate.py', b'first\n')
    _write_sdist(
        sdist_path,
        [duplicate, _sdist_file('src/duplicate.py', b'second\n')],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f'{_SDIST_NAME} contains duplicate member: src/duplicate.py' in result.stderr
    assert 'sdist contents differ' not in result.stderr


def test_sdist_contract_rejects_exported_positive_symlink(tmp_path: Path) -> None:
    export_root = tmp_path / 'export'
    outside_path = tmp_path / 'outside.py'
    sdist_path = tmp_path / _SDIST_NAME
    outside_path.write_bytes(b'outside\n')
    exported_path = export_root / 'src' / 'package.py'
    exported_path.parent.mkdir(parents=True)
    exported_path.symlink_to(outside_path)
    _write_sdist(
        sdist_path,
        [
            _sdist_file('PKG-INFO', b'metadata\n'),
            _sdist_file('src/package.py', b'outside\n'),
        ],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert 'candidate export contains a symlink: src/package.py' in result.stderr


def test_sdist_contract_rejects_hardlink_before_positive_comparison(
    tmp_path: Path,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    link = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/src/hardlink.py')
    link.type = tarfile.LNKTYPE
    link.linkname = 'target.py'
    _write_sdist(sdist_path, [(link, None)])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f'{_SDIST_NAME} contains a link: {link.name}' in result.stderr
    assert 'sdist contents differ' not in result.stderr


@pytest.mark.parametrize(
    ('member_name', 'expected_error'),
    [
        pytest.param(
            f'{_SDIST_NAME.removesuffix(".tar.gz")}/../escape.py',
            'contains an unsafe path',
            id='unsafe-path',
        ),
        pytest.param(
            'wrong-root/file.py',
            'has unexpected archive root',
            id='wrong-root',
        ),
    ],
)
def test_sdist_contract_rejects_invalid_structure_before_positive_comparison(
    tmp_path: Path,
    member_name: str,
    expected_error: str,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    bad_member = tarfile.TarInfo(member_name)
    bad_member.size = len(b'bad\n')
    _write_sdist(sdist_path, [(bad_member, b'bad\n')])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert expected_error in result.stderr
    assert 'sdist contents differ' not in result.stderr


def test_sdist_contract_rejects_missing_positive_source(tmp_path: Path) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(
        export_root,
        {
            'tests/zeta.py': b'zeta\n',
            'src/alpha.py': b'alpha\n',
        },
    )
    _write_sdist(sdist_path, [_sdist_file('PKG-INFO', b'metadata\n')])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert ("sdist contents differ; missing=['src/alpha.py'], unexpected=[]") in result.stderr


@pytest.mark.parametrize(
    'member_type',
    [
        pytest.param(tarfile.CONTTYPE, id='contiguous-file'),
        pytest.param(tarfile.GNUTYPE_SPARSE, id='gnu-sparse'),
    ],
)
def test_sdist_contract_rejects_nonstandard_file_member_before_comparison(
    tmp_path: Path,
    member_type: bytes,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    member = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/src/unsupported')
    member.type = member_type
    _write_sdist(sdist_path, [(member, None)])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f'{_SDIST_NAME} contains unsupported member: {member.name}' in result.stderr
    assert 'sdist contents differ' not in result.stderr


@pytest.mark.parametrize(
    'name',
    [
        'COMPATIBILITY.md',
        'docs/guide.md',
        'examples/nested/example.py',
        'Makefile',
        'scripts/check.sh',
        'tests/test_package.py',
    ],
)
def test_sdist_contract_rejects_retired_source_even_when_exported(
    tmp_path: Path, name: str
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {name: b'checkout source\n'})
    _write_sdist(
        sdist_path,
        [
            _sdist_file(name, b'checkout source\n'),
            _sdist_file('PKG-INFO', b'metadata\n'),
        ],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f"sdist contents differ; missing=[], unexpected=['{name}']" in result.stderr


def test_sdist_contract_rejects_symlink_before_positive_comparison(
    tmp_path: Path,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    link = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/src/symlink.py')
    link.type = tarfile.SYMTYPE
    link.linkname = 'target.py'
    _write_sdist(sdist_path, [(link, None)])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f'{_SDIST_NAME} contains a link: {link.name}' in result.stderr
    assert 'sdist contents differ' not in result.stderr


def test_sdist_contract_rejects_unexpected_source(tmp_path: Path) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(
        export_root,
        {
            'zzz-control.txt': b'zzz\n',
            '.github/workflows/ci.yml': b'ci\n',
        },
    )
    _write_sdist(
        sdist_path,
        [
            _sdist_file('zzz-control.txt', b'zzz\n'),
            _sdist_file('.github/workflows/ci.yml', b'ci\n'),
            _sdist_file('PKG-INFO', b'metadata\n'),
        ],
    )

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert (
        "sdist contents differ; missing=[], unexpected=['.github/workflows/ci.yml', "
        "'zzz-control.txt']"
    ) in result.stderr


def test_sdist_contract_rejects_unsupported_member_before_positive_comparison(
    tmp_path: Path,
) -> None:
    export_root = tmp_path / 'export'
    sdist_path = tmp_path / _SDIST_NAME
    _write_tree(export_root, {'src/missing.py': b'missing\n'})
    fifo = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/src/unsupported')
    fifo.type = tarfile.FIFOTYPE
    _write_sdist(sdist_path, [(fifo, None)])

    result = _run_sdist_contract(export_root, sdist_path)

    assert result.returncode != 0
    assert f'{_SDIST_NAME} contains unsupported member: {fifo.name}' in result.stderr
    assert 'sdist contents differ' not in result.stderr


def test_sdist_contract_scans_all_structure_before_reading_member_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdist_path = tmp_path / _SDIST_NAME
    fifo = tarfile.TarInfo(f'{_SDIST_NAME.removesuffix(".tar.gz")}/src/unsupported')
    fifo.type = tarfile.FIFOTYPE
    _write_sdist(
        sdist_path,
        [_sdist_file('src/first.py', b'first\n'), (fifo, None)],
    )
    extract_calls = 0

    def fail_extractfile(*_args: object, **_kwargs: object) -> Never:
        nonlocal extract_calls
        extract_calls += 1
        raise AssertionError('extractfile called before structural scan completed')

    monkeypatch.setattr(tarfile.TarFile, 'extractfile', fail_extractfile)
    with pytest.raises(
        AssertionError,
        match=f'{_SDIST_NAME} contains unsupported member: {fifo.name}',
    ):
        contract._read_sdist(sdist_path)
    assert extract_calls == 0


def test_validator_package_inventory_matches_packaged_source() -> None:
    package_source = _REPO_ROOT / 'src' / 'shotgun_api3'
    source_files = {
        path.relative_to(package_source).as_posix()
        for path in package_source.rglob('*')
        if path.is_file() and (path.suffix == '.py' or path.name == 'py.typed')
    }

    assert source_files == contract._EXPECTED_PACKAGE_FILES
