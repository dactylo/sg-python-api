from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path
from typing import Never
from unittest.mock import patch

import pytest

from scripts import _artifact_evidence as evidence
from scripts import _sdist_contract as contract
from scripts import run_installed_tests as installed_runner
from tests._git import git_env

_FAKE_CP = r"""#!/usr/bin/env bash
set -euo pipefail

if [[ ${FAIL_SECOND_COPY:-0} == '1' ]]; then
    copy_count=0
    if [[ -f $COPY_COUNT_FILE ]]; then
        copy_count=$(<"$COPY_COUNT_FILE")
    fi
    copy_count=$((copy_count + 1))
    printf '%s\n' "$copy_count" >"$COPY_COUNT_FILE"
    if [[ $copy_count -eq 2 ]]; then
        printf '%s\n' 'simulated copy failure' >&2
        exit 1
    fi
fi

exec "$REAL_CP" "$@"
"""
_FAKE_PYTHON = r"""#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == '-c' ]]; then
    preflight_import='import anyio, hatchling, httpx2, installer, pytest, urllib3; '
    preflight_import+="print('artifact tools OK')"
    if [[ ${2:-} == "$preflight_import" ]]; then
        printf '%s\n' 'artifact tools OK'
        exit 0
    fi
    version_probe='import importlib.metadata, sys; '
    version_probe+='print(importlib.metadata.version(sys.argv[1]))'
    if [[ ${2:-} == "$version_probe" ]]; then
        printf '%s\n' 'test-version'
        exit 0
    fi
    case ${2:-} in
        *hashlib*) exec "$REAL_PYTHON" "$@" ;;
        *pathlib*) exec "$REAL_PYTHON" "$@" ;;
        *platform.python_version*) printf '%s\n' '3.14.0' ;;
        *) exit 0 ;;
    esac
fi

if [[ ${1:-} == '-m' && ${2:-} == 'tarfile' && ${3:-} == '-e' ]]; then
    if [[ $4 == *.tar.gz ]]; then
        mkdir -p -- "$5/sg_python_api-0.1.0"
        exit 0
    fi
    exec "$REAL_PYTHON" "$@"
fi

if [[ ${1:-} == '-m' && ${2:-} == 'hatchling' ]]; then
    output_dir=''
    for ((index = 1; index <= $#; index++)); do
        if [[ ${!index} == '-d' ]]; then
            next_index=$((index + 1))
            output_dir=${!next_index}
        fi
    done
    mkdir -p -- "$output_dir"
    printf '%s\n' 'wheel bytes' >"$output_dir/sg_python_api-0.1.0-py3-none-any.whl"
    if [[ " $* " == *' sdist '* ]]; then
        printf '%s\n' 'sdist bytes' >"$output_dir/sg_python_api-0.1.0.tar.gz"
    fi
    exit 0
fi

if [[ ${1:-} == '-m' && ${2:-} == 'installer' ]]; then
    exit 0
fi

if [[ ${1:-} == '-I' && ${2:-} == */scripts/_sdist_contract.py && ${3:-} != '--artifacts' ]]; then
    if [[ ! -f $2 ]]; then
        printf '%s\n' 'candidate-exported sdist contract helper is missing' >&2
        exit 1
    fi
    work_root=${4%/artifacts/*}
    if [[ -e $work_root/sdist/sg_python_api-0.1.0 ]]; then
        printf '%s\n' 'sdist contract ran after extraction' >&2
        exit 1
    fi
    if [[ ${FAIL_SDIST_CONTRACT:-0} == '1' ]]; then
        printf '%s\n' 'simulated sdist contract failure' >&2
        exit 1
    fi
    if [[ -n ${SDIST_CONTRACT_MARKER:-} ]]; then
        printf '%s\n' "$2" "$3" "$4" >"$SDIST_CONTRACT_MARKER"
    fi
    exit 0
fi

if [[ ${1:-} == '-I' && ${2:-} == */scripts/_sdist_contract.py && ${3:-} == '--artifacts' ]]; then
    exit 0
fi

if [[ ${1:-} == '-I' && ${2:-} == '-' ]]; then
    exit 0
fi

if [[ ${1:-} == '-I' && ${2:-} == */scripts/run_installed_tests.py ]]; then
    exit 0
fi

exec "$REAL_PYTHON" "$@"
"""
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SDIST_CONTRACT = _REPO_ROOT / 'scripts' / '_sdist_contract.py'
_SDIST_METADATA = b"""Metadata-Version: 2.4
Version: 0.1.0
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

"""
_SDIST_NAME = 'sg_python_api-0.1.0.tar.gz'
_VALIDATOR = _REPO_ROOT / 'scripts' / 'validate_artifacts.sh'
_WHEEL_METADATA = b"""Metadata-Version: 2.4
Name: sg-python-api
Version: 0.1.0
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

"""
_WHEEL_NAME = 'sg_python_api-0.1.0-py3-none-any.whl'


def _git_bash() -> str:
    if sys.platform != 'win32':
        return 'bash'

    git = shutil.which('git')
    if git is None:
        raise RuntimeError('Git is required to run the artifact validator tests')
    # Git Bash puts architecture-specific Git binaries ahead of the cmd wrapper.
    for git_dir in Path(git).resolve().parents:
        for bash in (git_dir / 'bash.exe', git_dir / 'bin' / 'bash.exe'):
            if bash.is_file():
                return str(bash)
    raise RuntimeError('Git Bash is required to run the artifact validator tests')


def _head_commit(candidate: Path) -> str:
    return subprocess.run(
        ['git', 'rev-parse', '--verify', 'HEAD^{commit}'],
        cwd=candidate,
        env=git_env(),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _prepare_candidate(
    tmp_path: Path, *, candidate_name: str = 'candidate', private_docs: bool = True
) -> tuple[Path, dict[str, str]]:
    candidate = tmp_path / candidate_name
    fake_bin = tmp_path / 'bin'
    (candidate / 'scripts').mkdir(parents=True)
    (candidate / 'tests').mkdir()
    fake_bin.mkdir()

    shutil.copy2(_SDIST_CONTRACT, candidate / 'scripts' / _SDIST_CONTRACT.name)
    shutil.copy2(_VALIDATOR, candidate / 'scripts' / _VALIDATOR.name)
    (candidate / '.gitignore').write_text('dist/\n', encoding='ascii')
    if private_docs:
        (candidate / 'docs' / 'drafts').mkdir(parents=True)
        (candidate / 'docs' / 'drafts' / 'notes.md').write_text(
            '# Private notes\n',
            encoding='ascii',
        )
    fake_python = fake_bin / 'python'
    fake_python.write_text(_FAKE_PYTHON, encoding='ascii', newline='\n')
    fake_python.chmod(0o755)
    fake_cp = fake_bin / 'cp'
    fake_cp.write_text(_FAKE_CP, encoding='ascii', newline='\n')
    fake_cp.chmod(0o755)

    subprocess.run(['git', 'init', '--quiet'], cwd=candidate, env=git_env(), check=True)
    subprocess.run(['git', 'add', '.'], cwd=candidate, env=git_env(), check=True)
    subprocess.run(
        [
            'git',
            '-c',
            'user.name=Artifact Test',
            '-c',
            'user.email=artifact-test@example.invalid',
            'commit',
            '--quiet',
            '-m',
            'test candidate',
        ],
        cwd=candidate,
        env=git_env(),
        check=True,
    )

    env = git_env()
    env['PATH'] = f'{fake_bin}{os.pathsep}{env["PATH"]}'
    env['COPY_COUNT_FILE'] = str(tmp_path / 'copy-count')
    env['FAKE_BIN'] = str(fake_bin)
    env['REAL_CP'] = shutil.which('cp', path=os.defpath) or '/bin/cp'
    env['REAL_PYTHON'] = sys.executable
    return candidate, env


def _requirement_signature(value: str) -> tuple[str, frozenset[str], str]:
    value = value.replace(' ', '').replace('"', "'")
    name = re.match(r'[^<>=!~;]+', value)
    assert name is not None
    specifiers, _, marker = value[name.end() :].partition(';')
    return name.group(), frozenset(specifiers.split(',')), marker


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


def _run_validator(
    candidate: Path,
    env: dict[str, str],
    output_dir: Path | None,
) -> subprocess.CompletedProcess[str]:
    command = [_git_bash()]
    if sys.platform == 'win32':
        command.extend(
            (
                '-c',
                'set -e; PATH="$(cygpath -u "$FAKE_BIN"):$PATH"; export PATH; exec "$BASH" "$@"',
                'bash',
            )
        )
    command.append(str(candidate / 'scripts' / _VALIDATOR.name))
    if output_dir is not None:
        command.extend(('--output-dir', str(output_dir)))
    return subprocess.run(
        command,
        cwd=candidate,
        env=env,
        capture_output=True,
        text=True,
        check=False,
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


def _write_wheel(path: Path, files: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, mode='w') as archive:
        for name, contents in files.items():
            archive.writestr(zipfile.ZipInfo(name), contents)


def _replace_wheel_member(path: Path, name: str, contents: bytes) -> None:
    with zipfile.ZipFile(path) as archive:
        files = {member.filename: archive.read(member) for member in archive.infolist()}
    files[name] = contents
    _write_wheel(path, files)


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
    dist_info = 'sg_python_api-0.1.0.dist-info'
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
    _write_wheel(source_wheel, wheel_files)
    _write_wheel(derived_wheel, wheel_files)
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
        record = archive.read('sg_python_api-0.1.0.dist-info/RECORD')
    if field == 'hash':
        damaged = record.replace(b'sha256=', b'sha256=x', 1)
    else:
        damaged = record.replace(b',12\r\n', b',999\r\n', 1)
        assert damaged != record
    _replace_wheel_member(source_wheel, 'sg_python_api-0.1.0.dist-info/RECORD', damaged)

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
    fifo = tarfile.TarInfo('sg_python_api-0.1.0/src/fifo')
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


def test_default_validation_report_preserves_trusted_wheel_path(tmp_path: Path) -> None:
    candidate, env = _prepare_candidate(tmp_path)

    result = _run_validator(candidate, env, None)

    assert result.returncode == 0, result.stderr
    report = (candidate / 'dist' / 'artifact-results.md').read_text(encoding='ascii')
    wheel_name = evidence._field(report, 'Source wheel')
    wheel_hash = evidence._field(report, 'Source wheel SHA256')
    wheel_path = evidence._field(report, 'Preserved source wheel')
    assert wheel_name == _WHEEL_NAME
    assert wheel_hash == hashlib.sha256(b'wheel bytes\n').hexdigest()
    assert wheel_path == f'dist/validated-artifacts/source/{_WHEEL_NAME}'
    assert str(candidate) not in report

    preserved_wheel, digest = evidence._artifact_path(
        candidate, wheel_name, wheel_path, wheel_hash
    )
    assert preserved_wheel == candidate / 'dist' / 'validated-artifacts' / 'source' / _WHEEL_NAME
    assert preserved_wheel.read_bytes() == b'wheel bytes\n'
    assert digest == wheel_hash


@pytest.mark.parametrize('staged', [False, True], ids=['unstaged', 'staged'])
def test_dirty_tracked_artifact_input_is_rejected(tmp_path: Path, *, staged: bool) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    (candidate / '.gitignore').write_text('dist/\n# dirty\n', encoding='ascii')
    if staged:
        subprocess.run(['git', 'add', '.gitignore'], cwd=candidate, env=git_env(), check=True)

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode != 0
    assert 'tracked files to match HEAD exactly' in result.stderr
    assert not output_dir.exists()


@pytest.mark.parametrize(
    ('name', 'public'),
    [
        ('.scratch/notes.md', False),
        ('docs/drafts/notes.md', False),
        ('README.md', True),
        ('docs/guide.md', True),
        ('src/package/module.py', True),
    ],
)
def test_candidate_tracks_public_input_commits(tmp_path: Path, name: str, *, public: bool) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    artifact_input_commit = _head_commit(candidate)
    notes_file = candidate / name
    notes_file.parent.mkdir(parents=True, exist_ok=True)
    notes_file.write_text('# Updated input\n', encoding='ascii')
    subprocess.run(['git', 'add', str(notes_file)], cwd=candidate, env=git_env(), check=True)
    subprocess.run(
        [
            'git',
            '-c',
            'user.name=Artifact Test',
            '-c',
            'user.email=artifact-test@example.invalid',
            'commit',
            '--quiet',
            '-m',
            'update input',
        ],
        cwd=candidate,
        env=git_env(),
        check=True,
    )

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode == 0, result.stderr
    report = (candidate / 'dist' / 'artifact-results.md').read_text(encoding='ascii')
    expected_commit = _head_commit(candidate) if public else artifact_input_commit
    assert f'- Candidate commit: `{expected_commit}`' in report
    assert artifact_input_commit != _head_commit(candidate)


def test_failed_preservation_leaves_no_artifact_output(tmp_path: Path) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    env['FAIL_SECOND_COPY'] = '1'

    result = _run_validator(candidate, env, output_dir)

    assert (tmp_path / 'copy-count').read_text(encoding='ascii') == '2\n'
    assert result.returncode != 0
    assert not output_dir.exists()


def test_failed_sdist_contract_reports_stage_and_leaves_no_artifact_output(
    tmp_path: Path,
) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    env['FAIL_SDIST_CONTRACT'] = '1'

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode != 0
    report = (candidate / 'dist' / 'artifact-results.md').read_text(encoding='ascii')
    assert '- FAIL: raw sdist contract validation' in report
    assert '- FAIL: source wheel and sdist build' not in report
    assert '- PASS: Raw sdist contract validation' not in report
    assert not output_dir.exists()


def test_generated_evidence_does_not_block_validation(tmp_path: Path) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    results_file = candidate / 'dist' / 'artifact-results.md'
    results_file.parent.mkdir(parents=True, exist_ok=True)
    results_file.write_text('# Previous artifact validation results\n', encoding='ascii')

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode == 0, result.stderr


def test_output_directory_must_be_vcs_ignored(tmp_path: Path) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'release-artifacts'

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode != 0
    assert 'VCS-ignored' in result.stderr
    assert not output_dir.exists()


def test_public_checkout_creates_report_parent_before_later_validation_failure(
    tmp_path: Path,
) -> None:
    candidate, env = _prepare_candidate(tmp_path, private_docs=False)
    assert not (candidate / 'dist').exists()
    env['FAIL_SDIST_CONTRACT'] = '1'

    result = _run_validator(candidate, env, candidate / 'dist' / 'validated')

    assert result.returncode != 0
    report = candidate / 'dist' / 'artifact-results.md'
    assert report.is_file(), result.stderr
    assert '- Status: FAIL' in report.read_text(encoding='ascii')
    assert '- FAIL: raw sdist contract validation' in report.read_text(encoding='ascii')
    assert not (candidate / 'docs').exists()
    assert 'No such file or directory' not in result.stderr


def test_release_metadata_contracts_match_project() -> None:
    with (_REPO_ROOT / 'pyproject.toml').open('rb') as source:
        project = tomllib.load(source)['project']
    extras = project['optional-dependencies']
    requirements = {value.replace(' ', '').replace('"', "'") for value in project['dependencies']}
    for extra, dependencies in extras.items():
        requirements.update(
            dependency.replace(' ', '').replace('"', "'") + f";extra=='{extra}'"
            for dependency in dependencies
        )
    assert set(project['classifiers']) == contract._EXPECTED_CLASSIFIERS
    for module in (contract, installed_runner):
        assert project['version'] == module._EXPECTED_VERSION
        assert set(extras) == module._EXPECTED_EXTRAS
        assert {_requirement_signature(value) for value in requirements} == {
            _requirement_signature(value) for value in module._EXPECTED_REQUIREMENTS
        }
        assert project['urls'] == module._EXPECTED_URLS


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
        'COMPATIBILITY.md',
        'LICENSE',
        'Makefile',
        'README.md',
        'docs/guide.md',
        'examples/nested/example.py',
        'pyproject.toml',
        'scripts/check.sh',
        'src/shotgun_api3/__init__.py',
        'tests/test_package.py',
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
    assert (
        "sdist contents differ; missing=['src/alpha.py', 'tests/zeta.py'], unexpected=[]"
    ) in result.stderr


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


def test_successful_validation_preserves_consumable_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    invalid_git_dir = tmp_path / 'not-a-repository'
    invalid_git_dir.write_text('not a Git directory', encoding='ascii')
    for name in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
        monkeypatch.setenv(name, str(invalid_git_dir))
    candidate, env = _prepare_candidate(tmp_path, candidate_name='candidate with spaces')
    output_dir = candidate / 'dist' / 'validated'
    candidate_commit = _head_commit(candidate)

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode == 0, result.stderr
    source_wheel = output_dir / 'source' / _WHEEL_NAME
    source_sdist = output_dir / 'source' / _SDIST_NAME
    derived_wheel = output_dir / 'from-sdist' / _WHEEL_NAME
    assert source_wheel.read_text(encoding='ascii') == 'wheel bytes\n'
    assert source_sdist.read_text(encoding='ascii') == 'sdist bytes\n'
    assert derived_wheel.read_text(encoding='ascii') == 'wheel bytes\n'

    report = (candidate / 'dist' / 'artifact-results.md').read_text(encoding='ascii')
    assert f'- Candidate commit: `{candidate_commit}`' in report
    for artifact in (source_wheel, source_sdist, derived_wheel):
        relative_artifact = artifact.relative_to(candidate).as_posix()
        assert f'`{relative_artifact}`' in report
    assert str(candidate) not in report


def test_synthetic_wheels_ignore_wall_clock_zip_timestamps(tmp_path: Path) -> None:
    first = tmp_path / 'first.whl'
    second = tmp_path / 'second.whl'
    files = {'shotgun_api3/__init__.py': b'package contents\n'}
    stamps = [
        (2026, 10, 7, 12, 0, 0, 2, 280, -1),
        (2026, 10, 7, 12, 0, 2, 2, 280, -1),
    ]

    with patch('zipfile.time.localtime', side_effect=stamps) as clock:
        _write_wheel(first, files)
        _write_wheel(second, files)

    assert first.read_bytes() == second.read_bytes()
    assert clock.call_count == 0


def test_validator_package_inventory_matches_packaged_source() -> None:
    package_source = _REPO_ROOT / 'src' / 'shotgun_api3'
    source_files = {
        path.relative_to(package_source).as_posix()
        for path in package_source.rglob('*')
        if path.is_file() and (path.suffix == '.py' or path.name == 'py.typed')
    }

    assert source_files == contract._EXPECTED_PACKAGE_FILES


def test_validator_runs_sdist_contract_from_candidate_export(tmp_path: Path) -> None:
    candidate, env = _prepare_candidate(tmp_path)
    output_dir = candidate / 'dist' / 'validated'
    marker = tmp_path / 'sdist-contract-marker'
    env['SDIST_CONTRACT_MARKER'] = str(marker)

    result = _run_validator(candidate, env, output_dir)

    assert result.returncode == 0, result.stderr
    helper, export_root, sdist_path = marker.read_text(encoding='ascii').splitlines()
    assert helper.endswith('/export/scripts/_sdist_contract.py')
    assert export_root.endswith('/export')
    assert sdist_path.endswith('/artifacts/sg_python_api-0.1.0.tar.gz')


@pytest.mark.parametrize('git_directory', ['cmd', 'bin', 'mingw64/bin', 'clangarm64/bin'])
def test_validator_uses_git_bash_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, git_directory: str
) -> None:
    git_bin = tmp_path / 'Git' / git_directory
    git_bin.mkdir(parents=True)
    git = git_bin / 'git.exe'
    git.touch()
    git_bash = tmp_path / 'Git' / 'bin' / 'bash.exe'
    git_bash.parent.mkdir(exist_ok=True)
    git_bash.touch()
    wsl_bash = tmp_path / 'Windows' / 'bash.exe'
    wsl_bash.parent.mkdir()
    wsl_bash.touch()

    def fake_which(name: str) -> str:
        return str(git if name == 'git' else wsl_bash)

    monkeypatch.setattr(sys, 'platform', 'win32')
    monkeypatch.setattr(shutil, 'which', fake_which)

    assert _git_bash() == str(git_bash)
