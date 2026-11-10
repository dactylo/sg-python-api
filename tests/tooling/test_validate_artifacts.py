from __future__ import annotations

import ast
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts import _artifact_evidence as evidence
from scripts import _sdist_contract as contract
from scripts import run_installed_tests as installed_runner
from shotgun_api3._version import PACKAGE_VERSION
from tests._git import commit_fixture, git_bash, git_env
from tests.tooling._wheels import write_wheel

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
        mkdir -p -- "$5/sg_python_api-@PACKAGE_VERSION@"
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
    printf '%s\n' 'wheel bytes' >"$output_dir/sg_python_api-@PACKAGE_VERSION@-py3-none-any.whl"
    if [[ " $* " == *' sdist '* ]]; then
        printf '%s\n' 'sdist bytes' >"$output_dir/sg_python_api-@PACKAGE_VERSION@.tar.gz"
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
    if [[ -e $work_root/sdist/sg_python_api-@PACKAGE_VERSION@ ]]; then
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
_SDIST_NAME = f'sg_python_api-{PACKAGE_VERSION}.tar.gz'
_VALIDATOR = _REPO_ROOT / 'scripts' / 'validate_artifacts.sh'
_WHEEL_NAME = f'sg_python_api-{PACKAGE_VERSION}-py3-none-any.whl'


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
    shutil.copy2(
        _REPO_ROOT / 'scripts' / 'artifact_inputs.txt',
        candidate / 'scripts' / 'artifact_inputs.txt',
    )
    (candidate / '.gitignore').write_text('dist/\n', encoding='ascii')
    if private_docs:
        (candidate / 'docs' / 'drafts').mkdir(parents=True)
        (candidate / 'docs' / 'drafts' / 'notes.md').write_text(
            '# Private notes\n',
            encoding='ascii',
        )
    fake_python = fake_bin / 'python'
    fake_python.write_text(
        _FAKE_PYTHON.replace('@PACKAGE_VERSION@', PACKAGE_VERSION), encoding='ascii', newline='\n'
    )
    fake_python.chmod(0o755)
    fake_cp = fake_bin / 'cp'
    fake_cp.write_text(_FAKE_CP, encoding='ascii', newline='\n')
    fake_cp.chmod(0o755)

    subprocess.run(['git', 'init', '--quiet'], cwd=candidate, env=git_env(), check=True)
    subprocess.run(['git', 'add', '.'], cwd=candidate, env=git_env(), check=True)
    commit_fixture(
        candidate, 'test candidate', identity=('Artifact Test', 'artifact-test@example.invalid')
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


def _run_validator(
    candidate: Path,
    env: dict[str, str],
    output_dir: Path | None,
) -> subprocess.CompletedProcess[str]:
    command = [git_bash()]
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


@pytest.mark.parametrize('script_name', ['_sdist_contract.py', 'run_installed_tests.py'])
def test_artifact_helpers_read_source_version_without_importing_package(
    tmp_path: Path, script_name: str
) -> None:
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    script = scripts / script_name
    shutil.copy2(_REPO_ROOT / 'scripts' / script_name, script)
    package = tmp_path / 'src' / 'shotgun_api3'
    package.mkdir(parents=True)
    (package / '__init__.py').write_text(
        "raise AssertionError('package imported')\n", encoding='ascii'
    )
    (package / '_version.py').write_text("COMPATIBILITY_VERSION = '8.8.8'\n", encoding='ascii')
    (tmp_path / 'pyproject.toml').write_text(
        '[project]\nname = "sg-python-api"\nversion = "9.9.9"\n', encoding='ascii'
    )

    result = subprocess.run(
        [
            sys.executable,
            '-I',
            '-S',
            '-c',
            (
                'import runpy, sys; namespace = runpy.run_path(sys.argv[1]); '
                "assert 'shotgun_api3' not in sys.modules; print(namespace['_EXPECTED_VERSION'])"
            ),
            str(script),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == '9.9.9\n'


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
    commit_fixture(
        candidate, 'update input', identity=('Artifact Test', 'artifact-test@example.invalid')
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


def test_release_metadata_matches_pyproject() -> None:
    with (_REPO_ROOT / 'pyproject.toml').open('rb') as source:
        configuration = tomllib.load(source)
    project = configuration['project']
    extras = project['optional-dependencies']
    requirements = {value.replace(' ', '').replace('"', "'") for value in project['dependencies']}
    for extra, dependencies in extras.items():
        requirements.update(
            dependency.replace(' ', '').replace('"', "'") + f";extra=='{extra}'"
            for dependency in dependencies
        )
    assert set(project['classifiers']) == contract._EXPECTED_CLASSIFIERS
    assert project['license'] == contract._EXPECTED_LICENSE == installed_runner._EXPECTED_LICENSE
    assert project['name'] == contract._EXPECTED_NAME == installed_runner._DIST_NAME
    assert (
        project['requires-python']
        == contract._EXPECTED_REQUIRES_PYTHON
        == installed_runner._EXPECTED_REQUIRES_PYTHON
    )
    configured_markers = {
        entry.partition(':')[0]
        for entry in configuration['tool']['pytest']['ini_options']['markers']
    }
    installed_markers = {
        entry.partition(':')[0] for entry in installed_runner._EXPECTED_PYTEST_MARKERS
    }
    assert configured_markers == installed_markers
    ci_source = (_REPO_ROOT / '.github' / 'workflows' / 'ci.yml').read_text(encoding='utf-8')
    match = re.search(
        r'          sdist_roots = (\{.*?^          \})', ci_source, re.MULTILINE | re.DOTALL
    )
    assert match is not None
    ci_roots = ast.literal_eval(match.group(1))
    assert ci_roots == contract._ROOT_SOURCE_FILES | contract._TREE_SOURCE_ROOTS | {
        'PKG-INFO',
    }
    for module in (contract, installed_runner):
        assert PACKAGE_VERSION == module._EXPECTED_VERSION
        assert set(extras) == module._EXPECTED_EXTRAS
        assert {_requirement_signature(value) for value in requirements} == {
            _requirement_signature(value) for value in module._EXPECTED_REQUIREMENTS
        }
        assert project['urls'] == module._EXPECTED_URLS


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
        write_wheel(first, files, fixed_timestamp=True)
        write_wheel(second, files, fixed_timestamp=True)

    assert first.read_bytes() == second.read_bytes()
    assert clock.call_count == 0


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
    assert sdist_path.endswith(f'/artifacts/{_SDIST_NAME}')


@pytest.mark.parametrize('git_directory', ['cmd', 'bin', 'mingw64/bin', 'clangarm64/bin'])
def test_validator_uses_git_bash_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, git_directory: str
) -> None:
    git_bin = tmp_path / 'Git' / git_directory
    git_bin.mkdir(parents=True)
    git = git_bin / 'git.exe'
    git.touch()
    expected = tmp_path / 'Git' / 'bin' / 'bash.exe'
    expected.parent.mkdir(exist_ok=True)
    expected.touch()
    wsl_bash = tmp_path / 'Windows' / 'bash.exe'
    wsl_bash.parent.mkdir()
    wsl_bash.touch()

    def fake_which(name: str) -> str:
        return str(git if name == 'git' else wsl_bash)

    monkeypatch.setattr(sys, 'platform', 'win32')
    monkeypatch.setattr(shutil, 'which', fake_which)

    assert git_bash() == str(expected)
