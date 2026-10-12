from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests._git import git_env

_FAKE_GIT = r"""#!/bin/sh
set -eu

printf '%s\n' "$*" >>"$COMMAND_LOG"
if [ "${1:-}" = 'clone' ]; then
    destination=''
    for argument do
        destination=$argument
    done
    mkdir -p "$destination/tests" "$destination/shotgun_api3"
    printf '%s\n' 'official import candidate' >"$destination/shotgun_api3/__init__.py"
    exit 0
fi

if [ "${1:-}" = '-C' ] && [ "${2:-}" != "$CANDIDATE_ROOT" ]; then
    case ${3:-} in
    rev-parse)
        printf '%s\n' "$FAKE_OFFICIAL_COMMIT"
        exit 0
        ;;
    checkout)
        exit 0
        ;;
    esac
fi

# Resolve wrapper fallthrough without rediscovering this fake Git.
PATH=$REAL_GIT_PATH exec "$REAL_GIT" "$@"
"""

_FAKE_PYTHON = r"""#!/bin/sh
set -eu

if [ "${1:-}" = '-I' ] && [ "${2:-}" = '-c' ] &&
    [ "${3:-}" = 'import installer, pytest' ]; then
    exit 0
fi

if [ "${1:-}" = '-m' ] && [ "${2:-}" = 'installer' ]; then
    printf '%s\n' "$*" >>"$COMMAND_LOG"
    destination=''
    previous=''
    for argument do
        if [ "$previous" = '--destdir' ]; then
            destination=$argument
            break
        fi
        previous=$argument
    done
    [ -n "$destination" ]
    package_root="$destination/opt/custom/lib/python3.13/site-packages"
    mkdir -p "$package_root/shotgun_api3"
    printf '%s\n' "__version__ = '0.1.0'" >"$package_root/shotgun_api3/__init__.py"
    exit 0
fi

case ${2:-} in
    */_conformance_setup.py)
        printf '%s\n' "$*" >>"$COMMAND_LOG"
        if [ "${FAKE_SETUP_FAIL:-0}" = 1 ]; then
            printf '%s\n' 'Conformance setup failed' >&2
            exit 1
        fi
        printf '%s\n' 'script-secret' 'human-secret' >"$4/config"
        chmod 600 "$4/config"
        exit 0
        ;;
    */conformance_driver.py)
        printf '%s\n' "$*" >>"$COMMAND_LOG"
        [ "${1:-}" = '-I' ]
        [ -z "${PYTHONPATH+x}" ]
        [ -z "${SG_HUMAN_NAME+x}" ]
        [ -z "${SG_SERVER_URL+x}" ]
        clone_root=${5%/tests}
        [ ! -e "$clone_root/shotgun_api3" ]
        [ -f "$5/config" ]
        if [ "$("$REAL_PYTHON" -c 'import os; print(os.name)')" = 'posix' ]; then
            mode=$("$REAL_PYTHON" -c \
                'import os, sys; print(oct(os.stat(sys.argv[1]).st_mode & 0o777))' \
                "$5/config")
            [ "$mode" = '0o600' ]
        fi
        grep -q 'script-secret' "$5/config"
        grep -q 'human-secret' "$5/config"
        printf '%s\n' 'RAW AUTODESK OUTPUT script-secret human-secret'
        printf '%s\n' 'RAW AUTODESK STDERR' >&2
        case ${FAKE_PYTEST_RC:-0} in
            0)
                xml='<testsuites><testsuite tests="3" failures="0"'
                xml=$xml' errors="0" skipped="1" /></testsuites>'
                ;;
            1)
                xml='<testsuites><testsuite tests="4" failures="1"'
                xml=$xml' errors="1" skipped="1" /></testsuites>'
                ;;
            *)
                xml='<testsuites><testsuite tests="1" failures="0"'
                xml=$xml' errors="0" skipped="0" /></testsuites>'
                ;;
        esac
        printf '%s\n' "$xml" >"$6"
        exit "${FAKE_PYTEST_RC:-0}"
        ;;
esac

exec "$REAL_PYTHON" "$@"
"""

_GIT = shutil.which('git')
_HEADER = (
    '| Date | Candidate commit | Our version | Artifact SHA256 | Official suite ref | '
    'Official suite commit | Passed | Failed | Skipped | Notes |'
)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNNER = _REPO_ROOT / 'scripts' / 'run_conformance.sh'
_SEPARATOR = (
    '| ---- | ---------------- | ----------- | --------------- | '
    '------------------ | --------------------- | ------ | ------ | '
    '------- | ----- |'
)
_WHEEL_NAME = 'sg_python_api-0.1.0-py3-none-any.whl'


def _credential_env() -> dict[str, str]:
    env = git_env()
    env.update(
        {
            'SG_TEST_HUMAN_LOGIN': 'human@example.invalid',
            'SG_TEST_HUMAN_PASSWORD': 'human-secret',
            'SG_TEST_KEY': 'script-secret',
            'SG_TEST_SCRIPT': 'conformance-script',
            'SG_TEST_SITE': 'https://example.invalid',
        },
    )
    return env


def _driver_source() -> str:
    script = _RUNNER.read_text(encoding='ascii')
    return script.split(
        'cat >"$WORK_ROOT/conformance_driver.py" <<\'PY\'\n',
        maxsplit=1,
    )[1].split(
        '\nPY\n\n# The suite receives credentials only through its mode-0600 transient config.',
        maxsplit=1,
    )[0]


def _git(candidate: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    assert _GIT is not None
    return subprocess.run(
        [_GIT, '-C', str(candidate), *arguments],
        env=git_env(),
        capture_output=True,
        text=True,
        check=True,
    )


def _ledger_lines(candidate: Path) -> list[str]:
    ledger = candidate / 'dist' / 'conformance-results.md'
    return ledger.read_text(encoding='ascii').splitlines()


def _run_driver_collection(
    tmp_path: Path,
    module_name: str,
    *,
    import_during_test: bool = False,
    module_present: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    driver_path = tmp_path / 'conformance_driver.py'
    driver_path.write_text(_driver_source(), encoding='ascii')

    install_root = tmp_path / 'installed'
    site_packages = install_root / 'lib' / 'python' / 'site-packages'
    package_root = site_packages / 'shotgun_api3'
    library_root = package_root / 'lib'
    library_root.mkdir(parents=True)
    (package_root / '__init__.py').write_text("__version__ = '0.1.0'\n", encoding='ascii')
    (library_root / '__init__.py').write_text('', encoding='ascii')
    if module_present:
        module_basename = module_name.rsplit('.', maxsplit=1)[1]
        (library_root / f'{module_basename}.py').write_text('', encoding='ascii')

    tests_root = tmp_path / 'tests'
    tests_root.mkdir()
    (tests_root / 'test_applicable.py').write_text(
        'def test_applicable() -> None:\n    pass\n',
        encoding='ascii',
    )
    if import_during_test:
        test_source = f'def test_runtime_import() -> None:\n    import {module_name}\n'
    else:
        test_source = f'import {module_name}\n\n\ndef test_collected() -> None:\n    pass\n'
    (tests_root / 'test_collection.py').write_text(test_source, encoding='ascii')
    results_xml = tmp_path / 'results.xml'
    result = subprocess.run(
        [
            sys.executable,
            '-I',
            str(driver_path),
            str(site_packages),
            str(install_root),
            str(tests_root),
            str(results_xml),
            '0.1.0',
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return result, results_xml


def _run_gate(candidate: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ['sh', str(candidate / 'scripts' / _RUNNER.name)],
        cwd=candidate,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _write_wheel_fixture(candidate: Path) -> str:
    wheel = candidate / 'dist' / 'validated-artifacts' / 'source' / _WHEEL_NAME
    wheel.parent.mkdir(parents=True)
    metadata = zipfile.ZipInfo(
        'sg_python_api-0.1.0.dist-info/METADATA',
        date_time=(1980, 1, 1, 0, 0, 0),
    )
    with zipfile.ZipFile(wheel, mode='w') as archive:
        archive.writestr(
            metadata,
            'Metadata-Version: 2.4\nName: sg-python-api\nVersion: 0.1.0\n',
        )
    return hashlib.sha256(wheel.read_bytes()).hexdigest()


def _prepare_gate_candidate(tmp_path: Path) -> tuple[Path, dict[str, str], str]:
    candidate = tmp_path / 'candidate'
    fake_bin = tmp_path / 'bin'
    (candidate / 'scripts').mkdir(parents=True)
    fake_bin.mkdir()
    shutil.copy2(_RUNNER, candidate / 'scripts' / _RUNNER.name)
    shutil.copy2(
        _REPO_ROOT / 'scripts' / '_artifact_evidence.py',
        candidate / 'scripts' / '_artifact_evidence.py',
    )
    wheel_hash = _write_wheel_fixture(candidate)
    (candidate / '.gitignore').write_text('dist/\n', encoding='ascii')
    (candidate / 'dist' / 'conformance-results.md').write_text(_HEADER + '\n', encoding='ascii')
    (candidate / 'artifact-input.txt').write_text('validated input\n', encoding='ascii')
    _git(candidate, 'init', '--quiet')
    _git(candidate, 'add', '.')
    _git(
        candidate,
        '-c',
        'user.name=Conformance Test',
        '-c',
        'user.email=conformance@example.invalid',
        'commit',
        '--quiet',
        '-m',
        'candidate',
    )
    candidate_commit = _git(candidate, 'rev-parse', 'HEAD').stdout.strip()
    (candidate / 'dist' / 'artifact-results.md').write_text(
        '<!-- Generated by scripts/validate_artifacts.sh. Do not edit by hand. -->\n\n'
        '# Artifact validation results\n\n'
        '- Status: PASS\n'
        f'- Candidate commit: `{candidate_commit}`\n\n'
        '## Artifacts\n\n'
        f'- Source wheel: `{_WHEEL_NAME}`\n'
        f'- Source wheel SHA256: `{wheel_hash}`\n'
        f'- Preserved source wheel: `dist/validated-artifacts/source/{_WHEEL_NAME}`\n',
        encoding='ascii',
    )
    fake_git = fake_bin / 'git'
    fake_git.write_text(_FAKE_GIT, encoding='ascii', newline='\n')
    fake_git.chmod(0o755)
    fake_python = fake_bin / 'python'
    fake_python.write_text(_FAKE_PYTHON, encoding='ascii', newline='\n')
    fake_python.chmod(0o755)

    env = _credential_env()
    env['CANDIDATE_ROOT'] = str(candidate)
    env['COMMAND_LOG'] = str(tmp_path / 'commands.log')
    Path(env['COMMAND_LOG']).write_text('', encoding='ascii')
    env['FAKE_OFFICIAL_COMMIT'] = 'db056df3fb663efa6a69a2c07d2d0b69db35c336'
    env['REAL_GIT_PATH'] = subprocess.run(
        ['sh', '-c', 'printf "%s" "$PATH"'],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    env['PATH'] = f'{fake_bin}{os.pathsep}{env["PATH"]}'
    env['PYTHONPATH'] = '/tainted/source/tree'
    assert _GIT is not None
    env['REAL_GIT'] = _GIT
    env['REAL_PYTHON'] = sys.executable
    env['TMPDIR'] = str(tmp_path / 'work')
    Path(env['TMPDIR']).mkdir()
    return candidate, env, wheel_hash


def test_broken_pytest_run_produces_no_tally(tmp_path: Path) -> None:
    candidate, env, _ = _prepare_gate_candidate(tmp_path)
    env['FAKE_PYTEST_RC'] = '2'
    ledger_before = _ledger_lines(candidate)

    result = _run_gate(candidate, env)

    assert result.returncode == 2
    assert result.stdout == ''
    assert result.stderr == 'Conformance test execution failed (pytest exit 2)\n'
    assert 'passed:' not in result.stdout
    assert _ledger_lines(candidate) == ledger_before
    assert list(Path(env['TMPDIR']).iterdir()) == []


def test_candidate_import_precedes_competing_site_package(tmp_path: Path) -> None:
    driver_path = tmp_path / 'conformance_driver.py'
    driver_path.write_text(_driver_source(), encoding='ascii')

    install_root = tmp_path / 'installed'
    site_packages = install_root / 'lib' / 'python' / 'site-packages'
    candidate = site_packages / 'shotgun_api3'
    candidate.mkdir(parents=True)
    (candidate / '__init__.py').write_text("__version__ = '0.1.0'\n", encoding='ascii')

    competing_site = tmp_path / 'competing-site'
    competitor = competing_site / 'shotgun_api3'
    competitor.mkdir(parents=True)
    (competitor / '__init__.py').write_text("__version__ = '0.1.0'\n", encoding='ascii')

    tests_root = tmp_path / 'tests'
    tests_root.mkdir()
    (tests_root / 'test_candidate.py').write_text(
        'def test_candidate_is_importable():\n'
        '    import shotgun_api3\n'
        "    assert shotgun_api3.__version__ == '0.1.0'\n",
        encoding='ascii',
    )
    results_xml = tmp_path / 'results.xml'
    bootstrap = (
        'import runpy, sys\n'
        'sys.path.insert(0, sys.argv.pop(1))\n'
        'driver = sys.argv.pop(1)\n'
        'sys.argv[0] = driver\n'
        "runpy.run_path(driver, run_name='__main__')\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            '-I',
            '-c',
            bootstrap,
            str(competing_site),
            str(driver_path),
            str(site_packages),
            str(install_root),
            str(tests_root),
            str(results_xml),
            '0.1.0',
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_driver_does_not_skip_documented_httplib2_runtime_import(tmp_path: Path) -> None:
    result, results_xml = _run_driver_collection(
        tmp_path,
        'shotgun_api3.lib.httplib2',
        import_during_test=True,
    )

    assert result.returncode == 1, result.stdout + result.stderr
    xml = results_xml.read_text(encoding='utf-8')
    suite = re.search(r'<testsuite\b([^>]*)>', xml)
    assert suite is not None
    counts = dict(re.findall(r'\b(errors|failures|skipped)="([0-9]+)"', suite.group(1)))
    assert counts == {'errors': '0', 'failures': '1', 'skipped': '0'}


def test_driver_does_not_skip_other_missing_lib_module(tmp_path: Path) -> None:
    result, _results_xml = _run_driver_collection(
        tmp_path,
        'shotgun_api3.lib.other_missing',
    )

    assert result.returncode == 2


def test_driver_imports_present_documented_httplib2_module(tmp_path: Path) -> None:
    result, results_xml = _run_driver_collection(
        tmp_path,
        'shotgun_api3.lib.httplib2',
        module_present=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    xml = results_xml.read_text(encoding='utf-8')
    suite = re.search(r'<testsuite\b([^>]*)>', xml)
    assert suite is not None
    counts = dict(re.findall(r'\b(errors|failures|skipped)="([0-9]+)"', suite.group(1)))
    assert counts == {'errors': '0', 'failures': '0', 'skipped': '0'}


def test_driver_rejects_unadapted_httplib2_collection_import(tmp_path: Path) -> None:
    result, _results_xml = _run_driver_collection(
        tmp_path,
        'shotgun_api3.lib.httplib2',
    )

    assert result.returncode == 2


def test_git_fallthrough_supports_path_resolving_shim(tmp_path: Path) -> None:
    candidate, env, _ = _prepare_gate_candidate(tmp_path)
    shim = tmp_path / 'git-shim'
    shim.write_text(
        '#!/bin/sh\n'
        'set -eu\n'
        'if [ "${SHIM_ACTIVE:-}" = 1 ]; then exit 99; fi\n'
        'export SHIM_ACTIVE=1\n'
        'exec git "$@"\n',
        encoding='ascii',
        newline='\n',
    )
    shim.chmod(0o755)
    env.pop('SHIM_ACTIVE', None)
    env['REAL_GIT'] = str(shim)

    result = subprocess.run(
        ['sh', str(tmp_path / 'bin' / 'git'), '-C', str(candidate), 'rev-parse', 'HEAD'],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == _git(candidate, 'rev-parse', 'HEAD').stdout


def test_missing_artifact_evidence_fails_before_git(tmp_path: Path) -> None:
    candidate = tmp_path / 'candidate'
    fake_bin = tmp_path / 'bin'
    (candidate / 'scripts').mkdir(parents=True)
    fake_bin.mkdir()
    shutil.copy2(_RUNNER, candidate / 'scripts' / _RUNNER.name)
    shutil.copy2(
        _REPO_ROOT / 'scripts' / '_artifact_evidence.py',
        candidate / 'scripts' / '_artifact_evidence.py',
    )
    git_marker = tmp_path / 'git-called'
    fake_git = fake_bin / 'git'
    fake_git.write_text(
        '#!/bin/sh\nset -eu\nprintf called >"$GIT_MARKER"\nexit 99\n',
        encoding='ascii',
        newline='\n',
    )
    fake_git.chmod(0o755)
    env = _credential_env()
    env['GIT_MARKER'] = str(git_marker)
    env['PATH'] = f'{fake_bin}{os.pathsep}{env["PATH"]}'

    result = subprocess.run(
        ['sh', str(candidate / 'scripts' / _RUNNER.name)],
        cwd=candidate,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert 'Missing artifact validation evidence' in result.stderr
    assert not git_marker.exists()


def test_missing_credentials_fail_before_external_commands(tmp_path: Path) -> None:
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    git_marker = tmp_path / 'git-called'
    fake_git = fake_bin / 'git'
    fake_git.write_text(
        '#!/bin/sh\nset -eu\nprintf called >"$GIT_MARKER"\nexit 99\n',
        encoding='ascii',
        newline='\n',
    )
    fake_git.chmod(0o755)
    env = git_env()
    env['GIT_MARKER'] = str(git_marker)
    env['PATH'] = f'{fake_bin}{os.pathsep}{env["PATH"]}'
    for name in (
        'SG_TEST_HUMAN_LOGIN',
        'SG_TEST_HUMAN_PASSWORD',
        'SG_TEST_KEY',
        'SG_TEST_SCRIPT',
        'SG_TEST_SITE',
    ):
        env.pop(name, None)

    result = subprocess.run(
        ['sh', str(_RUNNER)],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert 'Missing required environment variable: SG_TEST_SITE' in result.stderr
    assert not git_marker.exists()


def test_pytest_failures_produce_a_valid_sanitized_tally(tmp_path: Path) -> None:
    candidate, env, wheel_hash = _prepare_gate_candidate(tmp_path)
    env['FAKE_PYTEST_RC'] = '1'
    candidate_commit = _git(candidate, 'rev-parse', 'HEAD').stdout.strip()
    ledger_before = _ledger_lines(candidate)

    result = _run_gate(candidate, env)

    assert result.returncode == 1, result.stderr
    assert 'passed: 1\nfailed: 2\nskipped: 1\n' in result.stdout
    combined_output = result.stdout + result.stderr
    assert 'RAW AUTODESK' not in combined_output
    assert 'script-secret' not in combined_output
    assert 'human-secret' not in combined_output
    ledger_after = _ledger_lines(candidate)
    assert ledger_after[:-1] == ledger_before
    assert ledger_after[-1] == (
        f'| {datetime.now(UTC).date().isoformat()} | {candidate_commit} | 0.1.0 | '
        f'{wheel_hash} | '
        'v3.10.2 | db056df3fb663efa6a69a2c07d2d0b69db35c336 | 1 | 2 | 1 | '
        'FAIL (pytest exit 1) |'
    )
    assert 'RAW AUTODESK' not in ledger_after[-1]
    assert 'script-secret' not in ledger_after[-1]
    assert 'human-secret' not in ledger_after[-1]
    assert list(Path(env['TMPDIR']).iterdir()) == []


def test_setup_failure_stops_before_pytest_and_keeps_ledger(tmp_path: Path) -> None:
    candidate, env, _ = _prepare_gate_candidate(tmp_path)
    env['FAKE_SETUP_FAIL'] = '1'
    ledger_before = _ledger_lines(candidate)

    result = _run_gate(candidate, env)

    assert result.returncode == 1
    assert result.stdout == ''
    assert result.stderr == 'Conformance setup failed\n'
    assert _ledger_lines(candidate) == ledger_before
    assert 'conformance_driver.py' not in Path(env['COMMAND_LOG']).read_text()
    assert list(Path(env['TMPDIR']).iterdir()) == []


@pytest.mark.parametrize('change_state', ['committed', 'staged', 'unstaged'])
def test_stale_artifact_input_fails_before_clone(
    tmp_path: Path,
    change_state: str,
) -> None:
    candidate, env, _ = _prepare_gate_candidate(tmp_path)
    (candidate / 'artifact-input.txt').write_text('stale input\n', encoding='ascii')
    if change_state in {'committed', 'staged'}:
        _git(candidate, 'add', 'artifact-input.txt')
    if change_state == 'committed':
        _git(
            candidate,
            '-c',
            'user.name=Conformance Test',
            '-c',
            'user.email=conformance@example.invalid',
            'commit',
            '--quiet',
            '-m',
            'stale input',
        )

    result = _run_gate(candidate, env)

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == 'Artifact validation evidence is stale\n'
    commands = Path(env['COMMAND_LOG']).read_text(encoding='ascii')
    assert not any(
        marker in commands
        for marker in ('clone ', 'checkout ', '-m installer', 'conformance_driver.py')
    )


def test_success_creates_a_missing_ledger(tmp_path: Path) -> None:
    candidate, env, wheel_hash = _prepare_gate_candidate(tmp_path)
    (candidate / 'dist' / 'conformance-results.md').unlink()
    candidate_commit = _git(candidate, 'rev-parse', 'HEAD').stdout.strip()

    result = _run_gate(candidate, env)

    assert result.returncode == 0, result.stderr
    assert _ledger_lines(candidate) == [
        '# Conformance results',
        '',
        _HEADER,
        _SEPARATOR,
        (
            f'| {datetime.now(UTC).date().isoformat()} | {candidate_commit} | 0.1.0 | '
            f'{wheel_hash} | '
            'v3.10.2 | db056df3fb663efa6a69a2c07d2d0b69db35c336 | 2 | 0 | 1 | PASS |'
        ),
    ]


def test_success_prints_only_sanitized_tally_and_cleans_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    invalid_git_dir = tmp_path / 'not-a-repository'
    invalid_git_dir.write_text('not a Git directory', encoding='ascii')
    for name in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
        monkeypatch.setenv(name, str(invalid_git_dir))
    candidate, env, wheel_hash = _prepare_gate_candidate(tmp_path)
    env['SG_HUMAN_NAME'] = 'must-not-override@example.invalid'
    env['SG_SERVER_URL'] = 'https://must-not-override.invalid'
    candidate_commit = _git(candidate, 'rev-parse', 'HEAD').stdout.strip()
    ledger_before = _ledger_lines(candidate)

    result = _run_gate(candidate, env)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f'candidate commit: {candidate_commit}',
        'candidate version: 0.1.0',
        f'artifact sha256: {wheel_hash}',
        'official suite ref: v3.10.2',
        'official suite commit: db056df3fb663efa6a69a2c07d2d0b69db35c336',
        'passed: 2',
        'failed: 0',
        'skipped: 1',
    ]
    combined_output = result.stdout + result.stderr
    assert 'RAW AUTODESK' not in combined_output
    assert 'script-secret' not in combined_output
    assert 'human-secret' not in combined_output
    ledger_after = _ledger_lines(candidate)
    assert ledger_after[:-1] == ledger_before
    assert ledger_after[-1] == (
        f'| {datetime.now(UTC).date().isoformat()} | {candidate_commit} | 0.1.0 | '
        f'{wheel_hash} | '
        'v3.10.2 | db056df3fb663efa6a69a2c07d2d0b69db35c336 | 2 | 0 | 1 | PASS |'
    )
    assert 'RAW AUTODESK' not in ledger_after[-1]
    assert 'script-secret' not in ledger_after[-1]
    assert 'human-secret' not in ledger_after[-1]
    assert list(Path(env['TMPDIR']).iterdir()) == []

    commands = Path(env['COMMAND_LOG']).read_text(encoding='ascii')
    assert (
        'clone --quiet --no-checkout --depth 1 --branch v3.10.2 '
        'https://github.com/shotgunsoftware/python-api'
    ) in commands
    assert 'checkout --quiet --detach db056df3fb663efa6a69a2c07d2d0b69db35c336' in commands
    assert '-m installer --destdir' in commands
    assert '-I ' in commands


def test_work_root_inside_candidate_fails_before_clone_or_config(tmp_path: Path) -> None:
    candidate, env, _ = _prepare_gate_candidate(tmp_path)
    unsafe_tmpdir = candidate / 'unsafe-tmp'
    unsafe_tmpdir.mkdir()
    env['TMPDIR'] = str(unsafe_tmpdir)

    result = _run_gate(candidate, env)

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == 'Disposable work root must be outside the repository\n'
    combined_output = result.stdout + result.stderr
    assert 'RAW AUTODESK' not in combined_output
    assert 'script-secret' not in combined_output
    assert 'human-secret' not in combined_output
    commands = Path(env['COMMAND_LOG']).read_text(encoding='ascii')
    assert not any(
        marker in commands
        for marker in ('clone ', 'checkout ', '-m installer', 'conformance_driver.py')
    )
    assert list(unsafe_tmpdir.iterdir()) == []
