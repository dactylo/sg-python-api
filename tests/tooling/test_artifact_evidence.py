from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from scripts import _artifact_evidence as evidence
from tests._git import git_env

HELPER = Path(__file__).resolve().parents[2] / 'scripts' / '_artifact_evidence.py'
REPORT = 'dist/artifact-results.md'
WHEEL_NAME = 'sg_python_api-0.1.1-py3-none-any.whl'


def _git(candidate: Path, *arguments: str) -> str:
    return subprocess.run(
        ['git', '-C', str(candidate), '-c', 'core.excludesFile=/dev/null', *arguments],
        env=git_env(),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _commit(candidate: Path) -> None:
    _git(candidate, 'add', '.')
    _git(
        candidate,
        '-c',
        'user.name=Evidence Test',
        '-c',
        'user.email=evidence@example.invalid',
        'commit',
        '--quiet',
        '-m',
        'fixture',
    )


def _run(candidate: Path) -> subprocess.CompletedProcess[str]:
    env = git_env()
    env['PYTHONPATH'] = '/tainted/source/tree'
    return subprocess.run(
        [sys.executable, '-I', str(candidate / 'scripts' / HELPER.name), str(candidate)],
        cwd=candidate.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _write_wheel(wheel: Path, metadata: dict[str, str]) -> str:
    with zipfile.ZipFile(wheel, 'w') as archive:
        for name, contents in metadata.items():
            archive.writestr(name, contents)
    return hashlib.sha256(wheel.read_bytes()).hexdigest()


def _prepare_artifact_evidence(tmp_path: Path) -> tuple[Path, str, Path, str]:
    candidate = tmp_path / 'candidate'
    (candidate / 'scripts').mkdir(parents=True)
    shutil.copy2(HELPER, candidate / 'scripts' / HELPER.name)
    commit = 'a' * 40
    wheel = candidate / 'dist' / 'validated-artifacts' / 'source' / WHEEL_NAME
    wheel.parent.mkdir(parents=True)
    digest = _write_wheel(
        wheel,
        {
            'sg_python_api-0.1.1.dist-info/METADATA': (
                'Metadata-Version: 2.4\nName: sg-python-api\nVersion: 0.1.1\n'
            ),
        },
    )
    report = candidate / REPORT
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        '- Status: PASS\n'
        f'- Candidate commit: `{commit}`\n'
        f'- Source wheel: `{WHEEL_NAME}`\n'
        f'- Source wheel SHA256: `{digest}`\n'
        f'- Preserved source wheel: `dist/validated-artifacts/source/{WHEEL_NAME}`\n',
        encoding='ascii',
    )
    return candidate, commit, wheel, digest


def prepare_evidence_candidate(tmp_path: Path) -> tuple[Path, str, Path, str]:
    candidate, placeholder, wheel, digest = _prepare_artifact_evidence(tmp_path)
    (candidate / '.gitignore').write_text('dist/\n', encoding='ascii')
    (candidate / 'input.txt').write_text('validated\n', encoding='ascii')
    _git(candidate, 'init', '--quiet')
    _commit(candidate)
    commit = _git(candidate, 'rev-parse', 'HEAD')
    report = candidate / REPORT
    report.write_text(report.read_text().replace(placeholder, commit), encoding='ascii')
    return candidate, commit, wheel, digest


def test_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    candidate, _, wheel, _ = _prepare_artifact_evidence(tmp_path)
    wheel.write_bytes(b'tampered')

    result = _run(candidate)

    assert result.returncode == 1
    assert result.stderr == 'Artifact SHA-256 does not match validation evidence\n'


def test_git_diff_error_is_not_accepted_as_clean(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def git_status(_root: Path, *arguments: str) -> int:
        return 2 if 'diff' in arguments else 0

    monkeypatch.setattr(evidence, '_git_status', git_status)

    with pytest.raises(evidence._EvidenceError, match='Could not verify artifact validation'):
        evidence._verify_freshness(tmp_path, 'a' * 40)


def test_git_runner_failure_has_a_sanitized_error(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(command)
        assert kwargs == {
            'stdout': subprocess.DEVNULL,
            'stderr': subprocess.DEVNULL,
            'check': False,
        }
        raise OSError('SECRET diagnostic from Git startup')

    with pytest.raises(
        evidence._EvidenceError, match=r'^Could not run artifact validation Git check$'
    ) as error:
        evidence._git_status(tmp_path, 'rev-parse', '--verify', 'HEAD', run=run)

    assert 'SECRET' not in str(error.value)
    assert len(calls) == 1
    assert Path(calls[0][0]).is_absolute()
    assert calls[0][1:] == ['-C', str(tmp_path), 'rev-parse', '--verify', 'HEAD']


def test_isolated_cli_returns_exact_evidence_tuple(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    invalid_git_dir = tmp_path / 'not-a-repository'
    invalid_git_dir.write_text('not a Git directory', encoding='ascii')
    for name in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
        monkeypatch.setenv(name, str(invalid_git_dir))
    candidate, commit, wheel, digest = prepare_evidence_candidate(tmp_path)
    assert _git(candidate, 'check-ignore', '--', REPORT) == REPORT
    assert _git(candidate, 'ls-files', '--', REPORT) == ''
    assert not (candidate / 'docs').exists()

    result = _run(candidate)

    assert result.returncode == 0, result.stderr
    assert result.stderr == ''
    assert result.stdout == f'{commit}|0.1.1|{wheel.resolve()}|{digest}\n'


@pytest.mark.parametrize(
    'name',
    [
        '.gitattributes',
        '.github/workflows/ci.yml',
        'docs/guide.md',
        'examples/nested/example.py',
        'scripts/check.py',
        'src/package/module.py',
        'tests/nested/test_package.py',
    ],
)
def test_new_public_inputs_make_evidence_stale(tmp_path: Path, name: str) -> None:
    candidate, _, _, _ = prepare_evidence_candidate(tmp_path)
    source = candidate / name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('new input\n')
    _commit(candidate)

    result = _run(candidate)

    assert result.returncode == 1
    assert result.stderr == 'Artifact validation evidence is stale\n'


def test_symlink_containment_is_checked_before_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, _, wheel, digest = _prepare_artifact_evidence(tmp_path)
    outside = tmp_path / wheel.name
    outside.write_bytes(wheel.read_bytes())
    wheel.unlink()
    wheel.symlink_to(outside)

    def unexpected_read(_path: Path) -> bytes:
        pytest.fail('read bytes before checking containment')

    monkeypatch.setattr(Path, 'read_bytes', unexpected_read)
    with pytest.raises(evidence._EvidenceError, match='outside the validated source directory'):
        evidence._artifact_path(
            candidate, wheel.name, wheel.relative_to(candidate).as_posix(), digest
        )


@pytest.mark.parametrize('path', ['/outside/wheel.whl', '../wheel.whl'])
def test_unsafe_preserved_paths_are_rejected(tmp_path: Path, path: str) -> None:
    candidate, _, _, _ = _prepare_artifact_evidence(tmp_path)
    report = candidate / REPORT
    report.write_text(
        report.read_text().replace(
            f'dist/validated-artifacts/source/{WHEEL_NAME}',
            path,
        )
    )

    with pytest.raises(
        evidence._EvidenceError,
        match=r'^Unsafe preserved source wheel path in artifact validation evidence$',
    ):
        evidence._load_evidence(candidate)


@pytest.mark.parametrize(
    ('field', 'replacement', 'message'),
    [
        ('- Status: PASS', '- Status: FAIL', 'does not have PASS status'),
        ('Candidate commit', 'Candidate commits', 'field: Candidate commit'),
        ('Source wheel SHA256', 'Source wheel digest', 'field: Source wheel SHA256'),
        ('Preserved source wheel', 'Preserved wheel', 'field: Preserved source wheel'),
    ],
)
def test_missing_or_invalid_fields_are_rejected(
    tmp_path: Path,
    field: str,
    replacement: str,
    message: str,
) -> None:
    candidate, _, _, _ = _prepare_artifact_evidence(tmp_path)
    report = candidate / REPORT
    report.write_text(report.read_text().replace(field, replacement))

    with pytest.raises(evidence._EvidenceError, match=message):
        evidence._load_evidence(candidate)


@pytest.mark.parametrize(
    'label',
    [
        'Candidate commit',
        'Source wheel',
        'Source wheel SHA256',
        'Preserved source wheel',
    ],
)
def test_duplicate_scalar_fields_are_rejected(tmp_path: Path, label: str) -> None:
    candidate, _, _, _ = _prepare_artifact_evidence(tmp_path)
    report = candidate / REPORT
    text = report.read_text()
    duplicate = next(line for line in text.splitlines() if line.startswith(f'- {label}:'))
    report.write_text(text + duplicate + '\n')

    with pytest.raises(
        evidence._EvidenceError, match=f'^Invalid artifact validation evidence field: {label}$'
    ):
        evidence._load_evidence(candidate)


@pytest.mark.parametrize('state', ['unstaged', 'staged', 'staged-restored', 'committed'])
def test_stale_candidate_sources_are_rejected(tmp_path: Path, state: str) -> None:
    candidate, _, _, _ = prepare_evidence_candidate(tmp_path)
    source = candidate / 'input.txt'
    source.write_text('changed\n')
    if state.startswith('staged'):
        _git(candidate, 'add', 'input.txt')
    if state == 'staged-restored':
        source.write_text('validated\n')
    if state == 'committed':
        _commit(candidate)

    result = _run(candidate)

    assert result.returncode == 1
    assert result.stderr == 'Artifact validation evidence is stale\n'


@pytest.mark.parametrize('directory', ['.scratch', 'notes', 'docs/drafts'])
def test_excluded_candidate_paths_do_not_make_evidence_stale(
    tmp_path: Path, directory: str
) -> None:
    candidate, _, _, _ = prepare_evidence_candidate(tmp_path)
    excluded = candidate / directory / 'notes.md'
    excluded.parent.mkdir(parents=True, exist_ok=True)
    excluded.write_text('first\n')
    _commit(candidate)
    excluded.write_text('staged\n')
    _git(candidate, 'add', str(excluded))
    excluded.write_text('unstaged\n')

    result = _run(candidate)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ('metadata', 'message'),
    [
        ({}, 'invalid metadata contents'),
        ({'a.dist-info/METADATA': '', 'b.dist-info/METADATA': ''}, 'invalid metadata contents'),
        ({'a.dist-info/METADATA': 'Name: other\nVersion: 0.1.1\n'}, 'wrong project name'),
        ({'a.dist-info/METADATA': 'Name: sg-python-api\n'}, 'invalid version'),
        (
            {'a.dist-info/METADATA': 'Name: sg-python-api\nVersion: bad|version\n'},
            'invalid version',
        ),
    ],
)
def test_invalid_wheel_metadata_is_rejected(
    tmp_path: Path,
    metadata: dict[str, str],
    message: str,
) -> None:
    candidate, _, wheel, digest = _prepare_artifact_evidence(tmp_path)
    actual_digest = _write_wheel(wheel, metadata)
    report = candidate / REPORT
    report.write_text(report.read_text().replace(digest, actual_digest))

    with pytest.raises(evidence._EvidenceError, match=message):
        evidence._load_evidence(candidate)


@pytest.mark.parametrize('available', [False, True], ids=['unavailable', 'not-ancestor'])
def test_candidate_must_be_an_available_ancestor(tmp_path: Path, *, available: bool) -> None:
    candidate, commit, _, _ = prepare_evidence_candidate(tmp_path)
    other_commit = 'a' * 40
    if available:
        tree = _git(candidate, 'rev-parse', 'HEAD^{tree}')
        other_commit = _git(
            candidate,
            '-c',
            'user.name=Evidence Test',
            '-c',
            'user.email=evidence@example.invalid',
            'commit-tree',
            tree,
            '-m',
            'root',
        )
    report = candidate / REPORT
    report.write_text(report.read_text().replace(commit, other_commit))
    result = _run(candidate)

    assert result.returncode == 1
    assert ('not an ancestor' if available else 'unavailable') in result.stderr


@pytest.mark.parametrize('field', ['commit', 'digest', 'wheel-name'])
def test_invalid_scalar_values_are_rejected(tmp_path: Path, field: str) -> None:
    candidate, commit, _, digest = _prepare_artifact_evidence(tmp_path)
    report = candidate / REPORT
    original, replacement, message = {
        'commit': (commit, 'invalid', 'Invalid candidate commit'),
        'digest': (digest, 'invalid', 'Invalid source wheel SHA-256'),
        'wheel-name': (
            f'- Source wheel: `{WHEEL_NAME}`',
            '- Source wheel: `wrong.whl`',
            'Preserved source wheel does not match',
        ),
    }[field]
    report.write_text(report.read_text().replace(original, replacement))

    with pytest.raises(evidence._EvidenceError, match=message):
        evidence._load_evidence(candidate)
