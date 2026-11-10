import shutil
import subprocess
from pathlib import Path

from tests._git import commit_fixture, git_env
from tests.tooling._wheels import write_wheel

HELPER = Path(__file__).resolve().parents[2] / 'scripts' / '_artifact_evidence.py'


INPUTS = HELPER.with_name('artifact_inputs.txt')


REPORT = 'dist/artifact-results.md'


WHEEL_NAME = 'sg_python_api-9.9.9-py3-none-any.whl'


def git(candidate: Path, *arguments: str) -> str:
    return subprocess.run(
        ['git', '-C', str(candidate), '-c', 'core.excludesFile=/dev/null', *arguments],
        env=git_env(),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def commit_all(candidate: Path) -> None:
    git(candidate, 'add', '.')
    commit_fixture(candidate, 'fixture', identity=('Evidence Test', 'evidence@example.invalid'))


def prepare_artifact_evidence(tmp_path: Path) -> tuple[Path, str, Path, str]:
    candidate = tmp_path / 'candidate'
    (candidate / 'scripts').mkdir(parents=True)
    shutil.copy2(HELPER, candidate / 'scripts' / HELPER.name)
    shutil.copy2(INPUTS, candidate / 'scripts' / INPUTS.name)
    commit = 'a' * 40
    wheel = candidate / 'dist' / 'validated-artifacts' / 'source' / WHEEL_NAME
    wheel.parent.mkdir(parents=True)
    digest = write_wheel(
        wheel,
        {
            'sg_python_api-9.9.9.dist-info/METADATA': (
                'Metadata-Version: 2.4\nName: sg-python-api\nVersion: 9.9.9\n'
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
    candidate, placeholder, wheel, digest = prepare_artifact_evidence(tmp_path)
    (candidate / '.gitignore').write_text('dist/\n', encoding='ascii')
    (candidate / 'input.txt').write_text('validated\n', encoding='ascii')
    git(candidate, 'init', '--quiet')
    commit_all(candidate)
    commit = git(candidate, 'rev-parse', 'HEAD')
    report = candidate / REPORT
    report.write_text(report.read_text().replace(placeholder, commit), encoding='ascii')
    return candidate, commit, wheel, digest
