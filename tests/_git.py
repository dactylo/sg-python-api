import os
import shutil
import subprocess
import sys
from pathlib import Path


def git_bash() -> str:
    # A bare `bash` on Windows finds the WSL launcher in System32 before Git Bash.
    if sys.platform != 'win32':
        return 'bash'

    git = shutil.which('git')
    if git is None:
        raise RuntimeError('Git is required to run the Bash script tests')
    # Git Bash puts architecture-specific Git binaries ahead of the cmd wrapper.
    for git_dir in Path(git).resolve().parents:
        for bash in (git_dir / 'bash.exe', git_dir / 'bin' / 'bash.exe'):
            if bash.is_file():
                return str(bash)
    raise RuntimeError('Git Bash is required to run the Bash script tests')


def git_env() -> dict[str, str]:
    # Fixture repositories must not inherit the caller's repository or Git options.
    environment = {
        name: value for name, value in os.environ.items() if not name.startswith('GIT_')
    }
    environment['GIT_CONFIG_NOSYSTEM'] = '1'
    environment['GIT_CONFIG_GLOBAL'] = os.devnull
    return environment


def commit_fixture(
    candidate: Path,
    message: str,
    *,
    identity: tuple[str, str],
) -> None:
    name, email = identity
    subprocess.run(
        [
            'git',
            '-C',
            str(candidate),
            '-c',
            f'user.name={name}',
            '-c',
            f'user.email={email}',
            '-c',
            f'core.hooksPath={os.devnull}',
            '-c',
            'commit.gpgsign=false',
            'commit',
            '--quiet',
            '-m',
            message,
        ],
        env=git_env(),
        capture_output=True,
        text=True,
        check=True,
    )
