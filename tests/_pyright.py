import subprocess
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1] / 'pyproject.toml'


def run_pyright(probe: Path, *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(Path(sys.executable).with_name('pyright')),
            '--project',
            str(PROJECT),
            '--pythonpath',
            sys.executable,
            '--outputjson',
            str(probe),
        ],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
