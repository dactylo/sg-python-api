import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def environment(path: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith('SG_TEST_')}
    env['PYTHONPATH'] = os.pathsep.join([str(path), str(ROOT / 'src'), str(ROOT)])
    env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    return env
