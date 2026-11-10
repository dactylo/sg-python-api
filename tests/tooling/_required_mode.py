import os
from collections.abc import Iterable
from pathlib import Path

import pytest

from tests._required_mode import REQUIRED_LIVE_ENV

_REPO_ROOT = Path(__file__).resolve().parents[2]


def configure_isolated_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    present: Iterable[str] = (),
) -> None:
    for name in REQUIRED_LIVE_ENV:
        monkeypatch.delenv(name, raising=False)
    for name in present:
        monkeypatch.setenv(name, 'configured-for-isolated-test')
    inherited = os.environ.get('PYTHONPATH')
    pythonpath = str(_REPO_ROOT) if inherited is None else f'{_REPO_ROOT}{os.pathsep}{inherited}'
    monkeypatch.setenv('PYTHONPATH', pythonpath)


def install_plugin(pytester: pytest.Pytester) -> None:
    pytester.makeconftest("pytest_plugins = ('tests._required_mode',)")
