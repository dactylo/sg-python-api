import os
from dataclasses import dataclass

import pytest
from _pytest.terminal import TerminalReporter

REQUIRED_LIVE_ENV = (
    'SG_TEST_KEY',
    'SG_TEST_PROJECT',
    'SG_TEST_SCRIPT',
    'SG_TEST_SITE',
)


@dataclass
class _RequiredState:
    reject_skips: bool = False
    selected_skips: int = 0


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup('required modes')
    group.addoption(
        '--require-live',
        action='store_true',
        help='require configured live tests and reject selected skips',
    )
    group.addoption(
        '--require-no-skips',
        action='store_true',
        help='fail when any selected test skips',
    )


def pytest_configure(config: pytest.Config) -> None:
    _STATE.reject_skips = bool(
        config.getoption('require_live') or config.getoption('require_no_skips')
    )
    _STATE.selected_skips = 0


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _STATE.reject_skips and report.skipped:
        _STATE.selected_skips += 1


def pytest_runtest_setup(item: pytest.Item) -> None:
    if not item.config.getoption('require_live') or item.get_closest_marker('live') is None:
        return
    missing = [name for name in REQUIRED_LIVE_ENV if not os.environ.get(name)]
    if missing:
        pytest.fail(
            f'required live configuration missing: {", ".join(missing)}',
            pytrace=False,
        )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int | pytest.ExitCode) -> None:
    if not _STATE.reject_skips or not _STATE.selected_skips:
        return
    reporter = session.config.pluginmanager.get_plugin('terminalreporter')
    if isinstance(reporter, TerminalReporter):
        reporter.write_line(f'required mode rejected {_STATE.selected_skips} selected skip(s)')
    if exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


_STATE = _RequiredState()
