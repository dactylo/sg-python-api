from __future__ import annotations

import argparse
import importlib
import inspect
import logging
import os
import signal
import sys
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NoReturn, Self, TextIO, cast
from urllib.parse import urlsplit

import anyio

import shotgun_api3
import tests
from tests._live_safety import LiveBudget, LiveStop
from tests._live_vocabulary import SESSION_NAMES
from tests._sockets import refuse_connections
from tests.live._evidence import (
    digest_bytes,
    exception_label,
    privacy_hits,
    template,
    write_artifact,
)
from tests.live._probe import run_probe
from tests.live._verification import LiveCase, LiveTarget, baseline_equal

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from types import FrameType, ModuleType, TracebackType

    import pytest

_CANARY = ('script_actor_is_api_user', 'update_returns_dictionary', 'unlisted_relation_faults')
_EXTENDED = (
    'extended_archive_scope',
    'extended_project_retirement_cascades',
    'extended_archive_summary_and_search',
    'extended_sudo_users_and_automatic_follows',
    'extended_follow_roundtrip_and_filters',
    'extended_work_schedule_project_exception',
    'extended_subscription_invalid_user_faults',
    'extended_last_accessed_per_actor',
)
_PROFILES: dict[str, tuple[int, int, int, int, int]] = dict(
    zip(
        SESSION_NAMES,
        (
            (100, 30, 0, 0, 10),
            (1500, 100, 0, 0, 30),
            (2250, 150, 9, 3584, 300),
            # L2's unexecuted remainder after its 2026-10-21 stop, within L2's unused allocation.
            (878, 150, 4, 3384, 252),
            (300, 60, 9, 3584, 150),
            (400, 100, 0, 0, 30),
            (120, 30, 1, 1024, 60),
            # The 2026-10-21 confirmation run: scenarios changed or added after Part A.
            (800, 150, 8, 3584, 240),
            (250, 60, 7, 3584, 150),
            (100, 30, 0, 0, 30),
            # The ninth characterization's display names, confirmed live (tracker M1 and M2).
            (120, 40, 0, 0, 10),
            (150, 0, 0, 0, 120),
        ),
        strict=True,
    )
)
_RECHECK = (
    'failed_batch_changes_nothing',
    'unlisted_relation_faults',
    'cascade_retires_children',
    'summary_counts_and_numbers',
    'multi_entity_and_letter_groups',
    'text_search_single_shot',
    'text_search_order',
    'text_search_match_fields',
    'plain_upload_event_rows',
    'attachment_downloads',
    'thumbnail_identities_sharing_and_clearing',
    'note_thread_contents',
    'activity_actions_and_views',
    'week_exact_and_checkbox_group_names',
    'computed_task_note_and_reply_values',
    'attachment_file_dictionary',
)
_RECHECK_ASYNC = (
    'plain_upload_event_rows',
    'thumbnail_identities_sharing_and_clearing',
    'note_thread_contents',
    'attachment_file_dictionary',
)
_SUDO_CREATE_LIMITS = {'L4': 2, 'L5': 1, 'R3': 1}
_UNFOLLOW_LIMITS = {'L4': 6, 'L5': 1, 'R3': 2}
_UPLOADS = (
    'plain_upload_attachment_fields',
    'plain_upload_event_rows',
    'field_upload_links_version',
    'upload_to_missing_record_fails',
    'attachment_downloads',
    'thumbnail_identities_sharing_and_clearing',
    'note_thread_contents',
)


class _Capture:
    def __init__(self) -> None:
        self.error = ''
        self.template = ''

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del error_type, traceback
        if error is not None:
            self.error = exception_label(error)
            self.template = template(error)
        return error is not None


def _after(names: list[str], last: str) -> list[str]:
    return names[names.index(last) + 1 :]


def _check_import_roots() -> None:

    root = Path(__file__).resolve().parents[2]
    assert Path(tests.__file__).resolve() == root / 'tests/__init__.py'
    assert Path(shotgun_api3.__file__).resolve() == root / 'src/shotgun_api3/__init__.py'


def _interrupt(_signum: int, _frame: FrameType | None) -> None:
    raise KeyboardInterrupt


@contextmanager
def _null_stream() -> Generator[TextIO]:
    with Path(os.devnull).open('w') as stream:
        yield stream


@dataclass
class SessionPlugin:
    name: str
    directory: Path
    budget: LiveBudget
    target: ModuleType | None = None
    expected: list[str] = field(default_factory=list[str])
    collected: list[str] = field(default_factory=list[str])
    selected: list[str] = field(default_factory=list[str])
    deselected: dict[str, str] = field(default_factory=dict[str, str])
    executed: list[str] = field(default_factory=list[str])
    outcomes: dict[str, dict[str, object]] = field(default_factory=dict[str, dict[str, object]])
    stop_template: str = ''
    cleanup_verified: bool = True
    baselines: dict[str, object] = field(default_factory=dict[str, object])
    baseline_labels: dict[str, str] = field(
        default_factory=lambda: dict.fromkeys(('F', 'S', 'W', 'P'), 'UNVERIFIED')
    )
    prefix_before: int = 0
    prefix_after: int = 0
    secrets: list[str] = field(default_factory=list[str])
    error: str = ''
    _resolved: LiveCase | None = None

    async def finish(self, case: LiveCase) -> None:
        self.budget.current_test = 'cleanup'
        with self.budget.cleanup():
            current = await case.read_baselines()
            self.baseline_labels = {
                key: 'equal' if baseline_equal(key, current[key], value) else 'different'
                for key, value in self.baselines.items()
            }
            self.prefix_after = await case.prefix_count()
        if self.name == 'Sweep':
            self.baseline_labels = dict.fromkeys(('F', 'S', 'W', 'P'), 'UNVERIFIED')
        await case.call('close')

    async def make_case(self, kind: str) -> LiveCase:
        namespace = f'sg-python-api-live-{uuid.uuid4().hex[:12]}'
        if self.target is None:
            case = await production_case(kind, namespace, self.budget)
        else:
            result = cast('Callable[..., object]', self.target.make_case)(
                kind, namespace, self.budget
            )
            case = cast('LiveCase', await result if inspect.isawaitable(result) else result)
        assert case.target is not None
        if self._resolved is None:
            if self.target is None:
                await _resolve_case(case)
            self._resolved = case
        else:
            assert self._resolved.target is not None
            case.project = self._resolved.project.copy()
            case.target.human = self._resolved.target.human.copy()
            case.run_date = self._resolved.run_date
            case.target.run_date = self._resolved.target.run_date
        if not self.baselines or self.name in {'L4', 'L5', 'R3'}:
            observed = await case.read_baselines()
            if self.baselines and any(
                not baseline_equal(key, observed[key], value)
                for key, value in self.baselines.items()
            ):
                await case.call('close')
                self.stop_template = 'baseline'
                self.budget.refuse('baseline')
            case.target.baselines = self.baselines.copy() if self.baselines else observed
        return case

    async def initialize(self) -> LiveCase:
        case = await self.make_case('sync')
        assert case.target is not None
        self.baselines = case.target.baselines.copy()
        self.prefix_before = await case.prefix_count()
        if self.prefix_before and self.name != 'Sweep':
            self.stop_template = 'baseline'
            raise RuntimeError('live prefix is not empty')
        self.secrets = [value for key, value in os.environ.items() if key.startswith('SG_TEST_')]
        self.secrets.extend(str(case.target.human.get(key, '')) for key in ('login', 'name'))
        self.secrets.append(case.target.site_host)
        if self.name == 'L4':
            start = case.target.run_date + timedelta(days=45)
            start -= timedelta(days=start.weekday())
            end = start + timedelta(days=6)
            rules = cast('list[dict[str, object]]', self.baselines['W'])
            if any(
                str(rule['start_date']) <= end.isoformat()
                and str(rule['end_date']) >= start.isoformat()
                for rule in rules
            ):
                for node in self.expected:
                    if 'extended_work_schedule_project_exception' in node:
                        self.deselected[node] = 'schedule-overlap'
        return case

    def pytest_collection_modifyitems(
        self, config: pytest.Config, items: list[pytest.Item]
    ) -> None:
        pytest = importlib.import_module('pytest')

        self.collected = [item.nodeid for item in items]
        if self.collected != self.expected:
            self.stop_template = 'collection'
            pytest.exit('live collection refused', returncode=2)
        for item in items:
            names = cast('list[str]', getattr(item, 'fixturenames', []))
            if set(names) & {'capsys', 'capfd', 'caplog', 'capsysbinary', 'capfdbinary'}:
                self.stop_template = 'collection'
                pytest.exit('live capture fixture refused', returncode=2)
        removed = [item for item in items if item.nodeid in self.deselected]
        items[:] = [item for item in items if item.nodeid not in self.deselected]
        config.hook.pytest_deselected(items=removed)
        self.selected = [item.nodeid for item in items]

    def pytest_configure(self, config: pytest.Config) -> None:
        if self.target is not None:
            plugin = config.pluginmanager.get_plugin('tests._required_mode')
            if plugin is not None:
                config.pluginmanager.unregister(plugin)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self.outcomes.setdefault(report.nodeid, {})[report.when] = report.outcome
        if report.when == 'teardown' and report.failed:
            self.cleanup_verified = False
            self.budget.stopped = 'live cleanup or restoration failed'

    def pytest_runtest_setup(self, item: pytest.Item) -> None:
        pytest = importlib.import_module('pytest')

        try:
            self.budget.start_case()
        except LiveStop:
            pytest.exit('live session stopped', returncode=2)
        self.budget.current_test = item.nodeid
        self.executed.append(item.nodeid)


def _artifact(plugin: SessionPlugin, started: str) -> dict[str, object]:
    conftest = importlib.import_module('tests.live.conftest')

    tests = conftest.LIVE_EVIDENCE.get('tests', plugin.outcomes)
    return {
        'session': plugin.name,
        'started_utc': started,
        'finished_utc': datetime.now(tz=UTC).isoformat(),
        'collected': plugin.collected,
        'selected': plugin.selected,
        'deselected': plugin.deselected,
        'executed': plugin.executed,
        'tests': tests,
        'counts': plugin.budget.counts,
        'requests_by_test': plugin.budget.requests_by_test,
        'stop_template': plugin.stop_template,
        'baselines': plugin.baseline_labels,
        'prefix_before': plugin.prefix_before,
        'prefix_after': plugin.prefix_after,
        'privacy_hits': 0,
        'cleanup_verified': plugin.cleanup_verified,
        'error': plugin.error,
    }


def _pytest_run(plugin: SessionPlugin) -> int:
    pytest = importlib.import_module('pytest')
    conftest = importlib.import_module('tests.live.conftest')

    pytest.hookimpl(trylast=True)(SessionPlugin.pytest_collection_modifyitems)
    args = [
        *plugin.expected,
        '--capture=no',
        '-p',
        'no:cacheprovider',
        '-p',
        'no:faulthandler',
        '--tb=no',
        '-o',
        'addopts=',
        '-o',
        'log_cli=false',
        '-o',
        'log_file=',
        '--require-live',
        '--live-evidence-dir',
        str(plugin.directory),
    ]
    if plugin.name in {'L4', 'R3', 'R4'}:
        args.append('--live-extended')
    args.extend(['-p', 'anyio.pytest_plugin'])
    conftest.set_session_plugin(plugin)
    conftest.LIVE_STATE['budget'] = plugin.budget
    conftest.LIVE_INITIAL_COUNTS.clear()
    conftest.LIVE_INITIAL_COUNTS.update(plugin.budget.counts)
    conftest.LIVE_EVIDENCE.clear()
    conftest.LIVE_EVIDENCE.update(started_utc=datetime.now(tz=UTC).isoformat(), tests={})
    plugins: list[object] = [plugin, 'tests._required_mode']
    if plugin.target is not None and hasattr(plugin.target, 'nodes'):
        plugins.append(conftest)
    return int(pytest.main(args, plugins=plugins))


def _execute(plugin: SessionPlugin, case: LiveCase) -> int:
    if plugin.name == 'Sweep':
        anyio.run(case.sweep)
        return 0
    if plugin.name != 'L5':
        return _pytest_run(plugin)
    plugin.budget.current_test = 'probe'
    probe_case = anyio.run(plugin.make_case, 'sync')
    try:
        probe = anyio.run(run_probe, probe_case)
        write_artifact(plugin.directory / 'probe.json', probe, [])
    finally:
        anyio.run(probe_case.cleanup)
        anyio.run(probe_case.call, 'close')
    return 0


async def _resolve_case(case: LiveCase) -> None:
    target = case.target
    assert target is not None
    project = await case.call('find_one', 'Project', [['name', 'is', target.project_name]], ['id'])
    human = await case.call(
        'find_one', 'HumanUser', [['login', 'is', target.human_login]], ['name', 'login']
    )
    if not isinstance(project, dict) or not isinstance(human, dict):
        raise TypeError('live target resolution failed')
    case.project = {'type': 'Project', 'id': cast('dict[str, object]', project)['id']}
    target.human = cast('dict[str, object]', human)


def _run(plugin: SessionPlugin) -> int:
    pytest = importlib.import_module('pytest')
    conftest = importlib.import_module('tests.live.conftest')

    def interrupted(signum: int, frame: FrameType | None) -> None:
        plugin.budget.stopped = 'live interrupted'
        _interrupt(signum, frame)

    signal.signal(signal.SIGTERM, interrupted)
    if sys.platform != 'win32':
        signal.signal(signal.SIGHUP, interrupted)
    result = 2
    case = None
    with pytest.MonkeyPatch.context() as patch:
        conftest.patch_live_transports(plugin.budget, patch)
        with _Capture() as failure:
            case = anyio.run(plugin.initialize)
            result = _execute(plugin, case)
        if failure.error:
            plugin.error = failure.error
            plugin.stop_template = plugin.stop_template or failure.template
            plugin.budget.stopped = plugin.budget.stopped or 'live cleanup or restoration failed'
        if case is not None:
            with _Capture() as final_failure:
                anyio.run(plugin.finish, case)
            if final_failure.error:
                plugin.error = final_failure.error
                plugin.stop_template = plugin.stop_template or final_failure.template
    if plugin.budget.stopped:
        plugin.stop_template = plugin.stop_template or template(LiveStop(plugin.budget.stopped))
    if result not in {0, 1} and not plugin.stop_template:
        plugin.stop_template = 'unmatched'
    return result


def _session_scenarios(scenarios: Sequence[Callable[..., object]]) -> dict[str, list[str]]:
    return dict(
        zip(
            SESSION_NAMES,
            (
                list(_CANARY),
                [
                    scenario.__name__
                    for scenario in scenarios[:46]
                    if scenario.__name__ not in _CANARY
                ],
                [scenario.__name__ for scenario in scenarios[46:93]],
                _after(
                    [scenario.__name__ for scenario in scenarios[46:93]], 'attachment_downloads'
                ),
                list(_UPLOADS),
                list(_EXTENDED),
                [],
                [scenario.__name__ for scenario in scenarios if scenario.__name__ in _RECHECK],
                [
                    scenario.__name__
                    for scenario in scenarios
                    if scenario.__name__ in _RECHECK_ASYNC
                ],
                ['extended_sudo_activity_creator'],
                ['extended_other_type_display_names'],
                [],
            ),
            strict=True,
        )
    )


def _silence(stack: ExitStack) -> int:
    descriptor = os.dup(1)
    null = os.open(os.devnull, os.O_WRONLY)
    os.dup2(null, 1)
    os.dup2(null, 2)
    os.close(null)
    sys.stdout = stack.enter_context(_null_stream())
    sys.stderr = stack.enter_context(_null_stream())
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    os.environ.pop('PYTEST_ADDOPTS', None)
    os.environ.pop('PYTEST_PLUGINS', None)
    logging.disable(logging.CRITICAL)
    sys.tracebacklimit = 0
    sys.dont_write_bytecode = True
    signal.signal(signal.SIGTERM, _interrupt)
    if sys.platform != 'win32':
        signal.signal(signal.SIGHUP, _interrupt)
    return descriptor


def block_connections() -> None:

    def refuse() -> NoReturn:
        raise RuntimeError('offline target forbids network')

    refuse_connections(refuse)


async def production_case(kind: str, namespace: str, budget: LiveBudget) -> LiveCase:

    def factory(**options: object) -> object:
        constructor = shotgun_api3.AsyncShotgun if kind == 'async' else shotgun_api3.Shotgun
        return cast('Callable[..., object]', constructor)(
            os.environ['SG_TEST_SITE'],
            script_name=os.environ['SG_TEST_SCRIPT'],
            api_key=os.environ['SG_TEST_KEY'],
            connect=False,
            **options,
        )

    target = LiveTarget(
        factory,
        budget,
        human_login=os.environ['SG_TEST_HUMAN_LOGIN'],
        site_host=urlsplit(os.environ['SG_TEST_SITE']).hostname or '',
        project_name=os.environ['SG_TEST_PROJECT'],
    )
    return LiveCase(
        cast("Literal['sync', 'async']", kind),
        {},
        namespace,
        script_name=os.environ['SG_TEST_SCRIPT'],
        target=target,
    )


def session_budget(name: str) -> LiveBudget:

    requests, allowance, uploads, size, waits = _PROFILES[name]
    limits = {
        'requests': requests,
        'uploads': uploads,
        'upload_bytes': size,
        'waits': waits,
        'human_authentications': 0,
        'schema_creates': 0,
        'subscription_creates': int(name == 'L4'),
        'project_creates': 3 if name == 'L4' else 0,
        'schedule_writes': 3 if name == 'L4' else 0,
        'follows': 4 if name == 'L4' else 0,
        'unfollows': _UNFOLLOW_LIMITS.get(name, 0),
        'sudo_creates': _SUDO_CREATE_LIMITS.get(name, 0),
        'last_accessed_updates': 2 if name == 'L4' else 0,
    }
    return LiveBudget(limits=limits, cleanup_requests=allowance, session=name)


def session_nodes(name: str) -> list[str]:
    scenarios = cast(
        'list[Callable[..., object]]', importlib.import_module('tests._parity').SCENARIOS
    )
    names = _session_scenarios(scenarios)[name]
    function = (
        'test_extended_shared_scenario' if name in {'L4', 'R3', 'R4'} else 'test_shared_scenario'
    )
    kind = 'async' if name in {'L3', 'R2'} else 'sync'
    return [f'tests/live/test_parity.py::{function}[{kind}-{scenario}]' for scenario in names]


def _command(descriptor: int) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('session', nargs='?', choices=SESSION_NAMES)
    parser.add_argument('run_dir', nargs='?', type=Path)
    parser.add_argument('--sweep', type=Path)
    parser.add_argument('--target')
    args = parser.parse_args()
    name = 'Sweep' if args.sweep else cast('str', args.session)
    directory = cast('Path', args.sweep or args.run_dir) / name
    directory.mkdir(parents=True, exist_ok=False)
    _check_import_roots()
    target = None
    if args.target:
        block_connections()
        target = importlib.import_module(cast('str', args.target))
    plugin = SessionPlugin(name, directory, session_budget(name), target)
    plugin.expected = (
        cast('Callable[[str], list[str]]', target.nodes)(name)
        if target is not None and hasattr(target, 'nodes')
        else session_nodes(name)
    )
    started = datetime.now(tz=UTC).isoformat()
    result = _run(plugin)

    artifact = _artifact(plugin, started)
    write_artifact(directory / 'session.json', artifact, plugin.collected)
    hits = privacy_hits(directory, plugin.secrets, plugin.collected)
    artifact['privacy_hits'] = hits
    write_artifact(directory / 'session.json', artifact, plugin.collected)
    phases = list(plugin.outcomes.values())
    mismatches = sum(phase.get('call') == 'failed' for phase in phases)
    # A failed scenario assertion is parity evidence; only safety failures stop the run.
    safe = (
        (result == 0 or (result == 1 and mismatches > 0))
        and not plugin.stop_template
        and plugin.cleanup_verified
        and not hits
        and not plugin.prefix_after
        and not any(phase.get('setup') == 'failed' for phase in phases)
        and all(
            value == ('UNVERIFIED' if name == 'Sweep' else 'equal')
            for value in plugin.baseline_labels.values()
        )
    )
    clean = safe and result == 0
    digest = {
        'session': name,
        'clean': clean,
        'safe': safe,
        'mismatches': mismatches,
        'requests': plugin.budget.counts['requests'],
    }
    os.write(descriptor, digest_bytes(digest))
    return 0 if clean else 1 if safe else 2


def _main() -> int:
    result = 2
    with ExitStack() as stack:
        descriptor = _silence(stack)
        stack.callback(os.close, descriptor)
        with _Capture():
            result = _command(descriptor)
    return result


if __name__ == '__main__':
    raise SystemExit(_main())
