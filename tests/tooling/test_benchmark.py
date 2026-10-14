from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import threading
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol, Self, TypedDict, TypeVar, cast

import pytest

from shotgun_api3 import _files
from shotgun_api3.lib.sgtimezone import SgTimezone
from tests._git import git_env
from tests._pyright import run_pyright
from tests.tooling.test_artifact_evidence import prepare_evidence_candidate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import Unpack

_EXPECTED_LATENCY_OPERATIONS = (
    'auth_first_request',
    'batch',
    'create',
    'delete',
    'download',
    'find_medium',
    'find_one',
    'find_paged',
    'find_small',
    'parallel_read',
    'payload_decode',
    'payload_encode',
    'schema_entity_read',
    'schema_field_read',
    'session_first_request',
    'summarize',
    'update',
    'upload_direct',
    'upload_generic',
    'upload_multipart',
)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_BENCHMARK = _REPO_ROOT / 'scripts' / 'benchmark.py'
_REQUIRED_ENV = (
    'SG_OFFICIAL_CLIENT',
    'SG_TEST_KEY',
    'SG_TEST_PROJECT',
    'SG_TEST_SCRIPT',
    'SG_TEST_SITE',
)

_MeasuredResult = TypeVar('_MeasuredResult')


class _CleanupWorkerClient:
    def __init__(
        self,
        events: list[tuple[object, ...]],
        present: set[tuple[str, int]],
        *,
        false_delete: int | None = None,
        namespaced: set[tuple[str, int]] | None = None,
        raised_delete: int | None = None,
    ) -> None:
        self.events = events
        self.false_delete = false_delete
        self.namespaced = set[tuple[str, int]]() if namespaced is None else namespaced
        self.present = present
        self.raised_delete = raised_delete

    def close(self) -> None:
        self.events.append(('close', threading.get_ident()))

    def delete(self, entity_type: str, entity_id: int) -> bool:
        self.events.append(('delete', entity_type, entity_id, threading.get_ident()))
        self.namespaced.discard((entity_type, entity_id))
        if entity_id == self.raised_delete:
            raise RuntimeError('RAW delete response')
        if entity_id == self.false_delete:
            return False
        self.present.discard((entity_type, entity_id))
        return True

    def find(
        self,
        entity_type: str,
        filters: list[list[object]],
        _fields: list[str],
    ) -> list[dict[str, object]]:
        self.events.append(('sweep', entity_type, filters, threading.get_ident()))
        return [
            {'type': candidate_type, 'id': candidate_id}
            for candidate_type, candidate_id in sorted(self.namespaced)
            if candidate_type == entity_type
        ]

    def find_one(
        self,
        entity_type: str,
        filters: list[list[object]],
        _fields: list[str] | None = None,
    ) -> dict[str, object] | None:
        if entity_type == 'Project':
            self.events.append(('project', threading.get_ident()))
            return {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}
        entity_id = cast('int', filters[0][2])
        self.events.append(('query', entity_type, entity_id, threading.get_ident()))
        if (entity_type, entity_id) in self.present | self.namespaced:
            return {'type': entity_type, 'id': entity_id}
        return None


class _InterruptedWorkerLauncher:
    def __init__(
        self, benchmark: ModuleType, process_control: BaseException, recovery_case: str
    ) -> None:
        self.benchmark = benchmark
        self.process_control = process_control
        self.recovery_case = recovery_case
        self.events: list[str] = []
        self.worker_path: Path | None = None
        self.original_recover = benchmark._recover_worker

    def __call__(self, config_path: Path, root: Path) -> dict[str, object]:
        journal_type = self.benchmark._ResourceJournal
        config = cast('dict[str, object]', json.loads(config_path.read_text(encoding='ascii')))
        if config['mode'] == 'preflight':
            return _controller_worker_result(self.benchmark, config)
        if config['mode'] == 'cleanup':
            self.events.append('cleanup')
            assert self.worker_path is not None
            assert self.worker_path.exists()
            assert journal_type._load(root)._resources == (
                () if self.recovery_case == 'empty' else (('Shot', 17),)
            )
            if self.recovery_case == 'failure':
                raise RuntimeError('SECRET cleanup response')
            if self.recovery_case == 'interrupt':
                raise KeyboardInterrupt('cleanup interrupt')
            if self.recovery_case == 'exit':
                raise SystemExit(23)
            journal_type._load(root)._clear()
            return {'cleanup': 'PASS'}
        self.worker_path = config_path
        assert journal_type._load(root)._resources == ()
        if self.recovery_case == 'malformed':
            (root / 'resource-journal.json').write_text(
                '{"resources":[{"entity_type":"SECRET-journal","id":17}]}',
                encoding='ascii',
            )
        elif self.recovery_case != 'empty':
            journal_type._load(root)._record('Shot', 17)
        self.events.append('interrupted')
        raise self.process_control

    def recover(self, config: Mapping[str, object], root: Path, sequence: int) -> None:
        self.events.append('recover')
        assert self.worker_path is not None
        assert self.worker_path.exists()
        self.original_recover(config, root, sequence)


class _LatencyObserverClient:
    def __init__(
        self,
        role: str,
        emitted_roles: list[str],
        entity: dict[str, object],
        *,
        emit_events: bool,
    ) -> None:
        self.config = SimpleNamespace(on_request=None)
        self.emit_events = emit_events
        self.emitted_roles = emitted_roles
        self.entity = entity
        self.role = role

    def _emit(self, category: str) -> None:
        observer = self.config.on_request
        if self.emit_events and observer is not None:
            self.emitted_roles.append(self.role)
            observer(SimpleNamespace(operation=category, route='rpc'))

    def close(self) -> None:
        pass

    def find_one(self, *_args: object, **_kwargs: object) -> dict[str, object]:
        self._emit('read')
        return self.entity

    def get_session_token(self) -> str:
        self._emit('session_authentication')
        return 'session-token'


class _LatencyObserverState:
    def __init__(self, _module: object, config: Mapping[str, object]) -> None:
        self.clients: list[_LatencyObserverClient] = []
        self.emit_events = config.get('emit_events', True) is True
        self.emitted_roles = cast('list[str]', config['emitted_roles'])
        self.entities: list[dict[str, object]] = [{'type': 'Shot', 'id': 1, 'code': 'ns-shot-000'}]

    def _cleanup(self) -> None:
        pass

    def _client(self, *, session_token: str | None = None) -> _LatencyObserverClient:
        if not self.clients:
            role = 'control'
        elif len(self.clients) == 1:
            role = 'outer'
        elif session_token is not None:
            role = 'session'
        else:
            role = 'authentication'
        client = _LatencyObserverClient(
            role,
            self.emitted_roles,
            self.entities[0],
            emit_events=self.emit_events,
        )
        self.clients.append(client)
        return client

    def _prepare(self) -> None:
        self._client()


class _ParallelLatencyFailureClient:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def close(self) -> None:
        self.events.append('client:close')


class _ParallelLatencyFailureState:
    def __init__(self, _module: object, config: Mapping[str, object]) -> None:
        self.call_failure = cast('Callable[[], None]', config['call_failure'])
        self.events = cast('list[str]', config['events'])
        self.prepare_failure = cast('Callable[[], None]', config['prepare_failure'])

    def _cleanup(self) -> None:
        self.events.append('state:cleanup')

    def _client(self) -> _ParallelLatencyFailureClient:
        return _ParallelLatencyFailureClient(self.events)

    def _prepare(self) -> None:
        self.events.append('state:prepare')


class _ParallelLatencyFailureOperations:
    def __init__(self, _client: object, state: _ParallelLatencyFailureState) -> None:
        self.state = state

    def _call(self, _name: str, _invocation: int) -> object:
        self.state.events.append('operations:call')
        self.state.call_failure()
        return {'type': 'Shot', 'id': 1, 'code': 'shot-1'}

    def _close(self) -> None:
        self.state.events.append('operations:close')

    def _prepare(self, _name: str) -> None:
        self.state.events.append('operations:prepare')
        self.state.prepare_failure()

    def _verify_result(self, _name: str, _result: object) -> bool:
        return True


class _ParallelRunnerFailureClient:
    def __init__(self, barrier: threading.Barrier, raw_error: BaseException) -> None:
        self.barrier = barrier
        self.close_thread: int | None = None
        self.owner_thread = threading.get_ident()
        self.raw_error = raw_error

    def close(self) -> None:
        self.close_thread = threading.get_ident()

    def find_one(
        self,
        _entity_type: str,
        filters: list[list[object]],
        _fields: list[str],
    ) -> dict[str, object]:
        entity_id = cast('int', filters[0][2])
        self.barrier.wait(timeout=5)
        if entity_id == 3:
            raise self.raw_error
        return {'type': 'Shot', 'id': entity_id, 'code': f'shot-{entity_id}'}


class _ParallelRunnerMeasuredClient:
    def __init__(self, close_threads: list[int]) -> None:
        self.close_threads = close_threads

    def close(self) -> None:
        self.close_threads.append(threading.get_ident())


class _ParallelRunnerFailureState:
    def __init__(self, _module: object, config: Mapping[str, object]) -> None:
        self.barrier = cast('threading.Barrier', config['barrier'])
        self.clients = cast('list[_ParallelRunnerFailureClient]', config['parallel_clients'])
        self.entities = [
            {'type': 'Shot', 'id': identity, 'code': f'shot-{identity}'}
            for identity in range(1, 5)
        ]
        self.lock = cast('threading.Lock', config['lock'])
        self.make_calls = cast('list[int]', config['make_calls'])
        self.measured_close_threads = cast('list[int]', config['measured_close_threads'])
        self.raw_error = cast('BaseException', config['raw_error'])

    def _cleanup(self) -> None:
        pass

    def _client(self) -> _ParallelRunnerMeasuredClient:
        return _ParallelRunnerMeasuredClient(self.measured_close_threads)

    def _make_client(self) -> _ParallelRunnerFailureClient:
        with self.lock:
            self.make_calls[0] += 1
        client = _ParallelRunnerFailureClient(self.barrier, self.raw_error)
        with self.lock:
            self.clients.append(client)
        return client

    def _prepare(self) -> None:
        pass


class _PartialStartClient:
    def __init__(self) -> None:
        self.close_thread: int | None = None
        self.owner_thread = threading.get_ident()

    def close(self) -> None:
        self.close_thread = threading.get_ident()


class _PayloadWriteRecorder:
    def __init__(self) -> None:
        self.lengths: list[int] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exception_type: object,
        _exception: object,
        _traceback: object,
    ) -> None:
        pass

    def write(self, value: bytes) -> int:
        self.lengths.append(len(value))
        return len(value)


class _RecoveryStateSeam(Protocol):
    clients: list[_CleanupWorkerClient]
    download_attachment_id: int
    multipart_payload: Path
    project: dict[str, object]

    def _client(self) -> _CleanupWorkerClient: ...

    def _record_resource(self, entity_type: str, entity_id: int) -> None: ...


class _RecoveryTimeoutLauncher:
    def __init__(self, benchmark: ModuleType) -> None:
        self.benchmark = benchmark
        self.original_launch = benchmark._launch_worker

    def __call__(self, config_path: Path, root: Path) -> dict[str, object]:
        config = cast(
            'dict[str, object]',
            json.loads(config_path.read_text(encoding='ascii')),
        )
        if config['mode'] == 'preflight':
            return _controller_worker_result(self.benchmark, config)
        if config['mode'] == 'cleanup':
            return self.original_launch(config_path, root, env={}, run=self._timed_out)
        self.benchmark._ResourceJournal._load(root)._record('Shot', 987654321)
        raise self.benchmark._BenchmarkError('Benchmark worker failed with status -9')

    @staticmethod
    def _timed_out(
        _command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(
            ['SECRET-command', 'SECRET-path'],
            3600,
            output='SECRET-stdout',
            stderr='SECRET-stderr and traceback',
        )


class _RssFixtureClient:
    def __init__(self, events: list[str], source: Path) -> None:
        self.config = SimpleNamespace(on_request=None)
        self.events = events
        self.source = source

    def download_attachment(self, _identity: int, *, file_path: str) -> str:
        self.events.append('download')
        Path(file_path).write_bytes(self.source.read_bytes())
        return file_path

    def find_one(
        self,
        entity_type: str,
        _filters: object,
        _fields: object = None,
    ) -> dict[str, object]:
        if entity_type == 'Attachment':
            return {'type': 'Attachment', 'id': 7}
        return {'type': 'Version', 'id': 5, 'sg_uploaded_movie': {'id': 7}}

    def upload(self, *_args: object, **_kwargs: object) -> int:
        self.events.append('upload')
        return 7


class _RssFixtureState:
    def __init__(self, events: list[str], source: Path, work_root: Path) -> None:
        self.clients: list[_RssFixtureClient] = []
        self.direct_payload = source
        self.download_attachment_id = 7
        self.events = events
        self.multipart_payload = source
        self.namespace = 'ns'
        self.recorded_resources: list[tuple[str, int]] = []
        self.resources: tuple[tuple[str, int], ...] = ()
        self.server_info = {
            's3_direct_uploads_enabled': True,
            's3_enabled_upload_types': {'Version': ['sg_uploaded_movie']},
        }
        self.source = source
        self.version = {'id': 5}
        self.work_root = work_root

    def _cleanup(self) -> None:
        self.events.append('cleanup')

    def _cleanup_clients(self) -> None:
        self.events.append('client-cleanup')

    def _client(self) -> _RssFixtureClient:
        self.events.append('client')
        client = _RssFixtureClient(self.events, self.source)
        self.clients.append(client)
        return client

    def _prepare(self) -> None:
        self.events.append('full-prepare')

    def _prepare_rss(self, name: str) -> None:
        self.events.append(f'prepare:{name}')
        client = self._client()
        if name == 'rss_download':
            client.upload()

    def _record_resource(self, entity_type: str, entity_id: int) -> None:
        self.recorded_resources.append((entity_type, entity_id))


class _StopBenchmark(BaseException):
    pass


class _ThreadFactory(Protocol):
    def __call__(
        self,
        *,
        target: Callable[[int], None],
        args: tuple[int],
        name: str,
    ) -> threading.Thread: ...


class _PartialStartThread:
    def __init__(
        self,
        thread_type: _ThreadFactory,
        raw_error: BaseException,
        target: Callable[[int], None],
        args: tuple[int],
        name: str,
    ) -> None:
        self.index = args[0]
        self.raw_error = raw_error
        self.started = False
        self.target = target
        self.thread = thread_type(target=target, args=args, name=name)

    @property
    def ident(self) -> int | None:
        return self.thread.ident

    def join(self) -> None:
        if self.started:
            self.thread.join()

    def start(self) -> None:
        if self.index == 2:
            raise self.raw_error
        self.started = True
        self.thread.start()


class _PostStartFailureThread:
    def __init__(
        self,
        thread_type: _ThreadFactory,
        raw_error: BaseException,
        target: Callable[[int], None],
        args: tuple[int],
        name: str,
    ) -> None:
        self.index = args[0]
        self.raw_error = raw_error
        self.target = target
        self.thread = thread_type(target=target, args=args, name=name)

    @property
    def ident(self) -> int | None:
        return self.thread.ident

    def join(self) -> None:
        self.thread.join()

    def start(self) -> None:
        self.thread.start()
        if self.index == 2:
            raise self.raw_error


class _WorkerRunOptions(TypedDict):
    capture_output: bool
    check: bool
    cwd: Path
    env: dict[str, str]
    text: bool
    timeout: int


def _absent_resource(_kind: str, _identity: int) -> None:
    pass


def _assert_controller_configs(
    benchmark: ModuleType,
    configs: list[dict[str, object]],
    install_root: Path,
    official: Path,
) -> None:
    latency_clients = [
        cast('str', config['client']) for config in configs if config['mode'] == 'latency'
    ]
    assert latency_clients == [
        client for order in benchmark._client_orders(2, seed=7) for client in order
    ]
    for config in configs:
        if config['client'] == 'ours':
            assert config['client_root'] == str(install_root / 'site-packages')
            assert config['expected_root'] == str(install_root)
        else:
            assert config['client_root'] == str(official)
            assert config['expected_root'] == str(official)


def _ignore_worker_module(_root: Path) -> None:
    pass


def _load_benchmark() -> ModuleType:
    spec = importlib.util.spec_from_file_location('benchmark_under_test', _BENCHMARK)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _noop() -> None:
    pass


def _route(client: str, name: str) -> str:
    if client == 'official' and name == 'upload_generic':
        return 'capability:form'
    if name == 'upload_direct':
        return (
            'observed:allocation+link+object_put+rpc' if client == 'ours' else 'capability:direct'
        )
    if name == 'upload_multipart':
        return (
            'observed:allocation+finalize+link+part_put+rpc'
            if client == 'ours'
            else 'capability:multipart'
        )
    return 'rpc'


def _operation_result(
    benchmark: ModuleType,
    value: float = 0.1,
    *,
    client: str = 'ours',
) -> dict[str, Any]:
    return {
        name: {
            'category': benchmark._OPERATION_CATEGORIES[name],
            'cold': value,
            'correct': True,
            'route': _route(client, name),
            'warm': value / 2,
        }
        for name in benchmark._LATENCY_OPERATIONS
    }


def _rss_result(client: str, name: str, value: int = 1024) -> dict[str, object]:
    route = 'download'
    if name == 'rss_upload':
        route = (
            'observed:allocation+finalize+link+part_put+rpc'
            if client == 'ours'
            else 'capability:multipart'
        )
    return {
        'baseline_bytes': value,
        'correct': True,
        'peak_bytes': value * 2,
        'peak_delta_bytes': value,
        'route': route,
    }


def _controller_worker_result(
    benchmark: ModuleType,
    config: Mapping[str, object],
) -> dict[str, object]:
    client = cast('str', config['client'])
    if config['mode'] == 'preflight':
        origin = Path(cast('str', config['client_root'])) / 'shotgun_api3' / '__init__.py'
        return {
            'client': client,
            'origin': str(origin.resolve()),
            'version': config['expected_version'],
        }
    if config['mode'] == 'latency':
        return {
            'client': client,
            'operations': _operation_result(benchmark, client=client),
            'rss': {},
        }
    rss_name = cast('str', config['rss_operation'])
    return {
        'client': client,
        'operations': {},
        'rss': {rss_name: _rss_result(client, rss_name)},
    }


def _successful_delete(_kind: str, _identity: int) -> bool:
    return True


def _unused_worker_module(_config: Mapping[str, object]) -> ModuleType:
    return ModuleType('unused_worker_client')


def _worker_config(root: Path, *, mode: str = 'latency') -> dict[str, object]:
    return {
        'api_key': 'api-key',
        'client': 'official',
        'client_root': str(root / 'client'),
        'expected_root': str(root / 'expected'),
        'expected_version': '3.10.2',
        'mode': mode,
        'namespace': 'benchmark-recovery',
        'project': 'Benchmark Project',
        'round': 4,
        'script_name': 'benchmark-script',
        'site': 'https://example.invalid',
        'work_root': str(root),
    }


def test_artifact_evidence_uses_real_isolated_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    invalid_git_dir = tmp_path / 'not-a-repository'
    invalid_git_dir.write_text('not a Git directory', encoding='ascii')
    for name in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
        monkeypatch.setenv(name, str(invalid_git_dir))
    benchmark = _load_benchmark()
    candidate, commit, wheel, digest = prepare_evidence_candidate(tmp_path)

    monkeypatch.setattr(benchmark, '_PROCESS_RUN', partial(subprocess.run, env=git_env()))

    result = benchmark._load_artifact_evidence(candidate)

    assert result.candidate_commit == commit
    assert result.version == '0.1.2'
    assert result.wheel_path == wheel.resolve()
    assert result.sha256 == digest
    wheel.write_bytes(b'tampered')
    with pytest.raises(
        benchmark._BenchmarkError, match=r'^Artifact SHA-256 does not match validation evidence$'
    ):
        benchmark._load_artifact_evidence(candidate)


def test_atomic_report_is_ascii_formatted_sanitized_and_contains_required_evidence(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    result: dict[str, Any] = {
        'artifact_sha256': '1' * 64,
        'candidate_commit': '2' * 40,
        'clients': {
            'official': {
                'operations': {
                    name: {
                        'category': benchmark._OPERATION_CATEGORIES[name],
                        'cold': benchmark._sample_summary([0.2], tail_percentile=0.95),
                        'correct': True,
                        'route': _route('official', name),
                        'warm': benchmark._sample_summary([0.1], tail_percentile=0.95),
                    }
                    for name in benchmark._LATENCY_OPERATIONS
                },
                'rss': {
                    name: {
                        'baseline_bytes': [1024],
                        'correct': True,
                        'peak_bytes': [2048],
                        'peak_delta_bytes': [1024],
                        'route': ('capability:multipart' if name == 'rss_upload' else 'download'),
                    }
                    for name in benchmark._RSS_OPERATIONS
                },
                'version': benchmark._OFFICIAL_VERSION,
            },
            'ours': {
                'operations': {
                    name: {
                        'category': benchmark._OPERATION_CATEGORIES[name],
                        'cold': benchmark._sample_summary([0.1], tail_percentile=0.95),
                        'correct': True,
                        'route': _route('ours', name),
                        'warm': benchmark._sample_summary([0.05], tail_percentile=0.95),
                    }
                    for name in benchmark._LATENCY_OPERATIONS
                },
                'rss': {
                    name: {
                        'baseline_bytes': [512],
                        'correct': True,
                        'peak_bytes': [1024],
                        'peak_delta_bytes': [512],
                        'route': (
                            'observed:allocation+finalize+link+part_put+rpc'
                            if name == 'rss_upload'
                            else 'download'
                        ),
                    }
                    for name in benchmark._RSS_OPERATIONS
                },
                'version': '0.1.2',
            },
        },
        'cleanup': 'PASS',
        'official_commit': benchmark._OFFICIAL_COMMIT,
        'official_ref': benchmark._OFFICIAL_REF,
        'machine': {
            'architecture': 'arm64',
            'os': 'Darwin',
            'platform': 'darwin',
            'python_version': '3.14.0',
        },
        'sample_count': 2,
        'seed': 7,
        'site': 'https://example.invalid',
        'tail_percentile': 0.95,
        'workload': {
            'created_entities': 184,
            'upload_bytes': 332032784,
            'workers': 18,
        },
    }
    destination = tmp_path / 'benchmarks.md'
    destination.write_text('old\n', encoding='ascii')

    benchmark._write_report_atomic(destination, result)

    report = destination.read_text(encoding='ascii')
    format_status = subprocess.run(
        [
            sys.executable,
            '-m',
            'mdformat',
            '--check',
            '--number',
            '--wrap',
            '99',
            str(destination),
        ],
        capture_output=True,
        check=False,
        text=True,
    ).returncode
    text = ' '.join(report.split())
    assert format_status == 0
    assert '# Benchmarks' in report
    assert 'Results status: complete' in report
    assert 'example.invalid' in report
    assert 'Raw cold samples' in report
    assert f'- Artifact SHA-256: `{"1" * 64}`' in report
    assert f'- Official ref: `{benchmark._OFFICIAL_REF}`' in report
    assert f'- Official version: `{benchmark._OFFICIAL_VERSION}`' in report
    assert '- Candidate commit:' not in report
    assert '- Official commit:' not in report
    assert '2' * 40 not in report
    assert benchmark._OFFICIAL_COMMIT not in report
    assert 'pinned ref and commit' not in text
    assert (
        'The official client was loaded only from the caller-supplied checkout at the recorded '
        'official ref.' in text
    )
    assert 'default 2 samples it is the maximum observed value and is descriptive' in report
    assert 'Every isolated worker has a 3,600-second wall-clock timeout' in text
    assert 'After any live worker failure, a fresh serial worker recovers' in text
    assert 'journaled resources and verifies the benchmark namespace' in text
    assert 'Cleanup failure or timeout blocks report publication' in text
    assert 'default 2 paired rounds' in text
    assert '18 worker processes' in text
    assert '184 server entities' in text
    assert '332,032,784 uploaded bytes' in text
    assert '`BENCH_N=20` hard ceiling' in text
    assert '162 worker processes' in text
    assert '1,840 server entities' in text
    assert '3,320,327,840 uploaded bytes' in text
    assert 'RSS download uses a fresh download-only process after setup' in text
    assert 'remains an explicit publication choice' in text
    assert 'larger even `BENCH_N` up to 20 for publication' in report
    assert not list(tmp_path.glob('.benchmarks.md.*'))
    invalid_candidate = dict(result, candidate_commit='invalid')
    with pytest.raises(benchmark._BenchmarkError, match='candidate commit'):
        benchmark._write_report_atomic(destination, invalid_candidate)
    invalid_official = dict(result, official_commit='0' * 40)
    with pytest.raises(benchmark._BenchmarkError, match='official identity'):
        benchmark._write_report_atomic(destination, invalid_official)
    unsafe = dict(result, site='https://user:secret@example.invalid')
    with pytest.raises(benchmark._BenchmarkError, match='credentials'):
        benchmark._write_report_atomic(destination, unsafe)
    assert 'secret' not in destination.read_text(encoding='ascii')


@pytest.mark.parametrize(
    ('work_error', 'close_error', 'unlink_error'),
    [
        (
            KeyboardInterrupt(),
            OSError('RAW close failure'),
            OSError('RAW unlink failure'),
        ),
        (
            OSError('RAW write failure'),
            SystemExit(17),
            OSError('RAW unlink failure'),
        ),
    ],
    ids=('work-process-control', 'cleanup-process-control'),
)
def test_atomic_report_preserves_first_process_control_and_finishes_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    work_error: BaseException,
    close_error: BaseException,
    unlink_error: BaseException,
) -> None:
    benchmark = _load_benchmark()
    process_control = next(
        error
        for error in (work_error, close_error, unlink_error)
        if not isinstance(error, Exception)
    )
    events: list[str] = []
    temporary = tmp_path / '.benchmarks.md.test'

    def close(descriptor: int) -> None:
        assert descriptor == 17
        events.append('close')
        raise close_error

    def make_temporary(**_kwargs: object) -> tuple[int, str]:
        return 17, str(temporary)

    def render(_result: Mapping[str, Any]) -> str:
        return 'new page\n'

    def write(descriptor: int, _value: bytes) -> int:
        assert descriptor == 17
        events.append('write')
        raise work_error

    def unlink(path: Path, *, missing_ok: bool = False) -> None:
        assert path == temporary
        assert missing_ok is True
        events.append('unlink')
        raise unlink_error

    monkeypatch.setattr(benchmark, '_render_report', render)
    monkeypatch.setattr(benchmark.os, 'close', close)
    monkeypatch.setattr(benchmark.os, 'write', write)
    monkeypatch.setattr(benchmark.tempfile, 'mkstemp', make_temporary)
    monkeypatch.setattr(Path, 'unlink', unlink)

    with pytest.raises(type(process_control)) as captured:
        benchmark._write_report_atomic(tmp_path / 'benchmarks.md', {})

    assert captured.value is process_control
    assert events == ['write', 'close', 'unlink']


def test_atomic_report_replace_failure_preserves_prior_page(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    destination = tmp_path / 'benchmarks.md'
    destination.write_text('prior page\n', encoding='ascii')

    def render(_result: Mapping[str, Any]) -> str:
        return 'new page\n'

    monkeypatch.setattr(benchmark, '_render_report', render)

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError('replace failed')

    monkeypatch.setattr(Path, 'replace', fail_replace)

    with pytest.raises(benchmark._BenchmarkError, match='atomically write'):
        benchmark._write_report_atomic(destination, {})

    assert destination.read_text(encoding='ascii') == 'prior page\n'
    assert not list(tmp_path.glob('.benchmarks.md.*'))


def test_atomic_report_sanitizes_ordinary_work_and_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    temporary = tmp_path / '.benchmarks.md.test'

    def close(descriptor: int) -> None:
        assert descriptor == 17
        events.append('close')
        raise OSError('RAW close failure')

    def make_temporary(**_kwargs: object) -> tuple[int, str]:
        return 17, str(temporary)

    def render(_result: Mapping[str, Any]) -> str:
        return 'new page\n'

    def write(descriptor: int, _value: bytes) -> int:
        assert descriptor == 17
        events.append('write')
        raise OSError('RAW write failure')

    def unlink(path: Path, *, missing_ok: bool = False) -> None:
        assert path == temporary
        assert missing_ok is True
        events.append('unlink')
        raise OSError('RAW unlink failure')

    monkeypatch.setattr(benchmark, '_render_report', render)
    monkeypatch.setattr(benchmark.os, 'close', close)
    monkeypatch.setattr(benchmark.os, 'write', write)
    monkeypatch.setattr(benchmark.tempfile, 'mkstemp', make_temporary)
    monkeypatch.setattr(Path, 'unlink', unlink)

    with pytest.raises(benchmark._BenchmarkError, match='atomically write') as captured:
        benchmark._write_report_atomic(tmp_path / 'benchmarks.md', {})

    assert 'RAW' not in str(captured.value)
    assert events == ['write', 'close', 'unlink']


def test_auth_cold_call_constructs_client_and_warm_call_reuses_it() -> None:
    benchmark = _load_benchmark()

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def find_one(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            self.calls += 1
            return {'type': 'Shot', 'id': 1, 'code': 'ns-shot-000'}

    outer = Client()
    fresh = Client()
    constructions: list[None] = []

    def client() -> Client:
        constructions.append(None)
        return fresh

    state = SimpleNamespace(
        _client=client,
        entities=[{'type': 'Shot', 'id': 1, 'code': 'ns-shot-000'}],
    )
    operations = benchmark._Operations(outer, state)

    operations._call('auth_first_request', 0)
    operations._call('auth_first_request', 1)

    assert len(constructions) == 1
    assert fresh.calls == 2
    assert outer.calls == 0


@pytest.mark.parametrize(
    ('operation_name', 'expected_roles'),
    [
        ('auth_first_request', ['authentication', 'authentication']),
        ('session_first_request', ['control', 'session', 'session']),
    ],
)
def test_auth_rows_accept_route_and_category_events_from_secondary_clients(
    monkeypatch: pytest.MonkeyPatch,
    operation_name: str,
    expected_roles: list[str],
) -> None:
    benchmark = _load_benchmark()
    emitted_roles: list[str] = []

    monkeypatch.setattr(benchmark, '_LATENCY_OPERATIONS', (operation_name,))
    monkeypatch.setattr(benchmark, '_LiveState', _LatencyObserverState)

    result = benchmark._latency_worker(
        SimpleNamespace(),
        {'client': 'ours', 'emitted_roles': emitted_roles},
    )

    row = result['operations'][operation_name]
    assert row['category'] == benchmark._OPERATION_CATEGORIES[operation_name]
    assert row['route'] == 'observed:rpc'
    assert emitted_roles == expected_roles


def test_batch_result_pair_arity_is_independent_of_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    operations = benchmark._Operations(SimpleNamespace(), SimpleNamespace())
    responses = [
        {'description': 'first'},
        {'description': 'second'},
        {'description': 'third'},
    ]
    expected = ['first', 'second', 'third']

    monkeypatch.setattr(benchmark, '_BATCH_SIZE', 3)

    assert operations._verify_result('batch', (responses, expected)) is True
    assert operations._verify_result('batch', (responses,)) is False
    assert operations._verify_result('batch', (responses, expected, None)) is False
    assert operations._verify_result('batch', (responses, ['first', 'wrong', 'third'])) is False


@pytest.mark.parametrize(
    ('enabled', 'allowed', 'entity_type', 'field_name', 'expected'),
    [
        (False, {'Version': ['sg_uploaded_movie']}, 'Version', 'sg_uploaded_movie', False),
        (True, {'Shot': ['sg_file']}, 'Shot', 'sg_file', True),
        (True, {'*': '*'}, 'Shot', None, True),
        (True, {'Version': '*'}, 'Version', 'sg_other', True),
        (True, {'Version': ['sg_uploaded_movie']}, 'Version', 'sg_uploaded_movie', True),
        (True, {'Version': ['*']}, 'Version', 'sg_other', True),
        (True, {}, 'Version', 'sg_uploaded_movie', True),
        (True, {}, 'Version', 'sg_other', False),
        (True, {}, 'Shot', 'sg_uploaded_movie', False),
        (True, {'Version': ['sg_other']}, 'Shot', None, False),
    ],
    ids=[
        'direct-disabled',
        'requested-entity-allowlist',
        'global-entity-wildcard',
        'literal-field-wildcard',
        'requested-field-allowlist',
        'field-list-wildcard',
        'version-movie-fallback',
        'version-nonmatching-field-rejected',
        'non-version-movie-rejected',
        'nonmatching-entity-and-field',
    ],
)
def test_benchmark_upload_route_matches_library_matrix(
    enabled: bool,
    allowed: Mapping[str, object],
    entity_type: str,
    field_name: str | None,
    expected: bool,
) -> None:
    benchmark = _load_benchmark()
    server_info: Mapping[str, object] = MappingProxyType(
        {
            's3_direct_uploads_enabled': enabled,
            's3_enabled_upload_types': allowed,
        }
    )

    assert _files._requires_direct_upload(server_info, entity_type, field_name) is expected
    assert benchmark._capability_upload_route(
        server_info,
        entity_type,
        field_name,
        1,
    ) == ('capability:direct' if expected else 'capability:form')


def test_benchmark_upload_route_uses_size_boundary() -> None:
    benchmark = _load_benchmark()
    classify = benchmark._capability_upload_route
    server_info = {
        's3_direct_uploads_enabled': True,
        's3_enabled_upload_types': {'Version': ['sg_uploaded_movie']},
    }

    assert (
        classify(
            server_info,
            'Version',
            'sg_uploaded_movie',
            20_000_000,
        )
        == 'capability:direct'
    )
    assert (
        classify(
            server_info,
            'Version',
            'sg_uploaded_movie',
            20_000_001,
        )
        == 'capability:multipart'
    )


@pytest.mark.parametrize('delete_result', [False, RuntimeError('already absent')])
def test_cleanup_accepts_attachment_delete_failure_only_when_absent(delete_result: object) -> None:
    benchmark = _load_benchmark()

    def delete(_kind: str, _identity: int) -> object:
        if isinstance(delete_result, BaseException):
            raise delete_result
        return delete_result

    benchmark._cleanup_resources(
        [('Attachment', 7)],
        delete,
        _absent_resource,
    )


def test_cleanup_attempts_every_delete_and_query_then_rejects_residue() -> None:
    benchmark = _load_benchmark()
    resources = [('Shot', 1), ('Attachment', 2), ('Version', 3)]
    calls: list[tuple[str, str, int]] = []

    def delete(kind: str, identity: int) -> bool:
        calls.append(('delete', kind, identity))
        return True

    def find_one(kind: str, identity: int) -> object:
        calls.append(('query', kind, identity))
        return {'type': kind, 'id': identity} if kind == 'Attachment' else None

    with pytest.raises(benchmark._CleanupError, match='residue'):
        benchmark._cleanup_resources(resources, delete, find_one)

    reverse = list(reversed(resources))
    assert calls == [
        *(('delete', kind, identity) for kind, identity in reverse),
        *(('query', kind, identity) for kind, identity in reverse),
    ]


def test_cleanup_failure_takes_precedence_without_raw_details() -> None:
    benchmark = _load_benchmark()

    def work() -> None:
        raise ValueError('RAW SERVER PAYLOAD')

    def cleanup() -> None:
        raise RuntimeError('SECRET cleanup response')

    with pytest.raises(benchmark._CleanupError, match='cleanup failed') as captured:
        benchmark._run_with_cleanup(work, cleanup)

    assert 'RAW SERVER PAYLOAD' not in str(captured.value)
    assert 'SECRET' not in str(captured.value)


def test_cleanup_rejects_unverifiable_identity_after_all_queries() -> None:
    benchmark = _load_benchmark()
    queried: list[int] = []

    def find_one(_kind: str, identity: int) -> None:
        queried.append(identity)
        if identity == 2:
            raise RuntimeError('query failed')

    with pytest.raises(benchmark._CleanupError, match='unverifiable'):
        benchmark._cleanup_resources(
            [('Shot', 1), ('Attachment', 2)],
            _successful_delete,
            find_one,
        )

    assert queried == [2, 1]


def test_cleanup_resources_finishes_attempts_then_propagates_process_control() -> None:
    benchmark = _load_benchmark()
    resources = [('Shot', 1), ('Attachment', 2)]
    calls: list[tuple[str, str, int]] = []
    process_control = KeyboardInterrupt()

    def delete(kind: str, identity: int) -> None:
        calls.append(('delete', kind, identity))
        if identity == 2:
            raise process_control

    def find_one(kind: str, identity: int) -> None:
        calls.append(('query', kind, identity))

    with pytest.raises(KeyboardInterrupt) as captured:
        benchmark._cleanup_resources(resources, delete, find_one)

    assert captured.value is process_control
    reverse = list(reversed(resources))
    assert calls == [
        *(('delete', kind, identity) for kind, identity in reverse),
        *(('query', kind, identity) for kind, identity in reverse),
    ]


def test_cleanup_worker_attempts_all_work_and_retains_journal_on_failure(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    resources = (('Shot', 1), ('Attachment', 2), ('Version', 3))
    for entity_type, entity_id in resources:
        journal._record(entity_type, entity_id)
    events: list[tuple[object, ...]] = []
    client = _CleanupWorkerClient(events, {('Attachment', 2)}, false_delete=2)

    def make_client(*_args: object, **_kwargs: object) -> _CleanupWorkerClient:
        return client

    module = SimpleNamespace(Shotgun=make_client)

    with pytest.raises(benchmark._CleanupError, match='cleanup failed') as captured:
        benchmark._cleanup_worker(
            module,
            _worker_config(tmp_path, mode='cleanup'),
        )

    assert 'Attachment' not in str(captured.value)
    assert '2' not in str(captured.value)
    assert journal_type._load(tmp_path)._resources == resources
    assert [event[:3] for event in events if event[0] == 'delete'] == [
        ('delete', 'Version', 3),
        ('delete', 'Attachment', 2),
        ('delete', 'Shot', 1),
    ]
    assert [event[:3] for event in events if event[0] == 'query'] == [
        ('query', 'Version', 3),
        ('query', 'Attachment', 2),
        ('query', 'Shot', 1),
    ]
    assert events[-1][0] == 'close'


def test_cleanup_worker_deletes_and_verifies_every_namespaced_identity(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal_type._create(tmp_path)
    events: list[tuple[object, ...]] = []
    client = _CleanupWorkerClient(
        events,
        set(),
        false_delete=17,
        namespaced={('Attachment', 16), ('Shot', 17), ('Version', 18)},
        raised_delete=18,
    )

    def make_client(*_args: object, **_kwargs: object) -> _CleanupWorkerClient:
        return client

    result = benchmark._cleanup_worker(
        SimpleNamespace(Shotgun=make_client),
        _worker_config(tmp_path, mode='cleanup'),
    )

    assert result == {'cleanup': 'PASS'}
    assert client.namespaced == set()
    worker_thread = threading.get_ident()
    assert events == [
        ('project', worker_thread),
        (
            'sweep',
            'Attachment',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['filename', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        ('delete', 'Attachment', 16, worker_thread),
        ('query', 'Attachment', 16, worker_thread),
        (
            'sweep',
            'Shot',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['code', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        ('delete', 'Shot', 17, worker_thread),
        ('query', 'Shot', 17, worker_thread),
        (
            'sweep',
            'Version',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['code', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        ('delete', 'Version', 18, worker_thread),
        ('query', 'Version', 18, worker_thread),
        ('close', worker_thread),
    ]
    assert journal_type._load(tmp_path)._resources == ()


def test_cleanup_worker_deletes_verifies_sweeps_closes_and_clears_serially(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    resources = (('Shot', 1), ('Attachment', 2), ('Version', 3))
    for entity_type, entity_id in resources:
        journal._record(entity_type, entity_id)
    events: list[tuple[object, ...]] = []
    client = _CleanupWorkerClient(
        events,
        {('Shot', 1)},
        false_delete=2,
        raised_delete=3,
    )

    def make_client(*_args: object, **_kwargs: object) -> _CleanupWorkerClient:
        return client

    module = SimpleNamespace(Shotgun=make_client)

    result = benchmark._cleanup_worker(
        module,
        _worker_config(tmp_path, mode='cleanup'),
    )

    assert result == {'cleanup': 'PASS'}
    assert journal_type._load(tmp_path)._resources == ()
    worker_thread = threading.get_ident()
    assert events == [
        ('project', worker_thread),
        ('delete', 'Version', 3, worker_thread),
        ('delete', 'Attachment', 2, worker_thread),
        ('delete', 'Shot', 1, worker_thread),
        ('query', 'Version', 3, worker_thread),
        ('query', 'Attachment', 2, worker_thread),
        ('query', 'Shot', 1, worker_thread),
        (
            'sweep',
            'Attachment',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['filename', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        (
            'sweep',
            'Shot',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['code', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        (
            'sweep',
            'Version',
            [
                ['project', 'is', {'type': 'Project', 'id': 91, 'name': 'Benchmark Project'}],
                ['code', 'starts_with', 'benchmark-recovery'],
            ],
            worker_thread,
        ),
        ('close', worker_thread),
    ]


def test_cleanup_worker_malformed_journal_fails_before_live_client(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    (tmp_path / 'resource-journal.json').write_text(
        '{"resources":[{"entity_type":"SECRET-journal","id":987654321}]}\n',
        encoding='ascii',
    )
    client_created = False

    def create_client(*_args: object, **_kwargs: object) -> object:
        nonlocal client_created
        client_created = True
        raise AssertionError('live client must not be created')

    with pytest.raises(benchmark._BenchmarkError, match='resource journal') as captured:
        benchmark._cleanup_worker(
            SimpleNamespace(Shotgun=create_client),
            _worker_config(tmp_path, mode='cleanup'),
        )

    assert client_created is False
    assert 'SECRET' not in str(captured.value)
    assert '987654321' not in str(captured.value)


def test_cleanup_worker_mode_uses_exact_base_configuration(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    config_path = tmp_path / 'cleanup.json'
    config = _worker_config(tmp_path, mode='cleanup')
    config_path.write_text(json.dumps(config), encoding='ascii')

    assert benchmark._read_worker_config(config_path) == config


def test_cleanup_worker_result_dispatches_and_verifies_import_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    expected = tmp_path / 'expected'
    expected.mkdir()
    module = SimpleNamespace()
    calls: list[str] = []
    config_path = tmp_path / 'worker.json'
    config_path.write_text('{}', encoding='ascii')
    current_config: dict[str, Any] = {}

    def import_client(_config: Mapping[str, Any]) -> object:
        return module

    def cleanup_worker(_module: object, _config: Mapping[str, Any]) -> dict[str, str]:
        calls.append('cleanup')
        return {'cleanup': 'PASS'}

    def latency_worker(_module: object, _config: Mapping[str, Any]) -> dict[str, str]:
        calls.append('latency')
        return {'mode': 'latency'}

    def read_config(_path: Path) -> dict[str, Any]:
        return current_config

    def rss_worker(_module: object, _config: Mapping[str, Any]) -> dict[str, str]:
        calls.append('rss')
        return {'mode': 'rss'}

    def verify_modules(root: Path) -> None:
        calls.append(f'verify:{root}')

    monkeypatch.setattr(benchmark, '_import_worker_client', import_client)
    monkeypatch.setattr(benchmark, '_verify_worker_modules', verify_modules)
    monkeypatch.setattr(benchmark, '_cleanup_worker', cleanup_worker)
    monkeypatch.setattr(benchmark, '_latency_worker', latency_worker)
    monkeypatch.setattr(benchmark, '_rss_worker', rss_worker)
    monkeypatch.setattr(benchmark, '_read_worker_config', read_config)

    for mode in ('cleanup', 'latency', 'rss'):
        current_config = {
            'expected_root': str(expected),
            'mode': mode,
            'work_root': str(tmp_path),
        }
        expected_result = {'cleanup': 'PASS'} if mode == 'cleanup' else {'mode': mode}
        assert benchmark._worker_result(config_path) == expected_result

    assert calls == [
        'cleanup',
        f'verify:{expected}',
        'latency',
        f'verify:{expected}',
        'rss',
        f'verify:{expected}',
    ]


def test_cleanup_worker_timeout_blocks_report_publication_without_leaks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    official = tmp_path / 'official'
    (official / 'shotgun_api3').mkdir(parents=True)
    (official / 'shotgun_api3' / '__init__.py').touch()
    evidence = benchmark._ArtifactEvidence('a' * 40, 'b' * 64, '0.1.2', tmp_path / 'wheel')
    report_called = False
    for name in _REQUIRED_ENV:
        monkeypatch.setenv(name, f'SECRET-{name}')
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')

    def load(_root: Path) -> object:
        return evidence

    def official_checkout(_repo_root: Path, _official_root: Path) -> Path:
        return official

    monkeypatch.setattr(benchmark, '_preflight_tools', _noop)
    monkeypatch.setattr(benchmark, '_load_artifact_evidence', load)
    monkeypatch.setattr(benchmark, '_official_checkout', official_checkout)

    def install(_evidence: object, install_root: Path) -> Path:
        candidate = install_root / 'site-packages'
        (candidate / 'shotgun_api3').mkdir(parents=True)
        (candidate / 'shotgun_api3' / '__init__.py').touch()
        return candidate

    def publish(_destination: Path, _result: Mapping[str, object]) -> None:
        nonlocal report_called
        report_called = True

    monkeypatch.setattr(benchmark, '_install_candidate', install)
    monkeypatch.setattr(benchmark, '_launch_worker', _RecoveryTimeoutLauncher(benchmark))
    monkeypatch.setattr(benchmark, '_write_report_atomic', publish)

    with pytest.raises(benchmark._CleanupError) as captured:
        benchmark._controller(tmp_path)

    assert str(captured.value) == (
        'Benchmark worker failed with status -9; warning: Benchmark worker recovery failed'
    )
    assert report_called is False
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert all(
        sentinel not in str(captured.value)
        for sentinel in (
            'SECRET-command',
            'SECRET-path',
            'SECRET-stdout',
            'SECRET-stderr',
            'SECRET-SG_TEST_KEY',
            'traceback',
            '987654321',
            str(tmp_path),
        )
    )


def test_client_order_is_counterbalanced_and_seeded() -> None:
    benchmark = _load_benchmark()
    first = benchmark._client_orders(8, seed=17)
    second = benchmark._client_orders(8, seed=17)

    assert first == second
    assert all(order in (('ours', 'official'), ('official', 'ours')) for order in first)
    assert first.count(('ours', 'official')) == first.count(('official', 'ours'))
    assert all(first[index] != first[index + 1] for index in range(0, len(first), 2))


@pytest.mark.parametrize('process_control', [KeyboardInterrupt(), SystemExit(17)])
@pytest.mark.parametrize('mode', ['cleanup', 'preflight'])
def test_configured_worker_nonlive_interruption_does_not_recover(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_control: BaseException,
    mode: str,
) -> None:
    benchmark = _load_benchmark()
    config_path = tmp_path / 'worker-0.json'

    def launch(path: Path, _root: Path) -> dict[str, object]:
        assert path == config_path
        assert path.exists()
        raise process_control

    def recover(_config: Mapping[str, object], _root: Path, _sequence: int) -> None:
        pytest.fail('non-live interruption must not launch recovery')

    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    monkeypatch.setattr(benchmark, '_recover_worker', recover)

    with pytest.raises(type(process_control)) as captured:
        benchmark._launch_configured_worker(_worker_config(tmp_path, mode=mode), tmp_path, 0)

    assert captured.value is process_control
    assert not config_path.exists()
    assert not (tmp_path / 'resource-journal.json').exists()


@pytest.mark.parametrize(
    ('process_control', 'ordinary_error'),
    [
        (KeyboardInterrupt(), OSError('ordinary unlink failure')),
        (_StopBenchmark(), RuntimeError('ordinary work failure')),
    ],
    ids=('work-process-control', 'cleanup-process-control'),
)
def test_configured_worker_preserves_first_process_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_control: BaseException,
    ordinary_error: Exception,
) -> None:
    benchmark = _load_benchmark()

    if isinstance(process_control, KeyboardInterrupt):

        def launch(_config_path: Path, _root: Path) -> dict[str, object]:
            raise process_control

        def unlink(_path: Path, *, missing_ok: bool = False) -> None:
            assert missing_ok is True
            raise ordinary_error

    else:

        def launch(_config_path: Path, _root: Path) -> dict[str, object]:
            raise ordinary_error

        def unlink(_path: Path, *, missing_ok: bool = False) -> None:
            assert missing_ok is True
            raise process_control

    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    monkeypatch.setattr(Path, 'unlink', unlink)

    with pytest.raises(type(process_control)) as captured:
        benchmark._launch_configured_worker({}, tmp_path, 0)

    assert captured.value is process_control


def test_controller_defaults_to_bounded_workload_after_isolated_preflights(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    official = tmp_path / 'official'
    (official / 'shotgun_api3').mkdir(parents=True)
    (official / 'shotgun_api3' / '__init__.py').touch()
    evidence = benchmark._ArtifactEvidence('a' * 40, 'b' * 64, '0.1.2', tmp_path / 'wheel')
    events: list[str] = []
    configs: list[dict[str, object]] = []
    install_roots: list[Path] = []
    published: dict[str, object] = {}
    for name in _REQUIRED_ENV:
        monkeypatch.setenv(name, 'value')
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')
    monkeypatch.delenv('BENCH_N', raising=False)
    monkeypatch.setenv('BENCH_SEED', '7')

    def load(_root: Path) -> object:
        events.append('artifact')
        return evidence

    def official_checkout(_repo_root: Path, _official_root: Path) -> Path:
        events.append('official')
        return official

    def preflight() -> None:
        events.append('preflight')

    monkeypatch.setattr(benchmark, '_preflight_tools', preflight)
    monkeypatch.setattr(benchmark, '_load_artifact_evidence', load)
    monkeypatch.setattr(benchmark, '_official_checkout', official_checkout)

    def install(_evidence: object, install_root: Path) -> Path:
        events.append('install')
        install_roots.append(install_root)
        candidate = install_root / 'site-packages'
        (candidate / 'shotgun_api3').mkdir(parents=True)
        (candidate / 'shotgun_api3' / '__init__.py').touch()
        return candidate

    def launch(config: dict[str, object], _root: Path, _sequence: int) -> dict[str, object]:
        events.append('worker')
        configs.append(dict(config))
        return _controller_worker_result(benchmark, config)

    def publish(_destination: Path, result: dict[str, object]) -> None:
        events.append('publish')
        published.update(result)

    monkeypatch.setattr(benchmark, '_install_candidate', install)
    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)
    monkeypatch.setattr(benchmark, '_write_report_atomic', publish)

    assert benchmark._controller(tmp_path) == 0

    assert events.index('preflight') < events.index('worker')
    assert [config['mode'] for config in configs[:2]] == ['preflight', 'preflight']
    assert len(configs) == 14
    assert sum(config['mode'] == 'rss_download_setup' for config in configs) == 4
    install_root = install_roots[0]
    _assert_controller_configs(benchmark, configs, install_root, official)
    assert published['workload'] == {
        'created_entities': 184,
        'upload_bytes': 332032784,
        'workers': 18,
    }
    assert set(cast('dict[str, str]', published['machine'])) == {
        'architecture',
        'os',
        'platform',
        'python_version',
    }


@pytest.mark.parametrize('process_control', [KeyboardInterrupt(), SystemExit(17)])
@pytest.mark.parametrize(
    'recovery_case',
    ['empty', 'nonempty', 'malformed', 'failure', 'interrupt', 'exit'],
)
def test_controller_interruption_recovers_before_removal_and_blocks_publication(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_control: BaseException,
    recovery_case: str,
) -> None:
    benchmark = _load_benchmark()
    launcher = _InterruptedWorkerLauncher(benchmark, process_control, recovery_case)
    original_unlink = Path.unlink
    official = tmp_path / 'official'
    (official / 'shotgun_api3').mkdir(parents=True)
    (official / 'shotgun_api3' / '__init__.py').touch()
    evidence = benchmark._ArtifactEvidence('a' * 40, 'b' * 64, '0.1.2', tmp_path / 'wheel')
    for name in _REQUIRED_ENV:
        monkeypatch.setenv(name, 'unused')
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')
    monkeypatch.delenv('BENCH_N', raising=False)
    monkeypatch.delenv('BENCH_SEED', raising=False)

    def load(_root: Path) -> object:
        return evidence

    def official_checkout(_repo_root: Path, _official_root: Path) -> Path:
        return official

    def install(_evidence: object, install_root: Path) -> Path:
        candidate = install_root / 'site-packages'
        (candidate / 'shotgun_api3').mkdir(parents=True)
        (candidate / 'shotgun_api3' / '__init__.py').touch()
        return candidate

    def unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path == launcher.worker_path:
            launcher.events.append('remove')
        original_unlink(path, missing_ok=missing_ok)

    def publish(_destination: Path, _result: Mapping[str, object]) -> None:
        launcher.events.append('publish')

    monkeypatch.setattr(benchmark, '_preflight_tools', _noop)
    monkeypatch.setattr(benchmark, '_load_artifact_evidence', load)
    monkeypatch.setattr(benchmark, '_official_checkout', official_checkout)
    monkeypatch.setattr(benchmark, '_install_candidate', install)
    monkeypatch.setattr(benchmark, '_launch_worker', launcher)
    monkeypatch.setattr(benchmark, '_recover_worker', launcher.recover)
    monkeypatch.setattr(benchmark, '_write_report_atomic', publish)
    monkeypatch.setattr(Path, 'unlink', unlink)

    with pytest.raises(type(process_control)) as captured:
        benchmark._controller(tmp_path)

    assert captured.value is process_control
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert launcher.events == (
        ['interrupted', 'recover', 'remove']
        if recovery_case == 'malformed'
        else ['interrupted', 'recover', 'cleanup', 'remove']
    )
    assert launcher.worker_path is not None
    assert not launcher.worker_path.exists()


def test_controller_rejects_excessive_workload_before_order_allocation_or_workers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    official = tmp_path / 'official'
    official.mkdir()
    evidence = benchmark._ArtifactEvidence('a' * 40, 'b' * 64, '0.1.2', tmp_path / 'wheel')
    order_called = False
    worker_called = False
    for name in _REQUIRED_ENV:
        monkeypatch.setenv(name, 'value')
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')
    monkeypatch.setenv('BENCH_N', str(10**18))

    def load(_root: Path) -> object:
        return evidence

    def official_checkout(_repo_root: Path, _official_root: Path) -> Path:
        return official

    monkeypatch.setattr(benchmark, '_preflight_tools', _noop)
    monkeypatch.setattr(benchmark, '_load_artifact_evidence', load)
    monkeypatch.setattr(benchmark, '_official_checkout', official_checkout)

    def orders(_sample_count: int, *, seed: int) -> list[tuple[str, str]]:
        nonlocal order_called
        order_called = True
        raise AssertionError(f'order construction must not start: {seed}')

    def launch(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal worker_called
        worker_called = True
        raise AssertionError('worker must not start')

    monkeypatch.setattr(benchmark, '_client_orders', orders)
    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)

    with pytest.raises(benchmark._BenchmarkError, match='workload budget'):
        benchmark._controller(tmp_path)

    assert order_called is False
    assert worker_called is False


def test_created_result_pair_arity_is_independent_of_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    operations = benchmark._Operations(SimpleNamespace(), SimpleNamespace())

    monkeypatch.setattr(benchmark, '_BATCH_SIZE', 3)

    assert operations._verify_result('create', ({'id': 7, 'code': 'expected'}, 'expected')) is True
    assert operations._verify_result('create', ({'id': 7, 'code': 'expected'},)) is False
    assert operations._verify_result('create', ({'id': 7}, 'expected', None)) is False
    assert operations._verify_result('create', ({'id': 7, 'code': 'wrong'}, 'expected')) is False


def test_delete_preparation_allocates_one_target_per_cold_warm_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    created: list[dict[str, object]] = []

    def create(
        _client: object,
        entity_type: str,
        data: dict[str, object],
    ) -> dict[str, object]:
        created.append({'entity_type': entity_type, **data})
        return {'id': len(created)}

    state = SimpleNamespace(
        clients=[SimpleNamespace()],
        _create=create,
        namespace='namespace',
        project={'id': 9},
    )
    operations = benchmark._Operations(SimpleNamespace(), state)

    monkeypatch.setattr(benchmark, '_BATCH_SIZE', 3)

    operations._prepare('delete')

    assert created == [
        {
            'entity_type': 'Shot',
            'code': 'namespace-delete-0',
            'project': state.project,
        },
        {
            'entity_type': 'Shot',
            'code': 'namespace-delete-1',
            'project': state.project,
        },
    ]


def test_deleted_result_pair_arity_is_independent_of_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()

    class Control:
        def find_one(self, _entity_type: str, _filters: object) -> None:
            pass

    state = SimpleNamespace(clients=[Control()], verified_deleted_shot_ids=set())
    operations = benchmark._Operations(SimpleNamespace(), state)

    monkeypatch.setattr(benchmark, '_BATCH_SIZE', 3)

    assert operations._verify_result('delete', (True, 7)) is True
    assert operations._verify_result('delete', (True,)) is False
    assert operations._verify_result('delete', (True, 8, None)) is False
    assert operations._verify_result('delete', (False, 8)) is False
    assert state.verified_deleted_shot_ids == {7}


def test_disposable_path_inside_repository_is_rejected(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    repo = tmp_path / 'repo'
    outside = tmp_path / 'outside'
    inside = repo / 'work'
    for path in (repo, outside, inside):
        path.mkdir(exist_ok=True)

    assert benchmark._validate_disposable_path(outside, repo) == outside.resolve()
    with pytest.raises(benchmark._BenchmarkError, match='outside the repository'):
        benchmark._validate_disposable_path(inside, repo)


@pytest.mark.parametrize('upload_name', ['upload_generic', 'upload_direct', 'upload_multipart'])
def test_download_and_upload_verification_compare_exact_bytes(
    tmp_path: Path,
    upload_name: str,
) -> None:
    benchmark = _load_benchmark()
    source = tmp_path / 'source.bin'
    source.write_bytes(b'expected-content')
    downloaded = tmp_path / 'download-0.bin'
    downloaded.write_bytes(b'x' * len(source.read_bytes()))

    class Control:
        def download_attachment(self, _identity: int, *, file_path: str) -> str:
            Path(file_path).write_bytes(b'x' * len(source.read_bytes()))
            return file_path

        def find_one(
            self,
            entity_type: str,
            _filters: object,
            fields: object = None,
        ) -> dict[str, object]:
            if entity_type == 'Attachment':
                return {'type': 'Attachment', 'id': 7}
            assert fields == ['sg_uploaded_movie']
            return {'type': 'Version', 'id': 5, 'sg_uploaded_movie': {'id': 7}}

    state = SimpleNamespace(
        clients=[Control()],
        direct_payload=source,
        download_attachment_id=7,
        entities=[{'id': 1}],
        multipart_payload=source,
        generic_payload=source,
        version={'id': 5},
        work_root=tmp_path,
    )
    operations = benchmark._Operations(SimpleNamespace(), state)

    assert operations._verify_result('download', str(downloaded)) is False
    assert operations._verify_result(upload_name, 7) is False


@pytest.mark.parametrize('worker_outcome', ['handled', 'unexpected', 'signal', 'timeout'])
def test_empty_journal_recovers_every_worker_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    worker_outcome: str,
) -> None:
    benchmark = _load_benchmark()
    actual_launch = benchmark._launch_worker
    recoveries: list[int] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if worker_outcome == 'timeout':
            raise subprocess.TimeoutExpired(command, 3600)
        returncode = -9 if worker_outcome == 'signal' else 1
        diagnostic = (
            'benchmark operation failed: create\n'
            if worker_outcome == 'handled'
            else 'Benchmark worker failed at an unexpected boundary\n'
        )
        return subprocess.CompletedProcess(command, returncode, '', diagnostic)

    def launch(config_path: Path, root: Path) -> dict[str, object]:
        return actual_launch(config_path, root, env={}, run=run)

    def recover(_config: Mapping[str, object], _root: Path, sequence: int) -> None:
        recoveries.append(sequence)

    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    monkeypatch.setattr(benchmark, '_recover_worker', recover)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_configured_worker(_worker_config(tmp_path), tmp_path, 12)

    assert recoveries == [12]
    if worker_outcome == 'handled':
        assert str(captured.value).endswith('benchmark operation failed: create')
    assert not tuple(tmp_path.glob('*worker*.json'))


@pytest.mark.parametrize('client_name', ['ours', 'official'])
def test_empty_observer_is_rejected_only_for_our_client(
    monkeypatch: pytest.MonkeyPatch,
    client_name: str,
) -> None:
    benchmark = _load_benchmark()
    emitted_roles: list[str] = []

    monkeypatch.setattr(benchmark, '_LATENCY_OPERATIONS', ('find_one',))
    monkeypatch.setattr(benchmark, '_LiveState', _LatencyObserverState)
    config = {
        'client': client_name,
        'emit_events': False,
        'emitted_roles': emitted_roles,
    }

    if client_name == 'ours':
        with pytest.raises(benchmark._CorrectnessError, match='evidence failed'):
            benchmark._latency_worker(
                SimpleNamespace(),
                config,
            )
    else:
        result = benchmark._latency_worker(
            SimpleNamespace(),
            config,
        )
        assert result['operations']['find_one']['category'] == 'read'
        assert result['operations']['find_one']['route'] == 'rpc'


def test_fresh_benchmark_modules_do_not_share_globals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _load_benchmark()
    monkeypatch.setattr(first, '_BATCH_SIZE', 987)

    second = _load_benchmark()

    assert first is not second
    assert second._BATCH_SIZE != 987
    assert first._ResourceJournal is not second._ResourceJournal


def test_full_inventory_is_required_without_silent_skips() -> None:
    benchmark = _load_benchmark()
    result = {
        'client': 'ours',
        'operations': _operation_result(benchmark),
        'rss': {name: _rss_result('ours', name) for name in benchmark._RSS_OPERATIONS},
    }
    benchmark._validate_round_result(result)

    missing = json.loads(json.dumps(result))
    missing['operations'].pop(benchmark._LATENCY_OPERATIONS[-1])
    with pytest.raises(benchmark._BenchmarkError, match='inventory'):
        benchmark._validate_round_result(missing)

    skipped = json.loads(json.dumps(result))
    skipped['operations'][benchmark._LATENCY_OPERATIONS[0]]['skipped'] = True
    with pytest.raises(benchmark._BenchmarkError, match='unexpected field'):
        benchmark._validate_round_result(skipped)

    bad_rss_route = json.loads(json.dumps(result))
    bad_rss_route['rss']['rss_upload']['route'] = 'observed:rpc'
    with pytest.raises(benchmark._BenchmarkError, match='route evidence'):
        benchmark._validate_round_result(bad_rss_route)


def test_generated_entity_codes_are_distinct_and_fit_observed_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    recorded: list[str] = []
    fixed_uuid = SimpleNamespace(hex='f' * 32)
    monkeypatch.setattr(benchmark.uuid, 'uuid4', lambda: fixed_uuid)
    namespace = benchmark._new_namespace('official', 19)

    def create(
        _client: object,
        _entity_type: str,
        data: dict[str, object],
    ) -> dict[str, object]:
        recorded.append(cast('str', data['code']))
        return {'id': len(recorded)}

    state = SimpleNamespace(
        clients=[SimpleNamespace()],
        _create=create,
        namespace=namespace,
        project={'id': 1},
    )
    operations = benchmark._Operations(SimpleNamespace(), state)

    operations._call('create', 0)
    operations._call('create', 1)
    operations._prepare('delete')

    assert recorded == [
        f'{namespace}-created-0',
        f'{namespace}-created-1',
        f'{namespace}-delete-0',
        f'{namespace}-delete-1',
    ]
    assert len(set(recorded)) == len(recorded)
    assert max(map(len, recorded)) <= 100


def test_generated_entity_codes_reject_overflow_before_live_create() -> None:
    benchmark = _load_benchmark()
    live_create_called = False

    def create(
        _client: object,
        _entity_type: str,
        _data: dict[str, object],
    ) -> dict[str, object]:
        nonlocal live_create_called
        live_create_called = True
        return {'id': 1}

    state = SimpleNamespace(
        clients=[SimpleNamespace()],
        _create=create,
        namespace='x' * 100,
        project={'id': 1},
    )
    operations = benchmark._Operations(SimpleNamespace(), state)

    with pytest.raises(benchmark._BenchmarkError, match='100-character'):
        operations._call('create', 0)

    assert live_create_called is False


def test_generated_live_fixture_codes_use_shared_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    namespace = benchmark._new_namespace('official', 19)
    original_entity_code = benchmark._entity_code
    suffixes: list[str] = []

    def entity_code(value: str, suffix: str) -> str:
        suffixes.append(suffix)
        return original_entity_code(value, suffix)

    class Control:
        def __init__(self) -> None:
            self.identity = 1

        def create(
            self,
            entity_type: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            self.identity += 1
            return {'id': self.identity, 'type': entity_type, **data}

        def find_one(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            return {'id': 1, 'type': 'Project', 'name': 'Benchmark'}

        def info(self) -> dict[str, object]:
            return {}

        def upload(self, *_args: object, **_kwargs: object) -> int:
            return 99

    control = Control()

    def shotgun(_site: str, **_kwargs: object) -> Control:
        return control

    def write_payload(_path: Path, _byte_count: int) -> None:
        pass

    module = SimpleNamespace(Shotgun=shotgun)
    config = {
        'api_key': 'unused',
        'namespace': namespace,
        'project': 'Benchmark',
        'script_name': 'unused',
        'site': 'https://example.invalid',
        'work_root': str(tmp_path),
    }
    monkeypatch.setattr(benchmark, '_entity_code', entity_code)
    monkeypatch.setattr(benchmark, '_write_payload', write_payload)
    benchmark._ResourceJournal._create(tmp_path)

    benchmark._LiveState(module, config)._prepare()

    assert suffixes == ['version', *(f'shot-{index:03d}' for index in range(30))]


def test_generated_payload_is_deterministic_nontrivial_content(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    first = tmp_path / 'first.bin'
    second = tmp_path / 'second.bin'

    benchmark._write_payload(first, 8193)
    benchmark._write_payload(second, 8193)

    assert first.read_bytes() == second.read_bytes()
    assert first.stat().st_size == 8193
    assert any(first.read_bytes())


@pytest.mark.parametrize('size', [0, 31, 32, 33, 1 << 20, (1 << 20) + 17])
def test_generated_payload_matches_repeated_digest(size: int, tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    path = tmp_path / 'payload.bin'
    digest = hashlib.sha256(b'sg-python-api benchmark transfer payload').digest()
    expected = (digest * ((size + len(digest) - 1) // len(digest)))[:size]

    benchmark._write_payload(path, size)

    assert path.read_bytes() == expected


@pytest.mark.parametrize('with_residue', [False, True])
@pytest.mark.parametrize('recovery_fails', [False, True])
def test_handled_worker_failure_recovers_namespace_and_retains_diagnostics(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    with_residue: bool,
    recovery_fails: bool,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    actual_launch = benchmark._launch_worker
    actual_worker_result = benchmark._worker_result
    monkeypatch.chdir(tmp_path)
    events: list[tuple[object, ...]] = []
    worker_status: int | None = None
    present = {('Shot', 17)}
    client = _CleanupWorkerClient(
        events, present, namespaced=present.copy(), false_delete=17 if recovery_fails else None
    )

    monkeypatch.setattr(benchmark, '_import_worker_client', _unused_worker_module)
    monkeypatch.setattr(benchmark, '_verify_worker_modules', _ignore_worker_module)

    def track(state: _RecoveryStateSeam) -> _CleanupWorkerClient:
        state.clients.append(client)
        return client

    monkeypatch.setattr(benchmark._LiveState, '_client', track)

    def fail_worker(_config_path: Path) -> dict[str, object]:
        raise benchmark._BenchmarkError('benchmark operation failed: create')

    monkeypatch.setattr(benchmark, '_worker_result', fail_worker)

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal worker_status
        with pytest.raises(SystemExit) as worker_exit:
            benchmark._worker(Path(command[-1]))
        assert isinstance(worker_exit.value.code, int)
        worker_status = worker_exit.value.code
        return subprocess.CompletedProcess(
            command, worker_exit.value.code, 'SECRET-stdout', capsys.readouterr().err
        )

    def launch(config_path: Path, root: Path) -> dict[str, object]:
        config = json.loads(config_path.read_text(encoding='ascii'))
        if config['mode'] == 'cleanup':
            return actual_worker_result(config_path)
        if with_residue:
            journal_type._load(root)._record('Shot', 17)
        return actual_launch(config_path, root, env={}, run=run)

    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_configured_worker(_worker_config(tmp_path), tmp_path, 2)

    expected = (
        f'Benchmark worker failed with status {worker_status}: benchmark operation failed: create'
    )
    if recovery_fails:
        expected += '; warning: Benchmark worker recovery failed'
    assert str(captured.value) == expected
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert any(event[0] == 'sweep' for event in events)
    assert (('Shot', 17) in present) is recovery_fails
    assert not tuple(tmp_path.glob('*worker*.json'))
    assert not tuple(tmp_path.glob('*cleanup*.json'))
    if not recovery_fails:
        assert journal_type._load(tmp_path)._resources == ()


@pytest.mark.parametrize(
    ('change', 'operation_key'),
    [('remove', 'find_one'), ('add', 'unknown_verifier')],
)
def test_initialized_operations_reject_mismatched_bound_verifiers_before_workload(
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    operation_key: str,
) -> None:
    benchmark = _load_benchmark()
    original = benchmark._Operations._verifiers

    def changed_mapping(operations: object) -> dict[str, object]:
        mapping = original(operations)
        if change == 'remove':
            mapping.pop(operation_key)
        else:
            mapping[operation_key] = _noop
        return mapping

    monkeypatch.setattr(benchmark._Operations, '_verifiers', changed_mapping)
    with pytest.raises(
        benchmark._BenchmarkError,
        match=rf'^Benchmark operation metadata mismatch: verifiers: {operation_key}$',
    ):
        benchmark._Operations(SimpleNamespace(), SimpleNamespace())


@pytest.mark.parametrize(
    ('platform', 'raw', 'expected'),
    [('darwin', 2048, 2048), ('linux', 2048, 2 * 1024 * 1024)],
)
def test_isolated_peak_rss_units_are_normalized(
    platform: str,
    raw: int,
    expected: int,
) -> None:
    benchmark = _load_benchmark()

    assert benchmark._normalize_peak_rss(raw, platform) == expected


def test_journal_leak_is_sanitized_when_recovery_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_path = tmp_path / 'resource-journal.json'
    config = _worker_config(tmp_path)

    def launch(_config_path: Path, _root: Path) -> dict[str, object]:
        journal_path.write_text(
            '{"resources":[{"entity_type":"SECRET-journal-content",'
            '"id":987654321}],"raw":"SECRET-response"}\n',
            encoding='ascii',
        )
        raise benchmark._BenchmarkError('Benchmark worker failed with status -9')

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._CleanupError) as captured:
        benchmark._launch_configured_worker(config, tmp_path, 11)

    assert str(captured.value) == (
        'Benchmark worker failed with status -9; warning: Benchmark worker recovery failed'
    )
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert all(
        sentinel not in str(captured.value)
        for sentinel in ('SECRET-journal-content', 'SECRET-response', '987654321')
    )


@pytest.mark.parametrize('client', ['official', 'ours'])
def test_known_entity_and_payload_decode_require_loaded_client_timezone_identity(
    client: str,
) -> None:
    benchmark = _load_benchmark()
    modules = {name: SimpleNamespace(sg_timezone=SgTimezone()) for name in ('official', 'ours')}
    other_client = 'ours' if client == 'official' else 'official'
    entities = [
        {'type': 'Shot', 'id': index, 'code': f'ns-shot-{index:03d}'} for index in range(1, 31)
    ]
    state = SimpleNamespace(
        config={'client': client},
        entities=entities,
        expected_descriptions={entity['id']: None for entity in entities},
        module=modules[client],
        namespace='ns',
    )
    operations = benchmark._Operations(SimpleNamespace(), state)
    wrong_known = {'type': 'Shot', 'id': 999, 'code': entities[0]['code']}
    wrong_payload = [
        {**entity, 'created_at': '2026-07-19T00:00:00Z', 'description': 'wrong'}
        for entity in entities
    ]

    assert operations._verify_result('find_one', wrong_known) is False
    assert operations._verify_result('payload_decode', wrong_payload) is False

    naive_payload = [
        {
            **entity,
            'created_at': datetime(2026, 7, 19, tzinfo=UTC).replace(tzinfo=None),
            'description': None,
        }
        for entity in entities
    ]
    assert operations._verify_result('payload_decode', naive_payload) is False

    created_at = datetime(2026, 7, 19, tzinfo=UTC)
    loaded_datetime = created_at.astimezone(modules[client].sg_timezone.local)
    other_datetime = created_at.astimezone(modules[other_client].sg_timezone.local)
    assert loaded_datetime.tzinfo is not other_datetime.tzinfo
    assert loaded_datetime.utcoffset() == other_datetime.utcoffset()

    wrong_timezone_payload = [
        {
            **entity,
            'created_at': other_datetime,
            'description': None,
        }
        for entity in entities
    ]
    assert operations._verify_result('payload_decode', wrong_timezone_payload) is False

    valid_payload = [
        {
            **entity,
            'created_at': loaded_datetime,
            'description': None,
        }
        for entity in entities
    ]
    assert operations._verify_result('payload_decode', valid_payload) is True


def test_latency_operation_presentation_order_is_stable() -> None:
    benchmark = _load_benchmark()

    assert benchmark._LATENCY_OPERATIONS == _EXPECTED_LATENCY_OPERATIONS


def test_latency_worker_preserves_measurement_process_control_during_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    process_control = KeyboardInterrupt()
    events: list[str] = []

    class Client:
        def close(self) -> None:
            events.append('close')

    class Operations:
        def __init__(self, _client: object, _state: object) -> None:
            self._close = lambda: events.append('operations-close')

        def _call(self, _name: str, _invocation: int) -> object:
            raise process_control

        def _prepare(self, _name: str) -> None:
            pass

        def _verify_result(self, _name: str, _result: object) -> bool:
            return True

    class State:
        def __init__(self, _module: object, _config: object) -> None:
            pass

        def _cleanup(self) -> None:
            events.append('cleanup')

        def _client(self) -> Client:
            return Client()

        def _prepare(self) -> None:
            events.append('prepare')

    monkeypatch.setattr(benchmark, '_LATENCY_OPERATIONS', ('find_one',))
    monkeypatch.setattr(benchmark, '_LiveState', State)
    monkeypatch.setattr(benchmark, '_Operations', Operations)

    with pytest.raises(KeyboardInterrupt) as captured:
        benchmark._latency_worker(
            SimpleNamespace(),
            {'client': 'official'},
        )

    assert captured.value is process_control
    assert events == ['prepare', 'operations-close', 'close', 'cleanup']


def test_live_state_cleanup_finishes_then_propagates_first_process_control() -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    first = KeyboardInterrupt()
    second = SystemExit(17)

    class Control:
        def close(self) -> None:
            events.append('close:control')

        def delete(self, _kind: str, _identity: int) -> None:
            events.append('delete')
            raise first

        def find(self, kind: str, *_args: object) -> list[object]:
            events.append(f'sweep:{kind}')
            return []

        def find_one(self, _kind: str, _filters: object) -> None:
            events.append('query')

    class Other:
        def close(self) -> None:
            events.append('close:other')
            raise second

    state = object.__new__(benchmark._LiveState)
    state.clients = [Control(), Other()]
    state.namespace = 'ns'
    state.project = {'id': 1}
    state.resources = [('Attachment', 7)]

    with pytest.raises(KeyboardInterrupt) as captured:
        state._cleanup()

    assert captured.value is first
    assert events == [
        'delete',
        'query',
        'sweep:Attachment',
        'sweep:Shot',
        'sweep:Version',
        'close:other',
        'close:control',
    ]


def test_live_state_journal_clears_only_after_final_client_closure(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    journal._record('Attachment', 7)
    events: list[str] = []
    original_clear = journal._clear

    def clear() -> None:
        events.append('journal:clear')
        original_clear()

    journal._clear = clear

    class Control:
        def close(self) -> None:
            events.append('close:control')

        def delete(self, _entity_type: str, _entity_id: int) -> bool:
            events.append('delete')
            return True

        def find(self, entity_type: str, *_args: object) -> list[object]:
            events.append(f'sweep:{entity_type}')
            return []

        def find_one(self, _entity_type: str, _filters: object) -> None:
            events.append('query')

    class Other:
        def close(self) -> None:
            events.append('close:other')

    state = object.__new__(benchmark._LiveState)
    state.clients = [Control(), Other()]
    state.journal = journal
    state.namespace = 'ns'
    state.project = {'id': 1}
    state.resources = [('Attachment', 7)]

    state._cleanup()

    assert events == [
        'delete',
        'query',
        'sweep:Attachment',
        'sweep:Shot',
        'sweep:Version',
        'close:other',
        'close:control',
        'journal:clear',
    ]
    assert state.resources == []
    assert journal_type._load(tmp_path)._resources == ()


def test_live_state_journal_does_not_record_unsuccessful_mutations(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal_type._create(tmp_path)
    state = benchmark._LiveState(
        SimpleNamespace(),
        {'namespace': 'ns', 'work_root': str(tmp_path)},
    )

    class InvalidCreate:
        def create(self, *_args: object) -> dict[str, object]:
            return {'type': 'Shot', 'id': True}

    class FailedCreate:
        def create(self, *_args: object) -> object:
            raise RuntimeError('create failed')

    with pytest.raises(benchmark._CorrectnessError, match='creation failed'):
        state._create(InvalidCreate(), 'Shot', {})
    with pytest.raises(RuntimeError, match='create failed'):
        state._create(FailedCreate(), 'Shot', {})

    operations = benchmark._Operations(SimpleNamespace(), state)
    for invalid in (None, True, 0, '7'):
        assert operations._track_upload(invalid) is invalid

    assert state.resources == []
    assert journal_type._load(tmp_path)._resources == ()


def test_live_state_journal_records_validated_create_and_upload_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal_type._create(tmp_path)

    class Control:
        def __init__(self) -> None:
            self.identity = 1

        def create(
            self,
            entity_type: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            self.identity += 1
            return {'id': self.identity, 'type': entity_type, **data}

        def find_one(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            return {'id': 1, 'type': 'Project', 'name': 'Benchmark'}

        def info(self) -> dict[str, object]:
            return {}

        def upload(self, *_args: object, **_kwargs: object) -> int:
            return 40

    control = Control()

    def shotgun(_site: str, **_kwargs: object) -> Control:
        return control

    def write_payload(path: Path, _size: int) -> None:
        namespace = 'rss' if path.parent.name == 'rss' else 'ns'
        assert path.name.startswith(f'{namespace}-')

    monkeypatch.setattr(benchmark, '_write_payload', write_payload)
    state = benchmark._LiveState(
        SimpleNamespace(Shotgun=shotgun),
        {
            'api_key': 'unused',
            'namespace': 'ns',
            'project': 'Benchmark',
            'script_name': 'unused',
            'site': 'https://example.invalid',
            'work_root': str(tmp_path),
        },
    )

    state._prepare()
    operations = benchmark._Operations(SimpleNamespace(), state)
    assert operations._track_upload(41) == 41

    expected = (
        ('Version', 2),
        *(('Shot', identity) for identity in range(3, 33)),
        ('Attachment', 40),
        ('Attachment', 41),
    )
    assert tuple(state.resources) == expected
    assert journal_type._load(tmp_path)._resources == expected

    rss_root = tmp_path / 'rss'
    rss_root.mkdir()
    journal_type._create(rss_root)
    rss_state = benchmark._LiveState(
        SimpleNamespace(Shotgun=shotgun),
        {
            'api_key': 'unused',
            'namespace': 'rss',
            'project': 'Benchmark',
            'script_name': 'unused',
            'site': 'https://example.invalid',
            'work_root': str(rss_root),
        },
    )
    rss_state._prepare_rss('rss_download')
    assert journal_type._load(rss_root)._resources == (
        ('Version', 33),
        ('Attachment', 40),
    )


@pytest.mark.parametrize('failing_stage', ['absence', 'namespace', 'client-close'])
def test_live_state_journal_retains_conservative_snapshot_after_partial_cleanup(
    tmp_path: Path,
    failing_stage: str,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    journal._record('Attachment', 7)
    events: list[str] = []

    class Control:
        def close(self) -> None:
            events.append('close:control')
            if failing_stage == 'client-close':
                raise RuntimeError('close failed')

        def delete(self, _entity_type: str, _entity_id: int) -> bool:
            events.append('delete')
            return True

        def find(self, entity_type: str, *_args: object) -> list[object]:
            events.append(f'sweep:{entity_type}')
            if failing_stage == 'namespace':
                raise RuntimeError('sweep failed')
            return []

        def find_one(self, _entity_type: str, _filters: object) -> None:
            events.append('query')
            if failing_stage == 'absence':
                raise RuntimeError('query failed')

    state = object.__new__(benchmark._LiveState)
    state.clients = [Control()]
    state.journal = journal
    state.namespace = 'ns'
    state.project = {'id': 1}
    state.resources = [('Attachment', 7)]

    with pytest.raises(benchmark._CleanupError, match='cleanup failed'):
        state._cleanup()

    assert journal_type._load(tmp_path)._resources == (('Attachment', 7),)
    assert 'close:control' in events


def test_live_state_journal_retains_in_memory_identity_after_publication_failure() -> None:
    benchmark = _load_benchmark()

    class Client:
        def create(
            self,
            entity_type: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            return {'type': entity_type, 'id': 7, **data}

    class Journal:
        def _record(self, _entity_type: str, _entity_id: int) -> None:
            raise benchmark._BenchmarkError('journal publication failed')

    state = object.__new__(benchmark._LiveState)
    state.journal = Journal()
    state.resources = list[tuple[str, int]]()

    with pytest.raises(benchmark._BenchmarkError, match='journal publication failed'):
        state._create(Client(), 'Attachment', {})

    assert state.resources == [('Attachment', 7)]


def test_live_state_journal_retains_resources_without_cleanup_client(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    journal._record('Attachment', 7)
    state = object.__new__(benchmark._LiveState)
    state.clients = []
    state.journal = journal
    state.namespace = 'ns'
    state.project = {'id': 1}
    state.resources = [('Attachment', 7)]

    with pytest.raises(benchmark._CleanupError, match='cleanup failed'):
        state._cleanup()

    assert journal_type._load(tmp_path)._resources == (('Attachment', 7),)


def test_live_state_journal_rss_upload_uses_central_resource_ownership(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    rss_name = 'rss_upload'
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')

    samples = iter((100, 160))

    def getrusage(_kind: object) -> SimpleNamespace:
        events.append('rss')
        return SimpleNamespace(ru_maxrss=next(samples))

    resource = SimpleNamespace(RUSAGE_SELF=object(), getrusage=getrusage)
    original_import = benchmark.importlib.import_module
    states: list[_RssFixtureState] = []

    def import_module(name: str) -> object:
        return resource if name == 'resource' else original_import(name)

    def state_factory(_module: object, _config: Mapping[str, Any]) -> _RssFixtureState:
        state = _RssFixtureState(events, source, tmp_path)
        states.append(state)
        return state

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark, '_MULTIPART_LIMIT', 1)
    monkeypatch.setattr(benchmark.importlib, 'import_module', import_module)
    monkeypatch.setattr(benchmark.sys, 'platform', 'darwin')

    result = benchmark._rss_worker(
        SimpleNamespace(),
        {'client': 'official', 'rss_operation': rss_name},
    )

    assert 'full-prepare' not in events
    assert events.count('rss') == 2
    assert states[0].recorded_resources == [('Attachment', 7)]
    assert result['rss'][rss_name] == {
        'baseline_bytes': 100,
        'correct': True,
        'peak_bytes': 160,
        'peak_delta_bytes': 60,
        'route': 'download' if rss_name == 'rss_download' else 'capability:multipart',
    }


def test_loaded_client_version_and_origin_are_both_verified(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    root = tmp_path / 'client'
    module_path = root / 'shotgun_api3' / '__init__.py'
    module_path.parent.mkdir(parents=True)
    module_path.touch()
    module = SimpleNamespace(__file__=str(module_path), __version__='0.1.2')

    benchmark._verify_loaded_client(module, root, '0.1.2')
    with pytest.raises(benchmark._BenchmarkError, match='version'):
        benchmark._verify_loaded_client(module, root, '9.9.9')


@pytest.mark.parametrize(
    'metadata_case',
    [
        ('callbacks', 'remove', 'find_one'),
        ('callbacks', 'add', 'unknown_callback'),
        ('categories', 'remove', 'find_one'),
        ('categories', 'add', 'unknown_category'),
        ('route exceptions', 'add', 'unknown_route'),
    ],
)
@pytest.mark.parametrize(
    'entrypoint_case',
    [
        ('controller', []),
        ('worker', ['--worker', 'unused-worker-config.json']),
    ],
)
def test_main_rejects_operation_metadata_before_runtime_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
    metadata_case: tuple[str, str, str],
    entrypoint_case: tuple[str, list[str]],
) -> None:
    benchmark = _load_benchmark()
    metadata_class, change, operation_key = metadata_case
    entrypoint, argv = entrypoint_case
    touched: list[str] = []

    def fail_runtime(*_args: object, **_kwargs: object) -> None:
        touched.append(entrypoint)
        raise AssertionError(f'{entrypoint} runtime must not start')

    monkeypatch.setattr(benchmark, '_controller', fail_runtime)
    monkeypatch.setattr(benchmark, '_worker', fail_runtime)
    if metadata_class == 'callbacks':
        original = benchmark._Operations._callbacks

        def changed_mapping() -> dict[str, object]:
            mapping = original().copy()
            if change == 'remove':
                mapping.pop(operation_key)
            else:
                mapping[operation_key] = _noop
            return mapping

        monkeypatch.setattr(benchmark._Operations, '_callbacks', staticmethod(changed_mapping))
    elif metadata_class == 'categories':
        categories = dict(benchmark._OPERATION_CATEGORIES)
        if change == 'remove':
            categories.pop(operation_key)
        else:
            categories[operation_key] = 'read'
        monkeypatch.setattr(benchmark, '_OPERATION_CATEGORIES', categories)
    else:
        exceptions = frozenset({'upload_direct', operation_key})
        monkeypatch.setattr(benchmark, '_DIRECT_ROUTE_OPERATIONS', frozenset(exceptions))

    with pytest.raises(
        benchmark._BenchmarkError,
        match=rf'^Benchmark operation metadata mismatch: {metadata_class}: {operation_key}$',
    ):
        benchmark.main(argv)

    assert touched == []


def test_measurement_is_accepted_only_after_correctness() -> None:
    benchmark = _load_benchmark()
    accepted: list[float] = []
    clock = iter((2.0, 2.25))

    def reject_result(_result: dict[str, int]) -> bool:
        return False

    with pytest.raises(benchmark._CorrectnessError, match='find_small'):
        benchmark._measure_verified(
            'find_small',
            lambda: {'id': 1},
            reject_result,
            accepted,
            timer=lambda: next(clock),
        )

    assert accepted == []


def test_module_origin_must_stay_inside_isolated_root(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    expected = tmp_path / 'installed'
    expected.mkdir()
    inside = expected / 'shotgun_api3' / '__init__.py'
    outside = tmp_path / 'checkout' / 'src' / 'shotgun_api3' / '__init__.py'
    inside.parent.mkdir()
    outside.parent.mkdir(parents=True)
    inside.touch()
    outside.touch()

    assert benchmark._validate_module_origin(inside, expected) == inside.resolve()
    with pytest.raises(benchmark._BenchmarkError, match='outside'):
        benchmark._validate_module_origin(outside, expected)


def test_namespace_is_unique_and_cleanup_is_reverse_order() -> None:
    benchmark = _load_benchmark()
    first = benchmark._new_namespace('ours', 3)
    second = benchmark._new_namespace('ours', 3)
    deleted: list[tuple[str, int]] = []
    resources = [('Shot', 1), ('Attachment', 2), ('Version', 3)]

    def delete(kind: str, identity: int) -> None:
        deleted.append((kind, identity))

    benchmark._cleanup_resources(resources, delete, _absent_resource)

    assert first != second
    assert first.startswith('sg-python-api-benchmark-ours-3-')
    assert first.isascii()
    assert deleted == list(reversed(resources))


@pytest.mark.parametrize('recovery_fails', [False, True])
def test_nested_download_failure_recovers_shared_namespace_and_parent_journal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recovery_fails: bool,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    state_type = benchmark._LiveState
    actual_launch = benchmark._launch_worker
    events: list[tuple[object, ...]] = []
    present = {('Version', 5), ('Attachment', 7)}
    client = _CleanupWorkerClient(
        events, present, namespaced=present.copy(), false_delete=7 if recovery_fails else None
    )
    roots: list[Path] = []

    def client_factory(state: _RecoveryStateSeam) -> _CleanupWorkerClient:
        state.clients.append(client)
        return client

    def prepare(state: _RecoveryStateSeam, _name: str) -> None:
        state.project = {'type': 'Project', 'id': 1}
        state._client()
        for kind, identity in sorted(present):
            state._record_resource(kind, identity)
        state.download_attachment_id = 7
        state.multipart_payload.write_bytes(b'download-fixture')

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command, 1, '', 'benchmark operation failed: download\n'
        )

    def launch(config_path: Path, root: Path) -> dict[str, object]:
        config = json.loads(config_path.read_text(encoding='ascii'))
        if config['mode'] == 'rss':
            assert journal_type._load(root)._resources == ()
            return actual_launch(config_path, root, env={}, run=run)
        if config['mode'] == 'cleanup':
            roots.append(root)
        return benchmark._worker_result(config_path)

    monkeypatch.setattr(state_type, '_client', client_factory)
    monkeypatch.setattr(state_type, '_prepare_rss', prepare)

    monkeypatch.setattr(benchmark, '_import_worker_client', _unused_worker_module)
    monkeypatch.setattr(benchmark, '_verify_worker_modules', _ignore_worker_module)
    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    config = {
        **_worker_config(tmp_path, mode='rss_download_setup'),
        'rss_operation': 'rss_download',
    }
    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_configured_worker(config, tmp_path, 5)

    assert roots == [tmp_path / 'rss-download-worker', tmp_path]
    assert 'benchmark operation failed: download' in str(captured.value)
    assert ('Benchmark worker recovery failed' in str(captured.value)) is recovery_fails
    assert not tuple(tmp_path.rglob('*worker*.json'))
    assert not tuple(tmp_path.rglob('*cleanup*.json'))
    assert journal_type._load(tmp_path / 'rss-download-worker')._resources == ()
    if recovery_fails:
        assert present == {('Attachment', 7)}
        assert journal_type._load(tmp_path)._resources
    else:
        assert not present
        assert journal_type._load(tmp_path)._resources == ()
    # Parent cleanup still checks identities deleted by child recovery.
    assert sum(event[:3] == ('delete', 'Attachment', 7) for event in events) >= 2


@pytest.mark.parametrize(
    'diagnostic',
    [
        'Benchmark worker failed with status 1: benchmark operation failed: download',
        'Benchmark worker timed out; warning: Benchmark worker recovery failed',
        (
            'Benchmark worker returned invalid structured output; '
            'warning: Benchmark worker recovery failed'
        ),
        'Benchmark worker returned a non-object result; warning: Benchmark worker recovery failed',
        (
            'Benchmark worker failed with status 1: benchmark operation failed: download; '
            'warning: Benchmark worker recovery failed; warning: benchmark cleanup failed'
        ),
    ],
)
def test_nested_worker_diagnostics_cross_subprocess_boundary_safely(
    tmp_path: Path, diagnostic: str
) -> None:
    benchmark = _load_benchmark()

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, '', diagnostic + '\n')

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_worker(tmp_path / 'worker.json', tmp_path, env={}, run=run)
    assert str(captured.value) == 'Benchmark worker failed with status 1: ' + diagnostic

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_worker(
            tmp_path / 'worker.json', tmp_path, env={'SG_TEST_KEY': 'download'}, run=run
        )
    if 'download' in diagnostic:
        assert str(captured.value) == 'Benchmark worker failed with status 1'


@pytest.mark.parametrize(
    'status',
    [
        'M  shotgun_api3/__init__.py',
        ' M shotgun_api3/__init__.py',
        '?? shotgun_api3/shadow.py',
    ],
)
def test_official_checkout_rejects_package_dirt_before_use(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: str,
) -> None:
    benchmark = _load_benchmark()
    candidate = tmp_path / 'candidate'
    official = tmp_path / 'official'
    candidate.mkdir()
    official.mkdir()

    def checked(command: list[str], _message: str) -> str:
        if command[-2:] == ['rev-parse', '--show-toplevel']:
            return str(official)
        if command[-3:] == ['rev-parse', '--verify', 'HEAD^{commit}']:
            return benchmark._OFFICIAL_COMMIT
        if command[-3:] == ['rev-parse', '--verify', f'{benchmark._OFFICIAL_REF}^{{commit}}']:
            return benchmark._OFFICIAL_COMMIT
        if command[-3:] == ['remote', 'get-url', 'origin']:
            return benchmark._OFFICIAL_REPOSITORY
        if 'status' in command:
            return status
        raise AssertionError(command)

    monkeypatch.setattr(benchmark, '_checked_command', checked)

    with pytest.raises(benchmark._BenchmarkError, match='local package changes'):
        benchmark._official_checkout(candidate, official)


def test_official_generic_upload_requires_a_capability_route() -> None:
    benchmark = _load_benchmark()
    validate = benchmark._validate_route_evidence

    for route in (
        'capability:form',
        'capability:direct',
        'capability:multipart',
    ):
        validate('official', 'upload_generic', route)

    for route in ('rpc', 'observed:rpc', '', 'capability:unknown'):
        with pytest.raises(benchmark._BenchmarkError, match='route evidence'):
            validate('official', 'upload_generic', route)


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('top_level', 'other', 'top level'),
        ('head', 'b' * 40, 'HEAD'),
        ('ref_commit', 'b' * 40, 'ref'),
        ('origin', 'https://example.invalid/fork', 'origin'),
    ],
)
def test_official_identity_is_exactly_pinned(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    benchmark = _load_benchmark()
    official = tmp_path / 'official'
    official.mkdir()
    identity = {
        'top_level': str(official.resolve()),
        'head': benchmark._OFFICIAL_COMMIT,
        'ref_commit': benchmark._OFFICIAL_COMMIT,
        'origin': benchmark._OFFICIAL_REPOSITORY,
    }
    identity[field] = value
    if field == 'top_level':
        identity[field] = str(tmp_path / value)

    with pytest.raises(benchmark._BenchmarkError, match=message):
        benchmark._validate_official_identity(official, **identity)


def test_operation_route_supplies_exact_upload_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    generic = tmp_path / 'generic.bin'
    direct = tmp_path / 'direct.bin'
    multipart = tmp_path / 'multipart.bin'
    generic.write_bytes(b'g')
    direct.write_bytes(b'dd')
    multipart.write_bytes(b'mmm')
    state = SimpleNamespace(
        direct_payload=direct,
        generic_payload=generic,
        multipart_payload=multipart,
        server_info={},
    )
    observed: list[tuple[str, str | None, int]] = []

    def classify(
        _server_info: Mapping[str, object],
        entity_type: str,
        field_name: str | None,
        file_size: int,
    ) -> str:
        observed.append((entity_type, field_name, file_size))
        return 'capability:direct'

    monkeypatch.setattr(benchmark, '_capability_upload_route', classify)

    for name in ('upload_generic', 'upload_direct', 'upload_multipart', 'rss_upload'):
        benchmark._operation_route(name, state)

    assert observed == [
        ('Shot', None, 1),
        ('Version', 'sg_uploaded_movie', 2),
        ('Version', 'sg_uploaded_movie', 3),
        ('Version', 'sg_uploaded_movie', 3),
    ]


def test_original_work_failure_is_preserved_when_cleanup_succeeds() -> None:
    benchmark = _load_benchmark()
    failure = ValueError('work failed')

    def work() -> None:
        raise failure

    with pytest.raises(ValueError, match='work failed') as captured:
        benchmark._run_with_cleanup(work, _noop)

    assert captured.value is failure


@pytest.mark.parametrize('failure_phase', ['prepare', 'call'])
def test_parallel_read_latency_worker_closes_operations_and_client_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    raw_error = RuntimeError('RAW parallel failure sentinel')

    def fail() -> None:
        raise raw_error

    prepare_failure, call_failure = {
        'prepare': (fail, _noop),
        'call': (_noop, fail),
    }[failure_phase]

    monkeypatch.setattr(benchmark, '_LATENCY_OPERATIONS', ('parallel_read',))
    monkeypatch.setattr(benchmark, '_LiveState', _ParallelLatencyFailureState)
    monkeypatch.setattr(benchmark, '_Operations', _ParallelLatencyFailureOperations)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._latency_worker(
            SimpleNamespace(),
            {
                'call_failure': call_failure,
                'client': 'official',
                'events': events,
                'prepare_failure': prepare_failure,
            },
        )

    assert 'RAW' not in str(captured.value)
    expected = {
        'prepare': ['state:prepare', 'operations:prepare'],
        'call': ['state:prepare', 'operations:prepare', 'operations:call'],
    }[failure_phase]
    assert events == [*expected, 'operations:close', 'client:close', 'state:cleanup']


def test_parallel_read_methodology_describes_asymmetric_client_ownership() -> None:
    benchmark = _load_benchmark()
    result = {
        'artifact_sha256': '1' * 64,
        'candidate_commit': '2' * 40,
        'official_commit': benchmark._OFFICIAL_COMMIT,
        'official_ref': benchmark._OFFICIAL_REF,
        'machine': {
            'architecture': 'arm64',
            'os': 'Darwin',
            'platform': 'darwin',
            'python_version': '3.14.0',
        },
        'workload': {'created_entities': 184, 'upload_bytes': 332032784, 'workers': 18},
    }
    clients = {
        'official': {'version': benchmark._OFFICIAL_VERSION},
        'ours': {'version': '0.1.2'},
    }
    generated = ' '.join(
        benchmark._report_header_lines(
            result,
            'example.invalid',
            clients,
            2,
            7,
        )
    )
    pending = ' '.join((_REPO_ROOT / 'docs' / 'benchmarks.md').read_text(encoding='ascii').split())

    for text in (generated, pending):
        assert 'official row uses one thread-owned client per persistent thread' in text
        assert 'replacement row shares one client across four persistent threads' in text
        assert 'Both issue four concurrent reads' in text
        assert 'exclude thread and client setup and closure from timing' in text


def test_parallel_runner_official_clients_close_on_owning_threads_after_call_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    barrier = threading.Barrier(4)
    clients: list[_ParallelRunnerFailureClient] = []
    lock = threading.Lock()
    make_calls = [0]
    measured_close_threads: list[int] = []
    raw_error = RuntimeError('RAW runner failure sentinel')
    timer_calls = 0

    original_measure_verified = benchmark._measure_verified

    def measure_verified(
        name: str,
        operation: Callable[[], _MeasuredResult],
        verify: Callable[[_MeasuredResult], bool],
        accepted: list[float],
    ) -> _MeasuredResult:
        # Observe the timer boundary after persistent setup completes.
        nonlocal timer_calls
        timer_calls += 1
        assert make_calls == [4]
        assert len(clients) == 4
        return original_measure_verified(
            name,
            operation,
            verify,
            accepted,
            timer=lambda: 1.0,
        )

    monkeypatch.setattr(benchmark, '_LATENCY_OPERATIONS', ('parallel_read',))
    monkeypatch.setattr(benchmark, '_LiveState', _ParallelRunnerFailureState)
    monkeypatch.setattr(benchmark, '_measure_verified', measure_verified)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._latency_worker(
            SimpleNamespace(),
            {
                'barrier': barrier,
                'client': 'official',
                'lock': lock,
                'make_calls': make_calls,
                'measured_close_threads': measured_close_threads,
                'parallel_clients': clients,
                'raw_error': raw_error,
            },
        )

    assert 'RAW' not in str(captured.value)
    assert make_calls == [4]
    assert len(clients) == 4
    assert all(client.close_thread == client.owner_thread for client in clients)
    assert len(measured_close_threads) == 1
    assert timer_calls == 1


def test_parallel_runner_official_clients_close_on_owning_threads_after_setup_failure() -> None:
    benchmark = _load_benchmark()
    clients: list[Client] = []
    lock = threading.Lock()
    make_calls = 0
    raw_error = RuntimeError('RAW runner setup failure sentinel')

    class Client:
        def __init__(self) -> None:
            self.close_thread: int | None = None
            self.owner_thread = threading.get_ident()

        def close(self) -> None:
            self.close_thread = threading.get_ident()

    class State:
        def _make_client(self) -> Client:
            nonlocal make_calls
            with lock:
                make_calls += 1
                call_number = make_calls
            if call_number == 1:
                raise raw_error
            client = Client()
            with lock:
                clients.append(client)
            return client

    with pytest.raises(RuntimeError) as captured:
        benchmark._ParallelReadRunner(State(), None)

    assert captured.value is raw_error
    assert make_calls == 4
    assert len(clients) == 3
    assert all(client.close_thread == client.owner_thread for client in clients)


def test_parallel_runner_official_clients_remain_owned_by_persistent_threads() -> None:
    benchmark = _load_benchmark()
    barrier = threading.Barrier(4)
    clients: list[Client] = []
    lock = threading.Lock()

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []
            self.close_thread: int | None = None
            self.owner_thread = threading.get_ident()

        def close(self) -> None:
            self.close_thread = threading.get_ident()

        def find_one(
            self,
            _entity_type: str,
            filters: list[list[object]],
            _fields: list[str],
        ) -> dict[str, object]:
            entity_id = cast('int', filters[0][2])
            self.calls.append((threading.get_ident(), entity_id))
            barrier.wait(timeout=5)
            return {'type': 'Shot', 'id': entity_id, 'code': f'shot-{entity_id}'}

    class State:
        def _make_client(self) -> Client:
            client = Client()
            with lock:
                clients.append(client)
            return client

    runner = benchmark._ParallelReadRunner(State(), None)
    try:
        cold = runner._read((7, 3, 11, 5))
        warm = runner._read((7, 3, 11, 5))
    finally:
        runner._close()

    assert [result['id'] for result in cold] == [7, 3, 11, 5]
    assert [result['id'] for result in warm] == [7, 3, 11, 5]
    assert len(clients) == 4
    assert len({client.owner_thread for client in clients}) == 4
    for client in clients:
        assert client.close_thread == client.owner_thread
        assert len(client.calls) == 2
        assert {thread_id for thread_id, _entity_id in client.calls} == {client.owner_thread}


def test_parallel_runner_partial_thread_start_failure_closes_started_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    clients: list[_PartialStartClient] = []
    clients_ready = threading.Event()
    created_threads: list[_PartialStartThread] = []
    lock = threading.Lock()
    raw_error = RuntimeError('RAW partial thread start failure')
    real_thread = benchmark.threading.Thread

    def make_client() -> _PartialStartClient:
        client = _PartialStartClient()
        with lock:
            clients.append(client)
            if len(clients) == 2:
                clients_ready.set()
        return client

    def thread_factory(
        *,
        target: Callable[[int], None],
        args: tuple[int],
        name: str,
    ) -> _PartialStartThread:
        thread = _PartialStartThread(real_thread, raw_error, target, args, name)
        created_threads.append(thread)
        return thread

    monkeypatch.setattr(benchmark.threading, 'Thread', thread_factory)

    with pytest.raises(RuntimeError) as captured:
        benchmark._ParallelReadRunner(
            SimpleNamespace(_make_client=make_client),
            None,
        )

    runner = cast('Any', created_threads[0].target).__self__
    try:
        assert clients_ready.wait(timeout=5)
        assert captured.value is raw_error
        assert all(client.close_thread == client.owner_thread for client in clients)
    finally:
        runner._close()


def test_parallel_runner_post_start_failure_closes_started_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    clients: list[_PartialStartClient] = []
    clients_ready = threading.Event()
    created_threads: list[_PostStartFailureThread] = []
    lock = threading.Lock()
    raw_error = RuntimeError('RAW post-start failure')
    real_thread = benchmark.threading.Thread

    def make_client() -> _PartialStartClient:
        client = _PartialStartClient()
        with lock:
            clients.append(client)
            if len(clients) == 3:
                clients_ready.set()
        return client

    def thread_factory(
        *,
        target: Callable[[int], None],
        args: tuple[int],
        name: str,
    ) -> _PostStartFailureThread:
        thread = _PostStartFailureThread(real_thread, raw_error, target, args, name)
        created_threads.append(thread)
        return thread

    monkeypatch.setattr(benchmark.threading, 'Thread', thread_factory)

    with pytest.raises(RuntimeError) as captured:
        benchmark._ParallelReadRunner(
            SimpleNamespace(_make_client=make_client),
            None,
        )

    runner = cast('Any', created_threads[0].target).__self__
    try:
        assert clients_ready.wait(timeout=5)
        assert captured.value is raw_error
        assert [thread.ident is not None for thread in created_threads] == [
            True,
            True,
            True,
            False,
        ]
        assert len(clients) == 3
        assert all(client.close_thread == client.owner_thread for client in clients)
    finally:
        runner._queues[2].put(None)
        created_threads[2].join()


def test_parallel_runner_replacement_shares_one_client_across_persistent_threads() -> None:
    benchmark = _load_benchmark()
    barrier = threading.Barrier(4)
    calls: list[tuple[int, int]] = []
    close_threads: list[int] = []
    lock = threading.Lock()

    class Client:
        def close(self) -> None:
            close_threads.append(threading.get_ident())

        def find_one(
            self,
            _entity_type: str,
            filters: list[list[object]],
            _fields: list[str],
        ) -> dict[str, object]:
            entity_id = cast('int', filters[0][2])
            with lock:
                calls.append((threading.get_ident(), entity_id))
            barrier.wait(timeout=5)
            return {'type': 'Shot', 'id': entity_id, 'code': f'shot-{entity_id}'}

    def make_client() -> object:
        pytest.fail('replacement runner constructed an extra client')

    shared = Client()
    runner = benchmark._ParallelReadRunner(
        SimpleNamespace(_make_client=make_client),
        shared,
    )
    try:
        cold = runner._read((13, 2, 8, 1))
        warm = runner._read((13, 2, 8, 1))
    finally:
        runner._close()
        runner._close()

    assert [result['id'] for result in cold] == [13, 2, 8, 1]
    assert [result['id'] for result in warm] == [13, 2, 8, 1]
    thread_ids = {thread_id for thread_id, _entity_id in calls}
    assert len(thread_ids) == 4
    assert all(
        sum(call_thread == thread_id for call_thread, _entity_id in calls) == 2
        for thread_id in thread_ids
    )
    assert close_threads == []


def test_payload_write_uses_bounded_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    benchmark = _load_benchmark()
    recorder = _PayloadWriteRecorder()

    def open_payload(_path: Path, _mode: str) -> _PayloadWriteRecorder:
        return recorder

    monkeypatch.setattr(Path, 'open', open_payload)

    benchmark._write_payload(Path('empty.bin'), 0)
    assert recorder.lengths == []

    benchmark._write_payload(Path('large.bin'), (2 << 20) + 17)
    assert recorder.lengths == [1 << 20, 1 << 20, 17]


def test_pending_benchmark_docs_describe_safe_default_and_download_setup() -> None:
    source = (_REPO_ROOT / 'docs' / 'benchmarks.md').read_text(encoding='ascii')
    text = ' '.join(source.split())

    assert 'default 2 paired rounds' in text
    assert '18 worker processes' in text
    assert '184 server entities' in text
    assert '332,032,784 uploaded bytes' in text
    assert '162 worker processes' in text
    assert '1,840 server entities' in text
    assert '3,320,327,840 uploaded bytes' in text
    assert 'fresh download-only process' in text
    assert 'larger even `BENCH_N`' in text


def test_preflight_crosses_real_isolated_process_and_selects_exact_package(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    selected_root = tmp_path / 'selected'
    selected_package = selected_root / 'shotgun_api3'
    selected_package.mkdir(parents=True)
    selected_init = selected_package / '__init__.py'
    selected_init.write_text("__version__ = '1.2.3'\n", encoding='ascii')
    shadow_root = tmp_path / 'shadow'
    shadow_package = shadow_root / 'shotgun_api3'
    shadow_package.mkdir(parents=True)
    (shadow_package / '__init__.py').write_text("__version__ = '9.9.9'\n", encoding='ascii')
    monkeypatch.setenv('PYTHONPATH', str(shadow_root))
    config = {
        'api_key': 'unused',
        'client': 'ours',
        'client_root': str(selected_root),
        'expected_root': str(selected_root),
        'expected_version': '1.2.3',
        'mode': 'preflight',
        'namespace': 'preflight',
        'project': 'unused',
        'round': 0,
        'script_name': 'unused',
        'site': 'https://example.invalid',
        'work_root': str(tmp_path),
    }

    result = benchmark._launch_configured_worker(config, tmp_path, 0)

    assert result == {
        'client': 'ours',
        'origin': str(selected_init.resolve()),
        'version': '1.2.3',
    }
    assert not (tmp_path / 'worker-0.json').exists()


def test_preflight_rejects_unsupported_rss_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()

    def find_spec(_name: str) -> object:
        return object()

    def getrusage(_kind: object) -> None:
        pass

    def import_module(_name: str) -> object:
        return SimpleNamespace(RUSAGE_SELF=object(), getrusage=getrusage)

    def which(_name: str) -> str:
        return '/usr/bin/git'

    monkeypatch.setattr(benchmark.shutil, 'which', which)
    monkeypatch.setattr(benchmark.importlib.util, 'find_spec', find_spec)
    monkeypatch.setattr(benchmark.importlib, 'import_module', import_module)
    monkeypatch.setattr(benchmark.sys, 'platform', 'win32')

    with pytest.raises(benchmark._BenchmarkError, match='RSS platform'):
        benchmark._preflight_tools()


def test_raw_cold_and_warm_samples_have_median_and_nearest_rank_tail() -> None:
    benchmark = _load_benchmark()

    summary = benchmark._sample_summary([0.4, 0.1, 0.3, 0.2], tail_percentile=0.95)

    assert summary == {
        'median': pytest.approx(0.25),
        'p95': pytest.approx(0.4),
        'raw': [0.4, 0.1, 0.3, 0.2],
    }
    with pytest.raises(benchmark._BenchmarkError, match='finite'):
        benchmark._sample_summary([float('nan')], tail_percentile=0.95)


@pytest.mark.parametrize('owned_failure', [True, False])
def test_recover_worker_failure_preserves_both_diagnostics_without_leaking_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    owned_failure: bool,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    config = _worker_config(tmp_path)

    def launch(config_path: Path, root: Path) -> dict[str, str]:
        launched = cast(
            'dict[str, object]',
            json.loads(config_path.read_text(encoding='ascii')),
        )
        if launched['mode'] != 'cleanup':
            journal_type._load(root)._record('Attachment', 987654321)
            if owned_failure:
                raise benchmark._BenchmarkError('Benchmark worker timed out')
            raise RuntimeError('SECRET worker exception detail')
        raise benchmark._BenchmarkError('SECRET cleanup response and traceback')

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._CleanupError) as captured:
        benchmark._launch_configured_worker(config, tmp_path, 9)

    expected = (
        'Benchmark worker timed out'
        if owned_failure
        else 'Benchmark worker failed at an unexpected boundary'
    )
    assert str(captured.value) == expected + '; warning: Benchmark worker recovery failed'
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert 'SECRET' not in str(captured.value)
    assert '987654321' not in str(captured.value)
    assert not tuple(tmp_path.glob('*worker*.json'))
    assert not tuple(tmp_path.glob('*cleanup*.json'))


def test_recover_worker_rejects_malformed_journal_without_launching_live_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    (tmp_path / 'resource-journal.json').write_text(
        '{"resources":[{"entity_type":"SECRET-journal","id":987654321}]}\n',
        encoding='ascii',
    )
    launched = False

    def launch(_config_path: Path, _root: Path) -> dict[str, str]:
        nonlocal launched
        launched = True
        raise AssertionError('cleanup worker must not launch')

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._CleanupError) as captured:
        benchmark._recover_worker(_worker_config(tmp_path), tmp_path, 3)

    assert str(captured.value) == 'Benchmark worker recovery failed'
    assert launched is False
    assert not tuple(tmp_path.glob('*cleanup*.json'))


def test_recover_worker_rejects_nonempty_success_after_successful_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal

    def launch(config_path: Path, root: Path) -> dict[str, object]:
        config = cast(
            'dict[str, object]',
            json.loads(config_path.read_text(encoding='ascii')),
        )
        if config['mode'] == 'cleanup':
            journal_type._load(root)._clear()
            return {'cleanup': 'PASS'}
        journal_type._load(root)._record('Version', 19)
        return {'client': 'official', 'operations': {}, 'rss': {}}

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_configured_worker(
            _worker_config(tmp_path),
            tmp_path,
            10,
        )

    assert str(captured.value) == 'Benchmark worker cleanup contract failed'
    assert journal_type._load(tmp_path)._resources == ()


@pytest.mark.parametrize(
    ('result', 'clear_journal'),
    [
        ({'cleanup': 'PASS', 'extra': 'SECRET-response'}, True),
        ({'cleanup': 'PASS'}, False),
    ],
    ids=('unexpected-result', 'nonempty-journal'),
)
def test_recover_worker_requires_exact_result_and_empty_journal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    result: dict[str, str],
    clear_journal: bool,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    journal._record('Shot', 987654321)
    cleanup_config: Path | None = None

    def launch(config_path: Path, root: Path) -> dict[str, str]:
        nonlocal cleanup_config
        cleanup_config = config_path
        if clear_journal:
            journal_type._load(root)._clear()
        return result

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._CleanupError) as captured:
        benchmark._recover_worker(_worker_config(tmp_path), tmp_path, 2)

    assert str(captured.value) == 'Benchmark worker recovery failed'
    assert 'SECRET-response' not in str(captured.value)
    assert '987654321' not in str(captured.value)
    assert cleanup_config is not None
    assert not cleanup_config.exists()


def test_recover_worker_reuses_exact_client_identity_and_removes_private_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    journal._record('Shot', 17)
    config = {**_worker_config(tmp_path), 'rss_operation': 'rss_upload'}
    observed: list[dict[str, object]] = []
    config_paths: list[Path] = []

    def launch(config_path: Path, root: Path) -> dict[str, str]:
        assert root == tmp_path
        if os.name == 'posix':
            assert config_path.stat().st_mode & 0o777 == 0o600
        cleanup_config = cast(
            'dict[str, object]',
            json.loads(config_path.read_text(encoding='ascii')),
        )
        observed.append(cleanup_config)
        config_paths.append(config_path)
        journal_type._load(root)._clear()
        return {'cleanup': 'PASS'}

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    benchmark._recover_worker(config, tmp_path, 6)

    assert len(observed) == 1
    assert observed[0] == {**_worker_config(tmp_path), 'mode': 'cleanup'}
    assert config_paths[0].name != 'worker-6.json'
    assert not config_paths[0].exists()
    assert journal_type._load(tmp_path)._resources == ()


@pytest.mark.parametrize(
    'worker_failure',
    [
        'Benchmark worker failed with status -11',
        'Benchmark worker failed at an ordinary boundary',
        'Benchmark worker timed out',
    ],
    ids=('abnormal-exit', 'ordinary-failure', 'timeout'),
)
def test_recover_worker_runs_after_each_worker_failure_and_preserves_original(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    worker_failure: str,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    config = _worker_config(tmp_path)
    launches: list[dict[str, object]] = []

    def launch(config_path: Path, root: Path) -> dict[str, str]:
        launched_config = cast(
            'dict[str, object]',
            json.loads(config_path.read_text(encoding='ascii')),
        )
        launches.append(launched_config)
        if launched_config['mode'] != 'cleanup':
            journal_type._load(root)._record('Shot', 17)
            raise benchmark._BenchmarkError(worker_failure)
        journal_type._load(root)._clear()
        return {'cleanup': 'PASS'}

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_configured_worker(config, tmp_path, 8)

    assert str(captured.value) == worker_failure
    assert [launch['mode'] for launch in launches] == ['latency', 'cleanup']
    assert launches[1] == {**config, 'mode': 'cleanup'}
    assert journal_type._load(tmp_path)._resources == ()
    assert not tuple(tmp_path.glob('*worker*.json'))
    assert not tuple(tmp_path.glob('*cleanup*.json'))


@pytest.mark.parametrize(
    'process_control',
    [KeyboardInterrupt(), SystemExit(17), asyncio.CancelledError(), _StopBenchmark()],
)
def test_recovery_process_control_overrides_ordinary_worker_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_control: BaseException,
) -> None:
    benchmark = _load_benchmark()

    def launch(_config_path: Path, _root: Path) -> dict[str, object]:
        raise benchmark._BenchmarkError('Benchmark worker timed out')

    def recover(_config: Mapping[str, object], _root: Path, _sequence: int) -> None:
        raise process_control

    monkeypatch.setattr(benchmark, '_launch_worker', launch)
    monkeypatch.setattr(benchmark, '_recover_worker', recover)
    with pytest.raises(type(process_control)) as captured:
        benchmark._launch_configured_worker(_worker_config(tmp_path), tmp_path, 4)
    assert captured.value is process_control
    assert not tuple(tmp_path.glob('*worker*.json'))


def test_resource_journal_accepts_exact_schema_and_exposes_read_only_resources(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_path = tmp_path / 'resource-journal.json'
    journal_path.write_text(
        json.dumps(
            {
                'resources': [
                    {'entity_type': 'Shot', 'id': 7},
                    {'entity_type': 'Version', 'id': 8},
                    {'entity_type': 'Attachment', 'id': 9},
                ]
            }
        ),
        encoding='ascii',
    )

    journal = benchmark._ResourceJournal._load(tmp_path)

    assert journal._resources == (
        ('Shot', 7),
        ('Version', 8),
        ('Attachment', 9),
    )
    with pytest.raises(AttributeError):
        journal._resources = ()


def test_resource_journal_enforces_64_entry_bound_without_partial_record(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    for identity in range(1, 65):
        journal._record('Shot', identity)

    with pytest.raises(benchmark._BenchmarkError, match='resource journal'):
        journal._record('Shot', 65)

    expected = tuple(('Shot', identity) for identity in range(1, 65))
    assert journal._resources == expected
    assert journal_type._load(tmp_path)._resources == expected


@pytest.mark.parametrize('failing_operation', ['write', 'replace'])
def test_resource_journal_failure_preserves_published_snapshot_and_removes_temporary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failing_operation: str,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    destination = tmp_path / 'resource-journal.json'
    published = destination.read_bytes()
    original_write = os.write
    write_calls = 0

    def replace(_source: Path, _target: Path) -> Path:
        raise OSError('RAW replace failure')

    def write(descriptor: int, value: bytes) -> int:
        nonlocal write_calls
        write_calls += 1
        if failing_operation == 'write' and write_calls == 2:
            raise OSError('RAW write failure')
        size = max(1, len(value) // 2) if failing_operation == 'write' else len(value)
        return original_write(descriptor, value[:size])

    monkeypatch.setattr(benchmark.os, 'write', write)
    if failing_operation == 'replace':
        monkeypatch.setattr(Path, 'replace', replace)

    with pytest.raises(benchmark._BenchmarkError, match='resource journal') as captured:
        journal._record('Shot', 7)

    assert 'RAW' not in str(captured.value)
    assert destination.read_bytes() == published
    assert journal._resources == ()
    assert not tuple(tmp_path.glob('.resource-journal.json.*'))


@pytest.mark.parametrize(
    ('mode', 'expects_journal'),
    [
        ('latency', True),
        ('rss', True),
        ('rss_download_setup', True),
        ('preflight', False),
    ],
)
def test_resource_journal_is_created_empty_before_live_worker_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: str,
    expects_journal: bool,
) -> None:
    benchmark = _load_benchmark()
    journal_path = tmp_path / 'resource-journal.json'
    observed: list[bool] = []

    def launch(_config_path: Path, root: Path) -> dict[str, object]:
        observed.append(journal_path.exists())
        if expects_journal:
            journal = benchmark._ResourceJournal._load(root)
            assert journal._resources == ()
        return {}

    monkeypatch.setattr(benchmark, '_launch_worker', launch)

    benchmark._launch_configured_worker({'mode': mode}, tmp_path, 0)

    assert observed == [expects_journal]
    assert journal_path.exists() is expects_journal


@pytest.mark.parametrize(
    'payload',
    cast(
        'list[object]',
        [
            {},
            {'resources': [], 'extra': True},
            {'resources': 'not-a-list'},
            {'resources': [[]]},
            {'resources': [{'entity_type': 'Shot', 'id': 1, 'extra': True}]},
            {'resources': [{'entity_type': 'Project', 'id': 1}]},
            {'resources': [{'entity_type': 'Shot', 'id': True}]},
            {'resources': [{'entity_type': 'Shot', 'id': 0}]},
            {'resources': [{'entity_type': 'Shot', 'id': -1}]},
            {'resources': [{'entity_type': 'Shot', 'id': '1'}]},
            {
                'resources': [
                    {'entity_type': 'Shot', 'id': 1},
                    {'entity_type': 'Shot', 'id': 1},
                ]
            },
            {'resources': [{'entity_type': 'Shot', 'id': identity} for identity in range(1, 66)]},
        ],
    ),
    ids=(
        'missing-resources',
        'top-level-extra',
        'resources-not-list',
        'entry-not-object',
        'entry-extra',
        'unknown-entity-type',
        'boolean-id',
        'zero-id',
        'negative-id',
        'noninteger-id',
        'duplicate',
        'oversized',
    ),
)
def test_resource_journal_rejects_invalid_schema(
    tmp_path: Path,
    payload: object,
) -> None:
    benchmark = _load_benchmark()
    (tmp_path / 'resource-journal.json').write_text(
        json.dumps(payload),
        encoding='ascii',
    )

    with pytest.raises(benchmark._BenchmarkError, match='resource journal'):
        benchmark._ResourceJournal._load(tmp_path)


def test_resource_journal_rejects_non_ascii_data(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    (tmp_path / 'resource-journal.json').write_bytes(b'{"resources":[],"\xc3\xa9":true}')

    with pytest.raises(benchmark._BenchmarkError, match='resource journal'):
        benchmark._ResourceJournal._load(tmp_path)


@pytest.mark.parametrize('operation', ['_create', '_load'])
def test_resource_journal_rejects_path_outside_worker_root(
    tmp_path: Path,
    operation: str,
) -> None:
    benchmark = _load_benchmark()
    worker_root = tmp_path / 'worker'
    worker_root.mkdir()
    outside = tmp_path / 'outside.json'
    outside.write_text('{"resources":[]}', encoding='ascii')
    (worker_root / 'resource-journal.json').symlink_to(outside)

    with pytest.raises(benchmark._BenchmarkError, match='resource journal'):
        getattr(benchmark._ResourceJournal, operation)(worker_root)


def test_resource_journal_writes_atomically_with_owner_only_permissions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    journal_type = benchmark._ResourceJournal
    journal = journal_type._create(tmp_path)
    destination = tmp_path / 'resource-journal.json'
    original_replace = Path.replace
    replacements: list[tuple[Path, Path]] = []
    flushes: list[int] = []

    def replace(source: Path, target: Path) -> Path:
        assert destination.read_text(encoding='ascii') == '{"resources":[]}\n'
        if os.name == 'posix':
            assert source.stat().st_mode & 0o777 == 0o600
        replacements.append((source, target))
        return original_replace(source, target)

    original_fsync = os.fsync

    def recording_fsync(descriptor: int) -> None:
        flushes.append(descriptor)
        original_fsync(descriptor)

    monkeypatch.setattr(benchmark.os, 'fsync', recording_fsync)
    monkeypatch.setattr(Path, 'replace', replace)

    journal._record('Shot', 7)

    assert flushes
    assert replacements
    assert replacements[-1][1] == destination
    if os.name == 'posix':
        assert destination.stat().st_mode & 0o777 == 0o600
    assert journal_type._load(tmp_path)._resources == (('Shot', 7),)
    assert not tuple(tmp_path.glob('.resource-journal.json.*'))


@pytest.mark.parametrize(
    ('client', 'name', 'bad_route'),
    [
        ('ours', 'upload_direct', 'observed:rpc'),
        ('ours', 'upload_direct', 'observed:rpc+part_put'),
        ('ours', 'upload_multipart', 'observed:allocation+link+object_put'),
        (
            'ours',
            'upload_multipart',
            'observed:finalize+rpc',
        ),
        ('official', 'upload_direct', 'capability:form'),
        ('official', 'upload_multipart', 'capability:direct'),
    ],
)
def test_round_rejects_direct_and_multipart_route_mismatch(
    client: str,
    name: str,
    bad_route: str,
) -> None:
    benchmark = _load_benchmark()
    result: dict[str, Any] = {
        'client': client,
        'operations': _operation_result(benchmark, client=client),
        'rss': {rss_name: _rss_result(client, rss_name) for rss_name in benchmark._RSS_OPERATIONS},
    }
    result['operations'][name]['route'] = bad_route

    with pytest.raises(benchmark._BenchmarkError, match='route evidence'):
        benchmark._validate_round_result(result)


@pytest.mark.parametrize('rss_name', ['rss_download', 'rss_upload'])
@pytest.mark.parametrize('cleanup_fails', [False, True])
def test_rss_correctness_and_cleanup_diagnostics_survive_nested_workers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    rss_name: str,
    cleanup_fails: bool,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')

    class Client(_RssFixtureClient):
        def download_attachment(self, _identity: int, *, file_path: str) -> str:
            events.append('corrupt-download')
            Path(file_path).write_bytes(b'corrupted-transfer')
            return file_path

    class State(_RssFixtureState):
        def _cleanup(self) -> None:
            events.append('cleanup')
            if cleanup_fails:
                raise RuntimeError('SECRET cleanup response')

        def _cleanup_clients(self) -> None:
            self._cleanup()

        def _client(self) -> Client:
            client = Client(events, source)
            self.clients.append(client)
            return client

    def state_factory(_module: object, _config: Mapping[str, Any]) -> State:
        return State(events, source, tmp_path)

    def getrusage(_kind: object) -> SimpleNamespace:
        return SimpleNamespace(ru_maxrss=100)

    resource = SimpleNamespace(RUSAGE_SELF=0, getrusage=getrusage)
    original_import = benchmark.importlib.import_module

    def import_module(name: str) -> object:
        return resource if name == 'resource' else original_import(name)

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark.importlib, 'import_module', import_module)
    monkeypatch.setattr(benchmark, '_MULTIPART_LIMIT', 1)
    config = {
        'client': 'official',
        'download_attachment_id': 7,
        'download_route': 'download',
        'expected_download_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
        'rss_operation': rss_name,
    }
    with pytest.raises(benchmark._BenchmarkError) as rss_error:
        benchmark._rss_worker(SimpleNamespace(), config)
    diagnostic = f'benchmark correctness failed for {rss_name}'
    diagnostic += '; warning: benchmark cleanup failed' if cleanup_fails else ''
    assert str(rss_error.value) == diagnostic
    assert 'corrupt-download' in events
    assert events[-1] == 'cleanup'

    stderr = f'{rss_error.value}\n'

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, '', stderr)

    with pytest.raises(benchmark._BenchmarkError) as child_error:
        benchmark._launch_worker(tmp_path / 'child.json', tmp_path, env={}, run=run)
    diagnostic = 'Benchmark worker failed with status 1: ' + diagnostic
    assert str(child_error.value) == diagnostic

    with pytest.raises(benchmark._CleanupError) as recovery_error:
        benchmark._raise_worker_errors(
            child_error.value, None, RuntimeError('SECRET recovery response')
        )
    diagnostic += '; warning: Benchmark worker recovery failed'
    assert str(recovery_error.value) == diagnostic

    stderr = f'{recovery_error.value}\n'
    with pytest.raises(benchmark._BenchmarkError) as outer_error:
        benchmark._launch_worker(tmp_path / 'outer.json', tmp_path, env={}, run=run)
    assert str(outer_error.value) == 'Benchmark worker failed with status 1: ' + diagnostic


def test_rss_download_setup_cleans_after_download_worker_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')
    failure = benchmark._BenchmarkError('download worker failed')

    def launch(*_args: object) -> dict[str, object]:
        events.append('launch')
        raise failure

    def state_factory(_module: object, _config: Mapping[str, Any]) -> _RssFixtureState:
        return _RssFixtureState(events, source, tmp_path)

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._rss_download_setup_worker(
            SimpleNamespace(),
            {
                'client': 'official',
                'mode': 'rss_download_setup',
                'namespace': 'ns',
                'rss_operation': 'rss_download',
                'work_root': str(tmp_path),
            },
        )

    assert captured.value is failure
    assert events == ['prepare:rss_download', 'client', 'upload', 'launch', 'cleanup']


def test_rss_download_setup_cleanup_failure_has_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')

    class State(_RssFixtureState):
        def _cleanup(self) -> None:
            events.append('cleanup')
            raise RuntimeError('raw cleanup detail')

    def launch(*_args: object) -> dict[str, object]:
        events.append('launch')
        raise benchmark._BenchmarkError('download worker failed')

    def state_factory(_module: object, _config: Mapping[str, Any]) -> State:
        return State(events, source, tmp_path)

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)

    with pytest.raises(benchmark._CleanupError, match='cleanup failed') as captured:
        benchmark._rss_download_setup_worker(
            SimpleNamespace(),
            {
                'client': 'official',
                'mode': 'rss_download_setup',
                'namespace': 'ns',
                'rss_operation': 'rss_download',
                'work_root': str(tmp_path),
            },
        )

    assert 'raw cleanup detail' not in str(captured.value)
    assert events == ['prepare:rss_download', 'client', 'upload', 'launch', 'cleanup']


def test_rss_download_setup_hands_off_sanitized_fixture_and_cleans(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')
    launched: dict[str, object] = {}

    def launch(
        config: Mapping[str, object],
        root: Path,
        sequence: int,
    ) -> dict[str, object]:
        events.append('launch')
        launched.update(config=config, root=root, sequence=sequence)
        return {
            'client': 'official',
            'operations': {},
            'rss': {'rss_download': _rss_result('official', 'rss_download')},
        }

    def state_factory(_module: object, _config: Mapping[str, Any]) -> _RssFixtureState:
        return _RssFixtureState(events, source, tmp_path)

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)

    result = benchmark._rss_download_setup_worker(
        SimpleNamespace(),
        {
            'client': 'official',
            'mode': 'rss_download_setup',
            'namespace': 'ns',
            'rss_operation': 'rss_download',
            'work_root': str(tmp_path),
        },
    )

    child = cast('dict[str, object]', launched['config'])
    assert child['mode'] == 'rss'
    assert child['rss_operation'] == 'rss_download'
    assert child['download_attachment_id'] == 7
    assert child['expected_download_sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert child['download_route'] == 'download'
    assert cast('Path', launched['root']).parent == tmp_path
    assert events == ['prepare:rss_download', 'client', 'upload', 'launch', 'cleanup']
    assert result['rss']['rss_download']['correct'] is True


@pytest.mark.parametrize('expected_matches', [True, False])
def test_rss_download_worker_uses_handoff_digest_without_setup_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    expected_matches: bool,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []
    source = tmp_path / 'large.bin'
    source.write_bytes(b'large-transfer-content')
    expected_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if not expected_matches:
        expected_digest = '0' * 64
    samples = iter((100, 160))

    def getrusage(_kind: object) -> SimpleNamespace:
        events.append('rss')
        return SimpleNamespace(ru_maxrss=next(samples))

    resource = SimpleNamespace(RUSAGE_SELF=object(), getrusage=getrusage)
    original_import = benchmark.importlib.import_module

    def import_module(name: str) -> object:
        return resource if name == 'resource' else original_import(name)

    def state_factory(_module: object, _config: Mapping[str, Any]) -> _RssFixtureState:
        return _RssFixtureState(events, source, tmp_path)

    monkeypatch.setattr(benchmark, '_LiveState', state_factory)
    monkeypatch.setattr(benchmark.importlib, 'import_module', import_module)
    monkeypatch.setattr(benchmark.sys, 'platform', 'darwin')
    config = {
        'client': 'official',
        'download_attachment_id': 7,
        'download_route': 'download',
        'expected_download_sha256': expected_digest,
        'rss_operation': 'rss_download',
    }

    if expected_matches:
        result = benchmark._rss_worker(SimpleNamespace(), config)
        assert result['rss']['rss_download']['correct'] is True
    else:
        with pytest.raises(benchmark._CorrectnessError, match='rss_download'):
            benchmark._rss_worker(SimpleNamespace(), config)

    assert 'prepare:rss_download' not in events
    assert 'upload' not in events
    assert events[-1] == 'client-cleanup'


def test_run_with_cleanup_preserves_first_process_control() -> None:
    benchmark = _load_benchmark()
    first = KeyboardInterrupt()
    second = SystemExit(17)
    cleanup_called = False

    def cleanup() -> None:
        nonlocal cleanup_called
        cleanup_called = True
        raise second

    def work() -> None:
        raise first

    with pytest.raises(KeyboardInterrupt) as captured:
        benchmark._run_with_cleanup(work, cleanup)

    assert cleanup_called is True
    assert captured.value is first


@pytest.mark.parametrize(
    'process_control',
    [KeyboardInterrupt(), SystemExit(17), _StopBenchmark()],
)
def test_run_with_cleanup_propagates_cleanup_process_control(
    process_control: BaseException,
) -> None:
    benchmark = _load_benchmark()

    def cleanup() -> None:
        raise process_control

    def work() -> None:
        raise RuntimeError('ordinary benchmark failure')

    with pytest.raises(type(process_control)) as captured:
        benchmark._run_with_cleanup(work, cleanup)

    assert captured.value is process_control


def test_successful_controller_keeps_curated_page_and_writes_private_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    curated = tmp_path / 'docs' / 'benchmarks.md'
    curated.parent.mkdir()
    curated.write_text('curated page\n', encoding='ascii')
    official = tmp_path / 'official'
    (official / 'shotgun_api3').mkdir(parents=True)
    (official / 'shotgun_api3' / '__init__.py').touch()
    evidence = benchmark._ArtifactEvidence('a' * 40, 'b' * 64, '0.1.2', tmp_path / 'wheel')
    for name in _REQUIRED_ENV:
        monkeypatch.setenv(name, 'value')
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')
    monkeypatch.delenv('BENCH_N', raising=False)

    def load(_root: Path) -> object:
        return evidence

    def official_checkout(_root: Path, _path: Path) -> Path:
        return official

    monkeypatch.setattr(benchmark, '_preflight_tools', _noop)
    monkeypatch.setattr(benchmark, '_load_artifact_evidence', load)
    monkeypatch.setattr(benchmark, '_official_checkout', official_checkout)

    def install(_evidence: object, install_root: Path) -> Path:
        candidate = install_root / 'site-packages'
        (candidate / 'shotgun_api3').mkdir(parents=True)
        (candidate / 'shotgun_api3' / '__init__.py').touch()
        return candidate

    monkeypatch.setattr(benchmark, '_install_candidate', install)

    def launch(config: Mapping[str, object], _root: Path, _sequence: int) -> dict[str, object]:
        return _controller_worker_result(benchmark, config)

    monkeypatch.setattr(benchmark, '_launch_configured_worker', launch)

    assert benchmark._controller(tmp_path) == 0
    report = tmp_path / 'dist' / 'benchmarks' / 'benchmark-results.md'
    assert report.is_file()
    assert '- Site: `https://example.invalid`' in report.read_text(encoding='ascii')
    assert curated.read_text(encoding='ascii') == 'curated page\n'


def test_summarize_uses_shared_verified_deletions_across_operation_runners() -> None:
    benchmark = _load_benchmark()

    class Control:
        def find_one(self, entity_type: str, filters: list[list[object]]) -> None:
            assert entity_type == 'Shot'
            assert filters[0][:2] == ['id', 'is']

    state = SimpleNamespace(
        clients=[Control()],
        namespace='ns',
        project={'id': 1},
        resources=[('Shot', identity) for identity in range(1, 35)],
        verified_deleted_shot_ids=set(),
    )
    delete_runner = benchmark._Operations(SimpleNamespace(), state)

    assert delete_runner._verify_result('delete', (True, 33)) is True
    assert delete_runner._verify_result('delete', (True, 34)) is True

    summarize_runner = benchmark._Operations(SimpleNamespace(), state)
    filters = summarize_runner._filters()

    assert filters == [
        ['project', 'is', state.project],
        ['code', 'starts_with', 'ns'],
    ]
    assert state.resources == [('Shot', identity) for identity in range(1, 35)]
    assert state.verified_deleted_shot_ids == {33, 34}
    assert summarize_runner._verify_result('summarize', {'summaries': {'id': 32}}) is True
    assert summarize_runner._verify_result('summarize', {'summaries': {'id': 340}}) is False


def test_update_result_pair_arity_is_independent_of_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark()
    operations = benchmark._Operations(SimpleNamespace(), SimpleNamespace())

    monkeypatch.setattr(benchmark, '_BATCH_SIZE', 3)

    assert operations._verify_result('update', ({'description': 'expected'}, 'expected')) is True
    assert operations._verify_result('update', ({'description': 'expected'},)) is False
    assert (
        operations._verify_result('update', ({'description': 'expected'}, 'expected', None))
        is False
    )
    assert operations._verify_result('update', ({'description': 'wrong'}, 'expected')) is False


def test_worker_benchmark_error_exits_nonzero_with_owned_diagnostic(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    config_path = tmp_path / 'worker-8.json'
    monkeypatch.chdir(tmp_path)

    def worker_result(_config_path: Path) -> dict[str, object]:
        raise benchmark._BenchmarkError('benchmark operation failed: create')

    monkeypatch.setattr(benchmark, '_worker_result', worker_result)

    with pytest.raises(SystemExit) as captured:
        benchmark._worker(config_path)

    assert captured.value.code == 1
    assert capsys.readouterr().err == 'benchmark operation failed: create\n'


@pytest.mark.parametrize(
    ('write_error', 'close_error'),
    [
        (KeyboardInterrupt(), OSError('RAW close failure')),
        (OSError('RAW write failure'), _StopBenchmark()),
    ],
    ids=('work-process-control', 'cleanup-process-control'),
)
def test_worker_config_preserves_first_process_control_and_closes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    write_error: BaseException,
    close_error: BaseException,
) -> None:
    benchmark = _load_benchmark()
    process_control = next(
        error for error in (write_error, close_error) if not isinstance(error, Exception)
    )
    events: list[str] = []

    def close(descriptor: int) -> None:
        assert descriptor == 17
        events.append('close')
        raise close_error

    def open_file(_path: Path, _flags: int, _mode: int) -> int:
        events.append('open')
        return 17

    def write(descriptor: int, _value: bytes) -> int:
        assert descriptor == 17
        events.append('write')
        raise write_error

    monkeypatch.setattr(benchmark.os, 'close', close)
    monkeypatch.setattr(benchmark.os, 'open', open_file)
    monkeypatch.setattr(benchmark.os, 'write', write)

    with pytest.raises(type(process_control)) as captured:
        benchmark._write_worker_config(tmp_path / 'worker.json', {})

    assert captured.value is process_control
    assert events == ['open', 'write', 'close']


@pytest.mark.parametrize('failing_operation', ['write', 'close'])
def test_worker_config_sanitizes_ordinary_serialization_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failing_operation: str,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []

    def close(descriptor: int) -> None:
        assert descriptor == 17
        events.append('close')
        if failing_operation == 'close':
            raise OSError('RAW close failure')

    def open_file(_path: Path, _flags: int, _mode: int) -> int:
        events.append('open')
        return 17

    def write(descriptor: int, value: bytes) -> int:
        assert descriptor == 17
        events.append('write')
        if failing_operation == 'write':
            raise OSError('RAW write failure')
        return len(value)

    monkeypatch.setattr(benchmark.os, 'close', close)
    monkeypatch.setattr(benchmark.os, 'open', open_file)
    monkeypatch.setattr(benchmark.os, 'write', write)

    with pytest.raises(benchmark._BenchmarkError, match='worker configuration') as captured:
        benchmark._write_worker_config(tmp_path / 'worker.json', {})

    assert 'RAW' not in str(captured.value)
    assert events == ['open', 'write', 'close']


def test_worker_config_static_boundary_requires_a_dictionary() -> None:
    source = """from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from scripts import benchmark

path = Path('worker.json')
config: dict[str, object] = {}
benchmark._write_worker_config(path, config)

config_mapping: Mapping[str, object] = MappingProxyType(config)
benchmark._write_worker_config(path, config_mapping)
"""
    invalid_line = source.splitlines().index(
        'benchmark._write_worker_config(path, config_mapping)'
    )
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
        probe = Path(directory) / 'worker_config_types.py'
        probe.write_text(source, encoding='utf-8')
        result = run_pyright(probe, cwd=_REPO_ROOT)

    report = json.loads(result.stdout)
    diagnostics = report['generalDiagnostics']
    assert result.returncode == 1, result.stdout
    assert report['summary']['errorCount'] == 1
    assert {item['rule'] for item in diagnostics} == {'reportArgumentType'}
    assert {item['range']['start']['line'] for item in diagnostics} == {invalid_line}


@pytest.mark.parametrize(
    'diagnostic',
    [
        'benchmark operation failed: create',
        'benchmark correctness failed for rss_download',
        'benchmark correctness failed for rss_upload',
        'benchmark cleanup failed',
    ],
)
def test_worker_diagnostic_is_retained_only_for_safe_owned_line(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    diagnostic: str,
) -> None:
    benchmark = _load_benchmark()
    config = tmp_path / 'worker.json'
    env = {name: f'configured-{name.lower()}' for name in _REQUIRED_ENV}

    def launch(stderr: str) -> str:
        def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            stdout = '/private/worker/path\nTraceback: configured-sg_test_key\n'
            return subprocess.CompletedProcess(command, 1, stdout, stderr)

        with pytest.raises(benchmark._BenchmarkError) as captured:
            benchmark._launch_worker(config, tmp_path, env=env, run=run)
        return str(captured.value)

    assert launch(f'{diagnostic}\n') == f'Benchmark worker failed with status 1: {diagnostic}'
    assert launch(f'{diagnostic}\nTraceback: arbitrary stderr\n') == (
        'Benchmark worker failed with status 1'
    )
    assert launch(f'{diagnostic}\N{NO-BREAK SPACE}\n') == 'Benchmark worker failed with status 1'
    assert launch('server Fault: raw response\n') == 'Benchmark worker failed with status 1'
    for unsafe in (
        'Benchmark worker failed with status 1: SECRET-host',
        f'{diagnostic}; warning: SECRET cleanup response',
        f'{diagnostic}; warning: Benchmark worker recovery failed SECRET',
        'benchmark correctness failed for rss_unknown',
    ):
        assert launch(unsafe + '\n') == 'Benchmark worker failed with status 1'

    env['SG_TEST_KEY'] = diagnostic.rsplit(maxsplit=1)[-1]
    assert launch(f'{diagnostic}\n') == 'Benchmark worker failed with status 1'

    env['SG_TEST_KEY'] = 'configured-sg_test_key'
    monkeypatch.setattr(benchmark, '_MAX_WORKER_DIAGNOSTIC_LENGTH', len(diagnostic) - 1)
    assert launch(f'{diagnostic}\n') == 'Benchmark worker failed with status 1'


def test_worker_invalid_output_is_absent_from_complete_exception_chain(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    sentinel = 'SECRET-worker-output{'

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, sentinel, '')

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_worker(tmp_path / 'worker.json', tmp_path, env={}, run=run)

    retained: list[str] = []
    current: BaseException | None = captured.value
    while current is not None:
        retained.extend((str(current), str(getattr(current, 'doc', ''))))
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    assert sentinel not in '\n'.join(retained)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_worker_process_is_isolated_from_checkout_and_pythonpath(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    config = tmp_path / 'worker.json'
    config.write_text('{}', encoding='ascii')
    captured: dict[str, object] = {}

    def run(
        command: list[str],
        **kwargs: Unpack[_WorkerRunOptions],
    ) -> subprocess.CompletedProcess[str]:
        cwd = kwargs['cwd']
        env = kwargs['env']
        captured.update(command=command, cwd=cwd, env=env)
        assert kwargs['capture_output'] is True
        assert kwargs['text'] is True
        assert kwargs['check'] is False
        payload: dict[str, object] = {'client': 'ours', 'operations': {}, 'rss': {}}
        return subprocess.CompletedProcess(command, 0, json.dumps(payload) + '\n', '')

    env = os.environ.copy()
    env['PYTHONPATH'] = '/tainted/source'
    result = benchmark._launch_worker(config, tmp_path, env=env, run=run)

    command = cast('list[str]', captured['command'])
    assert command[:2] == [sys.executable, '-I']
    assert command[-2:] == ['--worker', str(config)]
    assert captured['cwd'] == tmp_path
    assert 'PYTHONPATH' not in cast('dict[str, str]', captured['env'])
    assert result['client'] == 'ours'


@pytest.mark.parametrize(
    'process_control',
    [KeyboardInterrupt(), SystemExit(17), _StopBenchmark()],
)
def test_worker_reraises_exact_process_control(
    monkeypatch: pytest.MonkeyPatch,
    process_control: BaseException,
) -> None:
    benchmark = _load_benchmark()
    events: list[str] = []

    def cleanup() -> None:
        events.append('cleanup')

    def work() -> dict[str, object]:
        raise process_control

    def worker_result(_config_path: Path) -> dict[str, object]:
        return benchmark._run_with_cleanup(work, cleanup)

    monkeypatch.setattr(benchmark, '_worker_result', worker_result)

    with pytest.raises(type(process_control)) as captured:
        benchmark._worker(Path('/unused-worker-config'))

    assert captured.value is process_control
    assert events == ['cleanup']


def test_worker_timeout_is_fixed_for_every_isolated_launch(tmp_path: Path) -> None:
    benchmark = _load_benchmark()
    config = tmp_path / 'worker.json'
    observed: list[int] = []

    def run(
        command: list[str],
        **kwargs: Unpack[_WorkerRunOptions],
    ) -> subprocess.CompletedProcess[str]:
        observed.append(kwargs['timeout'])
        payload: dict[str, object] = {'client': 'ours', 'operations': {}, 'rss': {}}
        return subprocess.CompletedProcess(command, 0, json.dumps(payload) + '\n', '')

    benchmark._launch_worker(config, tmp_path, env={}, run=run)
    benchmark._launch_worker(config, tmp_path, env={}, run=run)

    assert observed == [3600, 3600]


def test_worker_timeout_is_sanitized_without_retaining_exception_details(
    tmp_path: Path,
) -> None:
    benchmark = _load_benchmark()
    config = tmp_path / 'worker-SECRET-path.json'
    timeout = subprocess.TimeoutExpired(
        ['SECRET-command', str(config)],
        3600,
        output='SECRET-output',
        stderr='SECRET-stderr',
    )

    def run(
        _command: list[str],
        **kwargs: Unpack[_WorkerRunOptions],
    ) -> subprocess.CompletedProcess[str]:
        assert kwargs['timeout'] == 3600
        raise timeout

    with pytest.raises(benchmark._BenchmarkError) as captured:
        benchmark._launch_worker(config, tmp_path, env={'SG_TEST_KEY': 'SECRET-key'}, run=run)

    assert str(captured.value) == 'Benchmark worker timed out'
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert all(
        secret not in str(captured.value)
        for secret in (
            'SECRET-command',
            'SECRET-path',
            'SECRET-output',
            'SECRET-stderr',
            'SECRET-key',
        )
    )


def test_workload_budget_has_exact_default_and_hard_ceiling() -> None:
    benchmark = _load_benchmark()

    assert benchmark._workload_budget(2) == {
        'created_entities': 184,
        'upload_bytes': 332032784,
        'workers': 18,
    }
    assert benchmark._workload_budget(20) == {
        'created_entities': 1840,
        'upload_bytes': 3320327840,
        'workers': 162,
    }
