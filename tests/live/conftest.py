from __future__ import annotations

import importlib
import json
import os
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from shotgun_api3 import Shotgun
from shotgun_api3._transport import (
    TransportRequest,
    TransportResponse,
    Urllib3Transport,
)
from tests._live_safety import LiveBudget, LiveStop, LiveStopError
from tests._required_mode import REQUIRED_LIVE_ENV
from tests.live._evidence import failure, location, write_artifact
from tests.live._verification import LiveCase

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator, Mapping
    from http.client import HTTPResponse
    from types import TracebackType

    import urllib3

    from shotgun_api3._async_httpx2 import Httpx2Transport
    from shotgun_api3._async_transport import AsyncStreamingResponse
    from tests.live._session import SessionPlugin

_LIVE_SESSION: dict[str, SessionPlugin] = {}
LIVE_INITIAL_COUNTS: dict[str, int] = {}
LIVE_STATE: dict[str, LiveBudget] = {}
LIVE_EVIDENCE: dict[str, Any] = {}


class _FailureCapture:
    def __init__(self) -> None:
        self.failed = False

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        self.failed = exc is not None
        return isinstance(exc, Exception)


@dataclass(frozen=True)
class _Resource:
    entity_type: str
    entity_id: int


class LiveResources:
    def __init__(
        self,
        client: Shotgun,
        project: Mapping[str, object],
        namespace: str,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._project = project
        self._resources: list[_Resource] = []

    def _delete(self, resource: _Resource, failures: list[str]) -> None:
        capture = _FailureCapture()
        deleted = False
        with capture:
            deleted = self._client.delete(resource.entity_type, resource.entity_id)
        identity = f'{resource.entity_type}:{resource.entity_id}'
        if capture.failed:
            failures.append(f'delete raised for {identity}')
        elif not deleted:
            failures.append(f'delete returned false for {identity}')

    def _is_residual(self, resource: _Resource, failures: list[str]) -> bool:
        capture = _FailureCapture()
        found: dict[str, object] | None = None
        with capture:
            found = self._client.find_one(
                resource.entity_type,
                [['id', 'is', resource.entity_id]],
            )
        if capture.failed:
            failures.append(
                f'residual verification raised for {resource.entity_type}:{resource.entity_id}'
            )
            return False
        return found is not None

    def _namespace_residuals(
        self,
        entity_type: str,
        field_name: str,
        failures: list[str],
        *,
        project_scoped: bool = True,
    ) -> list[_Resource]:
        capture = _FailureCapture()
        found: list[dict[str, object]] = []
        filters: list[list[object]] = [[field_name, 'starts_with', self._namespace]]
        if project_scoped:
            filters.insert(0, ['project', 'is', self._project])
        with capture:
            found = self._client.find(entity_type, filters, ['id'])
        if capture.failed:
            failures.append(f'namespace verification raised for {entity_type}')
            return []
        return [
            _Resource(entity_type, entity_id)
            for entity in found
            if isinstance((entity_id := entity.get('id')), int)
        ]

    def cleanup(self) -> None:
        failures: list[str] = []
        # A committed upload can lose its response before its ID is tracked.
        attachments = self._namespace_residuals('Attachment', 'filename', failures)
        cleanup_order = tuple(dict.fromkeys([*attachments, *reversed(self._resources)]))
        for resource in cleanup_order:
            self._delete(resource, failures)

        residuals = [
            resource for resource in cleanup_order if self._is_residual(resource, failures)
        ]
        for entity_type, field_name, project_scoped in (
            ('Attachment', 'filename', True),
            ('Shot', 'code', True),
            ('Asset', 'code', True),
            ('Note', 'subject', True),
            ('Reply', 'content', False),
        ):
            residuals.extend(
                self._namespace_residuals(
                    entity_type,
                    field_name,
                    failures,
                    project_scoped=project_scoped,
                )
            )

        unique_residuals = tuple(dict.fromkeys(residuals))
        if failures or unique_residuals:
            details: list[str] = []
            if unique_residuals:
                details.append(
                    'residual resources: '
                    + ', '.join(
                        f'{resource.entity_type}:{resource.entity_id}'
                        for resource in unique_residuals
                    )
                )
            if failures:
                details.append('cleanup checks failed: ' + '; '.join(failures))
            raise RuntimeError('live cleanup failed; ' + '; '.join(details))

    def track(self, entity_type: str, entity_id: int) -> None:
        self._resources.append(_Resource(entity_type, entity_id))

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        entity = self._client.create(entity_type, data)
        entity_id = entity.get('id')
        if not isinstance(entity_id, int):
            raise TypeError(f'{entity_type} creation returned no integer id')
        self.track(entity_type, entity_id)
        return entity


@pytest.fixture(scope='session')
def live_sg(request: pytest.FixtureRequest) -> Generator[Shotgun]:
    missing = [name for name in REQUIRED_LIVE_ENV if not os.environ.get(name)]
    if missing:
        if request.config.getoption('require_live'):
            pytest.fail(f'required live configuration missing: {", ".join(missing)}')
        pytest.skip('live-site configuration not present')
    client = Shotgun(
        os.environ['SG_TEST_SITE'],
        script_name=os.environ['SG_TEST_SCRIPT'],
        api_key=os.environ['SG_TEST_KEY'],
        connect=False,
    )
    yield client
    client.close()


@pytest.fixture(scope='session')
def live_project(
    live_sg: Shotgun,
    request: pytest.FixtureRequest,
) -> dict[str, object]:
    name = os.environ['SG_TEST_PROJECT']
    project = live_sg.find_one('Project', [['name', 'is', name]], ['name'])
    if project is None:
        message = f'test project {name!r} missing; run scripts/bootstrap_test_site.py'
        if request.config.getoption('require_live'):
            pytest.fail(message)
        pytest.skip(message)
    return project


def new_run_namespace() -> str:
    return f'sg-python-api-live-{uuid.uuid4().hex[:12]}'


@pytest.fixture(scope='session')
def live_namespace() -> str:
    return new_run_namespace()


@pytest.fixture
def live_resources(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
) -> Generator[LiveResources]:
    resources = LiveResources(live_sg, live_project, live_namespace)
    yield resources
    resources.cleanup()


def pytest_sessionfinish(session: pytest.Session) -> None:
    budget = LIVE_STATE.get('budget')
    if budget is None:
        return
    LIVE_EVIDENCE.update(
        finished_utc=datetime.now(tz=UTC).isoformat(),
        counts={key: value - LIVE_INITIAL_COUNTS[key] for key, value in budget.counts.items()},
        total_counts=budget.counts,
        requests_by_test=budget.requests_by_test,
        stop_reason=budget.stopped,
    )
    evidence_dir_value = session.config.getoption('live_evidence_dir')
    if evidence_dir_value:
        evidence_dir = Path(evidence_dir_value)
        mode = 'extended' if session.config.getoption('live_extended') else 'default'
        collected = [item.nodeid for item in session.items]
        write_artifact(evidence_dir / f'{mode}.json', LIVE_EVIDENCE, collected)
        write_artifact(evidence_dir / 'budget.json', budget.counts, collected)


def set_session_plugin(plugin: SessionPlugin) -> None:
    _LIVE_SESSION['session'] = plugin


def upload_size(request: TransportRequest) -> int:
    if request.method != 'PUT':
        return 0
    for name, value in request.headers.items():
        if name.lower() == 'content-length':
            return int(value)
    return len(request.body) if isinstance(request.body, bytes) else 0


def patch_live_transports(budget: LiveBudget, patch: pytest.MonkeyPatch) -> None:
    async_transport = cast(
        'type[Httpx2Transport]',
        importlib.import_module('shotgun_api3._async_httpx2').Httpx2Transport,
    )
    sync_start = Urllib3Transport._start_request
    async_once = async_transport._send_once
    async_stream = async_transport.send_streaming
    urllib_open = urllib.request.OpenerDirector.open

    def guarded_sync(
        transport: Urllib3Transport, outgoing: TransportRequest, *, preload_content: bool
    ) -> urllib3.response.HTTPResponse:
        budget.before(body=outgoing.body, upload_bytes=upload_size(outgoing))
        response = sync_start(transport, outgoing, preload_content=preload_content)
        try:
            budget.response(
                response.status,
                response.headers.get('Retry-After', ''),
            )
        except LiveStop:
            response.close()
            raise
        return response

    async def guarded_async(
        transport: Httpx2Transport, outgoing: TransportRequest
    ) -> TransportResponse:
        budget.before(body=outgoing.body, upload_bytes=upload_size(outgoing))
        response = await async_once(transport, outgoing)
        budget.response(response.status, response.headers.get('Retry-After', ''))
        return response

    async def guarded_stream(
        transport: Httpx2Transport, outgoing: TransportRequest
    ) -> AsyncStreamingResponse:
        budget.before(body=outgoing.body, upload_bytes=upload_size(outgoing))
        response = await async_stream(transport, outgoing)
        try:
            budget.response(response.status, response.headers.get('Retry-After', ''))
        except LiveStop:
            await response.close()
            raise
        return response

    def guarded_urllib(
        opener: urllib.request.OpenerDirector, *args: object, **kwargs: object
    ) -> HTTPResponse:
        budget.before()
        try:
            response = cast('Callable[..., HTTPResponse]', urllib_open)(opener, *args, **kwargs)
        except urllib.error.HTTPError as error:
            budget.response(error.code, error.headers.get('Retry-After', ''))
            raise
        try:
            budget.response(response.status, response.headers.get('Retry-After', ''))
        except LiveStop:
            response.close()
            raise
        return response

    patch.setattr(Urllib3Transport, '_start_request', guarded_sync)
    patch.setattr(async_transport, '_send_once', guarded_async)
    patch.setattr(async_transport, 'send_streaming', guarded_stream)
    patch.setattr(urllib.request.OpenerDirector, 'open', guarded_urllib)


@pytest.fixture(scope='session')
def live_budget(request: pytest.FixtureRequest) -> Generator[LiveBudget]:
    session_plugin = _LIVE_SESSION.get('session')
    if session_plugin is not None:
        yield session_plugin.budget
        return
    evidence_dir_value = request.config.getoption('live_evidence_dir')
    evidence_dir = Path(evidence_dir_value) if evidence_dir_value else None
    counts = None
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        budget_file = evidence_dir / 'budget.json'
        if budget_file.exists():
            counts = json.loads(budget_file.read_text(encoding='ascii'))
    budget = LiveBudget(counts)
    LIVE_STATE['budget'] = budget
    LIVE_INITIAL_COUNTS.clear()
    LIVE_INITIAL_COUNTS.update(budget.counts)
    LIVE_EVIDENCE.update(started_utc=datetime.now(tz=UTC).isoformat(), tests={})
    with pytest.MonkeyPatch.context() as patch:
        patch_live_transports(budget, patch)
        yield budget


@pytest.fixture(autouse=True)
def live_safety(request: pytest.FixtureRequest) -> None:
    node = request._pyfuncitem
    if node.get_closest_marker('live') is None:
        return
    if not all(os.environ.get(name) for name in REQUIRED_LIVE_ENV):
        return
    budget = cast('LiveBudget', request.getfixturevalue('live_budget'))
    if budget.stopped:
        pytest.exit(budget.stopped, returncode=2)
    budget.current_test = node.nodeid


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, Any, None]:
    outcome = yield
    report = outcome.get_result()
    budget = LIVE_STATE.get('budget')
    if budget is None or item.get_closest_marker('live') is None:
        return
    LIVE_EVIDENCE['tests'].setdefault(item.nodeid, {})[report.when] = report.outcome
    if call.excinfo is not None:
        locations = [
            value
            for entry in call.excinfo.traceback
            if (value := location(Path(str(entry.path)), entry.lineno + 1)) is not None
        ]
        LIVE_EVIDENCE['tests'][item.nodeid].setdefault('failures', {})[report.when] = failure(
            call.excinfo.value, locations
        )
    if report.when == 'teardown' and report.failed and not budget.stopped:
        budget.stopped = 'live cleanup or restoration failed'


@pytest.fixture(params=['sync', 'async'])
async def api_case(request: pytest.FixtureRequest) -> AsyncGenerator[LiveCase]:
    session_plugin = _LIVE_SESSION.get('session')
    if session_plugin is not None:
        try:
            case = await session_plugin.make_case(str(request.param))
        except LiveStop as stop:
            raise stop if isinstance(stop, Exception) else LiveStopError(str(stop)) from None
        try:
            yield case
        finally:
            try:
                await case.cleanup()
            finally:
                await case.call('close')
        return
    missing = [name for name in REQUIRED_LIVE_ENV if not os.environ.get(name)]
    if missing:
        if request.config.getoption('require_live'):
            pytest.fail('required live configuration missing', pytrace=False)
        pytest.skip('live-site configuration not present')
    case = LiveCase(
        request.param, {}, new_run_namespace(), script_name=os.environ['SG_TEST_SCRIPT']
    )
    try:
        project = await case.call(
            'find_one', 'Project', [['name', 'is', os.environ['SG_TEST_PROJECT']]], ['id']
        )
        if project is None:
            pytest.fail('configured test project was not found', pytrace=False)
        assert isinstance(project, dict)
        case.project = {'type': 'Project', 'id': cast('dict[str, object]', project)['id']}
        yield case
    finally:
        try:
            if case.project:
                await case.cleanup()
        finally:
            await case.call('close')
