import os
import uuid
from collections.abc import Generator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType

import pytest

from shotgun_api3 import Shotgun
from tests._required_mode import REQUIRED_LIVE_ENV


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


class _LiveResources:
    def __init__(
        self,
        client: Shotgun,
        project: Mapping[str, object],
        namespace: str,
    ) -> None:
        self.client = client
        self.namespace = namespace
        self.project = project
        self._resources: list[_Resource] = []

    def _delete(self, resource: _Resource, failures: list[str]) -> None:
        capture = _FailureCapture()
        deleted = False
        with capture:
            deleted = self.client.delete(resource.entity_type, resource.entity_id)
        identity = f'{resource.entity_type}:{resource.entity_id}'
        if capture.failed:
            failures.append(f'delete raised for {identity}')
        elif not deleted:
            failures.append(f'delete returned false for {identity}')

    def _is_residual(self, resource: _Resource, failures: list[str]) -> bool:
        capture = _FailureCapture()
        found: dict[str, object] | None = None
        with capture:
            found = self.client.find_one(
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
    ) -> list[_Resource]:
        capture = _FailureCapture()
        found: list[dict[str, object]] = []
        with capture:
            found = self.client.find(
                entity_type,
                [
                    ['project', 'is', self.project],
                    [field_name, 'starts_with', self.namespace],
                ],
                ['id'],
            )
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
        for entity_type, field_name in (
            ('Attachment', 'filename'),
            ('Shot', 'code'),
            ('Note', 'subject'),
        ):
            residuals.extend(self._namespace_residuals(entity_type, field_name, failures))

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
        entity = self.client.create(entity_type, data)
        entity_id = entity.get('id')
        if not isinstance(entity_id, int):
            raise TypeError(f'{entity_type} creation returned no integer id')
        self.track(entity_type, entity_id)
        return entity


def _new_run_namespace() -> str:
    timestamp = datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%S%fZ')
    return f'sg-python-api-live-{timestamp}-{uuid.uuid4().hex}'


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


@pytest.fixture(scope='session')
def live_namespace() -> str:
    return _new_run_namespace()


@pytest.fixture(scope='session')
def live_resources(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
) -> Generator[_LiveResources]:
    resources = _LiveResources(live_sg, live_project, live_namespace)
    yield resources
    resources.cleanup()
