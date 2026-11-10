from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from tests._required_mode import REQUIRED_LIVE_ENV

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from pathlib import Path
    from types import TracebackType

    from shotgun_api3 import AsyncShotgun


class FailureCapture:
    def __init__(self, *, suppress_base_exceptions: bool = False) -> None:
        self.exception: BaseException | None = None
        self._suppress_base_exceptions = suppress_base_exceptions

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        self.exception = exc
        return exc is not None and (self._suppress_base_exceptions or isinstance(exc, Exception))


@dataclass(frozen=True)
class _LiveConfiguration:
    api_key: str
    project_name: str
    script_name: str
    site: str


@dataclass(frozen=True)
class _Resource:
    entity_type: str
    entity_id: int


class AsyncLiveResources:
    def __init__(
        self,
        client: AsyncShotgun,
        project: dict[str, object],
        namespace: str,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._project = project
        self._resources: list[_Resource] = []

    async def _delete(self, resource: _Resource, failures: list[str]) -> None:
        identity = f'{resource.entity_type}:{resource.entity_id}'
        capture = FailureCapture()
        deleted = False
        with capture:
            deleted = await self._client.delete(resource.entity_type, resource.entity_id)
        if capture.exception is not None:
            failures.append(f'delete raised for {identity}')
        elif not deleted:
            failures.append(f'delete returned false for {identity}')

    async def _is_residual(self, resource: _Resource, failures: list[str]) -> bool:
        identity = f'{resource.entity_type}:{resource.entity_id}'
        capture = FailureCapture()
        found: dict[str, object] | None = None
        with capture:
            found = await self._client.find_one(
                resource.entity_type,
                [['id', 'is', resource.entity_id]],
            )
        if capture.exception is not None:
            failures.append(f'residual verification raised for {identity}')
            return False
        return found is not None

    async def _namespace_residuals(
        self,
        entity_type: str,
        field_name: str,
        *,
        project_scoped: bool,
        failures: list[str],
    ) -> list[_Resource]:
        filters: list[list[object]] = [[field_name, 'starts_with', self._namespace]]
        if project_scoped:
            filters.insert(0, ['project', 'is', self._project])
        capture = FailureCapture()
        found: list[dict[str, object]] = []
        with capture:
            found = await self._client.find(entity_type, filters, ['id'])
        if capture.exception is not None:
            failures.append(f'namespace verification raised for {entity_type}')
            return []
        return [
            _Resource(entity_type, entity_id)
            for entity in found
            if isinstance((entity_id := entity.get('id')), int) and not isinstance(entity_id, bool)
        ]

    async def _residuals(
        self,
        cleanup_order: tuple[_Resource, ...],
        failures: list[str],
    ) -> list[_Resource]:
        residuals = [
            resource for resource in cleanup_order if await self._is_residual(resource, failures)
        ]
        residuals.extend(
            await self._namespace_residuals(
                'Attachment',
                'filename',
                project_scoped=True,
                failures=failures,
            )
        )
        residuals.extend(
            await self._namespace_residuals(
                'Shot',
                'code',
                project_scoped=True,
                failures=failures,
            )
        )
        for entity_type, field_name, project_scoped in (
            ('Asset', 'code', True),
            ('Note', 'subject', True),
            ('Reply', 'content', False),
        ):
            residuals.extend(
                await self._namespace_residuals(
                    entity_type,
                    field_name,
                    project_scoped=project_scoped,
                    failures=failures,
                )
            )
        return residuals

    async def cleanup(self) -> None:
        failures: list[str] = []
        cleanup_order = tuple(reversed(self._resources))
        for resource in cleanup_order:
            await self._delete(resource, failures)

        recovery_order = sorted(
            dict.fromkeys(await self._residuals(cleanup_order, failures)),
            key=lambda resource: resource.entity_type != 'Attachment',
        )
        for resource in recovery_order:
            await self._delete(resource, failures)

        unique_residuals = tuple(dict.fromkeys(await self._residuals(cleanup_order, failures)))
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
            raise RuntimeError('async live cleanup failed; ' + '; '.join(details))

    def track(self, entity_type: str, entity_id: object) -> int:
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            raise TypeError(f'{entity_type} creation returned no integer id')
        self._resources.append(_Resource(entity_type, entity_id))
        return entity_id

    def track_created(self, entity_type: str, entity: dict[str, object]) -> int:
        return self.track(entity_type, entity.get('id'))


async def _drain_finalizer(
    operation: Coroutine[object, object, None],
    cancellations: list[asyncio.CancelledError],
) -> BaseException | None:
    task = asyncio.create_task(operation)
    while not task.done():
        capture = FailureCapture(suppress_base_exceptions=True)
        with capture:
            await asyncio.wait({task})
        failure = capture.exception
        if isinstance(failure, asyncio.CancelledError):
            if task.cancelled():
                break
            if not cancellations:
                cancellations.append(failure)
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
            continue
        if failure is not None:
            return failure

    result_capture = FailureCapture(suppress_base_exceptions=True)
    with result_capture:
        task.result()
    return result_capture.exception


def _raise_failures(failures: list[BaseException]) -> None:
    if not failures:
        return
    if len(failures) == 1:
        raise failures[0]
    raise BaseExceptionGroup('async live operation or finalization failed', failures)


async def exercise_roundtrip(
    client: AsyncShotgun,
    project: dict[str, object],
    resources: AsyncLiveResources,
    namespace: str,
    tmp_path: Path,
) -> None:
    code = f'{namespace}-roundtrip'
    shot = await client.create('Shot', {'code': code, 'project': project})
    shot_id = resources.track_created('Shot', shot)

    fetched = await client.find_one(
        'Shot',
        [['id', 'is', shot_id]],
        ['code', 'description'],
    )
    assert fetched is not None
    assert fetched['code'] == code

    description = f'{namespace}-updated'
    updated = await client.update('Shot', shot_id, {'description': description})
    assert updated['description'] == description

    payload = b'sg-python-api async tier-2 live payload\x00\xff'
    source = tmp_path / f'{namespace}-payload.bin'
    destination = tmp_path / 'async-tier-2-download.bin'
    await asyncio.to_thread(source.write_bytes, payload)
    attachment_id = await client.upload(
        'Shot',
        shot_id,
        str(source),
        display_name=f'{namespace}-payload.bin',
    )
    resources.track('Attachment', attachment_id)

    downloaded = await client.download_attachment(
        {'type': 'Attachment', 'id': attachment_id},
        file_path=str(destination),
    )
    assert downloaded == str(destination)
    assert await asyncio.to_thread(destination.read_bytes) == payload


async def finalize_live_test(
    resources: AsyncLiveResources | None,
    client: AsyncShotgun | None,
    operation_failure: BaseException | None,
) -> None:
    cancellations = (
        [operation_failure] if isinstance(operation_failure, asyncio.CancelledError) else []
    )
    cleanup_failure = (
        await _drain_finalizer(resources.cleanup(), cancellations)
        if resources is not None
        else None
    )
    close_failure = (
        await _drain_finalizer(client.close(), cancellations) if client is not None else None
    )
    failures = [operation_failure] if operation_failure is not None else []
    if cancellations and cancellations[0] is not operation_failure:
        failures.append(cancellations[0])
    if cleanup_failure is not None:
        failures.append(cleanup_failure)
    if close_failure is not None:
        failures.append(close_failure)
    _raise_failures(failures)


def live_configuration(request: pytest.FixtureRequest) -> _LiveConfiguration:
    missing = [name for name in REQUIRED_LIVE_ENV if not os.environ.get(name)]
    if missing:
        if request.config.getoption('require_live'):
            pytest.fail(f'required live configuration missing: {", ".join(missing)}')
        pytest.skip('live-site configuration not present: ' + ', '.join(missing))
    return _LiveConfiguration(
        api_key=os.environ['SG_TEST_KEY'],
        project_name=os.environ['SG_TEST_PROJECT'],
        script_name=os.environ['SG_TEST_SCRIPT'],
        site=os.environ['SG_TEST_SITE'],
    )


def new_run_namespace() -> str:
    timestamp = datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%S%fZ')
    return f'sg-python-api-live-async-{timestamp}-{uuid.uuid4().hex}'


async def resolve_project(
    client: AsyncShotgun,
    request: pytest.FixtureRequest,
    project_name: str,
) -> dict[str, object]:
    project = await client.find_one(
        'Project',
        [['name', 'is', project_name]],
        ['name'],
    )
    if project is not None:
        return project
    message = f'test project {project_name!r} missing; run scripts/bootstrap_test_site.py'
    if request.config.getoption('require_live'):
        pytest.fail(message)
    pytest.skip(message)
