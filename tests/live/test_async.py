from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest

import shotgun_api3
from tests.live._async_resources import (
    AsyncLiveResources,
    FailureCapture,
    exercise_roundtrip,
    finalize_live_test,
    live_configuration,
    new_run_namespace,
    resolve_project,
)
from tests.live._images import png

if TYPE_CHECKING:
    from pathlib import Path

    from shotgun_api3 import AsyncShotgun

pytestmark = [pytest.mark.async_required, pytest.mark.live]


async def _assert_readiness(client: AsyncShotgun) -> None:
    info = await client.info()
    version = info.get('version')
    assert isinstance(version, list)
    version = cast('list[object]', version)
    assert len(version) >= 3
    assert all(isinstance(part, int) for part in version[:3])
    major, minor, patch = version[:3]
    assert isinstance(major, int)
    assert isinstance(minor, int)
    assert isinstance(patch, int)
    assert (major, minor, patch) >= (7, 10, 0)
    assert client.server_caps.version >= (7, 10, 0)


@pytest.mark.anyio
async def test_async_live_roundtrip(request: pytest.FixtureRequest, tmp_path: Path) -> None:
    configuration = live_configuration(request)
    client: AsyncShotgun | None = None
    resources: AsyncLiveResources | None = None
    operation_capture = FailureCapture(suppress_base_exceptions=True)
    try:
        with operation_capture:
            client = shotgun_api3.AsyncShotgun(
                configuration.site,
                script_name=configuration.script_name,
                api_key=configuration.api_key,
                connect=False,
            )
            await _assert_readiness(client)
            project = await resolve_project(client, request, configuration.project_name)
            namespace = new_run_namespace()
            resources = AsyncLiveResources(client, project, namespace)
            await exercise_roundtrip(client, project, resources, namespace, tmp_path)
    finally:
        await finalize_live_test(resources, client, operation_capture.exception)


@pytest.mark.anyio
async def test_async_thumbnail_paths_upload_on_create_and_update(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    configuration = live_configuration(request)
    client: AsyncShotgun | None = None
    resources: AsyncLiveResources | None = None
    operation_capture = FailureCapture(suppress_base_exceptions=True)
    try:
        with operation_capture:
            client = shotgun_api3.AsyncShotgun(
                configuration.site,
                script_name=configuration.script_name,
                api_key=configuration.api_key,
                connect=False,
            )
            project = await resolve_project(client, request, configuration.project_name)
            namespace = new_run_namespace()
            resources = AsyncLiveResources(client, project, namespace)
            image = tmp_path / f'{namespace}-thumbnail.png'
            await asyncio.to_thread(image.write_bytes, png(64, 64))
            strip = tmp_path / f'{namespace}-filmstrip.png'
            await asyncio.to_thread(strip.write_bytes, png(480, 135))

            shot = await client.create(
                'Shot',
                {'code': f'{namespace}-thumbnail', 'project': project, 'image': str(image)},
            )
            shot_id = resources.track_created('Shot', shot)
            assert isinstance(shot['image'], str)
            assert shot['image'].startswith('https://')

            updated = await client.update('Shot', shot_id, {'filmstrip_image': str(strip)})
            assert set(updated) == {'type', 'id', 'filmstrip_image'}
            assert isinstance(updated['filmstrip_image'], str)
            assert updated['filmstrip_image'].startswith('https://')

            cleared = await client.update('Shot', shot_id, {'image': None})
            assert cleared['image'] is None
    finally:
        await finalize_live_test(resources, client, operation_capture.exception)
