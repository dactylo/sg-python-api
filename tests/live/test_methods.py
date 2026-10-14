from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pytest

import shotgun_api3
from tests.live.test_async import (
    _AsyncLiveResources,
    _finalize_live_test,
    _live_configuration,
    _new_run_namespace,
    _resolve_project,
)
from tests.live.test_roundtrip import _png

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from shotgun_api3 import AsyncShotgun, Shotgun
    from tests.live.conftest import _LiveResources

pytestmark = pytest.mark.live


@dataclass(frozen=True)
class _AsyncCase:
    client: AsyncShotgun
    project: dict[str, object]
    resources: _AsyncLiveResources
    namespace: str


def _assert_export_shape(text: str) -> None:
    assert isinstance(text, str)
    assert text
    lines = text.splitlines()
    assert lines
    assert ',' in lines[0] or '\t' in lines[0]
    assert any(character.isalpha() for character in lines[0])


def _assert_navigation_match(rows: list[dict[str, object]], entity: dict[str, object]) -> None:
    assert any(row.get('ref') == {'type': entity['type'], 'id': entity['id']} for row in rows)


def _assert_ready_image(entity: dict[str, object] | None) -> None:
    assert entity is not None
    image = entity.get('image')
    assert isinstance(image, str)
    assert image.startswith('https://')
    assert '/images/status/transient/' not in image


def _assert_subscriptions(rows: list[dict[str, object]]) -> None:
    for row in rows:
        assert set(row) == {'humanUserId', 'subscription'}
        assert isinstance(row['humanUserId'], str)
        assert row['humanUserId']
        subscription = row['subscription']
        assert (
            subscription is None
            or isinstance(subscription, str)
            or (
                isinstance(subscription, list)
                and all(isinstance(item, str) for item in cast('list[object]', subscription))
            )
        )


def _assert_thread(
    rows: list[dict[str, object]],
    note_id: int,
    reply_id: int,
    *,
    selected_fields: bool,
) -> None:
    assert len(rows) == 2
    note = next(row for row in rows if row.get('type') == 'Note' and row.get('id') == note_id)
    reply = next(row for row in rows if row.get('type') == 'Reply' and row.get('id') == reply_id)
    if selected_fields:
        assert isinstance(note.get('subject'), str)
        assert isinstance(reply.get('content'), str)


def _navigation_arguments(expanded: dict[str, object]) -> tuple[str, str]:
    root = expanded['path']
    assert isinstance(root, str)
    targets = expanded['target_entities']
    assert isinstance(targets, dict)
    presets = cast('dict[str, object]', targets)['additional_filter_presets']
    assert isinstance(presets, list)
    for preset_value in cast('list[object]', presets):
        assert isinstance(preset_value, dict)
        seed_value = cast('dict[str, object]', preset_value).get('seed')
        if not isinstance(seed_value, dict):
            continue
        seed = cast('dict[str, object]', seed_value)
        entity_type, field = seed.get('type'), seed.get('field')
        if isinstance(entity_type, str) and isinstance(field, str):
            return root, f'{entity_type}.{field}'
    pytest.fail('navigation expansion returned no qualified seed')


@pytest.fixture
async def async_method_case(request: pytest.FixtureRequest) -> AsyncGenerator[_AsyncCase]:
    configuration = _live_configuration(request)
    client = shotgun_api3.AsyncShotgun(
        configuration.site,
        script_name=configuration.script_name,
        api_key=configuration.api_key,
        connect=False,
    )
    resources = None
    try:
        project = await _resolve_project(client, request, configuration.project_name)
        namespace = _new_run_namespace()
        resources = _AsyncLiveResources(client, project, namespace)
        yield _AsyncCase(client, project, resources, namespace)
    finally:
        await _finalize_live_test(resources, client, None)


@pytest.fixture
def live_export_page() -> int:
    configured = os.environ.get('SG_TEST_EXPORT_PAGE')
    if configured is None:
        pytest.skip('SG_TEST_EXPORT_PAGE is not configured')
    if not configured.isdecimal() or int(configured) <= 0:
        pytest.fail('SG_TEST_EXPORT_PAGE must be a positive integer', pytrace=False)
    return int(configured)


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_export_page_returns_csv(
    request: pytest.FixtureRequest,
    live_export_page: int,
) -> None:
    configuration = _live_configuration(request)
    async with shotgun_api3.AsyncShotgun(
        configuration.site,
        script_name=configuration.script_name,
        api_key=configuration.api_key,
        connect=False,
    ) as client:
        _assert_export_shape(await client.export_page(live_export_page, 'csv'))


@pytest.mark.anyio
@pytest.mark.async_required
@pytest.mark.parametrize('method', ['nav_search_entity', 'nav_search_string'])
async def test_async_navigation_search_matches_asset(
    async_method_case: _AsyncCase,
    method: str,
) -> None:
    case = async_method_case
    code = case.namespace + '-navigation'
    entity = await case.client.create('Asset', {'project': case.project, 'code': code})
    case.resources.track_created('Asset', entity)
    expanded = await case.client.nav_expand(f'/Project/{case.project["id"]}')
    root, qualified_seed = _navigation_arguments(expanded)
    for seed in (None, qualified_seed):
        rows = (
            await case.client.nav_search_entity(root, entity, seed_entity_field=seed)
            if method == 'nav_search_entity'
            else await case.client.nav_search_string(root, case.namespace, seed_entity_field=seed)
        )
        _assert_navigation_match(rows, entity)


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_note_thread_returns_reply_and_selected_fields(
    async_method_case: _AsyncCase,
) -> None:
    case = async_method_case
    note = await case.client.create(
        'Note', {'project': case.project, 'subject': case.namespace + '-thread'}
    )
    note_id = case.resources.track_created('Note', note)
    reply = await case.client.create(
        'Reply', {'entity': note, 'content': case.namespace + '-reply'}
    )
    reply_id = case.resources.track_created('Reply', reply)
    fields = {'Note': ['subject'], 'Reply': ['content']}
    for selected in (False, True):
        rows = await case.client.note_thread_read(note_id, fields if selected else None)
        _assert_thread(rows, note_id, reply_id, selected_fields=selected)


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_thumbnail_sharing_from_file_and_ready_source(
    async_method_case: _AsyncCase,
    tmp_path: Path,
) -> None:
    case = async_method_case
    image = tmp_path / f'{case.namespace}-shared-thumbnail.png'
    await asyncio.to_thread(image.write_bytes, _png(64, 64))
    entities: list[dict[str, object]] = []
    for index in range(3):
        entity = await case.client.create(
            'Asset', {'project': case.project, 'code': f'{case.namespace}-sharing-{index}'}
        )
        case.resources.track_created('Asset', entity)
        entities.append(entity)

    uploaded = await case.client.share_thumbnail(entities[:2], thumbnail_path=str(image))
    assert uploaded > 0
    for entity in entities[:2]:
        _assert_ready_image(
            await case.client.find_one('Asset', [['id', 'is', entity['id']]], ['image'])
        )

    shared = await case.client.share_thumbnail(entities[2:], source_entity=entities[0])
    assert shared == uploaded
    _assert_ready_image(
        await case.client.find_one('Asset', [['id', 'is', entities[2]['id']]], ['image'])
    )


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_user_subscriptions_have_documented_fields(
    async_method_case: _AsyncCase,
) -> None:
    _assert_subscriptions(await async_method_case.client.user_subscriptions_read())


def test_export_page_returns_csv(live_sg: Shotgun, live_export_page: int) -> None:
    _assert_export_shape(live_sg.export_page(live_export_page, 'csv'))


@pytest.mark.parametrize('method', ['nav_search_entity', 'nav_search_string'])
def test_navigation_search_matches_asset(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_resources: _LiveResources,
    live_namespace: str,
    method: str,
) -> None:
    prefix = live_namespace + '-' + method
    entity = live_resources.create('Asset', {'project': live_project, 'code': prefix})
    expanded = live_sg.nav_expand(f'/Project/{live_project["id"]}')
    root, qualified_seed = _navigation_arguments(expanded)
    for seed in (None, qualified_seed):
        rows = (
            live_sg.nav_search_entity(root, entity, seed_entity_field=seed)
            if method == 'nav_search_entity'
            else live_sg.nav_search_string(root, prefix, seed_entity_field=seed)
        )
        _assert_navigation_match(rows, entity)


def test_note_thread_returns_reply_and_selected_fields(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_resources: _LiveResources,
    live_namespace: str,
) -> None:
    note = live_resources.create(
        'Note', {'project': live_project, 'subject': live_namespace + '-thread'}
    )
    reply = live_resources.create('Reply', {'entity': note, 'content': live_namespace + '-reply'})
    note_id, reply_id = note['id'], reply['id']
    assert isinstance(note_id, int)
    assert isinstance(reply_id, int)
    fields = {'Note': ['subject'], 'Reply': ['content']}
    for selected in (False, True):
        rows = live_sg.note_thread_read(note_id, fields if selected else None)
        _assert_thread(rows, note_id, reply_id, selected_fields=selected)


def test_thumbnail_sharing_from_file_and_ready_source(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_resources: _LiveResources,
    live_namespace: str,
    tmp_path: Path,
) -> None:
    image = tmp_path / f'{live_namespace}-shared-thumbnail.png'
    image.write_bytes(_png(64, 64))
    entities = [
        live_resources.create(
            'Asset', {'project': live_project, 'code': f'{live_namespace}-sharing-{index}'}
        )
        for index in range(3)
    ]

    uploaded = live_sg.share_thumbnail(entities[:2], thumbnail_path=str(image))
    assert uploaded > 0
    for entity in entities[:2]:
        _assert_ready_image(live_sg.find_one('Asset', [['id', 'is', entity['id']]], ['image']))

    shared = live_sg.share_thumbnail(entities[2:], source_entity=entities[0])
    assert shared == uploaded
    _assert_ready_image(live_sg.find_one('Asset', [['id', 'is', entities[2]['id']]], ['image']))


def test_user_subscriptions_have_documented_fields(live_sg: Shotgun) -> None:
    _assert_subscriptions(live_sg.user_subscriptions_read())
