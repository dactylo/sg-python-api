from __future__ import annotations

import pickle
import uuid
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3.lib.mockgun._async_client import AsyncShotgun as SiteAsyncShotgun
from shotgun_api3.lib.mockgun._sync_client import Shotgun as SiteShotgun
from tests._parity import (
    EXTENDED_SCENARIOS,
    SCENARIOS,
    MockgunCase,
    Scenario,
    parity_schema,
    parity_schema_entity,
    status_and_list_percentages,
)

pytest_plugins = ('tests.lib._parity_fixture',)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.anyio
@pytest.mark.usefixtures('block_network')
@pytest.mark.parametrize(
    'scenario', [*SCENARIOS, *EXTENDED_SCENARIOS], ids=lambda scenario: scenario.__name__
)
async def test_shared_scenario(scenario: Scenario, parity_case: MockgunCase) -> None:
    parity_case.namespace = f'sg-python-api-live-{uuid.uuid4().hex[:12]}'
    await scenario(parity_case)


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_status_percentage_parity_uses_shot_schema_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    schema = parity_schema()
    shot = cast('dict[str, object]', schema['Shot'])
    status = cast('dict[str, object]', shot['sg_status_list'])
    properties = cast('dict[str, object]', status['properties'])
    properties['valid_values'] = {'value': ['todo', 'done']}
    schema_path = tmp_path / 'parity-schema.pickle'
    entity_path = tmp_path / 'parity-entities.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=2))
    entity_path.write_bytes(pickle.dumps(parity_schema_entity(), protocol=2))
    monkeypatch.setattr(SiteShotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(SiteShotgun, '_schema_entity_path', str(entity_path))
    monkeypatch.setattr(SiteAsyncShotgun, '_schema_paths', None)
    case = MockgunCase(kind, f'status-percentages-{kind}')
    await case.initialize()
    try:
        await status_and_list_percentages(case)
    finally:
        await case.call('close')
