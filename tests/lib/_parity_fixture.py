import pickle
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
from shotgun_api3.lib.mockgun._sync_client import Shotgun
from tests._parity import MockgunCase, parity_schema, parity_schema_entity


@pytest.fixture(params=['sync', 'async'])
async def parity_case(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[MockgunCase]:
    schema_path = tmp_path / 'parity-schema.pickle'
    entity_path = tmp_path / 'parity-entities.pickle'
    schema_path.write_bytes(pickle.dumps(parity_schema(), protocol=2))
    entity_path.write_bytes(pickle.dumps(parity_schema_entity(), protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    monkeypatch.setattr(AsyncShotgun, '_schema_paths', None)
    case = MockgunCase(str(request.param), f'parity-{request.param}')
    await case.initialize()
    try:
        yield case
    finally:
        await case.call('close')
