import inspect
import pickle
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

from shotgun_api3.lib.mockgun._async_client import AsyncShotgun as SiteAsyncShotgun
from shotgun_api3.lib.mockgun._sync_client import Shotgun as SiteShotgun


class FakeSiteCase:
    def __init__(self, kind: str, client: SiteShotgun | SiteAsyncShotgun) -> None:
        self.kind = kind
        self.client = client

    async def call(self, method: str, *args: object, **kwargs: object) -> object:
        result = getattr(self.client, method)(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


@dataclass(frozen=True)
class SiteDefinition:
    schema: object
    entities: object
    prefix: str
    now: datetime | None = None
    positional_credentials: bool = False


def site_case(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    definition: SiteDefinition,
) -> FakeSiteCase:
    schema_path = tmp_path / f'{definition.prefix}-schema.pickle'
    entity_path = tmp_path / f'{definition.prefix}-entities.pickle'
    schema_path.write_bytes(pickle.dumps(definition.schema, protocol=2))
    entity_path.write_bytes(pickle.dumps(definition.entities, protocol=2))
    monkeypatch.setattr(SiteShotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(SiteShotgun, '_schema_entity_path', str(entity_path))
    client_type = SiteShotgun if request.param == 'sync' else SiteAsyncShotgun
    client = (
        client_type('http://unit-test.example', 'fixture', 'secret')
        if definition.positional_credentials
        else client_type('http://unit-test.example', script_name='queries', api_key='key')
    )
    if definition.now is not None:
        client.site.set_time(definition.now)
    return FakeSiteCase(str(request.param), client)
