import json
import os
import subprocess
import sys
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

import shotgun_api3
from shotgun_api3 import AsyncShotgun, Fault, Shotgun
from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._async_client import AsyncShotgun as SiteAsyncShotgun
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._server import FakeServer
from shotgun_api3.lib.mockgun._sync_client import Shotgun as SiteShotgun
from shotgun_api3.lib.mockgun._transport import InMemoryTransport
from tests._threads import run_concurrently
from tests.lib._requests import request

from .conftest import SCHEMA, SCHEMA_ENTITY, FakeSiteCase, raw_call, raw_request

if TYPE_CHECKING:
    from collections.abc import Callable


def test_async_client_needs_no_httpx2(tmp_path: Path) -> None:
    script = textwrap.dedent("""
        import asyncio
        import importlib.abc
        import pickle
        import sys
        from pathlib import Path

        package_root = Path(sys.argv[2])
        sys.path.insert(0, str(package_root))

        class BlockHttpx2(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == 'httpx2' or fullname.startswith('httpx2.'):
                    raise ImportError('httpx2 import attempted')
                return None

        sys.meta_path.insert(0, BlockHttpx2())
        import shotgun_api3
        from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
        from shotgun_api3.lib.mockgun._sync_client import Shotgun
        assert Path(shotgun_api3.__file__).resolve().is_relative_to(package_root)
        path = Path(sys.argv[1]) / 'schema.pickle'
        path.write_bytes(pickle.dumps({}))
        Shotgun.set_schema_paths(str(path), str(path))

        async def main():
            client = AsyncShotgun('http://unit-test.example', 'script', 'key')
            assert (await client.info())['version'] == [7, 10, 0]
            await client.close()

        asyncio.run(main())
        assert not any(name == 'httpx2' or name.startswith('httpx2.') for name in sys.modules)
    """)
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            script,
            str(tmp_path),
            str(Path(shotgun_api3.__file__).resolve().parents[1]),
        ],
        env={key: value for key, value in os.environ.items() if not key.startswith('SG_TEST_')},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.anyio
async def test_async_subclasses_inherit_paths_until_they_override(fake_site: FakeSiteCase) -> None:
    del fake_site

    class Parent(SiteAsyncShotgun):
        pass

    class Child(Parent):
        pass

    class Sibling(SiteAsyncShotgun):
        pass

    initial = SiteShotgun.get_schema_paths()
    assert await Parent.get_schema_paths() == initial
    await Parent.set_schema_paths('parent-fields', 'parent-entities')
    assert await Child.get_schema_paths() == ('parent-fields', 'parent-entities')
    assert await Sibling.get_schema_paths() == initial
    await Child.set_schema_paths('child-fields', 'child-entities')
    assert await Child.get_schema_paths() == ('child-fields', 'child-entities')
    assert await Parent.get_schema_paths() == ('parent-fields', 'parent-entities')
    assert await SiteAsyncShotgun.get_schema_paths() == initial


@pytest.mark.anyio
async def test_clock_freeze_normalize_and_reset(fake_site: FakeSiteCase) -> None:
    site = fake_site.client.site
    when = datetime(2025, 1, 2, 3, 4, 5, 1234, tzinfo=UTC)
    site.set_time(when)
    assert site.now() == when.replace(microsecond=0)
    with pytest.raises(ValueError, match=r'naive|aware'):
        site.set_time(when.replace(tzinfo=None))
    site.set_time(None)
    assert abs(site.now() - datetime.now(UTC)) < timedelta(seconds=5)
    assert site.now().microsecond == 0


@pytest.mark.anyio
async def test_close_allows_a_new_transport(fake_site: FakeSiteCase) -> None:
    await fake_site.call('info')
    await fake_site.call('close')
    assert cast('dict[str, object]', await fake_site.call('info'))['version'] == [7, 10, 0]


def test_constructor_binding_precedes_missing_schema() -> None:
    class NoSchema(SiteShotgun):
        _schema_path = None
        _schema_entity_path = None

    construct = cast('Callable[..., NoSchema]', NoSchema)
    with pytest.raises(TypeError):
        construct('http://unit-test.example', unknown=True)
    with pytest.raises(MockgunError, match='schema paths are not configured'):
        construct('http://unit-test.example', 'script', 'key')


def test_first_successful_rpc_binds_file_routes_to_its_host() -> None:
    server = FakeServer(SiteSchema({}, {}))
    assert server.site_host is None
    fault = raw_call(server, 'schema_field_read', {'type': 'Missing'}, auth={})
    assert fault['exception'] is True
    assert server.site_host is None
    with pytest.raises(MockgunError, match=r'GET /file_serve/attachment/1'):
        raw_request(server, 'GET', 'http://unit-test.example/file_serve/attachment/1')
    assert server.site_host is None

    assert raw_call(server, 'info', {}, auth={})['version'] == [7, 10, 0]
    assert server.site_host == 'unit-test.example'
    other = raw_request(
        server,
        'POST',
        'http://other.example/api3/json',
        body=json.dumps({'method_name': 'info', 'params': [{}]}).encode(),
    )
    assert json.loads(other.content)['version'] == [7, 10, 0]
    assert server.site_host == 'unit-test.example'
    with pytest.raises(MockgunError, match=r'GET /file_serve/attachment/1'):
        raw_request(server, 'GET', 'http://other.example/file_serve/attachment/1')


@pytest.mark.anyio
async def test_metadata_refresh_and_detachment(fake_site: FakeSiteCase) -> None:
    await fake_site.call('info')
    client = fake_site.client
    client.site.update_server_metadata({'api_max_entities_per_page': 2})
    assert client.server_info['api_max_entities_per_page'] == 500
    metadata = client.site.server_metadata
    cast('list[int]', metadata['version'])[0] = 99
    await fake_site.call('info')
    assert client.server_info['api_max_entities_per_page'] == 2
    assert client.server_info['version'] == [7, 10, 0]


@pytest.mark.anyio
async def test_concurrent_metadata_reads_and_updates(fake_site: FakeSiteCase) -> None:
    site = fake_site.client.site

    def read_metadata() -> None:
        for _ in range(2_000):
            metadata = site.server_metadata
            assert metadata['version'] == [7, 10, 0]

    def write_metadata() -> None:
        for index in range(2_000):
            site.update_server_metadata({f'race_{index}': index})

    run_concurrently([read_metadata, write_metadata], timeout=30)
    assert site.server_metadata['race_1999'] == 1999


@pytest.mark.anyio
@pytest.mark.usefixtures('block_network')
async def test_offline_construction_and_reads(fake_site: FakeSiteCase) -> None:
    assert await fake_site.call('info')
    assert await fake_site.call('schema_read') == SCHEMA
    created = cast(
        'dict[str, object]', await fake_site.call('create', 'Shot', {'code': 'offline'})
    )
    assert await fake_site.call('find_one', 'Shot', [], ['code']) == created
    assert await fake_site.call('update', 'Shot', created['id'], {'code': 'updated'}) == {
        'type': 'Shot',
        'id': created['id'],
        'code': 'updated',
    }


@pytest.mark.anyio
async def test_readiness_and_real_client_inheritance(fake_site: FakeSiteCase) -> None:
    client = fake_site.client
    if fake_site.kind == 'async':
        await fake_site.call('info')
    assert isinstance(client, Shotgun if fake_site.kind == 'sync' else AsyncShotgun)
    assert client.server_info['version'] == [7, 10, 0]
    assert client.server_caps.version == (7, 10, 0)
    assert client.finds == 0


@pytest.mark.anyio
async def test_schema_field_selection(fake_site: FakeSiteCase) -> None:
    project_schema = cast('dict[str, object]', SCHEMA['Project'])
    assert await fake_site.call('schema_field_read', 'Project') == project_schema
    value = await fake_site.call(
        'schema_field_read', 'Project', 'name', project_entity={'type': 'Project', 'id': 99}
    )
    assert value == {'name': project_schema['name']}
    cast('dict[str, object]', cast('dict[str, object]', value)['name']).clear()
    assert await fake_site.call('schema_field_read', 'Project', 'name') == project_schema
    with pytest.raises(Fault, match=r'Project\.unknown'):
        await fake_site.call('schema_field_read', 'Project', 'unknown')


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('method', 'expected'), [('schema_read', SCHEMA), ('schema_entity_read', SCHEMA_ENTITY)]
)
async def test_schema_reads_are_detached(
    fake_site: FakeSiteCase, method: str, expected: dict[str, object]
) -> None:
    project = {'type': 'Project', 'id': 99}
    if method == 'schema_entity_read':
        with pytest.raises(MockgunError, match='schema_entity_read'):
            await fake_site.call(method, project_entity=project)
        value = await fake_site.call(method)
    else:
        value = await fake_site.call(method, project_entity=project)
    assert value == expected
    cast('dict[str, object]', cast('dict[str, object]', value)['Project']).clear()
    assert await fake_site.call(method) == expected


@pytest.mark.anyio
async def test_schema_rejection_uses_client_fault(fake_site: FakeSiteCase) -> None:
    with pytest.raises(Fault, match='Missing is not a valid entity'):
        await fake_site.call('schema_field_read', 'Missing')


@pytest.mark.parametrize(
    ('method', 'path', 'body'),
    [('GET', '/api3/json', None), ('POST', '/storage', b'{}'), ('POST', '/api3/json', b'invalid')],
)
def test_transport_rejects_unsupported_requests(
    method: str, path: str, body: bytes | None
) -> None:
    server = FakeServer(SiteSchema({}, {}))
    with pytest.raises(MockgunError) as raised:
        InMemoryTransport(server).send(request(method, path, body))
    assert method in str(raised.value)
    assert path in str(raised.value)


@pytest.mark.anyio
async def test_unsupported_rpc_uses_mockgun_error_over_http(fake_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match='mockgun_unknown_rpc'):
        raw_call(fake_site.client._server, 'mockgun_unknown_rpc', {})
