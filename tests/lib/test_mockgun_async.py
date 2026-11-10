import asyncio
from pathlib import Path

import pytest

from shotgun_api3 import ShotgunError
from shotgun_api3 import _config as config_module
from shotgun_api3._async_client import AsyncShotgun as RealAsyncShotgun
from shotgun_api3._config import Config
from shotgun_api3.lib import mockgun
from tests._pyright import run_pyright


@pytest.mark.parametrize('ca_certs', [None, '', '/not/read/argument.pem'])
def test_async_mockgun_accepts_ca_arguments_without_filesystem_validation(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
    ca_certs: str | None,
) -> None:
    del mockgun_client

    def fail_ca_validation(ca_certs: str | None) -> str | None:
        del ca_certs
        raise AssertionError('mockgun attempted CA filesystem validation')

    monkeypatch.setenv('SHOTGUN_API_CACERTS', '/not/read/environment.pem')
    monkeypatch.setattr(config_module, 'constructor_ca_certs', fail_ca_validation)
    client = mockgun.AsyncShotgun('https://mockgun.example', ca_certs=ca_certs)
    assert client._ca_certs == (ca_certs or '/not/read/environment.pem')

    async def exercise() -> None:
        assert await client.find('Shot', []) == []
        await client.close()

    asyncio.run(exercise())


def test_async_mockgun_exposes_constructor_base_url(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    async_client = mockgun.AsyncShotgun(
        'https://mockgun.example',
        script_name='fixture',
        api_key='secret',
    )

    assert async_client.base_url == 'https://mockgun.example'


def test_async_mockgun_exposes_live_config_and_finds(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    client = mockgun.AsyncShotgun(
        'https://mockgun.example',
        script_name='fixture',
        api_key='secret',
    )
    assert isinstance(client.config, Config)
    assert client.finds == 0

    async def exercise() -> None:
        await client.find('Shot', [])
        assert client.finds == 1
        await client.find_one('Shot', [])
        assert client.finds == 2

    asyncio.run(exercise())


def test_async_mockgun_finds_is_statically_integer(tmp_path: Path) -> None:
    probe = tmp_path / 'async_mockgun_finds.py'
    probe.write_text(
        """from typing import assert_type

from shotgun_api3.lib.mockgun import AsyncShotgun

client = AsyncShotgun('https://mockgun.example')
assert_type(client.finds, int)
assert_type(client.finds + 1, int)
""",
        encoding='utf-8',
    )
    repository = Path(__file__).parents[2]

    result = run_pyright(probe, cwd=repository)

    assert result.returncode == 0, result.stdout


def test_async_mockgun_forwards_data_validation_errors(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    client = mockgun.AsyncShotgun(
        'https://mockgun.example',
        script_name='fixture',
        api_key='secret',
    )

    async def exercise() -> None:
        with pytest.raises(ShotgunError, match=r'Shot\.reviewers.*multi_entity.*list'):
            await client.create('Shot', {'reviewers': None})

        shot = await client.create('Shot', {'code': 'original'})
        assert isinstance(shot['id'], int)
        with pytest.raises(ShotgunError, match=r'Shot\.reviewers.*multi_entity.*list'):
            await client.update('Shot', shot['id'], {'code': 'changed', 'reviewers': None})
        with pytest.raises(ShotgunError, match='field names must be nonempty strings'):
            await client.batch(
                [{'request_type': 'create', 'entity_type': 'Shot', 'data': {1: 'invalid'}}]
            )
        assert await client.find('Shot', [], ['code']) == [shot]

    asyncio.run(exercise())


def test_async_mockgun_returns_representative_results(
    mockgun_client: mockgun.Shotgun,
) -> None:
    async_client = mockgun.AsyncShotgun(
        'https://mockgun.example',
        script_name='fixture',
        api_key='secret',
    )
    assert isinstance(async_client, RealAsyncShotgun)

    async def exercise() -> None:
        created = await async_client.create('Shot', {'code': 'S1'})
        assert created == {'code': 'S1', 'type': 'Shot', 'id': created['id']}
        assert isinstance(created['id'], int)
        assert await async_client.find('Shot', [], ['code']) == [created]
        assert (
            await async_client.find_one('Shot', [['id', 'is', created['id']]], ['code']) == created
        )
        assert await async_client.update('Shot', created['id'], {'code': 'S2'}) == {
            'code': 'S2',
            'type': 'Shot',
            'id': created['id'],
        }
        target = await async_client.create('Project', {'name': 'P0'})
        results = await async_client.batch(
            [
                {'request_type': 'create', 'entity_type': 'Project', 'data': {'name': 'P1'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Project',
                    'entity_id': target['id'],
                    'data': {'name': 'P2'},
                },
                {'request_type': 'delete', 'entity_type': 'Project', 'entity_id': target['id']},
            ]
        )
        first_result = results[0]
        assert isinstance(first_result, dict)
        assert results == [
            {'name': 'P1', 'type': 'Project', 'id': first_result['id']},
            {'name': 'P2', 'type': 'Project', 'id': target['id']},
            True,
        ]
        assert isinstance(first_result['id'], int)
        assert first_result['id'] != target['id']
        assert await async_client.schema_read() == mockgun_client.schema_read()
        assert await async_client.schema_entity_read() == mockgun_client.schema_entity_read()
        assert await async_client.schema_field_read('Shot', 'code') == (
            mockgun_client.schema_field_read('Shot', 'code')
        )
        assert await async_client.delete('Shot', created['id'])
        assert await async_client.revive('Shot', created['id'])
        assert await mockgun.AsyncShotgun.get_schema_paths() == mockgun.Shotgun.get_schema_paths()

    asyncio.run(exercise())
