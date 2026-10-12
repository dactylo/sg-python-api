import asyncio
import inspect
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Literal, TypeAlias, cast

import pytest

from shotgun_api3 import ShotgunError
from shotgun_api3.lib import mockgun
from tests._pyright import run_pyright

_DescriptorKind: TypeAlias = Literal['classmethod', 'method', 'property']
_DescriptorInventory: TypeAlias = dict[
    str,
    tuple[_DescriptorKind, Callable[..., object] | None],
]


def _public_descriptors(class_: type[object]) -> _DescriptorInventory:
    descriptors: _DescriptorInventory = {}
    for base in reversed(class_.__mro__):
        for name, member in vars(base).items():
            if name.startswith('_'):
                continue
            if isinstance(member, classmethod):
                raw_member = cast('object', member)
                descriptors[name] = (
                    'classmethod',
                    cast(
                        'Callable[..., object]',
                        object.__getattribute__(raw_member, '__func__'),
                    ),
                )
            elif isinstance(member, property):
                descriptors[name] = ('property', None)
            elif inspect.isfunction(member):
                descriptors[name] = ('method', cast('Callable[..., object]', member))
    return descriptors


def test_async_mockgun_constructor_and_descriptor_parity() -> None:
    assert inspect.signature(mockgun.AsyncShotgun) == inspect.signature(mockgun.Shotgun)
    sync_descriptors = _public_descriptors(mockgun.Shotgun)
    async_descriptors = _public_descriptors(mockgun.AsyncShotgun)

    assert async_descriptors.keys() == sync_descriptors.keys()
    for name, (sync_kind, sync_member) in sync_descriptors.items():
        async_kind, async_member = async_descriptors[name]
        assert async_kind == sync_kind, name
        if sync_member is None or async_member is None:
            assert sync_kind == 'property'
            continue
        assert inspect.signature(async_member) == inspect.signature(sync_member), name
        assert inspect.iscoroutinefunction(async_member), name


def test_async_mockgun_delegates_representative_results(
    mockgun_client: mockgun.Shotgun,
) -> None:
    async_client = mockgun.AsyncShotgun(
        'https://mockgun.example',
        script_name='fixture',
        api_key='secret',
    )
    assert isinstance(object.__getattribute__(async_client, '_sync'), mockgun.Shotgun)

    async def exercise() -> None:
        created = await async_client.create('Shot', {'code': 'S1'})
        assert created == {'code': 'S1', 'type': 'Shot', 'id': 1}
        assert await async_client.find('Shot', [], ['code']) == [created]
        assert await async_client.find_one('Shot', [['id', 'is', 1]], ['code']) == created
        assert await async_client.update('Shot', 1, {'code': 'S2'}) == [
            {'code': 'S2', 'type': 'Shot', 'id': 1}
        ]
        assert await async_client.batch(
            [
                {'request_type': 'create', 'entity_type': 'Project', 'data': {'name': 'P1'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Project',
                    'entity_id': 1,
                    'data': {'name': 'P2'},
                },
                {'request_type': 'delete', 'entity_type': 'Project', 'entity_id': 1},
            ]
        ) == [
            {'name': 'P1', 'type': 'Project', 'id': 1},
            {'name': 'P2', 'type': 'Project', 'id': 1},
            True,
        ]
        assert await async_client.schema_read() == mockgun_client.schema_read()
        assert await async_client.schema_entity_read() == mockgun_client.schema_entity_read()
        assert await async_client.schema_field_read('Shot', 'code') == (
            mockgun_client.schema_field_read('Shot', 'code')
        )
        assert await async_client.delete('Shot', 1)
        assert await async_client.revive('Shot', 1)
        assert await mockgun.AsyncShotgun.get_schema_paths() == mockgun.Shotgun.get_schema_paths()

    asyncio.run(exercise())


def test_async_mockgun_exposes_constructor_base_url(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    async_client = mockgun.AsyncShotgun('https://mockgun.example')

    assert async_client.base_url == 'https://mockgun.example'


def test_async_mockgun_exposes_live_config_and_finds(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    client = mockgun.AsyncShotgun('https://mockgun.example')
    sync = object.__getattribute__(client, '_sync')

    assert client.config is sync.config
    assert client.finds == 0

    async def exercise() -> None:
        await client.find('Shot', [])
        assert client.finds == sync.finds == 1
        await client.find_one('Shot', [])
        assert client.finds == sync.finds == 2

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
    client = mockgun.AsyncShotgun('https://mockgun.example')

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


def test_async_mockgun_import_needs_no_httpx2() -> None:
    source_root = Path(__file__).parents[2] / 'src'
    script = textwrap.dedent(
        f"""
        import importlib.abc
        import sys

        sys.path.insert(0, {str(source_root)!r})

        class BlockHttpx2(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == 'httpx2' or fullname.startswith('httpx2.'):
                    raise ImportError('httpx2 import attempted')
                return None

        sys.meta_path.insert(0, BlockHttpx2())
        from shotgun_api3.lib import mockgun

        assert mockgun.AsyncShotgun
        assert not any(name == 'httpx2' or name.startswith('httpx2.') for name in sys.modules)
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_async_mockgun_rejects_unknown_delegate_attributes(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    client = mockgun.AsyncShotgun('https://mockgun.example')
    sync = object.__getattribute__(client, '_sync')
    delegate_name = '_schema'
    assert delegate_name in vars(sync)

    with pytest.raises(AttributeError):
        getattr(client, delegate_name)


def test_async_mockgun_schema_paths_are_subclass_isolated(
    mockgun_client: mockgun.Shotgun,
) -> None:
    del mockgun_client
    base_paths = mockgun.Shotgun.get_schema_paths()
    assert all(path is not None for path in base_paths)

    class FirstAsyncMockgun(mockgun.AsyncShotgun):
        pass

    class SecondAsyncMockgun(mockgun.AsyncShotgun):
        pass

    async def exercise() -> None:
        await FirstAsyncMockgun.set_schema_paths('first-schema', 'first-entity-schema')

        assert await FirstAsyncMockgun.get_schema_paths() == (
            'first-schema',
            'first-entity-schema',
        )
        assert await SecondAsyncMockgun.get_schema_paths() == base_paths
        assert await mockgun.AsyncShotgun.get_schema_paths() == base_paths

    try:
        asyncio.run(exercise())
    finally:
        schema_path, schema_entity_path = base_paths
        assert schema_path is not None
        assert schema_entity_path is not None
        mockgun.Shotgun.set_schema_paths(schema_path, schema_entity_path)
