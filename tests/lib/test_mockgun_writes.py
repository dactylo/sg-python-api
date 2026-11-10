import copy
import pickle
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import DEFAULT, patch

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
from shotgun_api3.lib.mockgun._server import FakeServer
from shotgun_api3.lib.mockgun._sync_client import Shotgun
from tests.conftest import entity_id as record_id
from tests.lib._schemas import schema

from .conftest import FakeSiteCase, raw_call

_AUDIT_TYPES = {
    'created_at': 'date_time',
    'created_by': 'entity',
    'updated_at': 'date_time',
    'updated_by': 'entity',
}
_ENTITY_FIELDS = {
    'ApiUser': {'firstname': 'text'},
    'HumanUser': {'login': 'text', 'name': 'text', 'sg_status_list': 'status_list'},
    'Project': {'name': 'text'},
    'Asset': {
        'code': 'text',
        'description': 'text',
        'project': 'entity',
        'sg_status_list': 'status_list',
        'sg_asset_type': 'list',
        'shots': 'multi_entity',
        'parents': 'multi_entity',
        'assets': 'multi_entity',
        **_AUDIT_TYPES,
    },
    'Shot': {
        'code': 'text',
        'description': 'text',
        'project': 'entity',
        'assets': 'multi_entity',
        'sg_sequence': 'entity',
        'tasks': 'multi_entity',
        'notes': 'multi_entity',
        **_AUDIT_TYPES,
    },
    'Sequence': {
        'code': 'text',
        'project': 'entity',
        'shots': 'multi_entity',
        **_AUDIT_TYPES,
    },
    'Version': {
        'code': 'text',
        'project': 'entity',
        'playlists': 'multi_entity',
        'published_files': 'multi_entity',
        **_AUDIT_TYPES,
    },
    'Playlist': {'code': 'text', 'project': 'entity', 'versions': 'multi_entity', **_AUDIT_TYPES},
    'PublishedFile': {'code': 'text', 'project': 'entity', 'version': 'entity'},
    'Task': {'content': 'text', 'project': 'entity', 'entity': 'entity'},
    'Note': {'subject': 'text', 'project': 'entity', 'note_links': 'multi_entity'},
    'Cut': {'code': 'text', 'project': 'entity', 'cut_items': 'multi_entity', **_AUDIT_TYPES},
    'CutItem': {'code': 'text', 'project': 'entity', 'cut': 'entity'},
}
_VALID_TYPES = {
    ('Asset', 'project'): ['Project'],
    ('Asset', 'shots'): ['Shot'],
    ('Asset', 'parents'): ['Asset'],
    ('Asset', 'assets'): ['Asset'],
    ('Shot', 'project'): ['Project'],
    ('Shot', 'assets'): ['Asset'],
    ('Shot', 'sg_sequence'): ['Sequence'],
    ('Shot', 'tasks'): ['Task'],
    ('Shot', 'notes'): ['Note'],
    ('Sequence', 'project'): ['Project'],
    ('Sequence', 'shots'): ['Shot'],
    ('Version', 'project'): ['Project'],
    ('Version', 'playlists'): ['Playlist'],
    ('Version', 'published_files'): ['PublishedFile'],
    ('Playlist', 'project'): ['Project'],
    ('Playlist', 'versions'): ['Version'],
    ('PublishedFile', 'project'): ['Project'],
    ('PublishedFile', 'version'): ['Version'],
    ('Task', 'project'): ['Project'],
    ('Task', 'entity'): ['Shot', 'Asset'],
    ('Note', 'project'): ['Project'],
    ('Note', 'note_links'): ['Shot', 'Asset'],
    ('Cut', 'project'): ['Project'],
    ('Cut', 'cut_items'): ['CutItem'],
    ('CutItem', 'project'): ['Project'],
    ('CutItem', 'cut'): ['Cut'],
    **{
        (entity_type, field): ['HumanUser', 'ApiUser']
        for entity_type in ('Asset', 'Shot', 'Sequence', 'Version', 'Playlist', 'Cut')
        for field in ('created_by', 'updated_by')
    },
}
_VALID_VALUES = {
    ('Asset', 'sg_status_list'): ['wtg', 'ip', 'fin'],
    ('Asset', 'sg_asset_type'): ['Character', 'Prop'],
}
_WRITES_SCHEMA = schema(
    _ENTITY_FIELDS,
    defaults={('Asset', 'sg_status_list'): 'wtg'},
    valid_types=_VALID_TYPES,
    valid_values=_VALID_VALUES,
)
_BOOTSTRAP_SCHEMA = {
    **_WRITES_SCHEMA,
    **{
        entity_type: {
            **_WRITES_SCHEMA[entity_type],
            **{
                field: {
                    'data_type': {'value': data_type},
                    'properties': {
                        'default_value': {'value': None},
                        'valid_types': {'value': ['HumanUser', 'ApiUser']},
                    },
                }
                if data_type == 'entity'
                else {
                    'data_type': {'value': data_type},
                    'properties': {'default_value': {'value': None}},
                }
                for field, data_type in _AUDIT_TYPES.items()
            },
        }
        for entity_type in ('ApiUser', 'HumanUser')
    },
}
_FROZEN = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_LATER = _FROZEN + timedelta(hours=1)
_SCRIPT = {'type': 'ApiUser', 'id': 1}
_ARTIST = {'type': 'HumanUser', 'id': 1}
_AUDIT_FIELDS = ['created_at', 'created_by', 'updated_at', 'updated_by']


async def _asset(case: FakeSiteCase, code: str) -> int:
    created = cast('dict[str, object]', await case.call('create', 'Asset', {'code': code}))
    return record_id(created)


def _identity(value: object) -> object:
    if isinstance(value, dict):
        link = cast('dict[str, object]', value)
        return {'type': link['type'], 'id': link['id']}
    return value


def _identities(value: object) -> object:
    if isinstance(value, list):
        return [_identity(link) for link in cast('list[object]', value)]
    return _identity(value)


def _identity_fields(case: FakeSiteCase, row: dict[str, object]) -> dict[str, object]:
    entity_type = cast('str', row['type'])
    for field, value in row.items():
        if field not in {'type', 'id'} and case.client._server.schema.field_type(
            entity_type, field
        ) in {'entity', 'multi_entity'}:
            row[field] = _identities(value)
    return row


async def _fields(case: FakeSiteCase, link: dict[str, object], *fields: str) -> dict[str, object]:
    row = cast(
        'dict[str, object]',
        await case.call('find_one', link['type'], [['id', 'is', link['id']]], list(fields)),
    )
    return _identity_fields(case, row)


def _query_raw_write(
    server: FakeServer,
    method: str,
    entity_type: str,
    data: dict[str, object],
    *,
    entity_id: int | None = None,
) -> dict[str, object]:
    params: dict[str, object] = {
        'type': entity_type,
        'fields': [{'field_name': name, 'value': value} for name, value in data.items()],
        'return_fields': [],
    }
    if entity_id is not None:
        params['id'] = entity_id
    return raw_call(server, method, params)


async def _record(case: FakeSiteCase, entity_type: str) -> dict[str, object]:
    display = {'Task': 'content', 'Note': 'subject', 'Project': 'name', 'HumanUser': 'name'}.get(
        entity_type, 'code'
    )
    data: dict[str, object] = {}
    if case.client._server.schema.declares(entity_type, display):
        count = sum(
            len(case.client._server.store.rows(entity_type, retired=retired))
            for retired in (False, True)
        )
        data[display] = f'{entity_type}-{count}'
    result = cast('dict[str, object]', await case.call('create', entity_type, data))
    return {'type': result['type'], 'id': result['id']}


def _server(case: FakeSiteCase) -> FakeServer:
    return case.client._server


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update'])
async def test_query_text_empty_values_store_as_null(
    query_site: FakeSiteCase, operation: str
) -> None:
    if operation == 'create':
        result = cast(
            'dict[str, object]', await query_site.call('create', 'Scalar', {'sg_text': ''})
        )
    else:
        other = cast(
            'dict[str, object]', await query_site.call('create', 'Scalar', {'sg_text': 'x'})
        )
        assert other['sg_text'] == 'x'
        result = cast(
            'dict[str, object]',
            await query_site.call('update', 'Scalar', other['id'], {'sg_text': ''}),
        )
    assert result['sg_text'] is None
    assert (
        await query_site.call('find_one', 'Scalar', [['id', 'is', result['id']]], ['sg_text'])
        == result
    )


@pytest.mark.anyio
@pytest.mark.parametrize('route', ['client', 'raw', 'batch'])
async def test_query_version_empty_code_create_fault_and_batch_rollback(
    query_site: FakeSiteCase, route: str
) -> None:
    if route == 'client':
        with pytest.raises(Fault, match=r'Cannot set identifier field to empty'):
            await query_site.call('create', 'Version', {'code': ''})
    elif route == 'raw':
        raw = _query_raw_write(query_site.client._server, 'create', 'Version', {'code': ''})
        assert raw['error_code'] == 104
        assert raw['message'] == (
            'API create() Create failed for [Version]: '
            'Cannot set identifier field to empty. (Version)'
        )
    else:
        with pytest.raises(Fault, match=r'Cannot set identifier field to empty'):
            await query_site.call(
                'batch',
                [
                    {
                        'request_type': 'create',
                        'entity_type': 'Version',
                        'data': {'code': 'undone'},
                    },
                    {'request_type': 'create', 'entity_type': 'Version', 'data': {'code': ''}},
                ],
            )
    assert await query_site.call('find', 'Version', [], ['code']) == []


@pytest.mark.anyio
async def test_query_version_empty_code_update_is_unsettled(query_site: FakeSiteCase) -> None:
    version = cast('dict[str, object]', await query_site.call('create', 'Version', {'code': 'v1'}))
    with pytest.raises(MockgunError, match=r'Version\.code'):
        await query_site.call('update', 'Version', version['id'], {'code': ''})
    assert (
        await query_site.call('find_one', 'Version', [['id', 'is', version['id']]], ['code'])
        == version
    )


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update'])
@pytest.mark.parametrize('route', ['client', 'raw'])
async def test_query_checkbox_null_faults_before_mutation(
    query_site: FakeSiteCase, operation: str, route: str
) -> None:
    scalar = None
    if operation == 'update':
        scalar = cast('dict[str, object]', await query_site.call('create', 'Scalar', {}))
    if route == 'client':
        if operation == 'create':
            call = query_site.call('create', 'Scalar', {'sg_checkbox': None})
        else:
            assert scalar is not None
            call = query_site.call('update', 'Scalar', scalar['id'], {'sg_checkbox': None})
        with pytest.raises(Fault, match=r'Scalar\.sg_checkbox.*checkbox'):
            await call
    else:
        raw = _query_raw_write(
            query_site.client._server,
            operation,
            'Scalar',
            {'sg_checkbox': None},
            entity_id=record_id(scalar) if scalar is not None else None,
        )
        assert raw['error_code'] == 103
        assert raw['message'] == (
            'Scalar.sg_checkbox is of schema type checkbox, but data None is not a bool'
        )
    if scalar is None:
        assert await query_site.call('find', 'Scalar', []) == []
    else:
        assert await query_site.call('find_one', 'Scalar', [], ['sg_checkbox']) == {
            'type': 'Scalar',
            'id': scalar['id'],
            'sg_checkbox': False,
        }


@pytest.mark.anyio
@pytest.mark.parametrize('route', ['client', 'raw'])
async def test_query_multi_entity_null_fault_keeps_message(
    query_site: FakeSiteCase, route: str
) -> None:
    message = 'Shot.assets is of type multi_entity, but data None is not a list'
    if route == 'client':
        with pytest.raises(Fault, match=r'Shot\.assets.*multi_entity.*list'):
            await query_site.call('create', 'Shot', {'code': 'S1', 'assets': None})
    else:
        raw = _query_raw_write(
            query_site.client._server, 'create', 'Shot', {'code': 'S1', 'assets': None}
        )
        assert raw['error_code'] == 103
        assert raw['message'] == message
    assert await query_site.call('find', 'Shot', []) == []


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['create', 'update'])
@pytest.mark.parametrize('route', ['client', 'raw'])
async def test_query_unknown_write_fields_fault_with_code(
    query_site: FakeSiteCase, operation: str, route: str
) -> None:
    message = 'no_such is not a valid field for entity Shot'
    shot = None
    if operation == 'update':
        shot = cast('dict[str, object]', await query_site.call('create', 'Shot', {'code': 'S1'}))
    if route == 'client':
        if operation == 'create':
            call = query_site.call('create', 'Shot', {'no_such': 1})
        else:
            assert shot is not None
            call = query_site.call('update', 'Shot', shot['id'], {'no_such': 1})
        with pytest.raises(Fault, match=message):
            await call
    else:
        raw = _query_raw_write(
            query_site.client._server,
            operation,
            'Shot',
            {'no_such': 1},
            entity_id=record_id(shot) if shot is not None else None,
        )
        assert raw['error_code'] == 103
        assert raw['message'] == message
    if shot is None:
        assert await query_site.call('find', 'Shot', []) == []
    else:
        assert await query_site.call('find_one', 'Shot', [], ['code']) == shot


@pytest.mark.anyio
async def test_query_nonempty_text_null_version_and_invalid_list_controls(
    query_site: FakeSiteCase,
) -> None:
    scalar = cast('dict[str, object]', await query_site.call('create', 'Scalar', {'sg_text': 'x'}))
    assert scalar['sg_text'] == 'x'
    assert await query_site.call('find_one', 'Scalar', [], ['sg_text']) == scalar
    assert await query_site.call('find_one', 'Scalar', [], ['sg_checkbox']) == {
        'type': 'Scalar',
        'id': scalar['id'],
        'sg_checkbox': False,
    }
    version = cast('dict[str, object]', await query_site.call('create', 'Version', {'code': None}))
    assert version['code'] is None
    shot = cast('dict[str, object]', await query_site.call('create', 'Shot', {'code': 'S2'}))
    assert await query_site.call('find_one', 'Shot', [], ['assets']) == {
        'type': 'Shot',
        'id': shot['id'],
        'assets': [],
    }
    raw = _query_raw_write(query_site.client._server, 'create', 'Scalar', {'sg_list': ''})
    assert raw['error_code'] == 104
    with pytest.raises(Fault):
        await query_site.call('create', 'Scalar', {'sg_list': ''})


def test_schema_without_audit_fields_does_not_gain_them(mockgun_client: Shotgun) -> None:
    created = mockgun_client.create('Shot', {'code': 'no audit'})
    row = mockgun_client.find_one('Shot', [], ['code'])
    assert created == {'code': 'no audit', 'id': 1, 'type': 'Shot'}
    assert row == created
    stored = mockgun_client._server.store.row('Shot', 1)
    assert stored is not None
    assert set(stored).isdisjoint(_AUDIT_FIELDS)
    mockgun_client.update('Shot', 1, {'code': 'changed'})
    stored = mockgun_client._server.store.row('Shot', 1)
    assert stored is not None
    assert set(stored).isdisjoint(_AUDIT_FIELDS)


@pytest.fixture(params=['sync', 'async'])
async def writes_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    schema_path = tmp_path / 'writes-schema.pickle'
    entity_path = tmp_path / 'writes-entities.pickle'
    schema_path.write_bytes(pickle.dumps(_WRITES_SCHEMA, protocol=2))
    entity_path.write_bytes(pickle.dumps({}, protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    client = (Shotgun if request.param == 'sync' else AsyncShotgun)(
        'http://unit-test.example', script_name='script', api_key='key'
    )
    case = FakeSiteCase(str(request.param), client)
    client.site.set_time(_FROZEN)
    yield case
    await case.call('close')


def _raw(
    server: FakeServer,
    method: str,
    params: dict[str, object],
    *,
    sudo: bool = False,
) -> dict[str, object]:
    auth: dict[str, object] = {'script_name': 'script', 'script_key': 'key'}
    if sudo:
        auth['sudo_as_login'] = 'artist'
    return raw_call(server, method, params, auth=auth)


async def _audit(case: FakeSiteCase, asset_id: int, *, retired: bool = False) -> dict[str, object]:
    row = cast(
        'dict[str, object]',
        await case.call(
            'find_one',
            'Asset',
            [['id', 'is', asset_id]],
            _AUDIT_FIELDS,
            retired_only=retired,
        ),
    )

    return _identity_fields(case, row)


async def _write_link(
    case: FakeSiteCase,
    link: dict[str, object],
    field: str,
    value: object,
    mode: str = 'set',
) -> None:
    await case.call('update', link['type'], link['id'], {field: value}, {field: mode})


@pytest.mark.anyio
async def test_batch_rolls_back_rows_pairs_retirement_and_audits(
    writes_site: FakeSiteCase,
) -> None:
    assets = [await _record(writes_site, 'Asset') for _ in range(2)]
    shot = await _record(writes_site, 'Shot')
    retired = await _record(writes_site, 'Shot')
    await _write_link(writes_site, shot, 'assets', [assets[0]])
    await _write_link(writes_site, retired, 'description', 'original')
    shot_before = await _fields(writes_site, shot, 'assets', *_AUDIT_FIELDS)
    retired_before = await _fields(writes_site, retired, 'description', *_AUDIT_FIELDS)
    assets_before = [
        await _fields(writes_site, asset, 'shots', *_AUDIT_FIELDS) for asset in assets
    ]
    server = _server(writes_site)
    store, schema, sessions = server.store, server.schema, server.sessions
    tables = store._db.copy()
    server.set_time(_LATER)
    with pytest.raises(Fault, match='No entity of type Shot exists with id 99999'):
        await writes_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Shot', 'data': {'code': 'undone'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': shot['id'],
                    'data': {'assets': assets},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': retired['id'],
                    'data': {'description': 'changed'},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': retired['id']},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'code': 'missing'},
                },
            ],
        )
    assert await writes_site.call('find', 'Shot', [['code', 'is', 'undone']]) == []
    assert await _fields(writes_site, shot, 'assets', *_AUDIT_FIELDS) == shot_before
    assert await _fields(writes_site, retired, 'description', *_AUDIT_FIELDS) == retired_before
    assert await writes_site.call('find', 'Shot', [], retired_only=True) == []
    assert [
        await _fields(writes_site, asset, 'shots', *_AUDIT_FIELDS) for asset in assets
    ] == assets_before
    assert server.store is store
    assert server.schema is schema
    assert server.sessions is sessions
    assert sessions._store is store
    assert sessions._schema is schema
    assert all(store._db[name] is table for name, table in tables.items())


@pytest.mark.anyio
async def test_batch_rolls_back_mockgun_error(writes_site: FakeSiteCase) -> None:
    with pytest.raises(MockgunError, match=r'project\.Project\.name'):
        await writes_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Shot', 'data': {'code': 'undone'}},
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {'code': 'unsupported'},
                    'return_fields': ['project.Project.name'],
                },
            ],
        )
    assert await writes_site.call('find', 'Shot', []) == []


@pytest.mark.anyio
async def test_batch_rolls_back_unexpected_error_without_retry(writes_site: FakeSiteCase) -> None:
    shots = [await _record(writes_site, 'Shot') for _ in range(2)]
    before = [await _fields(writes_site, shot, 'description', *_AUDIT_FIELDS) for shot in shots]
    _server(writes_site).set_time(_LATER)
    store = _server(writes_site).store
    with (
        patch.object(
            store, 'update', wraps=store.update, side_effect=[DEFAULT, RuntimeError('injected')]
        ) as update,
        pytest.raises(RuntimeError, match='injected'),
    ):
        await writes_site.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': shot['id'],
                    'data': {'description': 'changed'},
                }
                for shot in shots
            ],
        )
    assert update.call_count == 2
    assert [
        await _fields(writes_site, shot, 'description', *_AUDIT_FIELDS) for shot in shots
    ] == before


@pytest.mark.anyio
async def test_batch_guard_rollback_does_not_reuse_ids(writes_site: FakeSiteCase) -> None:
    first = await _record(writes_site, 'Project')
    with pytest.raises(Fault, match='No entity of type Project exists'):
        await writes_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Project', 'data': {'name': 'undone'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Project',
                    'entity_id': 99999,
                    'data': {'name': 'missing'},
                },
            ],
        )
    after = await _record(writes_site, 'Project')
    assert after['id'] == record_id(first) + 2


@pytest.mark.anyio
async def test_batch_guard_ids_start_above_seeded_event(fake_site: FakeSiteCase) -> None:
    assert await fake_site.call('find', 'EventLogEntry', []) == [
        {'type': 'EventLogEntry', 'id': 1}
    ]
    created = await _record(fake_site, 'EventLogEntry')
    assert created == {'type': 'EventLogEntry', 'id': 2}


@pytest.mark.anyio
async def test_batch_control_commits_results_in_order(writes_site: FakeSiteCase) -> None:
    shot = await _record(writes_site, 'Shot')
    result = cast(
        'list[object]',
        await writes_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Project', 'data': {'name': 'kept'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': shot['id'],
                    'data': {'description': 'kept'},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': shot['id']},
            ],
        ),
    )
    assert result == [
        {'type': 'Project', 'id': 1, 'name': 'kept'},
        {**shot, 'description': 'kept'},
        True,
    ]
    assert await writes_site.call('find', 'Project', [], ['name']) == [result[0]]
    assert await writes_site.call('find', 'Shot', []) == []
    assert await writes_site.call('find', 'Shot', [], ['description'], retired_only=True) == [
        result[1]
    ]


@pytest.mark.anyio
async def test_create_defaults_audit_fields_to_request_actor_and_time(
    writes_site: FakeSiteCase,
) -> None:
    asset_id = await _asset(writes_site, 'defaulted')
    assert await _audit(writes_site, asset_id) == {
        'type': 'Asset',
        'id': asset_id,
        'created_at': _FROZEN,
        'updated_at': _FROZEN,
        'created_by': _SCRIPT,
        'updated_by': _SCRIPT,
    }


@pytest.mark.anyio
async def test_create_replaces_supplied_updated_by(writes_site: FakeSiteCase) -> None:
    await writes_site.call('create', 'HumanUser', {'login': 'artist'})
    created = cast(
        'dict[str, object]',
        await writes_site.call(
            'create',
            'Asset',
            {'code': 'override', 'updated_by': _ARTIST},
        ),
    )
    assert (await _audit(writes_site, record_id(created)))['updated_by'] == _SCRIPT


@pytest.mark.anyio
async def test_sudo_update_changes_only_updated_audit_fields(
    writes_site: FakeSiteCase,
) -> None:
    asset_id = await _asset(writes_site, 'before')
    _server(writes_site).set_time(_LATER)
    response = _raw(
        _server(writes_site),
        'update',
        {
            'type': 'Asset',
            'id': asset_id,
            'fields': [{'field_name': 'code', 'value': 'after'}],
        },
        sudo=True,
    )
    assert 'exception' not in response
    assert await _audit(writes_site, asset_id) == {
        'type': 'Asset',
        'id': asset_id,
        'created_at': _FROZEN,
        'created_by': _SCRIPT,
        'updated_at': _LATER,
        'updated_by': _ARTIST,
    }


@pytest.mark.anyio
@pytest.mark.parametrize('method', ['delete', 'revive'])
async def test_sudo_retirement_and_revival_record_human_actor(
    writes_site: FakeSiteCase,
    method: str,
) -> None:
    asset_id = await _asset(writes_site, method)
    if method == 'revive':
        assert await writes_site.call('delete', 'Asset', asset_id)
    _server(writes_site).set_time(_LATER)
    response = _raw(_server(writes_site), method, {'type': 'Asset', 'id': asset_id}, sudo=True)
    assert response == {'results': True}
    assert await _audit(writes_site, asset_id, retired=method == 'delete') == {
        'type': 'Asset',
        'id': asset_id,
        'created_at': _FROZEN,
        'created_by': _SCRIPT,
        'updated_at': _LATER,
        'updated_by': _ARTIST,
    }


@pytest.mark.anyio
@pytest.mark.parametrize('field', _AUDIT_FIELDS)
async def test_update_rejects_audit_fields_before_mutation(
    writes_site: FakeSiteCase,
    field: str,
) -> None:
    asset_id = await _asset(writes_site, field)
    before = copy.deepcopy(await _audit(writes_site, asset_id))
    value = _LATER if field.endswith('_at') else _ARTIST
    with pytest.raises(Fault, match=rf'API update\(\) Asset\.{field} is editable on create only'):
        await writes_site.call('update', 'Asset', asset_id, {'code': 'changed', field: value})
    assert _raw(
        _server(writes_site),
        'update',
        {
            'type': 'Asset',
            'id': asset_id,
            'fields': [
                {
                    'field_name': field,
                    'value': _LATER.isoformat() if field.endswith('_at') else _ARTIST,
                }
            ],
        },
    ) == {
        'exception': True,
        'message': f'API update() Asset.{field} is editable on create only',
        'error_code': 103,
    }
    assert await _audit(writes_site, asset_id) == before
    assert await writes_site.call('find_one', 'Asset', [['id', 'is', asset_id]], ['code']) == {
        'type': 'Asset',
        'id': asset_id,
        'code': field,
    }


@pytest.mark.anyio
async def test_retire_and_revive_advance_updated_at_for_script(
    writes_site: FakeSiteCase,
) -> None:
    asset_id = await _asset(writes_site, 'clock')
    writes_site.client.site.set_time(_LATER)
    assert await writes_site.call('delete', 'Asset', asset_id)
    retired = await _audit(writes_site, asset_id, retired=True)
    assert (retired['updated_at'], retired['updated_by']) == (_LATER, _SCRIPT)
    final_time = _LATER + timedelta(hours=1)
    writes_site.client.site.set_time(final_time)
    assert await writes_site.call('revive', 'Asset', asset_id)
    revived = await _audit(writes_site, asset_id)
    assert (revived['updated_at'], revived['updated_by']) == (final_time, _SCRIPT)


@pytest.mark.anyio
async def test_create_keeps_supplied_audit_fields_except_updated_by(
    writes_site: FakeSiteCase,
) -> None:
    supplied_time = _FROZEN - timedelta(days=1)
    created = cast(
        'dict[str, object]',
        await writes_site.call(
            'create',
            'Asset',
            {
                'code': 'supplied',
                'created_at': supplied_time,
                'updated_at': supplied_time,
                'created_by': _ARTIST,
            },
        ),
    )
    audit = await _audit(writes_site, record_id(created))
    assert [audit[field] for field in _AUDIT_FIELDS] == [
        supplied_time,
        _ARTIST,
        supplied_time,
        _SCRIPT,
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'case',
    [
        ('sg_status_list', 'bogus', 'status', 'wtg, ip, fin'),
        ('sg_asset_type', 'Bogus', 'list value', 'Character, Prop'),
    ],
)
@pytest.mark.parametrize('method', ['create', 'update'])
async def test_declared_valid_values_reject_invalid_writes_before_mutation(
    writes_site: FakeSiteCase,
    case: tuple[str, str, str, str],
    method: str,
) -> None:
    field, invalid, kind, values = case
    message = (
        f'API {method}() Update failed for [Asset.{field}]: {invalid} is not a valid '
        f'{kind}. Valid {"statuses" if kind == "status" else "list values"}: {values}.'
    )
    if method == 'create':
        before = await writes_site.call('find', 'Asset', [], ['code', field])
        data = {'code': 'invalid', field: invalid}
        with pytest.raises(Fault) as error:
            await writes_site.call('create', 'Asset', data)
        assert str(error.value) == message
        assert await writes_site.call('find', 'Asset', [], ['code', field]) == before
        response = _raw(
            _server(writes_site),
            'create',
            {
                'type': 'Asset',
                'fields': [{'field_name': name, 'value': value} for name, value in data.items()],
                'return_fields': ['id'],
            },
        )
        assert await writes_site.call('find', 'Asset', [], ['code', field]) == before
    else:
        asset_id = await _asset(writes_site, 'unchanged')
        assert 'exception' not in _raw(
            _server(writes_site),
            'update',
            {'type': 'Asset', 'id': asset_id, 'fields': []},
            sudo=True,
        )
        before = await writes_site.call(
            'find_one',
            'Asset',
            [['id', 'is', asset_id]],
            ['code', field, 'updated_at', 'updated_by'],
        )
        _server(writes_site).set_time(_LATER)
        data = {'code': 'changed', field: invalid}
        with pytest.raises(Fault) as error:
            await writes_site.call('update', 'Asset', asset_id, data)
        assert str(error.value) == message
        assert (
            await writes_site.call(
                'find_one',
                'Asset',
                [['id', 'is', asset_id]],
                ['code', field, 'updated_at', 'updated_by'],
            )
            == before
        )
        response = _raw(
            _server(writes_site),
            'update',
            {
                'type': 'Asset',
                'id': asset_id,
                'fields': [{'field_name': name, 'value': value} for name, value in data.items()],
            },
        )
        assert (
            await writes_site.call(
                'find_one',
                'Asset',
                [['id', 'is', asset_id]],
                ['code', field, 'updated_at', 'updated_by'],
            )
            == before
        )
    assert response == {'exception': True, 'message': message, 'error_code': 104}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field', 'values'),
    [
        ('sg_status_list', ('wtg', 'ip', 'fin', None)),
        ('sg_asset_type', ('Character', 'Prop', None)),
    ],
)
async def test_declared_valid_values_accept_null_and_every_member(
    writes_site: FakeSiteCase,
    field: str,
    values: tuple[str | None, ...],
) -> None:
    for value in values:
        created = cast(
            'dict[str, object]',
            await writes_site.call('create', 'Asset', {'code': 'valid', field: value}),
        )
        asset_id = record_id(created)
        row = cast(
            'dict[str, object]',
            await writes_site.call('find_one', 'Asset', [['id', 'is', asset_id]], [field]),
        )
        assert row[field] == value
        if value == values[0]:
            for replacement in values:
                await writes_site.call('update', 'Asset', asset_id, {field: replacement})
                row = cast(
                    'dict[str, object]',
                    await writes_site.call('find_one', 'Asset', [['id', 'is', asset_id]], [field]),
                )
                assert row[field] == replacement


@pytest.mark.anyio
@pytest.mark.parametrize('method', ['create', 'update'])
@pytest.mark.parametrize('field', ['sg_status_list', 'sg_asset_type'])
async def test_declared_valid_values_check_type_first(
    writes_site: FakeSiteCase, method: str, field: str
) -> None:
    if method == 'create':
        with pytest.raises(Fault, match=f'Asset\\.{field} is of schema type') as error:
            await writes_site.call('create', 'Asset', {'code': 'invalid type', field: 42})
    else:
        asset_id = await _asset(writes_site, 'invalid type')
        with pytest.raises(Fault, match=f'Asset\\.{field} is of schema type') as error:
            await writes_site.call('update', 'Asset', asset_id, {field: 42})
    assert 'not a valid' not in str(error.value)


@pytest.mark.parametrize(
    ('entity_type', 'field'), [('Shot', 'sg_status_list'), ('ScalarTypes', 'list')]
)
def test_schema_without_valid_values_accepts_any_string(
    mockgun_client: Shotgun, entity_type: str, field: str
) -> None:
    created = mockgun_client.create(entity_type, {field: 'bogus'})
    assert created[field] == 'bogus'
    changed = mockgun_client.update(entity_type, record_id(created), {field: 'anything'})
    assert changed[field] == 'anything'


@pytest.mark.anyio
@pytest.mark.parametrize('batched', [False, True])
@pytest.mark.parametrize(
    ('return_fields', 'status_requested'),
    [(['sg_status_list'], True), (['description'], False)],
)
async def test_create_returns_supplied_and_declared_requested_fields(
    writes_site: FakeSiteCase,
    return_fields: list[str],
    status_requested: bool,
    batched: bool,
) -> None:
    project = cast(
        'dict[str, object]', await writes_site.call('create', 'Project', {'name': 'demo'})
    )
    project_link = {'type': 'Project', 'id': project['id']}
    data = {'code': 'created', 'description': 'description', 'project': project_link}
    if batched:
        results = cast(
            'list[dict[str, object]]',
            await writes_site.call(
                'batch',
                [
                    {
                        'request_type': 'create',
                        'entity_type': 'Asset',
                        'data': data,
                        'return_fields': return_fields,
                    }
                ],
            ),
        )
        result = results[0]
    else:
        result = cast(
            'dict[str, object]',
            await writes_site.call('create', 'Asset', data, return_fields=return_fields),
        )
    expected: dict[str, object] = {'type': 'Asset', 'id': 1, **data}
    if status_requested:
        expected['sg_status_list'] = 'wtg'
    assert _identity_fields(writes_site, result) == expected


@pytest.mark.anyio
async def test_create_ignores_unknown_requested_field(writes_site: FakeSiteCase) -> None:
    result = await writes_site.call(
        'create', 'Asset', {'code': 'created'}, return_fields=['code', 'no_such_field']
    )
    assert result == {'type': 'Asset', 'id': 1, 'code': 'created'}


@pytest.mark.anyio
async def test_create_rejects_dotted_requested_field_before_mutation(
    writes_site: FakeSiteCase,
) -> None:
    with pytest.raises(MockgunError, match='project\\.Project\\.name'):
        await writes_site.call(
            'create',
            'Asset',
            {'code': 'rejected'},
            return_fields=['project.Project.name'],
        )
    assert await writes_site.call('find', 'Asset', [['code', 'is', 'rejected']]) == []


@pytest.mark.anyio
async def test_create_without_return_fields_returns_supplied_fields(
    writes_site: FakeSiteCase,
) -> None:
    result = await writes_site.call(
        'create', 'Asset', {'code': 'created', 'description': 'description'}
    )
    assert result == {
        'type': 'Asset',
        'id': 1,
        'code': 'created',
        'description': 'description',
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('removals', 'remaining'), [([1, 2], [0]), ([0, 0], [1, 2]), ([], [0, 1, 2])]
)
async def test_pair_remove_keeps_only_unselected_identities(
    writes_site: FakeSiteCase, removals: list[int], remaining: list[int]
) -> None:
    shot = await _record(writes_site, 'Shot')
    assets = [await _record(writes_site, 'Asset') for _ in range(3)]
    await _write_link(writes_site, shot, 'assets', assets)
    await _write_link(writes_site, shot, 'assets', [assets[i] for i in removals], 'remove')
    assert (await _fields(writes_site, shot, 'assets'))['assets'] == [assets[i] for i in remaining]
    for i, asset in enumerate(assets):
        assert (await _fields(writes_site, asset, 'shots'))['shots'] == (
            [shot] if i in remaining else []
        )
    await _write_link(writes_site, shot, 'assets', [assets[1], assets[1]], 'add')
    assert (await _fields(writes_site, assets[1], 'shots'))['shots'] == [shot]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'relationship',
    [
        ('Shot', 'assets', 'Asset', 'shots', True),
        ('Asset', 'shots', 'Shot', 'assets', True),
        ('Asset', 'parents', 'Asset', 'assets', True),
        ('Asset', 'assets', 'Asset', 'parents', True),
        ('Version', 'playlists', 'Playlist', 'versions', True),
        ('Playlist', 'versions', 'Version', 'playlists', True),
        ('PublishedFile', 'version', 'Version', 'published_files', False),
        ('Task', 'entity', 'Shot', 'tasks', False),
        ('Note', 'note_links', 'Shot', 'notes', True),
        ('CutItem', 'cut', 'Cut', 'cut_items', False),
    ],
)
async def test_pair_additions_and_clearing(
    writes_site: FakeSiteCase,
    relationship: tuple[str, str, str, str, bool],
) -> None:
    writer_type, field, target_type, mirror, multi = relationship
    writer = await _record(writes_site, writer_type)
    target = await _record(writes_site, target_type)
    await _write_link(writes_site, writer, field, [target] if multi else target)
    assert (await _fields(writes_site, target, mirror))[mirror] == [writer]
    await _write_link(writes_site, writer, field, [] if multi else None)
    assert (await _fields(writes_site, target, mirror))[mirror] == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    'relationship',
    [
        ('Asset', 'parents', 'Asset', 'assets', True, 'remove'),
        ('Asset', 'assets', 'Asset', 'parents', True, 'remove'),
        ('Version', 'playlists', 'Playlist', 'versions', True, 'remove'),
        ('Playlist', 'versions', 'Version', 'playlists', True, 'remove'),
        ('Note', 'note_links', 'Shot', 'notes', True, 'remove'),
        ('Asset', 'parents', 'Asset', 'assets', True, 'set'),
        ('Shot', 'sg_sequence', 'Sequence', 'shots', False, 'set'),
        ('PublishedFile', 'version', 'Version', 'published_files', False, 'set'),
        ('Task', 'entity', 'Shot', 'tasks', False, 'set'),
        ('CutItem', 'cut', 'Cut', 'cut_items', False, 'set'),
    ],
)
async def test_pair_removal_and_reparenting(
    writes_site: FakeSiteCase,
    relationship: tuple[str, str, str, str, bool, str],
) -> None:
    writer_type, field, target_type, mirror, multi, mode = relationship
    writer = await _record(writes_site, writer_type)
    x = await _record(writes_site, target_type)
    y = await _record(writes_site, target_type)
    await _write_link(writes_site, writer, field, [x, y] if multi else x)
    value = ([x] if mode == 'remove' else [y]) if multi else y
    await _write_link(writes_site, writer, field, value, mode)
    assert (await _fields(writes_site, x, mirror))[mirror] == []
    assert (await _fields(writes_site, y, mirror))[mirror] == [writer]
    if not multi:
        await _write_link(writes_site, writer, field, None)
        assert (await _fields(writes_site, y, mirror))[mirror] == []


@pytest.mark.anyio
async def test_pair_sequence_moves_shot_and_preserves_other_members(
    writes_site: FakeSiteCase,
) -> None:
    x = await _record(writes_site, 'Sequence')
    y = await _record(writes_site, 'Sequence')
    p = await _record(writes_site, 'Shot')
    q = await _record(writes_site, 'Shot')
    await _write_link(writes_site, p, 'sg_sequence', x)
    assert (await _fields(writes_site, x, 'shots'))['shots'] == [p]
    await _write_link(writes_site, x, 'shots', [q], 'add')
    assert (await _fields(writes_site, q, 'sg_sequence'))['sg_sequence'] == x
    await _write_link(writes_site, y, 'shots', [p], 'add')
    assert (await _fields(writes_site, p, 'sg_sequence'))['sg_sequence'] == y
    assert (await _fields(writes_site, x, 'shots'))['shots'] == [q]
    assert (await _fields(writes_site, y, 'shots'))['shots'] == [p]
    await _write_link(writes_site, y, 'shots', [p], 'remove')
    assert (await _fields(writes_site, p, 'sg_sequence'))['sg_sequence'] is None


@pytest.mark.anyio
async def test_pair_create_and_batch_share_mirrors(writes_site: FakeSiteCase) -> None:
    asset = await _record(writes_site, 'Asset')
    result = cast(
        'dict[str, object]',
        await writes_site.call('create', 'Shot', {'code': 'paired', 'assets': [asset]}),
    )
    shot = {'type': 'Shot', 'id': result['id']}
    assert (await _fields(writes_site, asset, 'shots'))['shots'] == [shot]
    clear: dict[str, object] = {
        'request_type': 'update',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'data': {'assets': []},
    }
    await writes_site.call('batch', [clear])
    assert (await _fields(writes_site, asset, 'shots'))['shots'] == []
    await writes_site.call(
        'batch',
        [
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': shot['id'],
                'data': {'assets': [asset]},
            }
        ],
    )
    assert (await _fields(writes_site, asset, 'shots'))['shots'] == [shot]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'relationship',
    [
        pytest.param(
            ('Shot', 'assets', 'Asset', 'shots', True, (), (0,), 'add', (True, False)), id='W15-C'
        ),
        pytest.param(
            ('Shot', 'assets', 'Asset', 'shots', True, (0, 1), (0,), 'remove', (True, False)),
            id='W17-01',
        ),
        pytest.param(
            ('Asset', 'shots', 'Shot', 'assets', True, (), (0,), 'add', (True, False)), id='W17-02'
        ),
        pytest.param(
            ('Shot', 'sg_sequence', 'Sequence', 'shots', False, (0,), (1,), 'set', (True, True)),
            id='W17-03',
        ),
        pytest.param(
            ('Sequence', 'shots', 'Shot', 'sg_sequence', True, (), (0,), 'add', (True, False)),
            id='W17-04',
        ),
        pytest.param(
            ('Task', 'entity', 'Shot', 'tasks', False, (0,), (), 'set', (True, False)), id='W17-05'
        ),
        pytest.param(
            ('Asset', 'parents', 'Asset', 'assets', True, (0, 1), (0,), 'set', (False, True)),
            id='W17-06',
        ),
        pytest.param(
            ('Asset', 'assets', 'Asset', 'parents', True, (0,), (0,), 'remove', (True, False)),
            id='W17-07',
        ),
        pytest.param(
            (
                'Version',
                'playlists',
                'Playlist',
                'versions',
                True,
                (0,),
                (0,),
                'remove',
                (True, False),
            ),
            id='W17-08',
        ),
        pytest.param(
            ('Playlist', 'versions', 'Version', 'playlists', True, (), (0,), 'add', (True, False)),
            id='W17-09',
        ),
        pytest.param(
            (
                'PublishedFile',
                'version',
                'Version',
                'published_files',
                False,
                (0,),
                (1,),
                'set',
                (True, True),
            ),
            id='W17-10',
        ),
        pytest.param(
            ('Note', 'note_links', 'Shot', 'notes', True, (), (0,), 'add', (False, False)),
            id='W17-11',
        ),
        pytest.param(
            ('CutItem', 'cut', 'Cut', 'cut_items', False, (0,), (), 'set', (True, False)),
            id='W17-12',
        ),
        pytest.param(
            ('Asset', 'shots', 'Shot', 'assets', True, (0,), (0,), 'remove', (True, False)),
            id='W18-01',
        ),
        pytest.param(
            (
                'Sequence',
                'shots',
                'Shot',
                'sg_sequence',
                True,
                (0,),
                (0,),
                'remove',
                (False, False),
            ),
            id='W18-02',
        ),
        pytest.param(
            ('Task', 'entity', 'Shot', 'tasks', False, (0,), (1,), 'set', (True, True)),
            id='W18-03',
        ),
        pytest.param(
            ('Asset', 'parents', 'Asset', 'assets', True, (), (0,), 'add', (True, False)),
            id='W18-04',
        ),
        pytest.param(
            ('Asset', 'assets', 'Asset', 'parents', True, (), (0,), 'add', (True, False)),
            id='W18-05',
        ),
        pytest.param(
            ('Version', 'playlists', 'Playlist', 'versions', True, (), (0,), 'set', (True, False)),
            id='W18-06',
        ),
        pytest.param(
            (
                'Playlist',
                'versions',
                'Version',
                'playlists',
                True,
                (0,),
                (0,),
                'remove',
                (True, False),
            ),
            id='W18-07',
        ),
        pytest.param(
            ('Note', 'note_links', 'Shot', 'notes', True, (0,), (0,), 'remove', (False, False)),
            id='W18-08',
        ),
        pytest.param(
            ('CutItem', 'cut', 'Cut', 'cut_items', False, (0,), (1,), 'set', (True, True)),
            id='W18-09',
        ),
    ],
)
async def test_pair_side_audits_match_characterization(
    writes_site: FakeSiteCase,
    relationship: tuple[
        str, str, str, str, bool, tuple[int, ...], tuple[int, ...], str, tuple[bool, ...]
    ],
) -> None:
    writer_type, field, target_type, mirror, multi, before, after, mode, touched = relationship
    writer = await _record(writes_site, writer_type)
    targets = [await _record(writes_site, target_type) for _ in range(2)]
    previous = None
    if writer_type == 'Sequence' and mode == 'add':
        previous = await _record(writes_site, 'Sequence')
        await _write_link(writes_site, previous, 'shots', [targets[0]])
    initial = [targets[i] for i in before]
    await _write_link(
        writes_site, writer, field, initial if multi else (initial[0] if initial else None)
    )
    for i, target in enumerate(targets):
        expected = (
            (writer if i in before else previous if i == 0 else None)
            if mirror == 'sg_sequence'
            else ([writer] if i in before else [])
        )
        assert (await _fields(writes_site, target, mirror))[mirror] == expected
    baselines = [
        await _fields(writes_site, target, 'updated_at', 'updated_by') for target in targets
    ]
    for baseline in baselines:
        assert baseline['updated_by'] == _SCRIPT
        assert baseline['updated_at'] == _FROZEN
    previous_audit = (
        await _fields(writes_site, previous, 'updated_at', 'updated_by') if previous else None
    )
    _server(writes_site).set_time(_LATER)
    selected = [targets[i] for i in after]
    response = _raw(
        _server(writes_site),
        'update',
        {
            **writer,
            'fields': [
                {
                    'field_name': field,
                    'value': selected if multi else (selected[0] if selected else None),
                    'multi_entity_update_mode': mode,
                }
            ],
        },
        sudo=True,
    )
    assert 'exception' not in response
    remaining = [i for i in before if i not in after] if mode == 'remove' else after
    assert (await _fields(writes_site, writer, field))[field] == (
        [targets[i] for i in remaining]
        if multi
        else (targets[remaining[0]] if remaining else None)
    )
    for i, target in enumerate(targets):
        actual = await _fields(writes_site, target, mirror, 'updated_at', 'updated_by')
        assert actual[mirror] == (
            (writer if i in remaining else None)
            if mirror == 'sg_sequence'
            else ([writer] if i in remaining else [])
        )
        assert actual['updated_at'] == (_LATER if touched[i] else baselines[i]['updated_at'])
        assert actual['updated_by'] == (_ARTIST if touched[i] else baselines[i]['updated_by'])
    if previous:
        assert (await _fields(writes_site, previous, 'shots'))['shots'] == []
        assert await _fields(writes_site, previous, 'updated_at', 'updated_by') == previous_audit


@pytest.mark.anyio
async def test_pair_controls_leave_unobserved_directions_and_missing_targets_alone(
    writes_site: FakeSiteCase,
) -> None:
    asset = await _record(writes_site, 'Asset')
    task = await _record(writes_site, 'Task')
    baseline = await _fields(
        writes_site, asset, 'shots', 'parents', 'assets', 'updated_at', 'updated_by'
    )
    _server(writes_site).set_time(_LATER)
    await _write_link(writes_site, task, 'entity', asset)
    note = await _record(writes_site, 'Note')
    await _write_link(writes_site, note, 'note_links', [asset])
    assert (
        await _fields(writes_site, asset, 'shots', 'parents', 'assets', 'updated_at', 'updated_by')
        == baseline
    )
    shot = await _record(writes_site, 'Shot')
    await _write_link(writes_site, shot, 'tasks', [task])
    assert (await _fields(writes_site, task, 'entity'))['entity'] == asset
    missing = {'type': 'Asset', 'id': 10000}
    await _write_link(writes_site, shot, 'assets', [missing])
    assert (await _fields(writes_site, shot, 'assets'))['assets'] == [missing]


@pytest.mark.anyio
async def test_pair_control_schema_without_pairs_stays_single_sided(
    fake_site: FakeSiteCase,
) -> None:
    user = await _record(fake_site, 'HumanUser')
    shot = await _record(fake_site, 'Shot')
    baseline = await _fields(fake_site, user, 'login', 'project')
    await _write_link(fake_site, shot, 'reviewers', [user])
    assert await _fields(fake_site, user, 'login', 'project') == baseline
    assert (await _fields(fake_site, shot, 'reviewers'))['reviewers'] == [user]


@pytest.mark.anyio
async def test_pair_guard_invalid_write_changes_neither_side(writes_site: FakeSiteCase) -> None:
    shot = await _record(writes_site, 'Shot')
    asset = await _record(writes_site, 'Asset')
    baseline = await _fields(writes_site, asset, 'shots', 'updated_at', 'updated_by')
    with pytest.raises(Fault, match=r'code.*not a str'):
        await writes_site.call('update', 'Shot', shot['id'], {'assets': [asset], 'code': 42})
    assert (await _fields(writes_site, shot, 'assets'))['assets'] == []
    assert await _fields(writes_site, asset, 'shots', 'updated_at', 'updated_by') == baseline
