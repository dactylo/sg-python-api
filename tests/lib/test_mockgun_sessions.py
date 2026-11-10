import json
import pickle
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import AuthenticationFault, Fault
from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
from shotgun_api3.lib.mockgun._errors import MockgunError
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._server import FakeServer
from shotgun_api3.lib.mockgun._sessions import RequestContext, _Actor
from shotgun_api3.lib.mockgun._sync_client import Shotgun
from tests.conftest import entity_id
from tests.lib._requests import raw_request

from .conftest import QUERY_NOW, QUERY_SCHEMA, SCHEMA, FakeSiteCase, raw_read

_CREDENTIAL = 'arbitrary-credential'
_SESSION_VALUE = 'unknown'
_FROZEN = datetime(2025, 2, 3, 4, 5, 6, tzinfo=UTC)


def _field(data_type: str) -> dict[str, object]:
    return {
        'data_type': {'value': data_type},
        'properties': {'default_value': {'value': None}},
    }


def _rpc(
    server: FakeServer,
    auth: dict[str, object],
    method: str = 'get_session_token',
    params: object = None,
) -> object:
    values: list[object] = [auth]
    if params is not None:
        values.append(params)
    response = raw_request(
        server,
        'POST',
        'http://unit-test.example/api3/json',
        body=json.dumps({'method_name': method, 'params': values}).encode(),
    )
    return json.loads(response.content)


def _actor_schema(*, firstname: bool = True, login: bool = True) -> dict[str, dict[str, object]]:
    schema = cast('dict[str, dict[str, object]]', deepcopy(SCHEMA))
    schema['ApiUser'] = {'firstname': _field('text')} if firstname else {}
    schema['HumanUser']['name'] = _field('text')
    if not login:
        del schema['HumanUser']['login']
    for entity_type in ('ApiUser', 'HumanUser'):
        for field in ('created_at', 'updated_at'):
            schema[entity_type][field] = _field('date_time')
        for field in ('created_by', 'updated_by'):
            schema[entity_type][field] = _field('entity')
    return schema


def _server() -> FakeServer:
    return FakeServer(SiteSchema(cast('dict[str, dict[str, object]]', deepcopy(SCHEMA)), {}))


@pytest.mark.anyio
async def test_authentication_rejects_unknown_login_before_schema_lookup(
    fake_site: FakeSiteCase,
) -> None:
    assert await fake_site.call('authenticate_human_user', 'unknown', 'any-password') is None
    await fake_site.call('create', 'HumanUser', {'login': 'existing'})
    with pytest.raises(Fault, match=r"HumanUser\.sg_status_list doesn't exist"):
        await fake_site.call('authenticate_human_user', 'existing', 'any-password')


@pytest.mark.anyio
async def test_authentication_requires_active_existing_user(
    fake_site: FakeSiteCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = cast('dict[str, dict[str, object]]', deepcopy(SCHEMA))
    schema['HumanUser']['name'] = {
        'data_type': {'value': 'text'},
        'properties': {'default_value': {'value': None}},
    }
    schema['HumanUser']['sg_status_list'] = {
        'data_type': {'value': 'status_list'},
        'properties': {'default_value': {'value': None}},
    }
    path = tmp_path / 'active-schema.pickle'
    path.write_bytes(pickle.dumps(schema, protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(path))
    client = type(fake_site.client)(
        'http://unit-test.example',
        login='artist',
        password=_CREDENTIAL,
    )
    case = FakeSiteCase(fake_site.kind, client)
    try:
        user = cast(
            'dict[str, object]',
            await case.call(
                'find_one',
                'HumanUser',
                [],
                ['login', 'name', 'sg_status_list'],
            ),
        )
        assert user == {
            'type': 'HumanUser',
            'id': 1,
            'login': 'artist',
            'name': 'artist',
            'sg_status_list': 'act',
        }
        expected = {'type': 'HumanUser', 'id': 1, 'login': 'artist'}
        assert (
            await case.call(
                'authenticate_human_user',
                'artist',
                'different-password',
            )
            == expected
        )
        await case.call('update', 'HumanUser', 1, {'sg_status_list': 'dis'})
        assert await case.call('authenticate_human_user', 'artist', 'anything') is None
    finally:
        await case.call('close')


def test_bootstrapped_actor_audit_fields_use_one_request_time() -> None:
    server = FakeServer(SiteSchema(_actor_schema(), {}))
    server.set_time(_FROZEN)
    _rpc(server, {'script_name': 'bench', 'script_key': 'key'})
    _rpc(server, {'script_name': 'bench', 'script_key': 'key', 'sudo_as_login': 'artist'})
    for entity_type in ('ApiUser', 'HumanUser'):
        rows = server.store.rows(entity_type, retired=False)
        assert len(rows) == 1
        assert rows[0]['created_at'] == rows[0]['updated_at'] == _FROZEN
        assert rows[0]['created_by'] is rows[0]['updated_by'] is None


@pytest.mark.anyio
@pytest.mark.parametrize('auth_token', [None, 'any-token'])
async def test_constructor_login_and_token_exchange(
    fake_site: FakeSiteCase,
    auth_token: str | None,
) -> None:
    client = type(fake_site.client)(
        'http://unit-test.example',
        login='artist',
        password=_CREDENTIAL,
        auth_token=auth_token,
    )
    case = FakeSiteCase(fake_site.kind, client)
    try:
        expected = [{'type': 'HumanUser', 'id': 1, 'login': 'artist'}]
        assert await case.call('find', 'HumanUser', [], ['login']) == expected
        assert await case.call('find', 'HumanUser', [], ['login']) == expected
        if auth_token is not None:
            assert cast('str', client.config.session_token).startswith('mockgun-session-')
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_human_sudo_is_explicitly_unsupported(fake_site: FakeSiteCase) -> None:
    cls = type(fake_site.client)
    client = cls(
        'http://unit-test.example',
        login='artist',
        password=_CREDENTIAL,
        sudo_as_login='other',
    )
    case = FakeSiteCase(fake_site.kind, client)
    try:
        with pytest.raises(
            MockgunError, match=r'^mockgun does not emulate sudo with human credentials yet$'
        ):
            await case.call('find', 'HumanUser', [])
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_issued_session_token_sudo_is_authentication_fault(
    fake_site: FakeSiteCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _server()
    issued = cast('dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'key'}))
    client = type(fake_site.client)(
        'http://unit-test.example',
        session_token=cast('str', issued['session_id']),
        sudo_as_login='artist',
        connect=False,
    )
    monkeypatch.setattr(client, '_server', server)
    case = FakeSiteCase(fake_site.kind, client)
    try:
        with pytest.raises(
            AuthenticationFault, match='can only be used with script authentication'
        ):
            await case.call('find', 'HumanUser', [])
        assert server.store.rows('HumanUser', retired=False) == []
    finally:
        await case.call('close')


def test_schema_without_human_users_keeps_per_type_internal_identities() -> None:
    server = FakeServer(SiteSchema({}, {}))
    server.admit_login('artist')
    script = cast('dict[str, object]', _rpc(server, {'script_name': 'script', 'script_key': ''}))
    human = cast('dict[str, object]', _rpc(server, {'user_login': 'artist', 'user_password': ''}))
    assert script['user_type'] == 'ApiUser'
    assert human['user_type'] == 'HumanUser'
    assert script['user_id'] == human['user_id'] == 1
    assert _rpc(server, {'user_login': 'unknown', 'user_password': ''}) == {
        'exception': True,
        'message': 'Authentication failed.',
        'error_code': 102,
    }
    token_user = cast('dict[str, object]', _rpc(server, {'session_token': 'unknown'}))
    assert token_user['user_type'] == 'HumanUser'
    assert token_user['user_id'] == 2
    replay = cast('dict[str, object]', _rpc(server, {'session_token': 'unknown'}))
    assert replay['user_id'] == token_user['user_id']


def test_script_bootstraps_one_api_user_across_keys_and_token_exchange() -> None:
    server = FakeServer(SiteSchema(_actor_schema(), {}))
    first = cast(
        'dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'first'})
    )
    second = cast(
        'dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'second'})
    )
    rows = server.store.rows('ApiUser', retired=False)
    assert len(rows) == 1
    assert rows[0]['firstname'] == 'bench'
    assert first['user_type'] == second['user_type'] == 'ApiUser'
    assert first['user_id'] == second['user_id'] == rows[0]['id']


def test_script_without_firstname_uses_one_site_created_api_user() -> None:
    server = FakeServer(SiteSchema(_actor_schema(firstname=False), {}))
    first = cast(
        'dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'first'})
    )
    second = cast(
        'dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'second'})
    )
    assert first['user_id'] == second['user_id']
    assert len(server.store.rows('ApiUser', retired=False)) == 1


@pytest.mark.anyio
async def test_unknown_session_token_sudo_is_authentication_fault(
    fake_site: FakeSiteCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _server()
    client = type(fake_site.client)(
        'http://unit-test.example',
        session_token=_SESSION_VALUE,
        sudo_as_login='artist',
        connect=False,
    )
    monkeypatch.setattr(client, '_server', server)
    case = FakeSiteCase(fake_site.kind, client)
    try:
        with pytest.raises(
            AuthenticationFault, match='can only be used with script authentication'
        ):
            await case.call('find', 'HumanUser', [])
        assert server.store.rows('HumanUser', retired=False) == []
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_script_sudo_creates_acting_human(fake_site: FakeSiteCase) -> None:
    client = type(fake_site.client)(
        'http://unit-test.example',
        'script',
        'anything',
        sudo_as_login='artist',
    )
    case = FakeSiteCase(fake_site.kind, client)
    try:
        assert await case.call('find', 'HumanUser', [], ['login']) == [
            {'type': 'HumanUser', 'id': 1, 'login': 'artist'},
        ]
        assert cast('str', await case.call('get_session_token')).startswith('mockgun-session-')
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_script_token_survives_exchange_without_human_record(
    fake_site: FakeSiteCase,
) -> None:
    token = await fake_site.call('get_session_token')
    assert isinstance(token, str)
    assert token.startswith('mockgun-session-')
    assert await fake_site.call('get_session_token') == token
    assert await fake_site.call('find', 'HumanUser', []) == []


@pytest.mark.anyio
async def test_unknown_session_token_creates_one_placeholder(fake_site: FakeSiteCase) -> None:
    client = type(fake_site.client)('http://unit-test.example', session_token=_SESSION_VALUE)
    case = FakeSiteCase(fake_site.kind, client)
    try:
        expected = [{'type': 'HumanUser', 'id': 1, 'login': 'mockgun-session-user-1'}]
        assert await case.call('find', 'HumanUser', [], ['login']) == expected
        assert await case.call('find', 'HumanUser', [], ['login']) == expected
    finally:
        await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_script_client_finds_its_declared_api_user(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'script-schema.pickle'
    entity_path = tmp_path / 'script-entities.pickle'
    path.write_bytes(pickle.dumps(_actor_schema(), protocol=2))
    entity_path.write_bytes(pickle.dumps({}, protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    cls = Shotgun if kind == 'sync' else AsyncShotgun
    case = FakeSiteCase(kind, cls('http://unit-test.example', 'bench', _CREDENTIAL))
    try:
        users = await case.call('find', 'ApiUser', [], ['firstname'])
        assert users == [{'type': 'ApiUser', 'id': 1, 'firstname': 'bench'}]
    finally:
        await case.call('close')


@pytest.mark.parametrize('retired', [False, True])
def test_script_matches_existing_api_user_by_firstname_even_when_retired(retired: bool) -> None:
    server = FakeServer(SiteSchema(_actor_schema(), {}))
    context = RequestContext(None, _FROZEN)
    server.store.create('ApiUser', {'firstname': 'other'}, context=context)
    row = server.store.create('ApiUser', {'firstname': 'bench'}, context=context)
    if retired:
        server.store.retire('ApiUser', entity_id(row))
    response = cast(
        'dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'key'})
    )
    assert response['user_type'] == 'ApiUser'
    assert response['user_id'] == row['id']
    assert row['id'] == 2
    assert (
        len(server.store.rows('ApiUser', retired=False))
        + len(server.store.rows('ApiUser', retired=True))
        == 2
    )


@pytest.mark.parametrize('method', ['get_session_token', 'batch'])
def test_request_reads_clock_and_resolves_actor_once(
    method: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = FakeServer(SiteSchema(_actor_schema(), {}))
    clock_calls = 0
    resolve_calls = 0
    original_resolve = server.sessions.resolve

    def read_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return _FROZEN

    def resolve(auth: dict[str, object], *, now: datetime) -> object:
        nonlocal resolve_calls
        resolve_calls += 1
        assert now == _FROZEN
        return original_resolve(auth, now=now)

    monkeypatch.setattr(server, 'now', read_clock)
    monkeypatch.setattr(server.sessions, 'resolve', resolve)
    params = [
        {
            'request_type': 'create',
            'type': 'Project',
            'fields': [{'field_name': 'name', 'value': 'test'}],
            'return_fields': ['id'],
        }
    ]
    response = _rpc(server, {'script_name': 'bench', 'script_key': 'key'}, method, params)
    assert isinstance(response, dict)
    assert 'exception' not in response
    assert clock_calls == resolve_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('credential', ['login', 'session', 'sudo'])
async def test_loginless_human_schema_reuses_actor_across_calls(
    kind: str,
    credential: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'loginless-schema.pickle'
    entity_path = tmp_path / 'loginless-entities.pickle'
    path.write_bytes(pickle.dumps(_actor_schema(login=False), protocol=2))
    entity_path.write_bytes(pickle.dumps({}, protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    cls = Shotgun if kind == 'sync' else AsyncShotgun
    if credential == 'login':
        client = cls('http://unit-test.example', login='artist', password=_CREDENTIAL)
    elif credential == 'session':
        client = cls('http://unit-test.example', session_token=_SESSION_VALUE)
    else:
        client = cls(
            'http://unit-test.example',
            script_name='bench',
            api_key=_CREDENTIAL,
            sudo_as_login='artist',
        )
    case = FakeSiteCase(kind, client)
    try:
        first = cast('list[dict[str, object]]', await case.call('find', 'HumanUser', [], ['name']))
        second = cast(
            'list[dict[str, object]]', await case.call('find', 'HumanUser', [], ['name'])
        )
        assert first == second
        assert len(first) == 1
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_new_client_requires_credentials(fake_site: FakeSiteCase) -> None:
    with pytest.raises(ValueError, match='no authentication credentials provided'):
        fake_site.client.new_client()


@pytest.mark.anyio
async def test_construction_without_credentials_acts_as_default_script(
    fake_site: FakeSiteCase,
) -> None:
    # Earlier mockgun versions accepted a base URL alone, and existing tests rely on it.
    client = type(fake_site.client)('http://unit-test.example')
    case = FakeSiteCase(fake_site.kind, client)
    try:
        assert (client.config.script_name, client.config.api_key) == ('mockgun', 'mockgun')
        shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'S'}))
        assert await case.call('find', 'Shot', [], ['code']) == [
            {'type': 'Shot', 'id': shot['id'], 'code': 'S'}
        ]
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_construction_rejects_incomplete_credentials(
    fake_site: FakeSiteCase,
) -> None:
    with pytest.raises(ValueError, match='script_name provided without api_key'):
        type(fake_site.client)('http://unit-test.example', script_name='only')


@pytest.mark.parametrize('sudo', [False, True])
def test_request_context_records_actual_sudo_script_and_request_time(sudo: bool) -> None:
    server = FakeServer(SiteSchema(cast('dict[str, dict[str, object]]', QUERY_SCHEMA), {}))
    server.set_time(QUERY_NOW)
    assert server.store.context is None
    auth = {'script_name': 'queries', 'script_key': 'k'}
    if sudo:
        auth['sudo_as_login'] = 'artist'
    result = raw_read(server, 'Project', [], auth=auth)
    assert cast('dict[str, object]', result['results'])['entities'] == []
    context = server.store.context
    assert context is not None
    assert context.now == QUERY_NOW
    if sudo:
        assert context.actual == _Actor('ApiUser', 1, 'queries')
        assert context.actor == _Actor('HumanUser', 1, 'artist')
        assert server.store.rows('ApiUser', retired=False)[0]['firstname'] == 'queries'
    else:
        assert context.actual is None
        assert context.actor == _Actor('ApiUser', 1, 'queries')


def test_wire_issued_session_token_sudo_is_authentication_fault() -> None:
    server = _server()
    issued = cast('dict[str, object]', _rpc(server, {'script_name': 'bench', 'script_key': 'key'}))
    auth = {'session_token': issued['session_id'], 'sudo_as_login': 'artist'}
    expected = {
        'exception': True,
        'message': 'The sudo_as_login option can only be used with script authentication.',
        'error_code': 102,
    }
    assert _rpc(server, auth) == expected
    assert _rpc(server, {'session_token': _SESSION_VALUE, 'sudo_as_login': 'artist'}) == expected
    assert server.store.rows('HumanUser', retired=False) == []


def test_wire_tokens_preserve_script_and_sudo_actor_identity() -> None:
    server = _server()
    script: dict[str, object] = {'script_name': 'script', 'script_key': 'any'}
    first = cast('dict[str, object]', _rpc(server, script))
    assert first == {'session_id': 'mockgun-session-1', 'user_type': 'ApiUser', 'user_id': 1}
    second = cast('dict[str, object]', _rpc(server, {**script, 'script_key': 'different'}))
    assert second['user_id'] == first['user_id']
    replay = cast('dict[str, object]', _rpc(server, {'session_token': first['session_id']}))
    assert replay['user_type'] == 'ApiUser'
    assert replay['user_id'] == first['user_id']
    sudo = cast('dict[str, object]', _rpc(server, {**script, 'sudo_as_login': 'artist'}))
    replay_sudo = cast('dict[str, object]', _rpc(server, {'session_token': sudo['session_id']}))
    assert sudo['user_type'] == replay_sudo['user_type'] == 'HumanUser'
    assert sudo['user_id'] == replay_sudo['user_id'] == 1
    assert len(server.store.rows('HumanUser', retired=False)) == 1


def test_wire_unknown_login_is_authentication_fault() -> None:
    assert _rpc(_server(), {'user_login': 'unknown', 'user_password': 'any'}) == {
        'exception': True,
        'message': 'Authentication failed.',
        'error_code': 102,
    }
