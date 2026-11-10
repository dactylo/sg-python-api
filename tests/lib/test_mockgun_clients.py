import inspect
from typing import cast

import pytest

from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
from shotgun_api3.lib.mockgun._errors import MockgunError

from .conftest import FakeSiteCase, seed

_CREDENTIAL = 'arbitrary-credential'
_SESSION_VALUE = 'unknown'


@pytest.mark.anyio
async def test_mockgun_client_validates_connection_settings(fake_site: FakeSiteCase) -> None:
    config = fake_site.client.config
    config.max_connection_idle_secs = 30
    config.tcp_keepalive_timeout_secs = 30
    assert (config.max_connection_idle_secs, config.tcp_keepalive_timeout_secs) == (30, 30)
    with pytest.raises(ValueError, match='tcp_keepalive_timeout_secs'):
        config.tcp_keepalive_timeout_secs = 5
    with pytest.raises(TypeError, match='max_connection_idle_secs'):
        config.max_connection_idle_secs = True


@pytest.mark.anyio
async def test_new_client_shares_records_but_direct_construction_is_isolated(
    fake_site: FakeSiteCase,
) -> None:
    other = fake_site.client.new_client(script_name='second', api_key='k')
    assert isinstance(other, type(fake_site.client))
    shared = FakeSiteCase(fake_site.kind, other)
    independent = FakeSiteCase(
        fake_site.kind, type(fake_site.client)('http://unit-test.example', 'fixture', 'secret')
    )
    try:
        first = await fake_site.call('create', 'Shot', {'code': 'first'})
        assert await shared.call('find', 'Shot', [], ['code']) == [first]
        second = await shared.call('create', 'Shot', {'code': 'second'})
        assert await fake_site.call('find', 'Shot', [], ['code']) == [first, second]
        assert await independent.call('find', 'Shot', [], ['code']) == []
    finally:
        await shared.call('close')
        await independent.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('credential', 'credentials', 'expected_config', 'expected_actor'),
    [
        (
            'login',
            {'login': 'artist', 'password': 'x'},
            (None, None),
            {'type': 'HumanUser', 'id': 1, 'name': 'artist'},
        ),
        (
            'sudo',
            {'sudo_as_login': 'artist'},
            ('queries', 'artist'),
            {'type': 'HumanUser', 'id': 1, 'name': 'artist'},
        ),
        (
            'script',
            {'script_name': 'second', 'api_key': 'k'},
            ('second', None),
            {'type': 'ApiUser', 'id': 1, 'name': 'second'},
        ),
    ],
    ids=['login', 'sudo', 'script'],
)
async def test_new_client_replaces_credentials_or_preserves_script_for_sudo(
    workflow_site: FakeSiteCase,
    credential: str,
    credentials: dict[str, str],
    expected_config: tuple[str | None, str | None],
    expected_actor: dict[str, object],
) -> None:
    seed(
        workflow_site, 'HumanUser', {'login': 'artist', 'name': 'artist', 'sg_status_list': 'act'}
    )
    other = workflow_site.client.new_client(**credentials)
    case = FakeSiteCase(workflow_site.kind, other)
    try:
        shot = cast(
            'dict[str, object]',
            await case.call('create', 'Shot', {'code': credential}, ['created_by']),
        )
        assert (other.config.script_name, other.config.sudo_as_login) == expected_config
        assert shot['created_by'] == expected_actor
    finally:
        await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('convert', [False, True])
async def test_new_client_copies_settings_and_original_constructor_conversion(
    fake_site: FakeSiteCase, convert: bool
) -> None:
    original = type(fake_site.client)(
        'http://unit-test.example', 'fixture', 'secret', convert_datetimes_to_utc=convert
    )
    original.config.timeout_secs = 12
    original.config.max_rpc_attempts = 2
    original.config.rpc_attempt_interval = 0
    original.config.convert_datetimes_to_utc = not convert
    original.add_user_agent('probe/1')
    original.set_session_uuid('9b1f9b49-6b5f-4b8e-8d52-2c2d52a8a0b1')
    original_case = FakeSiteCase(fake_site.kind, original)
    try:
        other = original.new_client(script_name='second', api_key='k')
        case = FakeSiteCase(fake_site.kind, other)
        try:
            assert other.config.timeout_secs == 12
            assert other.config.max_rpc_attempts == 2
            assert other.config.rpc_attempt_interval == 0
            assert other.config.user_agents == original.config.user_agents
            assert other.config.session_uuid is None
            assert other.config.convert_datetimes_to_utc is convert
        finally:
            await case.call('close')
    finally:
        await original_case.call('close')


@pytest.mark.anyio
async def test_new_client_reads_current_metadata_independently(fake_site: FakeSiteCase) -> None:
    await fake_site.call('find', 'Shot', [])
    fake_site.client.site.update_server_metadata({'api_max_entities_per_page': 7})
    other = fake_site.client.new_client(script_name='second', api_key='k')
    case = FakeSiteCase(fake_site.kind, other)
    try:
        await case.call('find', 'Shot', [])
        assert other.server_info['api_max_entities_per_page'] == 7
        assert fake_site.client.server_info['api_max_entities_per_page'] == 500
    finally:
        await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('argument', ['base_url', 'connect'])
async def test_new_client_rejects_noncredential_arguments(
    fake_site: FakeSiteCase, argument: str
) -> None:
    value = 'http://x.example' if argument == 'base_url' else False
    with pytest.raises(TypeError, match=argument):
        fake_site.client.new_client(**{argument: value})


@pytest.mark.anyio
@pytest.mark.parametrize('credential', ['login', 'auth_token', 'session_token'])
async def test_new_client_sudo_retains_non_script_credentials(
    fake_site: FakeSiteCase, credential: str
) -> None:
    cls = type(fake_site.client)
    original = (
        cls('http://unit-test.example', session_token=_SESSION_VALUE, connect=False)
        if credential == 'session_token'
        else cls(
            'http://unit-test.example',
            login='artist',
            password=_CREDENTIAL,
            auth_token='token' if credential == 'auth_token' else None,
            connect=False,
        )
    )
    original_case = FakeSiteCase(fake_site.kind, original)
    try:
        if credential == 'auth_token' and fake_site.kind == 'sync':
            with pytest.raises(MockgunError, match='sudo with human credentials'):
                original.new_client(sudo_as_login='other')
            return
        other = original.new_client(sudo_as_login='other')
        case = FakeSiteCase(fake_site.kind, other)
        try:
            for field in ('user_login', 'user_password', 'auth_token', 'session_token'):
                assert getattr(other.config, field) == getattr(original.config, field)
            assert other.config.sudo_as_login == 'other'
            assert original.config.sudo_as_login is None
        finally:
            await case.call('close')
    finally:
        await original_case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize('fake_site', ['async'], indirect=True)
async def test_async_new_client_is_plain_method_with_working_first_find(
    fake_site: FakeSiteCase,
) -> None:
    assert not inspect.iscoroutinefunction(AsyncShotgun.new_client)
    assert isinstance(fake_site.client, AsyncShotgun)
    other = fake_site.client.new_client(script_name='second', api_key='k')
    assert isinstance(other, AsyncShotgun)
    try:
        assert await other.find('Shot', []) == []
    finally:
        await other.close()
