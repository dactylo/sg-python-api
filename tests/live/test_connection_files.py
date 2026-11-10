from __future__ import annotations

import asyncio
import os
import sys
import urllib.error
import urllib.request
import uuid
from dataclasses import fields
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import AsyncShotgun, Shotgun
from shotgun_api3._config import (
    ProgressEvent,
    RequestEvent,
    ResponseEvent,
    RetryEvent,
    _ObserverEvent,
)
from shotgun_api3._errors import AuthenticationFault, Fault, ShotgunError
from tests.live._images import png
from tests.live._shapes import dictionary, positive_id, require

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from tests.live._verification import LiveCase

pytestmark = [pytest.mark.live, pytest.mark.anyio, pytest.mark.async_required]


def _ca_bundle() -> Path:
    prefix = Path(sys.prefix).resolve()
    project_venv = Path(__file__).resolve().parents[2] / '.venv'
    require(
        prefix == project_venv.resolve(),
        'CA check requires the project .venv interpreter',
        pytrace=False,
    )
    system_bundle = Path('/etc/ssl/cert.pem')
    if system_bundle.is_file():
        return system_bundle
    for name in ('cacert.pem', 'ca-bundle.crt', 'cert.pem'):
        for candidate in prefix.rglob(name):
            if candidate.is_file():
                return candidate
    pytest.fail('CA bundle unavailable at /etc/ssl/cert.pem or in project .venv', pytrace=False)


def _download_with_cookie(handler: urllib.request.HTTPCookieProcessor, url: str) -> bytes:
    opener = urllib.request.build_opener(handler)
    try:
        with opener.open(url, timeout=20) as response:
            status = response.status
            require(status == 200, 'cookie download did not return HTTP 200', pytrace=False)
            return response.read()
    except urllib.error.HTTPError as error:
        if error.code in {429, 503}:
            pytest.fail('live site returned HTTP 429/503; stop live work', pytrace=False)
        pytest.fail('cookie download returned an unexpected HTTP error', pytrace=False)
    except urllib.error.URLError:
        pytest.fail('cookie download failed before an HTTP response', pytrace=False)


async def _invoke(
    client: Shotgun | AsyncShotgun,
    method: str,
    *args: object,
    **kwargs: object,
) -> object:
    function = cast('Callable[..., object]', getattr(client, method))
    if isinstance(client, AsyncShotgun):
        return await cast('Awaitable[object]', function(*args, **kwargs))
    return await asyncio.to_thread(function, *args, **kwargs)


async def _close(client: Shotgun | AsyncShotgun) -> None:
    await _invoke(client, 'close')


async def _human_token(case: LiveCase, login: str, password: str) -> str:
    human_client = case.make_client(login=login, password=password)
    try:
        token = await _invoke(human_client, 'get_session_token')
    finally:
        await _close(human_client)
    require(
        isinstance(token, str) and bool(token),
        'human token exchange returned no token',
        pytrace=False,
    )
    return cast('str', token)


async def test_attachment_download_forms_and_cookie_handler(
    api_case: LiveCase,
    tmp_path: Path,
) -> None:
    case = api_case
    owner = await case.create('Asset', {'project': case.project, 'code': case.namespace + '-file'})
    owner_id = positive_id(owner.get('id'), 'file owner has no integer ID')
    payload = b'live attachment roundtrip\x00\xff'
    source = tmp_path / f'{case.namespace}-source.bin'
    source.write_bytes(payload)
    attachment_id = positive_id(
        await case.call(
            'upload', 'Asset', owner_id, str(source), display_name=case.namespace + '-source.bin'
        ),
        'upload returned no Attachment ID',
    )
    case.track('Attachment', attachment_id)
    reference = {'type': 'Attachment', 'id': attachment_id}
    url = await case.call('get_attachment_download_url', reference)
    require(
        isinstance(url, str) and url.startswith('https://'),
        'Attachment URL was not HTTPS',
        pytrace=False,
    )
    url_string = cast('str', url)
    by_id_url = await case.call('get_attachment_download_url', attachment_id)
    require(
        isinstance(by_id_url, str) and by_id_url.startswith('https://'),
        'Attachment ID URL was not HTTPS',
        pytrace=False,
    )
    require(
        await case.call('get_attachment_download_url', None) is None,
        'None URL form changed',
        pytrace=False,
    )
    require(
        await case.call('get_attachment_download_url', {'url': url_string}) == url_string,
        'URL dictionary form changed',
        pytrace=False,
    )

    for arguments in ((attachment_id,), (reference,), ({'url': url_string},)):
        downloaded = await case.call('download_attachment', *arguments)
        require(downloaded == payload, 'Attachment download bytes differ', pytrace=False)
    require(
        await case.call('download_attachment', None) is None,
        'None download form changed',
        pytrace=False,
    )
    downloaded = await case.call('download_attachment', attachment_id=attachment_id)
    require(downloaded == payload, 'legacy attachment_id download bytes differ', pytrace=False)
    destination = tmp_path / f'{case.namespace}-download.bin'
    result = await case.call('download_attachment', reference, file_path=str(destination))
    require(
        result == str(destination), 'download did not return its destination path', pytrace=False
    )
    require(
        destination.read_bytes() == payload, 'download destination bytes differ', pytrace=False
    )

    handler = await case.call('get_auth_cookie_handler')
    require(
        isinstance(handler, urllib.request.HTTPCookieProcessor),
        'cookie handler has wrong type',
        pytrace=False,
    )
    cookie_bytes = await asyncio.to_thread(
        _download_with_cookie,
        cast('urllib.request.HTTPCookieProcessor', handler),
        url_string,
    )
    require(cookie_bytes == payload, 'cookie handler download bytes differ', pytrace=False)


async def test_connection_metadata_and_reconnect_after_configuration_changes(
    api_case: LiveCase,
) -> None:
    case = api_case
    client = case.client
    project_id = positive_id(case.project['id'], 'test project has no integer ID')
    await case.call('connect')
    raw_info = await case.call('info')
    require(isinstance(raw_info, dict), 'info did not return metadata', pytrace=False)
    info = cast('dict[str, object]', raw_info)
    version = info.get('version')
    require(
        isinstance(version, list) and len(cast('list[object]', version)) >= 3,
        'info has no server version',
        pytrace=False,
    )
    require(
        client.server_caps.version >= (7, 10, 0),
        'server capability floor was not met',
        pytrace=False,
    )
    require(
        client.server_info.get('version') == version,
        'server_info differs from info',
        pytrace=False,
    )
    require(
        client.client_caps.platform in {'linux', 'mac', 'windows'},
        'invalid client platform',
        pytrace=False,
    )
    require(client.config.records_per_page > 0, 'config has no page size', pytrace=False)
    require(client.base_url.startswith('https://'), 'base_url is not HTTPS', pytrace=False)
    require(not client.base_url.endswith('/'), 'base_url has a trailing slash', pytrace=False)

    await case.call('close')
    prior_session_uuid = client.config.session_uuid
    try:
        for operation in ('after-close', 'with-user-agent', 'with-session-uuid', 'reset-agent'):
            if operation == 'with-user-agent':
                client.add_user_agent('live-verification')
            elif operation == 'with-session-uuid':
                client.set_session_uuid(str(uuid.uuid4()))
            elif operation == 'reset-agent':
                client.reset_user_agent()
            found = cast(
                'dict[str, object] | None',
                await case.call('find_one', 'Project', [['id', 'is', project_id]], ['id']),
            )
            require(
                isinstance(found, dict) and found.get('id') == project_id,
                'read failed after configuration change',
                pytrace=False,
            )
    finally:
        client.config.session_uuid = prior_session_uuid
        client.reset_user_agent()

    if case.kind == 'async':
        scoped = case.make_client(connect=True)
        require(
            isinstance(scoped, AsyncShotgun), 'async fixture created a sync client', pytrace=False
        )
        scoped_async = cast('AsyncShotgun', scoped)
        try:
            async with scoped_async as entered:
                require(
                    entered is scoped_async, 'async context returned another client', pytrace=False
                )
                found = await entered.find_one('Project', [['id', 'is', project_id]], ['id'])
                require(
                    isinstance(found, dict) and found.get('id') == project_id,
                    'async context did not connect',
                    pytrace=False,
                )
            found = await scoped_async.find_one('Project', [['id', 'is', project_id]], ['id'])
            require(
                isinstance(found, dict) and found.get('id') == project_id,
                'async context exit did not permit reconnect',
                pytrace=False,
            )
        finally:
            await scoped_async.close()


@pytest.mark.parametrize('failure', ['unknown-field', 'missing-entity'])
async def test_documented_field_and_missing_entity_faults(
    api_case: LiveCase, failure: str
) -> None:
    case = api_case
    if failure == 'unknown-field':
        asset = await case.create(
            'Asset', {'project': case.project, 'code': case.namespace + '-unknown-field'}
        )
        asset_id = positive_id(asset.get('id'), 'unknown-field Asset has no integer ID')
        unknown_field = case.namespace + '_unknown_field'
        with pytest.raises(Fault):
            await case.call(
                'find', 'Asset', [['id', 'is', asset_id], [unknown_field, 'is', 'invalid']], ['id']
            )
        found = await case.call('find', 'Asset', [['id', 'is', asset_id]], [unknown_field])
        require(
            isinstance(found, list) and len(cast('list[object]', found)) == 1,
            'projection did not return the Asset',
            pytrace=False,
        )
        record = dictionary(
            cast('list[object]', found)[0], 'projection returned no dictionary', pytrace=False
        )
        require(
            record.get('id') == asset_id and unknown_field not in record,
            'projection changed the record identity or included an unknown field',
            pytrace=False,
        )
    else:
        with pytest.raises(Fault):
            await case.call('update', 'Asset', -1, {'description': case.namespace})


async def test_explicit_ca_bundle_connects(api_case: LiveCase) -> None:
    case = api_case
    bundle = _ca_bundle()
    client = case.make_client(ca_certs=str(bundle))
    try:
        await _invoke(client, 'connect')
        found = cast(
            'dict[str, object] | None',
            await _invoke(
                client, 'find_one', 'Project', [['id', 'is', case.project['id']]], ['id']
            ),
        )
        require(
            isinstance(found, dict) and found.get('id') == case.project['id'],
            'explicit CA bundle did not authenticate a read',
            pytrace=False,
        )
    finally:
        await _close(client)


async def test_human_authentication_and_session_token_reuse(api_case: LiveCase) -> None:
    case = api_case
    login = os.environ['SG_TEST_HUMAN_LOGIN']
    password = os.environ['SG_TEST_HUMAN_PASSWORD']
    human = dictionary(
        await case.call('authenticate_human_user', login, password),
        'valid human authentication returned no user',
        pytrace=False,
    )
    require(
        human.get('type') == 'HumanUser',
        'authentication returned the wrong entity type',
        pytrace=False,
    )
    positive_id(human.get('id'), 'authenticated human has no integer ID')
    token_string = await _human_token(case, login, password)

    session_client = case.make_client(session_token=token_string)
    try:
        reused = await _invoke(session_client, 'get_session_token')
        require(
            isinstance(reused, str) and reused == token_string,
            'session client did not reuse its token',
            pytrace=False,
        )
        found = cast(
            'dict[str, object] | None',
            await _invoke(
                session_client,
                'find_one',
                'Project',
                [['id', 'is', case.project['id']]],
                ['id'],
            ),
        )
        require(
            isinstance(found, dict) and found.get('id') == case.project['id'],
            'session token did not authenticate a new client',
            pytrace=False,
        )

    finally:
        await _close(session_client)


async def test_human_session_rejects_permission_change(api_case: LiveCase) -> None:
    case = api_case
    login = os.environ['SG_TEST_HUMAN_LOGIN']
    password = os.environ['SG_TEST_HUMAN_PASSWORD']
    human = dictionary(
        await case.call(
            'find_one', 'HumanUser', [['login', 'is', login]], ['id', 'permission_rule_set']
        ),
        'permission test human could not be found',
        pytrace=False,
    )
    human_id = positive_id(human.get('id'), 'permission test human has no integer ID')
    token = await _human_token(case, login, password)
    session_client = case.make_client(session_token=token)
    try:
        with pytest.raises(Fault) as denied:
            await _invoke(
                session_client,
                'update',
                'HumanUser',
                human_id,
                {'permission_rule_set': human.get('permission_rule_set')},
            )
        require(
            not isinstance(denied.value, AuthenticationFault),
            'human permission check failed authentication instead',
            pytrace=False,
        )
        denial_text = str(denied.value).casefold()
        require(
            any(
                phrase in denial_text
                for phrase in ('permission', 'not allowed', 'not permitted', 'access denied')
            ),
            'human update failed for a reason other than permission denial',
            pytrace=False,
        )
    finally:
        await _close(session_client)


async def test_invalid_script_key_raises_authentication_fault(api_case: LiveCase) -> None:
    case = api_case
    invalid_client = case.make_client(api_key=case.namespace + '-invalid-key')
    try:
        with pytest.raises(AuthenticationFault):
            await _invoke(
                invalid_client, 'find_one', 'Project', [['id', 'is', case.project['id']]], ['id']
            )
    finally:
        await _close(invalid_client)


async def test_live_request_response_and_progress_observers(
    api_case: LiveCase,
    tmp_path: Path,
) -> None:
    case = api_case
    owner = await case.create(
        'Asset', {'project': case.project, 'code': case.namespace + '-events'}
    )
    owner_id = positive_id(owner.get('id'), 'observer owner has no integer ID')
    requests: list[_ObserverEvent] = []
    responses: list[_ObserverEvent] = []
    progress: list[_ObserverEvent] = []
    case.client.config.on_request = requests.append
    case.client.config.on_response = responses.append
    case.client.config.on_progress = progress.append
    try:
        payload = b'live observer payload' * 64
        source = tmp_path / f'{case.namespace}-observed.bin'
        source.write_bytes(payload)
        attachment_id = positive_id(
            await case.call('upload', 'Asset', owner_id, str(source)),
            'observed upload returned no ID',
        )
        case.track('Attachment', attachment_id)
        downloaded = await case.call('download_attachment', attachment_id)
        require(downloaded == payload, 'observed transfer bytes differ', pytrace=False)
    finally:
        case.client.config.on_request = None
        case.client.config.on_response = None
        case.client.config.on_progress = None

    request_events = [event for event in requests if isinstance(event, RequestEvent)]
    response_events = [event for event in responses if isinstance(event, ResponseEvent)]
    progress_events = [event for event in progress if isinstance(event, ProgressEvent)]
    require(
        bool(request_events) and bool(response_events) and bool(progress_events),
        'live observers missed a transfer',
        pytrace=False,
    )
    require(
        len(request_events) == len(requests)
        and len(response_events) == len(responses)
        and len(progress_events) == len(progress),
        'live observers received an undocumented event type',
        pytrace=False,
    )
    require(
        all(
            {field.name for field in fields(event)}
            == {'operation', 'route', 'http_method', 'attempt'}
            for event in request_events
        ),
        'request observer exposed undocumented fields',
        pytrace=False,
    )
    require(
        all(
            {field.name for field in fields(event)}
            == {'operation', 'route', 'attempt', 'latency', 'status', 'outcome', 'correlation_id'}
            for event in response_events
        ),
        'response observer exposed undocumented fields',
        pytrace=False,
    )
    require(
        all(
            {field.name for field in fields(event)}
            == {'operation', 'route', 'attempt', 'bytes_transferred', 'total_bytes'}
            for event in progress_events
        ),
        'progress observer exposed undocumented fields',
        pytrace=False,
    )
    require(
        any(event.status == 200 for event in response_events),
        'no successful response event occurred',
        pytrace=False,
    )
    require(
        any(event.bytes_transferred > 0 for event in progress_events),
        'progress never advanced',
        pytrace=False,
    )


async def test_sudo_create_attributes_human_user(api_case: LiveCase) -> None:
    case = api_case
    login = os.environ['SG_TEST_HUMAN_LOGIN']
    human = dictionary(
        await case.call('find_one', 'HumanUser', [['login', 'is', login]], ['id']),
        'sudo target human could not be found',
        pytrace=False,
    )
    human_id = positive_id(human.get('id'), 'sudo target human has no integer ID')
    sudo_client = case.make_client(sudo_as_login=login)
    case.scanned_types.add('Note')
    try:
        created_result = await _invoke(
            sudo_client,
            'create',
            'Note',
            {'project': case.project, 'subject': case.namespace + '-sudo'},
        )
        require(
            isinstance(created_result, dict)
            and isinstance(cast('dict[str, object]', created_result).get('id'), int),
            'sudo create returned no ID',
            pytrace=False,
        )
        note_id = positive_id(
            cast('dict[str, object]', created_result)['id'], 'sudo create returned no ID'
        )
        case.track('Note', note_id)
        fetched = cast(
            'dict[str, object] | None',
            await case.call('find_one', 'Note', [['id', 'is', note_id]], ['created_by']),
        )
        if fetched is None:
            pytest.fail('sudo Note could not be read', pytrace=False)
        creator = fetched.get('created_by')
        require(
            isinstance(creator, dict)
            and cast('dict[str, object]', creator).get('type') == 'HumanUser'
            and cast('dict[str, object]', creator).get('id') == human_id,
            'sudo create did not attribute the human user',
            pytrace=False,
        )
    finally:
        await _close(sudo_client)


@pytest.mark.parametrize('entity_type', ['Asset', 'Task', 'Version'])
async def test_thumbnail_and_filmstrip_uploads_on_supported_entities(
    api_case: LiveCase,
    tmp_path: Path,
    entity_type: str,
) -> None:
    case = api_case
    asset = await case.create(
        'Asset', {'project': case.project, 'code': case.namespace + '-image-owner'}
    )
    if entity_type == 'Asset':
        entity = asset
    elif entity_type == 'Task':
        entity = await case.create(
            'Task',
            {'project': case.project, 'entity': asset, 'content': case.namespace + '-task'},
        )
    else:
        entity = await case.create(
            'Version',
            {'project': case.project, 'entity': asset, 'code': case.namespace + '-version'},
        )
    entity_id = positive_id(entity.get('id'), 'thumbnail owner has no integer ID')
    image = tmp_path / f'{case.namespace}-thumbnail.png'
    strip = tmp_path / f'{case.namespace}-filmstrip.png'
    image.write_bytes(png(64, 64))
    strip.write_bytes(png(480, 135))

    thumbnail_id = positive_id(
        await case.call('upload_thumbnail', entity_type, entity_id, str(image)),
        'thumbnail upload returned no ID',
    )
    case.track('Attachment', thumbnail_id)
    filmstrip_id = positive_id(
        await case.call('upload_filmstrip_thumbnail', entity_type, entity_id, str(strip)),
        'filmstrip upload returned no ID',
    )
    case.track('Attachment', filmstrip_id)

    found = dictionary(
        await case.call(
            'find_one', entity_type, [['id', 'is', entity_id]], ['image', 'filmstrip_image']
        ),
        'thumbnail owner could not be read',
        pytrace=False,
    )
    require(
        isinstance(found.get('image'), str) and cast('str', found['image']).startswith('https://'),
        'uploaded thumbnail did not appear on its entity',
        pytrace=False,
    )
    require(
        isinstance(found.get('filmstrip_image'), str)
        and cast('str', found['filmstrip_image']).startswith('https://'),
        'uploaded filmstrip did not appear on its entity',
        pytrace=False,
    )


async def test_tiny_connection_timeout_reports_one_retry(api_case: LiveCase) -> None:
    case = api_case
    client = case.make_client()
    retries: list[_ObserverEvent] = []
    client.config.timeout_secs = 0.000001
    client.config.max_rpc_attempts = 2
    client.config.rpc_attempt_interval = 0
    client.config.on_retry = retries.append
    try:
        with pytest.raises(ShotgunError):
            await _invoke(
                client, 'find_one', 'Project', [['id', 'is', case.project['id']]], ['id']
            )
        require(
            len(retries) == 1,
            'tiny connection timeout did not report exactly one retry',
            pytrace=False,
        )
        retry = retries[0]
        require(
            isinstance(retry, RetryEvent),
            'retry observer reported the wrong event type',
            pytrace=False,
        )
        retry_event = cast('RetryEvent', retry)
        require(
            retry_event.attempt == 1, 'retry event did not identify first attempt', pytrace=False
        )
        require(
            {field.name for field in fields(retry_event)}
            == {'operation', 'route', 'attempt', 'delay', 'status'},
            'retry observer exposed undocumented fields',
            pytrace=False,
        )
    finally:
        await _close(client)
