import asyncio
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple, cast
from urllib.parse import urlsplit
from urllib.request import HTTPCookieProcessor

import pytest

from shotgun_api3 import Fault, ShotgunError, ShotgunFileDownloadError
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id
from tests.lib._events import cursor, delta

from .conftest import QUERY_NOW, FakeSiteCase, raw_call, raw_request, seed, write_upload

pytestmark = pytest.mark.anyio


class _ThumbnailCase(NamedTuple):
    source_image: str | None
    retire: bool
    removed_schema: tuple[str, str | None] | None
    method: str
    missing_id: int | None
    kwargs: dict[str, object]
    reason: str | None


def _control_file() -> dict[str, object]:
    return {
        'url': 'https://mockgun-storage.invalid/control',
        'name': 'Control',
        'content_type': 'text/plain',
        'link_type': 'upload',
        'type': 'Attachment',
        'id': 1,
    }


async def _scene(case: FakeSiteCase) -> tuple[dict[str, object], dict[str, object]]:
    project = seed(case, 'Project', {'name': 'P'})
    note = cast(
        'dict[str, object]',
        await case.call('create', 'Note', {'project': project, 'subject': 's', 'content': 'c'}),
    )
    return project, note


def _url(case: FakeSiteCase, identity: object) -> str:
    row = case.client._server.store.require('Attachment', cast('int', identity))
    return cast('str', cast('dict[str, object]', row['this_file'])['url'])


async def test_ascii_text_field_upload_replaces_unknown_file(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    version = seed(workflow_site, 'Version', {'code': 'V'})
    await workflow_site.call(
        'upload',
        'Version',
        version['id'],
        write_upload(tmp_path, 'a.png', b'png'),
        field_name='sg_uploaded_movie',
    )
    with pytest.raises(MockgunError, match='sg_uploaded_movie'):
        await workflow_site.call(
            'find_one', 'Version', [['id', 'is', version['id']]], ['sg_uploaded_movie']
        )
    identity = await workflow_site.call(
        'upload',
        'Version',
        version['id'],
        write_upload(tmp_path, 'a.txt', b'a'),
        field_name='sg_uploaded_movie',
    )
    row = await workflow_site.call(
        'find_one', 'Version', [['id', 'is', version['id']]], ['sg_uploaded_movie']
    )
    assert isinstance(row, dict)
    file_value = cast('dict[str, object]', row['sg_uploaded_movie'])
    assert file_value['type'] == 'Attachment'
    assert file_value['id'] == identity


async def test_ascii_text_upload_file_dictionary(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    identity = await workflow_site.call(
        'upload',
        'Note',
        note['id'],
        write_upload(tmp_path, 'a.txt', b'a' * 40),
        display_name='Shown',
    )
    row = await workflow_site.call(
        'find_one', 'Attachment', [['id', 'is', identity]], ['this_file']
    )
    assert isinstance(row, dict)
    file_value = cast('dict[str, object]', row['this_file'])
    assert set(file_value) == {'url', 'name', 'content_type', 'link_type', 'type', 'id'}
    assert file_value['name'] == 'Shown'
    assert file_value['content_type'] == 'text/plain'
    assert file_value['link_type'] == 'upload'
    assert file_value['type'] == 'Attachment'
    assert file_value['id'] == identity
    url = urlsplit(cast('str', file_value['url']))
    assert url.scheme == 'https'
    assert url.hostname == 'mockgun-storage.invalid'
    assert str(identity) not in url.path


async def test_attachment_url_and_cookie_controls(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    identity = cast(
        'int',
        await workflow_site.call(
            'upload', 'Note', note['id'], write_upload(tmp_path, 'source.bin', b'hello mockgun')
        ),
    )
    url = f'http://unit-test.example/file_serve/attachment/{identity}'
    assert await workflow_site.call('get_attachment_download_url', identity) == url
    assert (
        await workflow_site.call(
            'get_attachment_download_url', {'type': 'Attachment', 'id': identity}
        )
        == url
    )
    assert await workflow_site.call('get_attachment_download_url', {'url': url}) == url
    assert await workflow_site.call('get_attachment_download_url', None) is None
    handler = await workflow_site.call('get_auth_cookie_handler')
    assert isinstance(handler, HTTPCookieProcessor)
    cookies = list(handler.cookiejar)
    assert len(cookies) == 1
    assert cookies[0].name == '_session_id'
    assert cookies[0].domain == 'unit-test.example'
    response = raw_call(
        workflow_site.client._server,
        'create',
        {
            'type': 'Shot',
            'fields': [{'field_name': 'code', 'value': 'cookie'}],
            'return_fields': ['created_by'],
        },
        auth={'session_token': cookies[0].value},
    )
    result = cast('dict[str, object]', response['results'])
    assert result['created_by'] == {'type': 'ApiUser', 'id': 1, 'name': 'queries'}


async def test_deleted_control_file_preserves_uploaded_filter_fault(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    control = seed(
        workflow_site,
        'Attachment',
        {'this_file': _control_file()},
    )
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call('find', 'Attachment', [['this_file', 'is', None]])
    await workflow_site.call('delete', 'Attachment', control['id'])
    await workflow_site.call('upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a'))
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call('find', 'Attachment', [['this_file', 'is', None]], ['id'])


async def test_field_upload_missing_file_declaration_names_schema_gap(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    version = seed(workflow_site, 'Version', {'code': 'V'})
    workflow_site.client._server.schema.fields['Attachment'].pop('this_file')
    await workflow_site.call(
        'upload',
        'Version',
        version['id'],
        write_upload(tmp_path, 'a.txt', b'a'),
        field_name='sg_uploaded_movie',
    )
    with pytest.raises(
        MockgunError,
        match=r'api_link_file.*Attachment.this_file.*declaration.*missing',
    ):
        await workflow_site.call(
            'find_one', 'Version', [['id', 'is', version['id']]], ['sg_uploaded_movie']
        )


async def test_field_upload_unknowns(workflow_site: FakeSiteCase, tmp_path: Path) -> None:
    version = seed(workflow_site, 'Version', {'code': 'V'})
    identity = await workflow_site.call(
        'upload',
        'Version',
        version['id'],
        write_upload(tmp_path, 'a.txt', b'a'),
        field_name='sg_uploaded_movie',
    )
    assert await workflow_site.call(
        'find_one', 'Attachment', [['id', 'is', identity]], ['attachment_links']
    ) == {
        'type': 'Attachment',
        'id': identity,
        'attachment_links': [{**version, 'name': 'V'}],
    }
    attachment = await workflow_site.call(
        'find_one', 'Attachment', [['id', 'is', identity]], ['this_file']
    )
    uploaded = await workflow_site.call(
        'find_one', 'Version', [['id', 'is', version['id']]], ['sg_uploaded_movie']
    )
    assert isinstance(attachment, dict)
    assert isinstance(uploaded, dict)
    assert uploaded['sg_uploaded_movie'] == attachment['this_file']
    for field in ['sg_uploaded_movie_transcoding_status', 'updated_at']:
        with pytest.raises(MockgunError, match=field):
            await workflow_site.call('find_one', 'Version', [['id', 'is', version['id']]], [field])
    with pytest.raises(MockgunError):
        await workflow_site.call('find', 'EventLogEntry', [])


async def test_file_routes_reject_foreign_host_and_bad_forms(workflow_site: FakeSiteCase) -> None:
    await workflow_site.call('info')
    server = workflow_site.client._server
    route = '/upload/api_get_upload_link_info'
    valid = (
        'filename=a.txt&upload_type=Attachment&multipart_upload=false'
        '&script_name=queries&script_key=key'
    )
    with pytest.raises(MockgunError, match=f'POST {route}'):
        raw_request(server, 'POST', 'https://elsewhere.invalid' + route, body=valid.encode())
    for extra in ('&unexpected=x', '&filename=duplicate'):
        with pytest.raises(MockgunError, match='field'):
            raw_request(
                server, 'POST', 'http://unit-test.example' + route, body=(valid + extra).encode()
            )
    with pytest.raises(MockgunError, match='POST'):
        raw_request(
            server,
            'POST',
            'http://unit-test.example' + route,
            body=valid.replace('&script_key=key', '').encode(),
        )
    assert not server.storage._allocations
    response = raw_request(
        server, 'POST', 'http://unit-test.example/prefix' + route, body=valid.encode()
    )
    assert response.status == 200
    assert response.content.startswith(b'1\nhttps://mockgun-storage.invalid/')


async def test_file_serve_redirect_and_storage_get(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    identity = cast(
        'int',
        await workflow_site.call(
            'upload', 'Note', note['id'], write_upload(tmp_path, 'source.bin', b'hello mockgun')
        ),
    )
    server = workflow_site.client._server
    token = cast('str', await workflow_site.call('get_session_token'))
    url = f'http://unit-test.example/file_serve/attachment/{identity}'
    response = raw_request(server, 'GET', url, headers={'cookie': f'_session_id={token}'})
    assert response.status == 302
    location = response.headers['location']
    assert urlsplit(location).hostname == 'mockgun-storage.invalid'
    storage = raw_request(server, 'GET', location)
    assert storage.status == 200
    assert storage.content == b'hello mockgun'
    assert await workflow_site.call('download_attachment', {'url': location}) == storage.content
    with pytest.raises(MockgunError, match='file_serve/attachment'):
        raw_request(server, 'GET', url)


async def test_image_update_event_plan_unsettled(workflow_site: FakeSiteCase) -> None:
    asset = seed(workflow_site, 'Asset', {'code': 'A1'})
    await workflow_site.call('update', 'Asset', asset['id'], {'image': None})
    with pytest.raises(MockgunError):
        await workflow_site.call('find', 'EventLogEntry', [])


async def test_legacy_upload_control(workflow_site: FakeSiteCase, tmp_path: Path) -> None:
    _, note = await _scene(workflow_site)
    workflow_site.client.site.update_server_metadata(
        {'s3_direct_uploads_enabled': False, 's3_enabled_upload_types': {}}
    )
    await workflow_site.call('info')
    with pytest.raises(ShotgunError, match='legacy form uploads'):
        await workflow_site.call(
            'upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a')
        )
    assert not workflow_site.client._server.storage._blobs


async def test_missing_or_retired_attachment_downloads_fail(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    identity = cast(
        'int',
        await workflow_site.call(
            'upload', 'Note', note['id'], write_upload(tmp_path, 'source.bin', b'hello mockgun')
        ),
    )
    server = workflow_site.client._server
    thumbnail = server.store.reserve_id('Attachment')
    await workflow_site.call('delete', 'Attachment', identity)
    token = cast('str', await workflow_site.call('get_session_token'))
    for missing in (identity, identity + 1000, thumbnail):
        with pytest.raises(ShotgunFileDownloadError, match='attachment download failed'):
            await workflow_site.call('download_attachment', missing)
        response = raw_request(
            server,
            'GET',
            f'http://unit-test.example/file_serve/attachment/{missing}',
            headers={'cookie': f'_session_id={token}'},
        )
        assert response.status == 404
    unbound = cast(
        'dict[str, object]',
        await workflow_site.call('create', 'Attachment', {'display_name': 'x'}),
    )
    with pytest.raises(MockgunError, match='file_serve/attachment'):
        await workflow_site.call('download_attachment', unbound['id'])


async def test_missing_owner_keeps_blob_and_identity(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    path = write_upload(tmp_path, 'a.txt', b'a')
    first = await workflow_site.call('upload', 'Note', note['id'], path)
    storage = workflow_site.client._server.storage
    before = len(storage._blobs)
    with pytest.raises(ShotgunError):
        await workflow_site.call('upload', 'Asset', 10_000_000, path)
    assert len(storage._blobs) == before + 1
    assert len(cast('list[object]', await workflow_site.call('find', 'Attachment', []))) == 1
    second = await workflow_site.call('upload', 'Note', note['id'], path)
    assert second == cast('int', first) + 1


async def test_ordinary_singleton_url_order_control(workflow_site: FakeSiteCase) -> None:
    version = seed(
        workflow_site,
        'Version',
        {'code': 'V', 'sg_uploaded_movie': {'url': 'https://example.invalid/a'}},
    )
    assert await workflow_site.call(
        'find',
        'Version',
        [],
        ['id'],
        order=[{'field_name': 'sg_uploaded_movie', 'direction': 'asc'}],
    ) == [version]


@pytest.mark.parametrize(
    ('filename', 'data'),
    [('a.c', b'a'), ('a.TXT', b'a'), ('a.txt', b'\x80'), ('a.png', b'png')],
)
async def test_other_upload_kinds_keep_file_fields_unknown(
    workflow_site: FakeSiteCase, tmp_path: Path, filename: str, data: bytes
) -> None:
    _, note = await _scene(workflow_site)
    identity = await workflow_site.call(
        'upload', 'Note', note['id'], write_upload(tmp_path, filename, data)
    )
    with pytest.raises(MockgunError, match='this_file'):
        await workflow_site.call('find_one', 'Attachment', [['id', 'is', identity]], ['this_file'])
    version = seed(workflow_site, 'Version', {'code': 'V'})
    await workflow_site.call(
        'upload',
        'Version',
        version['id'],
        write_upload(tmp_path, filename, data),
        field_name='sg_uploaded_movie',
    )
    with pytest.raises(MockgunError, match='sg_uploaded_movie'):
        await workflow_site.call(
            'find_one', 'Version', [['id', 'is', version['id']]], ['sg_uploaded_movie']
        )


@pytest.mark.parametrize('display_name', [None, 'Shown'])
async def test_plain_upload_event_rows(
    workflow_site: FakeSiteCase, tmp_path: Path, display_name: str | None
) -> None:
    _, note = await _scene(workflow_site)
    before = await cursor(workflow_site)
    identity = await workflow_site.call(
        'upload',
        'Note',
        note['id'],
        write_upload(tmp_path, 'a.txt', b'a'),
        display_name=display_name,
    )
    rows = await delta(workflow_site, before, ['event_type', 'attribute_name', 'entity'])
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        ('Shotgun_Attachment_New', None),
        ('Shotgun_Attachment_Change', 'attachment_links'),
        ('Shotgun_Note_Change', 'attachments'),
        *([('Shotgun_Attachment_Change', 'display_name')] if display_name else []),
        ('Shotgun_Attachment_Change', 'project'),
    ]
    assert [cast('dict[str, object]', row['entity'])['id'] for row in rows] == [
        identity,
        identity,
        note['id'],
        *([identity] if display_name else []),
        identity,
    ]
    assert await workflow_site.call(
        'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
    ) == {
        'type': 'EventLogEntry',
        'id': rows[0]['id'],
        'meta': {'type': 'new_entity', 'entity_type': 'Attachment', 'entity_id': identity},
    }
    for row in rows:
        for field in ['project'] + (['meta'] if row != rows[0] else []):
            with pytest.raises(MockgunError, match=field):
                await workflow_site.call(
                    'find_one', 'EventLogEntry', [['id', 'is', row['id']]], [field]
                )


async def test_plain_upload_fields_and_unknown_file(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    project, note = await _scene(workflow_site)
    workflow_site.client.site.set_time(QUERY_NOW + timedelta(seconds=10))
    path = write_upload(tmp_path, 'mgc-plain.txt', b'x' * 40)
    identity = await workflow_site.call(
        'upload',
        'Note',
        note['id'],
        path,
        display_name='Shown',
        created_at=QUERY_NOW - timedelta(days=1),
    )
    assert isinstance(identity, int)
    fields = [
        'filename',
        'original_fname',
        'display_name',
        'created_at',
        'created_by',
        'project',
        'attachment_links',
        'cached_display_name',
    ]
    result = await workflow_site.call('find_one', 'Attachment', [['id', 'is', identity]], fields)
    assert result == {
        'type': 'Attachment',
        'id': identity,
        'filename': 'mgc-plain.txt',
        'original_fname': 'mgc-plain.txt',
        'display_name': 'Shown',
        'cached_display_name': 'Shown',
        'created_at': QUERY_NOW - timedelta(days=1),
        'created_by': {'type': 'ApiUser', 'id': 1, 'name': 'queries'},
        'project': {**project, 'name': 'P'},
        'attachment_links': [{'type': 'Note', 'id': note['id'], 'name': 's - c'}],
    }
    assert await workflow_site.call(
        'find_one', 'Note', [['id', 'is', note['id']]], ['attachments']
    ) == {
        'type': 'Note',
        'id': note['id'],
        'attachments': [{'type': 'Attachment', 'id': identity, 'name': 'Shown'}],
    }
    raw = workflow_site.client._server.store.require('Attachment', identity)['this_file']
    assert isinstance(raw, dict)
    assert raw == {
        'url': _url(workflow_site, identity),
        'name': 'Shown',
        'content_type': 'text/plain',
        'link_type': 'upload',
        'type': 'Attachment',
        'id': identity,
    }
    url = urlsplit(_url(workflow_site, identity))
    assert url.hostname == 'mockgun-storage.invalid'
    assert not any(char.isdigit() for char in url.path)
    assert workflow_site.client._server.storage.read(url.geturl()) == b'x' * 40
    second = await workflow_site.call('upload', 'Note', note['id'], path)
    assert await workflow_site.call(
        'find_one', 'Attachment', [['id', 'is', second]], ['created_at', 'display_name']
    ) == {
        'type': 'Attachment',
        'id': second,
        'created_at': QUERY_NOW + timedelta(seconds=10),
        'display_name': 'mgc-plain.txt',
    }


async def test_raw_filmstrip_update_events_unsettled(workflow_site: FakeSiteCase) -> None:
    asset = seed(workflow_site, 'Asset', {'code': 'A1'})
    result = raw_call(
        workflow_site.client._server,
        'update',
        {
            'type': 'Asset',
            'id': asset['id'],
            'fields': [{'field_name': 'filmstrip_image', 'value': None}],
        },
    )
    assert result['results'] == {**asset, 'filmstrip_image': None}
    with pytest.raises(MockgunError):
        await workflow_site.call('find', 'EventLogEntry', [])


async def test_retired_attachment_keeps_note_writable(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    _, note = await _scene(workflow_site)
    identity = await workflow_site.call(
        'upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a')
    )
    second = seed(workflow_site, 'Note', {'subject': 'second'})
    await workflow_site.call(
        'update', 'Note', second['id'], {'attachments': [{'type': 'Attachment', 'id': identity}]}
    )
    await workflow_site.call('delete', 'Attachment', identity)
    await workflow_site.call('update', 'Note', second['id'], {'subject': 'x'})
    with pytest.raises(MockgunError, match='attachments'):
        await workflow_site.call('find_one', 'Note', [['id', 'is', second['id']]], ['attachments'])


@pytest.mark.parametrize('upload_type', ['Attachment', 'Thumbnail'])
@pytest.mark.parametrize('created_at', ['invalid', '2026-10-21T12:00:00'])
async def test_upload_allocation_audit_fault_names_entity_and_field(
    workflow_site: FakeSiteCase, upload_type: str, created_at: str
) -> None:
    await workflow_site.call('info')
    route = '/upload/api_get_upload_link_info'
    form = (
        f'filename=a.txt&upload_type={upload_type}&multipart_upload=false'
        f'&created_at={created_at}&script_name=queries&script_key=key'
    )
    with pytest.raises(MockgunError, match=f'POST {route} {upload_type}.created_at'):
        raw_request(
            workflow_site.client._server,
            'POST',
            'http://unit-test.example' + route,
            body=form.encode(),
        )


@pytest.mark.parametrize('owner_type', ['Note', 'Version'])
async def test_retired_owner_keeps_attachment(
    workflow_site: FakeSiteCase, tmp_path: Path, owner_type: str
) -> None:
    owner = seed(
        workflow_site, owner_type, {'subject': 's'} if owner_type == 'Note' else {'code': 'V'}
    )
    identity = await workflow_site.call(
        'upload', owner_type, owner['id'], write_upload(tmp_path, 'a.txt', b'a')
    )
    await workflow_site.call('delete', owner_type, owner['id'])
    assert await workflow_site.call('find', 'Attachment', [['id', 'is', identity]]) == [
        {'type': 'Attachment', 'id': identity}
    ]


@pytest.mark.parametrize('variant', ['retired', 'tag_list', 'naive', 'code', 'missing'])
async def test_upload_guards_create_nothing(
    workflow_site: FakeSiteCase, tmp_path: Path, variant: str
) -> None:
    _, note = await _scene(workflow_site)
    kwargs: dict[str, object] = {}
    owner = note
    if variant == 'retired':
        await workflow_site.call('delete', 'Note', note['id'])
    elif variant == 'tag_list':
        kwargs['tag_list'] = 'a'
    elif variant == 'naive':
        kwargs['created_at'] = QUERY_NOW.replace(tzinfo=None)
    else:
        owner = seed(workflow_site, 'Version', {'code': 'V'})
        kwargs['field_name'] = variant
    with pytest.raises(MockgunError):
        await workflow_site.call(
            'upload', owner['type'], owner['id'], write_upload(tmp_path, 'a.txt', b'a'), **kwargs
        )
    assert await workflow_site.call('find', 'Attachment', []) == []


@pytest.mark.parametrize(
    ('method', 'url'),
    [
        ('POST', 'http://unit-test.example/upload/unknown'),
        ('PUT', 'https://mockgun-storage.invalid/never-issued'),
        ('GET', 'https://elsewhere.invalid/x'),
    ],
)
async def test_unknown_file_routes(workflow_site: FakeSiteCase, method: str, url: str) -> None:
    with pytest.raises(MockgunError, match=f'{method} {urlsplit(url).path}'):
        raw_request(workflow_site.client._server, method, url, body=b'x')


@pytest.mark.parametrize(
    ('removed_fields', 'valid_types', 'error'),
    [
        pytest.param(('attachment_links',), None, 'api_link_file', id='missing-link'),
        pytest.param(
            ('this_file', 'filename', 'display_name', 'project'), None, None, id='missing-optional'
        ),
        pytest.param((), ['Note'], 'api_link_file', id='bad-validation'),
    ],
)
async def test_upload_partial_schema_boundaries(
    workflow_site: FakeSiteCase,
    tmp_path: Path,
    removed_fields: tuple[str, ...],
    valid_types: list[str] | None,
    error: str | None,
) -> None:
    server = workflow_site.client._server
    owner = seed(workflow_site, 'Version', {'code': 'V'})
    fields = server.schema.fields['Attachment']
    for name in removed_fields:
        fields.pop(name)
    if valid_types is not None:
        declaration = cast('dict[str, object]', fields['attachment_links'])
        properties = cast('dict[str, object]', declaration['properties'])
        cast('dict[str, object]', properties['valid_types'])['value'] = valid_types
    path = write_upload(tmp_path, 'a.txt', b'a')
    if error is None:
        identity = await workflow_site.call(
            'upload', 'Version', owner['id'], path, field_name='sg_uploaded_movie'
        )
        assert isinstance(identity, int)
        assert server.storage.bound_key(identity) is not None
        with pytest.raises(MockgunError, match='sg_uploaded_movie'):
            await workflow_site.call(
                'find_one', 'Version', [['id', 'is', owner['id']]], ['sg_uploaded_movie']
            )
    else:
        with pytest.raises(MockgunError, match=error):
            await workflow_site.call('upload', 'Version', owner['id'], path)
        assert server.store.rows('Attachment', retired=False) == []
        assert len(server.storage._blobs) == 1


async def test_upload_readiness(workflow_site: FakeSiteCase) -> None:
    await workflow_site.call('info')
    info = workflow_site.client.server_info
    assert info['s3_uploads_enabled'] is True
    assert info['s3_direct_uploads_enabled'] is True
    assert info['s3_enabled_upload_types'] == {'*': ['*']}


async def test_upload_unknown_filter_preserves_url_fault_and_scope(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call('find', 'Attachment', [['this_file', 'is', None]])
    _, note = await _scene(workflow_site)
    control = seed(
        workflow_site,
        'Attachment',
        {
            'this_file': _control_file(),
            'attachment_links': [note],
        },
    )
    seed(workflow_site, 'Note', {'subject': 'control', 'attachments': [control]})
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call(
            'find', 'Note', [['attachments.Attachment.this_file', 'is', None]]
        )
    identity = await workflow_site.call(
        'upload', 'Note', note['id'], write_upload(tmp_path, 'a.txt', b'a')
    )
    with pytest.raises(Fault, match='sg_uploaded_movie url data type cannot be used in a filter'):
        await workflow_site.call('find', 'Version', [['sg_uploaded_movie', 'is', None]])
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call(
            'find', 'Note', [['attachments.Attachment.this_file', 'is', None]]
        )
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call(
            'find', 'Attachment', [['this_file', 'is', None]], retired_only=True
        )
    await workflow_site.call('delete', 'Attachment', identity)
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call(
            'find', 'Note', [['attachments.Attachment.this_file', 'is', None]]
        )
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call('find', 'Attachment', [['this_file', 'is', None]])
    await workflow_site.call('delete', 'Attachment', control['id'])
    with pytest.raises(Fault, match='this_file url data type cannot be used in a filter'):
        await workflow_site.call(
            'find', 'Attachment', [['this_file', 'is', None]], retired_only=True
        )


@pytest.mark.parametrize(
    ('upload', 'operation', 'expected'),
    [
        (('a.txt', b'a'), 'read', (None, None)),
        (('a.txt', b'a'), 'filter', (Fault, 'this_file url data type cannot be used in a filter')),
        (('a.txt', b'a'), 'order', (None, None)),
        (('a.png', b'png'), 'read', (MockgunError, 'this_file')),
        (('a.png', b'png'), 'filter', (MockgunError, 'this_file')),
        (('a.png', b'png'), 'order', (MockgunError, 'this_file')),
        (('a.c', b'a'), 'read', (MockgunError, 'this_file')),
        (('a.c', b'a'), 'filter', (MockgunError, 'this_file')),
        (('a.c', b'a'), 'order', (MockgunError, 'this_file')),
        (('a.TXT', b'a'), 'read', (MockgunError, 'this_file')),
        (('a.TXT', b'a'), 'filter', (MockgunError, 'this_file')),
        (('a.TXT', b'a'), 'order', (MockgunError, 'this_file')),
        (('a.txt', b'\x80'), 'read', (MockgunError, 'this_file')),
        (('a.txt', b'\x80'), 'filter', (MockgunError, 'this_file')),
        (('a.txt', b'\x80'), 'order', (MockgunError, 'this_file')),
    ],
    ids=[
        'a.txt-a-read',
        'a.txt-a-filter',
        'a.txt-a-order',
        'a.png-png-read',
        'a.png-png-filter',
        'a.png-png-order',
        'a.c-a-read',
        'a.c-a-filter',
        'a.c-a-order',
        'a.TXT-a-read',
        'a.TXT-a-filter',
        'a.TXT-a-order',
        'a.txt-non-ascii-read',
        'a.txt-non-ascii-filter',
        'a.txt-non-ascii-order',
    ],
)
async def test_uploaded_file_dictionary_remains_unknown(
    workflow_site: FakeSiteCase,
    tmp_path: Path,
    operation: str,
    upload: tuple[str, bytes],
    expected: tuple[type[Exception] | None, str | None],
) -> None:
    filename, data = upload
    error, reason = expected
    _, note = await _scene(workflow_site)
    identity = await workflow_site.call(
        'upload', 'Note', note['id'], write_upload(tmp_path, filename, data)
    )
    filters = [['this_file', 'is', None]] if operation == 'filter' else []
    selected = ['this_file'] if operation == 'read' else ['id']
    kwargs = (
        {'order': [{'field_name': 'this_file', 'direction': 'asc'}]}
        if operation == 'order'
        else {}
    )
    if error is not None:
        with pytest.raises(error, match=reason):
            await workflow_site.call('find', 'Attachment', filters, selected, **kwargs)
    else:
        rows = cast(
            'list[dict[str, object]]',
            await workflow_site.call('find', 'Attachment', filters, selected, **kwargs),
        )
        assert len(rows) == 1
        assert rows[0]['id'] == identity
        if operation == 'read':
            assert cast('dict[str, object]', rows[0]['this_file'])['type'] == 'Attachment'


@pytest.mark.usefixtures('block_network')
@pytest.mark.parametrize('size', [13, 20_000_001], ids=['plain', 'multipart'])
async def test_download_attachment_forms_and_file(
    workflow_site: FakeSiteCase, tmp_path: Path, size: int
) -> None:
    _, note = await _scene(workflow_site)
    data = b'hello mockgun' if size == 13 else b'x' * size
    identity = cast(
        'int',
        await workflow_site.call(
            'upload', 'Note', note['id'], write_upload(tmp_path, 'source.bin', data)
        ),
    )
    assert workflow_site.client._server.storage.read(_url(workflow_site, identity)) == data
    assert await workflow_site.call('download_attachment', identity) == data
    assert (
        await workflow_site.call('download_attachment', {'type': 'Attachment', 'id': identity})
        == data
    )
    assert await workflow_site.call('download_attachment', attachment_id=identity) == data
    destination = str(tmp_path / 'out.bin')
    assert (
        await workflow_site.call('download_attachment', identity, file_path=destination)
        == destination
    )
    assert await asyncio.to_thread(Path(destination).read_bytes) == data
    assert await workflow_site.call('download_attachment', None) is None


async def _image(case: FakeSiteCase, entity: dict[str, object], field: str = 'image') -> object:
    result = cast(
        'dict[str, object]',
        await case.call('find_one', entity['type'], [['id', 'is', entity['id']]], [field]),
    )
    return result[field]


async def test_clear_image_clears_filmstrip_and_identity(workflow_site: FakeSiteCase) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A1'})
    workflow_site.client._server.store.require('Asset', entity_id(source)).update(
        image='image', filmstrip_image='filmstrip'
    )
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    store = workflow_site.client._server.store
    for field in ('image', 'filmstrip_image'):
        store.side_set('image_identity', ('Asset', entity_id(source), field), 123)
    assert await workflow_site.call('update', 'Asset', source['id'], {'image': None}) == {
        **source,
        'image': None,
    }
    assert await _image(workflow_site, source) is None
    assert await _image(workflow_site, source, 'filmstrip_image') is None
    for field in ('image', 'filmstrip_image'):
        assert store.side_get('image_identity', ('Asset', entity_id(source), field)) is None
    with pytest.raises(
        ShotgunError,
        match=rf'^share_thumbnail failed for entity_type=Asset entity_id={entity_id(target)}$',
    ):
        await workflow_site.call('share_thumbnail', [target], source_entity=source)
    with pytest.raises(MockgunError):
        await workflow_site.call('find', 'EventLogEntry', [])


async def test_share_invalid_later_target_is_atomic(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A1'})
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    await workflow_site.call(
        'upload_thumbnail', 'Asset', source['id'], write_upload(tmp_path, 'mgc-thumb.png', b'png')
    )
    before = workflow_site.client._server.store.snapshot()
    with pytest.raises(MockgunError, match='10000000'):
        await workflow_site.call(
            'share_thumbnail', [target, {'type': 'Asset', 'id': 10_000_000}], source_entity=source
        )
    assert workflow_site.client._server.store.snapshot() == before
    assert await _image(workflow_site, target) is None


async def test_share_thumbnail_null_source_clears_target(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A5'})
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    await workflow_site.call(
        'upload_thumbnail', 'Asset', target['id'], write_upload(tmp_path, 'target.png', b'png')
    )
    with pytest.raises(
        ShotgunError,
        match=rf'^share_thumbnail failed for entity_type=Asset entity_id={entity_id(target)}$',
    ):
        await workflow_site.call('share_thumbnail', [target], source_entity=source)
    assert await _image(workflow_site, target) is None


async def test_share_thumbnail_path(workflow_site: FakeSiteCase, tmp_path: Path) -> None:
    targets = [seed(workflow_site, 'Asset', {'code': name}) for name in ('A3', 'A4')]
    identity = await workflow_site.call(
        'share_thumbnail', targets, thumbnail_path=write_upload(tmp_path, 'mgc-thumb.png', b'png')
    )
    assert isinstance(identity, int)
    images = [await _image(workflow_site, target) for target in targets]
    assert all(isinstance(value, str) for value in images)
    assert urlsplit(cast('str', images[0])).path == urlsplit(cast('str', images[1])).path


async def test_share_thumbnail_source_identity_and_repeat(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A1'})
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    path = write_upload(tmp_path, 'mgc-thumb.png', b'png')
    identity = await workflow_site.call('upload_thumbnail', 'Asset', source['id'], path)
    await workflow_site.call('upload_thumbnail', 'Asset', target['id'], path)
    for _ in range(2):
        assert (
            await workflow_site.call('share_thumbnail', [target], source_entity=source) == identity
        )
        left, right = await _image(workflow_site, source), await _image(workflow_site, target)
        assert isinstance(left, str)
        assert isinstance(right, str)
        assert urlsplit(left).path == urlsplit(right).path
        assert left != right
    for field in ('updated_at', 'updated_by'):
        with pytest.raises(MockgunError, match=field):
            await _image(workflow_site, target, field)
    with pytest.raises(MockgunError, match='identity'):
        await workflow_site.call('share_thumbnail', [source], source_entity=target)
    with pytest.raises(MockgunError, match='filmstrip_thumbnail'):
        await workflow_site.call(
            'share_thumbnail', [target], source_entity=source, filmstrip_thumbnail=True
        )


async def test_share_thumbnail_taints_previously_readable_events(
    workflow_site: FakeSiteCase,
) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A1'})
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    server = workflow_site.client._server
    server.store.require('Asset', entity_id(source))['image'] = (
        'https://mockgun-storage.invalid/source-image'
    )
    server.store.side_set('image_identity', ('Asset', entity_id(source), 'image'), 123)
    assert await workflow_site.call('find', 'EventLogEntry', [])
    assert await workflow_site.call('share_thumbnail', [target], source_entity=source) == 123
    assert isinstance(await _image(workflow_site, target), str)
    with pytest.raises(MockgunError, match='/upload/share_thumbnail'):
        await workflow_site.call('find', 'EventLogEntry', [])


async def test_thumbnail_create_and_update_paths(
    workflow_site: FakeSiteCase, tmp_path: Path
) -> None:
    path = write_upload(tmp_path, 'mgc-thumb.png', b'png')
    shot = cast(
        'dict[str, object]',
        await workflow_site.call('create', 'Shot', {'code': 's', 'image': path}),
    )
    assert isinstance(shot['image'], str)
    assert shot['image'].startswith('https://mockgun-storage.invalid/')
    result = await workflow_site.call('update', 'Shot', shot['id'], {'filmstrip_image': path})
    filmstrip = await _image(workflow_site, shot, 'filmstrip_image')
    assert isinstance(filmstrip, str)
    assert filmstrip.startswith('https://mockgun-storage.invalid/')
    assert result == {'type': 'Shot', 'id': shot['id'], 'filmstrip_image': filmstrip}


@pytest.mark.parametrize(
    'case',
    [
        pytest.param(
            _ThumbnailCase(
                source_image='direct',
                retire=False,
                removed_schema=None,
                method='share_thumbnail',
                missing_id=None,
                kwargs={'filmstrip_thumbnail': True},
                reason=None,
            ),
            id='filmstrip',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image='direct',
                retire=False,
                removed_schema=None,
                method='share_thumbnail',
                missing_id=None,
                kwargs={'filmstrip_thumbnail': False},
                reason=None,
            ),
            id='direct-source',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image=None,
                retire=False,
                removed_schema=None,
                method='upload_thumbnail',
                missing_id=10000000,
                kwargs={},
                reason=None,
            ),
            id='missing',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image=None,
                retire=False,
                removed_schema=None,
                method='upload_thumbnail',
                missing_id=None,
                kwargs={'display_name': 'x'},
                reason=None,
            ),
            id='display_name',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image=None,
                retire=True,
                removed_schema=None,
                method='upload_thumbnail',
                missing_id=None,
                kwargs={},
                reason='missing or retired owner or image declaration is unsettled',
            ),
            id='retired-owner',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image=None,
                retire=False,
                removed_schema=('Asset', 'image'),
                method='upload_thumbnail',
                missing_id=None,
                kwargs={},
                reason='missing or retired owner or image declaration is unsettled',
            ),
            id='undeclared-image',
        ),
        pytest.param(
            _ThumbnailCase(
                source_image=None,
                retire=False,
                removed_schema=('Attachment', None),
                method='upload_thumbnail',
                missing_id=None,
                kwargs={},
                reason='Attachment schema is missing',
            ),
            id='undeclared-attachment',
        ),
    ],
)
async def test_thumbnail_guards(
    workflow_site: FakeSiteCase, tmp_path: Path, case: _ThumbnailCase
) -> None:
    source = seed(workflow_site, 'Asset', {'code': 'A1'})
    server = workflow_site.client._server
    if case.source_image is not None:
        server.store.require('Asset', entity_id(source))['image'] = case.source_image
    target = seed(workflow_site, 'Asset', {'code': 'A3'})
    if case.retire:
        await workflow_site.call('delete', 'Asset', source['id'])
    if case.removed_schema is not None:
        entity_type, field = case.removed_schema
        if field is None:
            server.schema.fields.pop(entity_type)
        else:
            server.schema.fields[entity_type].pop(field)
    if case.method == 'share_thumbnail':
        args: tuple[object, ...] = ([target],)
        kwargs = {'source_entity': source, **case.kwargs}
    else:
        args = (
            'Asset',
            source['id'] if case.missing_id is None else case.missing_id,
            write_upload(tmp_path, 'mgc-thumb.png', b'png'),
        )
        kwargs = case.kwargs
    with pytest.raises(MockgunError, match=case.reason):
        await workflow_site.call(case.method, *args, **kwargs)
    assert server.store.require('Asset', entity_id(source))['image'] == case.source_image
    assert server.store.require('Asset', entity_id(target))['image'] is None
    if case.removed_schema != ('Attachment', None):
        assert await workflow_site.call('find', 'Attachment', []) == []


@pytest.mark.parametrize('target_state', ['empty', 'filmstrip', 'image-and-filmstrip'])
async def test_null_source_share_preserves_unobserved_target_state(
    workflow_site: FakeSiteCase, tmp_path: Path, target_state: str
) -> None:
    case = workflow_site
    source = seed(case, 'Asset', {'code': 'empty-source'})
    target = seed(case, 'Asset', {'code': 'target'})
    path = write_upload(tmp_path, 'target.png', b'png')
    if target_state == 'image-and-filmstrip':
        await case.call('upload_thumbnail', 'Asset', target['id'], path)
    if target_state != 'empty':
        await case.call('upload_filmstrip_thumbnail', 'Asset', target['id'], path)
    fields = ['image', 'filmstrip_image']
    before = await case.call('find_one', 'Asset', [['id', 'is', target['id']]], fields)
    snapshot = case.client._server.store.snapshot()
    with pytest.raises(
        ShotgunError,
        match=rf'^share_thumbnail failed for entity_type=Asset entity_id={entity_id(target)}$',
    ):
        await case.call('share_thumbnail', [target], source_entity=source)
    assert case.client._server.store.snapshot() == snapshot
    assert await case.call('find_one', 'Asset', [['id', 'is', target['id']]], fields) == before


async def test_thumbnail_id_gap_and_filmstrip(workflow_site: FakeSiteCase, tmp_path: Path) -> None:
    asset = seed(workflow_site, 'Asset', {'code': 'A1'})
    path = write_upload(tmp_path, 'mgc-thumb.png', b'png')
    identity = await workflow_site.call('upload_thumbnail', 'Asset', asset['id'], path)
    assert isinstance(identity, int)
    for retired in (False, True):
        assert (
            await workflow_site.call(
                'find', 'Attachment', [['id', 'is', identity]], retired_only=retired
            )
            == []
        )
    image = await _image(workflow_site, asset)
    assert isinstance(image, str)
    assert image.startswith('https://mockgun-storage.invalid/')
    assert '/images/status/transient/' not in image
    with pytest.raises(MockgunError):
        await workflow_site.call('find', 'EventLogEntry', [])
    assert await workflow_site.call('upload', 'Asset', asset['id'], path) == identity + 1
    assert (
        await workflow_site.call('upload_filmstrip_thumbnail', 'Asset', asset['id'], path)
        == identity + 2
    )
    filmstrip = await _image(workflow_site, asset, 'filmstrip_image')
    assert isinstance(filmstrip, str)
    assert filmstrip.startswith('https://mockgun-storage.invalid/')
    assert await _image(workflow_site, asset) == image
    for field in ('updated_at', 'updated_by'):
        with pytest.raises(MockgunError, match=field):
            await _image(workflow_site, asset, field)
    assert await workflow_site.call('update', 'Asset', asset['id'], {'image': None}) == {
        **asset,
        'image': None,
    }
    assert await _image(workflow_site, asset) is None
    assert await _image(workflow_site, asset, 'filmstrip_image') is None
    target = seed(workflow_site, 'Asset', {'code': 'A2'})
    with pytest.raises(
        ShotgunError,
        match=rf'^share_thumbnail failed for entity_type=Asset entity_id={entity_id(target)}$',
    ):
        await workflow_site.call('share_thumbnail', [target], source_entity=asset)
