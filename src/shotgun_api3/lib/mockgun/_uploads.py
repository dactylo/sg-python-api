"""Dispatch observed upload forms and bind uploaded bytes to Attachments."""

from __future__ import annotations

from datetime import UTC, datetime
from mimetypes import guess_type
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, urlsplit

from shotgun_api3._transport import TransportRequest, TransportResponse

from ._activity import taint_activity
from ._errors import MockgunError, ServerFaultError
from ._events import record_attachment_upload
from ._storage import STORAGE_HOST, Allocation
from ._store import UPLOAD_LINK_REASON_PREFIX
from ._thumbnails import link_thumbnail, share_thumbnail

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._server import FakeServer
    from ._sessions import RequestContext
    from ._store import FieldValue, Row

_ASCII_LIMIT = 128
_AUTH_FIELDS = frozenset(
    {
        'script_name',
        'script_key',
        'user_login',
        'user_password',
        'auth_token',
        'session_token',
        'sudo_as_login',
        'session_uuid',
    }
)
_ROUTES: dict[tuple[str, str], Callable[[FakeServer, dict[str, str], RequestContext], str]] = {}


def _created_at(form: dict[str, str], route: str) -> datetime | None:
    """Parse an aware optional creation instant without inventing a timezone."""
    if 'created_at' not in form:
        return None
    entity_type = form.get('upload_type', 'Attachment')
    try:
        value = datetime.fromisoformat(form['created_at'])
    except ValueError as error:
        raise MockgunError(
            f'mockgun POST {route} {entity_type}.created_at: datetime is unsettled'
        ) from error
    if value.utcoffset() is None:
        raise MockgunError(
            f'mockgun POST {route} {entity_type}.created_at: naive datetime is unsettled'
        )
    return value.astimezone(UTC)


def _attachment_fields(
    server: FakeServer, owner: Row, allocation: Allocation, form: dict[str, str]
) -> dict[str, object]:
    """Select declared Attachment fields from the observed upload metadata."""
    fields: dict[str, object] = {
        'filename': allocation.filename,
        'original_fname': allocation.filename,
        'display_name': form['display_name'],
        'attachment_links': [{'type': owner['type'], 'id': owner['id']}],
    }
    stamp = _created_at(form, '/upload/api_link_file')
    if stamp is not None:
        fields['created_at'] = stamp
    if server.schema.declares(cast('str', owner['type']), 'project'):
        server.store.check_known(owner, 'project')
        fields['project'] = owner.get('project')
    return {
        key: value for key, value in fields.items() if server.schema.declares('Attachment', key)
    }


def _fields(form: dict[str, str], route: str, names: set[str]) -> None:
    """Reject unknown fields before the route changes state."""
    for name in form.keys() - names:
        raise MockgunError(f'mockgun POST {route} field {name}: form behavior is unsettled')


def _owner_changes(
    server: FakeServer, owner: Row, attachment: Row, form: dict[str, str], known_file: bool
) -> None:
    """Apply observed owner links and mark unsettled computed fields unknown."""
    store = server.store
    entity_type = cast('str', owner['type'])
    field = form.get('field_name')
    store.remember(owner)
    if field is not None and not known_file:
        reason = (
            'Attachment.this_file declaration is missing'
            if not server.schema.declares('Attachment', 'this_file')
            else 'file kind outside the observed .txt family is unsettled'
        )
        store.mark_unknown(
            owner,
            field,
            f'{UPLOAD_LINK_REASON_PREFIX}{reason}',
        )
    elif field is not None:
        store.forget_unknown(owner, field)
    if field is not None:
        if entity_type == 'Version' and server.schema.declares(
            entity_type, 'sg_uploaded_movie_transcoding_status'
        ):
            store.mark_unknown(
                owner,
                'sg_uploaded_movie_transcoding_status',
                f'{UPLOAD_LINK_REASON_PREFIX}transcoding status is unsettled',
            )
    elif server.schema.declares(entity_type, 'attachments'):
        if entity_type == 'Note':
            store.check_known(owner, 'attachments')
            owner['attachments'] = [
                *cast('list[FieldValue]', owner.get('attachments') or []),
                {'type': 'Attachment', 'id': attachment['id']},
            ]
        else:
            store.mark_unknown(
                owner, 'attachments', f'{UPLOAD_LINK_REASON_PREFIX}inverse links are unsettled'
            )
    if field is not None or entity_type == 'Note':
        for name in ('updated_at', 'updated_by'):
            if server.schema.declares(entity_type, name):
                store.mark_unknown(
                    owner, name, f'{UPLOAD_LINK_REASON_PREFIX}owner audit is unsettled'
                )


def _create_attachment(
    server: FakeServer,
    owner: Row,
    allocation: Allocation,
    form: dict[str, str],
    context: RequestContext,
) -> tuple[Row, bool]:
    """Create the Attachment and report whether its file family is observed."""
    store = server.store
    attachment = store.create(
        'Attachment', _attachment_fields(server, owner, allocation, form), context=context
    )
    known_file = (
        Path(allocation.filename).suffix == '.txt'
        and all(
            byte < _ASCII_LIMIT
            for byte in server.storage.read(f'https://{STORAGE_HOST}/{allocation.key}')
        )
        and server.schema.declares('Attachment', 'this_file')
    )
    file_value: Row = {
        'url': f'https://{STORAGE_HOST}/{allocation.key}',
        'name': form['display_name'],
        'content_type': 'text/plain' if known_file else guess_type(allocation.filename)[0],
        'link_type': 'upload',
        'type': 'Attachment',
        'id': attachment['id'],
    }
    if server.schema.declares('Attachment', 'this_file'):
        attachment['this_file'] = file_value
        if not known_file:
            store.mark_unknown(
                attachment,
                'this_file',
                f'{UPLOAD_LINK_REASON_PREFIX}file kind outside the observed '
                '.txt family is unsettled',
            )
    if server.schema.declares('Attachment', 'project') and not server.schema.declares(
        cast('str', owner['type']), 'project'
    ):
        store.mark_unknown(
            attachment,
            'project',
            f'{UPLOAD_LINK_REASON_PREFIX}owner project declaration is missing',
        )
    # The owner field needs the dictionary even in a partial Attachment schema.
    if 'field_name' in form:
        store.remember(owner)
        owner[form['field_name']] = file_value.copy()
    _owner_changes(server, owner, attachment, form, known_file)
    return attachment, known_file


def _route(
    method: str, suffix: str
) -> Callable[
    [Callable[[FakeServer, dict[str, str], RequestContext], str]],
    Callable[[FakeServer, dict[str, str], RequestContext], str],
]:
    """Register a file route by HTTP method and site path suffix."""

    def register(
        handler: Callable[[FakeServer, dict[str, str], RequestContext], str],
    ) -> Callable[[FakeServer, dict[str, str], RequestContext], str]:
        _ROUTES[method, suffix] = handler
        return handler

    return register


@_route('POST', '/upload/api_get_upload_link_info')
def _allocate(server: FakeServer, form: dict[str, str], context: RequestContext) -> str:
    """Allocate an upload according to the client's selected direct route."""
    route = '/upload/api_get_upload_link_info'
    _fields(form, route, {'filename', 'upload_type', 'multipart_upload', 'created_at'})
    _created_at(form, route)
    if form['upload_type'] not in {'Attachment', 'Thumbnail'} or form['multipart_upload'] not in {
        'true',
        'false',
    }:
        raise MockgunError(
            f'mockgun POST {route} upload_type {form["upload_type"]}: '
            'allocation kind or multipart_upload is unsettled'
        )
    return server.storage.allocate(
        form['filename'],
        form['upload_type'],
        multipart=form['multipart_upload'] == 'true',
        now=context.now,
    ).encoded


@_route('POST', '/upload/api_complete_multipart_upload')
def _complete(server: FakeServer, form: dict[str, str], context: RequestContext) -> str:
    """Complete the uploaded parts in their supplied ETag order."""
    del context
    _fields(
        form,
        '/upload/api_complete_multipart_upload',
        {'etags', 'filename', 'timestamp', 'upload_id', 'upload_type'},
    )
    return str(server.storage.complete(form['upload_id'], form['etags'].split(',')))


@_route('POST', '/upload/api_get_upload_link_for_part')
def _part(server: FakeServer, form: dict[str, str], context: RequestContext) -> str:
    """Issue a URL for one client-selected multipart part."""
    del context
    _fields(
        form,
        '/upload/api_get_upload_link_for_part',
        {'filename', 'part_number', 'timestamp', 'upload_id', 'upload_type'},
    )
    return '1 ' + server.storage.part_url(form['upload_id'], int(form['part_number']))


@_route('POST', '/upload/api_link_file')
def _link(server: FakeServer, form: dict[str, str], context: RequestContext) -> str:
    """Bind an uploaded file to its observed Attachment and owner links."""
    route = '/upload/api_link_file'
    allocation = server.storage.allocation(form['upload_link_info'])
    if not server.storage.has_bytes(allocation.key):
        raise MockgunError(
            f'mockgun POST {route} upload_id {allocation.upload_id}: uploaded bytes are missing'
        )
    if allocation.upload_type == 'Thumbnail':
        return link_thumbnail(server, allocation, form, context)
    _fields(
        form,
        route,
        {
            'entity_type',
            'entity_id',
            'field_name',
            'display_name',
            'created_at',
            'tag_list',
            'upload_link_info',
        },
    )
    entity_type, entity_id = form['entity_type'], int(form['entity_id'])
    owner = (
        server.store.row(entity_type, entity_id) if server.schema.declares(entity_type) else None
    )
    if owner is None:
        return f'0:mockgun upload link failed: {entity_type} {entity_id} does not exist'
    if owner['__retired'] or 'tag_list' in form:
        raise MockgunError(
            f'mockgun POST {route} {entity_type} {entity_id}: '
            'retired owner or tag_list behavior is unsettled'
        )
    server.store.check_writable(owner)
    if not server.schema.declares('Attachment', 'attachment_links'):
        raise MockgunError(
            f'mockgun POST {route} Attachment.attachment_links for {entity_type} {entity_id}: '
            'declaration is missing'
        )
    field = form.get('field_name')
    if field is not None and (
        not server.schema.declares(entity_type, field)
        or server.schema.field_type(entity_type, field) != 'url'
    ):
        raise MockgunError(
            f'mockgun POST {route} {entity_type} {entity_id} field {field}: '
            'URL field declaration is missing'
        )
    with server.store.transaction():
        attachment, known_file = _create_attachment(server, owner, allocation, form, context)
        if field is None and entity_type == 'Note' and context.session_kind == 'script':
            record_attachment_upload(
                server.store,
                attachment,
                owner,
                context=context,
                display_name_changed=(
                    not known_file or form['display_name'] != allocation.filename
                ),
            )
        else:
            reason = (
                f'POST {route} {entity_type} {entity_id}: upload events and activity are unsettled'
            )
            server.store.taint_events(reason)
            if field is not None or entity_type != 'Note':
                taint_activity(server.store, [owner], reason)
    server.storage.bind(cast('int', attachment['id']), allocation.key)
    return f'1:{attachment["id"]} {form["display_name"]}'


def _serve_attachment(
    server: FakeServer, request: TransportRequest, path: str
) -> TransportResponse:
    """Resolve a cookie-bearing Attachment URL to stored bytes or a missing row."""
    identity_text = path.rsplit('/file_serve/attachment/', 1)[1]
    if not identity_text.isdecimal():
        raise MockgunError(f'mockgun GET {path} Attachment {identity_text}: identity is unsettled')
    identity = int(identity_text)
    cookie = next(
        (value for name, value in request.headers.items() if name.lower() == 'cookie'), None
    )
    token = next(
        (
            item.strip().split('=', 1)[1]
            for item in (cookie or '').split(';')
            if item.strip().startswith('_session_id=')
        ),
        None,
    )
    if token is None:
        raise MockgunError(f'mockgun GET {path} Attachment {identity}: session cookie is missing')
    context = server.context_for({'session_token': token}, method_name=path)
    server.store.context = context
    row = server.store.row('Attachment', identity)
    if row is None or row['__retired']:
        return TransportResponse(404, 'Not Found', {}, b'')
    key = server.storage.bound_key(identity)
    if key is None or not server.storage.has_bytes(key):
        raise MockgunError(f'mockgun GET {path} Attachment {identity}: stored bytes are unsettled')
    return TransportResponse(302, 'Found', {'location': f'https://{STORAGE_HOST}/{key}'}, b'')


def _file_request(server: FakeServer, request: TransportRequest) -> TransportResponse | None:
    """Run a matching storage or site route without translating failures."""
    parsed = urlsplit(request.url)
    route = f'{request.method} {parsed.path}'
    if parsed.hostname == STORAGE_HOST and request.method == 'GET':
        return TransportResponse(200, 'OK', {}, server.storage.read(request.url))
    if (
        request.method == 'GET'
        and parsed.hostname == server.site_host
        and '/file_serve/attachment/' in parsed.path
    ):
        return _serve_attachment(server, request, parsed.path)
    if parsed.hostname == STORAGE_HOST and request.method == 'PUT':
        if not isinstance(request.body, bytes):
            raise MockgunError(f'mockgun {route} body: non-byte storage data is unsettled')
        etag = server.storage.put(request.url, request.body)
        return TransportResponse(200, 'OK', {'ETag': etag} if etag else {}, b'')
    for (method, suffix), handler in _ROUTES.items():
        if (
            request.method == method
            and parsed.path.endswith(suffix)
            and parsed.hostname == server.site_host
        ):
            if not isinstance(request.body, bytes):
                raise MockgunError(f'mockgun {route} body: non-byte form data is unsettled')
            values = parse_qs(request.body.decode(), keep_blank_values=True)
            if any(len(value) != 1 for value in values.values()):
                raise MockgunError(f'mockgun {route} form fields: repeated values are unsettled')
            form = {key: value[0] for key, value in values.items()}
            auth = {key: form.pop(key) for key in list(form) if key in _AUTH_FIELDS}
            context = server.context_for(auth, method_name=suffix)
            server.store.context = context
            return TransportResponse(200, 'OK', {}, handler(server, form, context).encode())
    return None


@_route('POST', '/upload/share_thumbnail')
def _share(server: FakeServer, form: dict[str, str], context: RequestContext) -> str:
    """Dispatch the observed source-thumbnail share form."""
    _fields(form, '/upload/share_thumbnail', {'entities', 'source_entity', 'filmstrip_thumbnail'})
    return share_thumbnail(server, form, context)


def handle_file_request(server: FakeServer, request: TransportRequest) -> TransportResponse:
    """Dispatch an observed HTTP upload without producing JSON-RPC faults.

    Raises:
        MockgunError: If the route, credentials, form or store rejection is
            outside the observed HTTP answers.
    """
    route = f'{request.method} {urlsplit(request.url).path}'
    try:
        response = _file_request(server, request)
    except (MockgunError, ServerFaultError, KeyError, ValueError) as error:
        raise MockgunError(
            f'mockgun {route} form or entity fields: rejection is unsettled: {error}'
        ) from error
    if response is None:
        raise MockgunError(f'mockgun does not emulate {route}')
    return response
