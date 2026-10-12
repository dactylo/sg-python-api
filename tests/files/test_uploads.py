import datetime
import inspect
import io
import json
import re
import urllib.parse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, BinaryIO, cast
from unittest.mock import patch

import pytest

from shotgun_api3 import _driver, _errors, _files, _protocol
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ObserverEvent, ProgressEvent, RequestEvent, ResponseEvent
from shotgun_api3._errors import ShotgunError
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    SendError,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._pyright import run_pyright
from tests._transports import _FakeTransport
from tests.conftest import _fake_client, _fake_transport, _make_response

_DIRECT_INFO: dict[str, object] = {
    'version': [9, 0, 0],
    'api_max_entities_per_page': 500,
    's3_direct_uploads_enabled': True,
    's3_enabled_upload_types': {'*': ['*']},
}
_FORM_INFO: dict[str, object] = {
    'version': [9, 0, 0],
    'api_max_entities_per_page': 500,
    's3_direct_uploads_enabled': False,
    's3_enabled_upload_types': {},
}
_FORM_UPLOAD_BOUNDARY = 'Task13Boundary'


class _ConsumingFakeTransport(_FakeTransport):
    bodies: list[bytes | None]
    body_ids: list[int | None]

    def __init__(self, responses: list[TransportResponse | TransportError]) -> None:
        super().__init__(responses)
        self.bodies = []
        self.body_ids = []

    def send(self, request: TransportRequest) -> TransportResponse:
        body = request.body
        self.body_ids.append(None if body is None else id(body))
        captured = body if body is None or isinstance(body, bytes) else b''.join(body)
        self.bodies.append(captured)
        return super().send(request)


class _PartiallyConsumingFakeTransport(_FakeTransport):
    closed_before_retry: bool | None
    first_handle_closed: Callable[[], bool]
    partial_body: bytes | None
    replay_body: bytes | None

    def __init__(
        self,
        responses: list[TransportResponse | TransportError],
        first_handle_closed: Callable[[], bool],
    ) -> None:
        self.closed_before_retry = None
        self.first_handle_closed = first_handle_closed
        self.partial_body = None
        self.replay_body = None
        super().__init__(responses)

    def send(self, request: TransportRequest) -> TransportResponse:
        body = request.body
        if request.url == 'https://storage.example/object':
            assert body is not None
            assert not isinstance(body, bytes)
            if self.partial_body is None:
                self.partial_body = next(body)
            else:
                self.closed_before_retry = self.first_handle_closed()
                self.replay_body = b''.join(body)
        return super().send(request)


def _form_fields(body: bytes) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(body.decode(), keep_blank_values=True))


def _freeze_form_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    original = _protocol.build_multipart_plan

    def build_multipart_plan(
        fields: dict[str, str],
        files: list[_protocol.FileSlot],
        boundary: str | None = None,
    ) -> _protocol.MultipartPlan:
        assert boundary is None
        return original(fields, files, boundary=_FORM_UPLOAD_BOUNDARY)

    monkeypatch.setattr(_protocol, 'build_multipart_plan', build_multipart_plan)


def _multipart_fields(body: bytes, content_type: str) -> tuple[dict[str, str], bytes]:
    boundary = content_type.removeprefix('multipart/form-data; boundary=').encode()
    fields: dict[str, str] = {}
    file_content = b''
    for part in body.split(b'--' + boundary):
        if b'Content-Disposition: form-data;' not in part:
            continue
        headers, _, payload = part.partition(b'\r\n\r\n')
        match = re.search(rb'name="([^"]+)"', headers)
        assert match is not None
        name = match.group(1).decode()
        value = payload.removesuffix(b'\r\n')
        if b'filename=' in headers:
            file_content = value
        else:
            fields[name] = value.decode()
    return fields, file_content


def _raw_upload_transcript(
    requests: list[TransportRequest],
    bodies: list[bytes | None],
) -> list[tuple[str, str, dict[str, str], bytes | None]]:
    return [
        (request.method, request.url, dict(request.headers), body)
        for request, body in zip(requests, bodies, strict=True)
    ]


def _text(
    content: str = '',
    *,
    headers: Mapping[str, str] | None = None,
    status: int = 200,
) -> TransportResponse:
    return TransportResponse(
        status=status,
        reason='OK' if status == 200 else 'Forbidden',
        headers=CaseInsensitiveHeaders(headers or {'content-type': 'text/plain'}),
        content=content.encode(),
    )


def _upload_client(
    responses: list[TransportResponse | TransportError],
    server_info: dict[str, object] = _DIRECT_INFO,
) -> Shotgun:
    sg = Shotgun('https://x.example', script_name='script', api_key='key', connect=False)
    transport = _ConsumingFakeTransport([_make_response(server_info), *responses])
    sg._transport = transport
    return sg


def _upload_headers(
    body: bytes,
    content_type: str,
) -> dict[str, str]:
    return {
        'content-length': str(len(body)),
        'content-type': content_type,
        'user-agent': 'upload-transcript',
    }


@pytest.mark.parametrize('server_info', [_DIRECT_INFO, _FORM_INFO], ids=['direct', 'form'])
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('http://remote.example', None),
        ('http://localhost', 'proxy.example:3128'),
        ('http://127.0.0.1', 'proxy.example:3128'),
        ('http://[::1]', 'proxy.example:3128'),
        ('http://localhost', 'localhost:3128'),
        ('http://127.0.0.1', '127.0.0.1:3128'),
        ('http://[::1]', '[::1]:3128'),
    ],
)
def test_authenticated_file_http_upload_rejects_before_lease(
    tmp_path: Path,
    server_info: dict[str, object],
    base_url: str,
    http_proxy: str | None,
) -> None:
    path = tmp_path / 'clip.mov'
    path.write_bytes(b'clip-data')
    sg = Shotgun(
        base_url,
        script_name='dummy-script',
        api_key='dummy-key',
        http_proxy=http_proxy,
        connect=False,
    )
    transport = _ConsumingFakeTransport([_make_response(server_info), _text('1:42')])
    sg._transport = transport
    events: list[object] = []
    sg.config.on_request = events.append
    with patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease:
        with pytest.raises(
            ValueError,
            match=r'^authenticated RPCs require HTTPS or a direct literal loopback HTTP origin$',
        ):
            sg.upload('Version', 1, str(path))
        lease.assert_not_called()
    assert transport.requests == []
    assert events == []


@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('https://remote.example', None),
        ('https://remote.example', 'proxy.example:3128'),
        ('https://remote.example', 'localhost:3128'),
        ('http://localhost', None),
        ('http://LOCALHOST', None),
        ('http://127.0.0.1', None),
        ('http://127.42.0.1', None),
        ('http://[::1]', None),
    ],
)
def test_authenticated_file_upload_allows_https_and_literal_loopback(
    tmp_path: Path, base_url: str, http_proxy: str | None
) -> None:
    path = tmp_path / 'clip.mov'
    path.write_bytes(b'clip-data')
    sg = Shotgun(
        base_url,
        http_proxy=http_proxy,
        script_name='dummy-script',
        api_key='dummy-key',
        connect=False,
    )
    transport = _ConsumingFakeTransport(
        [
            _make_response(_DIRECT_INFO),
            _text('1 https://storage.example/object timestamp Attachment upload-1'),
            _text(''),
            _text('1:42'),
        ]
    )
    sg._transport = transport

    assert sg.upload('Version', 1, str(path)) == 42
    assert b'dummy-key' in cast('bytes', transport.bodies[-1])


def test_build_share_thumbnail_fields_uses_current_wire_grammar() -> None:
    entities = [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}]
    source = {'type': 'Version', 'id': 2}

    assert _files.build_share_thumbnail_fields(
        entities,
        source,
        filmstrip_thumbnail=False,
    ) == {
        'entities': 'Version_2,Shot_7',
        'filmstrip_thumbnail': 'False',
        'source_entity': 'Version_2',
    }
    assert _files.build_share_thumbnail_fields(
        entities,
        source,
        filmstrip_thumbnail=True,
    ) == {
        'entities': 'Version_2,Shot_7',
        'filmstrip_thumbnail': '1',
        'source_entity': 'Version_2',
    }


def test_default_direct_attachment_upload_omits_field_name(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'clip-data')
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg = _upload_client([_text(encoded), _text(), _text('1:42 clip.mov')])

    assert sg.upload('Version', 5, str(movie)) == 42

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    assert _form_fields(cast('bytes', transport.bodies[3])) == {
        'entity_type': 'Version',
        'entity_id': '5',
        'display_name': 'clip.mov',
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }


def test_direct_storage_put_closes_partial_body_before_complete_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = b'a' * 70_000 + b'z'
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(payload)
    handles: list[io.BytesIO] = []

    def open_file(path: Path, mode: str) -> BinaryIO:
        assert path == movie
        assert mode == 'rb'
        handle = io.BytesIO(payload)
        handles.append(handle)
        return cast('BinaryIO', handle)

    monkeypatch.setattr(Path, 'open', open_file)
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    transport = _PartiallyConsumingFakeTransport(
        [
            _make_response(_DIRECT_INFO),
            _text(encoded),
            SendError('ambiguous'),
            _text(),
            _text('1:9 linked'),
        ],
        lambda: bool(handles and handles[0].closed),
    )
    sg = Shotgun('https://x.example', script_name='script', api_key='key', connect=False)
    sg._transport = transport
    sg.config.rpc_attempt_interval = 0

    assert sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    assert transport.partial_body == payload[:65_536]
    assert transport.closed_before_retry is True
    assert transport.replay_body == payload
    assert len(handles) == 2
    assert handles[0] is not handles[1]
    assert all(handle.closed for handle in handles)


def test_direct_storage_put_retries_with_fresh_complete_bodies(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'restartable')
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg = _upload_client([_text(encoded), SendError('ambiguous'), _text(), _text('1:9 linked')])
    sg.config.rpc_attempt_interval = 0

    assert sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    put_requests = [
        (request, body, body_id)
        for request, body, body_id in zip(
            transport.requests,
            transport.bodies,
            transport.body_ids,
            strict=True,
        )
        if request.url == 'https://storage.example/object'
    ]
    assert len(put_requests) == 2
    assert [body for _request, body, _body_id in put_requests] == [
        b'restartable',
        b'restartable',
    ]
    assert put_requests[0][2] != put_requests[1][2]


def test_direct_upload_uses_one_snapshot_and_forwards_observed_fields(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'clip-data')
    created_at = datetime.datetime(2026, 7, 18, 9, 10, 11, tzinfo=datetime.UTC)
    encoded = '1 https://storage.example/object 2026-07-18T09:10:11+00:00 Attachment upload-1'
    events: list[ObserverEvent] = []
    sg = _upload_client([_text(encoded), _text(), _text('1:42 Display Name.mov')])
    sg.config.on_request = events.append
    sg.config.on_progress = events.append
    sg.config.user_agents = ('upload-transcript',)

    assert (
        sg.upload(
            'Version',
            5,
            str(movie),
            field_name='sg_uploaded_movie',
            display_name='Display Name.mov',
            tag_list='tag-one,tag-two',
            created_at=created_at,
        )
        == 42
    )

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    requests = transport.requests
    allocation_body = (
        b'filename=clip.mov&upload_type=Attachment&multipart_upload=false&'
        b'created_at=2026-07-18T09%3A10%3A11%2B00%3A00&script_name=script&script_key=key'
    )
    link_body = (
        b'entity_type=Version&entity_id=5&field_name=sg_uploaded_movie&'
        b'display_name=Display+Name.mov&tag_list=tag-one%2Ctag-two&'
        b'created_at=2026-07-18+09%3A10%3A11%2B00%3A00&upload_link_info=1+'
        b'https%3A%2F%2Fstorage.example%2Fobject+2026-07-18T09%3A10%3A11%2B00%3A00+'
        b'Attachment+upload-1&script_name=script&script_key=key'
    )
    assert [request.url for request in requests[1:]] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/object',
        'https://x.example:443/upload/api_link_file',
    ]
    assert [request.method for request in requests[1:]] == ['POST', 'PUT', 'POST']
    assert [request.headers for request in requests[1:]] == [
        {
            'content-length': str(len(allocation_body)),
            'content-type': 'application/x-www-form-urlencoded',
            'user-agent': 'upload-transcript',
        },
        {
            'content-length': str(len(b'clip-data')),
            'content-type': 'application/octet-stream',
            'user-agent': 'upload-transcript',
        },
        {
            'content-length': str(len(link_body)),
            'content-type': 'application/x-www-form-urlencoded',
            'user-agent': 'upload-transcript',
        },
    ]
    assert transport.bodies[1:] == [allocation_body, b'clip-data', link_body]
    assert _form_fields(cast('bytes', transport.bodies[1])) == {
        'filename': 'clip.mov',
        'upload_type': 'Attachment',
        'multipart_upload': 'false',
        'created_at': '2026-07-18T09:10:11+00:00',
        'script_name': 'script',
        'script_key': 'key',
    }
    assert transport.bodies[2] == b'clip-data'
    assert requests[2].headers['content-length'] == str(len(b'clip-data'))
    link_fields = _form_fields(cast('bytes', transport.bodies[3]))
    assert link_fields == {
        'entity_type': 'Version',
        'entity_id': '5',
        'field_name': 'sg_uploaded_movie',
        'display_name': 'Display Name.mov',
        'tag_list': 'tag-one,tag-two',
        'created_at': '2026-07-18 09:10:11+00:00',
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }
    assert not {'filename', 'timestamp', 'upload_id', 'upload_type'} & link_fields.keys()
    request_events = [event for event in events if isinstance(event, RequestEvent)]
    assert [event.route for event in request_events] == [
        'rpc',
        'allocation',
        'object_put',
        'link',
    ]
    assert any(
        isinstance(event, ProgressEvent)
        and event.route == 'object_put'
        and event.bytes_transferred == len(b'clip-data')
        and event.total_bytes == len(b'clip-data')
        for event in events
    )


def test_disk_body_iterators_are_lazy_and_close_their_handles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direct_handle = io.BytesIO(b'abcdef')
    multipart_handle = io.BytesIO(b'abcdef')
    handles = iter([direct_handle, multipart_handle])

    def open_file(_path: Path, _mode: str) -> BinaryIO:
        return cast('BinaryIO', next(handles))

    monkeypatch.setattr(Path, 'open', open_file)
    direct = _files._iter_file('/unused/direct', chunk_size=2)
    assert not direct_handle.closed
    assert next(direct) == b'ab'
    direct.close()
    assert direct_handle.closed

    slot = _protocol.FileSlot(name='file', filename='clip.mov', content_type='video/quicktime')
    plan = _protocol.build_multipart_plan({}, [slot], boundary='Boundary')
    multipart = _files._pump_multipart_from_disk(
        plan,
        {'file': '/unused/multipart'},
        chunk_size=2,
    )
    assert not multipart_handle.closed
    assert next(multipart).startswith(b'--Boundary')
    assert not multipart_handle.closed
    multipart.close()
    assert multipart_handle.closed


def test_form_upload_stops_at_the_observed_403_request_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _freeze_form_boundary(monkeypatch)
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'form-data')
    sg = _upload_client([_text('unread', status=403)], _FORM_INFO)
    sg.config.user_agents = ('upload-transcript',)

    with pytest.raises(_errors.ProtocolError):
        sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    assert len(transport.requests) == 2
    request = transport.requests[1]
    body = (
        b'--Task13Boundary\r\nContent-Disposition: form-data; name="entity_type"\r\n\r\n'
        b'Version\r\n--Task13Boundary\r\nContent-Disposition: form-data; name="entity_id"'
        b'\r\n\r\n5\r\n--Task13Boundary\r\nContent-Disposition: form-data; name="field_name"'
        b'\r\n\r\nsg_uploaded_movie\r\n--Task13Boundary\r\nContent-Disposition: form-data; '
        b'name="display_name"\r\n\r\nclip.mov\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="script_name"\r\n\r\nscript\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="script_key"\r\n\r\nkey\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="file"; filename="clip.mov"\r\nContent-Type: application/octet-stream'
        b'\r\n\r\nform-data\r\n--Task13Boundary--\r\n'
    )
    assert _raw_upload_transcript(
        transport.requests[1:],
        transport.bodies[1:],
    ) == [
        (
            'POST',
            'https://x.example:443/upload/upload_file',
            _upload_headers(body, 'multipart/form-data; boundary=Task13Boundary'),
            body,
        )
    ]
    assert request.url == 'https://x.example:443/upload/upload_file'
    assert request.headers['content-length'] == str(len(cast('bytes', transport.bodies[1])))
    fields, file_content = _multipart_fields(
        cast('bytes', transport.bodies[1]),
        request.headers['content-type'],
    )
    assert fields == {
        'entity_type': 'Version',
        'entity_id': '5',
        'field_name': 'sg_uploaded_movie',
        'display_name': 'clip.mov',
        'script_name': 'script',
        'script_key': 'key',
    }
    assert file_content == b'form-data'


def test_form_upload_unsupported_success_is_driver_error_event(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'form-data')
    events: list[ObserverEvent] = []
    sg = _upload_client([_text('opaque success')], _FORM_INFO)
    sg.config.on_response = events.append

    with pytest.raises(
        ShotgunError,
        match=r'^form upload success response is not established$',
    ):
        sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == len(_fake_transport(sg).requests)
    assert response_events[-1].status == 200
    assert response_events[-1].route == 'rpc'
    assert response_events[-1].outcome == 'error'


def test_link_and_finalize_do_not_restart_after_ambiguous_send(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    small = tmp_path / 'small.mov'
    small.write_bytes(b'data')
    direct = _upload_client(
        [
            _text('1 https://storage.example/object timestamp Attachment upload-1'),
            _text(),
            SendError('ambiguous'),
            _text('1:unused'),
        ]
    )
    direct.config.rpc_attempt_interval = 0

    with pytest.raises(SendError, match='ambiguous'):
        direct.upload('Version', 5, str(small), field_name='sg_uploaded_movie')
    assert len(_fake_transport(direct).requests) == 4

    large = tmp_path / 'large.mov'
    large.write_bytes(b'a' * _files._MULTIPART_CHUNK_SIZE + b'z')
    multipart = _upload_client(
        [
            _text('1 https://storage.example/multipart timestamp Attachment upload-1 opaque'),
            _text('1 https://storage.example/part-1'),
            _text(headers={'etag': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'etag': 'etag-2'}),
            SendError('ambiguous'),
            _text('unused'),
        ]
    )
    multipart.config.rpc_attempt_interval = 0

    with pytest.raises(SendError, match='ambiguous'):
        multipart.upload('Version', 5, str(large), field_name='sg_uploaded_movie')
    assert len(_fake_transport(multipart).requests) == 7


@pytest.mark.parametrize(
    ('endpoint', 'expected_route'),
    [
        ('allocation', 'allocation'),
        ('finalize', 'finalize'),
        ('link', 'link'),
        ('part-link', 'allocation'),
    ],
)
def test_malformed_upload_endpoint_response_is_driver_error_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    endpoint: str,
    expected_route: str,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    movie = tmp_path / 'clip.mov'
    direct_allocation = '1 https://storage.example/object timestamp Attachment upload-1'
    multipart_allocation = (
        '1 https://storage.example/multipart timestamp Attachment upload-1 opaque'
    )
    responses: list[TransportResponse | TransportError]
    if endpoint == 'allocation':
        movie.write_bytes(b'data')
        responses = [_text('malformed')]
    elif endpoint == 'link':
        movie.write_bytes(b'data')
        responses = [_text(direct_allocation), _text(), _text('malformed')]
    elif endpoint == 'part-link':
        movie.write_bytes(b'a' * _files._MULTIPART_CHUNK_SIZE + b'z')
        responses = [_text(multipart_allocation), _text('malformed')]
    else:
        movie.write_bytes(b'a' * _files._MULTIPART_CHUNK_SIZE + b'z')
        responses = [
            _text(multipart_allocation),
            _text('1 https://storage.example/part-1'),
            _text(headers={'etag': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'etag': 'etag-2'}),
            _text('malformed'),
        ]
    events: list[ObserverEvent] = []
    sg = _upload_client(responses)
    sg.config.on_response = events.append

    with pytest.raises(ShotgunError):
        sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == len(_fake_transport(sg).requests)
    assert response_events[-1].status == 200
    assert response_events[-1].route == expected_route
    assert response_events[-1].outcome == 'error'


def test_multipart_part_put_missing_etag_is_driver_error_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    movie = tmp_path / 'large.mov'
    movie.write_bytes(b'a' * _files._MULTIPART_CHUNK_SIZE + b'z')
    events: list[ObserverEvent] = []
    sg = _upload_client(
        [
            _text('1 https://storage.example/multipart timestamp Attachment upload-1 opaque'),
            _text('1 https://storage.example/part-1'),
            _text(),
        ]
    )
    sg.config.on_response = events.append

    with pytest.raises(
        ShotgunError,
        match=r'^multipart part response did not include an ETag$',
    ):
        sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == len(_fake_transport(sg).requests)
    assert response_events[-1].status == 200
    assert response_events[-1].route == 'part_put'
    assert response_events[-1].outcome == 'error'


def test_multipart_upload_uses_observed_part_boundary_etags_and_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    movie = tmp_path / 'large.mov'
    movie.write_bytes(b'abcde')
    encoded = '1 https://storage.example/multipart timestamp Attachment upload-1 opaque-extra'
    sg = _upload_client(
        [
            _text(encoded),
            _text('1 https://storage.example/part-1'),
            _text(headers={'eTaG': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'ETAG': 'etag-2'}),
            _text('17'),
            _text('1:84 large.mov'),
        ]
    )
    events: list[ObserverEvent] = []
    sg.config.on_request = events.append
    sg.config.user_agents = ('upload-transcript',)

    assert sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 84

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    allocation_body = (
        b'filename=large.mov&upload_type=Attachment&multipart_upload=true&'
        b'script_name=script&script_key=key'
    )
    part_one_body = (
        b'filename=large.mov&part_number=1&timestamp=timestamp&upload_id=upload-1&'
        b'upload_type=Attachment&script_name=script&script_key=key'
    )
    part_two_body = (
        b'filename=large.mov&part_number=2&timestamp=timestamp&upload_id=upload-1&'
        b'upload_type=Attachment&script_name=script&script_key=key'
    )
    complete_body = (
        b'etags=etag-1%2Cetag-2&filename=large.mov&timestamp=timestamp&upload_id=upload-1&'
        b'upload_type=Attachment&script_name=script&script_key=key'
    )
    link_body = (
        b'entity_type=Version&entity_id=5&field_name=sg_uploaded_movie&display_name=large.mov&'
        b'upload_link_info=1+https%3A%2F%2Fstorage.example%2Fmultipart+timestamp+Attachment+'
        b'upload-1+opaque-extra&script_name=script&script_key=key'
    )
    form_content_type = 'application/x-www-form-urlencoded'
    assert _raw_upload_transcript(transport.requests[1:], transport.bodies[1:]) == [
        (
            'POST',
            'https://x.example:443/upload/api_get_upload_link_info',
            _upload_headers(allocation_body, form_content_type),
            allocation_body,
        ),
        (
            'POST',
            'https://x.example:443/upload/api_get_upload_link_for_part',
            _upload_headers(part_one_body, form_content_type),
            part_one_body,
        ),
        (
            'PUT',
            'https://storage.example/part-1',
            _upload_headers(b'abcd', 'application/octet-stream'),
            b'abcd',
        ),
        (
            'POST',
            'https://x.example:443/upload/api_get_upload_link_for_part',
            _upload_headers(part_two_body, form_content_type),
            part_two_body,
        ),
        (
            'PUT',
            'https://storage.example/part-2',
            _upload_headers(b'e', 'application/octet-stream'),
            b'e',
        ),
        (
            'POST',
            'https://x.example:443/upload/api_complete_multipart_upload',
            _upload_headers(complete_body, form_content_type),
            complete_body,
        ),
        (
            'POST',
            'https://x.example:443/upload/api_link_file',
            _upload_headers(link_body, form_content_type),
            link_body,
        ),
    ]
    assert transport.bodies[3] == b'abcd'
    assert transport.bodies[5] == b'e'
    assert _form_fields(cast('bytes', transport.bodies[2]))['part_number'] == '1'
    assert _form_fields(cast('bytes', transport.bodies[4]))['part_number'] == '2'
    complete_fields = _form_fields(cast('bytes', transport.bodies[6]))
    assert complete_fields['etags'] == 'etag-1,etag-2'
    link_fields = _form_fields(cast('bytes', transport.bodies[7]))
    assert link_fields['upload_link_info'] == encoded
    assert not {'filename', 'timestamp', 'upload_id', 'upload_type'} & link_fields.keys()
    request_events = [event for event in events if isinstance(event, RequestEvent)]
    assert [event.route for event in request_events] == [
        'rpc',
        'allocation',
        'allocation',
        'part_put',
        'allocation',
        'part_put',
        'finalize',
        'link',
    ]


@pytest.mark.parametrize('public_owner', ['shotgun', 'capabilities'])
def test_mutating_public_server_info_cannot_change_cached_upload_route(
    tmp_path: Path,
    public_owner: str,
) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'route-me-directly')
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg = _upload_client([_text(encoded), _text(), _text('1:9 linked')])

    exposed = sg.server_info if public_owner == 'shotgun' else sg.server_caps.server_info
    exposed['s3_direct_uploads_enabled'] = False
    allowed_types = cast('dict[str, list[str]]', exposed['s3_enabled_upload_types'])
    allowed_types['*'].clear()

    assert sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9
    assert [request.url for request in _fake_transport(sg).requests[1:]] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/object',
        'https://x.example:443/upload/api_link_file',
    ]


@pytest.mark.parametrize('path_source', [False, True], ids=['entity', 'path'])
@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('http://remote.example', None),
        ('http://localhost', 'proxy.example:3128'),
        ('http://127.0.0.1', 'proxy.example:3128'),
        ('http://[::1]', 'proxy.example:3128'),
        ('http://localhost', 'localhost:3128'),
        ('http://127.0.0.1', '127.0.0.1:3128'),
        ('http://[::1]', '[::1]:3128'),
    ],
)
def test_authenticated_file_http_share_rejects_before_lease(
    tmp_path: Path,
    path_source: bool,
    base_url: str,
    http_proxy: str | None,
) -> None:
    path = tmp_path / 'image.jpg'
    path.write_bytes(b'image')
    sg = Shotgun(
        base_url,
        script_name='dummy-script',
        api_key='dummy-key',
        http_proxy=http_proxy,
        connect=False,
    )
    transport = _FakeTransport([_make_response(_DIRECT_INFO), _text('1:42')])
    sg._transport = transport
    events: list[object] = []
    sg.config.on_request = events.append
    with patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease:
        with pytest.raises(
            ValueError,
            match=r'^authenticated RPCs require HTTPS or a direct literal loopback HTTP origin$',
        ):
            sg.share_thumbnail(
                [{'type': 'Asset', 'id': 1}],
                thumbnail_path=str(path) if path_source else None,
                source_entity=None if path_source else {'type': 'Asset', 'id': 2},
            )
        lease.assert_not_called()
    assert transport.requests == []
    assert events == []


@pytest.mark.parametrize(
    ('base_url', 'http_proxy'),
    [
        ('https://remote.example', None),
        ('https://remote.example', 'proxy.example:3128'),
        ('https://remote.example', 'localhost:3128'),
        ('http://localhost', None),
        ('http://LOCALHOST', None),
        ('http://127.0.0.1', None),
        ('http://127.42.0.1', None),
        ('http://[::1]', None),
    ],
)
@pytest.mark.parametrize('path_source', [False, True], ids=['entity', 'path'])
def test_authenticated_file_share_allows_https_and_literal_loopback(
    tmp_path: Path,
    base_url: str,
    path_source: bool,
    http_proxy: str | None,
) -> None:
    sg = Shotgun(
        base_url,
        http_proxy=http_proxy,
        script_name='dummy-script',
        api_key='dummy-key',
        connect=False,
    )
    responses: list[TransportResponse | TransportError] = [_make_response(_DIRECT_INFO)]
    if path_source:
        responses.extend(
            [_text('1 https://storage.example/image timestamp Thumbnail upload-1'), _text('')]
        )
    responses.append(_text('1:42'))
    transport = _FakeTransport(responses)
    sg._transport = transport
    path = tmp_path / 'image.jpg'
    path.write_bytes(b'image')

    assert (
        sg.share_thumbnail(
            [{'type': 'Asset', 'id': 1}],
            thumbnail_path=str(path) if path_source else None,
            source_entity=None if path_source else {'type': 'Asset', 'id': 2},
        )
        == 42
    )
    assert b'dummy-key' in cast('bytes', transport.requests[-1].body)


def test_part_etag_parser_requires_one_case_insensitive_header() -> None:
    assert _files.parse_part_etag('', CaseInsensitiveHeaders({'eTaG': 'etag-1'})) == 'etag-1'

    with pytest.raises(
        ShotgunError,
        match=r'^multipart part response did not include an ETag$',
    ):
        _files.parse_part_etag('', CaseInsensitiveHeaders({}))


@pytest.mark.parametrize(
    ('filmstrip_thumbnail', 'field_name'),
    [(False, 'image'), (True, 'filmstrip_image')],
)
def test_share_thumbnail_path_exhausts_25_driver_owned_poll_reads(
    tmp_path: Path,
    filmstrip_thumbnail: bool,
    field_name: str,
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    transient = {
        'entities': [
            {
                'type': 'Version',
                'id': 2,
                field_name: 'https://x.example/images/status/transient/processing',
            }
        ],
        'paging_info': {'entity_count': 1},
    }
    sg = _fake_client(
        [
            _text(allocation),
            _text(''),
            _text('1:41 image.jpg'),
            *[_make_response(transient) for _index in range(25)],
        ],
        server_info=_DIRECT_INFO,
    )
    sg.config.rpc_attempt_interval = 0
    sleeps: list[float] = []
    transport = _fake_transport(sg)
    sg._known_transport = transport
    sg._driver = _driver.Driver(transport, _driver._DriverTiming(sleep=sleeps.append))

    with pytest.raises(
        _errors.ShotgunThumbnailNotReady,
        match=r'^thumbnail source did not become ready$',
    ):
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            thumbnail_path=str(image),
            filmstrip_thumbnail=filmstrip_thumbnail,
        )

    requests = _fake_transport(sg).requests
    read_requests = [
        request
        for request in requests
        if request.url == 'https://x.example:443/api3/json'
        and isinstance(request.body, bytes)
        and b'"method_name": "read"' in request.body
    ]
    assert len(read_requests) == 25
    assert sleeps == [0.0] * 24
    assert sum(request.url.endswith(_files.GET_UPLOAD_LINK_INFO) for request in requests) == 1
    assert sum(request.url.endswith(_files.LINK_FILE) for request in requests) == 1
    assert not any(request.url.endswith(_files.SHARE_THUMBNAIL) for request in requests)


def test_share_thumbnail_path_polls_only_filmstrip_field_when_selected(
    tmp_path: Path,
) -> None:
    image = tmp_path / 'filmstrip.jpg'
    image.write_bytes(b'filmstrip')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    ready = {
        'entities': [
            {
                'type': 'Version',
                'id': 2,
                'filmstrip_image': 'https://x.example/filmstrip/ready.jpg',
            }
        ],
        'paging_info': {'entity_count': 1},
    }
    sg = _fake_client(
        [
            _text(allocation),
            _text(''),
            _text('1:41 filmstrip.jpg'),
            _make_response(ready),
            _text('1:42'),
        ],
        server_info=_DIRECT_INFO,
    )
    sg.config.rpc_attempt_interval = 0

    assert (
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            thumbnail_path=str(image),
            filmstrip_thumbnail=True,
        )
        == 42
    )

    requests = _fake_transport(sg).requests
    link_body = requests[3].body
    assert isinstance(link_body, bytes)
    assert dict(urllib.parse.parse_qsl(link_body.decode()))['filmstrip'] == 'true'
    read_request = requests[4]
    assert isinstance(read_request.body, bytes)
    read_payload = json.loads(read_request.body)
    read_params = cast('list[dict[str, Any]]', read_payload['params'])
    assert read_params[1]['return_fields'] == ['filmstrip_image']
    share_body = requests[-1].body
    assert isinstance(share_body, bytes)
    assert dict(urllib.parse.parse_qsl(share_body.decode()))['filmstrip_thumbnail'] == '1'


def test_share_thumbnail_path_polls_static_source_then_shares_remaining_target(
    tmp_path: Path,
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    transient = {
        'entities': [
            {
                'type': 'Version',
                'id': 2,
                'image': 'https://x.example/images/status/transient/processing',
            }
        ],
        'paging_info': {'entity_count': 1},
    }
    ready = {
        'entities': [{'type': 'Version', 'id': 2, 'image': 'https://x.example/image/ready.jpg'}],
        'paging_info': {'entity_count': 1},
    }
    sg = _fake_client(
        [
            _text(allocation),
            _text(''),
            _text('1:41 image.jpg'),
            _make_response(transient),
            _make_response(ready),
            _text('1:42'),
        ],
        server_info=_DIRECT_INFO,
    )
    sg.config.rpc_attempt_interval = 0

    assert (
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            thumbnail_path=str(image),
        )
        == 42
    )

    requests = _fake_transport(sg).requests
    assert [request.url for request in requests[1:]] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/image',
        'https://x.example:443/upload/api_link_file',
        'https://x.example:443/api3/json',
        'https://x.example:443/api3/json',
        'https://x.example:443/upload/share_thumbnail',
    ]
    read_payloads = [
        json.loads(request.body)
        for request in requests
        if request.url == 'https://x.example:443/api3/json'
        and isinstance(request.body, bytes)
        and b'"method_name": "read"' in request.body
    ]
    assert len(read_payloads) == 2
    for payload in read_payloads:
        params = cast('list[dict[str, Any]]', payload['params'])
        assert params[1]['return_fields'] == ['image']
    final_request = requests[-1]
    assert isinstance(final_request.body, bytes)
    assert dict(urllib.parse.parse_qsl(final_request.body.decode())) == {
        'entities': 'Shot_7',
        'filmstrip_thumbnail': 'False',
        'source_entity': 'Version_2',
        'script_name': 's',
        'script_key': 'k',
    }


def test_share_thumbnail_path_uploads_and_links_one_target_once(tmp_path: Path) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg = _fake_client(
        [_text(allocation), _text(''), _text('1:41 image.jpg')],
        server_info=_DIRECT_INFO,
    )

    assert (
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=str(image),
            display_name='Shared Image',
        )
        == 41
    )

    requests = _fake_transport(sg).requests
    assert [request.url for request in requests[1:]] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/image',
        'https://x.example:443/upload/api_link_file',
    ]
    assert isinstance(requests[3].body, bytes)
    assert dict(urllib.parse.parse_qsl(requests[3].body.decode())) == {
        'entity_type': 'Version',
        'entity_id': '2',
        'display_name': 'Shared Image',
        'upload_link_info': allocation,
        'script_name': 's',
        'script_key': 'k',
    }


def test_share_thumbnail_path_uses_direct_workflow_when_attachment_routing_is_form(
    tmp_path: Path,
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg = _fake_client(
        [_text(allocation), _text(''), _text('1:41 image.jpg')],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 500,
            's3_direct_uploads_enabled': False,
            's3_enabled_upload_types': {},
        },
    )

    assert (
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=str(image),
        )
        == 41
    )

    assert [request.url for request in _fake_transport(sg).requests[1:]] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/image',
        'https://x.example:443/upload/api_link_file',
    ]


def test_share_thumbnail_public_signature_preserves_forwarded_kwargs() -> None:
    signature = inspect.signature(Shotgun.share_thumbnail)

    assert tuple(signature.parameters) == (
        'self',
        'entities',
        'thumbnail_path',
        'source_entity',
        'filmstrip_thumbnail',
        'kwargs',
    )
    assert signature.parameters['kwargs'].kind is inspect.Parameter.VAR_KEYWORD
    assert signature.return_annotation is int


@pytest.mark.parametrize(
    ('thumbnail_path', 'source_entity'),
    [(None, None), ('image.jpg', {'type': 'Version', 'id': 1})],
)
def test_share_thumbnail_requires_exactly_one_source(
    thumbnail_path: str | None,
    source_entity: dict[str, Any] | None,
) -> None:
    sg = _fake_client([])

    with pytest.raises(
        ShotgunError,
        match=r'^exactly one of thumbnail_path and source_entity is required$',
    ):
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=thumbnail_path,
            source_entity=source_entity,
        )

    assert _fake_transport(sg).requests == []


def test_share_thumbnail_source_posts_exact_current_fields() -> None:
    sg = _fake_client([_text('1:31')])
    entities = [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}]

    assert (
        sg.share_thumbnail(
            entities,
            source_entity={'type': 'Version', 'id': 1},
            filmstrip_thumbnail=True,
        )
        == 31
    )

    assert entities == [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}]
    request = _fake_transport(sg).requests[1]
    assert request.url == 'https://x.example:443/upload/share_thumbnail'
    assert isinstance(request.body, bytes)
    assert dict(urllib.parse.parse_qsl(request.body.decode())) == {
        'entities': 'Version_2,Shot_7',
        'filmstrip_thumbnail': '1',
        'source_entity': 'Version_1',
        'script_name': 's',
        'script_key': 'k',
    }


def test_share_thumbnail_source_preserves_exact_endpoint_transcript() -> None:
    sg = _upload_client([_text('1:31')])
    sg.config.user_agents = ('upload-transcript',)

    assert (
        sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            source_entity={'type': 'Version', 'id': 1},
            filmstrip_thumbnail=True,
        )
        == 31
    )

    body = (
        b'entities=Version_2%2CShot_7&filmstrip_thumbnail=1&source_entity=Version_1&'
        b'script_name=script&script_key=key'
    )
    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    assert _raw_upload_transcript(transport.requests[1:], transport.bodies[1:]) == [
        (
            'POST',
            'https://x.example:443/upload/share_thumbnail',
            _upload_headers(body, 'application/x-www-form-urlencoded'),
            body,
        )
    ]


def test_thumbnail_forwarding_static_contract(tmp_path: Path) -> None:
    prefix = """import datetime
from shotgun_api3 import AsyncShotgun, Shotgun

sync = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
async_client = AsyncShotgun('https://x.example', script_name='s', api_key='k')

created = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

async def check() -> None:
"""
    positive = [
        "    sync.upload('Shot', 1, b'image.png')",
        "    await async_client.upload('Shot', 1, b'image.png')",
        (
            "    sync.share_thumbnail([{'type': 'Shot', 'id': 1}], "
            "source_entity={'type': 'Shot', 'id': 2}, ignored=object())"
        ),
        (
            "    await async_client.share_thumbnail([{'type': 'Shot', 'id': 1}], "
            "source_entity={'type': 'Shot', 'id': 2}, ignored=object())"
        ),
    ]
    negative: list[str] = []
    for client, await_prefix in (('sync', ''), ('async_client', 'await ')):
        for method in ('upload_thumbnail', 'upload_filmstrip_thumbnail'):
            call = f"    {await_prefix}{client}.{method}('Shot', 1, 'image.png', "
            positive.append(call + "display_name='Image', tag_list='a,b', created_at=created)")
            positive.append(call + 'display_name=None, tag_list=None, created_at=None)')
            negative.extend(
                call + argument + ')'
                for argument in (
                    'unexpected=True',
                    "field_name='image'",
                    'display_name=7',
                    "tag_list=['a']",
                    "created_at='2026-01-01'",
                )
            )
    repository = Path(__file__).parents[2]
    for name, lines, expected_errors in (
        ('accepted', positive, 0),
        ('rejected', negative, len(negative)),
    ):
        probe = tmp_path / f'thumbnail_{name}.py'
        probe.write_text(prefix + '\n'.join(lines) + '\n', encoding='utf-8')
        result = run_pyright(probe, cwd=repository)
        report = json.loads(result.stdout)
        assert result.returncode == (1 if expected_errors else 0), result.stdout
        assert report['summary']['errorCount'] == expected_errors, result.stdout
        if expected_errors:
            assert {entry['rule'] for entry in report['generalDiagnostics']} == {
                'reportArgumentType',
                'reportCallIssue',
            }


def test_thumbnail_source_ready_classifies_empty_transient_and_ready() -> None:
    assert not _files.thumbnail_source_ready({'entities': []}, 'image')
    assert not _files.thumbnail_source_ready(
        {'entities': [{'image': 'https://x.example/images/status/transient/processing'}]},
        'image',
    )
    assert _files.thumbnail_source_ready(
        {'entities': [{'image': 'https://x.example/image/ready.jpg'}]},
        'image',
    )


@pytest.mark.parametrize(
    ('result', 'message'),
    [
        (None, 'thumbnail readiness read did not return a dictionary'),
        ({}, 'thumbnail readiness read did not return an entities list'),
        ({'entities': [None]}, 'thumbnail readiness read returned a non-dictionary entity'),
    ],
)
def test_thumbnail_source_ready_rejects_invalid_results(
    result: object,
    message: str,
) -> None:
    with pytest.raises(ShotgunError, match=f'^{message}$'):
        _files.thumbnail_source_ready(result, 'image')


@pytest.mark.parametrize(
    ('method_name', 'expected_extra'),
    [('upload_thumbnail', {}), ('upload_filmstrip_thumbnail', {'filmstrip': 'true'})],
)
def test_thumbnail_wrappers_select_thumbnail_upload_and_forward_kwargs(
    tmp_path: Path,
    method_name: str,
    expected_extra: dict[str, str],
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    encoded = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg = _upload_client([_text(encoded), _text(), _text('1:7 image.jpg')])

    method = cast('object', getattr(sg, method_name))
    assert callable(method)
    assert method('Version', 5, str(image), display_name='Nice Image') == 7

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    allocation_fields = _form_fields(cast('bytes', transport.bodies[1]))
    assert allocation_fields['upload_type'] == 'Thumbnail'
    link_fields = _form_fields(cast('bytes', transport.bodies[3]))
    assert link_fields == {
        'entity_type': 'Version',
        'entity_id': '5',
        'display_name': 'Nice Image',
        **expected_extra,
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }


def test_unsafe_upload_endpoints_do_not_retry_ambiguous_send(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'data')
    sg = _upload_client([SendError('ambiguous'), _text('unused')])
    sg.config.rpc_attempt_interval = 0

    with pytest.raises(SendError, match='ambiguous'):
        sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    transport = _fake_transport(sg)
    assert len(transport.requests) == 2
    assert transport.requests[1].url.endswith(_files.GET_UPLOAD_LINK_INFO)

    form = _upload_client([SendError('ambiguous'), _text('unused')], _FORM_INFO)
    form.config.rpc_attempt_interval = 0

    with pytest.raises(SendError, match='ambiguous'):
        form.upload('Version', 5, str(movie), field_name='sg_uploaded_movie')

    form_transport = _fake_transport(form)
    assert len(form_transport.requests) == 2
    assert form_transport.requests[1].url.endswith(_files._UPLOAD_FILE)


@pytest.mark.parametrize(
    ('field_name', 'expected_extra'),
    [('image', {}), ('filmstrip_image', {'filmstrip': 'true'})],
)
def test_upload_display_thumbnail_fields_use_thumbnail_wire_protocol(
    tmp_path: Path,
    field_name: str,
    expected_extra: dict[str, str],
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    encoded = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg = _upload_client([_text(encoded), _text(), _text('1:7 image.jpg')])

    assert sg.upload('Task', 5, str(image), field_name) == 7

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    allocation_fields = _form_fields(cast('bytes', transport.bodies[1]))
    assert allocation_fields['upload_type'] == 'Thumbnail'
    link_fields = _form_fields(cast('bytes', transport.bodies[3]))
    assert link_fields == {
        'entity_type': 'Task',
        'entity_id': '5',
        **expected_extra,
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }


def test_upload_endpoint_exchange_preserves_exact_transcript() -> None:
    operation = _protocol.classify_endpoint('link')
    body = b'entity_type=Version&entity_id=5&display_name=Clip+Name.mov'

    exchange = _files.build_upload_endpoint_exchange(
        'https://x.example:443',
        'upload-transcript',
        operation,
        _files.LINK_FILE,
        {
            'entity_type': 'Version',
            'entity_id': '5',
            'display_name': 'Clip Name.mov',
        },
    )

    assert exchange.body_factory() == body
    assert exchange.headers == {
        'content-length': str(len(body)),
        'content-type': 'application/x-www-form-urlencoded',
        'user-agent': 'upload-transcript',
    }
    assert exchange.method == 'POST'
    assert exchange.operation is operation
    assert exchange.url == 'https://x.example:443/upload/api_link_file'
    with pytest.raises(
        ValueError,
        match=r'^server parameters must be configured before upload I/O$',
    ):
        _files.build_upload_endpoint_exchange(
            None,
            'upload-transcript',
            operation,
            _files.LINK_FILE,
            {},
        )


def test_upload_helpers_preserve_the_evidence_backed_wire_grammar() -> None:
    created_at = datetime.datetime(2026, 7, 18, 9, 10, 11, tzinfo=datetime.UTC)
    encoded = '1 https://storage.example/object 2026-07-18T09:10:11+00:00 Attachment upload-1'
    info = _files.UploadLinkInfo(encoded, multipart=False)
    entity_fields = {'entity_type': 'Version', 'entity_id': '5'}

    assert _files.build_upload_link_fields(
        'clip.mov', 'Attachment', multipart=False, created_at=created_at
    ) == {
        'filename': 'clip.mov',
        'upload_type': 'Attachment',
        'multipart_upload': 'false',
        'created_at': '2026-07-18T09:10:11+00:00',
    }
    assert info.encoded is encoded
    assert info.upload_url == 'https://storage.example/object'
    assert info.timestamp == '2026-07-18T09:10:11+00:00'
    assert info.upload_type == 'Attachment'
    assert info.upload_id == 'upload-1'
    assert _files.build_link_file_fields(entity_fields, info.encoded) == {
        'entity_type': 'Version',
        'entity_id': '5',
        'upload_link_info': encoded,
    }
    assert entity_fields == {'entity_type': 'Version', 'entity_id': '5'}
    assert _files.build_part_fields(info, 'clip.mov', 3) == {
        'filename': 'clip.mov',
        'part_number': '3',
        'timestamp': '2026-07-18T09:10:11+00:00',
        'upload_id': 'upload-1',
        'upload_type': 'Attachment',
    }
    assert _files.build_complete_fields(info, 'clip.mov', ['etag-1', 'etag-2']) == {
        'etags': 'etag-1,etag-2',
        'filename': 'clip.mov',
        'timestamp': '2026-07-18T09:10:11+00:00',
        'upload_id': 'upload-1',
        'upload_type': 'Attachment',
    }
    assert _files.parse_part_link('1 https://storage.example/part') == (
        'https://storage.example/part'
    )
    assert _files.parse_multipart_completion('17\n') == 17
    assert _files.parse_link_completion('1:42 Display Name.mov\n') == 42


@pytest.mark.parametrize(
    ('encoded', 'multipart'),
    [
        ('0 https://storage.example/object timestamp Attachment upload-1', False),
        ('1 https://storage.example/object timestamp Attachment', False),
        ('1 https://storage.example/object timestamp Attachment upload-1 extra', False),
        ('1 https://storage.example/object timestamp Attachment upload-1', True),
    ],
)
def test_upload_link_info_rejects_unobserved_response_shapes(
    encoded: str,
    multipart: bool,
) -> None:
    with pytest.raises(ShotgunError, match=r'^upload allocation returned an invalid response$'):
        _files.UploadLinkInfo(encoded, multipart=multipart)


@pytest.mark.parametrize('byte_path', [False, True], ids=['unicode-str', 'utf8-bytes'])
def test_upload_accepts_unicode_and_utf8_byte_paths(tmp_path: Path, byte_path: bool) -> None:
    source = tmp_path / 'caf\u00e9_\u65e5\u672c.jpg'
    source.write_bytes(b'image-data')
    path = str(source).encode('utf-8') if byte_path else str(source)
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg = _upload_client([_text(encoded), _text(), _text('1:42 linked')])

    assert sg.upload('Version', 5, path, 'sg_uploaded_movie') == 42

    transport = cast('_ConsumingFakeTransport', _fake_transport(sg))
    assert transport.bodies[2] == b'image-data'
    assert _form_fields(cast('bytes', transport.bodies[1]))['filename'] == source.name


def test_upload_public_signatures_match_the_observed_surface() -> None:
    assert inspect.signature(Shotgun.upload) == inspect.Signature(
        [
            inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter(
                'entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str
            ),
            inspect.Parameter(
                'entity_id',
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=int,
            ),
            inspect.Parameter(
                'path', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str | bytes
            ),
            inspect.Parameter(
                'field_name',
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=None,
                annotation=str | None,
            ),
            inspect.Parameter(
                'display_name',
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=None,
                annotation=str | None,
            ),
            inspect.Parameter(
                'tag_list',
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=None,
                annotation=str | None,
            ),
            inspect.Parameter(
                'created_at',
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=None,
                annotation=datetime.datetime | None,
            ),
        ],
        return_annotation=int,
    )
    for method in (Shotgun.upload_thumbnail, Shotgun.upload_filmstrip_thumbnail):
        assert tuple(inspect.signature(method).parameters) == (
            'self',
            'entity_type',
            'entity_id',
            'path',
            'kwargs',
        )


def test_upload_rejects_invalid_created_at_before_network(tmp_path: Path) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    sg = _upload_client([])
    upload = cast('Callable[..., int]', sg.upload)

    with pytest.raises(ShotgunError, match=r'^created_at must be a datetime\.datetime instance$'):
        upload('Version', 5, str(image), created_at='2026-02-15T10:30:00Z')

    assert _fake_transport(sg).requests == []


@pytest.mark.parametrize('kind', ['missing', 'empty'])
def test_upload_rejects_invalid_files_before_http(tmp_path: Path, kind: str) -> None:
    path = tmp_path / 'clip.mov'
    if kind == 'empty':
        path.touch()
    sg = _fake_client([])

    with pytest.raises(ShotgunError, match=r'^File must exist and be non-empty$'):
        sg.upload('Version', 5, str(path), field_name='sg_uploaded_movie')

    assert _fake_transport(sg).requests == []
