import asyncio
import dataclasses
import inspect
import json
import os
import tempfile
import threading
import traceback
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from pathlib import Path
from typing import Any, BinaryIO, Literal, ParamSpec, Self, TypeAlias, TypeVar, cast
from unittest.mock import patch

import pytest

from shotgun_api3 import (
    AsyncShotgun,
    Shotgun,
    _errors,
    _files,
    _protocol,
)
from shotgun_api3._async_transport import AsyncStreamingResponse
from shotgun_api3._config import ProgressEvent
from shotgun_api3._errors import ShotgunError
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    ReadTimeoutError,
    SendError,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _AsyncFakeTransport
from tests.conftest import _fake_async_transport, _make_response, _set_fake_async_transport

pytestmark = pytest.mark.async_required

_CREDENTIAL_VALUE = 'credential-value'
_DIRECT_INFO: dict[str, object] = {
    'version': [9, 0, 0],
    'api_max_entities_per_page': 500,
    's3_direct_uploads_enabled': True,
    's3_enabled_upload_types': {'*': ['*']},
}
_FILE_BATCH_SIZE = 1024 * 1024
_FILE_FINAL_BATCH_SIZE = 77_057
_FILE_TRANSFER_SIZE = 20_000_001
_FORM_INFO: dict[str, object] = {
    'version': [9, 0, 0],
    'api_max_entities_per_page': 500,
    's3_direct_uploads_enabled': False,
    's3_enabled_upload_types': {},
}
_FORM_TRANSFER_SIZE = 65_537
_FORM_UPLOAD_BOUNDARY = 'Task13Boundary'
_MULTIPART_FIRST_FINAL_BATCH_SIZE = 77_056
_ONE_USE_VALUE = 'one-use-value'
_SESSION_VALUE = 'session-one'

FakeAsyncClient = Callable[..., AsyncShotgun]
_CallParams = ParamSpec('_CallParams')
_CallResult = TypeVar('_CallResult')
_ScriptItem: TypeAlias = (
    'TransportResponse | TransportError | _PartialUploadFailure | _StreamScript'
)
_StreamItem: TypeAlias = bytes | TransportError
_WorkerPhase: TypeAlias = Literal['close', 'flush', 'open', 'replace', 'write']


class _FilesystemAudit:
    def __init__(self, loop_thread: int) -> None:
        self._loop_thread = loop_thread
        self._original_is_file = Path.is_file
        self._original_named_temporary = cast(
            'Callable[..., object]',
            tempfile.NamedTemporaryFile,
        )
        self._original_open = cast('Callable[..., object]', Path.open)
        self._original_replace = Path.replace
        self._original_stat = cast('Callable[..., os.stat_result]', Path.stat)
        self._original_unlink = Path.unlink
        self.observed: list[tuple[str, int]] = []
        self.read_sizes: list[int] = []
        self.write_sizes: list[int] = []

    def assert_complete(self) -> None:
        operations = {operation for operation, _thread_id in self.observed}
        assert {
            'close',
            'flush',
            'is_file',
            'open',
            'read',
            'replace',
            'stat',
            'temporary',
            'unlink',
            'write',
        } <= operations
        assert all(thread_id != self._loop_thread for _operation, thread_id in self.observed)

    def record(self, operation: str) -> None:
        self.observed.append((operation, threading.get_ident()))

    def _checked_is_file(self, path: Path) -> bool:
        self.record('is_file')
        return self._original_is_file(path)

    def _checked_named_temporary(
        self,
        *args: object,
        **kwargs: object,
    ) -> '_ObservedFile':
        self.record('temporary')
        return _ObservedFile(
            self._original_named_temporary(*args, **kwargs),
            self,
        )

    def _checked_open(
        self,
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> '_ObservedFile':
        self.record('open')
        return _ObservedFile(self._original_open(path, *args, **kwargs), self)

    def _checked_replace(self, path: Path, target: Path) -> Path:
        self.record('replace')
        return self._original_replace(path, target)

    def _checked_stat(
        self,
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> os.stat_result:
        self.record('stat')
        return self._original_stat(path, *args, **kwargs)

    def _checked_unlink(self, path: Path, missing_ok: bool = False) -> None:
        self.record('unlink')
        self._original_unlink(path, missing_ok=missing_ok)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:

        def checked_is_file(path: Path) -> bool:
            return self._checked_is_file(path)

        def checked_open(path: Path, *args: object, **kwargs: object) -> _ObservedFile:
            return self._checked_open(path, *args, **kwargs)

        def checked_replace(path: Path, target: Path) -> Path:
            return self._checked_replace(path, target)

        def checked_stat(
            path: Path,
            *args: object,
            **kwargs: object,
        ) -> os.stat_result:
            return self._checked_stat(path, *args, **kwargs)

        def checked_unlink(path: Path, missing_ok: bool = False) -> None:
            self._checked_unlink(path, missing_ok=missing_ok)

        monkeypatch.setattr(Path, 'is_file', checked_is_file)
        monkeypatch.setattr(Path, 'open', checked_open)
        monkeypatch.setattr(Path, 'replace', checked_replace)
        monkeypatch.setattr(Path, 'stat', checked_stat)
        monkeypatch.setattr(Path, 'unlink', checked_unlink)
        monkeypatch.setattr(
            tempfile,
            'NamedTemporaryFile',
            self._checked_named_temporary,
        )


class _FilesystemBarrier:
    def __init__(
        self,
        phase: _WorkerPhase | None,
        errors: Mapping[str, BaseException] | None = None,
    ) -> None:
        self._errors = dict(errors or {})
        self._lock = threading.Lock()
        self._original_named_temporary = cast(
            'Callable[..., BinaryIO]',
            tempfile.NamedTemporaryFile,
        )
        self._original_replace = Path.replace
        self._original_unlink = Path.unlink
        self._phase = phase
        self.active = 0
        self.after_release = threading.Event()
        self.counts: dict[str, int] = {}
        self.finish = threading.Event()
        self.idle = threading.Event()
        self.idle.set()
        self.release = threading.Event()
        self.started = threading.Event()

    def run(
        self,
        operation: str,
        function: Callable[_CallParams, _CallResult],
        *args: _CallParams.args,
        **kwargs: _CallParams.kwargs,
    ) -> _CallResult:
        with self._lock:
            self.active += 1
            self.idle.clear()
            self.counts[operation] = self.counts.get(operation, 0) + 1
        try:
            if operation == self._phase:
                self.started.set()
                assert self.release.wait(5)
                self.after_release.set()
                assert self.finish.wait(5)
            result = function(*args, **kwargs)
            error = self._errors.get(operation)
            if error is not None:
                raise error
            return result
        finally:
            with self._lock:
                self.active -= 1
                if self.active == 0:
                    self.idle.set()

    def _named_temporary(self, *args: object, **kwargs: object) -> '_BarrierFile':
        handle = self.run('open', self._original_named_temporary, *args, **kwargs)
        return _BarrierFile(handle, self)

    def _replace(self, path: Path, target: Path) -> Path:
        return self.run('replace', self._original_replace, path, target)

    def _unlink(self, path: Path, missing_ok: bool = False) -> None:
        self.run('unlink', self._original_unlink, path, missing_ok=missing_ok)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:

        def checked_replace(path: Path, target: Path) -> Path:
            return self._replace(path, target)

        def checked_unlink(path: Path, missing_ok: bool = False) -> None:
            self._unlink(path, missing_ok=missing_ok)

        monkeypatch.setattr(tempfile, 'NamedTemporaryFile', self._named_temporary)
        monkeypatch.setattr(Path, 'replace', checked_replace)
        monkeypatch.setattr(Path, 'unlink', checked_unlink)


@dataclasses.dataclass(frozen=True)
class _PartialUploadFailure:
    error: TransportError


@dataclasses.dataclass(frozen=True)
class _StreamOptions:
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict[str, str])
    status: int = 200
    blocker: asyncio.Event | None = None
    started: asyncio.Event | None = None


@dataclasses.dataclass(frozen=True)
class _StreamScript:
    chunks: tuple[_StreamItem, ...]
    label: str
    releases: list[str]
    requested_sizes: list[int] = dataclasses.field(default_factory=list[int])
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict[str, str])
    status: int = 200
    blocker: asyncio.Event | None = None
    started: asyncio.Event | None = None


class _AsyncFileTransport(_AsyncFakeTransport):
    def __init__(
        self,
        responses: list[_ScriptItem],
        on_send: Callable[[TransportRequest], None] | None = None,
    ) -> None:
        super().__init__([])
        self._on_send = on_send
        self._file_responses = list(responses)
        self.bodies: list[bytes | None] = []
        self.body_ids: list[int | None] = []

    @staticmethod
    def _consume_body(body: bytes | Iterator[bytes] | None) -> bytes | None:
        if body is None or isinstance(body, bytes):
            return body
        return b''.join(body)

    def _next(self, request: TransportRequest) -> _ScriptItem:
        self.requests.append(request)
        if self._on_send is not None:
            self._on_send(request)
        if not self._file_responses:
            raise AssertionError('async file transport responses exhausted')
        return self._file_responses.pop(0)

    async def send(self, request: TransportRequest) -> TransportResponse:
        outcome = self._next(request)
        body = request.body
        self.body_ids.append(None if body is None else id(body))
        if isinstance(outcome, _PartialUploadFailure):
            assert body is not None
            assert not isinstance(body, bytes)
            self.bodies.append(await asyncio.to_thread(next, body))
            raise outcome.error
        self.bodies.append(await asyncio.to_thread(self._consume_body, body))
        if isinstance(outcome, TransportError):
            raise outcome
        assert isinstance(outcome, TransportResponse)
        return outcome

    async def send_streaming(self, request: TransportRequest) -> AsyncStreamingResponse:
        outcome = self._next(request)
        self.body_ids.append(None)
        self.bodies.append(None)
        if isinstance(outcome, TransportError):
            raise outcome
        assert isinstance(outcome, _StreamScript)

        async def stream(size: int) -> AsyncIterator[bytes]:
            outcome.requested_sizes.append(size)
            for item in outcome.chunks:
                if isinstance(item, bytes):
                    for offset in range(0, len(item), size):
                        yield item[offset : offset + size]
                else:
                    raise item
            if outcome.started is not None:
                outcome.started.set()
            if outcome.blocker is not None:
                await outcome.blocker.wait()

        async def close_fn() -> None:
            outcome.releases.append(outcome.label)

        return AsyncStreamingResponse(
            status=outcome.status,
            reason='OK' if outcome.status == 200 else 'Request failed',
            headers=CaseInsensitiveHeaders(outcome.headers),
            stream=stream,
            close_fn=close_fn,
        )


class _BarrierFile:
    def __init__(self, handle: BinaryIO, barrier: _FilesystemBarrier) -> None:
        self._barrier = barrier
        self._handle = handle

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._barrier.run('close', self._handle.close)

    def flush(self) -> None:
        self._barrier.run('flush', self._handle.flush)

    @property
    def name(self) -> str:
        return cast('str', self._handle.name)

    def write(self, content: bytes) -> int:
        return self._barrier.run('write', self._handle.write, content)


class _ObservedFile:
    def __init__(self, handle: object, audit: _FilesystemAudit) -> None:
        self._audit = audit
        self._handle = handle

    def __enter__(self) -> Self:
        self._audit.record('enter')
        return self

    def __getattr__(
        self,
        name: str,
    ) -> Any:  # noqa: ANN401 - Transparently proxy the wrapped file object's attribute type.
        attribute = getattr(self._handle, name)
        if not callable(attribute) or name not in {
            'close',
            'flush',
            'read',
            'seek',
            'write',
        }:
            return attribute

        def observed(*args: object, **kwargs: object) -> object:
            self._audit.record(name)
            result = attribute(*args, **kwargs)
            if name == 'read':
                self._audit.read_sizes.append(len(cast('bytes', result)))
            elif name == 'write':
                self._audit.write_sizes.append(len(cast('bytes', args[0])))
            return result

        return observed

    def __exit__(self, *exc: object) -> None:
        close = cast('Callable[[], None]', self.__getattr__('close'))
        close()

    @property
    def name(self) -> str:
        return cast('str', self.__getattr__('name'))


def _client(
    responses: list[_ScriptItem],
    *,
    base_url: str = 'https://x.example',
    on_send: Callable[[TransportRequest], None] | None = None,
    server_info: dict[str, object] | None = _DIRECT_INFO,
    session_token: str | None = None,
) -> tuple[AsyncShotgun, _AsyncFileTransport]:
    sg = AsyncShotgun(
        base_url,
        script_name=None if session_token is not None else 'script',
        api_key=None if session_token is not None else 'key',
        session_token=session_token,
        connect=False,
    )
    scripted = responses if server_info is None else [_make_response(server_info), *responses]
    transport = _AsyncFileTransport(scripted, on_send)
    _set_fake_async_transport(sg, transport)
    sg.config.rpc_attempt_interval = 0
    return sg, transport


def _fault_response(code: int) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
        content=json.dumps(
            {
                'exception': True,
                'error_code': code,
                'message': 'download setup fault',
            }
        ).encode(),
    )


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


def _method_name(request: TransportRequest) -> str | None:
    if request.url.endswith('/api3/json') and isinstance(request.body, bytes):
        payload = cast('dict[str, object]', json.loads(request.body))
        return cast('str', payload['method_name'])
    return None


def _one_use_client(
    responses: list[_ScriptItem],
) -> tuple[AsyncShotgun, _AsyncFileTransport]:
    sg = AsyncShotgun(
        'https://x.example',
        login='artist',
        password=_CREDENTIAL_VALUE,
        auth_token=_ONE_USE_VALUE,
        connect=False,
    )
    transport = _AsyncFileTransport([_make_response(_DIRECT_INFO), *responses])
    _set_fake_async_transport(sg, transport)
    sg.config.rpc_attempt_interval = 0
    return sg, transport


def _raw_upload_transcript(
    requests: list[TransportRequest],
    bodies: list[bytes | None],
) -> list[tuple[str, str, dict[str, str], bytes | None]]:
    return [
        (request.method, request.url, dict(request.headers), body)
        for request, body in zip(requests, bodies, strict=True)
    ]


def _session_only_fields(body: bytes) -> None:
    fields = _form_fields(body)
    assert fields['session_token'] == _SESSION_VALUE
    assert not {'auth_token', 'user_login', 'user_password'} & fields.keys()


def _stream(
    chunks: tuple[_StreamItem, ...],
    label: str,
    releases: list[str],
    *,
    options: _StreamOptions | None = None,
) -> _StreamScript:
    controls = options or _StreamOptions()
    return _StreamScript(
        chunks=chunks,
        label=label,
        releases=releases,
        headers=controls.headers,
        status=controls.status,
        blocker=controls.blocker,
        started=controls.started,
    )


def _text(
    content: str = '',
    *,
    headers: Mapping[str, str] | None = None,
    status: int = 200,
) -> TransportResponse:
    return TransportResponse(
        status=status,
        reason='OK' if status == 200 else 'Request failed',
        headers=CaseInsensitiveHeaders(headers or {'content-type': 'text/plain'}),
        content=content.encode(),
    )


def _upload_headers(
    body: bytes,
    content_type: str,
) -> dict[str, str]:
    return {
        'content-length': str(len(body)),
        'content-type': content_type,
        'user-agent': 'upload-transcript',
    }


@pytest.mark.anyio
async def test_auth_cookie_handler_is_exact_origin_and_generation_bound() -> None:
    cached = AsyncShotgun(
        'https://x.example:8443',
        session_token=_SESSION_VALUE,
        connect=False,
    )
    cached_transport = _AsyncFileTransport([])
    _set_fake_async_transport(cached, cached_transport)
    cached_handler = await cached.get_auth_cookie_handler()
    cookies = list(cached_handler.cookiejar)
    assert len(cookies) == 1
    assert cookies[0].domain == 'x.example'
    assert not cookies[0].domain_specified
    assert cookies[0].secure
    assert cached_transport.requests == []
    same_origin = urllib.request.Request('https://x.example:8443/asset')
    cached_handler.https_request(same_origin)
    assert same_origin.get_header('Cookie') == '_session_id=session-one'
    off_origin = urllib.request.Request('https://sub.x.example:8443/asset')
    cached_handler.https_request(off_origin)
    assert off_origin.get_header('Cookie') is None

    http = AsyncShotgun(
        'http://x.example:8080',
        session_token=_SESSION_VALUE,
        connect=False,
    )
    http_transport = _AsyncFileTransport([])
    _set_fake_async_transport(http, http_transport)
    http_handler = await http.get_auth_cookie_handler()
    assert not next(iter(http_handler.cookiejar)).secure
    assert http_transport.requests == []

    changed: AsyncShotgun

    def change_origin(request: TransportRequest) -> None:
        if _method_name(request) == 'get_session_token':
            changed.config.set_server_params('http://new.example:8080')

    changed, changed_transport = _client(
        [_make_response({'session_id': 'old-origin-token'})],
        on_send=change_origin,
    )
    changed_handler = await changed.get_auth_cookie_handler()
    changed_cookies = list(changed_handler.cookiejar)
    assert len(changed_cookies) == 1
    assert changed_cookies[0].domain == 'x.example'
    assert changed_cookies[0].secure
    assert changed.config.session_token is None
    assert [_method_name(request) for request in changed_transport.requests] == [
        'info',
        'get_session_token',
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('site_url', 'expected_url'),
    [
        (
            'https://new.example/shotgun/',
            'https://new.example/shotgun/file_serve/attachment/41',
        ),
        (
            'https://new.example:443/shotgun/',
            'https://new.example:443/shotgun/file_serve/attachment/41',
        ),
        (
            'https://new.example:8443/shotgun/',
            'https://new.example:8443/shotgun/file_serve/attachment/41',
        ),
    ],
)
async def test_attachment_urls_use_current_site_without_auth_io(
    site_url: str,
    expected_url: str,
) -> None:
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        base_url='https://old.example',
        session_token=_SESSION_VALUE,
    )
    sg.config.set_server_params(site_url)
    external = {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }

    assert await sg.get_attachment_download_url(41) == expected_url
    assert await sg.get_attachment_download_url(external) == external['url']
    assert external == {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }
    assert transport.requests == []

    sg.config.session_token = _SESSION_VALUE
    assert await sg.download_attachment(41) == b'DATA'
    assert transport.requests[-1].url == expected_url
    assert releases == ['download']


@pytest.mark.anyio
@pytest.mark.parametrize('url', ['file:///unused', 'http://[::1', 'https:///missing-host'])
async def test_download_rejects_invalid_url_before_io(url: str, tmp_path: Path) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    sg, transport = _client([], session_token=_SESSION_VALUE)
    events: list[object] = []
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    with (
        patch.object(sg, '_acquire_driver', wraps=sg._acquire_driver) as acquire,
        patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease,
        patch('shotgun_api3._async_driver.asyncio.sleep') as wait,
    ):
        with pytest.raises(
            _errors.ShotgunFileDownloadError,
            match=r'^attachment download failed$',
        ):
            await sg.download_attachment({'url': url}, file_path=str(destination))
        acquire.assert_not_called()
        lease.assert_not_called()
        wait.assert_not_called()

    assert transport.requests == []
    assert events == []
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]


@pytest.mark.anyio
async def test_default_multipart_upload_batches_each_storage_range(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    content = b'a' * (_FILE_TRANSFER_SIZE - 1) + b'b'
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(content)
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    encoded = '1 https://storage.example/multipart timestamp Attachment upload-1 opaque'
    sg, transport = _client(
        [
            _text(encoded),
            _text('1 https://storage.example/part-1'),
            _text(headers={'etag': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'etag': 'etag-2'}),
            _text('17'),
            _text('1:9 linked'),
        ]
    )

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    uploaded_parts = [
        body
        for request, body in zip(transport.requests, transport.bodies, strict=True)
        if request.url.startswith('https://storage.example/part-')
    ]
    assert uploaded_parts == [content[:20_000_000], content[20_000_000:]]
    assert b''.join(cast('list[bytes]', uploaded_parts)) == content
    assert len(audit.read_sizes) <= 21
    assert audit.read_sizes == [_FILE_BATCH_SIZE] * 19 + [
        _MULTIPART_FIRST_FINAL_BATCH_SIZE,
        1,
    ]


@pytest.mark.anyio
async def test_direct_upload_replays_fresh_complete_body(tmp_path: Path) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'restartable')
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg, transport = _client(
        [
            _text(encoded),
            _PartialUploadFailure(SendError('ambiguous object PUT')),
            _text(),
            _text('1:9 linked'),
        ]
    )

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    object_puts = [
        (request, body, body_id)
        for request, body, body_id in zip(
            transport.requests,
            transport.bodies,
            transport.body_ids,
            strict=True,
        )
        if request.url == 'https://storage.example/object'
    ]
    assert [body for _request, body, _body_id in object_puts] == [
        b'restartable',
        b'restartable',
    ]
    assert object_puts[0][2] != object_puts[1][2]


@pytest.mark.anyio
async def test_direct_upload_uses_allocation_storage_and_link_routes(
    tmp_path: Path,
) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'clip-data')
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg, transport = _client([_text(encoded), _text(), _text('1:9 linked')])
    sg.config.user_agents = ('upload-transcript',)

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    requests = transport.requests[1:]
    allocation_body = (
        b'filename=clip.mov&upload_type=Attachment&multipart_upload=false&'
        b'script_name=script&script_key=key'
    )
    link_body = (
        b'entity_type=Version&entity_id=5&field_name=sg_uploaded_movie&'
        b'display_name=clip.mov&upload_link_info=1+https%3A%2F%2Fstorage.example%2Fobject+'
        b'timestamp+Attachment+upload-1&script_name=script&script_key=key'
    )
    assert [request.url for request in requests] == [
        'https://x.example:443/upload/api_get_upload_link_info',
        'https://storage.example/object',
        'https://x.example:443/upload/api_link_file',
    ]
    assert [request.method for request in requests] == ['POST', 'PUT', 'POST']
    assert [request.headers for request in requests] == [
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


@pytest.mark.anyio
async def test_direct_upload_batches_worker_file_pulls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', _FILE_TRANSFER_SIZE + 1)
    content = b'a' * (_FILE_TRANSFER_SIZE - 1) + b'b'
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(content)
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg, transport = _client([_text(encoded), _text(), _text('1:9 linked')])
    events: list[object] = []
    sg.config.on_progress = events.append

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    uploaded = next(
        body
        for request, body in zip(transport.requests, transport.bodies, strict=True)
        if request.url == 'https://storage.example/object'
    )
    assert uploaded == content
    assert len(audit.read_sizes) <= 21
    assert audit.read_sizes == [_FILE_BATCH_SIZE] * 19 + [_FILE_FINAL_BATCH_SIZE]
    progress = [
        event
        for event in events
        if isinstance(event, ProgressEvent) and event.route == 'object_put'
    ]
    assert progress[-1].bytes_transferred == _FILE_TRANSFER_SIZE
    assert progress[-1].total_bytes == _FILE_TRANSFER_SIZE


@pytest.mark.anyio
async def test_file_download_batches_worker_writes_and_publishes_atomically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    content = b'a' * (_FILE_TRANSFER_SIZE - 1) + b'b'
    releases: list[str] = []
    stream = _stream(
        (content,),
        'download',
        releases,
        options=_StreamOptions(headers={'content-length': str(_FILE_TRANSFER_SIZE)}),
    )
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    sg, transport = _client([stream], session_token=_SESSION_VALUE)
    events: list[object] = []
    sg.config.on_progress = events.append

    assert await sg.download_attachment(41, file_path=str(destination)) == str(destination)

    assert stream.requested_sizes == [_FILE_BATCH_SIZE]
    assert len(audit.write_sizes) <= 21
    assert audit.write_sizes == [_FILE_BATCH_SIZE] * 19 + [_FILE_FINAL_BATCH_SIZE]
    operations = [operation for operation, _thread_id in audit.observed]
    assert operations.count('close') == 1
    assert operations.count('replace') == 1
    assert operations.count('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == content
    progress = [
        event for event in events if isinstance(event, ProgressEvent) and event.route == 'download'
    ]
    assert progress[-1].bytes_transferred == _FILE_TRANSFER_SIZE
    assert progress[-1].total_bytes == _FILE_TRANSFER_SIZE
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_file_download_progress_uses_response_content_length() -> None:
    events: list[object] = []
    releases: list[str] = []
    stream = _stream(
        (b'DATA',),
        'download',
        releases,
        options=_StreamOptions(headers={'Content-Length': '4'}),
    )
    sg, transport = _client([stream], session_token=_SESSION_VALUE)
    sg.config.on_progress = events.append

    assert await sg.download_attachment(41) == b'DATA'

    progress = [
        event for event in events if isinstance(event, ProgressEvent) and event.route == 'download'
    ]
    assert [(event.bytes_transferred, event.total_bytes) for event in progress] == [(4, 4)]
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_download_cancellation_closes_once_and_preserves_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    releases: list[str] = []
    blocker = asyncio.Event()
    started = asyncio.Event()
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    sg, transport = _client(
        [
            _stream(
                (b'partial',),
                'blocked',
                releases,
                options=_StreamOptions(blocker=blocker, started=started),
            )
        ],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    await asyncio.wait_for(started.wait(), timeout=1)

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    close_threads = [thread_id for operation, thread_id in audit.observed if operation == 'close']
    unlink_threads = [
        thread_id for operation, thread_id in audit.observed if operation == 'unlink'
    ]
    assert len(close_threads) == len(unlink_threads) == 1
    assert close_threads[0] != threading.get_ident()
    assert unlink_threads[0] != threading.get_ident()
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
    assert releases == ['blocked']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_download_cancellation_during_cleanup_replaces_transfer_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier('close', {'write': ValueError('write failed')})
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    assert await asyncio.to_thread(barrier.started.wait, 5)

    operation.cancel('first cancellation')
    for _ in range(5):
        await asyncio.sleep(0)
    operation.cancel('second cancellation')
    barrier.release.set()
    assert await asyncio.to_thread(barrier.after_release.wait, 5)
    for _ in range(5):
        await asyncio.sleep(0)
    joined_before_return = not operation.done()
    barrier.finish.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await operation
    assert caught.value.args == ('first cancellation',)
    assert await asyncio.to_thread(barrier.idle.wait, 5)

    assert joined_before_return
    assert barrier.active == 0
    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('phase', 'expected_content'),
    [
        ('open', b'existing'),
        ('write', b'existing'),
        ('flush', b'existing'),
        ('close', b'existing'),
        ('replace', b'DATA'),
    ],
)
async def test_download_cancellation_joins_each_filesystem_worker_before_return(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: _WorkerPhase,
    expected_content: bytes,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier(phase)
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    assert await asyncio.to_thread(barrier.started.wait, 5)

    operation.cancel()
    barrier.release.set()
    assert await asyncio.to_thread(barrier.after_release.wait, 5)
    for _ in range(5):
        await asyncio.sleep(0)
    joined_before_return = not operation.done()
    pre_finish_close_count = barrier.counts.get('close', 0)
    pre_finish_unlink_count = barrier.counts.get('unlink', 0)
    barrier.finish.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert await asyncio.to_thread(barrier.idle.wait, 5)

    assert joined_before_return
    assert pre_finish_close_count == (1 if phase in {'close', 'replace'} else 0)
    assert pre_finish_unlink_count == 0
    assert barrier.active == 0
    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == expected_content
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
    assert releases == ['download']
    assert transport.leases == transport.checkins
    settled_content = await asyncio.to_thread(destination.read_bytes)
    await asyncio.sleep(0)
    assert await asyncio.to_thread(destination.read_bytes) == settled_content


@pytest.mark.anyio
async def test_download_cancellation_remains_primary_when_joined_worker_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier('write', {'write': ValueError('write failed')})
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    assert await asyncio.to_thread(barrier.started.wait, 5)

    operation.cancel()
    barrier.release.set()
    assert await asyncio.to_thread(barrier.after_release.wait, 5)
    barrier.finish.set()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_download_cancellation_survives_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier(
        'write',
        {
            'close': RuntimeError('close failed'),
            'unlink': RuntimeError('unlink failed'),
        },
    )
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    assert await asyncio.to_thread(barrier.started.wait, 5)

    operation.cancel()
    barrier.release.set()
    assert await asyncio.to_thread(barrier.after_release.wait, 5)
    barrier.finish.set()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
@pytest.mark.parametrize('error_type', [OSError, RuntimeError, SendError])
async def test_download_cleanup_only_failure_is_publicly_redacted(
    error_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier(
        None, {'unlink': error_type('secret backend unlink text request-secret')}
    )
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )

    sg.config.max_rpc_attempts = 3
    events: list[object] = []
    retries: list[object] = []
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = retries.append

    with pytest.raises(
        _errors.ShotgunFileDownloadError,
        match=r'^attachment download failed$',
    ) as caught:
        await sg.download_attachment(41, file_path=str(destination))

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    diagnostic = ''.join(traceback.format_exception(caught.value)) + repr(events + retries)
    assert 'secret backend' not in diagnostic
    assert 'request-secret' not in diagnostic
    assert retries == []
    assert len(transport.requests) == 2
    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'DATA'
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
@pytest.mark.parametrize('error_type', [OSError, RuntimeError, SendError])
async def test_download_close_failure_still_unlinks_temporary(
    error_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier(
        None, {'close': error_type('secret backend close text request-secret')}
    )
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )

    sg.config.max_rpc_attempts = 3
    events: list[object] = []
    retries: list[object] = []
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = retries.append

    with pytest.raises(
        _errors.ShotgunFileDownloadError,
        match=r'^attachment download failed$',
    ) as caught:
        await sg.download_attachment(41, file_path=str(destination))

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    diagnostic = ''.join(traceback.format_exception(caught.value)) + repr(events + retries)
    assert 'secret backend' not in diagnostic
    assert 'request-secret' not in diagnostic
    assert retries == []
    assert len(transport.requests) == 2
    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_download_exhaustion_redacts_signed_url() -> None:
    signed_url = 'https://storage.example/object?X-Amz-Signature=secret-value'
    sg, _transport = _client(
        [SendError(signed_url), SendError(signed_url), SendError(signed_url)],
        server_info=None,
    )

    with pytest.raises(
        _errors.ShotgunFileDownloadError,
        match=r'^attachment download failed$',
    ) as caught:
        await sg.download_attachment({'url': signed_url})

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert 'secret-value' not in str(caught.value)


@pytest.mark.anyio
async def test_download_first_transfer_cancellation_wins_during_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    blocker = asyncio.Event()
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    barrier = _FilesystemBarrier('close')
    barrier.install(monkeypatch)
    releases: list[str] = []
    started = asyncio.Event()
    sg, transport = _client(
        [
            _stream(
                (b'DATA',),
                'download',
                releases,
                options=_StreamOptions(blocker=blocker, started=started),
            )
        ],
        session_token=_SESSION_VALUE,
    )
    operation = asyncio.create_task(sg.download_attachment(41, file_path=str(destination)))
    startup = asyncio.create_task(started.wait())
    try:
        done, _ = await asyncio.wait(
            {startup, operation}, timeout=5, return_when=asyncio.FIRST_COMPLETED
        )
        assert done, 'download did not start within the deadlock watchdog'
        if operation in done:
            await operation
            pytest.fail('download completed before the stream started')
        assert started.is_set()
        assert not operation.done()

        operation.cancel('first cancellation')
        assert await asyncio.to_thread(barrier.started.wait, 5)
        operation.cancel('second cancellation')
        barrier.release.set()
        assert await asyncio.to_thread(barrier.after_release.wait, 5)
        done, _ = await asyncio.wait({operation}, timeout=0.1)
        assert not done
        barrier.finish.set()

        with pytest.raises(asyncio.CancelledError) as caught:
            await operation
        assert caught.value.args == ('first cancellation',)
        assert await asyncio.to_thread(barrier.idle.wait, 5)
        assert barrier.counts.get('close') == 1
        assert barrier.counts.get('unlink') == 1
        assert await asyncio.to_thread(destination.read_bytes) == b'existing'
        assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
        assert releases == ['download']
        assert transport.leases == transport.checkins
    finally:
        blocker.set()
        barrier.release.set()
        barrier.finish.set()
        startup.cancel()
        if not operation.done():
            operation.cancel('test teardown')
        await asyncio.wait_for(
            asyncio.gather(startup, operation, return_exceptions=True), timeout=5
        )


@pytest.mark.anyio
async def test_download_off_origin_skips_readiness_token_and_cookie() -> None:
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'PAGE',), 'page', releases)],
        server_info=None,
    )

    result = await sg.download_attachment(
        {'link_type': 'web', 'url': 'https://elsewhere.example/page?token=secret'}
    )

    assert result == b'PAGE'
    assert [_method_name(request) for request in transport.requests] == [None]
    assert 'cookie' not in transport.requests[0].headers
    assert releases == ['page']


@pytest.mark.anyio
@pytest.mark.parametrize('error_type', [OSError, RuntimeError, SendError])
async def test_download_primary_failure_survives_cleanup_failures(
    error_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    primary = ValueError('write failed')
    barrier = _FilesystemBarrier(
        None,
        {
            'close': error_type('secret close failed'),
            'unlink': error_type('secret unlink failed'),
            'write': primary,
        },
    )
    barrier.install(monkeypatch)
    releases: list[str] = []
    sg, transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(ValueError, match='write failed') as caught:
        await sg.download_attachment(41, file_path=str(destination))

    assert caught.value is primary
    assert barrier.counts.get('close') == 1
    assert barrier.counts.get('unlink') == 1
    assert await asyncio.to_thread(destination.read_bytes) == b'existing'
    assert releases == ['download']
    assert transport.leases == transport.checkins


@pytest.mark.anyio
async def test_download_redirect_and_token_publication_use_one_origin_snapshot() -> None:
    releases: list[str] = []
    signed_url = 'https://storage.example/object?X-Amz-Signature=secret-value'
    sg: AsyncShotgun

    def change_origin(request: TransportRequest) -> None:
        if _method_name(request) == 'get_session_token':
            sg.config.set_server_params('https://new.example')

    sg, transport = _client(
        [
            _make_response({'session_id': 'old-origin-token'}),
            _stream(
                (),
                'redirect',
                releases,
                options=_StreamOptions(
                    headers={'location': signed_url},
                    status=302,
                ),
            ),
            _stream((b'DA', b'TA'), 'storage', releases),
        ],
        on_send=change_origin,
    )

    assert await sg.download_attachment(41) == b'DATA'

    first_hop = transport.requests[2]
    storage_hop = transport.requests[3]
    assert first_hop.url == 'https://x.example/file_serve/attachment/41'
    assert first_hop.headers['cookie'] == '_session_id=old-origin-token'
    assert 'cookie' not in storage_hop.headers
    assert sg.config.session_token is None
    assert releases == ['redirect', 'storage']


@pytest.mark.anyio
async def test_download_retries_pre_send_and_retryable_status_from_byte_zero() -> None:
    releases: list[str] = []
    sg, transport = _client(
        [
            SendError('pre-send'),
            _stream((), 'unavailable', releases, options=_StreamOptions(status=503)),
            _stream((b'DATA',), 'complete', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert await sg.download_attachment(41) == b'DATA'

    download_requests = transport.requests[1:]
    assert len(download_requests) == 3
    assert len({request.token for request in download_requests}) == 3
    assert {request.url for request in download_requests} == {
        'https://x.example/file_serve/attachment/41'
    }
    assert releases == ['unavailable', 'complete']


@pytest.mark.anyio
async def test_download_retry_discards_memory_and_file_partial_attempts(
    tmp_path: Path,
) -> None:
    memory_releases: list[str] = []
    memory, _memory_transport = _client(
        [
            _stream(
                (b'partial-', ReadTimeoutError('body stalled')),
                'partial',
                memory_releases,
            ),
            _stream((b'complete',), 'complete', memory_releases),
        ],
        session_token=_SESSION_VALUE,
    )
    assert await memory.download_attachment(41) == b'complete'
    assert memory_releases == ['partial', 'complete']

    file_releases: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    file_client, _file_transport = _client(
        [
            _stream(
                (b'partial', ReadTimeoutError('body stalled')),
                'partial',
                file_releases,
            ),
            _stream((b'complete-', b'file'), 'complete', file_releases),
        ],
        session_token=_SESSION_VALUE,
    )
    assert await file_client.download_attachment(41, file_path=str(destination)) == str(
        destination
    )
    assert await asyncio.to_thread(destination.read_bytes) == b'complete-file'
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == [destination]
    assert file_releases == ['partial', 'complete']


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('code', 'expected'),
    [
        (102, _errors.AuthenticationFault),
        (106, _errors.MissingTwoFactorAuthenticationFault),
        (999, _errors.Fault),
    ],
)
async def test_download_setup_preserves_domain_exception_type(
    code: int,
    expected: type[_errors.ShotgunError],
) -> None:
    sg, _transport = _client([_fault_response(code)])

    with pytest.raises(expected) as caught:
        await sg.download_attachment(41)

    assert type(caught.value) is expected


@pytest.mark.anyio
async def test_download_attachment_rejects_string_before_network() -> None:
    sg, transport = _client([])
    download = cast('Callable[..., Any]', sg.download_attachment)

    with pytest.raises(TypeError, match=r'^attachment must be an id or dictionary$'):
        await download('/path/to/some/file.jpg')

    assert transport.requests == []


@pytest.mark.anyio
async def test_download_attachment_rejects_non_attachment_entity_before_network() -> None:
    sg, transport = _client([])

    with pytest.raises(ValueError, match=r'^attachment dictionary must reference an Attachment$'):
        await sg.download_attachment({'id': 123, 'type': 'Shot'})

    assert transport.requests == []


@pytest.mark.anyio
async def test_download_attachment_requires_reference_but_accepts_explicit_none() -> None:
    sg, transport = _client([])
    download = cast('Callable[..., Any]', sg.download_attachment)

    with pytest.raises(TypeError, match=r'^attachment or attachment_id is required$'):
        await download()
    assert await sg.download_attachment(None) is None
    assert transport.requests == []


def test_file_method_descriptors_and_signatures_match_sync() -> None:
    method_names = (
        'download_attachment',
        'get_attachment_download_url',
        'get_auth_cookie_handler',
        'share_thumbnail',
        'upload',
        'upload_filmstrip_thumbnail',
        'upload_thumbnail',
    )

    for method_name in method_names:
        sync_member = inspect.getattr_static(Shotgun, method_name)
        async_member = inspect.getattr_static(AsyncShotgun, method_name)
        assert type(async_member) is type(sync_member)
        assert inspect.signature(async_member) == inspect.signature(sync_member)
        assert inspect.iscoroutinefunction(async_member)


@pytest.mark.anyio
async def test_form_upload_forwards_async_file_batch_size(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / 'small.mov'
    content = b'a' * _FORM_TRANSFER_SIZE
    source.write_bytes(content)
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    captured_chunk_sizes: list[int] = []
    original = _files.build_form_upload_exchange

    def capture_chunk_size(
        canonical_origin: str,
        user_agent: str,
        prepared: _files.PreparedUpload,
        auth: Mapping[str, object],
        chunk_size: int = 65536,
    ) -> _files.UploadExchange:
        captured_chunk_sizes.append(chunk_size)
        return original(
            canonical_origin,
            user_agent,
            prepared,
            auth,
            chunk_size=chunk_size,
        )

    monkeypatch.setattr(_files, 'build_form_upload_exchange', capture_chunk_size)
    sg, transport = _client([_text()], server_info=_FORM_INFO)

    with pytest.raises(ShotgunError, match='form upload success response is not established'):
        await sg.upload('Version', 5, str(source), field_name='sg_uploaded_movie')

    assert captured_chunk_sizes == [_FILE_BATCH_SIZE]
    assert audit.read_sizes == [_FORM_TRANSFER_SIZE, 0]
    assert content in cast('bytes', transport.bodies[1])


@pytest.mark.anyio
async def test_form_and_unsafe_upload_endpoints_do_not_retry_ambiguous_send(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _freeze_form_boundary(monkeypatch)
    small = tmp_path / 'small.mov'
    small.write_bytes(b'form-data')
    form, form_transport = _client(
        [SendError('form'), _text('unused')],
        server_info=_FORM_INFO,
    )
    form.config.user_agents = ('upload-transcript',)
    with pytest.raises(SendError, match='form'):
        await form.upload('Version', 5, str(small), field_name='sg_uploaded_movie')
    assert len(form_transport.requests) == 2
    form_body = (
        b'--Task13Boundary\r\nContent-Disposition: form-data; name="entity_type"\r\n\r\n'
        b'Version\r\n--Task13Boundary\r\nContent-Disposition: form-data; name="entity_id"'
        b'\r\n\r\n5\r\n--Task13Boundary\r\nContent-Disposition: form-data; name="field_name"'
        b'\r\n\r\nsg_uploaded_movie\r\n--Task13Boundary\r\nContent-Disposition: form-data; '
        b'name="display_name"\r\n\r\nsmall.mov\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="script_name"\r\n\r\nscript\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="script_key"\r\n\r\nkey\r\n--Task13Boundary\r\nContent-Disposition: '
        b'form-data; name="file"; filename="small.mov"\r\nContent-Type: application/octet-stream'
        b'\r\n\r\nform-data\r\n--Task13Boundary--\r\n'
    )
    assert _raw_upload_transcript(
        form_transport.requests[1:],
        form_transport.bodies[1:],
    ) == [
        (
            'POST',
            'https://x.example:443/upload/upload_file',
            _upload_headers(form_body, 'multipart/form-data; boundary=Task13Boundary'),
            form_body,
        )
    ]

    direct_allocation = '1 https://storage.example/object timestamp Attachment upload-1'
    allocation, allocation_transport = _client([SendError('allocation'), _text('unused')])
    with pytest.raises(SendError, match='allocation'):
        await allocation.upload('Version', 5, str(small), field_name='sg_uploaded_movie')
    assert len(allocation_transport.requests) == 2

    link, link_transport = _client(
        [_text(direct_allocation), _text(), SendError('link'), _text('unused')]
    )
    with pytest.raises(SendError, match='link'):
        await link.upload('Version', 5, str(small), field_name='sg_uploaded_movie')
    assert len(link_transport.requests) == 4

    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    large = tmp_path / 'large.mov'
    large.write_bytes(b'abcde')
    multipart_allocation = (
        '1 https://storage.example/multipart timestamp Attachment upload-1 opaque'
    )
    part_link, part_link_transport = _client(
        [_text(multipart_allocation), SendError('part link'), _text('unused')]
    )
    with pytest.raises(SendError, match='part link'):
        await part_link.upload('Version', 5, str(large), field_name='sg_uploaded_movie')
    assert len(part_link_transport.requests) == 3

    finalize, finalize_transport = _client(
        [
            _text(multipart_allocation),
            _text('1 https://storage.example/part-1'),
            _text(headers={'etag': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'etag': 'etag-2'}),
            SendError('finalize'),
            _text('unused'),
        ]
    )
    with pytest.raises(SendError, match='finalize'):
        await finalize.upload('Version', 5, str(large), field_name='sg_uploaded_movie')
    assert len(finalize_transport.requests) == 7

    share, share_transport = _client([SendError('share'), _text('unused')])
    with pytest.raises(SendError, match='share'):
        await share.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            source_entity={'type': 'Version', 'id': 1},
        )
    assert len(share_transport.requests) == 2


@pytest.mark.anyio
async def test_multipart_upload_replays_parts_and_preserves_etag_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_files, '_MULTIPART_CHUNK_SIZE', 4)
    movie = tmp_path / 'large.mov'
    movie.write_bytes(b'abcde')
    encoded = '1 https://storage.example/multipart timestamp Attachment upload-1 opaque'
    sg, transport = _client(
        [
            _text(encoded),
            _text('1 https://storage.example/part-1'),
            _PartialUploadFailure(SendError('ambiguous part PUT')),
            _text(headers={'etag': 'etag-1'}),
            _text('1 https://storage.example/part-2'),
            _text(headers={'etag': 'etag-2'}),
            _text('17'),
            _text('1:84 large.mov'),
        ]
    )
    sg.config.user_agents = ('upload-transcript',)

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 84

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
        b'upload-1+opaque&script_name=script&script_key=key'
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
    assert transport.bodies[3:5] == [b'abcd', b'abcd']
    assert transport.bodies[6] == b'e'
    assert _form_fields(cast('bytes', transport.bodies[7]))['etags'] == 'etag-1,etag-2'


@pytest.mark.anyio
async def test_one_use_auth_direct_upload_exchanges_once_before_all_http(
    tmp_path: Path,
) -> None:
    movie = tmp_path / 'clip.mov'
    movie.write_bytes(b'data')
    allocation = '1 https://storage.example/object timestamp Attachment upload-1'
    sg, transport = _one_use_client(
        [
            _make_response({'session_id': _SESSION_VALUE}),
            _text(allocation),
            _text(),
            _text('1:9 linked'),
        ]
    )

    assert await sg.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    assert [_method_name(request) for request in transport.requests[:2]] == [
        'info',
        'get_session_token',
    ]
    assert [_method_name(request) for request in transport.requests].count(
        'get_session_token'
    ) == 1
    _session_only_fields(cast('bytes', transport.bodies[2]))
    _session_only_fields(cast('bytes', transport.bodies[4]))
    assert sg.config.session_token == _SESSION_VALUE


@pytest.mark.anyio
async def test_one_use_auth_source_and_path_share_use_exchanged_session(
    tmp_path: Path,
) -> None:
    source, source_transport = _one_use_client(
        [_make_response({'session_id': _SESSION_VALUE}), _text('1:31')]
    )
    assert (
        await source.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            source_entity={'type': 'Version', 'id': 1},
        )
        == 31
    )
    assert [_method_name(request) for request in source_transport.requests[:2]] == [
        'info',
        'get_session_token',
    ]
    _session_only_fields(cast('bytes', source_transport.bodies[2]))

    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    allocation = '1 https://storage.example/image timestamp Thumbnail upload-1'
    ready = {
        'entities': [{'type': 'Version', 'id': 2, 'image': 'https://x.example/image/ready.jpg'}],
        'paging_info': {'entity_count': 1},
    }
    path, path_transport = _one_use_client(
        [
            _make_response({'session_id': _SESSION_VALUE}),
            _text(allocation),
            _text(),
            _text('1:41 image.jpg'),
            _make_response(ready),
            _text('1:42'),
        ]
    )
    assert (
        await path.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            thumbnail_path=str(image),
        )
        == 42
    )
    assert [_method_name(request) for request in path_transport.requests].count(
        'get_session_token'
    ) == 1
    for index in (2, 4, 6):
        _session_only_fields(cast('bytes', path_transport.bodies[index]))
    read_request = path_transport.requests[5]
    assert isinstance(read_request.body, bytes)
    read_payload = cast('dict[str, object]', json.loads(read_request.body))
    read_params = cast('list[dict[str, object]]', read_payload['params'])
    assert read_params[0] == {'session_token': _SESSION_VALUE}


@pytest.mark.anyio
async def test_path_share_rejects_bind_error_before_one_use_exchange(
    tmp_path: Path,
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    sg, transport = _one_use_client([_make_response({'session_id': _SESSION_VALUE})])

    with pytest.raises(TypeError, match='unexpected'):
        await sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=str(image),
            unsupported_option=True,
        )

    assert transport.requests == []
    assert sg.config.auth_token == _ONE_USE_VALUE
    assert sg.config.session_token is None


@pytest.mark.anyio
async def test_path_share_rejects_missing_file_before_one_use_exchange(
    tmp_path: Path,
) -> None:
    sg, transport = _one_use_client([_make_response({'session_id': _SESSION_VALUE})])

    with pytest.raises(ShotgunError, match='File must exist and be non-empty'):
        await sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=str(tmp_path / 'missing.jpg'),
        )

    assert transport.requests == []
    assert sg.config.auth_token == _ONE_USE_VALUE
    assert sg.config.session_token is None


@pytest.mark.anyio
async def test_share_thumbnail_rejects_duplicate_field_name_before_network(
    tmp_path: Path,
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    sg, transport = _client([])

    with pytest.raises(TypeError, match='multiple values'):
        await sg.share_thumbnail(
            [{'type': 'Version', 'id': 2}],
            thumbnail_path=str(image),
            field_name='thumb_image',
        )

    assert transport.requests == []


@pytest.mark.anyio
async def test_share_thumbnail_source_and_path_workflows(tmp_path: Path) -> None:
    source, source_transport = _client([_text('1:31')])
    source.config.user_agents = ('upload-transcript',)
    entities = [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}]
    assert (
        await source.share_thumbnail(
            entities,
            source_entity={'type': 'Version', 'id': 1},
            filmstrip_thumbnail=True,
        )
        == 31
    )
    assert entities == [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}]
    source_body = (
        b'entities=Version_2%2CShot_7&filmstrip_thumbnail=1&source_entity=Version_1&'
        b'script_name=script&script_key=key'
    )
    assert _raw_upload_transcript(
        source_transport.requests[1:],
        source_transport.bodies[1:],
    ) == [
        (
            'POST',
            'https://x.example:443/upload/share_thumbnail',
            _upload_headers(source_body, 'application/x-www-form-urlencoded'),
            source_body,
        )
    ]
    assert _form_fields(cast('bytes', source_transport.bodies[1])) == {
        'entities': 'Version_2,Shot_7',
        'filmstrip_thumbnail': '1',
        'source_entity': 'Version_1',
        'script_name': 'script',
        'script_key': 'key',
    }

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
    path, path_transport = _client(
        [
            _text(allocation),
            _text(),
            _text('1:41 image.jpg'),
            _make_response(transient),
            _make_response(ready),
            _text('1:42'),
        ]
    )
    assert (
        await path.share_thumbnail(
            [{'type': 'Version', 'id': 2}, {'type': 'Shot', 'id': 7}],
            thumbnail_path=str(image),
        )
        == 42
    )
    assert (
        sum(
            request.url.endswith(_files.GET_UPLOAD_LINK_INFO)
            for request in path_transport.requests
        )
        == 1
    )
    assert [_method_name(request) for request in path_transport.requests].count('read') == 2
    assert path_transport.requests[-1].url.endswith(_files.SHARE_THUMBNAIL)
    assert _form_fields(cast('bytes', path_transport.bodies[-1]))['entities'] == 'Shot_7'


@pytest.mark.anyio
async def test_site_independent_attachment_urls_do_not_require_current_site() -> None:
    sg, transport = _client([], server_info=None)
    sg.config.update_fields(server=None)
    external = {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }

    assert await sg.get_attachment_download_url(None) is None
    assert await sg.get_attachment_download_url(external) == external['url']
    assert external == {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }
    assert transport.requests == []


@pytest.mark.anyio
async def test_streamed_download_uses_captured_same_origin_cookie(
    fake_async_client: FakeAsyncClient,
) -> None:
    response = TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-length': '4'}),
        content=b'DATA',
    )
    sg = fake_async_client([response], session_token=_SESSION_VALUE)

    assert (
        await sg.get_attachment_download_url({'type': 'Attachment', 'id': 41})
        == 'https://x.example/file_serve/attachment/41'
    )
    assert await sg.download_attachment(attachment_id=41) == b'DATA'

    request = _fake_async_transport(sg).requests[1]
    assert request.url == 'https://x.example/file_serve/attachment/41'
    assert request.headers['cookie'] == '_session_id=session-one'


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('method_name', 'expected_extra'),
    [
        ('upload_thumbnail', {}),
        ('upload_filmstrip_thumbnail', {'filmstrip': 'true'}),
    ],
)
async def test_thumbnail_wrappers_forward_kwargs(
    tmp_path: Path,
    method_name: str,
    expected_extra: dict[str, str],
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    encoded = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg, transport = _client([_text(encoded), _text(), _text('1:7 image.jpg')])
    method = cast('Callable[..., Any]', getattr(sg, method_name))

    assert await method('Version', 5, str(image), display_name='Nice Image') == 7

    assert _form_fields(cast('bytes', transport.bodies[1]))['upload_type'] == 'Thumbnail'
    assert _form_fields(cast('bytes', transport.bodies[3])) == {
        'entity_type': 'Version',
        'entity_id': '5',
        'display_name': 'Nice Image',
        **expected_extra,
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('field_name', 'expected_extra'),
    [('image', {}), ('filmstrip_image', {'filmstrip': 'true'})],
)
async def test_upload_display_thumbnail_fields_use_thumbnail_wire_protocol(
    tmp_path: Path,
    field_name: str,
    expected_extra: dict[str, str],
) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    encoded = '1 https://storage.example/image timestamp Thumbnail upload-1'
    sg, transport = _client([_text(encoded), _text(), _text('1:7 image.jpg')])

    assert await sg.upload('Task', 5, str(image), field_name) == 7

    assert _form_fields(cast('bytes', transport.bodies[1]))['upload_type'] == 'Thumbnail'
    assert _form_fields(cast('bytes', transport.bodies[3])) == {
        'entity_type': 'Task',
        'entity_id': '5',
        **expected_extra,
        'upload_link_info': encoded,
        'script_name': 'script',
        'script_key': 'key',
    }


@pytest.mark.anyio
async def test_upload_rejects_invalid_created_at_before_network(tmp_path: Path) -> None:
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'image')
    sg, transport = _client([])
    upload = cast('Callable[..., Any]', sg.upload)

    with pytest.raises(ShotgunError, match=r'^created_at must be a datetime\.datetime instance$'):
        await upload('Version', 5, str(image), created_at='2026-02-15T10:30:00Z')

    assert transport.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize('byte_path', [False, True], ids=['unicode-str', 'utf8-bytes'])
async def test_upload_accepts_unicode_and_utf8_byte_paths(tmp_path: Path, byte_path: bool) -> None:
    source = tmp_path / 'caf\u00e9_\u65e5\u672c.jpg'
    source.write_bytes(b'image-data')
    path = str(source).encode('utf-8') if byte_path else str(source)
    encoded = '1 https://storage.example/object timestamp Attachment upload-1'
    sg, transport = _client([_text(encoded), _text(), _text('1:42 linked')])

    assert await sg.upload('Version', 5, path, 'sg_uploaded_movie') == 42

    assert transport.bodies[2] == b'image-data'
    assert _form_fields(cast('bytes', transport.bodies[1]))['filename'] == source.name


@pytest.mark.anyio
async def test_upload_and_download_filesystem_work_runs_off_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    audit = _FilesystemAudit(threading.get_ident())
    audit.install(monkeypatch)
    movie = tmp_path / 'clip.mov'
    await asyncio.to_thread(movie.write_bytes, b'clip')
    allocation = '1 https://storage.example/object timestamp Attachment upload-1'
    uploader, _upload_transport = _client([_text(allocation), _text(), _text('1:9 linked')])
    assert await uploader.upload('Version', 5, str(movie), field_name='sg_uploaded_movie') == 9

    releases: list[str] = []
    downloader, _download_transport = _client(
        [_stream((b'DATA',), 'download', releases)],
        session_token=_SESSION_VALUE,
    )
    destination = tmp_path / 'out.mov'
    assert await downloader.download_attachment(41, file_path=str(destination)) == str(destination)
    assert await asyncio.to_thread(destination.read_bytes) == b'DATA'
    audit.assert_complete()
