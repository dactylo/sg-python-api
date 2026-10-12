import dataclasses
import json
import threading
import traceback
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Never, ParamSpec, Self, TypeAlias, TypeVar, cast
from unittest.mock import patch

import pytest

from shotgun_api3 import _errors, _files
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ProgressEvent
from shotgun_api3._transport import (
    AbortedError,
    CaseInsensitiveHeaders,
    ConnectError,
    ReadTimeoutError,
    SendError,
    StreamingResponse,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _FakeTransport
from tests.conftest import _fake_transport, _make_response

if TYPE_CHECKING:
    from shotgun_api3._config import ObserverEvent

_AmbientResult = TypeVar('_AmbientResult')
_CallParams = ParamSpec('_CallParams')
_StreamItem: TypeAlias = bytes | TransportError
_NEW_ORIGIN_API_KEY = 'new-origin-key'
_NEW_ORIGIN_SCRIPT = 'new-origin-script'
_NEW_ORIGIN_SESSION_VALUE = 'new-origin-token'
_SESSION_VALUE = 'session-one'


class _CleanupFile:
    def __init__(
        self,
        handle: BinaryIO,
        calls: list[str],
        errors: Mapping[str, BaseException],
    ) -> None:
        self._calls = calls
        self._errors = errors
        self._handle = handle

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._calls.append('close')
        self._handle.close()
        error = self._errors.get('close')
        if error is not None:
            raise error

    @property
    def name(self) -> str:
        return cast('str', self._handle.name)

    def write(self, content: bytes) -> int:
        self._calls.append('write')
        error = self._errors.get('write')
        if error is not None:
            raise error
        return self._handle.write(content)


@dataclasses.dataclass(frozen=True)
class _StreamScript:
    chunks: tuple[_StreamItem, ...]
    headers: Mapping[str, str]
    label: str
    releases: list[str]
    status: int = 200
    reason: str = 'OK'


class _DownloadTransport(_FakeTransport):
    def __init__(
        self,
        responses: list[TransportResponse | TransportError | _StreamScript],
        on_send: Callable[[TransportRequest], None] | None = None,
    ) -> None:
        super().__init__([])
        self._download_lock = threading.Lock()
        self._download_responses = list(responses)
        self._on_send = on_send

    def _next_scripted(
        self,
        request: TransportRequest,
    ) -> TransportResponse | _StreamScript:
        with self._download_lock:
            self.requests.append(request)
            if not self._download_responses:
                raise AssertionError('_DownloadTransport responses exhausted')
            response = self._download_responses.pop(0)
        if isinstance(response, TransportError):
            raise response
        return response

    def send(self, request: TransportRequest) -> TransportResponse:
        if self._on_send is not None:
            self._on_send(request)
        response = self._next_scripted(request)
        assert isinstance(response, TransportResponse)
        return response

    def send_streaming(self, request: TransportRequest) -> StreamingResponse:
        if self._on_send is not None:
            self._on_send(request)
        scripted = self._next_scripted(request)
        assert isinstance(scripted, _StreamScript)

        def stream(_size: int) -> Iterator[bytes]:
            for item in scripted.chunks:
                if isinstance(item, bytes):
                    yield item
                else:
                    raise item

        return StreamingResponse(
            status=scripted.status,
            reason=scripted.reason,
            headers=scripted.headers,
            stream=stream,
            release=lambda: scripted.releases.append(scripted.label),
        )


def _cleanup_file_factory(
    function: Callable[_CallParams, BinaryIO],
    calls: list[str],
    errors: Mapping[str, BaseException],
) -> Callable[_CallParams, _CleanupFile]:

    def wrapped(*args: _CallParams.args, **kwargs: _CallParams.kwargs) -> _CleanupFile:
        return _CleanupFile(function(*args, **kwargs), calls, errors)

    return wrapped


def _download_client(
    responses: list[TransportResponse | TransportError | _StreamScript],
    *,
    base_url: str = 'https://x.example',
    http_proxy: str | None = None,
    on_send: Callable[[TransportRequest], None] | None = None,
    session_token: str | None = None,
) -> Shotgun:
    sg = Shotgun(
        base_url,
        script_name=None if session_token is not None else 'script',
        api_key=None if session_token is not None else 'key',
        session_token=session_token,
        http_proxy=http_proxy,
        connect=False,
    )
    sg._transport = _DownloadTransport(responses, on_send)
    sg.config.rpc_attempt_interval = 0
    return sg


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


def _install_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[str],
    errors: Mapping[str, BaseException],
) -> None:
    original_named_temporary = cast(
        'Callable[..., BinaryIO]',
        _files.tempfile.NamedTemporaryFile,
    )
    original_unlink = Path.unlink

    def unlink(path: Path, missing_ok: bool = False) -> None:
        calls.append('unlink')
        original_unlink(path, missing_ok=missing_ok)
        error = errors.get('unlink')
        if error is not None:
            raise error

    monkeypatch.setattr(
        _files.tempfile,
        'NamedTemporaryFile',
        _cleanup_file_factory(original_named_temporary, calls, errors),
    )
    monkeypatch.setattr(Path, 'unlink', unlink)


def _raise_ambient_exception() -> Never:
    raise RuntimeError('ambient exception state')


def _call_inside_except(function: Callable[[], _AmbientResult]) -> _AmbientResult:
    try:
        _raise_ambient_exception()
    except RuntimeError:
        return function()


def _stream(
    chunks: tuple[_StreamItem, ...],
    label: str,
    releases: list[str],
    *,
    headers: Mapping[str, str] | None = None,
    status: int = 200,
) -> _StreamScript:
    return _StreamScript(
        chunks=chunks,
        headers=CaseInsensitiveHeaders(headers or {}),
        label=label,
        releases=releases,
        status=status,
        reason='OK' if status == 200 else 'Download failed',
    )


def test_attachment_url_rejects_unsupported_values_without_echoing_them() -> None:
    with pytest.raises(TypeError, match=r'^attachment must be an id or dictionary$'):
        _files.resolve_attachment_url(
            'https://secret.example/path?token=value', 'https://x.example'
        )


def test_attachment_urls_preserve_links_and_resolve_attachment_ids() -> None:
    assert (
        _files.resolve_attachment_url(
            {'link_type': 'web', 'url': 'https://elsewhere.example/file?secret=value'},
            'https://x.example',
        )
        == 'https://elsewhere.example/file?secret=value'
    )
    assert (
        _files.resolve_attachment_url(
            {'type': 'Attachment', 'id': 41},
            'https://x.example',
        )
        == 'https://x.example/file_serve/attachment/41'
    )
    assert (
        _files.resolve_attachment_url(42, 'https://x.example')
        == 'https://x.example/file_serve/attachment/42'
    )
    assert _files.resolve_attachment_url(None, 'https://x.example') is None


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
def test_attachment_urls_use_current_site_without_auth_io(
    site_url: str,
    expected_url: str,
) -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
        base_url='https://old.example',
        session_token=_SESSION_VALUE,
    )
    sg.config.set_server_params(site_url)
    transport = _fake_transport(sg)
    external = {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }

    assert sg.get_attachment_download_url(41) == expected_url
    assert sg.get_attachment_download_url(external) == external['url']
    assert external == {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }
    assert transport.requests == []

    sg.config.session_token = _SESSION_VALUE
    assert sg.download_attachment(41) == b'DATA'
    assert transport.requests[-1].url == expected_url
    assert releases == ['download']


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
@pytest.mark.parametrize('session_token', [None, _SESSION_VALUE], ids=['uncached', 'cached'])
def test_authenticated_file_download_allows_https_and_literal_loopback(
    base_url: str,
    session_token: str | None,
    http_proxy: str | None,
) -> None:
    responses: list[TransportResponse | TransportError | _StreamScript] = [
        _make_response({'version': [9, 0, 0]})
    ]
    if session_token is None:
        responses.append(_make_response({'session_id': _SESSION_VALUE}))
    responses.append(_stream((b'DATA',), 'download', []))
    sg = _download_client(
        responses, base_url=base_url, http_proxy=http_proxy, session_token=session_token
    )

    assert sg.download_attachment(41) == b'DATA'
    assert _fake_transport(sg).requests[-1].headers['cookie'] == f'_session_id={_SESSION_VALUE}'


@pytest.mark.parametrize('url', ['http://storage.example/file', 'http://remote.example:8080/file'])
def test_authenticated_file_external_http_download_omits_credentials(url: str) -> None:
    sg = _download_client(
        [_stream((b'DATA',), 'external', [])],
        base_url='http://remote.example',
        session_token=_SESSION_VALUE,
    )

    assert sg.download_attachment({'url': url}) == b'DATA'
    requests = _fake_transport(sg).requests
    assert len(requests) == 1
    assert requests[0].url == url
    assert set(requests[0].headers) == {'user-agent'}
    assert requests[0].body is None


@pytest.mark.parametrize('session_token', [None, _SESSION_VALUE], ids=['uncached', 'cached'])
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
def test_authenticated_file_http_download_rejects_before_lease(
    session_token: str | None, base_url: str, http_proxy: str | None
) -> None:
    sg = _download_client(
        [_make_response({'version': [9, 0, 0]}), _stream((b'DATA',), 'download', [])],
        base_url=base_url,
        http_proxy=http_proxy,
        session_token=session_token,
    )
    events: list[object] = []
    sg.config.on_request = events.append
    with patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease:
        with pytest.raises(
            ValueError,
            match=r'^authenticated RPCs require HTTPS or a direct literal loopback HTTP origin$',
        ):
            sg.download_attachment(41)
        lease.assert_not_called()
    assert _fake_transport(sg).requests == []
    assert events == []


def test_download_attachment_abort_is_terminal_and_closes_once() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'partial', AbortedError('request-token')), 'abort', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(
        _errors.ShotgunFileDownloadError,
        match=r'^attachment download failed$',
    ):
        sg.download_attachment(41)

    assert len(_fake_transport(sg).requests) == 2
    assert releases == ['abort']


def test_download_attachment_cached_token_first_call_runs_readiness() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0], 'api_max_entities_per_page': 500}),
            _stream((b'DATA',), 'download', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert sg.download_attachment(41) == b'DATA'

    requests = _fake_transport(sg).requests
    assert [request.url for request in requests] == [
        'https://x.example:443/api3/json',
        'https://x.example/file_serve/attachment/41',
    ]
    assert json.loads(cast('bytes', requests[0].body)) == {
        'method_name': 'info',
        'params': [],
    }
    assert requests[1].headers['cookie'] == f'_session_id={_SESSION_VALUE}'
    assert releases == ['download']


def test_download_attachment_discards_partial_memory_before_retry() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream(
                (b'partial-', ReadTimeoutError('body stalled')),
                'partial',
                releases,
            ),
            _stream((b'complete',), 'complete', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert sg.download_attachment(41) == b'complete'
    assert releases == ['partial', 'complete']


def test_download_attachment_exhaustion_preserves_destination_and_redacts_url(
    tmp_path: Path,
) -> None:
    releases: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    signed_url = 'https://storage.example/object?X-Amz-Signature=secret-value'
    sg = _download_client(
        [
            _stream((b'partial', SendError(signed_url)), 'one', releases),
            _stream((b'partial', SendError(signed_url)), 'two', releases),
            _stream((b'partial', SendError(signed_url)), 'three', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(
        _errors.ShotgunFileDownloadError,
        match=r'^attachment download failed$',
    ) as caught:
        sg.download_attachment({'url': signed_url}, file_path=str(destination))

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert 'secret-value' not in str(caught.value)
    assert destination.read_bytes() == b'existing'
    assert list(tmp_path.iterdir()) == [destination]
    assert releases == ['one', 'two', 'three']


def test_download_attachment_keeps_token_exchange_bound_to_original_snapshot() -> None:
    releases: list[str] = []
    old_transport: _FakeTransport
    sg: Shotgun

    def change_origin_during_token(request: TransportRequest) -> None:
        body = request.body
        if isinstance(body, bytes) and b'get_session_token' in body:
            sg.config.set_server_params('https://new.example')

    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0], 'api_max_entities_per_page': 500}),
            _make_response({'session_id': 'old-origin-token'}),
            _stream(
                (),
                'redirect',
                releases,
                headers={'Location': 'https://storage.example/signed?secret=old'},
                status=302,
            ),
            _stream((b'old-origin-data',), 'storage', releases),
        ],
        on_send=change_origin_during_token,
    )
    old_transport = _fake_transport(sg)

    assert sg.download_attachment(41) == b'old-origin-data'

    token_request = old_transport.requests[1]
    first_hop = old_transport.requests[2]
    assert json.loads(cast('bytes', token_request.body))['params'] == [
        {'script_name': 'script', 'script_key': 'key'}
    ]
    assert first_hop.url == 'https://x.example/file_serve/attachment/41'
    assert first_hop.headers['cookie'] == '_session_id=old-origin-token'

    replacement = _DownloadTransport(
        [
            _make_response({'version': [9, 0, 0], 'api_max_entities_per_page': 500}),
            _make_response({'session_id': _NEW_ORIGIN_SESSION_VALUE}),
        ]
    )
    sg._transport = replacement
    assert sg.get_session_token() == 'new-origin-token'
    assert len(replacement.requests) == 2


def test_download_attachment_off_origin_skips_readiness_and_token_lookup() -> None:
    releases: list[str] = []
    sg = _download_client([_stream((b'PAGE',), 'page', releases)])

    result = sg.download_attachment(
        {'link_type': 'web', 'url': 'https://elsewhere.example/page?token=secret'}
    )

    assert result == b'PAGE'
    requests = _fake_transport(sg).requests
    assert len(requests) == 1
    assert requests[0].url == 'https://elsewhere.example/page?token=secret'
    assert 'cookie' not in requests[0].headers
    assert releases == ['page']


def test_download_attachment_progress_uses_response_content_length() -> None:
    events: list[object] = []
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases, headers={'Content-Length': '4'}),
        ],
        session_token=_SESSION_VALUE,
    )
    sg.config.on_progress = events.append

    assert sg.download_attachment(41) == b'DATA'

    progress = [
        event for event in events if isinstance(event, ProgressEvent) and event.route == 'download'
    ]
    assert [(event.bytes_transferred, event.total_bytes) for event in progress] == [(4, 4)]
    assert releases == ['download']


def test_download_attachment_rejects_non_attachment_entity_before_network() -> None:
    sg = _download_client([])

    with pytest.raises(ValueError, match=r'^attachment dictionary must reference an Attachment$'):
        sg.download_attachment({'id': 123, 'type': 'Shot'})

    assert _fake_transport(sg).requests == []


def test_download_attachment_rejects_string_before_network() -> None:
    sg = _download_client([])
    download = cast('Callable[..., bytes | str | None]', sg.download_attachment)

    with pytest.raises(TypeError, match=r'^attachment must be an id or dictionary$'):
        download('/path/to/some/file.jpg')

    assert _fake_transport(sg).requests == []


def test_download_attachment_requires_reference_but_accepts_explicit_none() -> None:
    sg = _download_client([])
    download = cast('Callable[..., bytes | str | None]', sg.download_attachment)

    with pytest.raises(TypeError, match=r'^attachment or attachment_id is required$'):
        download()
    assert sg.download_attachment(None) is None
    assert _fake_transport(sg).requests == []


def test_download_attachment_resolves_id_from_active_origin_snapshot() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [10, 0, 0], 'api_max_entities_per_page': 250}),
            _make_response({'session_id': 'new-origin-token'}),
            _stream((b'new-origin-data',), 'download', releases),
        ]
    )
    sg.config.update_fields(
        api_key=_NEW_ORIGIN_API_KEY,
        scheme='https',
        script_name=_NEW_ORIGIN_SCRIPT,
        server='new.example',
    )

    assert sg.download_attachment(41) == b'new-origin-data'

    requests = _fake_transport(sg).requests
    assert [request.url for request in requests] == [
        'https://new.example:443/api3/json',
        'https://new.example:443/api3/json',
        'https://new.example/file_serve/attachment/41',
    ]
    assert json.loads(cast('bytes', requests[1].body)) == {
        'method_name': 'get_session_token',
        'params': [
            {
                'script_name': _NEW_ORIGIN_SCRIPT,
                'script_key': _NEW_ORIGIN_API_KEY,
            }
        ],
    }
    assert requests[2].headers['cookie'] == f'_session_id={_NEW_ORIGIN_SESSION_VALUE}'
    assert sg.config.server_info['version'] == [10, 0, 0]
    assert sg.config.session_token == _NEW_ORIGIN_SESSION_VALUE
    assert releases == ['download']


def test_download_attachment_retries_files_atomically_from_byte_zero(tmp_path: Path) -> None:
    releases: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'partial', SendError('body failed')), 'partial', releases),
            _stream((b'complete-', b'file'), 'complete', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert sg.download_attachment(41, file_path=str(destination)) == str(destination)
    assert destination.read_bytes() == b'complete-file'
    assert list(tmp_path.iterdir()) == [destination]
    assert releases == ['partial', 'complete']


def test_download_attachment_retries_pre_send_and_retryable_status() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            ConnectError('connection failed'),
            _stream((b'unused',), 'maintenance', releases, status=503),
            _stream((b'DATA',), 'success', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert sg.download_attachment(41) == b'DATA'

    requests = _fake_transport(sg).requests
    download_requests = requests[1:]
    assert len(download_requests) == 3
    assert len({request.token for request in download_requests}) == 3
    assert all(
        request.url == 'https://x.example/file_serve/attachment/41'
        for request in download_requests
    )
    assert releases == ['maintenance', 'success']


def test_download_attachment_uses_snapshot_token_and_credential_free_redirect() -> None:
    releases: list[str] = []
    events: list[ObserverEvent] = []
    signed_url = 'https://storage.example/object?X-Amz-Signature=secret-value'
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0], 'api_max_entities_per_page': 500}),
            _make_response({'session_id': 'session-one'}),
            _stream((), 'redirect', releases, headers={'Location': signed_url}, status=302),
            _stream((b'DA', b'TA'), 'storage', releases, headers={'Content-Length': '4'}),
        ]
    )
    sg.config.on_request = events.append
    sg.config.on_response = events.append

    result = sg.download_attachment({'type': 'Attachment', 'id': 41})

    assert result == b'DATA'
    transport = _fake_transport(sg)
    assert [request.url for request in transport.requests] == [
        'https://x.example:443/api3/json',
        'https://x.example:443/api3/json',
        'https://x.example/file_serve/attachment/41',
        signed_url,
    ]
    token_payload = json.loads(cast('bytes', transport.requests[1].body))
    assert token_payload == {
        'method_name': 'get_session_token',
        'params': [{'script_name': 'script', 'script_key': 'key'}],
    }
    first_hop = transport.requests[2]
    storage_hop = transport.requests[3]
    assert first_hop.headers['cookie'] == '_session_id=session-one'
    assert 'cookie' not in storage_hop.headers
    assert not {'authorization', 'script_name', 'script_key', 'session_token'} & {
        name.casefold() for name in storage_hop.headers
    }
    assert first_hop.body is storage_hop.body is None
    assert releases == ['redirect', 'storage']
    assert all('secret-value' not in repr(event) for event in events)


@pytest.mark.parametrize('error_type', [OSError, RuntimeError, SendError])
@pytest.mark.parametrize('phase', ['close', 'unlink'])
def test_download_cleanup_only_failure_inside_except_is_publicly_redacted(
    error_type: type[Exception],
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    _install_cleanup_failures(
        monkeypatch,
        calls,
        {phase: error_type(f'secret backend {phase} text request-secret')},
    )
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
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
        _call_inside_except(lambda: sg.download_attachment(41, file_path=str(destination)))

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    diagnostic = ''.join(traceback.format_exception(caught.value)) + repr(events + retries)
    assert 'secret backend' not in diagnostic
    assert 'request-secret' not in diagnostic
    assert retries == []
    assert len(_fake_transport(sg).requests) == 2
    assert calls == ['write', 'close', 'unlink']
    assert destination.read_bytes() == (b'existing' if phase == 'close' else b'DATA')
    assert list(tmp_path.iterdir()) == [destination]
    assert releases == ['download']


@pytest.mark.parametrize('error_type', [OSError, RuntimeError, SendError])
def test_download_primary_failure_survives_cleanup_failures(
    error_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    primary = _errors.ShotgunFileDownloadError('primary download failure')
    _install_cleanup_failures(
        monkeypatch,
        calls,
        {
            'close': error_type('secret backend close text'),
            'unlink': error_type('secret backend unlink text'),
            'write': primary,
        },
    )
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(_errors.ShotgunFileDownloadError) as caught:
        sg.download_attachment(41, file_path=str(destination))

    assert caught.value is primary
    assert calls == ['write', 'close', 'unlink']
    assert destination.read_bytes() == b'existing'
    assert releases == ['download']


@pytest.mark.parametrize('phase', ['close', 'unlink'])
def test_download_cleanup_control_exception_propagates_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: str,
) -> None:
    calls: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    control = KeyboardInterrupt('cleanup control outcome')
    _install_cleanup_failures(monkeypatch, calls, {phase: control})
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        sg.download_attachment(41, file_path=str(destination))

    assert caught.value is control
    assert calls == ['write', 'close', 'unlink']
    assert destination.read_bytes() == (b'existing' if phase == 'close' else b'DATA')
    assert releases == ['download']


@pytest.mark.parametrize('url', ['file:///unused', 'http://[::1', 'https:///missing-host'])
def test_download_rejects_invalid_url_before_io(url: str, tmp_path: Path) -> None:
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    sg = _download_client([], session_token=_SESSION_VALUE)
    transport = _fake_transport(sg)
    events: list[object] = []
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    with (
        patch.object(sg, '_acquire_driver', wraps=sg._acquire_driver) as acquire,
        patch.object(sg, '_driver_lease', wraps=sg._driver_lease) as lease,
        patch('shotgun_api3._driver.time.sleep') as wait,
    ):
        with pytest.raises(
            _errors.ShotgunFileDownloadError,
            match=r'^attachment download failed$',
        ):
            sg.download_attachment({'url': url}, file_path=str(destination))
        acquire.assert_not_called()
        lease.assert_not_called()
        wait.assert_not_called()

    assert transport.requests == []
    assert events == []
    assert destination.read_bytes() == b'existing'
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize(
    ('code', 'expected'),
    [
        (102, _errors.AuthenticationFault),
        (106, _errors.MissingTwoFactorAuthenticationFault),
        (999, _errors.Fault),
    ],
)
def test_download_setup_preserves_domain_exception_type(
    code: int,
    expected: type[_errors.ShotgunError],
) -> None:
    sg = _download_client([_fault_response(code)])

    with pytest.raises(expected) as caught:
        sg.download_attachment(41)

    assert type(caught.value) is expected


def test_download_transfer_runtime_error_propagates_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    destination = tmp_path / 'download.bin'
    destination.write_bytes(b'existing')
    primary = RuntimeError('transfer failed')
    _install_cleanup_failures(monkeypatch, calls, {'write': primary})
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    with pytest.raises(RuntimeError) as caught:
        sg.download_attachment(41, file_path=str(destination))

    assert caught.value is primary
    assert calls == ['write', 'close', 'unlink']
    assert destination.read_bytes() == b'existing'
    assert releases == ['download']


def test_get_attachment_download_url_and_legacy_attachment_id() -> None:
    releases: list[str] = []
    sg = _download_client(
        [
            _make_response({'version': [9, 0, 0]}),
            _stream((b'DATA',), 'download', releases),
        ],
        session_token=_SESSION_VALUE,
    )

    assert (
        sg.get_attachment_download_url({'type': 'Attachment', 'id': 41})
        == 'https://x.example/file_serve/attachment/41'
    )
    assert sg.download_attachment(attachment_id=41) == b'DATA'
    assert releases == ['download']


def test_origin_comparison_normalizes_idna_case_and_default_ports() -> None:
    origin = _files.canonical_origin('https://B\u00dcCHER.example')

    assert origin == ('https', 'xn--bcher-kva.example', 443)
    assert _files.needs_session_cookie('https://xn--bcher-kva.example:443/file', origin)


@pytest.mark.parametrize(
    'url',
    [
        'http://x.example/file',
        'https://x.example:444/file',
        'https://cdn.x.example/file',
        'https://x.example.evil.test/file',
        'https://x.example@evil.test/file',
        'ftp://x.example/file',
        'not a URL',
    ],
)
def test_origin_comparison_rejects_credential_leak_boundaries(url: str) -> None:
    origin = _files.canonical_origin('https://x.example')

    assert not _files.needs_session_cookie(url, origin)


def test_site_independent_attachment_urls_do_not_require_current_site() -> None:
    sg = _download_client([])
    sg.config.update_fields(server=None)
    transport = _fake_transport(sg)
    external = {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }

    assert sg.get_attachment_download_url(None) is None
    assert sg.get_attachment_download_url(external) == external['url']
    assert external == {
        'link_type': 'web',
        'url': 'https://storage.example/object?X-Amz-Signature=secret-value',
    }
    assert transport.requests == []
