import inspect
import json
import urllib.parse
import urllib.request
from typing import Any, cast

import pytest

from shotgun_api3 import _files
from shotgun_api3._client import Shotgun
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    TransportResponse,
)
from tests._transports import _FakeTransport
from tests.conftest import _fake_client, _fake_transport, _make_response

_COOKIE_VALUE = 'new-session'


class _CookieRequest:
    def __init__(self, url: str) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {'http', 'https'} or parsed.hostname is None:
            raise ValueError('cookie test URL must use HTTP or HTTPS')
        self.full_url = url
        self.headers: dict[str, str] = {}
        self.host = parsed.netloc
        self.origin_req_host = parsed.hostname
        self.type = parsed.scheme
        self.unverifiable = False

    def add_unredirected_header(self, name: str, value: str) -> None:
        self.headers[name] = value

    def get_full_url(self) -> str:
        return self.full_url

    def get_header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)

    def has_header(self, name: str) -> bool:
        return name in self.headers


def _cookie_header(
    handler: urllib.request.HTTPCookieProcessor,
    url: str,
) -> str | None:
    request = _CookieRequest(url)
    if request.type == 'https':
        handler.https_request(cast('Any', request))
    else:
        handler.http_request(cast('Any', request))
    return request.get_header('Cookie')


def _real_cookie_header(
    handler: urllib.request.HTTPCookieProcessor,
    request: urllib.request.Request,
) -> str | None:
    if request.type == 'https':
        handler.https_request(request)
    else:
        handler.http_request(request)
    return request.get_header('Cookie')


@pytest.mark.parametrize(
    ('origin_url', 'stdlib_request'),
    [
        pytest.param(
            'http://localhost',
            urllib.request.Request('http://localhost/asset'),
            id='localhost',
        ),
        pytest.param(
            'http://[::1]',
            urllib.request.Request('http://[::1]/asset'),
            id='ipv6-literal',
        ),
        pytest.param(
            'https://b\u00fccher.example',
            urllib.request.Request('https://b\u00fccher.example/asset'),
            id='unicode-idna-equivalent',
        ),
        pytest.param(
            'https://X.Example',
            urllib.request.Request('https://x.EXAMPLE/asset'),
            id='hostname-case',
        ),
        pytest.param(
            'http://x.example',
            urllib.request.Request('http://x.example:80/asset'),
            id='implicit-http-port',
        ),
        pytest.param(
            'https://x.example:443',
            urllib.request.Request('https://x.example/asset'),
            id='explicit-https-port',
        ),
    ],
)
def test_build_auth_cookie_handler_accepts_canonical_origin_for_real_request(
    origin_url: str,
    stdlib_request: urllib.request.Request,
) -> None:
    handler = _files.build_auth_cookie_handler(
        'session-secret',
        _files.canonical_origin(origin_url),
    )

    assert _real_cookie_header(handler, stdlib_request) == '_session_id=session-secret'


@pytest.mark.parametrize(
    'stdlib_request',
    [
        pytest.param(urllib.request.Request('https://x.example/asset'), id='scheme'),
        pytest.param(urllib.request.Request('http://x.example:8080/asset'), id='nondefault-port'),
        pytest.param(urllib.request.Request('http://sub.x.example/asset'), id='subdomain'),
    ],
)
def test_build_auth_cookie_handler_rejects_other_origin_for_real_request(
    stdlib_request: urllib.request.Request,
) -> None:
    handler = _files.build_auth_cookie_handler(
        'session-secret',
        _files.canonical_origin('http://x.example'),
    )

    assert _real_cookie_header(handler, stdlib_request) is None


@pytest.mark.parametrize(
    ('origin_url', 'secure'),
    [('https://X.Example:8443', True), ('http://X.Example:8080', False)],
)
def test_build_auth_cookie_handler_returns_exact_origin_host_only_cookie(
    origin_url: str,
    secure: bool,
) -> None:
    origin = _files.canonical_origin(origin_url)
    handler = _files.build_auth_cookie_handler('session-secret', origin)

    assert isinstance(handler, urllib.request.HTTPCookieProcessor)
    cookies = list(handler.cookiejar)
    assert len(cookies) == 1
    cookie = cookies[0]
    assert cookie.name == '_session_id'
    assert cookie.value == 'session-secret'
    assert cookie.domain == 'x.example'
    assert not cookie.domain_specified
    assert cookie.path == '/'
    assert cookie.path_specified
    assert cookie.secure is secure
    assert _cookie_header(handler, f'{origin[0]}://x.example:{origin[2]}/asset') == (
        '_session_id=session-secret'
    )
    other_scheme = 'http' if origin[0] == 'https' else 'https'
    assert _cookie_header(handler, f'{other_scheme}://x.example:{origin[2]}/asset') is None
    assert _cookie_header(handler, f'{origin[0]}://x.example:{origin[2] + 1}/asset') is None
    assert _cookie_header(handler, f'{origin[0]}://sub.x.example:{origin[2]}/asset') is None


def test_get_auth_cookie_handler_binds_token_and_cookie_to_one_origin_snapshot() -> None:
    sg = Shotgun(
        'https://old.example:8443',
        script_name='script',
        api_key='key',
        connect=False,
    )

    def change_origin() -> None:
        sg.config.set_server_params('https://new.example:9443')

    token_response = TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
        content=json.dumps({'results': {'session_id': 'old-origin-token'}}).encode(),
        on_close=change_origin,
    )
    transport = _FakeTransport(
        [
            _make_response({'version': [9, 0, 0], 'api_max_entities_per_page': 500}),
            token_response,
        ]
    )
    sg._transport = transport

    handler = sg.get_auth_cookie_handler()

    assert _cookie_header(handler, 'https://old.example:8443/asset') == (
        '_session_id=old-origin-token'
    )
    assert _cookie_header(handler, 'https://new.example:9443/asset') is None
    assert sg.config.session_token is None
    assert [request.url for request in transport.requests] == [
        'https://old.example:8443/api3/json',
        'https://old.example:8443/api3/json',
    ]


def test_get_auth_cookie_handler_exchanges_uncached_token() -> None:
    sg = _fake_client([_make_response({'session_id': _COOKIE_VALUE})])

    handler = sg.get_auth_cookie_handler()

    assert isinstance(handler, urllib.request.HTTPCookieProcessor)
    assert _cookie_header(handler, 'https://x.example/asset') == f'_session_id={_COOKIE_VALUE}'
    request = _fake_transport(sg).requests[1]
    assert isinstance(request.body, bytes)
    assert json.loads(request.body) == {
        'method_name': 'get_session_token',
        'params': [{'script_name': 's', 'script_key': 'k'}],
    }
    assert sg.config.session_token == _COOKIE_VALUE
    signature = inspect.signature(Shotgun.get_auth_cookie_handler)
    assert tuple(signature.parameters) == ('self',)
    assert signature.return_annotation is urllib.request.HTTPCookieProcessor
