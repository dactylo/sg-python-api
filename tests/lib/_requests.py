import json
from typing import cast

from shotgun_api3._config import TimeoutConfig
from shotgun_api3._transport import TransportRequest, TransportResponse
from shotgun_api3.lib.mockgun._server import FakeServer


def raw_call(
    server: FakeServer,
    method: str,
    params: object,
    *,
    auth: dict[str, object] | None = None,
) -> dict[str, object]:
    credentials = {'script_name': 'queries', 'script_key': 'key'} if auth is None else auth
    request = TransportRequest(
        'POST',
        'http://unit-test.example/api3/json',
        {},
        json.dumps(
            {
                'method_name': method,
                'params': [credentials, params] if credentials else [params],
            }
        ).encode(),
        TimeoutConfig(connect=None, read=None),
    )
    return cast('dict[str, object]', json.loads(server.handle(request).content))


def raw_request(
    server: FakeServer,
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> TransportResponse:
    return server.handle(
        TransportRequest(method, url, headers or {}, body, TimeoutConfig(connect=None, read=None))
    )


def request(
    method: str = 'POST', path: str = '/api3/json', body: bytes | None = b'{}'
) -> TransportRequest:
    return TransportRequest(
        method, 'http://unit-test.example' + path, {}, body, TimeoutConfig(connect=None, read=None)
    )
