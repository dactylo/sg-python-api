import json
from typing import TYPE_CHECKING, NotRequired, TypedDict, Unpack, cast

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._capabilities import ServerCapabilities
from shotgun_api3._client import Shotgun
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    TransportError,
    TransportResponse,
)
from tests._transports import _AsyncFakeTransport, _FakeTransport

pytest_plugins = ('pytester', 'tests._required_mode')

if TYPE_CHECKING:
    from collections.abc import Callable

    from shotgun_api3._async_transport import AsyncTransport


class _AsyncClientKwargs(TypedDict):
    api_key: NotRequired[str | None]
    auth_token: NotRequired[str | None]
    ca_certs: NotRequired[str | None]
    connect: NotRequired[bool]
    convert_datetimes_to_utc: NotRequired[bool]
    http_proxy: NotRequired[str | None]
    login: NotRequired[str | None]
    password: NotRequired[str | None]
    script_name: NotRequired[str | None]
    session_token: NotRequired[str | None]
    sudo_as_login: NotRequired[str | None]


def _fake_async_transport(sg: 'AsyncShotgun') -> _AsyncFakeTransport:  # pyright: ignore[reportUnusedFunction]
    transport = sg._transport
    assert isinstance(transport, _AsyncFakeTransport)
    return transport


def _fake_transport(sg: Shotgun) -> _FakeTransport:
    transport = sg._transport
    assert isinstance(transport, _FakeTransport)
    return transport


def _make_response(obj: object) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
        content=json.dumps({'results': obj}).encode(),
    )


def _set_fake_async_transport(sg: 'AsyncShotgun', transport: 'AsyncTransport') -> None:  # pyright: ignore[reportUnusedFunction]
    sg._transport = transport


@pytest.fixture
def anyio_backend() -> str:
    return 'asyncio'


@pytest.fixture
def fake_async_client() -> 'Callable[..., AsyncShotgun]':
    def build(
        script: list[TransportResponse | TransportError],
        *,
        server_info: dict[str, object] | None = None,
        preseed_readiness: bool = False,
        **kwargs: Unpack[_AsyncClientKwargs],
    ) -> 'AsyncShotgun':
        info: dict[str, object] = (
            {'version': [9, 0, 0], 'api_max_entities_per_page': 500}
            if server_info is None
            else server_info
        )
        kwargs.setdefault('script_name', 's')
        kwargs.setdefault('api_key', 'k')
        if kwargs.get('login') is not None or kwargs.get('session_token') is not None:
            kwargs.pop('script_name', None)
            kwargs.pop('api_key', None)
        sg = AsyncShotgun('https://x.example', **kwargs)
        responses = script if preseed_readiness else [_make_response(info), *script]
        sg._transport = _AsyncFakeTransport(responses)
        if preseed_readiness:
            context = config_module.snapshot(sg.config)
            capabilities = ServerCapabilities('x.example', info)
            ready = cast(
                'dict[tuple[str, int], object]',
                sg._ready_caps,
            )
            assert context.canonical_origin is not None
            ready[(context.canonical_origin, context.transport_generation)] = capabilities
            assert config_module.publish_server_info(sg.config, context, capabilities.server_info)
        return sg

    return build


def _fake_client(  # pyright: ignore[reportUnusedFunction]
    script: list[TransportResponse | TransportError],
    *,
    server_info: dict[str, object] | None = None,
) -> Shotgun:
    info = (
        {'version': [9, 0, 0], 'api_max_entities_per_page': 500}
        if server_info is None
        else server_info
    )
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sg._transport = _FakeTransport([_make_response(info), *script])
    return sg


def _sent_payload(sg: Shotgun, index: int = 0) -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]
    request = _fake_transport(sg).requests[index + 1]
    assert isinstance(request.body, bytes)
    return cast('dict[str, object]', json.loads(request.body))
