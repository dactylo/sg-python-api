import socket
from collections.abc import Callable, Iterable, Mapping
from typing import cast
from urllib.parse import urlsplit

import pytest

from shotgun_api3 import _async_httpx2
from shotgun_api3._async_httpx2 import Httpx2Transport
from shotgun_api3._connections import SocketOption, _keepalive_timings, tune_socket
from shotgun_api3._transport import ConnectError, TimeoutConfig, TransportRequest
from tests._keepalive import read_keepalive
from tests._loopback import KeepAliveServer

pytestmark = pytest.mark.async_required


def _assert_keepalive(sock: socket.socket, target: int) -> None:
    values = read_keepalive(sock)
    assert values.pop('enabled') != 0
    expected: dict[str, int] = dict(
        zip(('idle', 'interval', 'count'), _keepalive_timings(target), strict=True)
    )
    assert values == {name: expected[name] for name in values}


def _record_tuning(monkeypatch: pytest.MonkeyPatch) -> list[socket.socket]:
    recorded: list[socket.socket] = []

    def record(sock: object, options: Iterable[SocketOption]) -> None:
        recorded.append(cast('socket.socket', sock))
        tune_socket(sock, options)

    monkeypatch.setattr(_async_httpx2, 'tune_socket', record, raising=False)
    return recorded


def _request(url: str) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url=url,
        headers={'Content-Type': 'application/json'},
        body=b'{}',
        timeout=TimeoutConfig(connect=1.0, read=2.0),
    )


@pytest.mark.anyio
@pytest.mark.parametrize('mode', ['buffered', 'streaming'])
@pytest.mark.parametrize('target', [60, 6, 3600])
async def test_keepalive_is_applied_to_async_connections(
    monkeypatch: pytest.MonkeyPatch, mode: str, target: int
) -> None:
    recorded = _record_tuning(monkeypatch)
    with KeepAliveServer() as server:
        transport = Httpx2Transport(tcp_keepalive_timeout_secs=target)
        try:
            if mode == 'buffered':
                response = await transport.send(_request(server.url))
                assert response.status == 200
            else:
                stream = await transport.send_streaming(_request(server.url))
                assert stream.status == 200
                assert b''.join([chunk async for chunk in stream.aiter_chunks()]) == b'{}'
            assert len(recorded) == 1
            _assert_keepalive(recorded[0], target)
        finally:
            await transport.close()


@pytest.mark.anyio
@pytest.mark.parametrize('target', [60, 6, 3600])
async def test_keepalive_is_applied_to_the_proxy_connection(
    monkeypatch: pytest.MonkeyPatch, target: int
) -> None:
    recorded = _record_tuning(monkeypatch)
    with KeepAliveServer() as server:
        transport = Httpx2Transport(proxy_url=server.url, tcp_keepalive_timeout_secs=target)
        try:
            response = await transport.send(_request('http://destination.invalid'))
            assert response.status == 200
            assert len(recorded) == 1
            assert recorded[0].getpeername()[1] == urlsplit(server.url).port
            _assert_keepalive(recorded[0], target)
        finally:
            await transport.close()


@pytest.mark.anyio
async def test_disabled_keepalive_leaves_async_sockets_untuned() -> None:
    recorded: list[socket.socket] = []

    async def record(event: str, info: Mapping[str, object]) -> None:
        if event == 'connection.connect_tcp.complete':
            get_extra_info = cast(
                'Callable[[str], socket.socket]',
                getattr(info['return_value'], 'get_extra_info', None),
            )
            recorded.append(get_extra_info('socket'))

    with KeepAliveServer() as server:
        transport = Httpx2Transport(tcp_keepalive_timeout_secs=None)
        try:
            response = await transport._client.request(
                'POST', server.url, content=b'{}', extensions={'trace': record}
            )
            assert response.status_code == 200
            assert len(recorded) == 1
            assert read_keepalive(recorded[0])['enabled'] == 0
        finally:
            await transport.close()


@pytest.mark.anyio
async def test_refused_keepalive_option_does_not_fail_an_async_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refused_options(_target: int | None) -> tuple[SocketOption, ...]:
        return ((socket.IPPROTO_TCP, 0x7FFF, 1),)

    monkeypatch.setattr(_async_httpx2, 'keepalive_socket_options', refused_options, raising=False)
    with KeepAliveServer() as server:
        transport = Httpx2Transport()
        try:
            response = await transport.send(_request(server.url))
            assert response.status == 200
        finally:
            await transport.close()


@pytest.mark.anyio
async def test_connect_failure_still_raises_with_keepalive() -> None:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        host, port = listener.getsockname()
    transport = Httpx2Transport()
    try:
        with pytest.raises(ConnectError):
            await transport.send(_request(f'http://{host}:{port}'))
    finally:
        await transport.close()
