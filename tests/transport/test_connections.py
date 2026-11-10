import socket
from collections.abc import Callable
from contextlib import closing
from typing import Protocol, cast
from urllib.parse import urlsplit

import pytest
import urllib3
from urllib3.connection import HTTPConnection, HTTPSConnection

from shotgun_api3 import _connections, _transport
from shotgun_api3._transport import TimeoutConfig, TransportRequest, Urllib3Transport
from tests._keepalive import read_keepalive
from tests._loopback import KeepAliveServer


class _Clock:
    now = 1000.0

    def __call__(self) -> float:
        return self.now


def _assert_keepalive(sock: socket.socket, target: int) -> None:
    values = read_keepalive(sock)
    assert values.pop('enabled') != 0
    idle, interval, count = _connections._keepalive_timings(target)
    expected = {'idle': idle, 'interval': interval, 'count': count}
    assert values == {key: expected[key] for key in values}


def _pooled_socket(transport: Urllib3Transport, url: str) -> socket.socket:
    pool = transport._pool.connection_from_url(url).pool
    assert pool is not None
    connection = pool.get_nowait()
    assert connection is not None
    pool.put_nowait(connection)
    assert connection.sock is not None
    return connection.sock


def _request(url: str) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url=url + '/api3/json',
        headers={'content-type': 'application/json'},
        body=b'{}',
        timeout=TimeoutConfig(connect=5.0, read=2.0),
    )


def test_connection_past_the_limit_is_replaced() -> None:
    clock = _Clock()
    with KeepAliveServer() as server, closing(Urllib3Transport(clock=clock)) as transport:
        server.silent_after_first = True
        assert transport.send(_request(server.url)).status == 200
        clock.now += 61
        assert transport.send(_request(server.url)).status == 200
        assert server.connections == 2


def test_connection_within_the_limit_is_reused() -> None:
    clock = _Clock()
    with KeepAliveServer() as server, closing(Urllib3Transport(clock=clock)) as transport:
        assert transport.send(_request(server.url)).status == 200
        clock.now += 59
        assert transport.send(_request(server.url)).status == 200
        assert server.connections == 1


def test_disabled_settings_keep_todays_connection_behavior() -> None:
    clock = _Clock()
    with (
        KeepAliveServer() as server,
        closing(
            Urllib3Transport(
                max_connection_idle_secs=None,
                tcp_keepalive_timeout_secs=None,
                clock=clock,
            )
        ) as transport,
    ):
        assert transport.send(_request(server.url)).status == 200
        clock.now += 10_000
        assert transport.send(_request(server.url)).status == 200
        assert server.connections == 1
        assert read_keepalive(_pooled_socket(transport, server.url))['enabled'] == 0


def test_each_connection_keeps_its_own_idle_clock() -> None:
    clock = _Clock()
    connection_cls = _connections.connection_pool_classes(60.0, (), clock)['http'].ConnectionCls
    with KeepAliveServer() as server:
        address = urlsplit(server.url)
        with (
            closing(connection_cls('127.0.0.1', port=address.port, timeout=2.0)) as first,
            closing(connection_cls('127.0.0.1', port=address.port, timeout=2.0)) as second,
        ):
            first.request('GET', '/')
            first.getresponse().read()
            clock.now = 1050
            second.request('GET', '/')
            second.getresponse().read()
            clock.now = 1070
            assert first.is_connected is False
            assert second.is_connected is True


def test_keepalive_is_applied_to_direct_connections() -> None:
    with KeepAliveServer() as server, closing(Urllib3Transport()) as transport:
        assert transport.send(_request(server.url)).status == 200
        sock = _pooled_socket(transport, server.url)
        _assert_keepalive(sock, 60)
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0


def test_keepalive_options_are_empty_when_off() -> None:
    assert _connections.keepalive_socket_options(None) == ()


def test_keepalive_options_skip_constants_the_platform_lacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(socket, 'TCP_KEEPIDLE', raising=False)
    monkeypatch.delattr(socket, 'TCP_KEEPALIVE', raising=False)
    monkeypatch.delattr(socket, 'TCP_KEEPCNT', raising=False)
    options = _connections.keepalive_socket_options(60)
    expected = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    if hasattr(socket, 'TCP_KEEPINTVL'):
        expected.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10))
    assert options == tuple(expected)


@pytest.mark.parametrize(
    ('target', 'timings'),
    [(60, (30, 10, 3)), (6, (3, 1, 3)), (7, (3, 2, 3)), (30, (15, 5, 3)), (3600, (1800, 600, 3))],
)
def test_keepalive_timings_follow_the_target(target: int, timings: tuple[int, int, int]) -> None:
    assert _connections._keepalive_timings(target) == timings


class _ConnectionPolicy(Protocol):
    _idle_limit: float | None
    _keepalive: tuple[_connections.SocketOption, ...]
    _clock: Callable[[], float]


def test_keepalive_timings_never_exceed_the_target_by_more_than_two_seconds() -> None:
    for target in range(6, 3601):
        idle, interval, count = _connections._keepalive_timings(target)
        assert idle >= 1
        assert interval >= 1
        assert target <= idle + interval * count <= target + 2


def test_proxied_connections_follow_the_idle_limit() -> None:
    clock = _Clock()
    with (
        KeepAliveServer() as server,
        closing(
            Urllib3Transport(
                proxy_url=server.url,
                proxy_headers={'Proxy-Authorization': 'Basic dXNlcjpwYXNz'},
                clock=clock,
            )
        ) as transport,
    ):
        server.silent_after_first = True
        assert transport.send(_request('http://destination.invalid')).status == 200
        clock.now += 61
        assert transport.send(_request('http://destination.invalid')).status == 200
        assert server.connections == 2
        assert len(server.requests) == 2
        assert all(r['proxy-authorization'] == 'Basic dXNlcjpwYXNz' for r in server.requests)


def test_refused_keepalive_option_does_not_fail_the_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refused_options(_target: int | None) -> tuple[_connections.SocketOption, ...]:
        return ((socket.IPPROTO_TCP, 0x7FFF, 1),)

    monkeypatch.setattr(_transport, 'keepalive_socket_options', refused_options)
    with KeepAliveServer() as server, closing(Urllib3Transport()) as transport:
        assert transport.send(_request(server.url)).status == 200


def test_server_think_time_does_not_count_as_idle() -> None:
    clock = _Clock()

    def think() -> None:
        clock.now += 120

    with KeepAliveServer() as server, closing(Urllib3Transport(clock=clock)) as transport:
        server.before_response = think
        assert transport.send(_request(server.url)).status == 200
        clock.now += 10
        assert transport.send(_request(server.url)).status == 200
        assert server.connections == 1


@pytest.mark.parametrize(('elapsed', 'connections'), [(61, 2), (59, 1)])
def test_streamed_response_starts_the_idle_clock(elapsed: int, connections: int) -> None:
    clock = _Clock()
    with KeepAliveServer() as server, closing(Urllib3Transport(clock=clock)) as transport:
        server.silent_after_first = elapsed == 61
        response = transport.send_streaming(_request(server.url))
        assert response.status == 200
        assert b''.join(response.iter_chunks()) == b'{}'
        clock.now += elapsed
        assert transport.send(_request(server.url)).status == 200
        assert server.connections == connections


@pytest.mark.parametrize('target', [6, 3600])
def test_keepalive_bounds_are_accepted_by_the_platform(target: int) -> None:
    with (
        KeepAliveServer() as server,
        closing(Urllib3Transport(tcp_keepalive_timeout_secs=target)) as transport,
    ):
        assert transport.send(_request(server.url)).status == 200
        _assert_keepalive(_pooled_socket(transport, server.url), target)


@pytest.mark.parametrize('target', [60, 6, 3600])
def test_keepalive_is_applied_to_the_proxy_connection(target: int) -> None:
    with (
        KeepAliveServer() as server,
        closing(
            Urllib3Transport(
                proxy_url=server.url,
                proxy_headers={'Proxy-Authorization': 'Basic dXNlcjpwYXNz'},
                tcp_keepalive_timeout_secs=target,
            )
        ) as transport,
    ):
        assert transport.send(_request('http://destination.invalid')).status == 200
        sock = _pooled_socket(transport, 'http://destination.invalid/')
        assert sock.getpeername()[1] == urlsplit(server.url).port
        _assert_keepalive(sock, target)


@pytest.mark.parametrize('proxy_url', [None, 'http://127.0.0.1:8080'])
def test_both_schemes_use_policy_classes_with_and_without_a_proxy(proxy_url: str | None) -> None:
    clock = _Clock()
    with closing(Urllib3Transport(proxy_url=proxy_url, clock=clock)) as transport:
        classes = transport._pool.pool_classes_by_scheme
        for scheme, base in [('http', HTTPConnection), ('https', HTTPSConnection)]:
            connection_cls = classes[scheme].ConnectionCls
            assert issubclass(connection_cls, base)
            assert connection_cls is not base
            assert cast('_ConnectionPolicy', connection_cls)._idle_limit == 60.0
            assert cast(
                '_ConnectionPolicy', connection_cls
            )._keepalive == _connections.keepalive_socket_options(60)
            assert cast('_ConnectionPolicy', connection_cls)._clock is clock


def test_transports_keep_their_own_connection_policy() -> None:
    with (
        closing(Urllib3Transport(max_connection_idle_secs=15.0)) as first,
        closing(Urllib3Transport(max_connection_idle_secs=120.0)) as second,
    ):
        for scheme in ('http', 'https'):
            first_cls = first._pool.pool_classes_by_scheme[scheme].ConnectionCls
            second_cls = second._pool.pool_classes_by_scheme[scheme].ConnectionCls
            assert first_cls is not second_cls
            assert cast('_ConnectionPolicy', first_cls)._idle_limit == 15.0
            assert cast('_ConnectionPolicy', second_cls)._idle_limit == 120.0
        assert urllib3.poolmanager.pool_classes_by_scheme == {
            'http': urllib3.HTTPConnectionPool,
            'https': urllib3.HTTPSConnectionPool,
        }


def test_tuning_continues_past_a_refused_option() -> None:
    class RefusingSocket:
        def __init__(self) -> None:
            self.attempts: list[tuple[int, int, int]] = []

        def setsockopt(self, level: int, option: int, value: int) -> None:
            self.attempts.append((level, option, value))
            if option == 2:
                raise OSError('refused')

    sock = RefusingSocket()
    options = [(1, 1, 1), (1, 2, 2), (1, 3, 3)]
    _connections.tune_socket(sock, options)
    assert sock.attempts == options


def test_tuning_lets_other_errors_propagate() -> None:
    class InvalidSocket:
        def setsockopt(self, level: int, option: int, value: int) -> None:
            del level, option, value
            raise ValueError('invalid')

    with pytest.raises(ValueError, match='invalid'):
        _connections.tune_socket(InvalidSocket(), [(1, 2, 3)])


def test_tuning_skips_a_socket_without_setsockopt() -> None:
    _connections.tune_socket(object(), [(1, 2, 3)])
