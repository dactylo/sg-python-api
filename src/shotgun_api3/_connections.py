"""Build and apply TCP keepalive socket options."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING, Any, cast

import urllib3
from urllib3.connection import HTTPConnection, HTTPSConnection

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from urllib3.response import HTTPResponse

SocketOption = tuple[int, int, int]


def _keepalive_timings(target: int) -> tuple[int, int, int]:
    """Map a nominal target to whole-second probe timings."""
    idle = target // 2
    interval = -(-(target - idle) // 3)
    return idle, interval, 3


def keepalive_socket_options(target: int | None) -> tuple[SocketOption, ...]:
    """Return the available options for a nominal keepalive target.

    Args:
        target: Validated target in seconds, or `None` to disable keepalive.

    Returns:
        Enablement first, followed by available platform timing options.
    """
    if target is None:
        return ()

    idle, interval, count = _keepalive_timings(target)
    options: list[SocketOption] = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    idle_option = getattr(socket, 'TCP_KEEPIDLE', None)
    if idle_option is None:
        idle_option = getattr(socket, 'TCP_KEEPALIVE', None)
    if idle_option is not None:
        options.append((socket.IPPROTO_TCP, idle_option, idle))
    if hasattr(socket, 'TCP_KEEPINTVL'):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval))
    if hasattr(socket, 'TCP_KEEPCNT'):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count))
    return tuple(options)


def tune_socket(sock: object, options: Iterable[SocketOption]) -> None:
    """Apply each supported socket option without failing on OS refusals.

    Args:
        sock: Socket-like object, which may lack `setsockopt`.
        options: Options to apply in order.
    """
    setsockopt = getattr(sock, 'setsockopt', None)
    if setsockopt is None:
        return
    for level, option, value in options:
        try:
            setsockopt(level, option, value)
        except OSError:
            continue


def connection_pool_classes(
    idle_limit: float | None,
    keepalive: tuple[SocketOption, ...],
    clock: Callable[[], float],
) -> dict[str, type[urllib3.HTTPConnectionPool]]:
    """Create isolated HTTP and HTTPS pool classes with one connection policy.

    Args:
        idle_limit: Validated idle limit in seconds, or `None` to disable
            expiry.
        keepalive: Socket options to apply after connection setup.
        clock: Monotonic clock returning seconds.

    Returns:
        New pool classes indexed by scheme, with policy-specific connection
        classes.
    """

    class _HTTPConnection(HTTPConnection):
        """Track response arrival and tune each socket after connection setup."""

        _clock = staticmethod(clock)
        _idle_limit = idle_limit
        _keepalive = keepalive
        _response_at: float | None = None

        def connect(self) -> None:
            """Open the connection and apply best-effort keepalive tuning.

            Raises:
                OSError: If connection setup fails.
                urllib3.exceptions.HTTPError: If connection or proxy setup
                    fails.
            """
            self._response_at = None
            super().connect()
            tune_socket(self.sock, self._keepalive)

        def getresponse(self) -> HTTPResponse:
            """Record time after headers and any preloaded body are received.

            Raises:
                OSError: If receiving the response fails.
                urllib3.exceptions.HTTPError: If the response cannot be
                    received.
            """
            response = super().getresponse()
            self._response_at = self._clock()
            return response

        @property
        def is_connected(self) -> bool:
            """Return whether the connection is unexpired and still connected."""
            if (
                self._idle_limit is not None
                and self._response_at is not None
                and self._clock() - self._response_at > self._idle_limit
            ):
                return False
            return super().is_connected

    class _HTTPSConnection(_HTTPConnection, HTTPSConnection):
        """Apply the connection policy around HTTPS setup, including proxy tunnels."""

    class _HTTPConnectionPool(urllib3.HTTPConnectionPool):
        """Pool HTTP connections under this policy."""

        # urllib3's connection protocols reject its concrete classes under strict typing.
        ConnectionCls = cast('Any', _HTTPConnection)

    class _HTTPSConnectionPool(urllib3.HTTPSConnectionPool):
        """Pool HTTPS connections under this policy."""

        ConnectionCls = cast('Any', _HTTPSConnection)

    return {'http': _HTTPConnectionPool, 'https': _HTTPSConnectionPool}
