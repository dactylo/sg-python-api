import socket
import threading
from collections.abc import Callable
from typing import Any, NoReturn

_REAL_CONNECT = socket.socket.connect
_REAL_SOCKETPAIR = socket.socketpair


def refuse_connections(
    refuse: Callable[[], NoReturn],
    replace: Callable[[Any, str, object], object] = setattr,
) -> None:
    # Windows has no native socketpair(). Python's fallback connects a loopback socket, and every
    # asyncio event loop calls it when it starts, so only connections made inside it get through.
    pairing = threading.local()

    def connect(sock: socket.socket, address: tuple[Any, ...] | str | bytes) -> None:
        if not getattr(pairing, 'active', False):
            refuse()
        _REAL_CONNECT(sock, address)

    def create_connection(*_args: object, **_kwargs: object) -> NoReturn:
        refuse()

    def socketpair(*args: int, **kwargs: int) -> tuple[socket.socket, socket.socket]:
        pairing.active = True
        try:
            return _REAL_SOCKETPAIR(*args, **kwargs)
        finally:
            pairing.active = False

    replace(socket.socket, 'connect', connect)
    replace(socket, 'create_connection', create_connection)
    replace(socket, 'socketpair', socketpair)
