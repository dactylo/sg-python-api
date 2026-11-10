import contextlib
import socket
import threading
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from collections.abc import Callable


class KeepAliveServer:
    def __init__(self) -> None:
        self.url = ''
        self.connections = 0
        self.silent_after_first = False
        self.before_response: Callable[[], None] | None = None
        self.requests: list[dict[str, str]] = []
        self._listener = socket.socket()
        self._stopping = threading.Event()
        self._sockets: list[socket.socket] = []
        self._workers: list[threading.Thread] = []
        self._thread = threading.Thread(target=self._accept, daemon=True)

    def __enter__(self) -> Self:
        self._listener.bind(('127.0.0.1', 0))
        self._listener.listen()
        self._listener.settimeout(0.1)
        host, port = self._listener.getsockname()
        self.url = f'http://{host}:{port}'
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stopping.set()
        self._thread.join(timeout=5.0)
        self._listener.close()
        for connection in self._sockets:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            connection.close()
        for worker in self._workers:
            worker.join(timeout=5.0)
            assert not worker.is_alive()
        assert not self._thread.is_alive()

    def _respond(self, connection: socket.socket) -> None:
        answered = False
        with connection:
            while not self._stopping.is_set():
                try:
                    headers = receive_request(connection)[2]
                    self.requests.append(headers)
                    if answered and self.silent_after_first:
                        continue
                    if self.before_response is not None:
                        self.before_response()
                    connection.sendall(
                        b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n'
                        b'Content-Type: application/json\r\n\r\n{}'
                    )
                    answered = True
                except (EOFError, OSError):
                    return

    def _accept(self) -> None:
        while not self._stopping.is_set():
            try:
                connection, _address = self._listener.accept()
            except TimeoutError:
                continue
            self.connections += 1
            self._sockets.append(connection)
            worker = threading.Thread(target=self._respond, args=(connection,), daemon=True)
            self._workers.append(worker)
            worker.start()


def receive_request(connection: socket.socket) -> tuple[str, str, dict[str, str], bytes]:
    request = b''
    while b'\r\n\r\n' not in request:
        chunk = connection.recv(4096)
        if not chunk:
            raise EOFError('request ended before headers')
        request += chunk
    header_block, body = request.split(b'\r\n\r\n', maxsplit=1)
    request_line, *header_lines = header_block.split(b'\r\n')
    method, path, _version = request_line.decode().split()
    headers = {
        name.decode().casefold(): value.decode().strip()
        for line in header_lines
        for name, value in [line.split(b':', maxsplit=1)]
    }
    content_length = int(headers.get('content-length', '0'))
    while len(body) < content_length:
        chunk = connection.recv(4096)
        if not chunk:
            raise EOFError('request ended before body')
        body += chunk
    return method, path, headers, body
