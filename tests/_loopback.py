import socket


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
