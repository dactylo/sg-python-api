import socket


def read_keepalive(sock: socket.socket) -> dict[str, int]:
    values = {'enabled': sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)}
    idle_option = getattr(socket, 'TCP_KEEPIDLE', None)
    if idle_option is None:
        idle_option = getattr(socket, 'TCP_KEEPALIVE', None)
    if idle_option is not None:
        values['idle'] = sock.getsockopt(socket.IPPROTO_TCP, idle_option)
    if hasattr(socket, 'TCP_KEEPINTVL'):
        values['interval'] = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL)
    if hasattr(socket, 'TCP_KEEPCNT'):
        values['count'] = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT)
    return values
