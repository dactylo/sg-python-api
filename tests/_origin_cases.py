ALLOWED_ORIGIN_CASES = [('http://127.0.0.1', None)]
ORIGIN_AUTH_VALUE = 'old-auth-token'
ORIGIN_CASES = [
    ('https://remote.example', False),
    ('http://localhost', False),
    ('http://LOCALHOST', False),
    ('http://127.0.0.1', False),
    ('http://127.42.0.1', False),
    ('http://[::1]', False),
    ('http://[::ffff:127.0.0.1]', True),
    ('http://remote.example', True),
    ('http://192.0.2.10', True),
    ('http://[2001:db8::10]', True),
]
ORIGIN_CREDENTIAL = 'old-password'
ORIGIN_PROXY_SERVERS = [None, 'proxy.example', 'localhost', '127.0.0.1', '::1']
ORIGIN_SESSION_VALUE = 'old-session'
REJECTED_ORIGIN_CASES = [('http://remote.example', None)]

ORIGIN_OLD_VALUES = (
    ORIGIN_AUTH_VALUE.encode(),
    b'old-key',
    ORIGIN_CREDENTIAL.encode(),
    ORIGIN_SESSION_VALUE.encode(),
    b'old-sudo',
    b'old-user',
    b'old-uuid',
)
