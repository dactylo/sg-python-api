import concurrent.futures
import dataclasses
import threading
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from shotgun_api3._errors import ShotgunError
from shotgun_api3._transport import (
    AbortedError,
    CaseInsensitiveHeaders,
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    TimeoutConfig,
    TLSError,
    TransportError,
    TransportRequest,
    TransportResponse,
    body_close,
    content_length,
    narrow_total_timeout,
)
from tests._transports import _FakeTransport

_REQUEST_ID = 't1'
_REQ = TransportRequest(
    method='POST',
    url='https://x.example/api3/json',
    headers={},
    body=b'{}',
    timeout=TimeoutConfig(connect=15.0, read=300.0),
    token=_REQUEST_ID,
)


def test_body_close_returns_only_callable_close_attributes() -> None:
    def callback() -> None:
        pass

    assert body_close(SimpleNamespace(close=callback)) is callback
    assert body_close(None) is None
    assert body_close(b'buffered') is None
    assert body_close(object()) is None
    assert body_close(SimpleNamespace(close='not callable')) is None


def test_case_insensitive_headers_are_isolated_from_the_source_mapping() -> None:
    source = {'Content-Type': 'application/json'}
    headers = CaseInsensitiveHeaders(source)

    source['Content-Type'] = 'text/plain'

    assert headers['content-type'] == 'application/json'


@pytest.mark.parametrize(
    ('stored_name', 'lookup_name', 'value'),
    [
        ('Content-Type', 'content-type', 'application/json'),
        ('ETag', 'ETAG', 'abc123'),
        ('Retry-After', 'retry-after', '30'),
        ('X-Sg-Correlation-Id', 'x-SG-correlation-ID', 'request-42'),
    ],
)
def test_case_insensitive_headers_support_mixed_case_lookup(
    stored_name: str,
    lookup_name: str,
    value: str,
) -> None:
    headers = CaseInsensitiveHeaders({stored_name: value})

    assert headers[lookup_name] == value


def test_case_insensitive_headers_use_dict_like_duplicate_semantics() -> None:
    headers = CaseInsensitiveHeaders(
        {
            'Content-Type': 'text/plain',
            'X-Request-Id': 'request-1',
            'content-type': 'application/json',
        }
    )

    assert list(headers) == ['Content-Type', 'X-Request-Id']
    assert dict(headers) == {
        'Content-Type': 'application/json',
        'X-Request-Id': 'request-1',
    }


@pytest.mark.parametrize(
    ('headers', 'expected'),
    [
        ({'Content-Length': '0'}, 0),
        ({'CONTENT-LENGTH': '42'}, 42),
        ({'content-length': 'invalid'}, None),
        ({'content-length': '-1'}, None),
        ({}, None),
    ],
)
def test_content_length_parses_only_nonnegative_values(
    headers: dict[str, str],
    expected: int | None,
) -> None:
    assert content_length(headers) == expected


def test_fake_transport_abort_and_close_are_noops() -> None:
    fake = _FakeTransport([])

    fake.abort(_REQ.token)
    fake.close()


def test_fake_transport_raises_queued_errors() -> None:
    fake = _FakeTransport([ConnectError('refused')])

    with pytest.raises(ConnectError, match='refused'):
        fake.send(_REQ)

    assert fake.requests == [_REQ]


def test_fake_transport_raises_when_responses_are_exhausted() -> None:
    fake = _FakeTransport([])

    with pytest.raises(AssertionError, match='_FakeTransport responses exhausted'):
        fake.send(_REQ)

    assert fake.requests == [_REQ]


def test_fake_transport_replays_responses_and_records_requests() -> None:
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'Content-Type': 'application/json'},
        content=b'{"results": true}',
    )
    fake = _FakeTransport([response])

    assert fake.send(_REQ) is response
    assert fake.requests == [_REQ]
    assert response.headers['content-type'] == 'application/json'


@pytest.mark.parametrize(
    ('current_total', 'remaining', 'expected_total'),
    [
        (None, 3.0, 3.0),
        (2.0, 3.0, 2.0),
        (10.0, -0.5, 0.0),
    ],
)
def test_narrow_total_timeout_preserves_request_fields(
    current_total: float | None,
    remaining: float,
    expected_total: float,
) -> None:
    request = dataclasses.replace(
        _REQ,
        timeout=TimeoutConfig(connect=1.0, read=2.0, total=current_total),
    )

    narrowed = narrow_total_timeout(request, remaining)

    assert narrowed == dataclasses.replace(
        request,
        timeout=TimeoutConfig(connect=1.0, read=2.0, total=expected_total),
    )
    assert narrowed is not request
    assert narrowed.body is request.body
    assert narrowed.headers is request.headers


def test_narrow_total_timeout_returns_same_request_without_remaining_limit() -> None:
    assert narrow_total_timeout(_REQ, None) is _REQ


def test_timeout_config_defaults_total_and_is_frozen() -> None:
    timeout = TimeoutConfig(connect=15.0, read=300.0)
    field_name = 'total'

    assert timeout.total is None
    with pytest.raises(FrozenInstanceError):
        setattr(timeout, field_name, 60.0)


def test_transport_dataclass_semantic_field_order() -> None:
    assert tuple(field.name for field in dataclasses.fields(TimeoutConfig) if field.init) == (
        'connect',
        'read',
        'total',
    )
    assert tuple(field.name for field in dataclasses.fields(TransportRequest) if field.init) == (
        'method',
        'url',
        'headers',
        'body',
        'timeout',
        'token',
    )
    assert tuple(field.name for field in dataclasses.fields(TransportResponse) if field.init) == (
        'status',
        'reason',
        'headers',
        'content',
    )


def test_transport_error_is_a_shotgun_error() -> None:
    assert issubclass(TransportError, ShotgunError)


def test_transport_errors_share_a_base_class() -> None:
    error_types = (
        AbortedError,
        ConnectError,
        ProxyError,
        ReadTimeoutError,
        SendError,
        TLSError,
    )

    assert all(issubclass(error_type, TransportError) for error_type in error_types)


def test_transport_request_is_frozen() -> None:
    field_name = 'method'

    with pytest.raises(FrozenInstanceError):
        setattr(_REQ, field_name, 'GET')


def test_transport_response_close_consumes_callback_before_exception() -> None:
    callback_calls = 0

    def on_close() -> None:
        nonlocal callback_calls
        callback_calls += 1
        raise RuntimeError('close failed')

    response = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'',
        on_close=on_close,
    )

    with pytest.raises(RuntimeError, match='close failed'):
        response.close()
    response.close()

    assert callback_calls == 1


def test_transport_response_close_invokes_callback_once_concurrently() -> None:
    worker_count = 8
    start = threading.Barrier(worker_count)
    callback_entered = threading.Event()
    release_callback = threading.Event()
    callback_calls = 0
    callback_lock = threading.Lock()

    def close_response(response: TransportResponse) -> None:
        start.wait()
        response.close()

    def on_close() -> None:
        nonlocal callback_calls
        with callback_lock:
            callback_calls += 1
        callback_entered.set()
        assert release_callback.wait(timeout=5)

    response = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'',
        on_close=on_close,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(close_response, response) for _ in range(worker_count)]
        assert callback_entered.wait(timeout=5)
        release_callback.set()
        for future in futures:
            future.result(timeout=5)

    assert callback_calls == 1


def test_transport_response_preserves_normalized_headers() -> None:
    headers = CaseInsensitiveHeaders({'ETag': 'abc123'})
    response = TransportResponse(status=200, reason='OK', headers=headers, content=b'')

    assert response.headers is headers
