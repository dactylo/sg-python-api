import ast
import dataclasses
import json
import logging
import threading
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Never, TypeVar, cast

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3 import _driver as driver_module
from shotgun_api3 import _errors, _protocol
from shotgun_api3 import _transport as transport_module
from shotgun_api3._config import (
    CompatibilityWarningEvent,
    Config,
    ObserverEvent,
    ProgressEvent,
    RequestEvent,
    ResponseEvent,
    RetryEvent,
)
from shotgun_api3._driver import (
    AttemptControl,
    Driver,
    ExecutionOptions,
    LiveResponse,
    _DriverTiming,
)
from shotgun_api3._transport import (
    AbortedError,
    CaseInsensitiveHeaders,
    ConnectError,
    ProxyError,
    ReadTimeoutError,
    SendError,
    StreamingResponse,
    TimeoutConfig,
    TLSError,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._transports import _FakeTransport

_AmbientResult = TypeVar('_AmbientResult')
_AUTH_VALUE = 'one-use'
_BODY_CLOSE_FAILURE_REQUEST_ID = 'body-close-failure'
_CREDENTIAL = 'credential'
_GATEWAY = TransportResponse(status=502, reason='Bad Gateway', headers={}, content=b'')
_INFO = TransportResponse(
    status=200,
    reason='OK',
    headers={'content-type': 'application/json'},
    content=b'{"results": {"version": [8, 0]}}',
)
_OK = TransportResponse(
    status=200,
    reason='OK',
    headers={
        'content-type': 'application/json',
        'x-sg-correlation-id': 'correlation-1',
        'authorization': 'secret-response-header',
    },
    content=b'{"results": {"id": 7}}',
)


class _CancellationSignal(BaseException):
    pass


class _ChunkedResponse:
    def __init__(self, chunks: tuple[bytes, ...], events: list[str]) -> None:
        self.chunks = chunks
        self.headers: dict[str, str] = {}
        self.status = 200
        self._events = events

    def close(self) -> None:
        self._events.append('close')


class _ChunkedTransport:
    def __init__(self, response: LiveResponse) -> None:
        self.requests: list[TransportRequest] = []
        self.response = response

    def abort(self, token: str) -> None:
        del token

    def close(self) -> None:
        pass

    def send(self, request: TransportRequest) -> LiveResponse:
        self.requests.append(request)
        return self.response

    def send_streaming(self, request: TransportRequest) -> LiveResponse:
        return self.send(request)


class _SendCallVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self._aliases: list[dict[str, str]] = [{}]
        self._functions: list[str] = []
        self._transport_names: list[set[str]] = [set()]

    @staticmethod
    def _is_transport_annotation(node: ast.expr | None) -> bool:
        return (isinstance(node, ast.Name) and node.id in {'AsyncTransport', 'Transport'}) or (
            isinstance(node, ast.Attribute) and node.attr in {'AsyncTransport', 'Transport'}
        )

    def _is_transport_receiver(self, node: ast.expr) -> bool:
        return (
            isinstance(node, ast.Name)
            and any(node.id in names for names in reversed(self._transport_names))
        ) or (isinstance(node, ast.Attribute) and node.attr == '_transport')

    def _visit_function(
        self,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
    ) -> None:
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if node.args.vararg is not None:
            arguments.append(node.args.vararg)
        if node.args.kwarg is not None:
            arguments.append(node.args.kwarg)
        transport_names = {
            argument.arg
            for argument in arguments
            if argument.arg == 'transport' or self._is_transport_annotation(argument.annotation)
        }
        self._functions.append(node.name)
        self._aliases.append({})
        self._transport_names.append(transport_names)
        self.generic_visit(node)
        self._transport_names.pop()
        self._aliases.pop()
        self._functions.pop()

    def visit_Assign(self, node: ast.Assign) -> None:
        target_names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        for name in target_names:
            if self._is_transport_receiver(node.value):
                self._transport_names[-1].add(name)
            else:
                self._transport_names[-1].discard(name)
            self._aliases[-1].pop(name, None)
        if (
            isinstance(node.value, ast.Attribute)
            and node.value.attr in {'send', 'send_streaming'}
            and self._is_transport_receiver(node.value.value)
        ):
            for name in target_names:
                self._aliases[-1][name] = node.value.attr
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in {'send', 'send_streaming'}
            and self._is_transport_receiver(node.func.value)
        ):
            self.calls.append((self._functions[-1] if self._functions else '', node.func.attr))
        elif isinstance(node.func, ast.Name):
            for aliases in reversed(self._aliases):
                method = aliases.get(node.func.id)
                if method is not None:
                    self.calls.append((self._functions[-1] if self._functions else '', method))
                    break
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)


class _TrackedRequestBody:
    def __init__(
        self,
        chunks: tuple[bytes, ...],
        failure: BaseException | None = None,
        close_failure: BaseException | None = None,
    ) -> None:
        self.chunks = iter(chunks)
        self.failure = failure
        self.close_failure = close_failure
        self.close_failure_raised = False
        self.pulls = 0
        self.closes = 0

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        self.pulls += 1
        try:
            return next(self.chunks)
        except StopIteration:
            if self.failure is not None:
                raise self.failure from None
            raise

    def close(self) -> None:
        self.closes += 1
        if self.close_failure is not None:
            self.close_failure_raised = True
            raise self.close_failure


def _call(driver: Driver, config: Config, method: str = 'read') -> object:
    return driver.call(method, {'type': 'Shot'}, config_module.snapshot(config))


def _config() -> Config:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(script_name='script', api_key='key')
    return config


def _raise_ambient_exception() -> Never:
    raise RuntimeError('ambient exception state')


def _call_inside_except(function: Callable[[], _AmbientResult]) -> _AmbientResult:
    try:
        _raise_ambient_exception()
    except RuntimeError:
        return function()


def _request(
    context_timeout: TimeoutConfig,
    request_id: str,
    url: str = 'https://x.example:443/file',
) -> TransportRequest:
    return TransportRequest(
        method='GET',
        url=url,
        headers={},
        body=None,
        timeout=context_timeout,
        token=request_id,
    )


def _response_with_close_error(
    error: BaseException,
    calls: list[str],
    *,
    close_status: int | None = None,
    status: int = 200,
) -> TransportResponse:

    response: TransportResponse

    def close() -> None:
        calls.append('close')
        if close_status is not None:
            object.__setattr__(response, 'status', close_status)
        raise error

    response = TransportResponse(
        status=status,
        reason='OK' if status == 200 else 'Unavailable',
        headers={},
        content=b'',
        on_close=close,
    )
    return response


@pytest.mark.parametrize('chunks', [(), (b'', b'a', b'', b'bc')])
def test_attempt_control_collects_empty_and_multiple_chunks(chunks: tuple[bytes, ...]) -> None:
    clock_calls = 0
    events: list[ObserverEvent] = []

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 1.0

    control = AttemptControl(2, clock, 2.0, events.append, 'read', 'rpc')
    consumed = control.consume(chunks, total_bytes=3)

    assert type(consumed) is bytes
    assert consumed == b''.join(chunks)
    assert clock_calls == len(chunks)
    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == ([1, 3] if chunks else [])
    assert [event.total_bytes for event in progress] == ([3, 3] if chunks else [])


@pytest.mark.parametrize('body', [b'data', b''])
def test_attempt_control_consumes_buffered_body_without_copy(body: bytes) -> None:
    clock_calls = 0
    events: list[ObserverEvent] = []

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 1.0

    control = AttemptControl(
        _attempt=2,
        _clock=clock,
        _deadline=2.0,
        _observer=events.append,
        _operation='read',
        _route='rpc',
    )

    consumed = control._consume_buffered(body, total_bytes=len(body))

    assert consumed is body
    assert clock_calls == 1
    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == ([len(body)] if body else [])
    assert [event.total_bytes for event in progress] == ([len(body)] if body else [])


def test_call_detaches_nested_input_before_request_and_retry_observers() -> None:
    values = ['original']
    params = {'type': 'Shot', 'nested': {'values': values}}
    config = _config()
    config.on_request = lambda _event: values.append('request mutation')
    config.on_retry = lambda _event: values.append('retry mutation')
    fake = _FakeTransport([_GATEWAY, _OK])
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))

    assert driver.call('read', params, config_module.snapshot(config)) == {'id': 7}

    attempts = fake.requests
    assert len(attempts) == 2
    assert 'request mutation' in values
    assert 'retry mutation' in values
    for request in attempts:
        assert isinstance(request.body, bytes)
        assert json.loads(request.body)['params'][1] == {
            'type': 'Shot',
            'nested': {'values': ['original']},
        }
    assert attempts[0].body == attempts[1].body
    assert attempts[0].body is not attempts[1].body
    assert attempts[0].token != attempts[1].token


def test_call_extracts_results_with_fresh_rpc_request() -> None:
    fake = _FakeTransport([_OK])
    config = _config()

    assert _call(Driver(fake, _DriverTiming(sleep=lambda _delay: None)), config) == {'id': 7}

    assert len(fake.requests) == 1
    request = fake.requests[0]
    assert request.url == 'https://x.example:443/api3/json'
    assert request.method == 'POST'
    assert request.headers['content-type'] == 'application/json; charset=utf-8'
    assert b'"method_name": "read"' in cast('bytes', request.body)
    assert b'key' in cast('bytes', request.body)


@pytest.mark.parametrize('method', ['info', 'read'])
def test_call_rejects_unbuffered_response_and_closes_once(method: str) -> None:
    lifecycle: list[str] = []
    response = _ChunkedResponse((b'a', b'b'), lifecycle)
    transport = _ChunkedTransport(response)
    driver = Driver(transport, _DriverTiming(sleep=lambda _delay: None))

    with pytest.raises(TypeError, match=r'^RPC transport must return a buffered response$'):
        _call(driver, _config(), method)

    assert lifecycle == ['close']
    assert [
        json.loads(cast('bytes', request.body))['method_name'] for request in transport.requests
    ] == [method]


def test_call_routes_original_body_through_one_response_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UndecodableBytes(bytes):
        def decode(self, encoding: str = 'utf-8', errors: str = 'strict') -> str:
            del encoding, errors
            raise AssertionError('RPC status checking decoded the response body')

    body = UndecodableBytes(b'{"results": {"id": 7}}')
    decoded_bodies: list[bytes] = []

    def decode_response(_headers: Mapping[str, str], received: bytes) -> object:
        decoded_bodies.append(received)
        return {'results': {'id': 7}}

    monkeypatch.setattr(_protocol, 'decode_response', decode_response)
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=body,
    )

    assert Driver(
        _FakeTransport([response]),
        _DriverTiming(sleep=lambda _delay: None),
    ).call('info', {}, config_module.snapshot(_config())) == {'id': 7}
    assert decoded_bodies == [body]
    assert decoded_bodies[0] is body


def test_callbacks_and_sleep_run_without_configuration_lock() -> None:
    config = _config()
    transitions: list[str] = []

    def observe(_event: ObserverEvent) -> None:
        config.update_fields(page_prefetch=1)
        transitions.append('observe')

    def sleep(_delay: float) -> None:
        config.update_fields(page_prefetch=2)
        transitions.append('sleep')

    config.update_fields(on_request=observe, on_retry=observe)
    fake = _FakeTransport([_GATEWAY, _OK])

    assert _call(Driver(fake, _DriverTiming(sleep=sleep)), config) == {'id': 7}
    assert 'sleep' in transitions
    assert transitions.count('observe') == 3


def test_consumer_base_exception_closes_once_then_emits_terminal_response() -> None:
    order: list[str] = []
    events: list[ObserverEvent] = []
    response = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'',
        on_close=lambda: order.append('close'),
    )
    config = _config()

    def observe(event: ObserverEvent) -> None:
        events.append(event)
        if isinstance(event, ResponseEvent):
            order.append('response')

    config.on_response = observe
    context = config_module.snapshot(config)
    driver = Driver(_FakeTransport([response]), _DriverTiming(sleep=lambda _delay: None))

    def cancel(_response: object, _control: AttemptControl) -> object:
        raise _CancellationSignal

    with pytest.raises(_CancellationSignal):
        driver.execute(
            _protocol.classify_endpoint('download'),
            context,
            lambda attempt: _request(context.timeout, request_id=f'cancel-{attempt}'),
            cancel,
        )

    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert order == ['close', 'response']
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'


def test_deadline_retry_preserves_received_response_metadata() -> None:
    events: list[ObserverEvent] = []
    first = TransportResponse(
        status=200,
        reason='OK',
        headers={'x-sg-correlation-id': 'deadline-response'},
        content=b'',
    )
    second = TransportResponse(
        status=200,
        reason='OK',
        headers={'x-sg-correlation-id': 'success-response'},
        content=b'',
    )
    config = _config()
    config.update_fields(
        max_rpc_attempts=2,
        on_response=events.append,
        on_retry=events.append,
        rpc_attempt_interval=0,
    )
    context = dataclasses.replace(
        config_module.snapshot(config),
        timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0),
    )
    ticks = iter((0.0, 2.0, 2.0, 3.0, 3.5, 3.5))

    result = Driver(
        _FakeTransport([first, second]),
        _DriverTiming(clock=lambda: next(ticks), sleep=lambda _delay: None),
    ).execute(
        _protocol.classify_endpoint('download', body_restartable=True),
        context,
        lambda attempt: _request(context.timeout, request_id=f'deadline-{attempt}'),
        lambda _response, _control: {'ok': True},
    )

    assert result == {'ok': True}
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert [event.outcome for event in response_events] == ['retry', 'success']
    assert [event.status for event in response_events] == [200, 200]
    assert [event.correlation_id for event in response_events] == [
        'deadline-response',
        'success-response',
    ]
    retry_events = [event for event in events if isinstance(event, RetryEvent)]
    assert [event.status for event in retry_events] == [200]


def test_executor_accepts_live_response_without_buffered_content() -> None:
    lifecycle: list[str] = []
    response = _ChunkedResponse((b'a', b'b'), lifecycle)
    transport = _ChunkedTransport(response)
    context = config_module.snapshot(_config())
    driver = Driver(transport, _DriverTiming(sleep=lambda _delay: None))

    def consume(live_response: object, control: AttemptControl) -> bytes:
        assert isinstance(live_response, _ChunkedResponse)
        return control.consume(live_response.chunks, total_bytes=2)

    result = driver.execute(
        _protocol.classify_endpoint('download'),
        context,
        lambda attempt: _request(context.timeout, request_id=f'live-{attempt}'),
        consume,
    )

    assert result == b'ab'
    assert lifecycle == ['close']
    assert len(transport.requests) == 1


def test_exhausted_http_attempts_raise_after_exact_budget() -> None:
    config = _config()
    fake = _FakeTransport([_GATEWAY, _GATEWAY, _GATEWAY])

    with pytest.raises(_errors.ProtocolError):
        _call(Driver(fake, _DriverTiming(sleep=lambda _delay: None)), config)

    assert len(fake.requests) == 3


def test_fault_closes_response_and_raises_after_success_status() -> None:
    closed: list[bool] = []
    fault = TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=b'{"exception": true, "error_code": 102, "message": "no"}',
        on_close=lambda: closed.append(True),
    )
    fake = _FakeTransport([fault])

    with pytest.raises(_errors.AuthenticationFault):
        _call(Driver(fake, _DriverTiming(sleep=lambda _delay: None)), _config())

    assert closed == [True]


def test_generic_consumer_reports_progress_and_enforces_cumulative_total_deadline() -> None:
    ticks = iter((0.0, 0.2, 0.4, 0.8, 1.2, 1.4))
    events: list[ObserverEvent] = []
    config = _config()
    config.on_progress = events.append
    context = dataclasses.replace(
        config_module.snapshot(config),
        timeout=TimeoutConfig(connect=1.0, read=1.0, total=1.0),
        max_rpc_attempts=1,
    )
    fake = _FakeTransport([_OK])
    timing = _DriverTiming(clock=lambda: next(ticks), sleep=lambda _delay: None)
    driver = Driver(fake, timing)
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def consume(_response: LiveResponse, control: AttemptControl) -> bytes:
        return control.consume((b'a', b'b', b'c'), total_bytes=3)

    with pytest.raises(ReadTimeoutError):
        driver.execute(
            operation,
            context,
            lambda attempt: _request(context.timeout, request_id=f'attempt-{attempt}'),
            consume,
        )

    progress = [event for event in events if isinstance(event, ProgressEvent)]
    assert [event.bytes_transferred for event in progress] == [1, 2]
    assert len(fake.requests) == 1


def test_infinite_timeout_publishes_sanitized_compatibility_warning() -> None:
    events: list[ObserverEvent] = []
    config = _config()
    config.update_fields(timeout_secs=None, on_compatibility_warning=events.append)
    fake = _FakeTransport([_INFO])

    result = Driver(fake, _DriverTiming(sleep=lambda _delay: None)).call(
        'info',
        {},
        config_module.snapshot(config),
    )

    assert result == {'version': [8, 0]}
    assert [cast('CompatibilityWarningEvent', event).code for event in events] == [
        'legacy-infinite-timeout',
    ]
    assert 'script' not in repr(events)
    assert 'key' not in repr(events)


def test_info_retries_without_changing_protocol_classification() -> None:
    config = _config()
    fake = _FakeTransport([_GATEWAY, _INFO])
    sleeps: list[float] = []

    assert _call(Driver(fake, _DriverTiming(sleep=sleeps.append)), config, 'info') == {
        'version': [8, 0],
    }
    assert len(fake.requests) == 2
    assert len(sleeps) == 1


def test_localized_header_is_absent_when_false_and_auto_when_true() -> None:
    config = _config()
    fake = _FakeTransport([_OK, _OK])
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))

    assert _call(driver, config) == {'id': 7}
    config.localized = True
    assert _call(driver, config) == {'id': 7}

    assert 'locale' not in fake.requests[0].headers
    assert fake.requests[1].headers['locale'] == 'auto'


def test_merged_one_use_session_exchange_is_not_retried_after_ambiguous_send() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        extra_auth_params={'auth_token': _AUTH_VALUE},
        max_rpc_attempts=3,
        user_login='artist',
        user_password=_CREDENTIAL,
    )
    fake = _FakeTransport([SendError('ambiguous'), _OK])
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))

    with pytest.raises(SendError, match='ambiguous'):
        driver.call('get_session_token', None, config_module.snapshot(config))

    assert len(fake.requests) == 1
    body = fake.requests[0].body
    assert isinstance(body, bytes)
    assert json.loads(body) == {
        'method_name': 'get_session_token',
        'params': [
            {
                'user_login': 'artist',
                'user_password': _CREDENTIAL,
                'auth_token': _AUTH_VALUE,
            }
        ],
    }


@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('consumption', ['none', 'partial', 'exhausted', 'failure'])
def test_observed_request_body_closes_source_once_without_eager_pull(
    streaming: bool,
    consumption: str,
) -> None:
    primary = RuntimeError('source iteration failed')
    source = _TrackedRequestBody(
        (b'a',) if consumption == 'failure' else (b'a', b'bc'),
        failure=primary if consumption == 'failure' else None,
        close_failure=RuntimeError('secondary cleanup secret')
        if consumption == 'failure'
        else None,
    )
    events: list[ObserverEvent] = []
    progress_at_pull: list[list[int]] = []

    def progress() -> list[int]:
        return [event.bytes_transferred for event in events if isinstance(event, ProgressEvent)]

    class ConsumingTransport(_ChunkedTransport):
        def send(self, request: TransportRequest) -> LiveResponse:
            assert source.pulls == 0
            assert request.body is not None
            assert not isinstance(request.body, bytes)
            self.requests.append(request)
            if consumption != 'none':
                assert next(request.body) == b'a'
                progress_at_pull.append(progress())
            if consumption in {'exhausted', 'failure'}:
                if consumption == 'exhausted':
                    assert next(request.body) == b'bc'
                    progress_at_pull.append(progress())
                    with pytest.raises(StopIteration):
                        next(request.body)
                    progress_at_pull.append(progress())
                else:
                    with pytest.raises(RuntimeError) as caught:
                        next(request.body)
                    assert caught.value is primary
                    progress_at_pull.append(progress())
                    raise primary
            return self.response

    transport = ConsumingTransport(_OK)
    config = _config()
    config.on_progress = events.append
    context = dataclasses.replace(config_module.snapshot(config), max_rpc_attempts=1)

    def execute() -> bytes:
        return Driver(transport).execute(
            _protocol.classify_endpoint('object_put', body_restartable=True),
            context,
            lambda _attempt: dataclasses.replace(
                _request(context.timeout, request_id='observed-body'),
                body=source,
                headers={'content-length': '3'},
            ),
            lambda response, _control: cast('TransportResponse', response).content,
            options=ExecutionOptions(streaming=streaming),
        )

    if consumption == 'failure':
        with pytest.raises(RuntimeError) as caught:
            execute()
        assert caught.value is primary
    else:
        assert execute() == _OK.content

    assert source.pulls == {'none': 0, 'partial': 1, 'exhausted': 3, 'failure': 2}[consumption]
    assert source.closes == 1
    assert 'secondary cleanup secret' not in repr(events)
    assert (
        progress_at_pull
        == {
            'none': [],
            'partial': [[]],
            'exhausted': [[], [1], [1, 3]],
            'failure': [[], [1]],
        }[consumption]
    )
    body = transport.requests[0].body
    assert body is not None
    assert not isinstance(body, bytes)
    close = transport_module.body_close(body)
    assert close is not None
    close()
    assert source.closes == 1


def test_observer_events_are_ordered_frozen_and_redacted() -> None:
    events: list[ObserverEvent] = []
    config = _config()
    fake = _FakeTransport([_INFO, _GATEWAY, _OK])
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))
    assert driver.call('info', {}, config_module.snapshot(config)) == {'version': [8, 0]}
    config.update_fields(
        on_progress=events.append,
        on_request=events.append,
        on_response=events.append,
        on_retry=events.append,
    )

    assert _call(driver, config) == {'id': 7}

    assert [type(event) for event in events] == [
        RequestEvent,
        ResponseEvent,
        RetryEvent,
        RequestEvent,
        ProgressEvent,
        ResponseEvent,
    ]
    request_events = [event for event in events if isinstance(event, RequestEvent)]
    assert [event.http_method for event in request_events] == ['POST', 'POST']
    retry_response = cast('ResponseEvent', events[1])
    success_response = cast('ResponseEvent', events[-1])
    assert retry_response.outcome == 'retry'
    assert success_response.correlation_id == 'correlation-1'
    assert success_response.outcome == 'success'
    assert all(dataclasses.is_dataclass(event) for event in events)
    serialized = repr(events)
    assert 'script' not in serialized
    assert 'key' not in serialized
    assert 'authorization' not in serialized
    assert 'secret-response-header' not in serialized
    assert 'https://' not in serialized


def test_observer_failures_are_sanitized_and_non_invasive(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(_event: ObserverEvent) -> None:
        raise RuntimeError('observer-secret')

    config = _config()
    config.update_fields(
        on_progress=fail,
        on_request=fail,
        on_response=fail,
        on_retry=fail,
    )
    fake = _FakeTransport([_GATEWAY, _OK])

    with caplog.at_level(logging.WARNING, logger='shotgun_api3'):
        timing = _DriverTiming(sleep=lambda _delay: None)
        assert _call(Driver(fake, timing), config) == {'id': 7}

    assert len(fake.requests) == 2
    assert 'observer-secret' not in caplog.text


def test_one_use_session_exchange_is_not_retried_after_ambiguous_send() -> None:
    config = Config()
    config.set_server_params('https://x.example')
    config.update_fields(
        auth_token=_AUTH_VALUE,
        max_rpc_attempts=3,
        user_login='artist',
        user_password=_CREDENTIAL,
    )
    fake = _FakeTransport([SendError('ambiguous'), _OK])
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))

    with pytest.raises(SendError, match='ambiguous'):
        driver.call('get_session_token', None, config_module.snapshot(config))

    assert len(fake.requests) == 1
    body = fake.requests[0].body
    assert isinstance(body, bytes)
    assert json.loads(body) == {
        'method_name': 'get_session_token',
        'params': [
            {
                'user_login': 'artist',
                'user_password': _CREDENTIAL,
                'auth_token': _AUTH_VALUE,
            }
        ],
    }


def test_only_attempt_boundaries_call_transport_send_in_production() -> None:
    package = Path(__file__).parents[2] / 'src' / 'shotgun_api3'
    calls: set[tuple[str, str]] = set()
    for source_path in package.rglob('*.py'):
        visitor = _SendCallVisitor()
        visitor.visit(ast.parse(source_path.read_text(encoding='utf-8')))
        calls.update(
            (str(source_path.relative_to(package)), attribute)
            for _function, attribute in visitor.calls
        )

    assert calls == {
        ('_driver.py', 'send_streaming'),
        ('_driver.py', 'send'),
        ('_async_driver.py', 'send_streaming'),
        ('_async_driver.py', 'send'),
    }


@pytest.mark.parametrize('error_type', [RuntimeError, SendError])
@pytest.mark.parametrize('streaming', [False, True])
def test_post_response_body_close_failure_preserves_primary_and_closes_response(
    error_type: type[Exception],
    streaming: bool,
) -> None:
    primary = error_type('secret backend body close text')
    body_closes: list[str] = []
    response_closes: list[str] = []
    events: list[ObserverEvent] = []
    sleeps: list[float] = []
    consumer_calls: list[str] = []

    def body_chunks() -> Iterator[bytes]:
        try:
            yield b'first'
            yield b'second'
        finally:
            body_closes.append('close')
            raise primary

    class FailingCloseResponse(_ChunkedResponse):
        def __init__(self) -> None:
            super().__init__((b'',), response_closes)
            self.status = 201
            self.headers = {'x-sg-correlation-id': 'received-correlation'}

        def close(self) -> None:
            super().close()
            self.status = 599
            self.headers = {'x-sg-correlation-id': 'changed-correlation'}
            raise RuntimeError('secret backend response close text')

    class PartlyConsumingTransport(_ChunkedTransport):
        def send(self, request: TransportRequest) -> LiveResponse:
            assert request.body is not None
            assert not isinstance(request.body, bytes)
            assert next(request.body) == b'first'
            return super().send(request)

    transport = PartlyConsumingTransport(FailingCloseResponse())
    config = _config()
    config.update_fields(max_rpc_attempts=2, on_response=events.append, on_retry=events.append)
    context = config_module.snapshot(config)

    def consume(_response: LiveResponse, _control: AttemptControl) -> None:
        consumer_calls.append('consume')

    with pytest.raises(error_type) as caught:
        Driver(transport, _DriverTiming(sleep=sleeps.append)).execute(
            _protocol.classify_endpoint('object_put', body_restartable=True),
            context,
            lambda _attempt: TransportRequest(
                method='PUT',
                url='https://storage.example/object',
                headers={},
                body=body_chunks(),
                timeout=context.timeout,
                token=_BODY_CLOSE_FAILURE_REQUEST_ID,
            ),
            consume,
            options=ExecutionOptions(streaming=streaming),
        )

    assert caught.value is primary
    assert body_closes == ['close']
    assert response_closes == ['close']
    assert len(transport.requests) == 1
    assert sleeps == []
    assert consumer_calls == []
    response_events = [event for event in events if isinstance(event, ResponseEvent)]
    assert len(response_events) == 1
    assert response_events[0].outcome == 'error'
    assert response_events[0].status == 201
    assert response_events[0].correlation_id == 'received-correlation'
    assert [event for event in events if isinstance(event, RetryEvent)] == []
    assert 'secret backend' not in repr(events)
    body = transport.requests[0].body
    assert body is not None
    assert not isinstance(body, bytes)
    close = transport_module.body_close(body)
    assert close is not None
    close()
    assert body_closes == ['close']


def test_primary_consumer_error_survives_response_close_failure() -> None:
    close_calls: list[str] = []
    response = _response_with_close_error(
        RuntimeError('secret backend close text'),
        close_calls,
    )
    context = config_module.snapshot(_config())
    primary = _errors.ShotgunError('primary')

    def consume(_response: LiveResponse, _control: AttemptControl) -> object:
        raise primary

    with pytest.raises(_errors.ShotgunError) as caught:
        Driver(
            _FakeTransport([response]),
            _DriverTiming(sleep=lambda _delay: None),
        ).execute(
            _protocol.classify_endpoint('download'),
            context,
            lambda attempt: _request(context.timeout, request_id=f'error-{attempt}'),
            consume,
        )

    assert caught.value is primary
    assert close_calls == ['close']


def test_read_retries_exact_attempts_with_fresh_bodies() -> None:
    config = _config()
    fake = _FakeTransport([_GATEWAY, _GATEWAY, _OK])
    sleeps: list[float] = []

    timing = _DriverTiming(sleep=sleeps.append, jitter_seed=lambda: 1000.25)
    assert _call(Driver(fake, timing), config) == {'id': 7}

    attempts = fake.requests
    assert len(attempts) == config.max_rpc_attempts
    assert len(sleeps) == 2
    assert len({request.token for request in attempts}) == 3
    assert attempts[0].body == attempts[1].body == attempts[2].body
    assert attempts[0].body is not attempts[1].body


def test_redirect_close_failure_inside_except_preserves_redirect_and_observer() -> None:
    close_calls: list[str] = []
    events: list[ObserverEvent] = []
    redirect = _response_with_close_error(
        RuntimeError('secret backend close text'),
        close_calls,
        close_status=599,
        status=302,
    )
    object.__setattr__(
        redirect,
        'headers',
        CaseInsensitiveHeaders({'location': 'https://storage.example/signed'}),
    )
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'data',
    )
    transport = _FakeTransport([redirect, final])
    config = _config()
    config.on_response = events.append
    context = dataclasses.replace(config_module.snapshot(config), max_rpc_attempts=1)

    result = _call_inside_except(
        lambda: Driver(transport, _DriverTiming(sleep=lambda _delay: None)).execute(
            _protocol.classify_endpoint('download', body_restartable=True),
            context,
            lambda _attempt: _request(context.timeout, request_id='original'),
            lambda response, _control: cast('TransportResponse', response).content,
            options=ExecutionOptions(
                redirect_factory=lambda response: _request(
                    context.timeout,
                    request_id='redirect',
                    url=response.headers['location'],
                ),
            ),
        )
    )

    assert result == b'data'
    assert [request.url for request in transport.requests] == [
        'https://x.example:443/file',
        'https://storage.example/signed',
    ]
    assert close_calls == ['close']
    assert [event.outcome for event in events if isinstance(event, ResponseEvent)] == [
        'success',
        'success',
    ]
    assert [event.status for event in events if isinstance(event, ResponseEvent)] == [302, 200]
    assert 'secret backend close text' not in repr(events)


def test_request_body_cleanup_uses_shared_decision_inline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = iter(())
    close_threads: list[int] = []
    context = dataclasses.replace(config_module.snapshot(_config()), max_rpc_attempts=1)
    request = dataclasses.replace(
        _request(context.timeout, request_id='shared-body-close'),
        body=body,
    )

    def selected_close(_candidate: object) -> Callable[[], object] | None:
        return lambda: close_threads.append(threading.get_ident())

    monkeypatch.setattr(transport_module, 'body_close', selected_close)

    result = Driver(_FakeTransport([_OK])).execute(
        _protocol.classify_endpoint('object_put', body_restartable=True),
        context,
        lambda _attempt: request,
        lambda response, _control: cast('TransportResponse', response).content,
    )

    assert result == _OK.content
    assert close_threads == [threading.get_ident()]


def test_response_cleanup_only_failure_inside_except_is_redacted() -> None:
    events: list[ObserverEvent] = []
    response = _response_with_close_error(
        RuntimeError(
            'secret backend close text for https://x.example/file?session_token=request-secret'
        ),
        [],
    )

    with pytest.raises(SendError) as caught:
        _call_inside_except(
            lambda: driver_module._close_response(
                response,
                has_primary=False,
            )
        )

    assert type(caught.value) is SendError
    assert str(caught.value) == 'response cleanup failed'
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    exposed = repr((caught.value, events))
    assert 'secret backend close text' not in exposed
    assert 'request-secret' not in exposed


def test_response_close_failure_inside_except_preserves_retries_and_observers() -> None:
    close_calls: list[str] = []
    events: list[ObserverEvent] = []
    responses: list[TransportResponse | TransportError] = [
        _response_with_close_error(
            RuntimeError('secret backend close text'),
            close_calls,
            status=status,
        )
        for status in (503, 503, 200)
    ]
    config = _config()
    config.update_fields(
        max_rpc_attempts=3,
        on_response=events.append,
        on_retry=events.append,
        rpc_attempt_interval=0,
    )
    context = config_module.snapshot(config)
    transport = _FakeTransport(responses)

    result = _call_inside_except(
        lambda: Driver(transport, _DriverTiming(sleep=lambda _delay: None)).execute(
            _protocol.classify_endpoint('download', body_restartable=True),
            context,
            lambda attempt: _request(context.timeout, request_id=f'retry-{attempt}'),
            lambda _response, _control: {'ok': True},
        )
    )

    assert len(transport.requests) == 3
    assert result == {'ok': True}
    assert close_calls == ['close', 'close', 'close']
    assert [event.outcome for event in events if isinstance(event, ResponseEvent)] == [
        'retry',
        'retry',
        'success',
    ]
    assert len([event for event in events if isinstance(event, RetryEvent)]) == 2
    assert 'secret backend close text' not in repr(events)


def test_response_close_failure_inside_except_preserves_success_and_observer() -> None:
    close_calls: list[str] = []
    events: list[ObserverEvent] = []
    response = _response_with_close_error(
        RuntimeError('secret backend close text'),
        close_calls,
        close_status=599,
    )
    config = _config()
    config.on_response = events.append
    context = config_module.snapshot(config)

    result = _call_inside_except(
        lambda: Driver(
            _FakeTransport([response]),
            _DriverTiming(sleep=lambda _delay: None),
        ).execute(
            _protocol.classify_endpoint('download'),
            context,
            lambda attempt: _request(context.timeout, request_id=f'success-{attempt}'),
            lambda _response, _control: {'ok': True},
        )
    )

    assert result == {'ok': True}
    assert close_calls == ['close']
    assert [event.outcome for event in events if isinstance(event, ResponseEvent)] == ['success']
    assert [event.status for event in events if isinstance(event, ResponseEvent)] == [200]
    assert 'secret backend close text' not in repr(events)


def test_response_is_closed_before_retry_observer_and_sleep() -> None:
    order: list[str] = []
    gateway = TransportResponse(
        status=502,
        reason='Bad Gateway',
        headers={},
        content=b'error body',
        on_close=lambda: order.append('close'),
    )
    fake = _FakeTransport([gateway, _OK])
    config = _config()
    config.on_retry = lambda _event: order.append('retry')

    timing = _DriverTiming(sleep=lambda _delay: order.append('sleep'))
    assert _call(Driver(fake, timing), config) == {'id': 7}
    assert order == ['close', 'retry', 'sleep']


@pytest.mark.parametrize(
    ('method_name', 'category'),
    [('followers', 'read'), ('follow', 'write')],
)
def test_rpc_observers_use_operation_category(
    method_name: str,
    category: _protocol.OperationCategory,
) -> None:
    events: list[ObserverEvent] = []
    config = _config()
    config.on_request = events.append
    config.on_response = events.append
    fake = _FakeTransport([_OK])

    _call(Driver(fake, _DriverTiming(sleep=lambda _delay: None)), config, method_name)

    rpc_events = [event for event in events if isinstance(event, (RequestEvent, ResponseEvent))]
    assert [event.operation for event in rpc_events] == [category, category]


@pytest.mark.parametrize(
    ('retry_after', 'expected_delay', 'should_retry'),
    [
        pytest.param('0', 0.0, True, id='delta-zero'),
        pytest.param('30', 30.0, True, id='delta-cap'),
        pytest.param('31', None, False, id='delta-oversized'),
        pytest.param('9' * 5_000, None, False, id='delta-very-long-oversized'),
        pytest.param('Wed, 01 Jan 2025 00:00:00 GMT', 0.0, True, id='date-zero'),
        pytest.param('Wed, 01 Jan 2025 00:00:30 GMT', 30.0, True, id='date-cap'),
        pytest.param('Wed, 01 Jan 2025 00:00:31 GMT', None, False, id='date-oversized'),
        pytest.param('tomorrow', None, True, id='malformed'),
        pytest.param('-1', None, True, id='negative'),
        pytest.param('NaN', None, True, id='nan'),
        pytest.param('Infinity', None, True, id='positive-infinity'),
        pytest.param('-Infinity', None, True, id='negative-infinity'),
    ],
)
def test_retry_after_never_sleeps_above_backoff_cap(
    retry_after: str,
    expected_delay: float | None,
    should_retry: bool,
) -> None:
    retries: list[ObserverEvent] = []
    sleeps: list[float] = []
    config = _config()
    config.update_fields(rpc_attempt_interval=4000)
    config.on_retry = retries.append
    retry = TransportResponse(
        status=429,
        reason='Retry Later',
        headers={'retry-after': retry_after},
        content=b'',
    )
    transport = _FakeTransport([retry, _OK])
    driver = Driver(
        transport,
        _DriverTiming(
            jitter_seed=lambda: 1000.25,
            sleep=sleeps.append,
            wall_clock=lambda: 1_735_689_600.0,
        ),
    )

    if not should_retry:
        with pytest.raises(_errors.ProtocolError):
            driver.call('info', {}, config_module.snapshot(config))
        assert sleeps == []
        assert retries == []
        assert len(transport.requests) == 1
        return

    assert driver.call('info', {}, config_module.snapshot(config)) == {'id': 7}
    if expected_delay is None:
        fallback_delay = _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'rate_limit', 429, None),
            max_attempts=config.max_rpc_attempts,
            base_interval=config.rpc_attempt_interval / 1000.0,
            now=1000.25,
        )
        assert fallback_delay is not None
        assert sleeps == [fallback_delay]
    else:
        assert sleeps == [expected_delay]
    assert all(delay <= 30.0 for delay in sleeps)
    assert [type(event) for event in retries] == [RetryEvent]
    assert len(transport.requests) == 2


@pytest.mark.parametrize('streaming', [False, True])
def test_redirect_budget_includes_request_observer_time(streaming: bool) -> None:
    now = 0.0
    requests: list[RequestEvent] = []
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers={'location': 'https://storage.example/signed?secret=value'},
        content=b'',
        on_close=lambda: advance(3.0),
    )
    final = TransportResponse(status=200, reason='OK', headers={}, content=b'data')
    fake = _FakeTransport([redirect, final])
    config = _config()

    def advance(value: float) -> None:
        nonlocal now
        now = value

    def observe(event: ObserverEvent) -> None:
        assert isinstance(event, RequestEvent)
        requests.append(event)
        if len(requests) == 2:
            advance(5.0)

    config.on_request = observe
    context = dataclasses.replace(
        config_module.snapshot(config),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )

    def consume(response: LiveResponse, control: AttemptControl) -> bytes:
        if streaming:
            return control.consume(
                cast('StreamingResponse', response).iter_chunks(), total_bytes=4
            )
        return cast('TransportResponse', response).content

    result = Driver(fake, _DriverTiming(clock=lambda: now)).execute(
        _protocol.classify_endpoint('download'),
        context,
        lambda _attempt: _request(context.timeout, request_id='original'),
        consume,
        options=ExecutionOptions(
            redirect_factory=lambda response: _request(
                context.timeout,
                request_id='redirected',
                url=response.headers['location'],
            ),
            streaming=streaming,
        ),
    )

    assert result == b'data'
    assert len(requests) == 2
    assert [request.timeout.total for request in fake.requests] == [10.0, 5.0]


@pytest.mark.parametrize('replay_safe', [False, True])
def test_observed_request_body_preserves_send_failure_and_retry(replay_safe: bool) -> None:
    primary = SendError('send failed')
    transport = _FakeTransport([primary, _OK])
    sources: list[_TrackedRequestBody] = []
    events: list[ObserverEvent] = []
    config = _config()
    config.update_fields(max_rpc_attempts=2, on_retry=events.append)
    context = config_module.snapshot(config)

    def request_factory(attempt: int) -> TransportRequest:
        source = _TrackedRequestBody(
            (b'unsent',),
            close_failure=RuntimeError('secondary cleanup secret') if attempt == 1 else None,
        )
        sources.append(source)
        return dataclasses.replace(
            _request(context.timeout, request_id=f'send-failure-{attempt}'),
            body=source,
        )

    def execute() -> bytes:
        return Driver(transport, _DriverTiming(sleep=lambda _delay: None)).execute(
            _protocol.classify_endpoint('download' if replay_safe else 'allocation'),
            context,
            request_factory,
            lambda response, _control: cast('TransportResponse', response).content,
        )

    if replay_safe:
        assert execute() == _OK.content
    else:
        with pytest.raises(SendError) as caught:
            execute()
        assert caught.value is primary

    assert [source.pulls for source in sources] == ([0, 0] if replay_safe else [0])
    assert [source.closes for source in sources] == ([1, 1] if replay_safe else [1])
    assert len(transport.requests) == (2 if replay_safe else 1)
    assert len([event for event in events if isinstance(event, RetryEvent)]) == int(replay_safe)
    assert 'secondary cleanup secret' not in repr(events)


@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('expiry', ['cleanup', 'observer'])
@pytest.mark.parametrize('policy', ['one_attempt', 'replay_safe', 'unsafe'])
def test_redirect_deadline_expiry_uses_attempt_retry_policy(
    streaming: bool,
    expiry: str,
    policy: str,
) -> None:
    now = 0.0
    events: list[ObserverEvent] = []
    sleeps: list[float] = []
    response_closes: list[str] = []
    consumer_calls: list[str] = []
    redirect_bodies: list[_TrackedRequestBody] = []
    retry = policy == 'replay_safe'
    redirects = [
        TransportResponse(
            status=302,
            reason='Found',
            headers={'location': 'https://storage.example/signed?secret=value'},
            content=b'',
            on_close=lambda attempt=attempt: close_redirect(attempt),
        )
        for attempt in (1, 2)
    ]
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'data',
        on_close=lambda: response_closes.append('final'),
    )
    fake = _FakeTransport([redirects[0], redirects[1], final])
    config = _config()

    def close_redirect(attempt: int) -> None:
        nonlocal now
        response_closes.append(f'redirect-{attempt}')
        now = (11.0 if expiry == 'cleanup' else 3.0) if attempt == 1 else 23.0

    def observe_request(event: ObserverEvent) -> None:
        nonlocal now
        assert isinstance(event, RequestEvent)
        events.append(event)
        request_count = len([item for item in events if isinstance(item, RequestEvent)])
        if request_count == 2 and expiry == 'observer':
            now = 11.0
        if request_count == 4:
            now = 25.0

    def sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now = 20.0

    config.update_fields(
        on_request=observe_request,
        on_response=events.append,
        on_retry=events.append,
        max_rpc_attempts=2 if policy != 'one_attempt' else 1,
    )
    context = dataclasses.replace(
        config_module.snapshot(config),
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )

    def redirect_factory(response: LiveResponse) -> TransportRequest:
        attempt = len(redirect_bodies) + 1
        body = _TrackedRequestBody(
            (b'unsent',),
            close_failure=RuntimeError('secondary cleanup secret') if attempt == 1 else None,
        )
        redirect_bodies.append(body)
        return dataclasses.replace(
            _request(
                context.timeout, request_id=f'redirect-{attempt}', url=response.headers['location']
            ),
            body=body,
            headers={'authorization': 'secret-request-header'},
        )

    def consume(response: LiveResponse, _control: AttemptControl) -> bytes:
        consumer_calls.append('consume')
        return cast('TransportResponse', response).content if not streaming else b'data'

    driver = Driver(fake, _DriverTiming(clock=lambda: now, sleep=sleep))

    def call() -> bytes:
        return driver.execute(
            _protocol.classify_endpoint('allocation' if policy == 'unsafe' else 'download'),
            context,
            lambda attempt: _request(context.timeout, request_id=f'original-{attempt}'),
            consume,
            options=ExecutionOptions(redirect_factory=redirect_factory, streaming=streaming),
        )

    if retry:
        assert call() == b'data'
    else:
        with pytest.raises(ReadTimeoutError):
            call()

    assert [request.timeout.total for request in fake.requests] == (
        [10.0, 10.0, 5.0] if retry else [10.0]
    )
    assert len([event for event in events if isinstance(event, RequestEvent)]) == (
        4 if retry else 2
    )
    assert [
        (event.attempt, event.status, event.outcome)
        for event in events
        if isinstance(event, ResponseEvent)
    ] == (
        [(1, 302, 'success'), (1, None, 'retry'), (2, 302, 'success'), (2, 200, 'success')]
        if retry
        else [(1, 302, 'success'), (1, None, 'error')]
    )
    assert (
        len([event for event in events if isinstance(event, RetryEvent)])
        == len(sleeps)
        == int(retry)
    )
    assert (
        response_closes,
        [(body.pulls, body.closes) for body in redirect_bodies],
        consumer_calls,
    ) == (
        ['redirect-1', 'redirect-2', 'final'] if retry else ['redirect-1'],
        [(0, 1), (0, 1)] if retry else [(0, 1)],
        ['consume'] if retry else [],
    )
    assert (
        redirect_bodies[0].close_failure is not None,
        [body.close_failure_raised for body in redirect_bodies],
    ) == (
        True,
        [True, False] if retry else [True],
    )
    assert all(
        secret not in repr(events)
        for secret in ('secret=value', 'secret-request-header', 'secondary cleanup secret')
    )


def test_safe_read_retries_ambiguous_send() -> None:
    fake = _FakeTransport([SendError('ambiguous'), _OK])

    result = _call(
        Driver(fake, _DriverTiming(sleep=lambda _delay: None)),
        _config(),
        method='followers',
    )

    assert result == {'id': 7}
    assert len(fake.requests) == 2


def test_send_call_visitor_detects_annotated_transport_parameters() -> None:
    source = """
def sync_exchange(backend: Transport, request):
    return backend.send(request)

async def async_exchange(backend: AsyncTransport, request):
    return await backend.send_streaming(request)
"""
    visitor = _SendCallVisitor()

    visitor.visit(ast.parse(source))

    assert visitor.calls == [
        ('sync_exchange', 'send'),
        ('async_exchange', 'send_streaming'),
    ]


def test_send_call_visitor_detects_streaming_and_bound_method_aliases() -> None:
    source = """
def exchange(transport, request):
    send_attempt = transport.send_streaming
    return send_attempt(request)
"""
    visitor = _SendCallVisitor()

    visitor.visit(ast.parse(source))

    assert visitor.calls == [('exchange', 'send_streaming')]


def test_send_call_visitor_detects_transport_identity_alias() -> None:
    source = """
def exchange(self, request):
    backend = self._transport
    return backend.send(request)
"""
    visitor = _SendCallVisitor()

    visitor.visit(ast.parse(source))

    assert visitor.calls == [('exchange', 'send')]


def test_send_call_visitor_ignores_nontransport_client_send() -> None:
    source = """
async def exchange(self, request):
    return await self._client.send(request, stream=True)
"""
    visitor = _SendCallVisitor()

    visitor.visit(ast.parse(source))

    assert visitor.calls == []


def test_streaming_redirect_is_one_executor_owned_attempt() -> None:
    releases: list[str] = []
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers=CaseInsensitiveHeaders(
            {'location': 'https://storage.example/signed?secret=value'}
        ),
        content=b'',
        on_close=lambda: releases.append('redirect'),
    )
    final = TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-length': '4'}),
        content=b'data',
        on_close=lambda: releases.append('final'),
    )
    fake = _FakeTransport([redirect, final])
    context = dataclasses.replace(config_module.snapshot(_config()), max_rpc_attempts=1)
    driver = Driver(fake, _DriverTiming(sleep=lambda _delay: None))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def redirect_factory(response: LiveResponse) -> TransportRequest:
        return _request(
            context.timeout,
            request_id='redirect',
            url=response.headers['location'],
        )

    def consume(response: LiveResponse, control: AttemptControl) -> bytes:
        streaming = cast('StreamingResponse', response)
        return control.consume(streaming.iter_chunks(2), total_bytes=4)

    result = driver.execute(
        operation,
        context,
        lambda _attempt: _request(
            context.timeout,
            request_id='original',
            url='https://x.example/file_serve/attachment/41',
        ),
        consume,
        options=ExecutionOptions(
            redirect_factory=redirect_factory,
            streaming=True,
        ),
    )

    assert result == b'data'
    assert [request.url for request in fake.requests] == [
        'https://x.example/file_serve/attachment/41',
        'https://storage.example/signed?secret=value',
    ]
    assert releases == ['redirect', 'final']


def test_streaming_redirect_uses_only_remaining_total_timeout() -> None:
    ticks = iter((0.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    redirect = TransportResponse(
        status=302,
        reason='Found',
        headers={'location': 'https://storage.example/signed'},
        content=b'',
    )
    final = TransportResponse(
        status=200,
        reason='OK',
        headers={},
        content=b'data',
    )
    fake = _FakeTransport([redirect, final])
    context = dataclasses.replace(
        config_module.snapshot(_config()),
        max_rpc_attempts=1,
        timeout=TimeoutConfig(connect=10.0, read=10.0, total=10.0),
    )
    driver = Driver(fake, _DriverTiming(clock=lambda: next(ticks)))
    operation = _protocol.classify_endpoint('download', body_restartable=True)

    def redirect_factory(response: LiveResponse) -> TransportRequest:
        return _request(
            context.timeout,
            request_id='redirect',
            url=response.headers['location'],
        )

    def consume(response: LiveResponse, control: AttemptControl) -> bytes:
        streaming = cast('StreamingResponse', response)
        return control.consume(streaming.iter_chunks(), total_bytes=4)

    assert (
        driver.execute(
            operation,
            context,
            lambda _attempt: _request(context.timeout, request_id='original'),
            consume,
            options=ExecutionOptions(
                redirect_factory=redirect_factory,
                streaming=True,
            ),
        )
        == b'data'
    )

    assert fake.requests[0].timeout.total == 10.0
    assert fake.requests[1].timeout.total == 7.0


@pytest.mark.parametrize(
    'error',
    [AbortedError('abort-token'), ProxyError('proxy auth'), TLSError('certificate')],
)
def test_terminal_transport_categories_never_retry(error: TransportError) -> None:
    config = _config()
    fake = _FakeTransport([error])

    with pytest.raises(type(error)):
        _call(Driver(fake, _DriverTiming(sleep=lambda _delay: None)), config)

    assert len(fake.requests) == 1


def test_unsafe_write_retries_pre_send_but_not_ambiguous_send() -> None:
    config = _config()
    pre_send = _FakeTransport([ConnectError('refused'), _OK])
    no_sleep = _DriverTiming(sleep=lambda _delay: None)
    assert _call(Driver(pre_send, no_sleep), config, method='create') == {'id': 7}
    assert len(pre_send.requests) == 2

    ambiguous = _FakeTransport([SendError('connection lost mid-body')])
    with pytest.raises(SendError):
        _call(Driver(ambiguous, no_sleep), config, method='create')
    assert len(ambiguous.requests) == 1


def test_write_does_not_retry_ambiguous_send() -> None:
    fake = _FakeTransport([SendError('ambiguous'), _OK])

    with pytest.raises(SendError, match='ambiguous'):
        _call(
            Driver(fake, _DriverTiming(sleep=lambda _delay: None)),
            _config(),
            method='follow',
        )

    assert len(fake.requests) == 1
