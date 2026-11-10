import dataclasses
import inspect
import json
import warnings
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, cast, get_type_hints

import pytest

from shotgun_api3 import _config as config_module
from shotgun_api3 import _errors, _files
from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._async_driver import AsyncDriver, _AsyncDriverTiming
from shotgun_api3._async_transport import AsyncTransport
from shotgun_api3._client import Shotgun
from shotgun_api3._config import (
    CompatibilityWarningEvent,
    ResponseEvent,
    RetryEvent,
    _CompatibilityWarningCode,
    _ObserverEvent,
)
from shotgun_api3._driver import Driver, _DriverTiming
from shotgun_api3._transport import (
    CaseInsensitiveHeaders,
    TimeoutConfig,
    Transport,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._pyright import run_pyright

from .conftest import FakeAsyncClient, fake_async_transport, fake_client, fake_transport

pytestmark = pytest.mark.async_required


_CLIENT_METHOD_PARITY_METHODS = (
    'activity_stream_read',
    'authenticate_human_user',
    'entity_types',
    'export_page',
    'follow',
    'followers',
    'following',
    'nav_expand',
    'nav_search_entity',
    'nav_search_string',
    'note_thread_read',
    'preferences_read',
    'schema',
    'schema_entity_read',
    'schema_field_create',
    'schema_field_delete',
    'schema_field_read',
    'schema_field_update',
    'schema_read',
    'summarize',
    'text_search',
    'unfollow',
    'update_project_last_accessed',
    'user_subscriptions_create',
    'user_subscriptions_read',
    'work_schedule_read',
    'work_schedule_update',
)

_CRUD_PARITY_METHODS = ('batch', 'create', 'delete', 'find', 'find_one', 'revive', 'update')

_ENTITY = {'type': 'Shot', 'id': 1, 'code': 'X'}
_INVALID_PAGE_ID = True
_PAGE = {'entities': [_ENTITY], 'paging_info': {'entity_count': 1}}


@dataclasses.dataclass(frozen=True)
class _Failure:
    exception: type[Exception]


@dataclasses.dataclass(frozen=True)
class _RequestTranscript:
    body: bytes | None
    headers: tuple[tuple[str, str], ...]
    method: str
    timeout: TimeoutConfig
    url: str


@dataclasses.dataclass(frozen=True)
class _Success:
    result: object


@dataclasses.dataclass
class _Trace:
    closed_responses: list[str] = dataclasses.field(default_factory=list[str])
    response_events: list[ResponseEvent] = dataclasses.field(default_factory=list[ResponseEvent])
    retry_events: list[RetryEvent] = dataclasses.field(default_factory=list[RetryEvent])
    sleeps: list[float] = dataclasses.field(default_factory=list[float])
    warning_events: list[CompatibilityWarningEvent] = dataclasses.field(
        default_factory=list[CompatibilityWarningEvent]
    )


@dataclasses.dataclass(frozen=True)
class _Scenario:
    call: Callable[[Shotgun | AsyncShotgun], object]
    expected: _Failure | _Success
    expected_methods: tuple[str, ...]
    name: str
    script: Callable[[_Trace], list[TransportResponse | TransportError]]
    expected_closed_responses: tuple[str, ...] = ('result',)
    expected_response_outcomes: tuple[str, ...] = ('success', 'success')
    expected_sleeps: tuple[float, ...] = ()
    expected_warning_codes: tuple[_CompatibilityWarningCode, ...] = ()
    expected_operation_auth: dict[str, object] | None = None
    legacy_infinite_timeout: bool = False
    localized: bool = False
    mutate_session_uuid_after_readiness: bool = False
    secret_auth: bool = False
    session_uuid: str | None = None


def _assert_trace(scenario: _Scenario, trace: _Trace) -> None:
    assert tuple(trace.closed_responses) == scenario.expected_closed_responses
    assert tuple(event.outcome for event in trace.response_events) == (
        scenario.expected_response_outcomes
    )
    assert tuple(trace.sleeps) == scenario.expected_sleeps
    assert tuple(event.delay for event in trace.retry_events) == scenario.expected_sleeps
    assert tuple(event.code for event in trace.warning_events) == (scenario.expected_warning_codes)
    serialized_warnings = repr(trace.warning_events)
    assert 'parity-api-secret' not in serialized_warnings
    assert 'parity-script-secret' not in serialized_warnings


def _batch_requests() -> list[dict[str, Any]]:
    return [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {'code': 'X'},
            'return_fields': ['code'],
        },
        {
            'request_type': 'update',
            'entity_type': 'Shot',
            'entity_id': 1,
            'data': {'code': 'Y'},
        },
        {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': 2},
    ]


def _configure(sg: AsyncShotgun | Shotgun, scenario: _Scenario, trace: _Trace) -> None:
    def observe_response(event: _ObserverEvent) -> None:
        assert isinstance(event, ResponseEvent)
        trace.response_events.append(event)
        if scenario.mutate_session_uuid_after_readiness and len(trace.response_events) == 1:
            sg.config.session_uuid = 'later-session'

    def observe_retry(event: _ObserverEvent) -> None:
        assert isinstance(event, RetryEvent)
        trace.retry_events.append(event)

    def observe_warning(event: _ObserverEvent) -> None:
        assert isinstance(event, CompatibilityWarningEvent)
        trace.warning_events.append(event)

    fields: dict[str, object] = {
        'localized': scenario.localized,
        'max_rpc_attempts': 3,
        'on_compatibility_warning': observe_warning,
        'on_response': observe_response,
        'on_retry': observe_retry,
        'rpc_attempt_interval': 0,
    }
    if scenario.legacy_infinite_timeout:
        fields['timeout_secs'] = None
    if scenario.secret_auth:
        fields['api_key'] = 'parity-api-secret'
        fields['script_name'] = 'parity-script-secret'
    if scenario.session_uuid is not None:
        fields['session_uuid'] = scenario.session_uuid
    sg.config.update_fields(**fields)


async def _execute_call(sg: Shotgun | AsyncShotgun, scenario: _Scenario) -> object:
    result = scenario.call(sg)
    assert inspect.iscoroutine(result) is isinstance(sg, AsyncShotgun)
    if inspect.isawaitable(result):
        return await result
    return result


def _fault(trace: _Trace, label: str, code: int) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
        content=json.dumps(
            {
                'exception': True,
                'error_code': code,
                'message': 'scenario fault',
            }
        ).encode(),
        on_close=lambda: trace.closed_responses.append(label),
    )


def _install_async_timing(monkeypatch: pytest.MonkeyPatch, trace: _Trace) -> None:
    async def sleep(delay: float) -> None:
        trace.sleeps.append(delay)

    def build(transport: AsyncTransport) -> AsyncDriver:
        return AsyncDriver(
            transport,
            _AsyncDriverTiming(
                clock=lambda: 0.0,
                jitter_seed=lambda: 0.0,
                sleep=sleep,
                wall_clock=lambda: 0.0,
            ),
        )

    monkeypatch.setattr('shotgun_api3._async_client.AsyncDriver', build)


def _install_sync_timing(monkeypatch: pytest.MonkeyPatch, trace: _Trace) -> None:
    def build(transport: Transport) -> Driver:
        return Driver(
            transport,
            _DriverTiming(
                clock=lambda: 0.0,
                jitter_seed=lambda: 0.0,
                sleep=trace.sleeps.append,
                wall_clock=lambda: 0.0,
            ),
        )

    monkeypatch.setattr('shotgun_api3._client.Driver', build)


def _method_names(transcript: list[_RequestTranscript]) -> tuple[str, ...]:
    names: list[str] = []
    for request in transcript:
        assert isinstance(request.body, bytes)
        payload = cast('dict[str, object]', json.loads(request.body))
        names.append(cast('str', payload['method_name']))
    return tuple(names)


def _assert_transcript(scenario: _Scenario, transcript: list[_RequestTranscript]) -> None:
    assert _method_names(transcript) == scenario.expected_methods
    for request in transcript:
        headers = dict(request.headers)
        if scenario.localized:
            assert headers['locale'] == 'auto'
        else:
            assert 'locale' not in headers
        if scenario.legacy_infinite_timeout:
            assert request.timeout == TimeoutConfig(connect=None, read=None)
    if scenario.expected_operation_auth is not None:
        operation = transcript[-1]
        assert isinstance(operation.body, bytes)
        payload = cast('dict[str, object]', json.loads(operation.body))
        params = cast('list[object]', payload['params'])
        assert params[0] == scenario.expected_operation_auth


async def _observe(sg: Shotgun | AsyncShotgun, scenario: _Scenario) -> _Failure | _Success:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always', DeprecationWarning)
        if isinstance(scenario.expected, _Success):
            observed = _Success(await _execute_call(sg, scenario))
        else:
            with pytest.raises(scenario.expected.exception) as raised:
                await _execute_call(sg, scenario)
            assert type(raised.value) is scenario.expected.exception
            observed = _Failure(type(raised.value))
    assert [str(warning.message) for warning in caught] == []
    return observed


def _prepared_upload(path: Path) -> _files.PreparedUpload:
    return _files.PreparedUpload(
        filename=path.name,
        file_size=path.stat().st_size,
        options=_files.UploadOptions(
            arguments=_files.UploadArguments(
                entity_type='Version',
                entity_id=5,
                path=str(path),
                field_name='sg_uploaded_movie',
            ),
            upload_type='Attachment',
            filmstrip=False,
        ),
        path=str(path),
    )


def _response(trace: _Trace, label: str, result: object) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers=CaseInsensitiveHeaders({'content-type': 'application/json'}),
        content=json.dumps({'results': result}).encode(),
        on_close=lambda: trace.closed_responses.append(label),
    )


def _retry_response(trace: _Trace, label: str) -> TransportResponse:
    return TransportResponse(
        status=502,
        reason='Bad Gateway',
        headers=CaseInsensitiveHeaders({}),
        content=b'',
        on_close=lambda: trace.closed_responses.append(label),
    )


def _scenarios() -> tuple[_Scenario, ...]:
    return (
        _Scenario(
            call=lambda sg: sg.batch(_batch_requests()),
            expected=_Success([_ENTITY, {'type': 'Shot', 'id': 1, 'code': 'Y'}, True]),
            expected_methods=('info', 'batch'),
            name='batch',
            script=lambda trace: [
                _response(
                    trace,
                    'result',
                    [_ENTITY, {'type': 'Shot', 'id': 1, 'code': 'Y'}, True],
                )
            ],
        ),
        _Scenario(
            call=lambda sg: sg.create('Shot', {'code': 'X'}, ['code']),
            expected=_Success(_ENTITY),
            expected_methods=('info', 'create'),
            name='create',
            script=lambda trace: [_response(trace, 'result', _ENTITY)],
        ),
        _Scenario(
            call=lambda sg: sg.delete('Shot', 1),
            expected=_Success(result=True),
            expected_methods=('info', 'delete'),
            name='delete',
            script=lambda trace: [_response(trace, 'result', result=True)],
        ),
        _Scenario(
            call=lambda sg: sg.find(
                'Shot',
                [['id', 'is', 1]],
                ['code'],
                [{'field_name': 'code', 'direction': 'desc'}],
            ),
            expected=_Success([_ENTITY]),
            expected_methods=('info', 'read'),
            name='find',
            script=lambda trace: [_response(trace, 'result', _PAGE)],
        ),
        _Scenario(
            call=lambda sg: sg.find_one('Shot', [['id', 'is', 1]], ['code']),
            expected=_Success(_ENTITY),
            expected_methods=('info', 'read'),
            name='find_one',
            script=lambda trace: [_response(trace, 'result', _PAGE)],
        ),
        _Scenario(
            call=lambda sg: sg.revive('Shot', 1),
            expected=_Success(result=True),
            expected_methods=('info', 'revive'),
            name='revive',
            script=lambda trace: [_response(trace, 'result', result=True)],
        ),
        _Scenario(
            call=lambda sg: sg.update('Shot', 1, {'code': 'Y'}),
            expected=_Success({'type': 'Shot', 'id': 1, 'code': 'Y'}),
            expected_methods=('info', 'update'),
            name='update',
            script=lambda trace: [
                _response(trace, 'result', {'type': 'Shot', 'id': 1, 'code': 'Y'})
            ],
        ),
        _Scenario(
            call=lambda sg: sg.batch([]),
            expected=_Success([]),
            expected_methods=(),
            name='batch_empty',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.batch(_batch_requests()),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'batch'),
            name='batch_invalid_result',
            script=lambda trace: [_response(trace, 'result', {'not': 'a list'})],
        ),
        _Scenario(
            call=lambda sg: sg.create('Shot', {'code': 'X'}),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'create'),
            name='create_invalid_result',
            script=lambda trace: [_response(trace, 'result', ['not a dictionary'])],
        ),
        _Scenario(
            call=lambda sg: sg.delete('Shot', 1),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'delete'),
            name='delete_invalid_result',
            script=lambda trace: [_response(trace, 'result', 1)],
        ),
        _Scenario(
            call=lambda sg: sg.revive('Shot', 1),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'revive'),
            name='revive_invalid_result',
            script=lambda trace: [_response(trace, 'result', 'true')],
        ),
        _Scenario(
            call=lambda sg: sg.update('Shot', 1, {'code': 'Y'}),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'update'),
            name='update_invalid_result',
            script=lambda trace: [_response(trace, 'result', result=False)],
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'read'),
            name='find_invalid_dictionary_result',
            script=lambda trace: [_response(trace, 'result', ['not a dictionary'])],
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'read'),
            name='find_invalid_entities_result',
            script=lambda trace: [
                _response(
                    trace,
                    'result',
                    {'entities': {'id': 1}, 'paging_info': {'entity_count': 1}},
                )
            ],
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'read'),
            name='find_invalid_entity_member',
            script=lambda trace: [
                _response(
                    trace,
                    'result',
                    {'entities': [1], 'paging_info': {'entity_count': 1}},
                )
            ],
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'read'),
            name='find_invalid_paging_result',
            script=lambda trace: [
                _response(trace, 'result', {'entities': [_ENTITY], 'paging_info': []})
            ],
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Failure(_errors.AuthenticationFault),
            expected_methods=('info', 'read'),
            name='find_authentication_fault',
            script=lambda trace: [_fault(trace, 'fault', 102)],
            expected_closed_responses=('fault',),
            expected_response_outcomes=('success', 'error'),
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Success([_ENTITY]),
            expected_methods=('info', 'read', 'read'),
            name='find_retries_502',
            script=lambda trace: [
                _retry_response(trace, 'retry'),
                _response(trace, 'result', _PAGE),
            ],
            expected_closed_responses=('retry', 'result'),
            expected_response_outcomes=('success', 'retry', 'success'),
            expected_sleeps=(0.0,),
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Success([_ENTITY]),
            expected_methods=('info', 'read'),
            name='find_localized_true',
            script=lambda trace: [_response(trace, 'result', _PAGE)],
            localized=True,
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', []),
            expected=_Success([_ENTITY]),
            expected_methods=('info', 'read'),
            name='find_legacy_infinite_timeout',
            script=lambda trace: [_response(trace, 'result', _PAGE)],
            expected_warning_codes=(
                'legacy-infinite-timeout',
                'legacy-infinite-timeout',
            ),
            legacy_infinite_timeout=True,
            secret_auth=True,
        ),
        _Scenario(
            call=lambda sg: sg.find('Shot', [], limit=2),
            expected=_Success(
                [
                    {'type': 'Shot', 'id': 1},
                    {'type': 'Shot', 'id': 2},
                ]
            ),
            expected_methods=('info', 'read', 'read'),
            name='find_serial_paging',
            script=lambda trace: [
                _response(
                    trace,
                    'page-1',
                    {
                        'entities': [{'type': 'Shot', 'id': 1}],
                        'paging_info': {'entity_count': 2},
                    },
                ),
                _response(
                    trace,
                    'page-2',
                    {
                        'entities': [{'type': 'Shot', 'id': 2}],
                        'paging_info': {'entity_count': 2},
                    },
                ),
            ],
            expected_closed_responses=('page-1', 'page-2'),
            expected_response_outcomes=('success', 'success', 'success'),
        ),
        _Scenario(
            call=lambda sg: sg.summarize(
                'Shot',
                [],
                [{'field': 'id', 'type': 'count'}],
            ),
            expected=_Success({'summaries': {'id': {'count': 3}}}),
            expected_methods=('info', 'summarize'),
            name='summarize',
            script=lambda trace: [_response(trace, 'result', {'summaries': {'id': {'count': 3}}})],
        ),
        _Scenario(
            call=lambda sg: sg.schema_read(),
            expected=_Success({'Shot': {}}),
            expected_methods=('info', 'schema_read'),
            name='schema_read',
            script=lambda trace: [_response(trace, 'result', {'Shot': {}})],
        ),
        _Scenario(
            call=lambda sg: sg.text_search('boat', {'Shot': []}),
            expected=_Success({'matches': []}),
            expected_methods=('info', 'query_display_name_cache'),
            name='text_search',
            script=lambda trace: [_response(trace, 'result', {'matches': []})],
        ),
        _Scenario(
            call=lambda sg: sg.follow(
                {'type': 'HumanUser', 'id': 1},
                {'type': 'Shot', 'id': 2},
            ),
            expected=_Success({'followed': True}),
            expected_methods=('info', 'follow'),
            name='follow',
            script=lambda trace: [_response(trace, 'result', {'followed': True})],
        ),
        _Scenario(
            call=lambda sg: sg.work_schedule_update(
                '2026-07-04',
                working=False,
                project={'type': 'Project', 'id': 1},
                user={'type': 'HumanUser', 'id': 2},
            ),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='work_schedule_exclusivity',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.authenticate_human_user('me', 'pw', 'two-factor'),
            expected=_Success({'type': 'HumanUser', 'id': 7, 'login': 'me'}),
            expected_methods=('info', 'read'),
            name='auth_human_user',
            script=lambda trace: [
                _response(
                    trace,
                    'result',
                    {
                        'entities': [{'type': 'HumanUser', 'id': 7, 'login': 'me'}],
                        'paging_info': {'entity_count': 1},
                    },
                )
            ],
            expected_operation_auth={
                'user_login': 'me',
                'user_password': 'pw',
                'auth_token': 'two-factor',
                'session_uuid': 'captured-session',
            },
            mutate_session_uuid_after_readiness=True,
            secret_auth=True,
            session_uuid='captured-session',
        ),
        _Scenario(
            call=lambda sg: sg.entity_types(),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='entity_types_alias',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.schema('Shot'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='schema_alias',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.activity_stream_read(
                'Shot',
                2,
                {'Shot': ['code']},
                10,
                20,
                50,
            ),
            expected=_Success({'updates': []}),
            expected_methods=('info', 'activity_stream'),
            name='activity_stream_read',
            script=lambda trace: [_response(trace, 'result', {'updates': []})],
        ),
        _Scenario(
            call=lambda sg: sg.export_page(12, 'csv', layout_name='summary'),
            expected=_Success('csv,data'),
            expected_methods=('info', 'export_page'),
            name='export_page',
            script=lambda trace: [_response(trace, 'result', 'csv,data')],
        ),
        _Scenario(
            call=lambda sg: sg.followers({'type': 'Shot', 'id': 2}),
            expected=_Success([{'type': 'HumanUser', 'id': 1}]),
            expected_methods=('info', 'followers'),
            name='followers',
            script=lambda trace: [_response(trace, 'result', [{'type': 'HumanUser', 'id': 1}])],
        ),
        _Scenario(
            call=lambda sg: sg.following(
                {'type': 'HumanUser', 'id': 1},
                {'type': 'Project', 'id': 3},
                'Shot',
            ),
            expected=_Success([{'type': 'Shot', 'id': 2}]),
            expected_methods=('info', 'following'),
            name='following',
            script=lambda trace: [_response(trace, 'result', [{'type': 'Shot', 'id': 2}])],
        ),
        _Scenario(
            call=lambda sg: sg.nav_expand(
                '/Project/3',
                'entity',
                {'Shot': ['code']},
            ),
            expected=_Success({'label': 'Project'}),
            expected_methods=('info', 'nav_expand'),
            name='nav_expand',
            script=lambda trace: [_response(trace, 'result', {'label': 'Project'})],
        ),
        _Scenario(
            call=lambda sg: sg.nav_search_entity(
                '/',
                {'type': 'Shot', 'id': 2},
                'entity',
            ),
            expected=_Success([{'path': '/Project/3/Shot/2'}]),
            expected_methods=('info', 'nav_search'),
            name='nav_search_entity',
            script=lambda trace: [_response(trace, 'result', [{'path': '/Project/3/Shot/2'}])],
        ),
        _Scenario(
            call=lambda sg: sg.nav_search_string('/', 'boat', 'entity'),
            expected=_Success([{'path': '/Project/3/Shot/2'}]),
            expected_methods=('info', 'nav_search'),
            name='nav_search_string',
            script=lambda trace: [_response(trace, 'result', [{'path': '/Project/3/Shot/2'}])],
        ),
        _Scenario(
            call=lambda sg: sg.note_thread_read(41, {'Note': ['created_by']}),
            expected=_Success([{'type': 'Note', 'id': 41}]),
            expected_methods=('info', 'note_thread_contents'),
            name='note_thread_read',
            script=lambda trace: [_response(trace, 'result', [{'type': 'Note', 'id': 41}])],
        ),
        _Scenario(
            call=lambda sg: sg.nav_search_entity('/', {'type': 'Shot', 'id': 2}),
            expected=_Success([{'path': '/Project/3/Shot/2'}]),
            expected_methods=('info', 'nav_search', 'nav_search'),
            name='nav_search_entity_retries_as_read',
            script=lambda trace: [
                _retry_response(trace, 'retry'),
                _response(trace, 'result', [{'path': '/Project/3/Shot/2'}]),
            ],
            expected_closed_responses=('retry', 'result'),
            expected_response_outcomes=('success', 'retry', 'success'),
            expected_sleeps=(0.0,),
        ),
        _Scenario(
            call=lambda sg: sg.nav_search_string('/', 'boat'),
            expected=_Success([{'path': '/Project/3/Shot/2'}]),
            expected_methods=('info', 'nav_search', 'nav_search'),
            name='nav_search_string_retries_as_read',
            script=lambda trace: [
                _retry_response(trace, 'retry'),
                _response(trace, 'result', [{'path': '/Project/3/Shot/2'}]),
            ],
            expected_closed_responses=('retry', 'result'),
            expected_response_outcomes=('success', 'retry', 'success'),
            expected_sleeps=(0.0,),
        ),
        _Scenario(
            call=lambda sg: sg.note_thread_read(41),
            expected=_Success([{'type': 'Note', 'id': 41}]),
            expected_methods=('info', 'note_thread_contents', 'note_thread_contents'),
            name='note_thread_read_retries_as_read',
            script=lambda trace: [
                _retry_response(trace, 'retry'),
                _response(trace, 'result', [{'type': 'Note', 'id': 41}]),
            ],
            expected_closed_responses=('retry', 'result'),
            expected_response_outcomes=('success', 'retry', 'success'),
            expected_sleeps=(0.0,),
        ),
        _Scenario(
            call=lambda sg: sg.preferences_read(['timezone']),
            expected=_Success({'timezone': 'UTC'}),
            expected_methods=('info', 'preferences_read'),
            name='preferences_read',
            script=lambda trace: [_response(trace, 'result', {'timezone': 'UTC'})],
        ),
        _Scenario(
            call=lambda sg: sg.schema_entity_read({'type': 'Project', 'id': 3}),
            expected=_Success({'Shot': {}}),
            expected_methods=('info', 'schema_entity_read'),
            name='schema_entity_read',
            script=lambda trace: [_response(trace, 'result', {'Shot': {}})],
        ),
        _Scenario(
            call=lambda sg: sg.schema_field_create(
                'Shot',
                'text',
                'New Field',
                {'description': 'Description'},
            ),
            expected=_Success('sg_new_field'),
            expected_methods=('info', 'schema_field_create'),
            name='schema_field_create',
            script=lambda trace: [_response(trace, 'result', 'sg_new_field')],
        ),
        _Scenario(
            call=lambda sg: sg.schema_field_delete('Shot', 'sg_new_field'),
            expected=_Success(result=True),
            expected_methods=('info', 'schema_field_delete'),
            name='schema_field_delete',
            script=lambda trace: [_response(trace, 'result', result=True)],
        ),
        _Scenario(
            call=lambda sg: sg.schema_field_read(
                'Shot',
                'code',
                {'type': 'Project', 'id': 3},
            ),
            expected=_Success({'code': {}}),
            expected_methods=('info', 'schema_field_read'),
            name='schema_field_read',
            script=lambda trace: [_response(trace, 'result', {'code': {}})],
        ),
        _Scenario(
            call=lambda sg: sg.schema_field_update(
                'Shot',
                'sg_new_field',
                {'name': 'Renamed'},
                {'type': 'Project', 'id': 3},
            ),
            expected=_Success(result=True),
            expected_methods=('info', 'schema_field_update'),
            name='schema_field_update',
            script=lambda trace: [_response(trace, 'result', result=True)],
        ),
        _Scenario(
            call=lambda sg: sg.unfollow(
                {'type': 'HumanUser', 'id': 1},
                {'type': 'Shot', 'id': 2},
            ),
            expected=_Success({'followed': False}),
            expected_methods=('info', 'unfollow'),
            name='unfollow',
            script=lambda trace: [_response(trace, 'result', {'followed': False})],
        ),
        _Scenario(
            call=lambda sg: sg.update_project_last_accessed(
                {'type': 'Project', 'id': 3},
                {'type': 'HumanUser', 'id': 1},
            ),
            expected=_Success(None),
            expected_methods=('info', 'update_project_last_accessed_by_current_user'),
            name='update_project_last_accessed',
            script=lambda trace: [_response(trace, 'result', {'updated': True})],
        ),
        _Scenario(
            call=lambda sg: sg.user_subscriptions_create(
                [{'humanUserId': '1', 'subscription': ['artist'], 'label': 'extra'}]
            ),
            expected=_Success(result=True),
            expected_methods=('info', 'user_subscriptions_create'),
            name='user_subscriptions_create',
            script=lambda trace: [_response(trace, 'result', {'1': None})],
        ),
        _Scenario(
            call=lambda sg: sg.user_subscriptions_read(),
            expected=_Success(
                [
                    {'humanUserId': '1', 'subscription': None},
                    {'humanUserId': '2', 'subscription': 'artist'},
                    {'humanUserId': 'user-A', 'subscription': ['reviewer']},
                    {'humanUserId': '4', 'subscription': cast('list[str]', [])},
                ]
            ),
            expected_methods=('info', 'user_subscriptions_read'),
            name='user_subscriptions_read',
            script=lambda trace: [
                _response(
                    trace, 'result', {'1': None, '2': 'artist', 'user-A': ['reviewer'], '4': []}
                )
            ],
        ),
        _Scenario(
            call=lambda sg: sg.work_schedule_read(
                '2026-07-01',
                '2026-07-31',
                project={'type': 'Project', 'id': 3},
            ),
            expected=_Success({'working_days': []}),
            expected_methods=('info', 'work_schedule_read'),
            name='work_schedule_read',
            script=lambda trace: [_response(trace, 'result', {'working_days': []})],
        ),
        _Scenario(
            call=lambda sg: sg.work_schedule_read(
                '2026-07-01',
                '2026-07-31',
                project={'type': 'Project', 'id': 3},
                user={'type': 'HumanUser', 'id': 7},
            ),
            expected=_Success({'working_days': []}),
            expected_methods=('info', 'work_schedule_read'),
            name='work_schedule_read_project_and_user',
            script=lambda trace: [_response(trace, 'result', {'working_days': []})],
        ),
        _Scenario(
            call=lambda sg: sg.work_schedule_update(
                '2026-07-04',
                working=False,
                description='holiday',
                project={'type': 'Project', 'id': 3},
                recalculate_field='sg_date',
            ),
            expected=_Success({'updated': True}),
            expected_methods=('info', 'work_schedule_update'),
            name='work_schedule_update',
            script=lambda trace: [_response(trace, 'result', {'updated': True})],
        ),
        _Scenario(
            call=lambda sg: sg.export_page(_INVALID_PAGE_ID, 'csv'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='export_page_validation_failure',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.following({'type': 'HumanUser', 'id': 1}),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'following'),
            name='list_result_failure',
            script=lambda trace: [_response(trace, 'result', {'not': 'a list'})],
        ),
        _Scenario(
            call=lambda sg: sg.schema_field_delete('Shot', 'sg_field'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'schema_field_delete'),
            name='boolean_result_failure',
            script=lambda trace: [_response(trace, 'result', 1)],
        ),
        _Scenario(
            call=lambda sg: sg.schema_read(),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'schema_read'),
            name='dictionary_result_failure',
            script=lambda trace: [_response(trace, 'result', [])],
        ),
        _Scenario(
            call=lambda sg: sg.export_page(12, 'csv'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'export_page'),
            name='string_result_failure',
            script=lambda trace: [_response(trace, 'result', {})],
        ),
        _Scenario(
            call=lambda sg: sg.authenticate_human_user('me', 'bad'),
            expected=_Success(None),
            expected_methods=('info', 'read'),
            name='authentication_fault_returns_none',
            script=lambda trace: [_fault(trace, 'fault', 102)],
            expected_closed_responses=('fault',),
            expected_response_outcomes=('success', 'error'),
        ),
        _Scenario(
            call=lambda sg: sg.authenticate_human_user('me', 'pw'),
            expected=_Failure(_errors.MissingTwoFactorAuthenticationFault),
            expected_methods=('info', 'read'),
            name='missing_two_factor_propagates',
            script=lambda trace: [_fault(trace, 'fault', 106)],
            expected_closed_responses=('fault',),
            expected_response_outcomes=('success', 'error'),
        ),
        _Scenario(
            call=lambda sg: sg.authenticate_human_user('me', 'pw'),
            expected=_Failure(_errors.Fault),
            expected_methods=('info', 'read'),
            name='other_auth_fault_propagates',
            script=lambda trace: [_fault(trace, 'fault', 999)],
            expected_closed_responses=('fault',),
            expected_response_outcomes=('success', 'error'),
        ),
        _Scenario(
            call=lambda sg: sg.authenticate_human_user('me', 'pw'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=('info', 'read'),
            name='auth_result_failure',
            script=lambda trace: [_response(trace, 'result', [])],
        ),
        _Scenario(
            call=lambda sg: sg.work_schedule_read('2026-02-30', '2026-07-31'),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='schedule_date_failure',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
        _Scenario(
            call=lambda sg: sg.user_subscriptions_create([{'subscription': 'missing-user-id'}]),
            expected=_Failure(_errors.ShotgunError),
            expected_methods=(),
            name='subscription_validation_failure',
            script=lambda _trace: [],
            expected_closed_responses=(),
            expected_response_outcomes=(),
        ),
    )


def _transcript(requests: list[TransportRequest]) -> list[_RequestTranscript]:
    transcript: list[_RequestTranscript] = []
    for request in requests:
        assert request.body is None or isinstance(request.body, bytes)
        transcript.append(
            _RequestTranscript(
                body=request.body,
                headers=tuple(
                    sorted((name.lower(), value) for name, value in request.headers.items())
                ),
                method=request.method,
                timeout=request.timeout,
                url=request.url,
            )
        )
    return transcript


def test_client_method_runtime_and_annotation_signatures_match() -> None:
    for method_name in _CLIENT_METHOD_PARITY_METHODS:
        sync_descriptor = inspect.getattr_static(Shotgun, method_name)
        async_descriptor = inspect.getattr_static(AsyncShotgun, method_name)
        assert type(async_descriptor) is type(sync_descriptor)
        sync_method = cast('Callable[..., object]', getattr(Shotgun, method_name))
        async_method = cast('Callable[..., object]', getattr(AsyncShotgun, method_name))
        assert inspect.signature(async_method) == inspect.signature(sync_method)
        assert get_type_hints(async_method) == get_type_hints(sync_method)
        assert inspect.iscoroutinefunction(async_method)
        assert not inspect.iscoroutinefunction(sync_method)


def test_client_method_static_surface_accepts_complete_contract(tmp_path: Path) -> None:
    probe = tmp_path / 'async_client_contract.py'
    probe.write_text(
        """from typing import assert_type

from shotgun_api3 import AsyncShotgun

async def probe(sg: AsyncShotgun) -> None:
    assert_type(await sg.export_page(1, 'csv'), str)
    assert_type(await sg.schema_field_delete('Shot', 'sg_field'), bool)
    assert_type(await sg.schema_field_update('Shot', 'sg_field', {}), bool)
    assert_type(await sg.update_project_last_accessed({'type': 'Project', 'id': 3}), None)
    assert_type(await sg.user_subscriptions_create([]), bool)
""",
        encoding='utf-8',
    )
    repository = Path(__file__).parents[1]

    result = run_pyright(probe, cwd=repository)

    assert result.returncode == 0, result.stdout


def test_client_method_static_surface_rejects_invalid_calls(tmp_path: Path) -> None:
    probe = tmp_path / 'async_client_probe.py'
    probe.write_text(
        """from shotgun_api3 import AsyncShotgun

async def probe(sg: AsyncShotgun) -> None:
    await sg.activity_stream_read('Shot')
    await sg.export_page(1)
    await sg.summarize('Shot')
    await sg.work_schedule_update('2026-07-04')
""",
        encoding='utf-8',
    )
    repository = Path(__file__).parents[1]

    result = run_pyright(probe, cwd=repository)

    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    assert report['summary']['errorCount'] == 4


def test_crud_runtime_and_annotation_signatures_match() -> None:
    for method_name in _CRUD_PARITY_METHODS:
        sync_method = cast('Callable[..., object]', getattr(Shotgun, method_name))
        async_method = cast('Callable[..., object]', getattr(AsyncShotgun, method_name))
        assert inspect.signature(async_method) == inspect.signature(sync_method)
        assert get_type_hints(async_method) == get_type_hints(sync_method)
        assert inspect.iscoroutinefunction(async_method)
        assert not inspect.iscoroutinefunction(sync_method)


def test_crud_static_surface_rejects_invalid_calls(tmp_path: Path) -> None:
    probe = tmp_path / 'async_crud_probe.py'
    probe.write_text(
        """from shotgun_api3 import AsyncShotgun

async def probe(sg: AsyncShotgun) -> None:
    await sg.create('Shot', [])
    await sg.delete('Shot')
    await sg.find('Shot')
    await sg.update('Shot', 1, [])
""",
        encoding='utf-8',
    )
    repository = Path(__file__).parents[1]

    result = run_pyright(probe, cwd=repository)

    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    assert report['summary']['errorCount'] == 4
    assert {diagnostic['rule'] for diagnostic in report['generalDiagnostics']} == {
        'reportArgumentType',
        'reportCallIssue',
    }


def test_supported_query_attachment_and_schedule_types(tmp_path: Path) -> None:
    repository = Path(__file__).parents[1]
    supported = tmp_path / 'supported_client_inputs.py'
    supported.write_text(
        """from collections import UserDict
from datetime import date

from shotgun_api3 import AsyncShotgun, Shotgun
from shotgun_api3.lib import mockgun

def sync_calls(sg: Shotgun, mock: mockgun.Shotgun) -> None:
    filters = [['code', 'is', 'A'], {'filter_operator': 'any', 'filters': [['id', 'is', 1]]}]
    sg.find('Shot', filters)
    sg.find_one('Shot', (['id', 'is', 1],))
    sg.find('Shot', {'logical_operator': 'and', 'conditions': []})
    mock.find('Shot', filters)
    mock.find_one('Shot', (['id', 'is', 1],))
    mock.find('Shot', {'logical_operator': 'and', 'conditions': []})
    sg.download_attachment(UserDict({'type': 'Attachment', 'id': 1}))
    sg.download_attachment(None)
    sg.download_attachment(attachment_id=1)
    sg.get_attachment_download_url(UserDict({'url': 'https://x.example/a'}))
    sg.work_schedule_read(date(2026, 7, 1), '2026-07-31')
    sg.work_schedule_update(date(2026, 7, 4), False)

async def async_calls(sg: AsyncShotgun, mock: mockgun.AsyncShotgun) -> None:
    filters = [['code', 'is', 'A'], {'filter_operator': 'any', 'filters': [['id', 'is', 1]]}]
    await sg.find('Shot', filters)
    await sg.find_one('Shot', (['id', 'is', 1],))
    await sg.find('Shot', {'logical_operator': 'and', 'conditions': []})
    await mock.find('Shot', filters)
    await mock.find_one('Shot', (['id', 'is', 1],))
    await mock.find('Shot', {'logical_operator': 'and', 'conditions': []})
    await sg.download_attachment(UserDict({'type': 'Attachment', 'id': 1}))
    await sg.download_attachment(None)
    await sg.download_attachment(attachment_id=1)
    await sg.get_attachment_download_url(UserDict({'url': 'https://x.example/a'}))
    await sg.work_schedule_read('2026-07-01', date(2026, 7, 31))
    await sg.work_schedule_update('2026-07-04', False)
""",
        encoding='utf-8',
    )
    positive = run_pyright(supported, cwd=repository)
    assert positive.returncode == 0, positive.stdout

    unsupported = tmp_path / 'unsupported_client_inputs.py'
    unsupported.write_text(
        """from shotgun_api3 import AsyncShotgun, Shotgun
from shotgun_api3.lib import mockgun

def sync_calls(sg: Shotgun, mock: mockgun.Shotgun) -> None:
    sg.find('Shot', 42)
    sg.find_one('Shot', None)
    mock.find('Shot', 42)
    mock.find_one('Shot', None)
    sg.download_attachment('bad')
    sg.download_attachment()
    sg.get_attachment_download_url('bad')
    sg.work_schedule_read(42, '2026-07-31')
    sg.work_schedule_update(42, False)

async def async_calls(sg: AsyncShotgun, mock: mockgun.AsyncShotgun) -> None:
    await sg.find('Shot', 42)
    await sg.find_one('Shot', None)
    await mock.find('Shot', 42)
    await mock.find_one('Shot', None)
    await sg.download_attachment('bad')
    await sg.download_attachment()
    await sg.get_attachment_download_url('bad')
    await sg.work_schedule_read(42, '2026-07-31')
    await sg.work_schedule_update(42, False)
""",
        encoding='utf-8',
    )
    negative = run_pyright(unsupported, cwd=repository)
    assert negative.returncode == 1, negative.stdout
    report = json.loads(negative.stdout)
    lines = unsupported.read_text(encoding='utf-8').splitlines()
    diagnostics = cast('list[dict[str, Any]]', report['generalDiagnostics'])
    rejected = {
        lines[cast('int', diagnostic['range']['start']['line'])].strip()
        for diagnostic in diagnostics
    }
    assert rejected == {
        line.strip()
        for line in lines
        if line.strip().startswith(('sg.', 'mock.', 'await sg.', 'await mock.'))
    }


@pytest.mark.anyio
@pytest.mark.parametrize('scenario', _scenarios(), ids=lambda scenario: scenario.name)
async def test_sync_async_scenario_parity(
    scenario: _Scenario,
    fake_async_client: FakeAsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_trace = _Trace()
    async_trace = _Trace()
    sync_sg = fake_client(scenario.script(sync_trace))
    async_sg = fake_async_client(scenario.script(async_trace))
    _configure(sync_sg, scenario, sync_trace)
    _configure(async_sg, scenario, async_trace)
    _install_sync_timing(monkeypatch, sync_trace)
    _install_async_timing(monkeypatch, async_trace)

    sync_result = await _observe(sync_sg, scenario)
    async_result = await _observe(async_sg, scenario)

    sync_transcript = _transcript(fake_transport(sync_sg).requests)
    async_transcript = _transcript(fake_async_transport(async_sg).requests)
    assert sync_result == async_result == scenario.expected
    _assert_transcript(scenario, sync_transcript)
    _assert_transcript(scenario, async_transcript)
    _assert_trace(scenario, sync_trace)
    _assert_trace(scenario, async_trace)
    assert async_transcript == sync_transcript
    assert async_trace == sync_trace


@pytest.mark.anyio
async def test_sync_async_upload_allocation_requires_identical_origin(tmp_path: Path) -> None:
    source = tmp_path / 'clip.mov'
    source.write_bytes(b'clip-data')
    prepared = _prepared_upload(source)
    sync_sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    async_sg = AsyncShotgun('https://x.example', script_name='s', api_key='k', connect=False)
    sync_sg.config.server = None
    async_sg.config.server = None
    message = r'^server parameters must be configured before upload I/O$'
    sync_allocate = cast(
        'Callable[..., object]',
        vars(Shotgun)['_upload_allocation'],
    )
    async_allocate = cast(
        'Callable[..., Coroutine[object, object, object]]',
        vars(AsyncShotgun)['_upload_allocation'],
    )

    with pytest.raises(ValueError, match=message) as sync_error:
        sync_allocate(
            None,
            config_module.snapshot(sync_sg.config),
            prepared,
            multipart=False,
        )
    with pytest.raises(ValueError, match=message) as async_error:
        await async_allocate(
            None,
            config_module.snapshot(async_sg.config),
            prepared,
            multipart=False,
        )

    assert type(async_error.value) is type(sync_error.value) is ValueError
    assert str(async_error.value) == str(sync_error.value)
