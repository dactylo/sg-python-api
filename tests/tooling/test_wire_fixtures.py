import base64
import copy
import json
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import cast
from urllib.parse import urlsplit

import pytest

from scripts import probe_wire
from shotgun_api3 import _config as config_module
from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._transport import (
    TimeoutConfig,
    TransportError,
    TransportRequest,
    TransportResponse,
)
from tests._pyright import run_pyright
from tests._transports import _FakeTransport
from tests.tooling._wire_replay import _REPLAY_RECIPES

_FIXTURES = Path('tests/fixtures')
_READINESS_BODY = b'{"results":{"version":[9,0,0],"api_max_entities_per_page":500}}'
_REPLAY_EXPECTED_RESULTS: dict[str, object] = {
    'activity_stream_read': {
        'entity_type': 'Project',
        'entity_id': 1,
        'latest_update_id': 1,
        'earliest_update_id': 1,
        'updates': [],
    },
    'batch': [
        {
            'id': 1,
            'subject': 'sg-python-api-wire-probe-replay-batch',
            'project': {'id': 1, 'name': 'Fixture Project', 'type': 'Project'},
            'type': 'Note',
        }
    ],
    'create': {
        'id': 1,
        'subject': 'sg-python-api-wire-probe-replay',
        'project': {'id': 1, 'name': 'Fixture Project', 'type': 'Project'},
        'type': 'Note',
    },
    'following': [{'type': 'HumanUser', 'id': 1}],
    'get_session_token': 'XXXXXXXXXXXXXXXXXXXXX',
    'info': {
        'version': [9, 0, 0],
        'full_version': [9, 0, 0, 0],
        's3_uploads_enabled': False,
        's3_direct_uploads_enabled': False,
        's3_enabled_upload_types': {},
        'shotgunlocalhost_browser_integration_enabled': False,
        'unified_login_flow_enabled': False,
        'authentication_app_session_launcher_enabled': False,
        'analytics_site_id': 'fixture-site',
        'analytics_site_name': 'Fixture Site',
        'user_authentication_method': 'default',
        'api_max_entities_per_page': 500,
    },
    'nav_expand': {
        'label': 'Fixture Root',
        'ref': {'kind': 'root', 'value': None},
        'path': '/',
        'target_entities': {'type': 'Project', 'additional_filter_presets': []},
        'has_children': False,
        'children': [],
    },
    'preferences_read': {
        'format_date_fields': '%Y-%m-%d',
        'date_component_order': 'YMD',
        'format_time_hour_fields': '24-hour',
        'format_currency_fields_display_dollar_sign': True,
        'format_currency_fields_decimal_options': '2',
        'format_currency_fields_negative_options': 'minus',
        'format_number_fields': '0',
        'format_float_fields': '0.00',
        'format_float_fields_rounding': 'round',
        'format_footage_fields': 'frames',
        'support_local_storage': False,
        'view_master_settings': 'default',
        'duration_units': 'hours',
        'hours_per_day': 8.0,
        'enable_rv_integration': False,
        'enable_shotgun_review_for_rv': False,
        'creative_review_settings': '{}',
    },
    'read': [
        {'type': 'Project', 'id': 1, 'name': 'Fixture Project 1'},
        {'type': 'Project', 'id': 2, 'name': 'Fixture Project 2'},
    ],
    'schema_entity_read': {
        'Project': {
            'name': {'value': 'Project', 'editable': False},
            'visible': {'value': True, 'editable': False},
        }
    },
    'schema_read': {
        'Project': {
            'id': {
                'name': {'value': 'Id', 'editable': False},
                'description': {'value': 'Fixture identifier', 'editable': False},
                'custom_metadata': {'value': None, 'editable': False},
                'entity_type': {'value': 'Project', 'editable': False},
                'data_type': {'value': 'number', 'editable': False},
                'editable': {'value': False, 'editable': False},
                'mandatory': {'value': True, 'editable': False},
                'unique': {'value': True, 'editable': False},
                'properties': {'default_value': {'value': None, 'editable': False}},
                'visible': {'value': True, 'editable': False},
                'ui_value_displayable': {'value': True, 'editable': False},
            }
        }
    },
    'summarize': {'summaries': {'id': 1}, 'groups': []},
    'text_search': {'matches': [], 'terms': ['wire', 'probe']},
    'update_project_last_accessed': None,
    'work_schedule_read': {
        '2026-01-05': {
            'working': True,
            'description': None,
            'reason': 'Fixture schedule',
        },
        '2026-01-06': {
            'working': True,
            'description': None,
            'reason': 'Fixture schedule',
        },
        '2026-01-07': {
            'working': True,
            'description': None,
            'reason': 'Fixture schedule',
        },
        '2026-01-08': {
            'working': True,
            'description': None,
            'reason': 'Fixture schedule',
        },
        '2026-01-09': {
            'working': True,
            'description': None,
            'reason': 'Fixture schedule',
        },
    },
}


class _CleanupClient:
    def __init__(
        self,
        *,
        cleanup_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.cleanup_error = cleanup_error
        self.close_error = close_error

    def close(self) -> None:
        self.calls.append(('close',))
        if self.close_error is not None:
            raise self.close_error

    def delete(self, entity_type: str, entity_id: int) -> bool:
        self.calls.append(('delete', entity_type, entity_id))
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return True


class _ClosingTransport(_FakeTransport):
    closed = False

    def close(self) -> None:
        self.closed = True


def _add_request_key(record: dict[str, object]) -> None:
    cast('dict[str, object]', record['request'])['extra'] = None


def _change_method(record: dict[str, object]) -> None:
    cast('dict[str, object]', record['request'])['method'] = 'GET'


def _change_operation(record: dict[str, object]) -> None:
    record['operation'] = ''


def _change_outcome(record: dict[str, object]) -> None:
    record['outcome'] = {'kind': 'skipped'}


def _change_response_bytes(record: dict[str, object]) -> None:
    cast('dict[str, object]', record['response'])['content_b64'] = 'not base64!'


def _change_schema_version(record: dict[str, object]) -> None:
    record['schema_version'] = 2


def _fixture(label: str = 'info') -> dict[str, object]:
    is_fault = label == 'fault_bad_auth'
    rpc_method = probe_wire._CAPTURE_SPECS[label][0]
    response = (
        {'exception': True, 'error_code': 102, 'message': 'REDACTED'}
        if is_fault
        else {'results': {'version': [9, 0, 0]}}
    )
    body: dict[str, object] = (
        {
            'method_name': 'read',
            'params': {
                'type': 'Project',
                'return_fields': ['id'],
                'filters': [],
                'return_only': 'active',
                'paging': {'entities_per_page': 1, 'current_page': 1},
                'api_return_image_urls': True,
            },
            'session_token': 'REDACTED',
        }
        if is_fault
        else {'method_name': rpc_method, 'params': []}
    )
    return {
        'schema_version': probe_wire._SCHEMA_VERSION,
        'operation': label,
        'auth_mode': 'session' if is_fault else 'script',
        'outcome': (
            {'kind': 'fault', 'exception': 'AuthenticationFault'}
            if is_fault
            else {'kind': 'success'}
        ),
        'request': {
            'method': 'POST',
            'path': '/api3/json',
            'headers': {'content-type': 'application/json'},
            'body': body,
        },
        'response': {
            'status': 200,
            'reason': 'OK',
            'headers': {'content-type': 'application/json'},
            'content_b64': base64.b64encode(json.dumps(response).encode()).decode('ascii'),
        },
    }


def _fixture_session_token() -> str:
    return f'invalid-{uuid.UUID(int=0).hex}'


def _inventory() -> list[dict[str, object]]:
    return [
        cast(
            'dict[str, object]',
            json.loads((_FIXTURES / f'{label}.json').read_text(encoding='utf-8')),
        )
        for label in probe_wire._REQUIRED_OPERATION_LABELS
    ]


def _mutate_activity_content(record: dict[str, object], canary: str) -> None:
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    content['updates'] = [{'content': canary}]
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _mutate_complete_create_record(record: dict[str, object], canary: str) -> None:
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    params = cast('list[object]', body['params'])
    create_params = cast('dict[str, object]', params[-1])
    fields = cast('list[object]', create_params['fields'])
    subject = cast('dict[str, object]', fields[0])
    project = cast('dict[str, object]', fields[1])
    subject['value'] = canary
    project_value = cast('dict[str, object]', project['value'])
    project_value['id'] = 987654321
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    results = cast('dict[str, object]', content['results'])
    results['id'] = 987654321
    results['subject'] = canary
    relationship = cast('dict[str, object]', results['project'])
    relationship['id'] = 987654321
    relationship['name'] = canary
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _mutate_headers_and_reason(record: dict[str, object], canary: str) -> None:
    request = cast('dict[str, object]', record['request'])
    cast('dict[str, object]', request['headers'])['content-type'] = canary
    response = cast('dict[str, object]', record['response'])
    response['reason'] = canary
    cast('dict[str, object]', response['headers'])['content-type'] = canary


def _mutate_preferences(record: dict[str, object], canary: str) -> None:
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    first = next(iter(content))
    content[first] = canary
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _mutate_request_filter(record: dict[str, object], canary: str) -> None:
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    params = cast('list[object]', body['params'])
    read_params = cast('dict[str, object]', params[-1])
    filters = cast('dict[str, object]', read_params['filters'])
    cast('list[object]', filters['conditions']).append(['name', 'is', canary])


def _mutate_request_shape(record: dict[str, object], canary: str) -> None:
    request = cast('dict[str, object]', record['request'])
    cast('dict[str, object]', request['body'])['unknown'] = canary


def _mutate_response_shape(record: dict[str, object], canary: str) -> None:
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    cast('dict[str, object]', content['results'])['unknown'] = canary
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _mutate_schema_labels(record: dict[str, object], canary: str) -> None:
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    results = cast('dict[str, object]', content['results'])
    first_key = next(iter(results))
    first = cast('dict[str, object]', results.pop(first_key))
    cast('dict[str, object]', first['name'])['value'] = canary
    results[canary] = first
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _mutate_work_schedule(record: dict[str, object], canary: str) -> None:
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    results = cast('dict[str, object]', content['results'])
    first_key = next(iter(results))
    first = cast('dict[str, object]', results.pop(first_key))
    first['reason'] = canary
    results[canary] = first
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()


def _remove_schema_version(record: dict[str, object]) -> None:
    record.pop('schema_version')


def _replay_expected_result(label: str) -> object:
    return _REPLAY_EXPECTED_RESULTS[label]


def _request(body: object) -> TransportRequest:
    return TransportRequest(
        method='POST',
        url='https://secret.example/api3/json',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer configured-secret'},
        body=json.dumps(body).encode(),
        timeout=TimeoutConfig(connect=1, read=1),
        token=uuid.UUID(int=0).hex,
    )


def _response(content: bytes, headers: Mapping[str, str] | None = None) -> TransportResponse:
    return TransportResponse(
        status=200,
        reason='OK',
        headers=headers or {'content-type': 'application/json'},
        content=content,
    )


def _live_mutation_state(
    label: str,
    result: object,
    *,
    cleanup_result: bool = True,
) -> tuple[probe_wire._LiveState, _ClosingTransport, _ClosingTransport]:
    labels = [
        'info',
        'read',
        'create',
        'summarize',
        'text_search',
        'schema_read',
        'schema_entity_read',
        'preferences_read',
        'activity_stream_read',
        'following',
        'nav_expand',
        'work_schedule_read',
        'update_project_last_accessed',
        'batch',
    ]
    responses: list[TransportResponse | TransportError] = [
        _response(_READINESS_BODY),
        _response(b'{"results":{"entities":[{"type":"Project","id":1}]}}'),
        _response(b'{"results":{"entities":[{"type":"HumanUser","id":2}]}}'),
    ]
    for operation in labels[: labels.index(label) + 1]:
        fixture = cast(
            'dict[str, object]', json.loads((_FIXTURES / f'{operation}.json').read_text())
        )
        response = cast('dict[str, object]', fixture['response'])
        content = base64.b64decode(cast('str', response['content_b64']))
        if operation == label:
            content = json.dumps({'results': result}).encode()
        elif operation == 'create':
            envelope = cast('dict[str, dict[str, object]]', json.loads(content))
            envelope['results']['id'] = 41
            content = json.dumps(envelope).encode()
        responses.append(_response(content))
    responses.extend(
        [
            _response(json.dumps({'results': cleanup_result}).encode()),
            _response(b'{"results":true}'),
        ]
    )
    inner = _ClosingTransport(responses)
    bad_inner = _ClosingTransport([])
    recorder = probe_wire._RecordingTransport(inner, {})
    bad_recorder = probe_wire._RecordingTransport(bad_inner, {})
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    bad_client = Shotgun(
        'https://fixture.invalid', session_token=_fixture_session_token(), connect=False
    )
    client._transport = recorder
    bad_client._transport = bad_recorder
    return probe_wire._LiveState(bad_client, bad_recorder, client, recorder), inner, bad_inner


def test_activity_capture_and_replay_use_current_flat_wire_contract() -> None:
    assert probe_wire._CAPTURE_SPECS['activity_stream_read'] == (
        'activity_stream',
        'script',
        'success',
    )
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    transport = _FakeTransport(
        [
            _response(_READINESS_BODY),
            _response(b'{"results":{"updates":[]}}'),
        ]
    )
    client._transport = transport

    try:
        assert _REPLAY_RECIPES['activity_stream_read'](client, None) == {'updates': []}
    finally:
        client.close()

    request = transport.requests[-1]
    assert isinstance(request.body, bytes)
    payload = json.loads(request.body)
    assert payload['method_name'] == 'activity_stream'
    params = payload['params']
    assert isinstance(params, list)
    assert params[-1] == {
        'type': 'Project',
        'id': 1,
        'max_id': None,
        'min_id': None,
        'limit': None,
        'entity_fields': {},
    }


def test_bad_auth_capture_uses_isolated_session_client_and_exact_fault() -> None:
    normal = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    before = config_module.auth_spec(normal.config)
    bad_inner = _FakeTransport(
        [
            _response(_READINESS_BODY),
            _response(b'{"exception":true,"error_code":102,"message":"rejected"}'),
        ]
    )
    recorder = probe_wire._RecordingTransport(
        bad_inner,
        {'invalid_session': 'fixture-invalid-session'},
    )
    bad = Shotgun(
        'https://fixture.invalid',
        session_token=_fixture_session_token(),
        connect=False,
    )
    bad._transport = recorder

    try:
        probe_wire._capture_bad_auth(bad, recorder)
    finally:
        bad.close()
        normal.close()

    assert config_module.auth_spec(normal.config) == before
    assert recorder.records['fault_bad_auth']['auth_mode'] == 'session'
    assert len(bad_inner.requests) == 2


def test_cleanup_and_close_failures_preserve_the_primary_failure() -> None:
    primary = RuntimeError('primary')
    cleanup = RuntimeError('cleanup')
    close = RuntimeError('close')
    normal = _CleanupClient(cleanup_error=cleanup)
    bad = _CleanupClient(close_error=close)
    resources = probe_wire._ResourceRegistry()
    resources._track('Note', 1)

    def fail() -> None:
        raise primary

    with pytest.raises(BaseExceptionGroup) as caught:
        probe_wire._run_guarded(fail, resources, normal, [normal, bad])

    assert caught.value.exceptions == (primary, cleanup, close)
    assert normal.calls[-1] == ('close',)
    assert bad.calls == [('close',)]


def test_cleanup_is_reverse_order_and_every_client_closes() -> None:
    normal = _CleanupClient()
    bad = _CleanupClient()
    resources = probe_wire._ResourceRegistry()
    resources._track('Note', 1)
    resources._track('Attachment', 2)

    result = probe_wire._run_guarded(lambda: 'result', resources, normal, [normal, bad])

    assert result == 'result'
    assert normal.calls == [
        ('delete', 'Attachment', 2),
        ('delete', 'Note', 1),
        ('close',),
    ]
    assert bad.calls == [('close',)]


def test_direct_probe_execution_imports_the_checkout_client() -> None:
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'import pathlib, runpy, sys; '
                "namespace = runpy.run_path('scripts/probe_wire.py'); "
                "client = namespace['_load_client_class'](); "
                'print(pathlib.Path(sys.modules[client.__module__].__file__).resolve())'
            ),
        ],
        check=True,
        capture_output=True,
        env={},
        text=True,
    )

    assert Path(result.stdout.strip()) == Path('src/shotgun_api3/_client.py').resolve()


def test_fixture_inventory_accepts_each_required_label_exactly_once() -> None:
    inventory = probe_wire._validate_fixture_inventory(_inventory())

    assert frozenset(inventory) == frozenset(probe_wire._REQUIRED_OPERATION_LABELS)


@pytest.mark.parametrize('case', ['empty', 'missing', 'unknown', 'duplicate'])
def test_fixture_inventory_rejects_incomplete_or_ambiguous_sets(case: str) -> None:
    records = _inventory()
    if case == 'empty':
        records.clear()
    elif case == 'missing':
        records.pop()
    elif case == 'unknown':
        records[0]['operation'] = 'unknown'
    else:
        records.append(copy.deepcopy(records[0]))

    with pytest.raises(probe_wire._FixtureError):
        probe_wire._validate_fixture_inventory(records)


def test_fixture_normalizer_inventory_has_one_explicit_owner_per_operation() -> None:
    normalizers = probe_wire._fixture_normalizers()
    assert tuple(normalizers) == tuple(probe_wire._REQUIRED_OPERATION_LABELS)
    assert len(set(normalizers.values())) == len(probe_wire._REQUIRED_OPERATION_LABELS)


@pytest.mark.parametrize(
    'value',
    [None, [], MappingProxyType({'operation': 'info'})],
)
def test_fixture_normalizer_rejects_non_dictionary_records(value: object) -> None:
    with pytest.raises(probe_wire._FixtureError) as caught:
        probe_wire._normalize_fixture_record(value)

    assert str(caught.value) == 'fixture normalization failed: operation'


@pytest.mark.parametrize('private_canary', [False, True])
def test_fixture_normalization_rejects_secret_matching_synthetic_content(
    private_canary: bool,
) -> None:
    record = next(item for item in _inventory() if item['operation'] == 'get_session_token')
    synthetic = 'X' * 21
    secrets = {} if private_canary else {'api_key': synthetic}
    canaries: set[str] = {synthetic} if private_canary else set()

    with pytest.raises(probe_wire._ProbeError, match='fixture final scan failed'):
        probe_wire._normalize_fixture_record(record, secrets, canaries)


def test_fixture_publication_failure_preserves_previous_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / 'fixtures'
    directory.mkdir()
    sentinel = directory / 'previous.json'
    sentinel.write_text('{"previous":true}\n')
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    original_replace = Path.replace
    moved_to_backup = False

    def fail_staging_replace(path: Path, target: Path) -> Path:
        nonlocal moved_to_backup
        if path == directory:
            moved_to_backup = True
            return original_replace(path, target)
        if path.name.endswith('.staging') and target == directory:
            assert moved_to_backup
            raise OSError('injected publication failure')
        return original_replace(path, target)

    monkeypatch.setattr(Path, 'replace', fail_staging_replace)

    with pytest.raises(OSError, match='injected publication failure'):
        probe_wire._write_fixture_directory(directory, _inventory())

    assert moved_to_backup
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before
    assert {path.name for path in tmp_path.iterdir()} == {'fixtures'}


@pytest.mark.parametrize(
    ('label', 'mutate', 'publishes'),
    [
        ('activity_stream_read', _mutate_activity_content, False),
        ('create', _mutate_complete_create_record, True),
        ('info', _mutate_headers_and_reason, True),
        ('preferences_read', _mutate_preferences, True),
        ('read', _mutate_request_filter, False),
        ('schema_entity_read', _mutate_schema_labels, True),
        ('work_schedule_read', _mutate_work_schedule, True),
        ('info', _mutate_request_shape, False),
        ('create', _mutate_response_shape, False),
    ],
)
def test_fixture_publication_normalizes_or_rejects_each_private_surface_before_staging(
    label: str,
    mutate: Callable[[dict[str, object], str], None],
    publishes: bool,
    tmp_path: Path,
) -> None:
    canary = f'private-{label}-{mutate.__name__}'
    records = _inventory()
    record = next(item for item in records if item['operation'] == label)
    mutate(record, canary)
    directory = tmp_path / 'fixtures'

    if publishes:
        probe_wire._write_fixture_directory(
            directory,
            records,
            private_canaries={canary, '987654321'},
        )
        assert canary.encode() not in b''.join(path.read_bytes() for path in directory.iterdir())
        assert b'987654321' not in b''.join(path.read_bytes() for path in directory.iterdir())
    else:
        with pytest.raises(probe_wire._FixtureError) as caught:
            probe_wire._write_fixture_directory(
                directory,
                records,
                private_canaries={canary},
            )
        assert label in str(caught.value)
        assert canary not in str(caught.value)
        assert not directory.exists()
        assert list(tmp_path.iterdir()) == []


def test_fixture_publication_replaces_stale_inventory(tmp_path: Path) -> None:
    directory = tmp_path / 'fixtures'
    directory.mkdir()
    (directory / 'stale.json').write_text('{}')

    probe_wire._write_fixture_directory(directory, _inventory())

    assert {path.stem for path in directory.glob('*.json')} == set(
        probe_wire._REQUIRED_OPERATION_LABELS
    )
    assert not (directory / 'stale.json').exists()


def test_fixture_scan_rejects_configured_secret_and_private_canary_safely() -> None:
    record = _fixture()
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    body['ordinary'] = 'configured-secret'

    with pytest.raises(probe_wire._ProbeError) as caught:
        probe_wire._assert_no_secret_bytes(
            record,
            {'api_key': 'configured-secret'},
            {'private-canary'},
        )

    assert str(caught.value) == 'info fixture final scan failed for api_key'
    assert 'configured-secret' not in str(caught.value)

    body['ordinary'] = 'private-canary'
    with pytest.raises(probe_wire._ProbeError) as caught:
        probe_wire._assert_no_secret_bytes(record, {}, {'private-canary'})

    assert str(caught.value) == 'info fixture final scan failed for private canary'
    assert 'private-canary' not in str(caught.value)


def test_fixture_scan_rejects_json_escaped_configured_value_safely() -> None:
    record = _fixture()
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    private_value = 'configured"value\\with\ncontrol'
    body['ordinary'] = private_value

    with pytest.raises(probe_wire._ProbeError) as caught:
        probe_wire._assert_no_secret_bytes(record, {'api_key': private_value})

    assert str(caught.value) == 'info fixture final scan failed for api_key'
    assert private_value not in str(caught.value)


def test_fixture_scan_rejects_json_escaped_private_canary_safely() -> None:
    record = _fixture()
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    private_value = 'private"value\\with\ncontrol'
    body['ordinary'] = private_value

    with pytest.raises(probe_wire._ProbeError) as caught:
        probe_wire._assert_no_secret_bytes(record, {}, {private_value})

    assert str(caught.value) == 'info fixture final scan failed for private canary'
    assert private_value not in str(caught.value)


def test_fixture_schema_accepts_one_exact_versioned_record() -> None:
    assert probe_wire._validate_fixture(_fixture())['operation'] == 'info'


def test_fixture_schema_allows_sensitive_field_names_in_exact_response_bytes() -> None:
    record = _fixture('schema_read')
    response = cast('dict[str, object]', record['response'])
    content = b'{"results":{"ApiUser":{"secret_token":{"data_type":"text"}}}}'
    response['content_b64'] = base64.b64encode(content).decode()

    validated = probe_wire._validate_fixture(record)

    validated_response = cast('dict[str, object]', validated['response'])
    assert (
        base64.b64decode(cast('str', validated_response['content_b64']), validate=True) == content
    )


def test_fixture_schema_rejects_legacy_response_redactions() -> None:
    record = _fixture()
    response = cast('dict[str, object]', record['response'])
    response['redactions'] = []

    with pytest.raises(probe_wire._FixtureError):
        probe_wire._validate_fixture(record)
    with pytest.raises(
        probe_wire._FixtureError, match='info fixture normalization failed: response shape'
    ):
        probe_wire._normalize_fixture_record(record)


@pytest.mark.parametrize(
    'mutation',
    [
        _remove_schema_version,
        _change_schema_version,
        _change_operation,
        _change_method,
        _add_request_key,
        _change_response_bytes,
        _change_outcome,
    ],
)
def test_fixture_schema_rejects_malformed_records(
    mutation: Callable[[dict[str, object]], object],
) -> None:
    record = _fixture()
    mutation(record)

    with pytest.raises(probe_wire._FixtureError):
        probe_wire._validate_fixture(record)


def test_fixture_session_token_normalization_emits_synthetic_response() -> None:
    record = next(item for item in _inventory() if item['operation'] == 'get_session_token')
    response = cast('dict[str, object]', record['response'])
    content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    raw_value = 'raw-session-value-with-a-different-length'
    content['session_id'] = raw_value
    response['content_b64'] = base64.b64encode(json.dumps(content).encode()).decode()
    normalized = probe_wire._normalize_fixture_record(
        record,
        {'session_token': raw_value},
    )

    normalized_response = cast('dict[str, object]', normalized['response'])
    final_content = base64.b64decode(
        cast('str', normalized_response['content_b64']),
        validate=True,
    )
    assert json.loads(final_content)['session_id'] == 'X' * 21
    assert 'redactions' not in normalized_response
    assert raw_value.encode() not in final_content


def test_fixture_utility_static_boundaries_require_dictionary_records() -> None:
    source = """from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import assert_type

from scripts import probe_wire

directory = Path('fixtures')
record: dict[str, object] = {}
records = [record]
secrets: Mapping[str, str] = MappingProxyType({'api_key': 'secret'})
headers: Mapping[str, str] = MappingProxyType({'content-type': 'application/json'})
assert_type(probe_wire._normalize_fixture_record(record, secrets), dict[str, object])
assert_type(probe_wire._validate_fixture_inventory(records), dict[str, dict[str, object]])
probe_wire._assert_no_secret_bytes(record, secrets)
probe_wire._write_fixture_directory(directory, records, secrets=secrets)
assert_type(probe_wire._select_headers(headers, (), ()), dict[str, str])

record_mapping: Mapping[str, object] = MappingProxyType(record)
record_mappings: list[Mapping[str, object]] = [record_mapping]
probe_wire._assert_no_secret_bytes(record_mapping, secrets)
probe_wire._normalize_fixture_record(record_mapping, secrets)
probe_wire._validate_fixture_inventory(record_mappings)
probe_wire._write_fixture_directory(directory, record_mappings, secrets=secrets)
"""
    invalid_lines = {
        index
        for index, line in enumerate(source.splitlines())
        if line.startswith('probe_wire.')
        and 'record_mapping' in line
        and '_normalize_fixture_record' not in line
    }
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
        probe = Path(directory) / 'fixture_utility_types.py'
        probe.write_text(source, encoding='utf-8')
        repository = Path(__file__).parents[2]
        result = run_pyright(probe, cwd=repository)

    report = json.loads(result.stdout)
    diagnostics = report['generalDiagnostics']
    assert result.returncode == 1, result.stdout
    assert report['summary']['errorCount'] == 3
    assert {item['rule'] for item in diagnostics} == {'reportArgumentType'}
    assert {item['range']['start']['line'] for item in diagnostics} == invalid_lines


def test_header_selection_is_case_insensitive_allowlisted_and_sanitized() -> None:
    headers = {
        'CONTENT-TYPE': 'application/json',
        'X-Request-ID': 'request-123',
        'Set-Cookie': 'session_id=configured-secret',
        'X-Ignored': 'must-not-survive',
    }

    selected = probe_wire._select_headers(
        headers,
        {'content-type', 'set-cookie', 'x-request-id'},
        {'configured-secret'},
    )

    assert selected == {
        'content-type': 'application/json',
        'set-cookie': 'REDACTED',
        'x-request-id': 'request-123',
    }


def test_issued_session_token_is_normalized_then_replays_synthetic_result() -> None:
    token = _fixture_session_token()
    content = json.dumps(
        {'session_id': token, 'user_type': 'ApiUser', 'user_id': 987654321},
        separators=(',', ':'),
    ).encode()
    recorder = probe_wire._RecordingTransport(
        _FakeTransport([_response(content)]),
        {'api_key': 'configured-key'},
    )
    spec = probe_wire._CaptureSpec('get_session_token', 'get_session_token', 'script', 'success')

    with recorder._capture(spec):
        recorder.send(
            _request(
                {
                    'method_name': 'get_session_token',
                    'params': [{'script_name': 'probe-script', 'script_key': 'configured-key'}],
                }
            )
        )
    recorder._add_secret('session_token', token)

    record = recorder.records['get_session_token']
    response_record = cast('dict[str, object]', record['response'])
    response = TransportResponse(
        status=cast('int', response_record['status']),
        reason=cast('str', response_record['reason']),
        headers=cast('dict[str, str]', response_record['headers']),
        content=base64.b64decode(
            cast('str', response_record['content_b64']),
            validate=True,
        ),
    )
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    transport = _FakeTransport([_response(_READINESS_BODY), response])
    client._transport = transport

    try:
        result = _REPLAY_RECIPES['get_session_token'](client, None)
    finally:
        client.close()

    assert result == 'X' * 21
    assert 'redactions' not in response_record


@pytest.mark.parametrize('results', [[], [{'id': 42}, {'id': 43}]])
def test_live_batch_rejects_unexpected_result_count_without_tracking(
    results: list[dict[str, int]],
) -> None:
    state, inner, bad_inner = _live_mutation_state('batch', results)
    resources = probe_wire._ResourceRegistry()

    with pytest.raises(ExceptionGroup) as caught:
        probe_wire._run_guarded(
            lambda: probe_wire._run_live_operations(state, resources, 'probe-test', {}),
            resources,
            state.client,
            [state.client, state.bad_client],
        )

    primary, normalization = caught.value.exceptions
    assert isinstance(primary, probe_wire._ProbeError)
    assert str(primary) == 'batch capture did not create exactly one entity'
    assert isinstance(normalization, probe_wire._FixtureError)
    bodies = [json.loads(cast('bytes', request.body)) for request in inner.requests]
    deletes = [body['params'][-1] for body in bodies if body['method_name'] == 'delete']
    assert deletes == [{'type': 'Note', 'id': 41}]
    assert inner.closed
    assert bad_inner.closed
    assert 'batch' not in state.recorder.records
    assert state.recorder._pending is None


@pytest.mark.parametrize('label', ['create', 'batch'])
@pytest.mark.parametrize(
    'identity',
    [
        {},
        {'id': True},
        {'id': 0},
        {'id': '42'},
        {'id': 42, 'type': 'Project'},
    ],
)
def test_live_malformed_mutation_identity_does_not_authorize_cleanup(
    label: str,
    identity: dict[str, object],
) -> None:
    state, inner, bad_inner = _live_mutation_state(
        label,
        [identity] if label == 'batch' else identity,
    )
    resources = probe_wire._ResourceRegistry()

    with pytest.raises(ExceptionGroup) as caught:
        probe_wire._run_guarded(
            lambda: probe_wire._run_live_operations(state, resources, 'probe-test', {}),
            resources,
            state.client,
            [state.client, state.bad_client],
        )

    primary, normalization = caught.value.exceptions
    assert isinstance(primary, probe_wire._ProbeError)
    assert isinstance(normalization, probe_wire._FixtureError)
    bodies = [json.loads(cast('bytes', request.body)) for request in inner.requests]
    deletes = [body['params'][-1] for body in bodies if body['method_name'] == 'delete']
    assert deletes == ([{'type': 'Note', 'id': 41}] if label == 'batch' else [])
    assert inner.closed
    assert bad_inner.closed
    assert label not in state.recorder.records
    assert state.recorder._pending is None


@pytest.mark.parametrize('label', ['create', 'batch'])
@pytest.mark.parametrize('cleanup_result', [True, False])
def test_live_mutation_is_cleaned_when_normalization_rejects_response(
    label: str,
    cleanup_result: bool,
) -> None:
    note = {
        'type': 'Note',
        'id': 42,
        'subject': 'probe-test',
        'project': {'type': 'Project', 'id': 1, 'name': 'Probe Project'},
        'unknown': 'private response field',
    }
    state, inner, bad_inner = _live_mutation_state(
        label,
        [note] if label == 'batch' else note,
        cleanup_result=cleanup_result,
    )
    resources = probe_wire._ResourceRegistry()

    expected_error = probe_wire._FixtureError if cleanup_result else ExceptionGroup
    with pytest.raises(expected_error) as caught:
        probe_wire._run_guarded(
            lambda: probe_wire._run_live_operations(state, resources, 'probe-test', {}),
            resources,
            state.client,
            [state.client, state.bad_client],
        )

    if cleanup_result:
        assert f'{label} fixture normalization failed' in str(caught.value)
    else:
        assert isinstance(caught.value, ExceptionGroup)
        primary, cleanup = caught.value.exceptions
        assert isinstance(primary, probe_wire._FixtureError)
        assert f'{label} fixture normalization failed' in str(primary)
        assert isinstance(cleanup, probe_wire._ProbeError)
        assert str(cleanup) == 'cleanup rejected Note 42'
    bodies = [json.loads(cast('bytes', request.body)) for request in inner.requests]
    deletes = [body['params'][-1] for body in bodies if body['method_name'] == 'delete']
    expected_ids = [42, 41] if label == 'batch' else [42]
    assert deletes == [{'type': 'Note', 'id': entity_id} for entity_id in expected_ids]
    assert inner.closed
    assert bad_inner.closed
    assert label not in state.recorder.records
    assert state.recorder._pending is None
    assert label not in state.recorder._raw_response_content


def test_nav_expand_replay_uses_the_site_independent_root_path() -> None:
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    transport = _FakeTransport(
        [
            _response(_READINESS_BODY),
            _response(b'{"results":{"label":"Project"}}'),
        ]
    )
    client._transport = transport

    try:
        assert _REPLAY_RECIPES['nav_expand'](client, None) == {'label': 'Project'}
    finally:
        client.close()

    request = transport.requests[-1]
    assert isinstance(request.body, bytes)
    payload = json.loads(request.body)
    params = payload['params']
    assert isinstance(params, list)
    assert params[-1] == {'path': '/'}


def test_project_access_capture_and_replay_use_current_wire_name() -> None:
    assert probe_wire._CAPTURE_SPECS['update_project_last_accessed'] == (
        'update_project_last_accessed_by_current_user',
        'script',
        'success',
    )
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    transport = _FakeTransport(
        [
            _response(_READINESS_BODY),
            _response(b'{"results":true}'),
        ]
    )
    client._transport = transport

    try:
        assert _REPLAY_RECIPES['update_project_last_accessed'](client, None) is None
    finally:
        client.close()

    request = transport.requests[-1]
    assert isinstance(request.body, bytes)
    payload = json.loads(request.body)
    assert payload['method_name'] == 'update_project_last_accessed_by_current_user'
    params = payload['params']
    assert isinstance(params, list)
    assert params[-1] == {'project_id': 1, 'user_id': 1}


def test_published_fixture_inventory_is_complete() -> None:
    records = probe_wire._load_fixture_directory(_FIXTURES)

    assert frozenset(records) == frozenset(probe_wire._REQUIRED_OPERATION_LABELS)


@pytest.mark.parametrize('label', sorted(probe_wire._REQUIRED_OPERATION_LABELS))
def test_published_fixture_replays_through_fake_transport(label: str) -> None:
    records = probe_wire._load_fixture_directory(_FIXTURES)
    record = records[label]
    request_record = cast('dict[str, object]', record['request'])
    request_body = cast('dict[str, object]', request_record['body'])
    response_record = cast('dict[str, object]', record['response'])
    response = TransportResponse(
        status=cast('int', response_record['status']),
        reason=cast('str', response_record['reason']),
        headers=cast('dict[str, str]', response_record['headers']),
        content=base64.b64decode(cast('str', response_record['content_b64']), validate=True),
    )
    auth_mode = record['auth_mode']
    if auth_mode == 'script':
        client = Shotgun(
            'https://fixture.invalid',
            script_name='fixture-script',
            api_key='fixture-key',
            connect=False,
        )
    else:
        client = Shotgun(
            'https://fixture.invalid',
            session_token=_fixture_session_token(),
            connect=False,
        )
    scripted: list[TransportResponse | TransportError] = (
        [response] if label == 'info' else [_response(_READINESS_BODY), response]
    )
    transport = _FakeTransport(scripted)
    client._transport = transport
    outcome = cast('dict[str, object]', record['outcome'])

    try:
        if outcome['kind'] == 'fault':
            assert outcome == {'kind': 'fault', 'exception': 'AuthenticationFault'}
            with pytest.raises(_errors.AuthenticationFault):
                _REPLAY_RECIPES[label](client, request_body)
        else:
            result = _REPLAY_RECIPES[label](client, request_body)
            expected = _replay_expected_result(label)
            assert result == expected

        sent = transport.requests[-1]
        assert isinstance(sent.body, bytes)
        sent_body = probe_wire._sanitize(
            json.loads(sent.body),
            {'fixture-script', 'fixture-key', _fixture_session_token()},
        )
        assert sent.method == request_record['method']
        assert urlsplit(sent.url).path == request_record['path']
        assert (
            probe_wire._select_headers(
                sent.headers,
                probe_wire._REQUEST_HEADER_ALLOWLIST,
                {'fixture-script', 'fixture-key', _fixture_session_token()},
            )
            == request_record['headers']
        )
        assert sent_body == request_record['body']
    finally:
        client.close()


def test_recording_transport_groups_operation_error_with_missing_exchange() -> None:
    recorder = probe_wire._RecordingTransport(_FakeTransport([]), {})
    spec = probe_wire._CaptureSpec('info', 'info', 'script', 'success')
    primary = RuntimeError('operation failed')

    with pytest.raises(ExceptionGroup) as caught, recorder._capture(spec):
        raise primary

    assert caught.value.exceptions[0] is primary
    capture_error = caught.value.exceptions[1]
    assert isinstance(capture_error, probe_wire._ProbeError)
    assert str(capture_error) == 'info did not capture exactly one exchange; captured 0'
    assert recorder.records == {}
    assert recorder._pending is None


def test_recording_transport_rejects_duplicate_exchange_without_publication() -> None:
    recorder = probe_wire._RecordingTransport(
        _FakeTransport([_response(_READINESS_BODY), _response(_READINESS_BODY)]),
        {},
    )
    spec = probe_wire._CaptureSpec('info', 'info', 'script', 'success')

    def send_twice() -> None:
        with recorder._capture(spec):
            recorder.send(_request({'method_name': 'info', 'params': []}))
            assert recorder.records == {}
            recorder.send(_request({'method_name': 'info', 'params': []}))

    with pytest.raises(ExceptionGroup) as caught:
        send_twice()

    assert all(isinstance(error, probe_wire._ProbeError) for error in caught.value.exceptions)
    assert [str(error) for error in caught.value.exceptions] == [
        'info matched more than one exchange',
        'info did not capture exactly one exchange; captured 2',
    ]
    assert recorder.records == {}
    assert recorder._pending is None
    assert recorder._raw_response_content == {}


def test_recording_transport_requires_one_matching_exchange() -> None:
    recorder = probe_wire._RecordingTransport(
        _FakeTransport([_response(b'{"results":{}}')]),
        {},
    )
    spec = probe_wire._CaptureSpec('read', 'read', 'script', 'success')

    with (
        pytest.raises(probe_wire._ProbeError, match='did not capture'),
        recorder._capture(spec),
    ):
        recorder.send(_request({'method_name': 'info', 'params': []}))

    assert recorder.records == {}
    assert recorder._pending is None


def test_recording_transport_returns_exact_response_but_publishes_normalized_metadata() -> None:
    content = json.dumps(
        {
            'version': [9, 1, 2],
            'full_version': [9, 1, 2, 3],
            's3_uploads_enabled': True,
            's3_direct_uploads_enabled': True,
            's3_enabled_upload_types': {'private': True},
            'shotgunlocalhost_browser_integration_enabled': True,
            'unified_login_flow_enabled': True,
            'authentication_app_session_launcher_enabled': True,
            'analytics_site_id': 'private-site-id',
            'analytics_site_name': 'Private Site',
            'user_authentication_method': 'private-auth-method',
            'api_max_entities_per_page': 999,
        },
        separators=(',', ':'),
    ).encode()
    inner = _FakeTransport(
        [
            TransportResponse(
                status=200,
                reason='Private Reason',
                headers={
                    'Content-Type': 'application/octet-stream',
                    'X-Request-ID': 'request-123',
                    'Set-Cookie': 'must-not-survive',
                },
                content=content,
            )
        ]
    )
    recorder = probe_wire._RecordingTransport(inner, {'api_key': 'configured-secret'})
    spec = probe_wire._CaptureSpec('info', 'info', 'script', 'success')

    with recorder._capture(spec):
        returned = recorder.send(_request({'method_name': 'info', 'params': []}))
        assert recorder.records == {}

    record = recorder.records['info']
    assert recorder._pending is None
    response = cast('dict[str, object]', record['response'])
    assert returned.content == content
    published = base64.b64decode(cast('str', response['content_b64']), validate=True)
    assert published != content
    assert b'Private Site' not in published
    assert response['status'] == 200
    assert response['reason'] == 'OK'
    assert response['headers'] == {'content-type': 'application/json'}


def test_recording_transport_sanitizes_request_and_preserves_json_response() -> None:
    content = json.dumps(
        {'session_id': 'issued-session', 'user_type': 'ApiUser', 'user_id': 987654321},
        separators=(',', ':'),
    ).encode()
    inner = _FakeTransport([_response(content)])
    recorder = probe_wire._RecordingTransport(
        inner,
        {
            'api_key': 'configured-secret',
            'script_key': 'script-key',
            'session_token': 'session-value',
        },
    )
    spec = probe_wire._CaptureSpec('get_session_token', 'get_session_token', 'script', 'success')

    with recorder._capture(spec):
        recorder.send(
            _request(
                {
                    'method_name': 'get_session_token',
                    'params': [{'script_name': 'probe-script', 'script_key': 'script-key'}],
                }
            )
        )

    record = recorder.records['get_session_token']
    request = cast('dict[str, object]', record['request'])
    body = cast('dict[str, object]', request['body'])
    response = cast('dict[str, object]', record['response'])

    auth = cast('dict[str, object]', cast('list[object]', body['params'])[0])
    assert auth['script_name'] == 'REDACTED'
    assert auth['script_key'] == 'REDACTED'
    assert request['path'] == '/api3/json'
    normalized_content = cast(
        'dict[str, object]',
        json.loads(base64.b64decode(cast('str', response['content_b64']), validate=True)),
    )
    assert normalized_content == {
        'session_id': 'X' * 21,
        'user_type': 'ApiUser',
        'user_id': 1,
    }


def test_required_inventory_matches_capture_and_replay_recipes() -> None:
    expected = frozenset(
        {
            'activity_stream_read',
            'batch',
            'create',
            'fault_bad_auth',
            'following',
            'get_session_token',
            'info',
            'nav_expand',
            'preferences_read',
            'read',
            'schema_entity_read',
            'schema_read',
            'summarize',
            'text_search',
            'update_project_last_accessed',
            'work_schedule_read',
        }
    )
    assert expected == frozenset(probe_wire._REQUIRED_OPERATION_LABELS)
    assert frozenset(probe_wire._CAPTURE_SPECS) == frozenset(probe_wire._REQUIRED_OPERATION_LABELS)
    assert frozenset(_REPLAY_RECIPES) == frozenset(probe_wire._REQUIRED_OPERATION_LABELS)


def test_sanitizer_removes_nested_secrets_and_common_sensitive_values() -> None:
    value = {
        'ordinary': 'prefix-configured-secret-suffix',
        'nested': [
            {'script_key': 'script-key'},
            {'Session-ID': 'session-value'},
            {'userPassword': 'password-value'},
            {'authorization': 'Bearer configured-secret'},
        ],
    }

    sanitized = probe_wire._sanitize(value, {'configured-secret', 'script-key'})
    encoded = json.dumps(sanitized, sort_keys=True).encode()

    assert b'configured-secret' not in encoded
    assert b'script-key' not in encoded
    assert b'session-value' not in encoded
    assert b'password-value' not in encoded
    assert cast('dict[str, object]', sanitized)['ordinary'] == 'prefix-REDACTED-suffix'


def test_secret_scan_allows_incidental_identifier_substrings_in_json_keys() -> None:
    content = b'{"results":{"latest_update_id":1}}'
    record = _fixture()
    response = cast('dict[str, object]', record['response'])
    response['content_b64'] = base64.b64encode(content).decode()

    probe_wire._assert_no_secret_bytes(record, {'script_name': 'test'})

    assert base64.b64decode(response['content_b64'], validate=True) == content


def test_secret_scan_blocks_credential_substrings_and_reports_safely() -> None:
    record = _fixture()
    response = cast('dict[str, object]', record['response'])
    response['content_b64'] = base64.b64encode(
        b'{"results":{"ordinary":"prefix-configured-secret-suffix"}}'
    ).decode()

    with pytest.raises(
        probe_wire._ProbeError,
        match=r'^info fixture final scan failed for api_key$',
    ) as caught:
        probe_wire._assert_no_secret_bytes(record, {'api_key': 'configured-secret'})

    assert 'configured-secret' not in str(caught.value)


@pytest.mark.parametrize(
    'content',
    [
        b'{"results":{"test":1}}',
        b'{"results":{"ordinary":"test"}}',
    ],
)
def test_secret_scan_blocks_exact_identifier_keys_and_values(content: bytes) -> None:
    record = _fixture()
    response = cast('dict[str, object]', record['response'])
    response['content_b64'] = base64.b64encode(content).decode()

    with pytest.raises(
        probe_wire._ProbeError,
        match=r'^info fixture final scan failed for script_name$',
    ):
        probe_wire._assert_no_secret_bytes(record, {'script_name': 'test'})


def test_text_search_capture_and_replay_use_current_flat_wire_contract() -> None:
    assert probe_wire._CAPTURE_SPECS['text_search'] == (
        'query_display_name_cache',
        'script',
        'success',
    )
    client = Shotgun(
        'https://fixture.invalid',
        script_name='fixture-script',
        api_key='fixture-key',
        connect=False,
    )
    transport = _FakeTransport(
        [
            _response(_READINESS_BODY),
            _response(b'{"results":{"matches":[]}}'),
        ]
    )
    client._transport = transport

    try:
        assert _REPLAY_RECIPES['text_search'](client, None) == {'matches': []}
    finally:
        client.close()

    request = transport.requests[-1]
    assert isinstance(request.body, bytes)
    payload = json.loads(request.body)
    assert payload['method_name'] == 'query_display_name_cache'
    params = payload['params']
    assert isinstance(params, list)
    assert params[-1] == {
        'text': 'wire probe',
        'entity_types': {
            'Project': {'logical_operator': 'and', 'conditions': []},
        },
        'project_ids': [],
        'max_results': None,
    }
