import dataclasses
import datetime
import locale
import random
from types import MappingProxyType
from typing import Any, cast

import pytest

from shotgun_api3 import _protocol


def test_attempt_exhaustion_stops_retrying() -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            3,
            _protocol.Failure('http_status', 'transient', 503, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize(
    ('normalized_params', 'replay_safe'),
    [
        ([{'request_type': 'read'}, {'request_type': 'read'}], True),
        (
            [{'request_type': 'read'}, {'request_type': 'update'}],
            False,
        ),
        ([{'request_type': 'read'}, {'request_type': 'unknown'}], False),
        ([{'request_type': 'read'}, {}], False),
        ([{'request_type': 'read'}, 'malformed'], False),
        ([], False),
        ({'requests': [{'request_type': 'read'}]}, True),
    ],
)
def test_classify_batch_inspects_every_member(
    normalized_params: object,
    replay_safe: bool,
) -> None:
    assert _protocol.classify(
        'batch',
        normalized_params,
    ) == _protocol.OperationDescriptor(
        category='batch',
        replay_safe=replay_safe,
        body_restartable=True,
        endpoint_role=None,
    )


@pytest.mark.parametrize(
    ('endpoint_role', 'category', 'replay_safe'),
    [
        ('read', 'read', True),
        ('download', 'download', True),
        ('object_put', 'upload', True),
        ('part_put', 'upload', True),
        ('allocation', 'upload', False),
        ('link', 'upload', False),
        ('finalize', 'upload', False),
    ],
)
def test_classify_endpoint_roles(
    endpoint_role: _protocol._EndpointRole,
    category: _protocol.OperationCategory,
    replay_safe: bool,
) -> None:
    assert _protocol.classify_endpoint(
        endpoint_role, body_restartable=False
    ) == _protocol.OperationDescriptor(
        category=category,
        replay_safe=replay_safe,
        body_restartable=False,
        endpoint_role=endpoint_role,
    )


@pytest.mark.parametrize(
    ('has_one_use_auth_token', 'replay_safe'),
    [(False, True), (True, False)],
)
def test_classify_human_authentication_explicitly(
    has_one_use_auth_token: bool,
    replay_safe: bool,
) -> None:
    assert _protocol.classify(
        'authenticate_human_user',
        {},
        has_one_use_auth_token=has_one_use_auth_token,
    ) == _protocol.OperationDescriptor(
        category='human_authentication',
        replay_safe=replay_safe,
        body_restartable=True,
        endpoint_role=None,
    )


def test_classify_info_remains_unknown_and_unsafe() -> None:
    assert _protocol.classify('info', {}) == _protocol.OperationDescriptor(
        category='unknown',
        replay_safe=False,
        body_restartable=True,
        endpoint_role=None,
    )


@pytest.mark.parametrize(
    ('method_name', 'category', 'replay_safe'),
    [
        ('read', 'read', True),
        ('activity_stream_read', 'read', True),
        ('export_page', 'read', True),
        ('followers', 'read', True),
        ('following', 'read', True),
        ('nav_expand', 'read', True),
        ('nav_search_entity', 'read', True),
        ('nav_search_string', 'read', True),
        ('note_thread_read', 'read', True),
        ('preferences_read', 'read', True),
        ('schema_entity_read', 'read', True),
        ('schema_field_read', 'read', True),
        ('schema_read', 'read', True),
        ('summarize', 'read', True),
        ('text_search', 'read', True),
        ('user_subscriptions_read', 'read', True),
        ('work_schedule_read', 'read', True),
        ('create', 'write', False),
        ('update', 'write', False),
        ('delete', 'write', False),
        ('revive', 'write', False),
        ('follow', 'write', False),
        ('schema_field_create', 'write', False),
        ('schema_field_delete', 'write', False),
        ('schema_field_update', 'write', False),
        ('unfollow', 'write', False),
        ('update_project_last_accessed', 'write', False),
        ('user_subscriptions_create', 'write', False),
        ('work_schedule_update', 'write', False),
        ('unknown_method', 'unknown', False),
    ],
)
def test_classify_rpc_methods(
    method_name: str,
    category: _protocol.OperationCategory,
    replay_safe: bool,
) -> None:
    assert _protocol.classify(method_name, {}) == _protocol.OperationDescriptor(
        category=category,
        replay_safe=replay_safe,
        body_restartable=True,
        endpoint_role=None,
    )


def test_classify_rpc_operation_treats_driver_info_as_safe_read() -> None:
    assert _protocol.classify_rpc_operation(
        'info',
        None,
        {},
        has_one_use_auth_token=False,
    ) == (
        _protocol.OperationDescriptor(
            category='read',
            replay_safe=True,
            body_restartable=True,
            endpoint_role=None,
        )
    )


@pytest.mark.parametrize(
    ('normalized_params', 'category', 'replay_safe'),
    [
        ({}, 'session_authentication', True),
        ({'auth_token': None}, 'session_authentication', True),
        ({'auth_token': 'one-use'}, 'one_use_auth_exchange', False),
        ([{'auth': {'auth_token': 'one-use'}}], 'one_use_auth_exchange', False),
    ],
)
def test_classify_session_token_exchange(
    normalized_params: object,
    category: _protocol.OperationCategory,
    replay_safe: bool,
) -> None:
    assert _protocol.classify(
        'get_session_token',
        normalized_params,
    ) == _protocol.OperationDescriptor(
        category=category,
        replay_safe=replay_safe,
        body_restartable=True,
        endpoint_role=None,
    )


@pytest.mark.parametrize(
    'retry_after',
    [
        float('-inf'),
        float('inf'),
        float('nan'),
    ],
)
def test_decide_rejects_non_finite_explicit_delays(
    retry_after: float,
) -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'rate_limit', 429, retry_after),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.000000003,
        )
        is None
    )


def test_decide_requires_named_policy_parameters() -> None:
    operation = _protocol.classify('read', {})
    failure = _protocol.Failure('ambiguous_after_send', 'transient', None, None)
    invalid_decide = cast('Any', _protocol.decide)

    with pytest.raises(TypeError):
        invalid_decide(operation, 1, failure, 3, 4.0, 1_000.000000003)
    decision = _protocol.decide(
        operation,
        1,
        failure,
        max_attempts=3,
        base_interval=4.0,
        now=1_000.000000003,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


def test_decide_stops_for_oversized_retry_after() -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'rate_limit', 429, 'too-large'),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


def test_decide_uses_computed_backoff_for_malformed_retry_after() -> None:
    failure = _protocol.classify_http_failure(
        429,
        {'Retry-After': 'tomorrow'},
        1_735_689_600.0,
    )

    assert failure is not None
    assert failure == _protocol.Failure('http_status', 'rate_limit', 429, None)
    decision = _protocol.decide(
        _protocol.classify('read', {}),
        1,
        failure,
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


def test_explicit_retry_after_replaces_jitter() -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'rate_limit', 429, 7.0),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        == 7.0
    )


def test_jitter_does_not_read_or_mutate_module_global_rng(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = random.getstate()

    def unexpected_global_rng(*_args: object, **_kwargs: object) -> None:
        raise AssertionError('module-global RNG was accessed')

    with monkeypatch.context() as context:
        context.setattr(random, 'getstate', unexpected_global_rng)
        context.setattr(random, 'random', unexpected_global_rng)
        context.setattr(random, 'seed', unexpected_global_rng)
        context.setattr(random, 'uniform', unexpected_global_rng)
        decision = _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('ambiguous_after_send', 'transient', None, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.000000003,
        )
        assert decision is not None
        assert 0.0 <= decision <= 4.0

    assert random.getstate() == state


def test_jitter_is_capped_and_derived_only_from_explicit_inputs() -> None:
    operation = _protocol.classify('read', {})
    failure = _protocol.Failure('ambiguous_after_send', 'transient', None, None)
    decisions = [
        _protocol.decide(
            operation,
            5,
            failure,
            max_attempts=6,
            base_interval=4.0,
            now=1_000.0 + offset,
        )
        for offset in range(64)
    ]

    assert all(decision is not None for decision in decisions)
    assert all(0.0 <= decision <= 30.0 for decision in decisions if decision is not None)
    decision = decisions[0]
    assert (
        _protocol.decide(
            operation,
            5,
            failure,
            max_attempts=6,
            base_interval=4.0,
            now=1_000.0,
        )
        == decision
    )


def test_nearby_jitter_seeds_decorrelate() -> None:
    operation = _protocol.classify('read', {})
    failure = _protocol.Failure('ambiguous_after_send', 'transient', None, None)

    first = _protocol.decide(
        operation,
        1,
        failure,
        max_attempts=3,
        base_interval=4.0,
        now=1_000.000000002,
    )
    second = _protocol.decide(
        operation,
        1,
        failure,
        max_attempts=3,
        base_interval=4.0,
        now=1_000.000000003,
    )

    assert first is not None
    assert second is not None
    assert 0.0 <= first <= 4.0
    assert 0.0 <= second <= 4.0
    assert first != second


@pytest.mark.parametrize(
    'header',
    [
        'Sun, 06 Nov 1994 08:49:37 GMT',
        'Sunday, 06-Nov-94 08:49:37 GMT',
        'Sun Nov  6 08:49:37 1994',
    ],
)
def test_parse_retry_after_accepts_all_http_date_forms(header: str) -> None:
    assert _protocol._parse_retry_after({'Retry-After': header}, 784_111_767.0) == 10.0


@pytest.mark.parametrize(
    ('headers', 'expected'),
    [
        ({'Retry-After': '12'}, 12.0),
        ({'retry-after': ' 0 '}, 0.0),
        ({'RETRY-AFTER': '001'}, 1.0),
        ({'Other': 'ignored', 'rEtRy-AfTeR': '7'}, 7.0),
    ],
)
def test_parse_retry_after_accepts_case_insensitive_delta_seconds(
    headers: dict[str, str],
    expected: float,
) -> None:
    assert _protocol._parse_retry_after(MappingProxyType(headers), 1_735_689_600.0) == expected


def test_parse_retry_after_accepts_http_date() -> None:
    assert (
        _protocol._parse_retry_after(
            {'Retry-After': 'Wed, 01 Jan 2025 00:00:10 GMT'},
            1_735_689_600.0,
        )
        == 10.0
    )


def test_parse_retry_after_accepts_http_dates_under_available_locales() -> None:
    original_locale = locale.setlocale(locale.LC_TIME)
    try:
        locale.setlocale(locale.LC_TIME, 'C')
        headers = (
            'Sun, 06 Nov 1994 08:49:37 GMT',
            'Sunday, 06-Nov-94 08:49:37 GMT',
            'Sun Nov  6 08:49:37 1994',
        )
        for header in headers:
            assert _protocol._parse_retry_after({'Retry-After': header}, 784_111_767.0) == 10.0

        for candidate in ('fr_FR.UTF-8', 'de_DE.UTF-8', 'es_ES.UTF-8'):
            try:
                locale.setlocale(locale.LC_TIME, candidate)
            except locale.Error:
                continue
            if datetime.date(1994, 11, 6).strftime('%b') != 'Nov':
                for header in headers:
                    assert (
                        _protocol._parse_retry_after({'Retry-After': header}, 784_111_767.0)
                        == 10.0
                    )
                break
    finally:
        locale.setlocale(locale.LC_TIME, original_locale)


def test_parse_retry_after_clamps_past_http_date_to_zero() -> None:
    assert (
        _protocol._parse_retry_after(
            {'Retry-After': 'Tue, 31 Dec 2024 23:59:50 GMT'},
            1_735_689_600.0,
        )
        == 0.0
    )


@pytest.mark.parametrize(
    ('header', 'expected'),
    [
        ('0', 0.0),
        ('30', 30.0),
        ('31', 'too-large'),
        ('86400', 'too-large'),
        pytest.param('9' * 5_000, 'too-large', id='very-long-oversized-delta'),
        pytest.param(
            '0' * 5_000 + '30',
            30.0,
            id='very-long-leading-zero-bounded-delta',
        ),
        ('Wed, 01 Jan 2025 00:00:30 GMT', 30.0),
        ('Wed, 01 Jan 2025 00:00:31 GMT', 'too-large'),
    ],
)
def test_parse_retry_after_distinguishes_bounded_and_oversized_values(
    header: str,
    expected: float | str,
) -> None:
    assert (
        _protocol._parse_retry_after(
            {'Retry-After': header},
            1_735_689_600.0,
        )
        == expected
    )


@pytest.mark.parametrize(
    'headers',
    [
        {},
        {'Retry-After': ''},
        {'Retry-After': '-1'},
        {'Retry-After': '+1'},
        {'Retry-After': '1.5'},
        {'Retry-After': 'tomorrow'},
        {'Retry-After': 'Wed, 99 Jan 2025 00:00:10 GMT'},
    ],
)
def test_parse_retry_after_rejects_invalid_values(headers: dict[str, str]) -> None:
    assert _protocol._parse_retry_after(headers, 1_735_689_600.0) is None


@pytest.mark.parametrize(
    'header',
    [
        'Sun, 06 Nov 1994 08:49:37 +0000',
        'Sun, 06 Nov 1994 08:49:37 -0000',
        'Sun, 06 Nov 1994 08:49:37 UTC',
        'Sun, 06 Nov 1994 08:49:37 PST',
        'Sunday, 06-Nov-94 08:49:37 +0000',
        'Sun Nov  6 08:49:37 1994 GMT',
    ],
)
def test_parse_retry_after_rejects_non_http_date_zones(header: str) -> None:
    assert _protocol._parse_retry_after({'Retry-After': header}, 784_111_767.0) is None


def test_pre_send_failure_does_not_retry_unrestartable_body() -> None:
    operation = _protocol.classify('read', {}, body_restartable=False)

    assert (
        _protocol.decide(
            operation,
            1,
            _protocol.Failure('pre_send', 'transient', None, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize('category', ['rate_limit', 'maintenance'])
def test_pre_send_retries_only_transient_failures(
    category: _protocol._FailureCategory,
) -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('pre_send', category, None, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize(
    'operation',
    [
        _protocol.classify('read', {}),
        _protocol.classify('create', {}),
        _protocol.classify('batch', [{'request_type': 'update'}]),
        _protocol.classify_endpoint('allocation'),
    ],
)
def test_pre_send_transient_failure_retries_restartable_bodies(
    operation: _protocol.OperationDescriptor,
) -> None:
    decision = _protocol.decide(
        operation,
        1,
        _protocol.Failure('pre_send', 'transient', None, None),
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


@pytest.mark.parametrize('phase', ['ambiguous_after_send', 'body_read', 'http_status'])
def test_replay_safe_operation_requires_restartable_body(
    phase: _protocol._FailurePhase,
) -> None:
    operation = _protocol.classify_endpoint('part_put', body_restartable=False)
    status = 503 if phase == 'http_status' else None

    assert (
        _protocol.decide(
            operation,
            1,
            _protocol.Failure(phase, 'transient', status, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize(
    'operation',
    [
        _protocol.classify('read', {}),
        _protocol.classify('batch', [{'request_type': 'read'}]),
        _protocol.classify('authenticate_human_user', {}),
        _protocol.classify('get_session_token', {}),
        _protocol.classify_endpoint('download'),
        _protocol.classify_endpoint('object_put'),
        _protocol.classify_endpoint('part_put'),
    ],
)
def test_replay_safe_operations_retry_ambiguous_transient_failure(
    operation: _protocol.OperationDescriptor,
) -> None:
    decision = _protocol.decide(
        operation,
        1,
        _protocol.Failure('ambiguous_after_send', 'transient', None, None),
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


def test_retry_policy_data_is_frozen() -> None:
    operation = _protocol.OperationDescriptor(
        category='read',
        replay_safe=True,
        body_restartable=True,
        endpoint_role=None,
    )
    failure = _protocol.Failure(
        phase='pre_send',
        category='transient',
        status=None,
        retry_after=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        type(operation).__setattr__(operation, 'replay_safe', False)
    with pytest.raises(dataclasses.FrozenInstanceError):
        type(failure).__setattr__(failure, 'phase', 'body_read')


def test_retryable_statuses_are_exact() -> None:
    assert frozenset({502, 503, 504}) == _protocol._RETRYABLE_STATUSES


@pytest.mark.parametrize('endpoint_role', ['read', 'download'])
def test_safe_body_read_failure_retries_from_byte_zero(
    endpoint_role: _protocol._EndpointRole,
) -> None:
    operation = _protocol.classify_endpoint(endpoint_role)

    decision = _protocol.decide(
        operation,
        1,
        _protocol.Failure('body_read', 'transient', None, None),
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


@pytest.mark.parametrize('status', sorted({502, 503, 504}))
def test_safe_read_retries_exact_transient_http_statuses(status: int) -> None:
    decision = _protocol.decide(
        _protocol.classify('read', {}),
        1,
        _protocol.Failure('http_status', 'transient', status, None),
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


@pytest.mark.parametrize(
    ('category', 'status'),
    [('rate_limit', 429), ('maintenance', 503)],
)
def test_safe_read_retries_server_signaled_categories(
    category: _protocol._FailureCategory,
    status: int,
) -> None:
    decision = _protocol.decide(
        _protocol.classify('read', {}),
        1,
        _protocol.Failure('http_status', category, status, None),
        max_attempts=3,
        base_interval=4.0,
        now=1_000.25,
    )
    assert decision is not None
    assert 0.0 <= decision <= 4.0


@pytest.mark.parametrize(
    'category',
    ['abort', 'authentication', 'configuration', 'proxy_auth', 'tls'],
)
def test_terminal_failure_categories_always_stop(
    category: _protocol._FailureCategory,
) -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', category, 503, 7.0),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize('status', [400, 429, 500, 505, None])
def test_transient_http_status_outside_allowlist_does_not_retry(
    status: int | None,
) -> None:
    assert (
        _protocol.decide(
            _protocol.classify('read', {}),
            1,
            _protocol.Failure('http_status', 'transient', status, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


@pytest.mark.parametrize(
    'operation',
    [
        _protocol.classify('create', {}),
        _protocol.classify('update', {}),
        _protocol.classify('delete', {}),
        _protocol.classify('revive', {}),
        _protocol.classify('batch', [{'request_type': 'read'}, {'request_type': 'update'}]),
        _protocol.classify('get_session_token', {'auth_token': 'one-use'}),
        _protocol.classify_endpoint('allocation'),
        _protocol.classify_endpoint('link'),
        _protocol.classify_endpoint('finalize'),
        _protocol.classify('unknown_method', {}),
    ],
)
def test_unsafe_operations_do_not_retry_ambiguous_transmission(
    operation: _protocol.OperationDescriptor,
) -> None:
    assert (
        _protocol.decide(
            operation,
            1,
            _protocol.Failure('ambiguous_after_send', 'transient', None, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )


def test_write_does_not_retry_body_read_failure() -> None:
    assert (
        _protocol.decide(
            _protocol.classify('create', {}),
            1,
            _protocol.Failure('body_read', 'transient', None, None),
            max_attempts=3,
            base_interval=4.0,
            now=1_000.25,
        )
        is None
    )
