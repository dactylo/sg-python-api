import copy
import inspect
import logging
import re
import threading
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytest

from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import CompatibilityWarningEvent, ObserverEvent, ResponseEvent
from tests.conftest import _fake_client, _make_response, _sent_payload

_SchemaReadCallable: TypeAlias = Callable[[dict[str, Any] | None], dict[str, object]]
_SchemaEntityReadCallable: TypeAlias = Callable[[dict[str, Any] | None], dict[str, object]]
_SchemaFieldReadCallable: TypeAlias = Callable[
    [str, str | None, dict[str, Any] | None],
    dict[str, object],
]
_SchemaFieldCreateCallable: TypeAlias = Callable[
    [str, str, str, dict[str, Any] | None],
    str,
]
_SchemaFieldUpdateCallable: TypeAlias = Callable[
    [str, str, dict[str, Any], dict[str, Any] | None],
    bool,
]
_SchemaFieldDeleteCallable: TypeAlias = Callable[[str, str], bool]
_SchemaCallable: TypeAlias = Callable[[str], dict[str, object]]
_EntityTypesCallable: TypeAlias = Callable[[], dict[str, object]]

if TYPE_CHECKING:
    _STATIC_CLIENT = cast('Shotgun', object())
    _ENTITY_TYPES: _EntityTypesCallable = _STATIC_CLIENT.entity_types
    _SCHEMA: _SchemaCallable = _STATIC_CLIENT.schema
    _SCHEMA_ENTITY_READ: _SchemaEntityReadCallable = _STATIC_CLIENT.schema_entity_read
    _SCHEMA_FIELD_CREATE: _SchemaFieldCreateCallable = _STATIC_CLIENT.schema_field_create
    _SCHEMA_FIELD_DELETE: _SchemaFieldDeleteCallable = _STATIC_CLIENT.schema_field_delete
    _SCHEMA_FIELD_READ: _SchemaFieldReadCallable = _STATIC_CLIENT.schema_field_read
    _SCHEMA_FIELD_UPDATE: _SchemaFieldUpdateCallable = _STATIC_CLIENT.schema_field_update
    _SCHEMA_READ: _SchemaReadCallable = _STATIC_CLIENT.schema_read


def _params(sg: Shotgun, index: int = 0) -> dict[str, object]:
    params = cast('list[object]', _sent_payload(sg, index)['params'])
    return cast('dict[str, object]', params[1])


def _run_result_contract_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'schema_read':
        return sg.schema_read()
    if method_name == 'schema_entity_read':
        return sg.schema_entity_read()
    if method_name == 'schema_field_read':
        return sg.schema_field_read('Shot')
    if method_name == 'schema_field_create':
        return sg.schema_field_create('Shot', 'text', 'Label')
    if method_name == 'schema_field_update':
        return sg.schema_field_update('Shot', 'sg_label', {'name': 'Label'})
    return sg.schema_field_delete('Shot', 'sg_label')


def test_alias_warning_context_is_thread_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias_events: list[ObserverEvent] = []
    direct_events: list[ObserverEvent] = []
    errors: list[AssertionError | RuntimeError | _errors.ShotgunError] = []
    caught_warnings: list[warnings.WarningMessage] = []
    delegate_entered = threading.Event()
    release_delegate = threading.Event()
    alias_sg = _fake_client([_make_response({'Alias': {}})])
    direct_sg = _fake_client([_make_response({'Direct': {}})])
    original = alias_sg.schema_entity_read
    alias_sg.config.on_compatibility_warning = alias_events.append
    direct_sg.config.on_compatibility_warning = direct_events.append

    def blocked_delegate() -> dict[str, object]:
        delegate_entered.set()
        assert release_delegate.wait(timeout=5)
        return original()

    def run_alias() -> None:
        try:
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter('always', DeprecationWarning)
                assert alias_sg.entity_types() == {'Alias': {}}
                caught_warnings.extend(recorded)
        except (AssertionError, RuntimeError, _errors.ShotgunError) as exc:
            errors.append(exc)

    monkeypatch.setattr(alias_sg, 'schema_entity_read', blocked_delegate)
    thread = threading.Thread(target=run_alias, daemon=True)
    thread.start()
    assert delegate_entered.wait(timeout=5)

    assert direct_sg.schema_entity_read() == {'Direct': {}}
    release_delegate.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert [warning.category for warning in caught_warnings] == [DeprecationWarning]
    assert direct_events == []
    assert alias_events == [
        CompatibilityWarningEvent(
            code='deprecated-entity-types-alias',
            message='The entity_types alias is deprecated.',
        )
    ]


def test_alias_warning_is_consumed_before_observer_reentry() -> None:
    events: list[ObserverEvent] = []
    sg = _fake_client([_make_response({'nested': {}}), _make_response({'outer': {}})])

    def reenter(event: ObserverEvent) -> None:
        events.append(event)
        assert sg.schema_field_read('Asset') == {'nested': {}}

    sg.config.on_compatibility_warning = reenter

    with pytest.warns(
        DeprecationWarning,
        match=r'^schema\(\) is deprecated; use schema_field_read\(\)$',
    ):
        assert sg.schema('Shot') == {'outer': {}}

    assert events == [
        CompatibilityWarningEvent(
            code='deprecated-schema-alias',
            message='The schema alias is deprecated.',
        )
    ]


def test_alias_warning_is_consumed_only_by_originating_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias_events: list[ObserverEvent] = []
    direct_events: list[ObserverEvent] = []
    alias_sg = _fake_client([_make_response({'Alias': {}})])
    direct_sg = _fake_client([_make_response({'Direct': {}})])
    original = alias_sg.schema_entity_read
    alias_sg.config.on_compatibility_warning = alias_events.append
    direct_sg.config.on_compatibility_warning = direct_events.append

    def interleaved_delegate() -> dict[str, object]:
        assert direct_sg.schema_entity_read() == {'Direct': {}}
        return original()

    monkeypatch.setattr(alias_sg, 'schema_entity_read', interleaved_delegate)

    with pytest.warns(
        DeprecationWarning,
        match=r'^entity_types\(\) is deprecated; use schema_entity_read\(\)$',
    ):
        assert alias_sg.entity_types() == {'Alias': {}}

    assert direct_events == []
    assert alias_events == [
        CompatibilityWarningEvent(
            code='deprecated-entity-types-alias',
            message='The entity_types alias is deprecated.',
        )
    ]


def test_alias_warning_scope_cleans_up_when_public_delegate_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ObserverEvent] = []
    sg = _fake_client([_make_response({'Shot': {}})])
    original = sg.schema_entity_read
    sg.config.on_compatibility_warning = events.append

    def fail() -> dict[str, object]:
        raise RuntimeError('delegate failed')

    monkeypatch.setattr(sg, 'schema_entity_read', fail)
    with (
        pytest.warns(
            DeprecationWarning,
            match=r'^entity_types\(\) is deprecated; use schema_entity_read\(\)$',
        ),
        pytest.raises(RuntimeError, match=r'^delegate failed$'),
    ):
        sg.entity_types()
    monkeypatch.setattr(sg, 'schema_entity_read', original)

    assert sg.schema_entity_read() == {'Shot': {}}
    assert events == []


@pytest.mark.parametrize('alias_name', ['schema', 'entity_types'])
def test_deprecated_alias_observer_failures_are_non_invasive_and_sanitized(
    alias_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(_event: ObserverEvent) -> None:
        raise RuntimeError('observer-secret')

    sg = _fake_client([_make_response({'result': 'ok'})])
    sg.config.on_compatibility_warning = fail
    alias = getattr(sg, alias_name)
    warning_message = (
        'schema() is deprecated; use schema_field_read()'
        if alias_name == 'schema'
        else 'entity_types() is deprecated; use schema_entity_read()'
    )

    with (
        caplog.at_level(logging.WARNING, logger='shotgun_api3'),
        pytest.warns(DeprecationWarning, match=rf'^{re.escape(warning_message)}$'),
    ):
        result = alias('Shot') if alias_name == 'schema' else alias()

    assert result == {'result': 'ok'}
    assert 'observer-secret' not in caplog.text


@pytest.mark.parametrize(
    ('alias_name', 'code', 'message'),
    [
        ('schema', 'deprecated-schema-alias', 'schema() is deprecated; use schema_field_read()'),
        (
            'entity_types',
            'deprecated-entity-types-alias',
            'entity_types() is deprecated; use schema_entity_read()',
        ),
    ],
)
def test_deprecated_aliases_warn_and_emit_redacted_structured_events(
    alias_name: str,
    code: str,
    message: str,
) -> None:
    events: list[ObserverEvent] = []
    response: dict[str, object] = {'caller-secret-result': {}}
    sg = _fake_client([_make_response(response)])
    sg.config.on_compatibility_warning = events.append
    alias = getattr(sg, alias_name)

    with pytest.warns(DeprecationWarning, match=rf'^{re.escape(message)}$'):
        result = alias('caller-secret-type') if alias_name == 'schema' else alias()

    assert result == response
    assert events == [
        CompatibilityWarningEvent(
            code=cast('Any', code),
            message=(
                'The schema alias is deprecated.'
                if alias_name == 'schema'
                else 'The entity_types alias is deprecated.'
            ),
        )
    ]
    serialized = repr(events)
    for secret in (
        'caller-secret-type',
        'caller-secret-result',
        'script_name',
        'script_key',
        'https://',
    ):
        assert secret not in serialized


def test_entity_types_alias_calls_public_target_with_ordinary_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ObserverEvent] = []
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    sg = _fake_client([_make_response({'Shot': {}})])
    original = sg.schema_entity_read
    sg.config.on_compatibility_warning = events.append

    def spy(*args: object, **kwargs: object) -> dict[str, object]:
        calls.append((args, kwargs))
        return original()

    monkeypatch.setattr(sg, 'schema_entity_read', spy)

    with pytest.warns(
        DeprecationWarning,
        match=r'^entity_types\(\) is deprecated; use schema_entity_read\(\)$',
    ):
        result = sg.entity_types()

    assert result == {'Shot': {}}
    assert calls == [((), {})]
    assert events == [
        CompatibilityWarningEvent(
            code='deprecated-entity-types-alias',
            message='The entity_types alias is deprecated.',
        )
    ]


def test_schema_alias_calls_public_target_with_ordinary_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ObserverEvent] = []
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    sg = _fake_client([_make_response({'code': {}})])
    original = sg.schema_field_read
    sg.config.on_compatibility_warning = events.append

    def spy(
        entity_type: str,
        *args: object,
        **kwargs: object,
    ) -> dict[str, object]:
        calls.append((entity_type, args, kwargs))
        return original(entity_type)

    monkeypatch.setattr(sg, 'schema_field_read', spy)

    with pytest.warns(
        DeprecationWarning,
        match=r'^schema\(\) is deprecated; use schema_field_read\(\)$',
    ):
        result = sg.schema('Shot')

    assert result == {'code': {}}
    assert calls == [('Shot', (), {})]
    assert events == [
        CompatibilityWarningEvent(
            code='deprecated-schema-alias',
            message='The schema alias is deprecated.',
        )
    ]


def test_schema_field_create_builds_properties_without_mutation() -> None:
    properties = {'description': {'nested': ['original']}, 'visible': True}
    original = copy.deepcopy(properties)
    sg = _fake_client([_make_response('sg_new_field')])

    name = sg.schema_field_create('Shot', 'text', 'New Field', properties)

    assert name == 'sg_new_field'
    assert _sent_payload(sg)['method_name'] == 'schema_field_create'
    assert _params(sg) == {
        'type': 'Shot',
        'data_type': 'text',
        'properties': [
            {'property_name': 'name', 'value': 'New Field'},
            {'property_name': 'description', 'value': {'nested': ['original']}},
            {'property_name': 'visible', 'value': True},
        ],
    }
    assert properties == original


def test_schema_field_delete_builds_payload() -> None:
    sg = _fake_client([_make_response(obj=True)])

    assert sg.schema_field_delete('Shot', 'sg_x') is True
    assert _sent_payload(sg)['method_name'] == 'schema_field_delete'
    assert _params(sg) == {'type': 'Shot', 'field_name': 'sg_x'}


def test_schema_field_properties_are_detached_before_readiness() -> None:
    nested = {'tags': ['original']}
    properties = {'description': nested}
    sg = _fake_client([_make_response('sg_new_field')])

    def mutate_after_readiness(event: ObserverEvent) -> None:
        if isinstance(event, ResponseEvent):
            nested['tags'][0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.schema_field_create('Shot', 'text', 'Field', properties)

    assert nested['tags'] == ['changed']
    assert _params(sg)['properties'] == [
        {'property_name': 'name', 'value': 'Field'},
        {'property_name': 'description', 'value': {'tags': ['original']}},
    ]


def test_schema_field_read_builds_optional_payloads() -> None:
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    sg = _fake_client([_make_response({}), _make_response({})])

    sg.schema_field_read('Shot', 'code', project)
    sg.schema_field_read('Asset')

    assert _sent_payload(sg)['method_name'] == 'schema_field_read'
    assert _params(sg) == {
        'type': 'Shot',
        'field_name': 'code',
        'project': {'type': 'Project', 'id': 2, 'name': 'P'},
    }
    assert _params(sg, 1) == {'type': 'Asset'}


def test_schema_field_update_builds_properties_and_project() -> None:
    properties = {'name': 'X'}
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    sg = _fake_client([_make_response(obj=True)])

    result = sg.schema_field_update('Shot', 'sg_x', properties, project)

    assert result is True
    assert _sent_payload(sg)['method_name'] == 'schema_field_update'
    assert _params(sg) == {
        'type': 'Shot',
        'field_name': 'sg_x',
        'properties': [{'property_name': 'name', 'value': 'X'}],
        'project': {'type': 'Project', 'id': 2, 'name': 'P'},
    }


@pytest.mark.parametrize(
    ('method_name', 'parameter_names', 'defaults', 'annotations', 'return_annotation'),
    [
        (
            'schema_read',
            ('self', 'project_entity'),
            (inspect.Parameter.empty, None),
            (inspect.Parameter.empty, dict[str, Any] | None),
            dict[str, object],
        ),
        (
            'schema_entity_read',
            ('self', 'project_entity'),
            (inspect.Parameter.empty, None),
            (inspect.Parameter.empty, dict[str, Any] | None),
            dict[str, object],
        ),
        (
            'schema_field_read',
            ('self', 'entity_type', 'field_name', 'project_entity'),
            (inspect.Parameter.empty, inspect.Parameter.empty, None, None),
            (
                inspect.Parameter.empty,
                str,
                str | None,
                dict[str, Any] | None,
            ),
            dict[str, object],
        ),
        (
            'schema_field_create',
            ('self', 'entity_type', 'data_type', 'display_name', 'properties'),
            (
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                None,
            ),
            (
                inspect.Parameter.empty,
                str,
                str,
                str,
                dict[str, Any] | None,
            ),
            str,
        ),
        (
            'schema_field_update',
            ('self', 'entity_type', 'field_name', 'properties', 'project_entity'),
            (
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                None,
            ),
            (
                inspect.Parameter.empty,
                str,
                str,
                dict[str, Any],
                dict[str, Any] | None,
            ),
            bool,
        ),
        (
            'schema_field_delete',
            ('self', 'entity_type', 'field_name'),
            (inspect.Parameter.empty, inspect.Parameter.empty, inspect.Parameter.empty),
            (inspect.Parameter.empty, str, str),
            bool,
        ),
        (
            'schema',
            ('self', 'entity_type'),
            (inspect.Parameter.empty, inspect.Parameter.empty),
            (inspect.Parameter.empty, str),
            dict[str, object],
        ),
        (
            'entity_types',
            ('self',),
            (inspect.Parameter.empty,),
            (inspect.Parameter.empty,),
            dict[str, object],
        ),
    ],
)
def test_schema_methods_expose_exact_signatures(
    method_name: str,
    parameter_names: tuple[str, ...],
    defaults: tuple[object, ...],
    annotations: tuple[object, ...],
    return_annotation: object,
) -> None:
    method = getattr(Shotgun, method_name)
    signature = inspect.signature(method)

    assert tuple(signature.parameters) == parameter_names
    assert tuple(parameter.default for parameter in signature.parameters.values()) == defaults
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in signature.parameters.values()
    )
    assert (
        tuple(parameter.annotation for parameter in signature.parameters.values()) == annotations
    )
    assert signature.return_annotation == return_annotation


def test_schema_methods_forward_values_without_extra_runtime_validation() -> None:
    properties = {1: {'nested': ['original']}}
    sg = _fake_client(
        [
            _make_response({}),
            _make_response({}),
            _make_response('sg_field'),
            _make_response(obj=True),
            _make_response(obj=True),
        ]
    )
    schema_read = cast('Callable[[object], object]', sg.schema_read)
    schema_field_read = cast('Callable[[object, object, object], object]', sg.schema_field_read)
    schema_field_create = cast(
        'Callable[[object, object, object, object], object]', sg.schema_field_create
    )
    schema_field_update = cast(
        'Callable[[object, object, object, object], object]', sg.schema_field_update
    )
    schema_field_delete = cast('Callable[[object, object], object]', sg.schema_field_delete)

    schema_read([])
    schema_field_read('', '', [])
    schema_field_create('', '', '', properties)
    schema_field_update('', '', properties, [])
    schema_field_delete('', '')

    assert _params(sg) == {'project': []}
    assert _params(sg, 1) == {'type': '', 'field_name': '', 'project': []}
    assert _params(sg, 2) == {
        'type': '',
        'data_type': '',
        'properties': [
            {'property_name': 'name', 'value': ''},
            {'property_name': 1, 'value': {'nested': ['original']}},
        ],
    }
    assert _params(sg, 3) == {
        'type': '',
        'field_name': '',
        'properties': [
            {'property_name': 1, 'value': {'nested': ['original']}},
        ],
        'project': [],
    }
    assert _params(sg, 4) == {'type': '', 'field_name': ''}


@pytest.mark.parametrize(
    ('method_name', 'expected_message'),
    [
        ('schema_read', 'schema_read did not return a dictionary'),
        ('schema_entity_read', 'schema_entity_read did not return a dictionary'),
        ('schema_field_read', 'schema_field_read did not return a dictionary'),
        ('schema_field_create', 'schema_field_create did not return a string'),
        ('schema_field_update', 'schema_field_update did not return a boolean'),
        ('schema_field_delete', 'schema_field_delete did not return a boolean'),
    ],
)
def test_schema_methods_validate_result_types(method_name: str, expected_message: str) -> None:
    wrong_result: object = [] if method_name != 'schema_field_create' else {}
    sg = _fake_client([_make_response(wrong_result)])

    with pytest.raises(_errors.ShotgunError, match=f'^{expected_message}$'):
        _run_result_contract_case(sg, method_name)


def test_schema_project_is_detached_before_readiness() -> None:
    project = {'type': 'Project', 'id': 2, 'name': ['original']}
    sg = _fake_client([_make_response({})])

    def mutate_after_readiness(event: ObserverEvent) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', project['name'])[0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.schema_read(project)

    assert project['name'] == ['changed']
    assert _params(sg)['project'] == {
        'type': 'Project',
        'id': 2,
        'name': ['original'],
    }


@pytest.mark.parametrize('method_name', ['schema_read', 'schema_entity_read'])
def test_schema_read_methods_build_project_payloads(method_name: str) -> None:
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    original = project.copy()
    sg = _fake_client([_make_response({'Shot': {}}), _make_response({'Shot': {}})])
    method = cast('Callable[[dict[str, Any] | None], dict[str, object]]', getattr(sg, method_name))

    method(project)
    method(None)

    assert _sent_payload(sg)['method_name'] == method_name
    assert _params(sg) == {'project': {'type': 'Project', 'id': 2, 'name': 'P'}}
    assert cast('list[object]', _sent_payload(sg, 1)['params']) == [
        {'script_name': 's', 'script_key': 'k'}
    ]
    assert project == original
