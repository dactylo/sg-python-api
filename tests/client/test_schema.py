import copy
import inspect
import re
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Never, TypeAlias, cast

import pytest

from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ResponseEvent, _ObserverEvent
from tests.client._payloads import rpc_params
from tests.conftest import fake_client, fake_transport, make_response, sent_payload

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
_SchemaCallable: TypeAlias = Callable[[str], Never]
_EntityTypesCallable: TypeAlias = Callable[[], Never]

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


@pytest.mark.parametrize(
    ('method_name', 'args', 'message'),
    [
        ('entity_types', (), 'Deprecated: use schema_entity_read() instead'),
        ('schema', ('Shot',), "Deprecated: use schema_field_read('Shot') instead"),
        ('schema', (7,), "Deprecated: use schema_field_read('7') instead"),
    ],
)
def test_deprecated_aliases_raise_before_io(
    method_name: str,
    args: tuple[object, ...],
    message: str,
) -> None:
    events: list[_ObserverEvent] = []
    sg = fake_client([])
    sg.config.on_compatibility_warning = events.append
    alias = cast('Callable[..., object]', getattr(sg, method_name))

    with warnings.catch_warnings():
        warnings.simplefilter('error')
        with pytest.raises(_errors.ShotgunError, match=f'^{re.escape(message)}$') as raised:
            alias(*args)

    assert type(raised.value) is _errors.ShotgunError
    assert events == []
    assert not fake_transport(sg).requests


def test_schema_field_create_builds_properties_without_mutation() -> None:
    properties = {'description': {'nested': ['original']}, 'visible': True}
    original = copy.deepcopy(properties)
    sg = fake_client([make_response('sg_new_field')])

    name = sg.schema_field_create('Shot', 'text', 'New Field', properties)

    assert name == 'sg_new_field'
    assert sent_payload(sg)['method_name'] == 'schema_field_create'
    assert rpc_params(sg) == {
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
    sg = fake_client([make_response(obj=True)])

    assert sg.schema_field_delete('Shot', 'sg_x') is True
    assert sent_payload(sg)['method_name'] == 'schema_field_delete'
    assert rpc_params(sg) == {'type': 'Shot', 'field_name': 'sg_x'}


def test_schema_field_properties_are_detached_before_readiness() -> None:
    nested = {'tags': ['original']}
    properties = {'description': nested}
    sg = fake_client([make_response('sg_new_field')])

    def mutate_after_readiness(event: _ObserverEvent) -> None:
        if isinstance(event, ResponseEvent):
            nested['tags'][0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.schema_field_create('Shot', 'text', 'Field', properties)

    assert nested['tags'] == ['changed']
    assert rpc_params(sg)['properties'] == [
        {'property_name': 'name', 'value': 'Field'},
        {'property_name': 'description', 'value': {'tags': ['original']}},
    ]


def test_schema_field_read_builds_optional_payloads() -> None:
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    sg = fake_client([make_response({}), make_response({})])

    sg.schema_field_read('Shot', 'code', project)
    sg.schema_field_read('Asset')

    assert sent_payload(sg)['method_name'] == 'schema_field_read'
    assert rpc_params(sg) == {
        'type': 'Shot',
        'field_name': 'code',
        'project': {'type': 'Project', 'id': 2, 'name': 'P'},
    }
    assert rpc_params(sg, 1) == {'type': 'Asset'}


def test_schema_field_update_builds_properties_and_project() -> None:
    properties = {'name': 'X'}
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    sg = fake_client([make_response(obj=True)])

    result = sg.schema_field_update('Shot', 'sg_x', properties, project)

    assert result is True
    assert sent_payload(sg)['method_name'] == 'schema_field_update'
    assert rpc_params(sg) == {
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
            Never,
        ),
        (
            'entity_types',
            ('self',),
            (inspect.Parameter.empty,),
            (inspect.Parameter.empty,),
            Never,
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
    sg = fake_client(
        [
            make_response({}),
            make_response({}),
            make_response('sg_field'),
            make_response(obj=True),
            make_response(obj=True),
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

    assert rpc_params(sg) == {'project': []}
    assert rpc_params(sg, 1) == {'type': '', 'field_name': '', 'project': []}
    assert rpc_params(sg, 2) == {
        'type': '',
        'data_type': '',
        'properties': [
            {'property_name': 'name', 'value': ''},
            {'property_name': 1, 'value': {'nested': ['original']}},
        ],
    }
    assert rpc_params(sg, 3) == {
        'type': '',
        'field_name': '',
        'properties': [
            {'property_name': 1, 'value': {'nested': ['original']}},
        ],
        'project': [],
    }
    assert rpc_params(sg, 4) == {'type': '', 'field_name': ''}


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
    sg = fake_client([make_response(wrong_result)])

    with pytest.raises(_errors.ShotgunError, match=f'^{expected_message}$'):
        _run_result_contract_case(sg, method_name)


def test_schema_project_is_detached_before_readiness() -> None:
    project = {'type': 'Project', 'id': 2, 'name': ['original']}
    sg = fake_client([make_response({})])

    def mutate_after_readiness(event: _ObserverEvent) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', project['name'])[0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.schema_read(project)

    assert project['name'] == ['changed']
    assert rpc_params(sg)['project'] == {
        'type': 'Project',
        'id': 2,
        'name': ['original'],
    }


@pytest.mark.parametrize('method_name', ['schema_read', 'schema_entity_read'])
def test_schema_read_methods_build_project_payloads(method_name: str) -> None:
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_secret': 'omit'}
    original = project.copy()
    sg = fake_client([make_response({'Shot': {}}), make_response({'Shot': {}})])
    method = cast('Callable[[dict[str, Any] | None], dict[str, object]]', getattr(sg, method_name))

    method(project)
    method(None)

    assert sent_payload(sg)['method_name'] == method_name
    assert rpc_params(sg) == {'project': {'type': 'Project', 'id': 2, 'name': 'P'}}
    assert cast('list[object]', sent_payload(sg, 1)['params']) == [
        {'script_name': 's', 'script_key': 'k'}
    ]
    assert project == original
