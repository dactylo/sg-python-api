import copy
import inspect
import json
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

import shotgun_api3.shotgun as shotgun_module
from shotgun_api3 import _crud, _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ResponseEvent
from shotgun_api3._transport import TransportResponse
from tests._pyright import run_pyright
from tests._transports import _FakeTransport
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

if TYPE_CHECKING:
    from collections.abc import Callable


def _page(ids: list[int], count: int) -> dict[str, object]:
    return {
        'entities': [{'type': 'Shot', 'id': entity_id} for entity_id in ids],
        'paging_info': {'entity_count': count},
    }


def _params(sg: Shotgun, index: int = 0) -> dict[str, object]:
    payload = _sent_payload(sg, index)
    params = cast('list[object]', payload['params'])
    return cast('dict[str, object]', params[1])


def _run_result_contract_case(sg: Shotgun, operation: str) -> object:
    if operation == 'create':
        return sg.create('Shot', {})
    if operation == 'update':
        return sg.update('Shot', 1, {'code': 'x'})
    if operation == 'delete':
        return sg.delete('Shot', 1)
    if operation == 'revive':
        return sg.revive('Shot', 1)
    return sg.batch([{'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': 1}])


def test_batch_builds_exact_calls_without_mutating_requests() -> None:
    requests: list[dict[str, Any]] = [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {'code': 'A', 'project': {'type': 'Project', 'id': 2, 'name': 'P'}},
            'return_fields': ['code'],
        },
        {
            'request_type': 'update',
            'entity_type': 'Shot',
            'entity_id': 9,
            'data': {
                'assets': [
                    {'type': 'Asset', 'id': 3, 'name': 'A', 'sg_asset_type': 'Model'},
                ]
            },
            'multi_entity_update_modes': {'assets': 'add'},
        },
        {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': 10},
    ]
    original = copy.deepcopy(requests)
    response = [{'type': 'Shot', 'id': 9}, {'type': 'Shot', 'id': 9}, True]
    sg = _fake_client([_make_response(response)])

    assert sg.batch(requests) == response

    assert requests == original
    assert _sent_payload(sg)['method_name'] == 'batch'
    assert cast('list[object]', _sent_payload(sg)['params'])[1] == [
        {
            'request_type': 'create',
            'type': 'Shot',
            'fields': [
                {'field_name': 'code', 'value': 'A'},
                {
                    'field_name': 'project',
                    'value': {'type': 'Project', 'id': 2, 'name': 'P'},
                },
            ],
            'return_fields': ['code'],
        },
        {
            'request_type': 'update',
            'type': 'Shot',
            'id': 9,
            'fields': [
                {
                    'field_name': 'assets',
                    'value': [{'type': 'Asset', 'id': 3, 'name': 'A'}],
                    'multi_entity_update_mode': 'add',
                }
            ],
        },
        {'request_type': 'delete', 'type': 'Shot', 'id': 10},
    ]


def test_batch_detaches_nested_data_before_readiness() -> None:
    nested = {'tags': ['original']}
    return_fields = ['code']
    requests: list[dict[str, Any]] = [
        {
            'request_type': 'create',
            'entity_type': 'Shot',
            'data': {'metadata': nested},
            'return_fields': return_fields,
        }
    ]
    sg = _fake_client([_make_response([{'type': 'Shot', 'id': 1}])])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            nested['tags'][0] = 'changed'
            return_fields[0] = 'id'

    sg.config.on_response = mutate_after_readiness

    sg.batch(requests)

    assert cast('list[object]', _sent_payload(sg)['params'])[1] == [
        {
            'request_type': 'create',
            'type': 'Shot',
            'fields': [{'field_name': 'metadata', 'value': {'tags': ['original']}}],
            'return_fields': ['code'],
        }
    ]
    assert nested == {'tags': ['changed']}
    assert return_fields == ['id']


def test_batch_empty_requests_are_a_noop_without_readiness() -> None:
    sg = _fake_client([])

    assert sg.batch([]) == []
    assert _fake_transport(sg).requests == []


def test_batch_empty_return_fields_use_default_in_one_rpc() -> None:
    sg = _fake_client([_make_response([{'type': 'Shot', 'id': 1}])])

    result = sg.batch(
        [
            {
                'request_type': 'create',
                'entity_type': 'Shot',
                'data': {'code': 'A'},
                'return_fields': [],
            }
        ]
    )

    assert result == [{'type': 'Shot', 'id': 1}]
    assert cast('list[object]', _sent_payload(sg)['params'])[1] == [
        {
            'request_type': 'create',
            'type': 'Shot',
            'fields': [{'field_name': 'code', 'value': 'A'}],
            'return_fields': ['id'],
        }
    ]
    assert len(_fake_transport(sg).requests) == 2


def test_batch_rejects_invalid_request_container_before_readiness() -> None:
    sg = _fake_client([])
    batch = cast('Callable[[object], object]', sg.batch)

    with pytest.raises(_errors.ShotgunError, match=r'^batch requests must be a list$'):
        batch(())

    assert _fake_transport(sg).requests == []


def test_batch_validates_every_member_before_readiness() -> None:
    sg = _fake_client([])
    requests: list[dict[str, Any]] = [
        {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': 1},
        {'request_type': 'update', 'entity_type': 'Shot', 'entity_id': True, 'data': {}},
    ]

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^batch update request 1 entity_id must be an integer$',
    ):
        sg.batch(requests)

    assert _fake_transport(sg).requests == []


def test_batch_validation_does_not_expose_request_values() -> None:
    sensitive_value = 'opaque-sensitive-value'
    sg = _fake_client([])

    with pytest.raises(_errors.ShotgunError) as error:
        sg.batch(
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'data': {'api_key': sensitive_value},
                }
            ]
        )

    assert sensitive_value not in str(error.value)


@pytest.mark.parametrize(
    ('request_value', 'message'),
    [
        ('not-a-dict', 'batch request 0 must be a dictionary'),
        ({'entity_type': 'Shot'}, "batch request 0 is missing 'request_type'"),
        ({'request_type': 'create'}, "batch request 0 is missing 'entity_type'"),
        (
            {'request_type': 'create', 'entity_type': 'Shot'},
            "batch create request 0 is missing 'data'",
        ),
        (
            {'request_type': 'create', 'entity_type': None, 'data': {}},
            'batch request 0 has invalid entity_type',
        ),
        (
            {'request_type': '', 'entity_type': 'Shot', 'data': {}},
            'batch request 0 has invalid request_type',
        ),
        (
            {'request_type': 'create', 'entity_type': 'Shot', 'data': []},
            'batch create request 0 data must be a dictionary',
        ),
        (
            {
                'request_type': 'create',
                'entity_type': 'Shot',
                'data': {},
                'return_fields': 'code',
            },
            'batch create request 0 return_fields must be a list of strings or None',
        ),
        (
            {
                'request_type': 'create',
                'entity_type': 'Shot',
                'data': {},
                'return_fields': [1],
            },
            'batch create request 0 return_fields must be a list of strings or None',
        ),
        (
            {'request_type': 'create', 'entity_type': 'Shot', 'data': {1: 'value'}},
            'batch create request 0 field names must be nonempty strings',
        ),
        (
            {'request_type': 'update', 'entity_type': 'Shot', 'data': {}},
            "batch update request 0 is missing 'entity_id'",
        ),
        (
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': True,
                'data': {},
            },
            'batch update request 0 entity_id must be an integer',
        ),
        (
            {'request_type': 'update', 'entity_type': 'Shot', 'entity_id': 1},
            "batch update request 0 is missing 'data'",
        ),
        (
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': 1,
                'data': {},
                'multi_entity_update_modes': {'assets': 1},
            },
            'batch update request 0 modes must map strings to strings',
        ),
        (
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': 1,
                'data': {},
                'multi_entity_update_modes': {1: 'add'},
            },
            'batch update request 0 modes must map strings to strings',
        ),
        (
            {'request_type': 'delete', 'entity_type': 'Shot'},
            "batch delete request 0 is missing 'entity_id'",
        ),
        (
            {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': '1'},
            'batch delete request 0 entity_id must be an integer',
        ),
        (
            {'request_type': 'revive', 'entity_type': 'Shot', 'entity_id': 1},
            'batch request 0 has invalid request_type',
        ),
    ],
)
def test_batch_validation_is_useful_and_does_not_send(
    request_value: object,
    message: str,
) -> None:
    sg = _fake_client([])
    batch = cast('Callable[[object], object]', sg.batch)

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        batch([request_value])

    assert _fake_transport(sg).requests == []


def test_create_detaches_nested_data_before_readiness() -> None:
    nested = {'tags': ['original']}
    data = {'metadata': nested}
    sg = _fake_client([_make_response({'type': 'Shot', 'id': 1})])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            nested['tags'][0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.create('Shot', data)

    assert cast('list[dict[str, object]]', _params(sg)['fields']) == [
        {'field_name': 'metadata', 'value': {'tags': ['original']}}
    ]
    assert data == {'metadata': {'tags': ['changed']}}


@pytest.mark.parametrize(
    'fields',
    ['code', '', b'code', b'', 0, 1, False, ['code', 1], ('code', None)],
)
def test_create_rejects_invalid_return_fields_before_io(fields: object) -> None:
    sg = _fake_client([])
    create = cast('Callable[[str, dict[str, object], object], object]', sg.create)

    with pytest.raises(_errors.ShotgunError, match=r'^create return_fields must be .*strings.*$'):
        create('Shot', {}, fields)

    assert _fake_transport(sg).requests == []


@pytest.mark.parametrize(
    ('fields_factory', 'expected'),
    [
        (lambda: None, ['id']),
        (list, ['id']),
        (tuple, ['id']),
        (lambda: ['code', ''], ['code', '']),
        (lambda: ('code', ''), ['code', '']),
        (lambda: (field for field in ['code', '']), ['code', '']),
        (lambda: (field for field in ''), []),
    ],
    ids=['none', 'empty-list', 'empty-tuple', 'list', 'tuple', 'iterator', 'empty-iterator'],
)
def test_create_return_fields_preserve_defaults_and_detach_before_readiness(
    fields_factory: 'Callable[[], object]', expected: list[str]
) -> None:
    fields = fields_factory()
    sg = _fake_client([_make_response({'type': 'Shot', 'id': 1})])
    create = cast('Callable[[str, dict[str, object], object], object]', sg.create)

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent) and isinstance(fields, list):
            fields[:] = ['changed']

    sg.config.on_response = mutate_after_readiness

    create('Shot', {}, fields)

    assert _params(sg)['return_fields'] == expected


def test_create_update_delete_and_revive_build_exact_payloads_without_mutation() -> None:
    create_data = {
        'code': 'X',
        'project': {'type': 'Project', 'id': 2, 'name': 'P', 'sg_status': 'Active'},
    }
    update_data = {
        'assets': [
            {'type': 'Asset', 'id': 3, 'name': 'A', 'sg_asset_type': 'Model'},
        ]
    }
    modes = {'assets': 'add'}
    create_original = copy.deepcopy(create_data)
    update_original = copy.deepcopy(update_data)
    modes_original = modes.copy()
    sg = _fake_client(
        [
            _make_response({'type': 'Shot', 'id': 5, 'code': 'X'}),
            _make_response({'type': 'Shot', 'id': 5, 'code': 'Y'}),
            _make_response(obj=True),
            _make_response(obj=True),
        ]
    )

    assert sg.create('Shot', create_data, return_fields=['code'])['id'] == 5
    assert sg.update('Shot', 5, update_data, modes)['code'] == 'Y'
    assert sg.delete('Shot', 5) is True
    assert sg.revive('Shot', 5) is True

    assert create_data == create_original
    assert update_data == update_original
    assert modes == modes_original
    assert _params(sg, 0) == {
        'type': 'Shot',
        'fields': [
            {'field_name': 'code', 'value': 'X'},
            {
                'field_name': 'project',
                'value': {'type': 'Project', 'id': 2, 'name': 'P'},
            },
        ],
        'return_fields': ['code'],
    }
    assert _params(sg, 1) == {
        'type': 'Shot',
        'id': 5,
        'fields': [
            {
                'field_name': 'assets',
                'value': [{'type': 'Asset', 'id': 3, 'name': 'A'}],
                'multi_entity_update_mode': 'add',
            }
        ],
    }
    assert _params(sg, 2) == {'type': 'Shot', 'id': 5}
    assert _params(sg, 3) == {'type': 'Shot', 'id': 5}


@pytest.mark.parametrize(
    ('operation', 'result', 'message'),
    [
        ('create', ['sensitive-create-result'], 'create did not return a dictionary'),
        ('update', 'sensitive-update-result', 'update did not return a dictionary'),
        ('delete', 1, 'delete did not return a boolean'),
        ('revive', 0, 'revive did not return a boolean'),
        ('batch', {'value': 'sensitive-batch-result'}, 'batch did not return a list'),
    ],
)
def test_crud_and_batch_reject_invalid_result_shapes_without_values(
    operation: str,
    result: object,
    message: str,
) -> None:
    sg = _fake_client([_make_response(result)])

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$') as error:
        _run_result_contract_case(sg, operation)

    assert 'sensitive-' not in str(error.value)


def test_crud_projection_reads_canonical_runtime_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entity = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_status': 'Active'}
    sg = _fake_client(
        [
            _make_response({'type': 'Shot', 'id': 5}),
            _make_response({'type': 'Shot', 'id': 5}),
        ]
    )

    monkeypatch.setattr(shotgun_module, 'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION', True)
    sg.create('Shot', {'project': entity})
    monkeypatch.setattr(shotgun_module, 'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION', False)
    sg.update('Shot', 5, {'project': entity})

    assert cast('list[dict[str, object]]', _params(sg, 0)['fields']) == [
        {'field_name': 'project', 'value': entity}
    ]
    assert cast('list[dict[str, object]]', _params(sg, 1)['fields']) == [
        {'field_name': 'project', 'value': {'type': 'Project', 'id': 2, 'name': 'P'}}
    ]


def test_crud_rejects_invalid_data_before_readiness() -> None:
    sg = _fake_client([])
    create = cast('Callable[[str, object], object]', sg.create)
    update = cast('Callable[[str, int, object], object]', sg.update)

    with pytest.raises(_errors.ShotgunError, match='create data must be a dictionary'):
        create('Shot', [])
    with pytest.raises(_errors.ShotgunError, match='update data must be a dictionary'):
        update('Shot', 1, [])

    assert _fake_transport(sg).requests == []


def test_crud_validates_field_names_and_update_modes_before_readiness() -> None:
    create_sg = _fake_client([])
    update_fields_sg = _fake_client([])
    update_sg = _fake_client([])
    create = cast('Callable[[str, object], object]', create_sg.create)
    update_fields = cast('Callable[..., object]', update_fields_sg.update)
    update = cast('Callable[..., object]', update_sg.update)

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^create field names must be nonempty strings$',
    ):
        create('Shot', {1: 'value'})
    with pytest.raises(
        _errors.ShotgunError,
        match=r'^update field names must be nonempty strings$',
    ):
        update_fields('Shot', 1, {1: 'value'})
    with pytest.raises(
        _errors.ShotgunError,
        match=r'^multi_entity_update_modes must map strings to strings$',
    ):
        update('Shot', 1, {}, {'assets': 1})

    assert _fake_transport(create_sg).requests == []
    assert _fake_transport(update_fields_sg).requests == []
    assert _fake_transport(update_sg).requests == []


def test_find_and_find_one_expose_exact_signatures_and_binding_identity() -> None:
    find_signature = inspect.signature(Shotgun.find)
    find_one_signature = inspect.signature(Shotgun.find_one)

    assert tuple(find_signature.parameters) == (
        'self',
        'entity_type',
        'filters',
        'fields',
        'order',
        'filter_operator',
        'limit',
        'retired_only',
        'page',
        'include_archived_projects',
        'additional_filter_presets',
    )
    assert tuple(parameter.default for parameter in find_signature.parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
        None,
        None,
        0,
        False,
        0,
        True,
        None,
    )
    assert tuple(find_one_signature.parameters) == (
        'self',
        'entity_type',
        'filters',
        'fields',
        'order',
        'filter_operator',
        'retired_only',
        'include_archived_projects',
        'additional_filter_presets',
    )
    assert tuple(parameter.default for parameter in find_one_signature.parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
        None,
        None,
        False,
        True,
        None,
    )

    sg = _fake_client([])
    find = cast('Callable[..., object]', sg.find)
    find_one = cast('Callable[..., object]', sg.find_one)
    with pytest.raises(TypeError, match=r'^Shotgun\.find\(\)'):
        find('Shot')
    with pytest.raises(TypeError, match=r'^Shotgun\.find_one\(\)'):
        find_one(unexpected=True)


def test_find_builds_exact_read_payload_without_mutating_inputs() -> None:
    filters = [['sg_status_list', 'is', 'ip']]
    fields = ['code']
    order = [
        {'field_name': 'code', 'direction': 'desc'},
        {'field_name': 'id'},
    ]
    presets: list[dict[str, object]] = []
    original = copy.deepcopy((filters, fields, order, presets))
    sg = _fake_client([_make_response(_page([], 0))])

    assert (
        sg.find(
            'Shot',
            filters,
            fields=fields,
            order=order,
            filter_operator='any',
            limit=10,
            retired_only=True,
            include_archived_projects=False,
            additional_filter_presets=presets,
        )
        == []
    )

    assert (filters, fields, order, presets) == original
    assert _params(sg) == {
        'type': 'Shot',
        'return_fields': ['code'],
        'filters': {
            'logical_operator': 'or',
            'conditions': [{'path': 'sg_status_list', 'relation': 'is', 'values': ['ip']}],
        },
        'return_only': 'retired',
        'paging': {'entities_per_page': 10, 'current_page': 1},
        'api_return_image_urls': True,
        'sorts': [
            {'field_name': 'code', 'direction': 'desc'},
            {'field_name': 'id', 'direction': 'asc'},
        ],
        'include_archived_projects': False,
        'additional_filter_presets': [],
    }


def test_find_defaults_to_id_and_omits_optional_read_keys() -> None:
    sg = _fake_client([_make_response(_page([], 0))])

    assert sg.find('Shot', [], fields=[]) == []

    assert _params(sg) == {
        'type': 'Shot',
        'return_fields': ['id'],
        'filters': {'logical_operator': 'and', 'conditions': []},
        'return_only': 'active',
        'paging': {'entities_per_page': 500, 'current_page': 1},
        'api_return_image_urls': True,
    }


def test_find_detaches_all_mutable_inputs_before_readiness() -> None:
    entity_type = ['original-type']
    filter_value = {'nested': ['original-filter']}
    filters = [['metadata', 'is', filter_value]]
    fields = [['original-field']]
    order = [{'field_name': 'code', 'direction': 'asc'}]
    preset_value = {'nested': ['original-preset']}
    presets = [{'preset_name': 'LATEST', 'values': [preset_value]}]
    sg = _fake_client([_make_response(_page([], 0))])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            entity_type[0] = 'changed-type'
            filter_value['nested'][0] = 'changed-filter'
            fields[0][0] = 'changed-field'
            order[0]['field_name'] = 'id'
            preset_value['nested'][0] = 'changed-preset'

    sg.config.on_response = mutate_after_readiness

    find = cast('Callable[..., object]', sg.find)
    find(
        entity_type,
        filters,
        fields=fields,
        order=order,
        additional_filter_presets=presets,
    )

    assert _params(sg) == {
        'type': ['original-type'],
        'return_fields': [['original-field']],
        'filters': {
            'logical_operator': 'and',
            'conditions': [
                {
                    'path': 'metadata',
                    'relation': 'is',
                    'values': [{'nested': ['original-filter']}],
                }
            ],
        },
        'return_only': 'active',
        'paging': {'entities_per_page': 500, 'current_page': 1},
        'api_return_image_urls': True,
        'sorts': [{'field_name': 'code', 'direction': 'asc'}],
        'additional_filter_presets': [
            {
                'preset_name': 'LATEST',
                'values': [{'nested': ['original-preset']}],
            }
        ],
    }


def test_find_explicit_page_fetches_one_page_without_result_truncation() -> None:
    sg = _fake_client([_make_response(_page([7, 8], 900))])

    result = sg.find('Shot', [], limit=1, page=3)

    assert [record['id'] for record in result] == [7, 8]
    assert cast('dict[str, object]', _params(sg)['paging']) == {
        'entities_per_page': 1,
        'current_page': 3,
    }


@pytest.mark.parametrize(
    ('paging_info', 'message'),
    [
        (
            {'entity_count': True},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'entity_count': '1'},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'has_next_page': 'yes'},
            'paging_info has_next_page must be a boolean',
        ),
    ],
)
def test_find_explicit_page_rejects_malformed_active_paging_signal(
    paging_info: dict[str, object],
    message: str,
) -> None:
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': paging_info,
                }
            )
        ]
    )

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        sg.find('Shot', [], page=3)

    assert len(_fake_transport(sg).requests) == 2
    assert sg._lease_counts == {}


def test_find_explicit_page_rejects_non_dictionary_paging_info() -> None:
    sg = _fake_client(
        [_make_response({'entities': [{'type': 'Shot', 'id': 1}], 'paging_info': []})]
    )

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^read did not return a paging_info dictionary$',
    ):
        sg.find('Shot', [], page=3)


@pytest.mark.parametrize(
    'paging_info',
    [
        {'entity_count': 900},
        {'has_next_page': True},
        {'has_next_page': False},
        {},
    ],
)
def test_find_explicit_page_validates_metadata_but_ignores_verdict(
    paging_info: dict[str, object],
) -> None:
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 1}],
                    'paging_info': paging_info,
                }
            )
        ]
    )

    assert sg.find('Shot', [], page=3) == [{'type': 'Shot', 'id': 1}]
    assert len(_fake_transport(sg).requests) == 2
    assert sg._lease_counts == {}


def test_find_injects_platform_local_path_fields() -> None:
    sg = _fake_client([])
    local_path_field = sg.client_caps.local_path_field
    assert local_path_field is not None
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [
                        {
                            'type': 'Shot',
                            'id': 1,
                            'media': {
                                'link_type': 'local',
                                local_path_field: '/show/file.mov',
                            },
                        }
                    ],
                    'paging_info': {'entity_count': 1},
                }
            )
        ]
    )

    result = sg.find('Shot', [])

    media = result[0]['media']
    assert isinstance(media, dict)
    assert media['local_path'] == '/show/file.mov'
    assert media['url'] == 'file:///show/file.mov'


def test_find_limit_stops_and_truncates_after_consuming_page() -> None:
    sg = _fake_client([_make_response(_page([1, 2, 3, 4], 900))])

    result = sg.find('Shot', [], limit=3)

    assert [record['id'] for record in result] == [1, 2, 3]
    assert cast('dict[str, object]', _params(sg)['paging']) == {
        'entities_per_page': 3,
        'current_page': 1,
    }


def test_find_one_returns_first_entity_or_none() -> None:
    sg = _fake_client([_make_response(_page([1], 1))])
    empty = _fake_client([_make_response(_page([], 0))])

    assert sg.find_one('Shot', [['id', 'is', 1]]) == {'type': 'Shot', 'id': 1}
    assert empty.find_one('Shot', [['id', 'is', 99]]) is None
    assert cast('dict[str, object]', _params(sg)['paging']) == {
        'entities_per_page': 1,
        'current_page': 1,
    }


def test_find_paginates_serially_until_count_is_reached() -> None:
    sg = _fake_client(
        [
            _make_response(_page(list(range(1, 501)), 502)),
            _make_response(_page([501, 502], 502)),
        ]
    )

    result = sg.find('Shot', [])

    assert len(result) == 502
    assert cast('dict[str, object]', _params(sg, 0)['paging'])['current_page'] == 1
    assert cast('dict[str, object]', _params(sg, 1)['paging'])['current_page'] == 2


@pytest.mark.parametrize(
    ('paging_info', 'message'),
    [
        (
            {'entity_count': True},
            'paging_info entity_count must be a nonnegative integer',
        ),
        (
            {'has_next_page': 'yes'},
            'paging_info has_next_page must be a boolean',
        ),
    ],
)
def test_find_rejects_malformed_active_paging_signal_without_extra_page(
    paging_info: dict[str, object],
    message: str,
) -> None:
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': index} for index in range(500)],
                    'paging_info': paging_info,
                }
            )
        ]
    )

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        sg.find('Shot', [])

    assert len(_fake_transport(sg).requests) == 2


@pytest.mark.parametrize('paging_info', [None, [], 'invalid'])
def test_find_rejects_non_dictionary_paging_info_without_extra_page(
    paging_info: object,
) -> None:
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': index} for index in range(500)],
                    'paging_info': paging_info,
                }
            )
        ]
    )

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^read did not return a paging_info dictionary$',
    ):
        sg.find('Shot', [])

    assert len(_fake_transport(sg).requests) == 2


def test_find_reuses_detached_presets_across_pages() -> None:
    preset_value = {'nested': ['original']}
    presets = [{'preset_name': 'LATEST', 'values': [preset_value]}]
    sg = _fake_client(
        [
            _make_response(_page(list(range(1, 501)), 501)),
            _make_response(_page([501], 501)),
        ]
    )
    response_count = 0

    def mutate_before_and_between_pages(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            preset_value['nested'][0] = 'changed-before-first-page'
        elif response_count == 2:
            preset_value['nested'][0] = 'changed-between-pages'

    sg.config.on_response = mutate_before_and_between_pages

    sg.find('Shot', [], additional_filter_presets=presets)

    expected = [
        {
            'preset_name': 'LATEST',
            'values': [{'nested': ['original']}],
        }
    ]
    assert _params(sg, 0)['additional_filter_presets'] == expected
    assert _params(sg, 1)['additional_filter_presets'] == expected
    assert preset_value == {'nested': ['changed-between-pages']}


def test_find_treats_none_entity_count_as_absent() -> None:
    terminal = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': index} for index in range(500)],
                    'paging_info': {'entity_count': None, 'has_next_page': False},
                }
            )
        ]
    )
    continuing = _fake_client(
        [
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': index} for index in range(500)],
                    'paging_info': {'entity_count': None, 'has_next_page': True},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 500}],
                    'paging_info': {'entity_count': None, 'has_next_page': False},
                }
            ),
        ]
    )

    assert len(terminal.find('Shot', [])) == 500
    assert len(continuing.find('Shot', [])) == 501
    assert len(_fake_transport(terminal).requests) == 2
    assert len(_fake_transport(continuing).requests) == 3


def test_find_uses_one_immutable_snapshot_for_all_pages() -> None:
    sg = _fake_client(
        [
            _make_response(_page(list(range(1, 501)), 502)),
            _make_response(_page([501, 502], 502)),
        ]
    )
    sg.set_session_uuid('original-session')
    response_count = 0

    def mutate_after_first_page(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 2:
            sg.set_session_uuid('next-operation-session')

    sg.config.on_response = mutate_after_first_page

    sg.find('Shot', [])

    for index in (0, 1):
        params = cast('list[object]', _sent_payload(sg, index)['params'])
        auth = cast('dict[str, object]', params[0])
        assert auth['session_uuid'] == 'original-session'
    assert sg.config.session_uuid == 'next-operation-session'


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    [
        ({'limit': -1}, 'limit must be a nonnegative integer'),
        ({'limit': True}, 'limit must be a nonnegative integer'),
        ({'page': -1}, 'page must be a nonnegative integer'),
        ({'page': True}, 'page must be a nonnegative integer'),
        (
            {'order': [{'field_name': 'code', 'direction': 'sideways'}]},
            "order item 0 has invalid direction 'sideways'",
        ),
    ],
)
def test_find_validates_paging_and_order_before_readiness(
    kwargs: dict[str, object],
    message: str,
) -> None:
    sg = _fake_client([])
    find = cast('Callable[..., object]', sg.find)

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        find('Shot', [], **kwargs)

    assert _fake_transport(sg).requests == []


def test_first_find_explicit_page_uses_readiness_page_limit() -> None:
    sg = _fake_client(
        [_make_response(_page([5, 6], 10))],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )

    assert [entity['id'] for entity in sg.find('Shot', [], page=3)] == [5, 6]
    assert cast('dict[str, object]', _params(sg)['paging']) == {
        'entities_per_page': 2,
        'current_page': 3,
    }


def test_first_find_keeps_the_leased_snapshot_when_readiness_publication_is_rejected() -> None:
    sg = Shotgun('https://x.example', script_name='s', api_key='k', connect=False)
    transport = _FakeTransport(
        [
            TransportResponse(
                status=200,
                reason='OK',
                headers={'content-type': 'application/json'},
                content=(b'{"results":{"version":[9,0,0],"api_max_entities_per_page":2}}'),
                on_close=lambda: sg.config.set_server_params('https://other.example'),
            ),
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': {'has_next_page': True},
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': {'has_next_page': False},
                }
            ),
        ]
    )
    sg._transport = transport

    assert [entity['id'] for entity in sg.find('Shot', [])] == [1, 2, 3]
    assert all(request.url == 'https://x.example:443/api3/json' for request in transport.requests)
    assert cast('dict[str, object]', _params(sg, 0)['paging']) == {
        'entities_per_page': 2,
        'current_page': 1,
    }
    assert sg.config.server_info == {}


@pytest.mark.parametrize(
    'paging_info',
    [
        pytest.param(
            [{'entity_count': 3}, {'entity_count': 3}],
            id='entity-count',
        ),
        pytest.param(
            [{'has_next_page': True}, {'has_next_page': False}],
            id='has-next-page',
        ),
        pytest.param(
            [dict[str, object](), dict[str, object]()],
            id='short-page-fallback',
        ),
    ],
)
def test_first_find_uses_readiness_page_limit(
    paging_info: list[dict[str, object]],
) -> None:
    sg = _fake_client(
        [
            _make_response(
                {
                    'entities': [
                        {'type': 'Shot', 'id': 1},
                        {'type': 'Shot', 'id': 2},
                    ],
                    'paging_info': paging_info[0],
                }
            ),
            _make_response(
                {
                    'entities': [{'type': 'Shot', 'id': 3}],
                    'paging_info': paging_info[1],
                }
            ),
        ],
        server_info={
            'version': [9, 0, 0],
            'api_max_entities_per_page': 2,
        },
    )

    assert [entity['id'] for entity in sg.find('Shot', [])] == [1, 2, 3]
    assert [cast('dict[str, object]', _params(sg, index)['paging']) for index in range(2)] == [
        {'entities_per_page': 2, 'current_page': 1},
        {'entities_per_page': 2, 'current_page': 2},
    ]


def test_prepared_find_finalizes_limit_and_page_from_server_limit() -> None:
    prepared = _crud.prepare_find(_crud.FindOptions(entity_type='Shot', filters=[], limit=3))

    plan = _crud.build_find_plan(prepared, 2)

    assert plan.multi_page is True
    assert plan.single_page is False
    assert plan.paging == {'entities_per_page': 2, 'current_page': 1}

    explicit = _crud.build_find_plan(
        _crud.prepare_find(_crud.FindOptions(entity_type='Shot', filters=[], page=3)),
        2,
    )
    assert explicit.multi_page is False
    assert explicit.single_page is True
    assert explicit.paging == {'entities_per_page': 2, 'current_page': 3}


@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('mock', [False, True])
def test_result_annotations_preserve_only_validated_topology(
    *, asynchronous: bool, mock: bool
) -> None:
    module = 'shotgun_api3.lib.mockgun' if mock else 'shotgun_api3'
    client = 'AsyncShotgun' if asynchronous else 'Shotgun'
    await_prefix = 'await ' if asynchronous else ''
    definition = 'async def' if asynchronous else 'def'
    contracts = {
        'batch([])': 'list[object]',
        "create('Shot', {})": 'dict[str, object]',
        "find('Shot', [])": 'list[dict[str, object]]',
        "find_one('Shot', [])": 'dict[str, object] | None',
        "update('Shot', 1, {})": ('list[dict[str, object]]' if mock else 'dict[str, object]'),
        'schema_entity_read()': 'dict[str, object]',
        "schema_field_read('Shot')": 'dict[str, object]',
        'schema_read()': 'dict[str, object]',
        "delete('Shot', 1)": 'bool',
        "revive('Shot', 1)": 'bool',
    }
    if not mock:
        contracts.update(
            {
                "activity_stream_read('Shot', 1)": 'dict[str, object]',
                "authenticate_human_user('user', 'password')": 'dict[str, object] | None',
                'follow({}, {})': 'dict[str, object]',
                'followers({})': 'list[dict[str, object]]',
                'following({})': 'list[dict[str, object]]',
                "nav_expand('/')": 'dict[str, object]',
                "nav_search_entity('/', {})": 'list[dict[str, object]]',
                "nav_search_string('/', 'shot')": 'list[dict[str, object]]',
                'note_thread_read(1)': 'list[dict[str, object]]',
                'preferences_read()': 'dict[str, object]',
                "summarize('Shot', [], [])": 'dict[str, object]',
                "text_search('shot', {'Shot': []})": 'dict[str, object]',
                'unfollow({}, {})': 'dict[str, object]',
                'user_subscriptions_read()': 'list[dict[str, object]]',
                "work_schedule_read('2026-07-01', '2026-07-31')": 'dict[str, object]',
                "work_schedule_update('2026-07-04', False)": 'dict[str, object]',
                'info()': 'dict[str, object]',
                "schema_field_create('Shot', 'text', 'Custom')": 'str',
            }
        )
    lines = [
        'from typing import assert_type',
        f'from {module} import {client}',
        f'{definition} check(client: {client}) -> None:',
        *(
            f'    assert_type({await_prefix}client.{call}, {expected})'
            for call, expected in contracts.items()
        ),
        f"    assert_type(({await_prefix}client.create('Shot', {{}}))['id'], object)",
        f"    ({await_prefix}client.create('Shot', {{}}))['id'].nonexistent()",
    ]
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
        probe = Path(directory) / 'result_types.py'
        probe.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        repository = Path(__file__).parents[2]
        result = run_pyright(probe, cwd=repository)

    report = json.loads(result.stdout)
    diagnostics = report['generalDiagnostics']
    assert result.returncode == 1, result.stdout
    assert {item['rule'] for item in diagnostics} == {
        'reportAttributeAccessIssue',
        'reportUnknownMemberType',
    }, result.stdout
    assert all(
        item['range']['start']['line'] == len(lines) - 1 and 'nonexistent' in item['message']
        for item in diagnostics
    ), result.stdout


@pytest.mark.parametrize(
    ('filter_operator', 'message'),
    [(None, 'filters must be a list or tuple'), ('invalid', "Invalid filter_operator: 'invalid'")],
)
def test_find_validates_operator_before_rejecting_noncontainer_filters(
    filter_operator: str | None,
    message: str,
) -> None:
    sg = _fake_client([])

    with pytest.raises(_errors.Fault) as caught:
        sg.find('Shot', cast('Any', None), filter_operator=filter_operator)

    assert str(caught.value) == message
    assert not _fake_transport(sg).requests


def test_update_detaches_nested_data_before_readiness() -> None:
    nested = {'tags': ['original']}
    data = {'metadata': nested}
    modes = {'metadata': 'set'}
    sg = _fake_client([_make_response({'type': 'Shot', 'id': 1})])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            nested['tags'][0] = 'changed'
            modes['metadata'] = 'add'

    sg.config.on_response = mutate_after_readiness

    sg.update('Shot', 1, data, modes)

    assert cast('list[dict[str, object]]', _params(sg)['fields']) == [
        {
            'field_name': 'metadata',
            'value': {'tags': ['original']},
            'multi_entity_update_mode': 'set',
        }
    ]
    assert data == {'metadata': {'tags': ['changed']}}
    assert modes == {'metadata': 'add'}
