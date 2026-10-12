import copy
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytest

from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ObserverEvent, RequestEvent, ResponseEvent, RetryEvent
from shotgun_api3._transport import SendError
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

_SummarizeCallable: TypeAlias = Callable[
    [
        str,
        list[Any] | tuple[Any, ...] | dict[str, Any],
        list[dict[str, Any]],
        str | None,
        list[dict[str, Any]] | None,
        bool,
    ],
    dict[str, object],
]
_TextSearchCallable: TypeAlias = Callable[
    [str, dict[str, Any], list[int] | None, int | None],
    dict[str, object],
]

if TYPE_CHECKING:
    _STATIC_CLIENT = cast('Shotgun', object())
    _SUMMARIZE: _SummarizeCallable = _STATIC_CLIENT.summarize
    _TEXT_SEARCH: _TextSearchCallable = _STATIC_CLIENT.text_search


def _params(sg: Shotgun) -> dict[str, object]:
    params = cast('list[object]', _sent_payload(sg)['params'])
    return cast('dict[str, object]', params[1])


def _run_result_contract_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'summarize':
        return sg.summarize('Shot', [], [])
    return sg.text_search('boat', {'Shot': []})


def test_summarize_and_text_search_expose_exact_signatures() -> None:
    summarize_signature = inspect.signature(Shotgun.summarize)
    text_search_signature = inspect.signature(Shotgun.text_search)

    assert tuple(summarize_signature.parameters) == (
        'self',
        'entity_type',
        'filters',
        'summary_fields',
        'filter_operator',
        'grouping',
        'include_archived_projects',
    )
    assert tuple(parameter.default for parameter in summarize_signature.parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
        None,
        True,
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in summarize_signature.parameters.values()
    )
    assert tuple(
        parameter.annotation for parameter in summarize_signature.parameters.values()
    ) == (
        inspect.Parameter.empty,
        str,
        list[Any] | tuple[Any, ...] | dict[str, Any],
        list[dict[str, Any]],
        str | None,
        list[dict[str, Any]] | None,
        bool,
    )
    assert summarize_signature.return_annotation == dict[str, object]
    assert tuple(text_search_signature.parameters) == (
        'self',
        'text',
        'entity_types',
        'project_ids',
        'limit',
    )
    assert tuple(parameter.default for parameter in text_search_signature.parameters.values()) == (
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        inspect.Parameter.empty,
        None,
        None,
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in text_search_signature.parameters.values()
    )
    assert tuple(
        parameter.annotation for parameter in text_search_signature.parameters.values()
    ) == (
        inspect.Parameter.empty,
        str,
        dict[str, Any],
        list[int] | None,
        int | None,
    )
    assert text_search_signature.return_annotation == dict[str, object]


@pytest.mark.parametrize('method_name', ['summarize', 'text_search'])
def test_summarize_and_text_search_require_dictionary_results(method_name: str) -> None:
    sg = _fake_client([_make_response([])])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^{method_name} did not return a dictionary$',
    ):
        _run_result_contract_case(sg, method_name)


def test_summarize_detaches_nested_data_before_readiness() -> None:
    entity_type = ['original-type']
    filter_value = {'nested': ['original-filter']}
    summary_fields = [
        {'field': 'id', 'type': 'count', 'options': {'nested': ['original-summary']}}
    ]
    grouping = [{'field': 'sg_status_list', 'type': 'exact', 'options': ['original-group']}]
    sg = _fake_client([_make_response({'summaries': {'id': 3}, 'groups': []})])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            entity_type[0] = 'changed-type'
            filter_value['nested'][0] = 'changed-filter'
            cast('dict[str, Any]', summary_fields[0])['options']['nested'][0] = 'changed-summary'
            cast('dict[str, Any]', grouping[0])['options'][0] = 'changed-group'

    sg.config.on_response = mutate_after_readiness

    summarize = cast('Callable[..., object]', sg.summarize)
    summarize(
        entity_type,
        [['metadata', 'is', filter_value]],
        summary_fields,
        grouping=grouping,
    )

    assert _params(sg) == {
        'type': ['original-type'],
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
        'summaries': [
            {'field': 'id', 'type': 'count', 'options': {'nested': ['original-summary']}}
        ],
        'grouping': [{'field': 'sg_status_list', 'type': 'exact', 'options': ['original-group']}],
    }


def test_summarize_docstring_documents_public_parameters() -> None:
    docstring = inspect.getdoc(Shotgun.summarize)
    assert docstring is not None
    for name in (
        'entity_type',
        'filters',
        'summary_fields',
        'filter_operator',
        'grouping',
        'include_archived_projects',
    ):
        assert f'{name}:' in docstring
    assert '*args:' not in docstring
    assert '**kwargs:' not in docstring


def test_summarize_forwards_filter_operator() -> None:
    sg = _fake_client([_make_response({'summaries': {'id': 2}})])

    sg.summarize(
        'Shot',
        [['code', 'is', 'A'], ['code', 'is', 'B']],
        [{'field': 'id', 'type': 'count'}],
        filter_operator='any',
    )

    filters = cast('dict[str, object]', _params(sg)['filters'])
    assert filters['logical_operator'] == 'or'


def test_summarize_omits_optional_params() -> None:
    sg = _fake_client([_make_response({'summaries': {'id': 0}})])

    sg.summarize('Shot', [], [{'field': 'id', 'type': 'count'}])

    assert 'grouping' not in _params(sg)
    assert 'include_archived_projects' not in _params(sg)


def test_summarize_payload() -> None:
    filters = [['project', 'is', {'type': 'Project', 'id': 1}]]
    summary_fields = [{'field': 'id', 'type': 'count'}]
    grouping = [{'field': 'sg_status_list', 'type': 'exact', 'direction': 'asc'}]
    original = copy.deepcopy((filters, summary_fields, grouping))
    sg = _fake_client([_make_response({'summaries': {'id': 3}, 'groups': []})])

    result = sg.summarize(
        'Shot',
        filters,
        summary_fields,
        grouping=grouping,
        include_archived_projects=False,
    )

    assert result['summaries'] == {'id': 3}
    assert _params(sg) == {
        'type': 'Shot',
        'filters': {
            'logical_operator': 'and',
            'conditions': [
                {
                    'path': 'project',
                    'relation': 'is',
                    'values': [{'type': 'Project', 'id': 1}],
                }
            ],
        },
        'summaries': [{'field': 'id', 'type': 'count'}],
        'grouping': [{'field': 'sg_status_list', 'type': 'exact', 'direction': 'asc'}],
        'include_archived_projects': False,
    }
    assert (filters, summary_fields, grouping) == original


def test_summarize_validates_inputs_before_readiness() -> None:
    filters_sg = _fake_client([])
    summaries_sg = _fake_client([])
    summarize_filters = cast('Any', filters_sg.summarize)
    summarize_fields = cast('Any', summaries_sg.summarize)

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^summarize\(\) requires list or dict filters$',
    ):
        summarize_filters('Shot', 'not-filters', [{'field': 'id', 'type': 'count'}])
    with pytest.raises(
        _errors.ShotgunError,
        match=r'^summarize\(\) requires a list of summary fields$',
    ):
        summarize_fields('Shot', [], 'not-a-list')

    assert _fake_transport(filters_sg).requests == []
    assert _fake_transport(summaries_sg).requests == []


def test_text_search_detaches_nested_data_before_readiness() -> None:
    filter_value = {'nested': ['original-filter']}
    entity_types = {'Shot': [['metadata', 'is', filter_value]]}
    project_ids = [1]
    sg = _fake_client([_make_response({'matches': []})])
    response_count = 0

    def mutate_after_readiness(event: object) -> None:
        nonlocal response_count
        if not isinstance(event, ResponseEvent):
            return
        response_count += 1
        if response_count == 1:
            filter_value['nested'][0] = 'changed-filter'
            entity_types.clear()
            project_ids[0] = 2

    sg.config.on_response = mutate_after_readiness

    sg.text_search('boat', entity_types, project_ids=project_ids)

    assert _params(sg) == {
        'text': 'boat',
        'entity_types': {
            'Shot': {
                'logical_operator': 'and',
                'conditions': [
                    {
                        'path': 'metadata',
                        'relation': 'is',
                        'values': [{'nested': ['original-filter']}],
                    }
                ],
            }
        },
        'project_ids': [1],
        'max_results': None,
    }


def test_text_search_includes_default_optional_params() -> None:
    sg = _fake_client([_make_response({'matches': []})])

    sg.text_search('boat', {'Shot': []})

    assert _sent_payload(sg)['method_name'] == 'query_display_name_cache'
    assert _params(sg) == {
        'text': 'boat',
        'entity_types': {
            'Shot': {'logical_operator': 'and', 'conditions': []},
        },
        'project_ids': [],
        'max_results': None,
    }


def test_text_search_payload() -> None:
    entity_types: dict[str, Any] = {'Shot': []}
    project_ids = [1, 2]
    original = copy.deepcopy((entity_types, project_ids))
    sg = _fake_client([_make_response({'matches': []})])

    result = sg.text_search('boat', entity_types, project_ids=project_ids, limit=5)

    assert result == {'matches': []}
    assert _sent_payload(sg)['method_name'] == 'query_display_name_cache'
    assert _params(sg) == {
        'text': 'boat',
        'entity_types': {
            'Shot': {'logical_operator': 'and', 'conditions': []},
        },
        'project_ids': [1, 2],
        'max_results': 5,
    }
    assert (entity_types, project_ids) == original


def test_text_search_requires_dict_before_readiness() -> None:
    sg = _fake_client([])
    text_search = cast('Any', sg.text_search)

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^text_search\(\) entity_types must be a dict$',
    ):
        text_search('boat', ['Shot'])

    assert _fake_transport(sg).requests == []


def test_text_search_retries_as_logical_read_with_private_wire_name() -> None:
    events: list[ObserverEvent] = []
    sg = _fake_client([SendError('ambiguous'), _make_response({'matches': []})])
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    assert sg.text_search('boat', {'Shot': []}) == {'matches': []}

    operation_events = [
        event
        for event in events
        if isinstance(event, (RequestEvent, ResponseEvent, RetryEvent))
        and event.operation == 'read'
    ]
    assert sum(isinstance(event, RequestEvent) for event in operation_events) == 3
    assert sum(isinstance(event, RetryEvent) for event in operation_events) == 1
    assert all(event.operation == 'read' for event in operation_events)
    assert _sent_payload(sg)['method_name'] == 'query_display_name_cache'
    assert _sent_payload(sg, 1)['method_name'] == 'query_display_name_cache'
