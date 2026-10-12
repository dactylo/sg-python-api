import copy
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytest

from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ResponseEvent
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

_NavExpandCallable: TypeAlias = Callable[
    [str, str | None, dict[str, list[str]] | None],
    dict[str, object],
]
_NavSearchEntityCallable: TypeAlias = Callable[
    [str, dict[str, Any], str | None],
    list[dict[str, object]],
]
_NavSearchStringCallable: TypeAlias = Callable[
    [str, str, str | None],
    list[dict[str, object]],
]

if TYPE_CHECKING:
    _STATIC_CLIENT = cast('Shotgun', object())
    _NAV_EXPAND: _NavExpandCallable = _STATIC_CLIENT.nav_expand
    _NAV_SEARCH_ENTITY: _NavSearchEntityCallable = _STATIC_CLIENT.nav_search_entity
    _NAV_SEARCH_STRING: _NavSearchStringCallable = _STATIC_CLIENT.nav_search_string


def _params(sg: Shotgun, index: int = 0) -> dict[str, object]:
    params = cast('list[object]', _sent_payload(sg, index)['params'])
    return cast('dict[str, object]', params[1])


def _run_search_result_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'nav_search_entity':
        return sg.nav_search_entity('/', {'type': 'Shot', 'id': 3})
    return sg.nav_search_string('/', 'bunny')


def test_nav_expand_builds_exact_payload_and_omits_optionals() -> None:
    entity_fields = {'Shot': ['code']}
    original = copy.deepcopy(entity_fields)
    sg = _fake_client([_make_response({'label': 'Root'}), _make_response({'label': 'Root'})])

    result = sg.nav_expand(
        '/Project/65',
        seed_entity_field='entity',
        entity_fields=entity_fields,
    )
    sg.nav_expand('/Project/65')

    assert result == {'label': 'Root'}
    assert _sent_payload(sg)['method_name'] == 'nav_expand'
    assert _params(sg) == {
        'path': '/Project/65',
        'seed_entity_field': 'entity',
        'entity_fields': {'Shot': ['code']},
    }
    assert _params(sg, 1) == {'path': '/Project/65'}
    assert entity_fields == original


def test_nav_expand_detaches_entity_fields_before_readiness() -> None:
    entity_fields = {'Shot': ['code']}
    sg = _fake_client([_make_response({'label': 'Root'})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            entity_fields['Shot'][0] = 'changed'
            entity_fields['Asset'] = ['code']

    sg.config.on_response = mutate_after_readiness

    sg.nav_expand('/Project/65', entity_fields=entity_fields)

    assert _params(sg)['entity_fields'] == {'Shot': ['code']}


def test_nav_expand_requires_dictionary_result() -> None:
    sg = _fake_client([_make_response(['sensitive-result'])])

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^nav_expand did not return a dictionary$',
    ) as error:
        sg.nav_expand('/')

    assert 'sensitive' not in str(error.value)


def test_nav_search_entity_omits_optional_seed_without_mutating_entity() -> None:
    entity = {'type': 'Shot', 'id': 3, 'extra': 'x'}
    original = copy.deepcopy(entity)
    sg = _fake_client([_make_response([])])

    sg.nav_search_entity('/', entity)

    assert _params(sg) == {
        'root_path': '/',
        'search_criteria': {'entity': {'type': 'Shot', 'id': 3}},
    }
    assert entity == original


def test_nav_search_entity_optimizes_and_detaches_entity() -> None:
    entity = {
        'type': 'Shot',
        'id': 3,
        'name': ['original-name'],
        'sg_status_list': 'ip',
    }
    sg = _fake_client([_make_response([])])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', entity['name'])[0] = 'changed-name'
            entity['sg_status_list'] = 'fin'

    sg.config.on_response = mutate_after_readiness

    sg.nav_search_entity('/', entity, seed_entity_field='entity')

    assert _sent_payload(sg)['method_name'] == 'nav_search_entity'
    assert _params(sg) == {
        'root_path': '/',
        'search_criteria': {
            'entity': {'type': 'Shot', 'id': 3, 'name': ['original-name']},
        },
        'seed_entity_field': 'entity',
    }


def test_nav_search_string_builds_exact_payload_and_omits_optional() -> None:
    sg = _fake_client([_make_response([]), _make_response([])])

    sg.nav_search_string('/', 'bunny', seed_entity_field='entity')
    sg.nav_search_string('/', 'bunny')

    assert _sent_payload(sg)['method_name'] == 'nav_search_string'
    assert _params(sg) == {
        'root_path': '/',
        'search_criteria': {'search_string': 'bunny'},
        'seed_entity_field': 'entity',
    }
    assert _params(sg, 1) == {
        'root_path': '/',
        'search_criteria': {'search_string': 'bunny'},
    }


@pytest.mark.parametrize('method_name', ['nav_search_entity', 'nav_search_string'])
@pytest.mark.parametrize(
    ('result', 'message_suffix'),
    [
        ({'sensitive': 'container'}, 'did not return a list'),
        ([{'id': 1}, 'sensitive-member'], 'returned a non-dictionary member'),
    ],
)
def test_nav_searches_require_lists_of_dictionaries(
    method_name: str,
    result: object,
    message_suffix: str,
) -> None:
    sg = _fake_client([_make_response(result)])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^{method_name} {message_suffix}$',
    ) as error:
        _run_search_result_case(sg, method_name)

    assert 'sensitive' not in str(error.value)


@pytest.mark.parametrize(
    ('method_name', 'args'),
    [
        ('nav_expand', ()),
        ('nav_search_entity', ('/',)),
        ('nav_search_string', ('/',)),
    ],
)
def test_navigation_methods_bind_against_public_signatures(
    method_name: str,
    args: tuple[object, ...],
) -> None:
    sg = _fake_client([])
    method = cast('Callable[..., object]', getattr(sg, method_name))

    with pytest.raises(TypeError, match=rf'^Shotgun\.{method_name}\(\)'):
        method(*args)

    assert _fake_transport(sg).requests == []


def test_navigation_methods_expose_exact_signatures() -> None:
    expected = {
        'nav_expand': (
            ('self', 'path', 'seed_entity_field', 'entity_fields'),
            (inspect.Parameter.empty, inspect.Parameter.empty, None, None),
            (
                inspect.Parameter.empty,
                str,
                str | None,
                dict[str, list[str]] | None,
            ),
            dict[str, object],
        ),
        'nav_search_entity': (
            ('self', 'root_path', 'entity', 'seed_entity_field'),
            (inspect.Parameter.empty, inspect.Parameter.empty, inspect.Parameter.empty, None),
            (
                inspect.Parameter.empty,
                str,
                dict[str, Any],
                str | None,
            ),
            list[dict[str, object]],
        ),
        'nav_search_string': (
            ('self', 'root_path', 'search_string', 'seed_entity_field'),
            (inspect.Parameter.empty, inspect.Parameter.empty, inspect.Parameter.empty, None),
            (
                inspect.Parameter.empty,
                str,
                str,
                str | None,
            ),
            list[dict[str, object]],
        ),
    }

    for method_name, (names, defaults, annotations, return_annotation) in expected.items():
        signature = inspect.signature(getattr(Shotgun, method_name))
        assert tuple(signature.parameters) == names
        assert tuple(parameter.default for parameter in signature.parameters.values()) == defaults
        assert all(
            parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            for parameter in signature.parameters.values()
        )
        assert (
            tuple(parameter.annotation for parameter in signature.parameters.values())
            == annotations
        )
        assert signature.return_annotation == return_annotation


def test_navigation_methods_forward_annotated_out_values() -> None:
    sg = _fake_client([_make_response({}), _make_response([]), _make_response([])])
    nav_expand = cast('Callable[..., object]', sg.nav_expand)
    nav_search_entity = cast('Callable[..., object]', sg.nav_search_entity)
    nav_search_string = cast('Callable[..., object]', sg.nav_search_string)

    nav_expand([], ['seed'], {'Shot': 'code'})
    nav_search_entity([], ['entity'], ['seed'])
    nav_search_string([], ['search'], ['seed'])

    assert _params(sg) == {
        'path': [],
        'seed_entity_field': ['seed'],
        'entity_fields': {'Shot': 'code'},
    }
    assert _params(sg, 1) == {
        'root_path': [],
        'search_criteria': {'entity': ['entity']},
        'seed_entity_field': ['seed'],
    }
    assert _params(sg, 2) == {
        'root_path': [],
        'search_criteria': {'search_string': ['search']},
        'seed_entity_field': ['seed'],
    }
