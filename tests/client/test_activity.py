import copy
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytest

from shotgun_api3 import _errors
from shotgun_api3._client import Shotgun
from shotgun_api3._config import RequestEvent, ResponseEvent, RetryEvent, _ObserverEvent
from shotgun_api3._transport import SendError
from tests.client._payloads import rpc_params
from tests.conftest import fake_client, fake_transport, make_response, sent_payload

_PROJECT = {'type': 'Project', 'id': 2, 'name': 'P'}
_SHOT = {'type': 'Shot', 'id': 3, 'code': 'X'}
_USER = {'type': 'HumanUser', 'id': 7, 'login': 'me'}

_ActivityStreamReadCallable: TypeAlias = Callable[
    [
        str,
        int,
        dict[str, list[str]] | None,
        int | None,
        int | None,
        int | None,
    ],
    dict[str, object],
]
_FollowCallable: TypeAlias = Callable[
    [dict[str, Any], dict[str, Any]],
    dict[str, object],
]
_FollowersCallable: TypeAlias = Callable[[dict[str, Any]], list[dict[str, object]]]
_FollowingCallable: TypeAlias = Callable[
    [dict[str, Any], dict[str, Any] | None, str | None],
    list[dict[str, object]],
]
_NoteThreadReadCallable: TypeAlias = Callable[
    [int, dict[str, list[str]] | None],
    list[dict[str, object]],
]

if TYPE_CHECKING:
    _STATIC_CLIENT = cast('Shotgun', object())
    _ACTIVITY_STREAM_READ: _ActivityStreamReadCallable = _STATIC_CLIENT.activity_stream_read
    _FOLLOW: _FollowCallable = _STATIC_CLIENT.follow
    _FOLLOWERS: _FollowersCallable = _STATIC_CLIENT.followers
    _FOLLOWING: _FollowingCallable = _STATIC_CLIENT.following
    _NOTE_THREAD_READ: _NoteThreadReadCallable = _STATIC_CLIENT.note_thread_read
    _UNFOLLOW: _FollowCallable = _STATIC_CLIENT.unfollow


def _run_dictionary_result_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'activity_stream_read':
        return sg.activity_stream_read('Shot', 3)
    method = cast('_FollowCallable', getattr(sg, method_name))
    return method(_USER, _SHOT)


def _run_list_result_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'note_thread_read':
        return sg.note_thread_read(41)
    if method_name == 'followers':
        return sg.followers(_SHOT)
    return sg.following(_USER)


def test_activity_methods_expose_exact_signatures() -> None:
    expected = {
        'activity_stream_read': (
            ('self', 'entity_type', 'entity_id', 'entity_fields', 'min_id', 'max_id', 'limit'),
            (
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                inspect.Parameter.empty,
                None,
                None,
                None,
                None,
            ),
            (
                inspect.Parameter.empty,
                str,
                int,
                dict[str, list[str]] | None,
                int | None,
                int | None,
                int | None,
            ),
            dict[str, object],
        ),
        'note_thread_read': (
            ('self', 'note_id', 'entity_fields'),
            (inspect.Parameter.empty, inspect.Parameter.empty, None),
            (inspect.Parameter.empty, int, dict[str, list[str]] | None),
            list[dict[str, object]],
        ),
        'follow': (
            ('self', 'user', 'entity'),
            (inspect.Parameter.empty,) * 3,
            (inspect.Parameter.empty, dict[str, Any], dict[str, Any]),
            dict[str, object],
        ),
        'unfollow': (
            ('self', 'user', 'entity'),
            (inspect.Parameter.empty,) * 3,
            (inspect.Parameter.empty, dict[str, Any], dict[str, Any]),
            dict[str, object],
        ),
        'followers': (
            ('self', 'entity'),
            (inspect.Parameter.empty,) * 2,
            (inspect.Parameter.empty, dict[str, Any]),
            list[dict[str, object]],
        ),
        'following': (
            ('self', 'user', 'project', 'entity_type'),
            (inspect.Parameter.empty, inspect.Parameter.empty, None, None),
            (
                inspect.Parameter.empty,
                dict[str, Any],
                dict[str, Any] | None,
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


def test_activity_reads_forward_annotated_out_values() -> None:
    sg = fake_client([make_response({'updates': []}), make_response([])])
    activity_stream_read = cast('Callable[..., object]', sg.activity_stream_read)
    note_thread_read = cast('Callable[..., object]', sg.note_thread_read)

    activity_stream_read(
        [],
        entity_id=True,
        entity_fields={'Shot': 'code'},
        min_id=-1,
        max_id=True,
        limit='10',
    )
    note_thread_read('41', {'Note': 'created_by'})

    assert rpc_params(sg) == {
        'type': [],
        'id': True,
        'max_id': True,
        'min_id': -1,
        'limit': '10',
        'entity_fields': {'Shot': 'code'},
    }
    assert rpc_params(sg, 1) == {
        'note_id': '41',
        'entity_fields': {'Note': 'created_by'},
    }


def test_activity_stream_read_binds_against_the_public_signature() -> None:
    sg = fake_client([])
    activity_stream_read = cast('Callable[..., object]', sg.activity_stream_read)

    with pytest.raises(TypeError, match=r'^Shotgun\.activity_stream_read\(\)'):
        activity_stream_read('Shot')

    assert fake_transport(sg).requests == []


def test_activity_stream_read_builds_exact_wire_payloads() -> None:
    entity_fields = {'Shot': ['code']}
    original = copy.deepcopy(entity_fields)
    sg = fake_client(
        [
            make_response({'updates': []}),
            make_response({'updates': []}),
        ]
    )

    sg.activity_stream_read(
        'Shot',
        3,
        entity_fields=entity_fields,
        min_id=10,
        max_id=20,
        limit=50,
    )
    sg.activity_stream_read('Shot', 3)

    expected_keys = ('type', 'id', 'max_id', 'min_id', 'limit', 'entity_fields')
    assert sent_payload(sg)['method_name'] == 'activity_stream'
    assert sent_payload(sg, 1)['method_name'] == 'activity_stream'
    assert tuple(rpc_params(sg)) == expected_keys
    assert tuple(rpc_params(sg, 1)) == expected_keys
    assert rpc_params(sg) == {
        'type': 'Shot',
        'id': 3,
        'max_id': 20,
        'min_id': 10,
        'limit': 50,
        'entity_fields': {'Shot': ['code']},
    }
    assert rpc_params(sg, 1) == {
        'type': 'Shot',
        'id': 3,
        'max_id': None,
        'min_id': None,
        'limit': None,
        'entity_fields': {},
    }
    assert entity_fields == original


def test_activity_stream_read_options_are_detached_before_readiness() -> None:
    entity_fields = {'Shot': ['code']}
    min_id = ['original-min']
    max_id = ['original-max']
    limit = ['original-limit']
    sg = fake_client([make_response({'updates': []})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            entity_fields['Shot'][0] = 'changed-field'
            min_id[0] = 'changed-min'
            max_id[0] = 'changed-max'
            limit[0] = 'changed-limit'

    sg.config.on_response = mutate_after_readiness
    activity_stream_read = cast('Callable[..., object]', sg.activity_stream_read)

    activity_stream_read(
        'Shot',
        3,
        entity_fields=entity_fields,
        min_id=min_id,
        max_id=max_id,
        limit=limit,
    )

    params = rpc_params(sg)
    assert params == {
        'type': 'Shot',
        'id': 3,
        'max_id': ['original-max'],
        'min_id': ['original-min'],
        'limit': ['original-limit'],
        'entity_fields': {'Shot': ['code']},
    }
    assert tuple(params) == ('type', 'id', 'max_id', 'min_id', 'limit', 'entity_fields')
    assert sent_payload(sg)['method_name'] == 'activity_stream'


def test_activity_stream_read_preserves_zero_optional_values() -> None:
    sg = fake_client([make_response({'updates': []})])

    sg.activity_stream_read('Shot', 3, min_id=0, max_id=0, limit=0)

    assert rpc_params(sg) == {
        'type': 'Shot',
        'id': 3,
        'max_id': 0,
        'min_id': 0,
        'limit': 0,
        'entity_fields': {},
    }


def test_activity_stream_read_retries_as_logical_read_with_private_wire_name() -> None:
    events: list[_ObserverEvent] = []
    sg = fake_client([SendError('ambiguous'), make_response({'updates': []})])
    sg.config.rpc_attempt_interval = 0
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    assert sg.activity_stream_read('Shot', 3) == {'updates': []}

    operation_events = [
        event
        for event in events
        if isinstance(event, (RequestEvent, ResponseEvent, RetryEvent))
        and event.operation == 'read'
    ]
    assert sum(isinstance(event, RequestEvent) for event in operation_events) == 3
    assert sum(isinstance(event, RetryEvent) for event in operation_events) == 1
    assert all(event.operation == 'read' for event in operation_events)
    assert sent_payload(sg)['method_name'] == 'activity_stream'
    assert sent_payload(sg, 1)['method_name'] == 'activity_stream'


@pytest.mark.parametrize('method_name', ['activity_stream_read', 'follow', 'unfollow'])
def test_dictionary_activity_methods_require_dictionary_results(method_name: str) -> None:
    sg = fake_client([make_response(['sensitive-result'])])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^{method_name} did not return a dictionary$',
    ) as error:
        _run_dictionary_result_case(sg, method_name)

    assert 'sensitive-' not in str(error.value)


@pytest.mark.parametrize('method_name', ['activity_stream_read', 'note_thread_read'])
def test_entity_fields_are_detached_before_readiness(method_name: str) -> None:
    entity_fields = {'Shot': ['code']}
    result: object = {'updates': []} if method_name == 'activity_stream_read' else []
    sg = fake_client([make_response(result)])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            entity_fields['Shot'][0] = 'changed'
            entity_fields['Asset'] = ['code']

    sg.config.on_response = mutate_after_readiness

    if method_name == 'activity_stream_read':
        sg.activity_stream_read('Shot', 3, entity_fields=entity_fields)
    else:
        sg.note_thread_read(41, entity_fields=entity_fields)

    assert rpc_params(sg)['entity_fields'] == {'Shot': ['code']}


def test_follow_and_unfollow_optimize_entities_without_mutation() -> None:
    user = {'type': 'HumanUser', 'id': 7, 'name': 'Me', 'login': 'me'}
    entity = {'type': 'Shot', 'id': 3, 'name': 'X', 'code': 'X'}
    original = copy.deepcopy((user, entity))
    sg = fake_client(
        [
            make_response({'followed': True}),
            make_response({'followed': False}),
        ]
    )

    sg.follow(user, entity)
    sg.unfollow(user, entity)

    expected = {
        'user': {'type': 'HumanUser', 'id': 7, 'name': 'Me'},
        'entity': {'type': 'Shot', 'id': 3, 'name': 'X'},
    }
    assert sent_payload(sg)['method_name'] == 'follow'
    assert sent_payload(sg, 1)['method_name'] == 'unfollow'
    assert rpc_params(sg) == expected
    assert rpc_params(sg, 1) == expected
    assert (user, entity) == original


@pytest.mark.parametrize('method_name', ['follow', 'unfollow'])
def test_follow_mutable_entities_are_detached_before_readiness(method_name: str) -> None:
    user = {'type': 'HumanUser', 'id': 7, 'name': ['original-user']}
    entity = {'type': 'Shot', 'id': 3, 'name': ['original-entity']}
    sg = fake_client([make_response({'followed': method_name == 'follow'})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', user['name'])[0] = 'changed-user'
            cast('list[str]', entity['name'])[0] = 'changed-entity'

    sg.config.on_response = mutate_after_readiness

    method = cast('_FollowCallable', getattr(sg, method_name))
    method(user, entity)

    assert rpc_params(sg) == {
        'user': {'type': 'HumanUser', 'id': 7, 'name': ['original-user']},
        'entity': {'type': 'Shot', 'id': 3, 'name': ['original-entity']},
    }


def test_followers_and_following_build_exact_payloads_and_omit_optionals() -> None:
    user = {'type': 'HumanUser', 'id': 7, 'login': 'me'}
    entity = {'type': 'Shot', 'id': 3, 'code': 'X'}
    project = {'type': 'Project', 'id': 2, 'name': 'P', 'sg_status': 'Active'}
    original = copy.deepcopy((user, entity, project))
    sg = fake_client([make_response([]), make_response([]), make_response([])])

    sg.followers(entity)
    sg.following(user, project=project, entity_type='Shot')
    sg.following(user)

    assert sent_payload(sg)['method_name'] == 'followers'
    assert rpc_params(sg) == {'entity': {'type': 'Shot', 'id': 3}}
    assert sent_payload(sg, 1)['method_name'] == 'following'
    assert rpc_params(sg, 1) == {
        'user': {'type': 'HumanUser', 'id': 7},
        'project': {'type': 'Project', 'id': 2, 'name': 'P'},
        'entity_type': 'Shot',
    }
    assert rpc_params(sg, 2) == {'user': {'type': 'HumanUser', 'id': 7}}
    assert (user, entity, project) == original


def test_followers_entity_is_detached_before_readiness() -> None:
    entity = {'type': 'Shot', 'id': 3, 'name': ['original']}
    sg = fake_client([make_response([])])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', entity['name'])[0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    sg.followers(entity)

    assert rpc_params(sg) == {
        'entity': {'type': 'Shot', 'id': 3, 'name': ['original']},
    }


def test_following_user_and_project_are_detached_before_readiness() -> None:
    user = {'type': 'HumanUser', 'id': 7, 'name': ['original-user']}
    project = {'type': 'Project', 'id': 2, 'name': ['original-project']}
    sg = fake_client([make_response([])])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', user['name'])[0] = 'changed-user'
            cast('list[str]', project['name'])[0] = 'changed-project'

    sg.config.on_response = mutate_after_readiness

    sg.following(user, project=project)

    assert rpc_params(sg) == {
        'user': {'type': 'HumanUser', 'id': 7, 'name': ['original-user']},
        'project': {'type': 'Project', 'id': 2, 'name': ['original-project']},
    }


@pytest.mark.parametrize('method_name', ['note_thread_read', 'followers', 'following'])
@pytest.mark.parametrize(
    ('result', 'message_suffix'),
    [
        ({'sensitive': 'container'}, 'did not return a list'),
        ([{'id': 1}, 'sensitive-member'], 'returned a non-dictionary member'),
    ],
)
def test_list_activity_methods_require_lists_of_dictionaries(
    method_name: str,
    result: object,
    message_suffix: str,
) -> None:
    sg = fake_client([make_response(result)])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^{method_name} {message_suffix}$',
    ) as error:
        _run_list_result_case(sg, method_name)

    assert 'sensitive' not in str(error.value)


def test_note_thread_read_builds_exact_payload_and_omits_entity_fields() -> None:
    entity_fields = {'Note': ['created_by']}
    original = copy.deepcopy(entity_fields)
    sg = fake_client([make_response([]), make_response([])])

    sg.note_thread_read(41, entity_fields=entity_fields)
    sg.note_thread_read(41)

    assert sent_payload(sg)['method_name'] == 'note_thread_contents'
    assert rpc_params(sg) == {'note_id': 41, 'entity_fields': {'Note': ['created_by']}}
    assert rpc_params(sg, 1) == {'note_id': 41}
    assert entity_fields == original


def test_social_methods_forward_values_the_optimizer_cannot_project() -> None:
    sg = fake_client(
        [
            make_response({'followed': True}),
            make_response({'followed': False}),
            make_response([]),
            make_response([]),
        ]
    )
    follow = cast('Callable[..., object]', sg.follow)
    unfollow = cast('Callable[..., object]', sg.unfollow)
    followers = cast('Callable[..., object]', sg.followers)
    following = cast('Callable[..., object]', sg.following)

    follow([], '')
    unfollow('', [])
    followers([])
    following([], project='', entity_type='')

    assert rpc_params(sg) == {'user': [], 'entity': ''}
    assert rpc_params(sg, 1) == {'user': '', 'entity': []}
    assert rpc_params(sg, 2) == {'entity': []}
    assert rpc_params(sg, 3) == {'user': [], 'project': '', 'entity_type': ''}
