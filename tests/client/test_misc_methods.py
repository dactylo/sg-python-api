import copy
import datetime
import inspect
from collections import UserDict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytest

from shotgun_api3 import _errors, _methods
from shotgun_api3._client import Shotgun
from shotgun_api3._config import ObserverEvent, RequestEvent, ResponseEvent, RetryEvent
from shotgun_api3._transport import SendError
from tests.conftest import _fake_client, _fake_transport, _make_response, _sent_payload

_CREATE_ENTRY_ERROR = (
    'user_subscriptions_create users must contain dictionaries with '
    'humanUserId and subscription keys'
)
_PROJECT = {'type': 'Project', 'id': 2, 'name': 'P'}
_USER = {'type': 'HumanUser', 'id': 7, 'name': 'Me'}

_ExportPageCallable: TypeAlias = Callable[[int, str, str | None], str]
_PreferencesReadCallable: TypeAlias = Callable[[list[str] | None], dict[str, object]]
_SubscriptionsCreateCallable: TypeAlias = Callable[[list[dict[str, Any]]], bool]
_SubscriptionsReadCallable: TypeAlias = Callable[[], list[dict[str, object]]]
_UpdateProjectLastAccessedCallable: TypeAlias = Callable[
    [dict[str, Any], dict[str, Any] | None],
    None,
]
_WorkScheduleReadCallable: TypeAlias = Callable[
    [datetime.date | str, datetime.date | str, dict[str, Any] | None, dict[str, Any] | None],
    dict[str, object],
]
_WorkScheduleUpdateCallable: TypeAlias = Callable[
    [
        datetime.date | str,
        bool,
        str | None,
        dict[str, Any] | None,
        dict[str, Any] | None,
        str | None,
    ],
    dict[str, object],
]

if TYPE_CHECKING:
    _STATIC_CLIENT = cast('Shotgun', object())
    _EXPORT_PAGE: _ExportPageCallable = _STATIC_CLIENT.export_page
    _PREFERENCES_READ: _PreferencesReadCallable = _STATIC_CLIENT.preferences_read
    _SUBSCRIPTIONS_CREATE: _SubscriptionsCreateCallable = _STATIC_CLIENT.user_subscriptions_create
    _SUBSCRIPTIONS_READ: _SubscriptionsReadCallable = _STATIC_CLIENT.user_subscriptions_read
    _UPDATE_PROJECT_LAST_ACCESSED: _UpdateProjectLastAccessedCallable = (
        _STATIC_CLIENT.update_project_last_accessed
    )
    _WORK_SCHEDULE_READ: _WorkScheduleReadCallable = _STATIC_CLIENT.work_schedule_read
    _WORK_SCHEDULE_UPDATE: _WorkScheduleUpdateCallable = _STATIC_CLIENT.work_schedule_update


def _params(sg: Shotgun, index: int = 0) -> object:
    params = cast('list[object]', _sent_payload(sg, index)['params'])
    return params[1] if len(params) > 1 else None


def _run_dictionary_result_case(sg: Shotgun, method_name: str) -> object:
    if method_name == 'work_schedule_read':
        return sg.work_schedule_read('2026-07-01', '2026-07-31')
    if method_name == 'work_schedule_update':
        return sg.work_schedule_update('2026-07-04', working=False)
    return sg.preferences_read()


@pytest.mark.parametrize(
    'method_name',
    ['work_schedule_read', 'work_schedule_update', 'preferences_read'],
)
def test_dictionary_methods_require_dictionary_results(method_name: str) -> None:
    sg = _fake_client([_make_response(['sensitive-result'])])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^{method_name} did not return a dictionary$',
    ) as error:
        _run_dictionary_result_case(sg, method_name)

    assert 'sensitive' not in str(error.value)


@pytest.mark.parametrize(
    ('args', 'kwargs'),
    [
        ((12,), {}),
        ((12, 'csv'), {'format': 'json'}),
        ((12, 'csv', None, 'extra'), {}),
        ((12, 'csv'), {'unexpected': 'value'}),
    ],
)
def test_export_page_binds_against_the_public_signature(
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    sg = _fake_client([])
    export_page = cast('Callable[..., object]', sg.export_page)

    with pytest.raises(TypeError, match=r'^Shotgun\.export_page\(\)'):
        export_page(*args, **kwargs)

    assert _fake_transport(sg).requests == []


def test_export_page_builds_exact_payloads_and_preserves_boundaries() -> None:
    sg = _fake_client(
        [
            _make_response('csv,data'),
            _make_response('empty-layout'),
            _make_response('keyword-format'),
            _make_response('all-keywords'),
        ]
    )

    assert sg.export_page(0, 'csv') == 'csv,data'
    assert sg.export_page(12, 'csv', layout_name='') == 'empty-layout'
    assert sg.export_page(13, format='csv') == 'keyword-format'
    assert sg.export_page(page_id=14, format='csv', layout_name='summary') == 'all-keywords'

    assert _sent_payload(sg)['method_name'] == 'export_page'
    assert _params(sg) == {'page_id': 0, 'format': 'csv'}
    assert _params(sg, 1) == {'page_id': 12, 'format': 'csv', 'layout_name': ''}
    assert _params(sg, 2) == {'page_id': 13, 'format': 'csv'}
    assert _params(sg, 3) == {'page_id': 14, 'format': 'csv', 'layout_name': 'summary'}


@pytest.mark.parametrize(
    ('page_id', 'page_format', 'message'),
    [
        ('12', 'csv', 'export_page page_id must be an integer'),
        (True, 'csv', 'export_page page_id must be an integer'),
        (12, '', 'export_page format must be a nonempty string'),
        (12, 1, 'export_page format must be a nonempty string'),
    ],
)
def test_export_page_validates_before_readiness(
    page_id: object,
    page_format: object,
    message: str,
) -> None:
    sg = _fake_client([])
    export_page = cast('Callable[[object, object], object]', sg.export_page)

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$'):
        export_page(page_id, page_format)

    assert _fake_transport(sg).requests == []


def test_misc_methods_expose_exact_signatures() -> None:
    expected = {
        'work_schedule_read': (
            ('self', 'start_date', 'end_date', 'project', 'user'),
            (inspect.Parameter.empty,) * 3 + (None, None),
            (
                inspect.Parameter.empty,
                datetime.date | str,
                datetime.date | str,
                dict[str, Any] | None,
                dict[str, Any] | None,
            ),
            dict[str, object],
        ),
        'work_schedule_update': (
            (
                'self',
                'date',
                'working',
                'description',
                'project',
                'user',
                'recalculate_field',
            ),
            (inspect.Parameter.empty,) * 3 + (None, None, None, None),
            (
                inspect.Parameter.empty,
                datetime.date | str,
                bool,
                str | None,
                dict[str, Any] | None,
                dict[str, Any] | None,
                str | None,
            ),
            dict[str, object],
        ),
        'update_project_last_accessed': (
            ('self', 'project', 'user'),
            (inspect.Parameter.empty, inspect.Parameter.empty, None),
            (inspect.Parameter.empty, dict[str, Any], dict[str, Any] | None),
            None,
        ),
        'preferences_read': (
            ('self', 'prefs'),
            (inspect.Parameter.empty, None),
            (inspect.Parameter.empty, list[str] | None),
            dict[str, object],
        ),
        'user_subscriptions_read': (
            ('self',),
            (inspect.Parameter.empty,),
            (inspect.Parameter.empty,),
            list[dict[str, object]],
        ),
        'user_subscriptions_create': (
            ('self', 'users'),
            (inspect.Parameter.empty, inspect.Parameter.empty),
            (inspect.Parameter.empty, list[dict[str, Any]]),
            bool,
        ),
        'export_page': (
            ('self', 'page_id', 'format', 'layout_name'),
            (inspect.Parameter.empty,) * 3 + (None,),
            (inspect.Parameter.empty, int, str, str | None),
            str,
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


def test_misc_scalar_methods_validate_exact_results() -> None:
    create_sg = _fake_client([_make_response(1)])
    export_sg = _fake_client([_make_response({'sensitive': 'value'})])

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^user_subscriptions_create returned an invalid result mapping$',
    ):
        create_sg.user_subscriptions_create([])
    with pytest.raises(_errors.ShotgunError, match=r'^export_page did not return a string$'):
        export_sg.export_page(12, 'csv')


def test_preferences_and_subscriptions_build_exact_payloads() -> None:
    prefs = ['timezone', 'thumbnail_visibility']
    users = [{'humanUserId': '7', 'subscription': ['original'], 'label': 'extra'}]
    original = copy.deepcopy((prefs, users))
    sg = _fake_client(
        [
            _make_response({'timezone': 'UTC'}),
            _make_response({'all': True}),
            _make_response({}),
            _make_response({'7': None}),
        ]
    )

    assert sg.preferences_read(prefs) == {'timezone': 'UTC'}
    assert sg.preferences_read() == {'all': True}
    assert sg.user_subscriptions_read() == []
    assert sg.user_subscriptions_create(users) is True

    assert _sent_payload(sg)['method_name'] == 'preferences_read'
    assert _params(sg) == {'prefs': prefs}
    assert _params(sg, 1) == {'prefs': []}
    assert _sent_payload(sg, 2)['method_name'] == 'user_subscriptions_read'
    assert _params(sg, 2) is None
    assert _sent_payload(sg, 3)['method_name'] == 'user_subscriptions_create'
    assert _params(sg, 3) == {'7': ['original']}
    assert (prefs, users) == original


def test_preferences_read_forwards_and_detaches_truthy_values() -> None:
    prefs = {'nested': ['original']}
    sg = _fake_client([_make_response({})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            prefs['nested'][0] = 'changed'

    sg.config.on_response = mutate_after_readiness
    preferences_read = cast('Callable[[object], object]', sg.preferences_read)

    preferences_read(prefs)

    assert _params(sg) == {'prefs': {'nested': ['original']}}


@pytest.mark.parametrize('prefs', [False, 0, '', ()])
def test_preferences_read_maps_general_falsy_values_to_empty_list(prefs: object) -> None:
    sg = _fake_client([_make_response({})])
    preferences_read = cast('Callable[[object], object]', sg.preferences_read)

    preferences_read(prefs)

    assert _params(sg) == {'prefs': []}


def test_subscriptions_builder_preserves_order_and_copies_lists() -> None:
    plans = ['reviewer', 'artist']
    users = [
        {'humanUserId': 9, 'subscription': plans, 'label': 'ignored'},
        {'humanUserId': '7', 'subscription': 'artist'},
    ]

    params = _methods.build_user_subscriptions_create_params(users)
    plans.reverse()

    assert list(params) == ['9', '7']
    assert params == {'9': ['reviewer', 'artist'], '7': 'artist'}
    assert _methods.build_user_subscriptions_create_params([]) == {}


def test_subscriptions_create_result_accepts_mapping_and_validates_all_keys() -> None:
    assert _methods.require_user_subscriptions_create_result(UserDict({'7': None})) is True
    for result in ({7: None}, {'7': 'sensitive', 9: None}):
        with pytest.raises(
            _errors.ShotgunError,
            match=r'^user_subscriptions_create returned an invalid result mapping$',
        ):
            _methods.require_user_subscriptions_create_result(result)


@pytest.mark.parametrize('integer_ids', [False, True])
def test_subscriptions_forward_accepted_values_and_detach_entries(integer_ids: bool) -> None:
    identifiers = (7, 8, 9, 10) if integer_ids else ('7', '8', '9', '10')
    users: list[dict[str, Any]] = [
        {'humanUserId': identifiers[0], 'subscription': None},
        {'humanUserId': identifiers[1], 'subscription': 'artist'},
        {'humanUserId': identifiers[2], 'subscription': ['original'], 'label': ['extra']},
        {'humanUserId': identifiers[3], 'subscription': []},
    ]
    sg = _fake_client([_make_response({'7': None, '8': None, '9': None, '10': None})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            users[0]['humanUserId'] = 'changed'
            users[1]['subscription'] = 'changed'
            cast('list[str]', users[2]['subscription'])[0] = 'changed'
            cast('list[str]', users[2]['label'])[0] = 'changed'

    sg.config.on_response = mutate_after_readiness

    assert sg.user_subscriptions_create(users) is True

    assert _params(sg) == {'7': None, '8': 'artist', '9': ['original'], '10': []}


@pytest.mark.parametrize('identifiers', [('7', '7'), (7, 7), ('7', 7), (7, '7')])
def test_subscriptions_reject_duplicate_users_before_readiness(
    identifiers: tuple[str | int, str | int],
) -> None:
    sg = _fake_client([])
    users = [{'humanUserId': item, 'subscription': 'sensitive'} for item in identifiers]

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^user_subscriptions_create users must not contain duplicate user IDs$',
    ):
        sg.user_subscriptions_create(users)

    assert _fake_transport(sg).requests == []


@pytest.mark.parametrize(
    ('result', 'expected'),
    [
        ({}, True),
        ({'7': None, '9': None}, True),
        ({'7': None, '9': 'sensitive'}, False),
        ({'7': False}, False),
        ({'7': 0}, False),
        ({'7': ''}, False),
        ({'7': []}, False),
        ({'7': {}}, False),
    ],
)
def test_subscriptions_create_interprets_per_user_results(result: object, expected: bool) -> None:
    sg = _fake_client([_make_response(result)])

    assert sg.user_subscriptions_create([]) is expected


@pytest.mark.parametrize('result', [True, False, None, 1, 'sensitive', [], [None]])
def test_subscriptions_create_rejects_non_mapping_results(result: object) -> None:
    sg = _fake_client([_make_response(result)])

    with pytest.raises(
        _errors.ShotgunError,
        match=r'^user_subscriptions_create returned an invalid result mapping$',
    ):
        sg.user_subscriptions_create([])


@pytest.mark.parametrize(
    ('argument', 'message'),
    [
        (
            {'humanUserId': '7', 'subscription': 'artist'},
            'user_subscriptions_create users must be a list',
        ),
        (
            [{'humanUserId': '7', 'subscription': 'artist'}, 'sensitive-member'],
            _CREATE_ENTRY_ERROR,
        ),
        (
            [{'subscription': 'artist'}],
            _CREATE_ENTRY_ERROR,
        ),
        (
            [{'user_id': '7', 'subscription': 'artist'}],
            _CREATE_ENTRY_ERROR,
        ),
        (
            [{'humanUserId': '7'}],
            _CREATE_ENTRY_ERROR,
        ),
        (
            [{'humanUserId': 7.0, 'subscription': 'artist'}],
            'user_subscriptions_create users must contain string or integer humanUserId values',
        ),
        (
            [{'humanUserId': True, 'subscription': 'artist'}],
            'user_subscriptions_create users must contain string or integer humanUserId values',
        ),
        (
            [{'humanUserId': False, 'subscription': 'artist'}],
            'user_subscriptions_create users must contain string or integer humanUserId values',
        ),
        (
            [{'humanUserId': None, 'subscription': 'artist'}],
            'user_subscriptions_create users must contain string or integer humanUserId values',
        ),
        (
            [{'humanUserId': '7', 'subscription': 1}],
            'user_subscriptions_create users must contain valid subscription values',
        ),
        (
            [{'humanUserId': '7', 'subscription': True}],
            'user_subscriptions_create users must contain valid subscription values',
        ),
        (
            [{'humanUserId': '7', 'subscription': {'sensitive': 'value'}}],
            'user_subscriptions_create users must contain valid subscription values',
        ),
        (
            [{'humanUserId': '7', 'subscription': ['artist', 2]}],
            'user_subscriptions_create users must contain valid subscription values',
        ),
    ],
)
def test_subscriptions_validate_completely_before_readiness(
    argument: object,
    message: str,
) -> None:
    sg = _fake_client([])
    method = cast('Callable[[object], object]', sg.user_subscriptions_create)

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$') as error:
        method(argument)

    assert 'sensitive' not in str(error.value)
    assert _fake_transport(sg).requests == []


def test_update_project_last_accessed_builds_exact_payloads() -> None:
    sg = _fake_client([_make_response(None), _make_response(None)])

    assert sg.update_project_last_accessed(_PROJECT, _USER) is None
    assert sg.update_project_last_accessed(_PROJECT) is None

    assert _sent_payload(sg)['method_name'] == 'update_project_last_accessed_by_current_user'
    assert _sent_payload(sg, 1)['method_name'] == 'update_project_last_accessed_by_current_user'
    assert _params(sg) == {'project_id': 2, 'user_id': 7}
    assert _params(sg, 1) == {'project_id': 2}


@pytest.mark.parametrize('acknowledgement', [False, {'updated': True}, 'ok'])
def test_update_project_last_accessed_discards_wire_acknowledgement(
    acknowledgement: object,
) -> None:
    sg = _fake_client([_make_response(acknowledgement)])

    assert sg.update_project_last_accessed(_PROJECT) is None


def test_update_project_last_accessed_forwards_and_owns_id_values() -> None:
    project_id = ['original-project']
    user_id = {'nested': ['original-user']}
    project = {'id': project_id}
    user = {'id': user_id}
    sg = _fake_client([_make_response({'updated': True})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            project_id[0] = 'changed-project'
            user_id['nested'][0] = 'changed-user'

    sg.config.on_response = mutate_after_readiness
    method = cast('Callable[[object, object], object]', sg.update_project_last_accessed)

    assert method(project, user) is None
    assert _params(sg) == {
        'project_id': ['original-project'],
        'user_id': {'nested': ['original-user']},
    }
    assert _sent_payload(sg)['method_name'] == 'update_project_last_accessed_by_current_user'


def test_update_project_last_accessed_is_logical_write_without_retry() -> None:
    events: list[ObserverEvent] = []
    sg = _fake_client([SendError('ambiguous'), _make_response(None)])
    sg.config.on_request = events.append
    sg.config.on_response = events.append
    sg.config.on_retry = events.append

    with pytest.raises(SendError, match='ambiguous'):
        sg.update_project_last_accessed(_PROJECT)

    operation_events = [
        event
        for event in events
        if isinstance(event, (RequestEvent, ResponseEvent, RetryEvent))
        and event.operation == 'write'
    ]
    assert sum(isinstance(event, RequestEvent) for event in operation_events) == 1
    assert not any(isinstance(event, RetryEvent) for event in operation_events)
    assert all(event.operation == 'write' for event in operation_events)
    assert len(_fake_transport(sg).requests) == 2
    assert _sent_payload(sg)['method_name'] == 'update_project_last_accessed_by_current_user'


def test_user_subscriptions_read_copies_list_values_at_conversion_boundary() -> None:
    source = ['artist']

    result = _methods.require_user_subscriptions_result({'user-A': source})
    cast('list[str]', result[0]['subscription']).append('reviewer')

    assert source == ['artist']


def test_user_subscriptions_read_preserves_keys_and_values_in_server_order() -> None:
    sg = _fake_client(
        [_make_response({'user-A': None, '2': 'artist', '03': ['reviewer'], '4': []})]
    )

    assert sg.user_subscriptions_read() == [
        {'humanUserId': 'user-A', 'subscription': None},
        {'humanUserId': '2', 'subscription': 'artist'},
        {'humanUserId': '03', 'subscription': ['reviewer']},
        {'humanUserId': '4', 'subscription': []},
    ]


@pytest.mark.parametrize(
    ('result', 'message_suffix'),
    [
        (['sensitive-result'], 'did not return a dictionary'),
        ({'': 'artist'}, 'returned an invalid user ID'),
        ({'7': {'sensitive': 'value'}}, 'returned an invalid subscription'),
        ({'7': 1}, 'returned an invalid subscription'),
        ({'7': True}, 'returned an invalid subscription'),
        ({'7': ['artist', 2]}, 'returned an invalid subscription'),
    ],
)
def test_user_subscriptions_read_rejects_malformed_results(
    result: object,
    message_suffix: str,
) -> None:
    sg = _fake_client([_make_response(result)])

    with pytest.raises(
        _errors.ShotgunError,
        match=rf'^user_subscriptions_read {message_suffix}$',
    ) as error:
        sg.user_subscriptions_read()

    assert 'sensitive' not in str(error.value)


@pytest.mark.parametrize('user_id', [True, 7])
def test_user_subscriptions_read_rejects_non_string_keys_at_conversion_boundary(
    user_id: object,
) -> None:
    with pytest.raises(
        _errors.ShotgunError,
        match=r'^user_subscriptions_read returned an invalid user ID$',
    ):
        _methods.require_user_subscriptions_result({user_id: 'artist'})


@pytest.mark.parametrize(
    ('method_name', 'parameter_names'),
    [
        ('export_page', ('page_id', 'format', 'layout_name')),
        (
            'work_schedule_update',
            ('date', 'working', 'description', 'project', 'user', 'recalculate_field'),
        ),
    ],
)
def test_variadic_public_docstrings_name_documented_parameters(
    method_name: str,
    parameter_names: tuple[str, ...],
) -> None:
    docstring = inspect.getdoc(getattr(Shotgun, method_name))

    assert docstring is not None
    for parameter_name in parameter_names:
        assert f'\n    {parameter_name}:' in f'\n{docstring}'
    assert '\n    *args:' not in f'\n{docstring}'
    assert '\n    **kwargs:' not in f'\n{docstring}'


def test_work_schedule_methods_build_exact_payloads_and_own_entities() -> None:
    project = {**_PROJECT, 'name': ['original'], 'sg_status': 'Active'}
    user = {**_USER, 'name': ['original'], 'login': 'me'}
    original = copy.deepcopy((project, user))
    sg = _fake_client([_make_response({}), _make_response({}), _make_response({})])

    sg.work_schedule_read(datetime.date(2026, 7, 1), '2026-07-31', project=project)
    sg.work_schedule_read('2026-07-01', datetime.date(2026, 7, 31), user=user)
    sg.work_schedule_update(
        '2026-07-04',
        working=False,
        description='',
        recalculate_field='',
    )

    assert _sent_payload(sg)['method_name'] == 'work_schedule_read'
    assert _params(sg) == {
        'start_date': '2026-07-01',
        'end_date': '2026-07-31',
        'project': {'type': 'Project', 'id': 2, 'name': ['original']},
    }
    assert _params(sg, 1) == {
        'start_date': '2026-07-01',
        'end_date': '2026-07-31',
        'user': {'type': 'HumanUser', 'id': 7, 'name': ['original']},
    }
    assert _sent_payload(sg, 2)['method_name'] == 'work_schedule_update'
    assert _params(sg, 2) == {
        'date': '2026-07-04',
        'working': False,
        'description': '',
        'recalculate_field': '',
    }
    assert (project, user) == original


def test_work_schedule_methods_optimize_or_forward_entities_without_id_validation() -> None:
    sg = _fake_client([_make_response({}), _make_response({})])
    work_schedule_read = cast('Callable[..., object]', sg.work_schedule_read)
    work_schedule_update = cast('Callable[..., object]', sg.work_schedule_update)

    work_schedule_read('2026-07-01', '2026-07-31', project=[])
    work_schedule_update(
        '2026-07-04',
        working=False,
        user={'type': 'HumanUser', 'id': True, 'login': 'omit'},
    )

    assert _params(sg) == {
        'start_date': '2026-07-01',
        'end_date': '2026-07-31',
        'project': [],
    }
    assert _params(sg, 1) == {
        'date': '2026-07-04',
        'working': False,
        'user': {'type': 'HumanUser', 'id': True},
    }


@pytest.mark.parametrize(
    ('method_name', 'args', 'kwargs', 'message'),
    [
        (
            'work_schedule_read',
            (datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC), '2026-07-31'),
            {},
            'start_date must be a date, not a datetime',
        ),
        (
            'work_schedule_read',
            ('2026-7-01', '2026-07-31'),
            {},
            'start_date must be YYYY-MM-DD',
        ),
        (
            'work_schedule_read',
            ('2026-02-30', '2026-07-31'),
            {},
            'start_date must be YYYY-MM-DD',
        ),
        (
            'work_schedule_read',
            (20260701, '2026-07-31'),
            {},
            'start_date must be a datetime.date or YYYY-MM-DD string',
        ),
        (
            'work_schedule_update',
            ('2026-07-04T00:00:00', False),
            {},
            'date must be YYYY-MM-DD',
        ),
        (
            'work_schedule_update',
            ('2026-07-04', False),
            {'project': _PROJECT, 'user': _USER},
            'project and user can only be used exclusively of each other',
        ),
    ],
)
def test_work_schedule_methods_validate_before_readiness(
    method_name: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    sg = _fake_client([])
    method = cast('Callable[..., object]', getattr(sg, method_name))

    with pytest.raises(_errors.ShotgunError, match=f'^{message}$') as error:
        method(*args, **kwargs)

    assert '2026' not in str(error.value)
    assert _fake_transport(sg).requests == []


def test_work_schedule_project_is_detached_before_readiness() -> None:
    project = {**_PROJECT, 'name': ['original-project']}
    sg = _fake_client([_make_response({})])

    def mutate_after_readiness(event: object) -> None:
        if isinstance(event, ResponseEvent):
            cast('list[str]', project['name'])[0] = 'changed-project'

    sg.config.on_response = mutate_after_readiness

    sg.work_schedule_read('2026-07-01', '2026-07-31', project=project)

    assert project['name'] == ['changed-project']
    assert cast('dict[str, object]', _params(sg))['project'] == {
        'type': 'Project',
        'id': 2,
        'name': ['original-project'],
    }


def test_work_schedule_read_accepts_project_and_user_together() -> None:
    sg = _fake_client([_make_response({'working_days': []})])

    assert sg.work_schedule_read('2026-07-01', '2026-07-31', project=_PROJECT, user=_USER) == {
        'working_days': []
    }
    assert _params(sg) == {
        'start_date': '2026-07-01',
        'end_date': '2026-07-31',
        'project': {'type': 'Project', 'id': 2, 'name': 'P'},
        'user': {'type': 'HumanUser', 'id': 7, 'name': 'Me'},
    }


@pytest.mark.parametrize(
    ('args', 'kwargs', 'message'),
    [
        ((), {}, "missing a required argument: 'date'"),
        (('2026-07-04',), {}, "missing a required argument: 'working'"),
        (
            ('2026-07-04', False),
            {'working': True},
            "multiple values for argument 'working'",
        ),
        (
            ('2026-07-04', False, None, None, None, None, 'extra'),
            {},
            'too many positional arguments',
        ),
        (
            ('2026-07-04', False),
            {'unexpected': 'value'},
            "got an unexpected keyword argument 'unexpected'",
        ),
    ],
)
def test_work_schedule_update_binds_against_the_public_signature(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    sg = _fake_client([])
    work_schedule_update = cast('Callable[..., object]', sg.work_schedule_update)

    with pytest.raises(TypeError, match=r'^Shotgun\.work_schedule_update\(\)') as error:
        work_schedule_update(*args, **kwargs)

    assert message in str(error.value)
    assert _fake_transport(sg).requests == []


def test_work_schedule_update_includes_all_optional_values() -> None:
    sg = _fake_client([_make_response({})])

    sg.work_schedule_update(
        datetime.date(2026, 7, 4),
        working=True,
        description='holiday',
        project=_PROJECT,
        recalculate_field='sg_is_working_day',
    )

    assert _params(sg) == {
        'date': '2026-07-04',
        'working': True,
        'description': 'holiday',
        'project': _PROJECT,
        'recalculate_field': 'sg_is_working_day',
    }
