from copy import deepcopy
from datetime import timedelta
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun import MockgunError
from tests.conftest import entity_id
from tests.lib._wire_recordings import recording, response

from .conftest import QUERY_NOW, FakeSiteCase, raw_call, seed

pytestmark = pytest.mark.anyio


def _scene(case: FakeSiteCase) -> tuple[dict[str, object], dict[str, object]]:
    return seed(case, 'Project', {'name': 'P'}), seed(
        case, 'HumanUser', {'login': 'artist', 'name': 'artist', 'sg_status_list': 'act'}
    )


async def test_canned_fault_and_missing_response(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    case.client.site.set_canned_fault(
        'export_page', {'page_id': 6, 'format': 'csv'}, 'not API exportable', 103
    )
    with pytest.raises(Fault, match='not API exportable'):
        await case.call('export_page', 6, 'csv')
    assert (
        raw_call(case.client._server, 'export_page', {'page_id': 6, 'format': 'csv'})['error_code']
        == 103
    )
    with pytest.raises(MockgunError, match=r"export_page.*'page_id': 7"):
        await case.call('export_page', 7, 'csv')
    with pytest.raises(ValueError, match='unsupported canned method'):
        case.client.site.set_canned_response('find', {}, [])


@pytest.mark.parametrize(
    'operation',
    [
        'filter',
        'order',
        'summary',
        'group',
        'create',
        'update',
        'deep_filter',
        'deep_order',
        'deep_summary',
        'deep_group',
    ],
)
async def test_last_access_guards(workflow_site: FakeSiteCase, operation: str) -> None:
    case = workflow_site
    project, _ = _scene(case)
    seed(case, 'Shot', {'code': 'S', 'project': project})
    field = 'last_accessed_by_current_user'
    kind = 'Shot' if operation.startswith('deep_') else 'Project'
    path = f'project.Project.{field}' if kind == 'Shot' else field
    args: list[object]
    kwargs: dict[str, object] = {}
    if operation.endswith('filter'):
        method = 'find'
        args = [kind, [[path, 'is', QUERY_NOW]]]
    elif operation.endswith('order'):
        method = 'find'
        args = [kind, []]
        kwargs = {'order': [{'field_name': path, 'direction': 'asc'}]}
    elif operation.endswith('summary'):
        method = 'summarize'
        args = [kind, [], [{'field': path, 'type': 'count'}]]
    elif operation.endswith('group'):
        method = 'summarize'
        args = [kind, [], [{'field': 'id', 'type': 'count'}]]
        kwargs = {'grouping': [{'field': path, 'type': 'exact', 'direction': 'asc'}]}
    elif operation == 'create':
        method = 'create'
        args = ['Project', {'name': 'x', field: QUERY_NOW}]
    else:
        method = 'update'
        args = ['Project', project['id'], {field: QUERY_NOW}]
    with pytest.raises(MockgunError, match=field):
        await case.call(method, *args, **kwargs)


@pytest.mark.parametrize(
    ('method', 'params', 'args'),
    [
        ('nav_expand', {'path': '/Project/1'}, ['/Project/1']),
        (
            'nav_search_string',
            {'root_path': '/Project/1', 'search_criteria': {'search_string': 'abc'}},
            ['/Project/1', 'abc'],
        ),
        (
            'nav_search_entity',
            {'root_path': '/Project/1', 'search_criteria': {'entity': {'type': 'Asset', 'id': 1}}},
            ['/Project/1', {'type': 'Asset', 'id': 1}],
        ),
        ('export_page', {'page_id': 5, 'format': 'csv'}, [5, 'csv']),
    ],
)
async def test_canned_responses_are_exact_detached_and_replace_equal_params(
    workflow_site: FakeSiteCase, method: str, params: dict[str, object], args: list[object]
) -> None:
    case = workflow_site
    params = deepcopy(params)
    fixture_response = 'a,b\n' if method == 'export_page' else response(recording(method))
    case.client.site.set_canned_response(method, params, fixture_response)
    assert await case.call(method, *args) == fixture_response
    replacement: object = (
        'new\n' if method == 'export_page' else {} if method == 'nav_expand' else []
    )
    case.client.site.set_canned_response(method, dict(reversed(list(params.items()))), replacement)
    assert await case.call(method, *args) == replacement
    params['extra'] = True
    assert await case.call(method, *args) == replacement


async def test_last_access_is_per_actor_and_can_attribute_a_human(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project, human = _scene(case)
    sudo = FakeSiteCase(case.kind, case.client.new_client(sudo_as_login='artist'))
    field = 'last_accessed_by_current_user'
    try:
        assert await case.call('update_project_last_accessed', project) is None
        assert await case.call('find_one', 'Project', [], [field]) == {**project, field: QUERY_NOW}
        with pytest.raises(MockgunError, match=field):
            await sudo.call('find', 'Project', [], [field])
        case.client.site.set_time(QUERY_NOW + timedelta(seconds=3))
        await sudo.call('update_project_last_accessed', project)
        assert await sudo.call('find_one', 'Project', [], [field]) == {
            **project,
            field: QUERY_NOW + timedelta(seconds=3),
        }
        assert await case.call('find_one', 'Project', [], [field]) == {**project, field: QUERY_NOW}
        case.client.site.set_time(QUERY_NOW + timedelta(seconds=5))
        await case.call('update_project_last_accessed', project, user=human)
        assert await sudo.call('find_one', 'Project', [], [field]) == {
            **project,
            field: QUERY_NOW + timedelta(seconds=5),
        }
        shot = seed(case, 'Shot', {'code': 'S', 'project': project})
        deep = f'project.Project.{field}'
        assert await case.call('find_one', 'Shot', [], [deep]) == {**shot, deep: QUERY_NOW}
    finally:
        await sudo.call('close')
    with pytest.raises(MockgunError):
        await case.call('find', 'EventLogEntry', [])
    with pytest.raises(MockgunError, match='activity_stream'):
        await case.call('activity_stream_read', 'Project', project['id'])


async def test_missing_or_retired_site_rpc_targets_raise(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, human = _scene(case)
    for target in ({'type': 'Project', 'id': 999},):
        with pytest.raises(MockgunError, match='Project 999'):
            await case.call('update_project_last_accessed', target)
        with pytest.raises(MockgunError, match='Project 999'):
            await case.call('work_schedule_update', '2026-08-20', working=False, project=target)
    with pytest.raises(MockgunError, match='HumanUser 999'):
        await case.call(
            'update_project_last_accessed', project, user={'type': 'HumanUser', 'id': 999}
        )
    await case.call('delete', 'HumanUser', human['id'])
    with pytest.raises(MockgunError, match='HumanUser'):
        await case.call('update_project_last_accessed', project, user=human)
    with pytest.raises(MockgunError, match='HumanUser'):
        await case.call(
            'user_subscriptions_create', [{'humanUserId': human['id'], 'subscription': 'a'}]
        )
    await case.call('delete', 'Project', project['id'])
    with pytest.raises(MockgunError, match='Project'):
        await case.call('update_project_last_accessed', project)
    with pytest.raises(MockgunError, match='Project'):
        await case.call('work_schedule_update', '2026-08-20', working=False, project=project)


async def test_nested_canned_response_is_detached_on_each_call(
    workflow_site: FakeSiteCase,
) -> None:
    params = {'path': '/Project/1'}
    original = {'groups': [{'name': 'original', 'items': [1, {'id': 2}]}]}
    workflow_site.client.site.set_canned_response('nav_expand', params, original)
    groups = cast('list[dict[str, object]]', original['groups'])
    groups[0]['name'] = 'changed before read'
    expected = {'groups': [{'name': 'original', 'items': [1, {'id': 2}]}]}
    first = cast('dict[str, object]', await workflow_site.call('nav_expand', '/Project/1'))
    assert first == expected
    first_groups = cast('list[dict[str, object]]', first['groups'])
    cast('dict[str, object]', cast('list[object]', first_groups[0]['items'])[1])['id'] = 3
    second = cast('dict[str, object]', await workflow_site.call('nav_expand', '/Project/1'))
    assert second == expected
    second_groups = cast('list[dict[str, object]]', second['groups'])
    second_groups.clear()
    assert await workflow_site.call('nav_expand', '/Project/1') == expected


async def test_preferences_are_explicit_ordered_and_detached(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    for names, key in (([], 'format_date_fields'), (['hours_per_day'], 'hours_per_day')):
        with pytest.raises(MockgunError, match=key):
            await case.call('preferences_read', names)
    recorded = cast('dict[str, object]', response(recording('preferences_read')))
    case.client.site.set_preferences(recorded)
    actual = cast('dict[str, object]', await case.call('preferences_read'))
    assert actual == recorded
    assert list(actual) == list(recorded)
    assert await case.call('preferences_read', ['format_date_fields']) == {
        'format_date_fields': '%Y-%m-%d'
    }
    assert await case.call('preferences_read', ['mgp_no_such_pref']) == {}
    custom: dict[str, object] = {'custom_pref': ['x']}
    case.client.site.set_preferences(custom)
    cast('list[str]', custom['custom_pref']).append('changed')
    assert await case.call('preferences_read', ['custom_pref']) == {'custom_pref': ['x']}
    assert 'custom_pref' not in cast('dict[str, object]', await case.call('preferences_read'))
    with pytest.raises(TypeError):
        case.client.site.set_preferences({'x': object()})


async def test_project_exception_replaces_rule_and_taints_events(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    project, _ = _scene(case)
    for description, working in (('off', False), ('again', False), ('default', True)):
        assert await case.call(
            'work_schedule_update', '2026-08-20', working, description, project=project
        ) == {
            'date': '2026-08-20',
            'working': working,
            'description': description,
            'project': {**project, 'name': 'P'},
            'user': None,
        }
        assert await case.call(
            'work_schedule_read', '2026-08-20', '2026-08-20', project=project
        ) == {
            '2026-08-20': {
                'working': working,
                'description': description,
                'reason': 'PROJECT_EXCEPTION',
            }
        }
        rules = cast(
            'list[dict[str, object]]',
            await case.call(
                'find',
                'WorkDayRule',
                [['project', 'is', project]],
                ['start_date', 'end_date', 'working', 'description', 'user'],
            ),
        )
        assert len(rules) == 1
        assert rules[0]['start_date'] == rules[0]['end_date'] == '2026-08-20'
        assert rules[0]['user'] is None
        for field in ('precedence', 'day_of_week'):
            with pytest.raises(MockgunError, match=field):
                await case.call('find', 'WorkDayRule', [], [field])
    retired = cast('list[object]', await case.call('find', 'WorkDayRule', [], retired_only=True))
    assert len(retired) == 2
    assert await case.call('work_schedule_read', '2026-08-20', '2026-08-20') == {
        '2026-08-20': {'working': True, 'description': None, 'reason': 'STUDIO_WORK_WEEK'}
    }
    with pytest.raises(MockgunError):
        await case.call('find', 'EventLogEntry', [])
    with pytest.raises(MockgunError, match='activity_stream'):
        await case.call('activity_stream_read', 'Project', project['id'])


@pytest.mark.parametrize('existing_rule', [False, True], ids=['new', 'replace'])
async def test_schedule_name_failure_preserves_rules(
    workflow_site: FakeSiteCase, existing_rule: bool
) -> None:
    case = workflow_site
    project = seed(case, 'Project', {'name': None})
    if existing_rule:
        seed(
            case,
            'WorkDayRule',
            {
                'start_date': '2026-08-20',
                'end_date': '2026-08-20',
                'working': True,
                'description': 'original',
                'project': project,
            },
        )
    store = case.client._server.store
    before = deepcopy(store.rows('WorkDayRule', retired=False))

    with pytest.raises(MockgunError, match=r'Project\.name.*display name'):
        await case.call('work_schedule_update', '2026-08-20', working=False, project=project)

    assert store.rows('WorkDayRule', retired=False) == before


@pytest.mark.parametrize(
    ('rpc', 'params', 'key'),
    [
        ('preferences_read', {}, 'prefs'),
        ('preferences_read', {'prefs': [1]}, 'prefs'),
        ('work_schedule_read', {'start_date': 'bad', 'end_date': '2026-07-15'}, 'start_date'),
        ('work_schedule_read', {'start_date': '2026-07-15'}, 'end_date'),
        ('work_schedule_update', {'date': '2026-07-15', 'working': 1}, 'working'),
        ('work_schedule_update', {'working': True}, 'date'),
        ('user_subscriptions_read', {}, 'params'),
        ('user_subscriptions_create', {'1': 42}, '1'),
        ('update_project_last_accessed_by_current_user', {'project_id': True}, 'project_id'),
        ('update_project_last_accessed_by_current_user', {}, 'project_id'),
        ('nav_expand', {}, 'path'),
        ('nav_expand', {'path': 1}, 'path'),
        ('nav_search', {'root_path': '/Project/1', 'search_criteria': []}, 'search_criteria'),
        ('nav_search', {'search_criteria': {'search_string': 'a'}}, 'root_path'),
        ('export_page', {'page_id': True, 'format': 'csv'}, 'page_id'),
        ('export_page', {'page_id': 1}, 'format'),
    ],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_rpc_parameter_shapes(
    workflow_site: FakeSiteCase, rpc: str, params: object, key: str
) -> None:
    with pytest.raises(MockgunError, match=rf'{rpc}.*{key}'):
        raw_call(workflow_site.client._server, rpc, params)


@pytest.mark.parametrize(
    'rpc',
    [
        'preferences_read',
        'work_schedule_read',
        'work_schedule_update',
        'user_subscriptions_read',
        'user_subscriptions_create',
        'update_project_last_accessed_by_current_user',
        'nav_expand',
        'nav_search',
        'export_page',
    ],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_rpc_rejects_nonmapping_and_extra_parameters(
    workflow_site: FakeSiteCase, rpc: str
) -> None:
    invalid: list[object] = [[], {'unrecognized': True}]
    for params in invalid:
        with pytest.raises(MockgunError, match=rpc):
            raw_call(workflow_site.client._server, rpc, params)


@pytest.mark.parametrize('rule_kind', ['span', 'duplicate', 'user'])
async def test_schedule_rejects_ambiguous_covering_rules(
    workflow_site: FakeSiteCase, rule_kind: str
) -> None:
    case = workflow_site
    project, human = _scene(case)
    fields: dict[str, object] = {
        'start_date': '2026-08-20',
        'end_date': '2026-08-21' if rule_kind == 'span' else '2026-08-20',
        'working': False,
        'project': project,
    }
    if rule_kind == 'user':
        fields['user'] = human
    seed(case, 'WorkDayRule', fields)
    if rule_kind == 'duplicate':
        seed(case, 'WorkDayRule', fields)
    with pytest.raises(MockgunError, match='work_schedule_read WorkDayRule'):
        await case.call('work_schedule_read', '2026-08-20', '2026-08-20', project=project)


@pytest.mark.parametrize(
    ('rpc', 'params', 'key'),
    [
        (
            'work_schedule_read',
            {
                'start_date': '2026-08-20',
                'end_date': '2026-08-20',
                'project': {'type': 'Project', 'id': True},
            },
            'project',
        ),
        (
            'work_schedule_update',
            {'date': '2026-08-20', 'working': False, 'description': []},
            'description',
        ),
        (
            'update_project_last_accessed_by_current_user',
            {'project_id': 1, 'user_id': True},
            'user_id',
        ),
        ('nav_expand', {'path': '/', 'entity_fields': {'Asset': [1]}}, 'entity_fields'),
        ('nav_expand', {'path': '/', 'seed_entity_field': []}, 'seed_entity_field'),
        (
            'nav_search',
            {'root_path': '/', 'search_criteria': {'entity': {'type': 'Asset', 'id': True}}},
            'search_criteria',
        ),
        (
            'nav_search',
            {'root_path': '/', 'search_criteria': {'search_string': []}},
            'search_string',
        ),
        ('export_page', {'page_id': 1, 'format': 'csv', 'layout_name': []}, 'layout_name'),
    ],
)
@pytest.mark.parametrize('workflow_site', ['sync'], indirect=True)
async def test_optional_and_nested_parameter_shapes(
    workflow_site: FakeSiteCase, rpc: str, params: dict[str, object], key: str
) -> None:
    _scene(workflow_site)
    with pytest.raises(MockgunError, match=rf'{rpc}.*{key}'):
        raw_call(workflow_site.client._server, rpc, params)


async def test_schedule_unsettled_scopes_and_rules(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    project, human = _scene(case)
    with pytest.raises(MockgunError, match='user'):
        await case.call('work_schedule_update', '2026-08-20', working=False, user=human)
    with pytest.raises(MockgunError, match='project'):
        await case.call('work_schedule_update', '2026-08-20', working=False)
    with pytest.raises(MockgunError, match='user'):
        await case.call('work_schedule_read', '2026-08-20', '2026-08-20', user=human)
    with pytest.raises(MockgunError, match='recalculate_field'):
        await case.call(
            'work_schedule_update',
            '2026-08-20',
            working=False,
            project=project,
            recalculate_field='duration',
        )
    with pytest.raises(MockgunError, match='start_date'):
        await case.call('work_schedule_read', '2026-08-21', '2026-08-20')
    await case.call(
        'create',
        'WorkDayRule',
        {'start_date': '2026-08-20', 'end_date': '2026-08-20', 'working': False},
    )
    with pytest.raises(MockgunError, match='WorkDayRule'):
        await case.call('work_schedule_read', '2026-08-20', '2026-08-20')


async def test_schedule_without_rule_schema_reads_week_but_rejects_update(
    fake_site: FakeSiteCase,
) -> None:
    case = fake_site
    project = seed(case, 'Project', {'name': 'P'})
    assert await case.call('work_schedule_read', '2026-08-20', '2026-08-20', project=project) == {
        '2026-08-20': {'working': True, 'description': None, 'reason': 'STUDIO_WORK_WEEK'}
    }
    with pytest.raises(MockgunError, match='WorkDayRule'):
        await case.call('work_schedule_update', '2026-08-20', working=False, project=project)


async def test_subscriptions_preserve_order_and_only_allow_noops(
    workflow_site: FakeSiteCase,
) -> None:
    case = workflow_site
    _, human = _scene(case)
    second = seed(
        case, 'HumanUser', {'login': 'second', 'name': 'second', 'sg_status_list': 'act'}
    )
    identity = entity_id(human)
    values = {entity_id(second): 'b', identity: 'a'}
    case.client.site.set_subscriptions(values)
    values[identity] = 'changed'
    expected = [
        {'humanUserId': str(second['id']), 'subscription': 'b'},
        {'humanUserId': str(identity), 'subscription': 'a'},
    ]
    assert await case.call('user_subscriptions_read') == expected
    assert (
        await case.call(
            'user_subscriptions_create', [{'humanUserId': identity, 'subscription': 'a'}]
        )
        is True
    )
    assert await case.call('user_subscriptions_read') == expected
    with pytest.raises(Fault, match='Invalid humanUserId 999'):
        await case.call('user_subscriptions_create', [{'humanUserId': 999, 'subscription': 'a'}])
    fault = raw_call(case.client._server, 'user_subscriptions_create', {'999': 'a'})
    assert fault['error_code'] is None
    assert 'Invalid humanUserId 999' in str(fault)
    for entries in (
        [{'humanUserId': identity, 'subscription': 'b'}],
        [
            {'humanUserId': 999, 'subscription': 'a'},
            {'humanUserId': identity, 'subscription': 'a'},
        ],
    ):
        with pytest.raises(MockgunError, match='user_subscriptions_create'):
            await case.call('user_subscriptions_create', entries)
    await case.call('delete', 'HumanUser', identity)
    with pytest.raises(MockgunError, match='HumanUser'):
        await case.call('user_subscriptions_read')
    with pytest.raises(TypeError):
        case.client.site.set_subscriptions({True: 'x'})


async def test_week_defaults_and_replacement(workflow_site: FakeSiteCase) -> None:
    case = workflow_site
    rows = cast(
        'dict[str, dict[str, object]]',
        await case.call('work_schedule_read', '2026-07-12', '2026-07-18'),
    )
    assert list(rows) == [f'2026-07-{day}' for day in range(12, 19)]
    assert [row['working'] for row in rows.values()] == [
        False,
        True,
        True,
        True,
        True,
        True,
        False,
    ]
    assert all(
        row['description'] is None and row['reason'] == 'STUDIO_WORK_WEEK' for row in rows.values()
    )
    working = [True] * 7
    case.client.site.set_work_week(working)
    working[0] = False
    rows = cast(
        'dict[str, dict[str, object]]',
        await case.call('work_schedule_read', '2026-07-12', '2026-07-18'),
    )
    assert all(row['working'] is True for row in rows.values())
    for invalid in ([True] * 6, [1] * 7):
        with pytest.raises(ValueError, match='seven booleans'):
            case.client.site.set_work_week(cast('list[bool]', invalid))
