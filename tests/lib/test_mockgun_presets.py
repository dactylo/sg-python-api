from datetime import timedelta
from typing import cast

import pytest

from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._presets import latest_rank_key
from tests.conftest import entity_id
from tests.lib._records import create_link

from .conftest import QUERY_NOW, FakeSiteCase, raw_read, seed


async def _archive_rows(
    case: FakeSiteCase,
) -> tuple[
    dict[str, object], dict[str, object], dict[str, object], dict[str, object], dict[str, object]
]:
    active = await create_link(case, 'Project', {'name': 'Active'})
    archived = await create_link(case, 'Project', {'name': 'Archived', 'archived': True})
    s1 = await create_link(case, 'Shot', {'code': 'S1', 'project': active})
    s2 = await create_link(case, 'Shot', {'code': 'S2', 'project': archived})
    s3 = await create_link(case, 'Shot', {'code': 'S3'})
    return active, archived, s1, s2, s3


async def _ids(case: FakeSiteCase, kind: str, **options: object) -> list[int]:
    rows = cast('list[dict[str, object]]', await case.call('find', kind, [], ['id'], **options))
    return [entity_id(row) for row in rows]


async def _version(
    case: FakeSiteCase, entity: dict[str, object] | None, hours: int | None, **data: object
) -> dict[str, object]:
    return await create_link(
        case,
        'Version',
        {
            'code': 'v',
            'entity': entity,
            'created_at': None if hours is None else QUERY_NOW + timedelta(hours=hours),
            **data,
        },
    )


@pytest.mark.anyio
async def test_archived_project_is_excluded(query_site: FakeSiteCase) -> None:
    active, _, _, _, _ = await _archive_rows(query_site)
    assert await _ids(query_site, 'Project', include_archived_projects=False) == [active['id']]


@pytest.mark.anyio
async def test_archived_project_shot_is_excluded(query_site: FakeSiteCase) -> None:
    _, _, s1, _, s3 = await _archive_rows(query_site)
    assert await _ids(query_site, 'Shot', include_archived_projects=False) == [
        s1['id'],
        s3['id'],
    ]


@pytest.mark.anyio
async def test_other_link_and_deep_filter_retain_archived_target(
    query_site: FakeSiteCase,
) -> None:
    active, archived, _, s2, _ = await _archive_rows(query_site)
    version = await create_link(
        query_site, 'Version', {'code': 'v1', 'project': active, 'entity': s2}
    )
    rows = cast(
        'list[dict[str, object]]',
        await query_site.call('find', 'Version', [], ['entity'], include_archived_projects=False),
    )
    assert rows == [{'type': 'Version', 'id': version['id'], 'entity': {**s2, 'name': 'S2'}}]
    matching = cast(
        'list[dict[str, object]]',
        await query_site.call(
            'find',
            'Version',
            [['entity.Shot.project', 'is', archived]],
            ['id'],
            include_archived_projects=False,
        ),
    )
    assert [row['id'] for row in matching] == [version['id']]


@pytest.mark.anyio
async def test_retired_scope_excludes_archived_project_shot(query_site: FakeSiteCase) -> None:
    _, _, s1, s2, _ = await _archive_rows(query_site)
    await query_site.call('delete', 'Shot', s1['id'])
    await query_site.call('delete', 'Shot', s2['id'])
    assert await _ids(query_site, 'Shot', retired_only=True, include_archived_projects=False) == [
        s1['id']
    ]


@pytest.mark.anyio
async def test_retired_scope_excludes_archived_project(query_site: FakeSiteCase) -> None:
    active, archived, _, _, _ = await _archive_rows(query_site)
    await query_site.call('delete', 'Project', active['id'])
    await query_site.call('delete', 'Project', archived['id'])
    assert await _ids(
        query_site, 'Project', retired_only=True, include_archived_projects=False
    ) == [active['id']]


@pytest.mark.anyio
async def test_unarchiving_project_restores_shot_to_scope(query_site: FakeSiteCase) -> None:
    _, archived, s1, s2, s3 = await _archive_rows(query_site)
    assert await _ids(query_site, 'Shot', include_archived_projects=False) == [
        s1['id'],
        s3['id'],
    ]
    await query_site.call('update', 'Project', archived['id'], {'archived': False})
    assert await _ids(query_site, 'Shot', include_archived_projects=False) == [
        s1['id'],
        s2['id'],
        s3['id'],
    ]


@pytest.mark.anyio
async def test_omitted_or_true_archive_flag_keeps_every_record(query_site: FakeSiteCase) -> None:
    active, archived, s1, s2, s3 = await _archive_rows(query_site)
    for options in ({}, {'include_archived_projects': True}):
        assert await _ids(query_site, 'Project', **options) == [active['id'], archived['id']]
        assert await _ids(query_site, 'Shot', **options) == [s1['id'], s2['id'], s3['id']]


@pytest.mark.anyio
async def test_type_without_project_is_unaffected(query_site: FakeSiteCase) -> None:
    await _archive_rows(query_site)
    step = await create_link(query_site, 'Step', {'code': 'Comp'})
    assert await _ids(query_site, 'Step', include_archived_projects=False) == [step['id']]


@pytest.mark.anyio
async def test_schema_without_project_archived_does_not_filter(
    fake_site: FakeSiteCase,
) -> None:
    project = await create_link(fake_site, 'Project', {'name': 'No archived field'})
    shot = await create_link(fake_site, 'Shot', {'code': 'S1', 'project': project})
    assert await _ids(fake_site, 'Project', include_archived_projects=False) == [project['id']]
    assert await _ids(fake_site, 'Shot', include_archived_projects=False) == [shot['id']]


def _latest(mode: str = 'ENTITIES_CREATED_AT') -> list[dict[str, object]]:
    return [{'preset_name': 'LATEST', 'latest_by': mode}]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'operation', ['winners', 'filter', 'retired', 'retired_only', 'page', 'order', 'one', 'count']
)
async def test_latest_version_selection(query_site: FakeSiteCase, operation: str) -> None:
    s1 = await create_link(query_site, 'Shot', {'code': 'S1'})
    s2 = await create_link(query_site, 'Shot', {'code': 'S2'})
    v1 = await _version(query_site, s1, -3)
    v2 = await _version(query_site, s1, -1)
    v3 = await _version(query_site, s2, -2)
    await _version(query_site, None, 0)
    options: dict[str, object] = {'additional_filter_presets': _latest()}
    expected = [v2['id'], v3['id']]
    filters: list[object] = []
    if operation in {'retired', 'retired_only'}:
        await query_site.call('delete', 'Version', v2['id'])
        expected = [v1['id'], v3['id']]
    if operation == 'retired_only':
        options['retired_only'] = True
        expected = []
    if operation == 'filter':
        filters = [['created_at', 'less_than', QUERY_NOW - timedelta(minutes=90)]]
        expected = [v1['id'], v3['id']]
    if operation == 'page':
        options.update(limit=1, page=2)
        expected = [v3['id']]
    if operation == 'order':
        options['order'] = [{'field_name': 'id', 'direction': 'desc'}]
        expected.reverse()
    if operation == 'one':
        assert await query_site.call('find_one', 'Version', [], ['id'], **options) == v2
    elif operation == 'count':
        result = raw_read(query_site.client._server, 'Version', [], **options)
        assert (
            cast('dict[str, object]', cast('dict[str, object]', result['results'])['paging_info'])[
                'entity_count'
            ]
            == 2
        )
    else:
        rows = cast(
            'list[dict[str, object]]',
            await query_site.call('find', 'Version', filters, ['id'], **options),
        )
        assert [row['id'] for row in rows] == expected


@pytest.mark.anyio
@pytest.mark.parametrize('hours', [-2, None])
async def test_latest_version_tie_and_null_raise(
    query_site: FakeSiteCase, hours: int | None
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    await _version(query_site, shot, -2)
    await _version(query_site, shot, hours)
    with pytest.raises(MockgunError, match=r'Version.*LATEST'):
        await _ids(query_site, 'Version', additional_filter_presets=_latest())


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario', ['higher_step', 'no_step', 'step_beats_none', 'distinct_steps']
)
async def test_latest_pipeline_step(query_site: FakeSiteCase, scenario: str) -> None:
    code = {'higher_step': 'S3', 'no_step': 'S4', 'step_beats_none': 'S5', 'distinct_steps': 'S6'}[
        scenario
    ]
    shot = await create_link(query_site, 'Shot', {'code': code})
    tasks: list[dict[str, object]] = []
    for name, order in [('A', 10), ('B', 20), ('C', 10)]:
        step = await create_link(query_site, 'Step', {'code': name, 'list_order': order})
        tasks.append(await create_link(query_site, 'Task', {'content': name, 'step': step}))
    no_step = await create_link(query_site, 'Task', {'content': 'N'})
    pairs = {
        'higher_step': [(tasks[1], -3), (tasks[0], -1)],
        'no_step': [(None, -1), (no_step, -2)],
        'step_beats_none': [(tasks[0], -3), (None, -1)],
        'distinct_steps': [(tasks[0], -2), (tasks[2], -1)],
    }
    versions = [
        await _version(query_site, shot, hour, sg_task=task) for task, hour in pairs[scenario]
    ]
    if scenario == 'distinct_steps':
        with pytest.raises(MockgunError, match=r'Version.*LATEST'):
            await _ids(
                query_site,
                'Version',
                additional_filter_presets=_latest(
                    'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'
                ),
            )
    else:
        assert await _ids(
            query_site,
            'Version',
            additional_filter_presets=_latest('BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
        ) == [versions[0]['id']]


@pytest.mark.anyio
@pytest.mark.parametrize('mode', ['ENTITIES_CREATED_AT', 'VERSION_NUMBER'])
@pytest.mark.parametrize('other_code', [False, True])
async def test_latest_published_files(
    query_site: FakeSiteCase, mode: str, other_code: bool
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    task = await create_link(query_site, 'Task', {'content': 'T1'})
    file_type = await create_link(query_site, 'PublishedFileType', {'code': 'PT1'})
    rows: list[dict[str, object]] = []
    for entity, name, pt, hour, number in [
        (shot, 'a', file_type, -2, 2),
        (shot, 'a', file_type, -1, 1),
        (shot, 'b', file_type, -3, 1),
        (None, 'a', file_type, 0, 9),
        (shot, 'a', None, 0, 9),
    ]:
        rows.append(
            await create_link(
                query_site,
                'PublishedFile',
                {
                    'code': 'pf',
                    'entity': entity,
                    'task': task,
                    'name': name,
                    'published_file_type': pt,
                    'created_at': QUERY_NOW + timedelta(hours=hour),
                    'version_number': number,
                    **(
                        {'path': {'url': 'https://unit-test.example/file'}}
                        if len(rows) == 0
                        else {}
                    ),
                },
            )
        )
    expected = [rows[1 if mode == 'ENTITIES_CREATED_AT' else 0]['id'], rows[2]['id']]
    if other_code:
        last = await create_link(
            query_site,
            'PublishedFile',
            {
                'code': 'pf-other',
                'entity': shot,
                'task': task,
                'name': 'a',
                'published_file_type': file_type,
                'created_at': QUERY_NOW - timedelta(minutes=30),
                'version_number': 3,
            },
        )
        expected = [rows[2]['id'], last['id']]
    assert (
        await _ids(query_site, 'PublishedFile', additional_filter_presets=_latest(mode))
        == expected
    )


@pytest.mark.anyio
@pytest.mark.parametrize('scenario', ['mixed_type', 'no_task', 'no_name', 'null_number'])
async def test_latest_published_file_group_guards(query_site: FakeSiteCase, scenario: str) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    task = await create_link(query_site, 'Task', {'content': 'T1'})
    pt1 = await create_link(query_site, 'PublishedFileType', {'code': 'PT1'})
    pt2 = await create_link(query_site, 'PublishedFileType', {'code': 'PT2'})
    data: dict[str, object] = {
        'code': 'pf',
        'entity': shot,
        'task': None if scenario == 'no_task' else task,
        'name': None if scenario == 'no_name' else 'c',
        'published_file_type': pt1,
        'created_at': QUERY_NOW,
        'version_number': 1,
    }
    await create_link(query_site, 'PublishedFile', data)
    if scenario in {'mixed_type', 'null_number'}:
        await create_link(
            query_site,
            'PublishedFile',
            {
                **data,
                'published_file_type': pt2 if scenario == 'mixed_type' else pt1,
                'version_number': None if scenario == 'null_number' else 2,
            },
        )
    reason = (
        r'published_file_type entity differs within group'
        if scenario == 'mixed_type'
        else r'PublishedFile.*LATEST'
    )
    with pytest.raises(MockgunError, match=reason):
        await _ids(
            query_site, 'PublishedFile', additional_filter_presets=_latest('VERSION_NUMBER')
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    [
        'revision',
        'time',
        'null_entity',
        'null_revision',
        'tie',
        'null_time_loses_revision',
        'null_time_tie',
    ],
)
async def test_latest_cut(query_site: FakeSiteCase, scenario: str) -> None:
    sequence = await create_link(query_site, 'Sequence', {'code': 'Q'})
    values = {
        'revision': [(1, -3), (2, -2)],
        'time': [(2, -2), (2, -1)],
        'null_entity': [(1, -3), (2, -2)],
        'null_revision': [(None, -3), (1, -2)],
        'tie': [(2, -1), (2, -1)],
        'null_time_loses_revision': [(3, None), (1, -1)],
        'null_time_tie': [(2, None), (2, -1)],
    }
    rows = [
        await create_link(
            query_site,
            'Cut',
            {
                'code': 'cut',
                'entity': None if scenario == 'null_entity' else sequence,
                'revision_number': rev,
                'created_at': None if hour is None else QUERY_NOW + timedelta(hours=hour),
            },
        )
        for rev, hour in values[scenario]
    ]
    if scenario in {'tie', 'null_time_tie'}:
        with pytest.raises(MockgunError, match=r'Cut.*LATEST'):
            await _ids(query_site, 'Cut', additional_filter_presets=_latest('REVISION_NUMBER'))
    else:
        assert await _ids(
            query_site, 'Cut', additional_filter_presets=_latest('REVISION_NUMBER')
        ) == [rows[0 if scenario == 'null_time_loses_revision' else 1]['id']]


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    ['normal', 'filter', 'missing', 'retired_cut', 'combined', 'retired_only', 'retired_item'],
)
async def test_cut_shot_versions(query_site: FakeSiteCase, scenario: str) -> None:
    shots = [await create_link(query_site, 'Shot', {'code': f'S{n}'}) for n in range(1, 4)]
    asset = await create_link(query_site, 'Asset', {'code': 'A'})
    versions = [
        await _version(query_site, entity, hour, code=f'V{n}')
        for n, (entity, hour) in enumerate(
            [
                (shots[0], -2),
                (shots[0], -1),
                (shots[1], 0),
                (shots[2], 0),
                (shots[2], -1),
                (asset, 0),
                (None, 0),
            ]
        )
    ]
    await query_site.call('delete', 'Version', versions[4]['id'])
    cut = await create_link(query_site, 'Cut', {'code': 'K'})
    item = await create_link(
        query_site, 'CutItem', {'code': 'I1', 'cut': cut, 'shot': shots[0], 'version': versions[2]}
    )
    await create_link(query_site, 'CutItem', {'code': 'I2', 'cut': cut, 'shot': shots[2]})
    await create_link(query_site, 'CutItem', {'code': 'I3', 'cut': cut, 'version': versions[5]})
    if scenario == 'retired_cut':
        await query_site.call('delete', 'Cut', cut['id'])
    if scenario == 'retired_item':
        await query_site.call('delete', 'CutItem', item['id'])
    presets = [
        {
            'preset_name': 'CUT_SHOT_VERSIONS',
            'cut_id': 9999 if scenario == 'missing' else cut['id'],
        }
    ]
    if scenario == 'combined':
        presets += _latest()
    if scenario in {'retired_only', 'retired_item'}:
        with pytest.raises(MockgunError, match=r'Version.*CUT_SHOT_VERSIONS'):
            await _ids(
                query_site,
                'Version',
                additional_filter_presets=presets,
                retired_only=scenario == 'retired_only',
            )
    else:
        rows = cast(
            'list[dict[str, object]]',
            await query_site.call(
                'find',
                'Version',
                [['code', 'is', 'V2']] if scenario == 'filter' else [],
                ['id'],
                additional_filter_presets=presets,
            ),
        )
        expected = (
            []
            if scenario in {'missing', 'filter'}
            else [versions[n]['id'] for n in ([1, 3] if scenario == 'combined' else [0, 1, 3])]
        )
        assert [row['id'] for row in rows] == expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'presets', 'code', 'message'),
    [
        (
            'Version',
            {'preset_name': 'LATEST'},
            103,
            'API read() additional_filter_presets must be an array',
        ),
        ('Version', [{'preset_name': 'BOGUS'}], 103, 'API read() unknown preset_name BOGUS'),
        (
            'Version',
            [{'preset_name': 'LATEST'}],
            104,
            'API read() LATEST preset requires latest_by',
        ),
        (
            'Version',
            _latest('REVISION_NUMBER'),
            104,
            'API read() latest_by REVISION_NUMBER is not valid for Version',
        ),
        ('Shot', _latest(), 103, 'API read() LATEST preset is not supported for Shot'),
        (
            'Shot',
            [{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': 1}],
            103,
            'API read() CUT_SHOT_VERSIONS preset is not supported for Shot',
        ),
    ],
)
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_preset_wire_faults(
    query_site: FakeSiteCase, kind: str, presets: object, code: int, message: str
) -> None:
    await create_link(query_site, kind, {'code': 'populated'})
    before = query_site.client._server.store.snapshot()
    result = raw_read(query_site.client._server, kind, [], additional_filter_presets=presets)
    assert result['error_code'] == code
    assert result['message'] == message
    assert query_site.client._server.store.snapshot() == before


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'presets', 'invalid_filter'),
    [
        ('Shot', [{'preset_name': 'LATEST'}], False),
        ('Shot', [{'preset_name': 'CUT_SHOT_VERSIONS'}], False),
        ('Version', _latest() + _latest('BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'), False),
        ('Version', [{'preset_name': 'BOGUS'}], True),
        ('Version', [42], False),
        (
            'Version',
            [{'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT', 'extra': 1}],
            False,
        ),
        ('Version', [{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': '1'}], False),
    ],
)
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_unsettled_preset_validation(
    query_site: FakeSiteCase, kind: str, presets: object, invalid_filter: bool
) -> None:
    with pytest.raises(MockgunError, match=kind):
        raw_read(
            query_site.client._server,
            kind,
            [('unknown', 'is', [1])] if invalid_filter else [],
            additional_filter_presets=presets,
        )


@pytest.mark.anyio
async def test_latest_archive_mixed_group_raises(query_site: FakeSiteCase) -> None:
    active, archived, shot, _, _ = await _archive_rows(query_site)
    await _version(query_site, shot, -1, project=archived)
    await _version(query_site, shot, -2, project=active)
    with pytest.raises(MockgunError, match=r'Version.*LATEST'):
        await _ids(
            query_site,
            'Version',
            additional_filter_presets=_latest(),
            include_archived_projects=False,
        )


@pytest.mark.anyio
async def test_latest_hidden_entity_raises(query_site: FakeSiteCase) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    version = await _version(query_site, shot, -1)
    await query_site.call('delete', 'Shot', shot['id'])
    await query_site.call('revive', 'Version', version['id'])
    with pytest.raises(MockgunError, match=r'Version.*LATEST'):
        await _ids(query_site, 'Version', additional_filter_presets=_latest())


@pytest.mark.anyio
@pytest.mark.parametrize('scenario', ['lone_null', 'missing_cut_id', 'archive_omitted'])
async def test_preset_accepted_controls(query_site: FakeSiteCase, scenario: str) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S2'})
    version = await _version(query_site, shot, None if scenario == 'lone_null' else -1)
    presets = _latest()
    if scenario == 'missing_cut_id':
        presets = [{'preset_name': 'CUT_SHOT_VERSIONS'}]
    elif scenario == 'archive_omitted':
        project = await create_link(query_site, 'Project', {'name': 'Archived', 'archived': True})
        await query_site.call('update', 'Version', version['id'], {'project': project})
        await _version(query_site, shot, -2)
    assert await _ids(query_site, 'Version', additional_filter_presets=presets) == (
        [] if scenario == 'missing_cut_id' else [version['id']]
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'field', 'queried', 'mode'),
    [
        ('Version', 'entity', 'Version', 'ENTITIES_CREATED_AT'),
        ('Version', 'created_at', 'Version', 'ENTITIES_CREATED_AT'),
        ('Version', 'sg_task', 'Version', 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
        ('Task', 'step', 'Version', 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
        ('Step', 'list_order', 'Version', 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
        ('PublishedFile', 'task', 'PublishedFile', 'VERSION_NUMBER'),
        ('PublishedFile', 'name', 'PublishedFile', 'VERSION_NUMBER'),
        ('PublishedFile', 'published_file_type', 'PublishedFile', 'VERSION_NUMBER'),
        ('PublishedFile', 'version_number', 'PublishedFile', 'VERSION_NUMBER'),
        ('Cut', 'code', 'Cut', 'REVISION_NUMBER'),
        ('Cut', 'revision_number', 'Cut', 'REVISION_NUMBER'),
        ('CutItem', 'cut', 'Version', 'CUT_SHOT_VERSIONS'),
        ('CutItem', 'shot', 'Version', 'CUT_SHOT_VERSIONS'),
    ],
)
async def test_preset_required_schema_before_empty_rows(
    query_site: FakeSiteCase, kind: str, field: str, queried: str, mode: str
) -> None:
    del query_site.client._server.schema.fields[kind][field]
    presets = (
        [{'preset_name': mode, 'cut_id': 1}] if mode == 'CUT_SHOT_VERSIONS' else _latest(mode)
    )
    with pytest.raises(MockgunError, match=rf'{kind}\.{field}'):
        await _ids(query_site, queried, additional_filter_presets=presets)


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario',
    ['missing_entity', 'missing_task', 'missing_step', 'retired_task', 'null_step_order'],
)
async def test_pipeline_link_guards(query_site: FakeSiteCase, scenario: str) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    step = await create_link(
        query_site,
        'Step',
        {'code': 'Comp', 'list_order': None if scenario == 'null_step_order' else 10},
    )
    task = await create_link(
        query_site,
        'Task',
        {
            'content': 'T1',
            'step': {'type': 'Step', 'id': 999} if scenario == 'missing_step' else step,
        },
    )
    await _version(
        query_site,
        {'type': 'Shot', 'id': 999} if scenario == 'missing_entity' else shot,
        -1,
        sg_task={'type': 'Task', 'id': 999} if scenario == 'missing_task' else task,
    )
    if scenario == 'retired_task':
        await query_site.call('delete', 'Task', task['id'])
    with pytest.raises(MockgunError, match='LATEST'):
        await _ids(
            query_site,
            'Version',
            additional_filter_presets=_latest('BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    'scenario', ['all_null_revision', 'null_code', 'version_tie', 'file_time_null']
)
async def test_additional_latest_group_guards(query_site: FakeSiteCase, scenario: str) -> None:
    if scenario in {'all_null_revision', 'null_code'}:
        for hour in (-2, -1):
            await create_link(
                query_site,
                'Cut',
                {
                    'code': None if scenario == 'null_code' else 'cut',
                    'created_at': QUERY_NOW + timedelta(hours=hour),
                },
            )
        kind, mode = 'Cut', 'REVISION_NUMBER'
    else:
        shot = await create_link(query_site, 'Shot', {'code': 'S1'})
        task = await create_link(query_site, 'Task', {'content': 'T1'})
        pt = await create_link(query_site, 'PublishedFileType', {'code': 'PT1'})
        for created_at in (None, QUERY_NOW):
            await create_link(
                query_site,
                'PublishedFile',
                {
                    'code': 'pf',
                    'entity': shot,
                    'task': task,
                    'name': 'a',
                    'published_file_type': pt,
                    'version_number': 1,
                    'created_at': created_at,
                },
            )
        kind, mode = (
            'PublishedFile',
            'VERSION_NUMBER' if scenario == 'version_tie' else 'ENTITIES_CREATED_AT',
        )
    with pytest.raises(MockgunError, match='LATEST') as error:
        await _ids(query_site, kind, additional_filter_presets=_latest(mode))
    if scenario == 'file_time_null':
        assert 'find/find_one PublishedFile LATEST preset: created_at' in str(error.value)


@pytest.mark.anyio
async def test_latest_rank_key_reads_stored_values_without_selection_guards(
    query_site: FakeSiteCase,
) -> None:
    store = query_site.client._server.store
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    step = await create_link(query_site, 'Step', {'code': 'Comp', 'list_order': 10})
    task = await create_link(query_site, 'Task', {'content': 'T1', 'step': step})
    version = await _version(query_site, shot, -1, sg_task=task)
    await query_site.call('delete', 'Task', task['id'])
    row = store.row('Version', entity_id(version))
    assert row is not None
    before = store.snapshot()
    del store.schema.fields['Step']['list_order']
    epoch_us = 1784113200000000  # QUERY_NOW minus one hour, as Unix microseconds.
    assert latest_rank_key(store, 'Version', row, 'ENTITIES_CREATED_AT') == (1, epoch_us)
    assert latest_rank_key(
        store, 'Version', row, 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'
    ) == (1, 1, 10, 1, epoch_us)
    assert latest_rank_key(
        store,
        'Version',
        {**row, 'sg_task': None, 'created_at': None},
        'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT',
    ) == (0, 0, 0, 0, 0)
    assert latest_rank_key(store, 'PublishedFile', {'version_number': -2}, 'VERSION_NUMBER') == (
        1,
        -2,
    )
    assert latest_rank_key(
        store, 'Cut', {'revision_number': None, 'created_at': None}, 'REVISION_NUMBER'
    ) == (0, 0, 0, 0)
    assert store.snapshot() == before
    with pytest.raises(MockgunError, match='missing linked record'):
        latest_rank_key(
            store,
            'Version',
            {**row, 'sg_task': {'type': 'Task', 'id': 999}},
            'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT',
        )
    with pytest.raises(MockgunError, match='unsupported latest_by'):
        latest_rank_key(store, 'Shot', row, 'ENTITIES_CREATED_AT')


@pytest.mark.anyio
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_explicit_null_presets_wire_fault(query_site: FakeSiteCase) -> None:
    result = raw_read(query_site.client._server, 'Version', [], additional_filter_presets=None)
    assert result['error_code'] == 103
    assert result['message'] == 'API read() additional_filter_presets must be an array'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'presets',
    [
        [{'preset_name': 'BOGUS'}, {'preset_name': 'BOGUS'}],
        [
            {'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': 1},
            {'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': 1},
        ],
        [{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': True}],
        [{'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT', 'cut_id': 1}],
    ],
)
@pytest.mark.parametrize('query_site', ['sync'], indirect=True)
async def test_invalid_preset_combinations_before_empty_rows(
    query_site: FakeSiteCase, presets: object
) -> None:
    with pytest.raises(MockgunError, match='Version'):
        raw_read(query_site.client._server, 'Version', [], additional_filter_presets=presets)


@pytest.mark.anyio
async def test_cut_shot_versions_requires_shot_entities(query_site: FakeSiteCase) -> None:
    asset = await create_link(query_site, 'Asset', {'code': 'A'})
    await _version(query_site, asset, -1)
    cut = await create_link(query_site, 'Cut', {'code': 'K'})
    await create_link(query_site, 'CutItem', {'code': 'I', 'cut': cut, 'shot': asset})
    assert (
        await _ids(
            query_site,
            'Version',
            additional_filter_presets=[{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': cut['id']}],
        )
        == []
    )


@pytest.mark.anyio
async def test_archive_scope_excludes_rows_before_nonpreset_filter_evaluation(
    query_site: FakeSiteCase,
) -> None:
    archived = await create_link(query_site, 'Project', {'name': 'Archived', 'archived': True})
    await create_link(
        query_site,
        'Shot',
        {'code': 'S1', 'project': archived, 'sg_sequence': {'type': 'Sequence', 'id': 999}},
    )
    assert (
        await query_site.call(
            'find',
            'Shot',
            [['sg_sequence.Sequence.code', 'is', 'Q']],
            ['id'],
            include_archived_projects=False,
        )
        == []
    )


@pytest.mark.anyio
@pytest.mark.parametrize('unknown_first', [False, True])
async def test_unknown_preset_rejects_complete_combination_before_wire_fault(
    query_site: FakeSiteCase, unknown_first: bool
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    await _version(query_site, shot, -1)
    presets = [*_latest(), {'preset_name': 'BOGUS'}]
    if unknown_first:
        presets.reverse()
    before = query_site.client._server.store.snapshot()
    with pytest.raises(MockgunError, match=r'Version.*unsupported preset combination'):
        await _ids(query_site, 'Version', additional_filter_presets=presets)
    assert query_site.client._server.store.snapshot() == before


@pytest.mark.anyio
@pytest.mark.parametrize('scenario', ['duplicate_latest', 'latest_cut', 'cut_latest'])
async def test_permitted_preset_combinations_survive_complete_validation(
    query_site: FakeSiteCase, scenario: str
) -> None:
    shot = await create_link(query_site, 'Shot', {'code': 'S1'})
    await _version(query_site, shot, -2)
    winner = await _version(query_site, shot, -1)
    cut = await create_link(query_site, 'Cut', {'code': 'K'})
    await create_link(query_site, 'CutItem', {'code': 'I', 'cut': cut, 'shot': shot})
    presets = _latest()
    if scenario == 'duplicate_latest':
        presets += _latest()
    else:
        presets += [{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': cut['id']}]
        if scenario == 'cut_latest':
            presets.reverse()
    assert await _ids(query_site, 'Version', additional_filter_presets=presets) == [winner['id']]


@pytest.mark.anyio
async def test_unknown_preset_rank_field_raises(effects_site: FakeSiteCase) -> None:
    shot = seed(effects_site, 'Shot', {'code': 's'})
    version = seed(effects_site, 'Version', {'code': 'v', 'entity': shot})
    store = effects_site.client._server.store
    row = store.require('Version', entity_id(version))
    known_key = latest_rank_key(store, 'Version', row, 'ENTITIES_CREATED_AT')
    store.mark_unknown(row, 'created_at', 'creation time unknown')
    assert latest_rank_key(store, 'Version', row, 'ENTITIES_CREATED_AT') == known_key
    with pytest.raises(mockgun.MockgunError, match=r'Version\.created_at.*creation time unknown'):
        await effects_site.call(
            'find',
            'Version',
            [],
            additional_filter_presets=[
                {'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'}
            ],
        )
