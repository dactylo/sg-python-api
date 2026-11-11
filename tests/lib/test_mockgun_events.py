import copy
import pickle
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun import _events
from shotgun_api3.lib.mockgun._sessions import RequestContext, _Actor
from tests.conftest import entity_id
from tests.lib._events import cursor, delta

from ._schemas import schema
from .conftest import EFFECTS_SCHEMA, QUERY_NOW, WORKFLOW_SCHEMA, FakeSiteCase, raw_call, seed

_COMMON_FIELDS = [
    'event_type',
    'attribute_name',
    'meta',
    'user',
    'project',
    'entity',
    'created_at',
    'session_uuid',
]
_PARTIAL_ASSET_SCHEMA = {
    **EFFECTS_SCHEMA,
    'Asset': {name: field for name, field in EFFECTS_SCHEMA['Asset'].items() if name != 'shots'},
}
_PLAIN_NOTE_SCHEMA = {
    **EFFECTS_SCHEMA,
    'Note': {name: field for name, field in EFFECTS_SCHEMA['Note'].items() if name != 'user'},
}
_SHOT_DEFAULTS_SCHEMA = {
    **EFFECTS_SCHEMA,
    'Shot': {
        **EFFECTS_SCHEMA['Shot'],
        'sg_status_list': {
            'data_type': {'value': 'status_list'},
            'properties': {
                'default_value': {'value': 'wtg'},
                'valid_values': {'value': ['wtg', 'ip', 'fin']},
            },
        },
        'sg_shot_type': {
            'data_type': {'value': 'list'},
            'properties': {
                'default_value': {'value': 'Feature'},
                'valid_values': {'value': ['Feature', 'Commercial']},
            },
        },
    },
}
# 2026-11-10 trial capture: number, duration and text defaults that a generated schema declares,
# which no create logs.
_SITE_DEFAULT_FIELDS = schema(
    {
        'Task': {'time_logs_sum': 'duration', 'workload_assignee_count': 'number'},
        'Note': {'publish_status': 'text'},
        'Reply': {'publish_status': 'text'},
        'Shot': {'sg_frame_count': 'number', 'sg_label': 'text'},
    },
    defaults={
        ('Task', 'time_logs_sum'): '0',
        ('Task', 'workload_assignee_count'): '0',
        ('Note', 'publish_status'): 'published',
        ('Reply', 'publish_status'): 'published',
        ('Shot', 'sg_frame_count'): '0',
        ('Shot', 'sg_label'): 'plate',
    },
)
_SITE_DEFAULTS_SCHEMA = {
    **WORKFLOW_SCHEMA,
    **{
        entity_type: {**WORKFLOW_SCHEMA[entity_type], **fields}
        for entity_type, fields in _SITE_DEFAULT_FIELDS.items()
    },
}
_SLATE_DEFAULT_SCHEMA = {
    **EFFECTS_SCHEMA,
    'Version': {
        **EFFECTS_SCHEMA['Version'],
        'sg_movie_has_slate': {
            'data_type': {'value': 'checkbox'},
            'properties': {'default_value': {'value': False}},
        },
    },
}


async def _rows(
    case: FakeSiteCase, link: dict[str, object], fields: list[str]
) -> list[dict[str, object]]:
    return cast(
        'list[dict[str, object]]',
        await case.call(
            'find',
            'EventLogEntry',
            [['entity', 'is', link]],
            fields,
            order=[{'field_name': 'id', 'direction': 'asc'}],
        ),
    )


def _scene(case: FakeSiteCase) -> dict[str, dict[str, object]]:
    project = seed(case, 'Project', {'name': 'Alpha'})
    note = seed(case, 'Note', {'subject': 'n', 'project': project})
    return {
        'Project': project,
        'Shot': seed(case, 'Shot', {'code': 's1', 'project': project, 'description': 'old'}),
        'Version': seed(
            case, 'Version', {'code': 'v1', 'project': project, 'sg_movie_has_slate': False}
        ),
        'Task': seed(
            case, 'Task', {'content': 't1', 'project': project, 'start_date': '2026-07-01'}
        ),
        'Asset': seed(case, 'Asset', {'code': 'a1', 'project': project, 'sg_asset_type': 'Prop'}),
        'Note': note,
        'Reply': seed(case, 'Reply', {'content': 'r', 'entity': note}),
    }


def _workflow_scene(case: FakeSiteCase) -> dict[str, dict[str, object]]:
    scene = _scene(case)
    for kind in ('Sequence', 'Cut', 'CutItem', 'Playlist'):
        scene[kind] = seed(case, kind, {'code': kind, 'project': scene['Project']})
    return scene


@pytest.fixture
async def create_event_site(
    effects_site: FakeSiteCase,
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    schema_name = getattr(request, 'param', 'effects')
    if schema_name == 'effects':
        yield effects_site
        return
    schema = _SHOT_DEFAULTS_SCHEMA if schema_name == 'shot-defaults' else _PLAIN_NOTE_SCHEMA
    if schema_name == 'partial-asset':
        schema = _PARTIAL_ASSET_SCHEMA
    if schema_name == 'slate-default':
        schema = _SLATE_DEFAULT_SCHEMA
    if schema_name == 'site-defaults':
        schema = _SITE_DEFAULTS_SCHEMA
    path = tmp_path / 'create-events.pickle'
    path.write_bytes(pickle.dumps(schema, protocol=2))
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', str(path))
    client = (mockgun.Shotgun if effects_site.kind == 'sync' else mockgun.AsyncShotgun)(
        'http://unit-test.example', script_name='queries', api_key='key'
    )
    case = FakeSiteCase(effects_site.kind, client)
    client.site.set_time(QUERY_NOW)
    try:
        yield case
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_lifecycle_events_batch_preserves_request_order(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
    other = seed(effects_site, 'Shot', {'code': 's2', 'project': project})
    before = await cursor(effects_site)
    await effects_site.call(
        'batch',
        [
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': shot['id'],
                'data': {'description': 'changed'},
            },
            {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': other['id']},
        ],
    )
    rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'entity'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Retirement',
    ]
    assert [row['attribute_name'] for row in rows] == ['description', 'retirement_date', None]
    assert [row['entity'] for row in rows] == [{**shot, 'name': 's1'}, None, None]
    assert [row['id'] for row in rows] == sorted(entity_id(row) for row in rows)


@pytest.mark.anyio
async def test_lifecycle_events_hide_and_restore_earlier_links(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'changed'})
    earlier = await _rows(effects_site, shot, ['entity'])
    assert len(earlier) == 1
    before = await cursor(effects_site)
    assert await effects_site.call('delete', 'Shot', shot['id']) is True
    retired = await delta(effects_site, before, ['event_type', 'attribute_name', 'created_at'])
    assert [row['event_type'] for row in retired] == [
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Retirement',
    ]
    assert [row['attribute_name'] for row in retired] == ['retirement_date', None]
    record = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'Shot', [['id', 'is', shot['id']]], ['updated_at'], retired_only=True
        ),
    )
    assert record['updated_at'] == QUERY_NOW
    assert [row['created_at'] for row in retired] == [record['updated_at']] * 2
    for row in retired:
        found = cast(
            'dict[str, object]',
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['meta']
            ),
        )
        meta = cast('dict[str, object]', found['meta'])
        assert (
            meta['new_value' if row['attribute_name'] else 'retirement_date']
            == '2026-07-15 12:00:00 UTC'
        )
    for row in [*earlier, *retired]:
        assert await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['entity']
        ) == {'type': 'EventLogEntry', 'id': row['id'], 'entity': None}
    assert await _rows(effects_site, shot, ['entity']) == []
    later = QUERY_NOW + timedelta(hours=1)
    effects_site.client.site.set_time(later)
    before = await cursor(effects_site)
    assert await effects_site.call('revive', 'Shot', shot['id']) is True
    revived = await delta(effects_site, before, ['event_type', 'attribute_name', 'created_at'])
    assert [row['event_type'] for row in revived] == [
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Revival',
    ]
    assert [row['attribute_name'] for row in revived] == ['retirement_date', None]
    assert [row['created_at'] for row in revived] == [later] * 2
    change = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', revived[0]['id']]], ['meta']
        ),
    )
    assert cast('dict[str, object]', change['meta'])['old_value'] == '2026-07-15 12:00:00 UTC'
    assert cast('dict[str, object]', change['meta'])['new_value'] is None
    assert await effects_site.call(
        'find_one', 'EventLogEntry', [['id', 'is', revived[1]['id']]], ['meta']
    ) == {
        'type': 'EventLogEntry',
        'id': revived[1]['id'],
        'meta': {
            'class_name': 'Shot',
            'display_name': 's1',
            'id': shot['id'],
            'entity_type': 'Shot',
            'entity_id': shot['id'],
            'type': 'entity_revival',
        },
    }
    for row in [*earlier, *retired]:
        assert await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['entity']
        ) == {'type': 'EventLogEntry', 'id': row['id'], 'entity': {**shot, 'name': 's1'}}
    all_rows = await _rows(effects_site, shot, ['entity'])
    assert [row['id'] for row in all_rows] == [row['id'] for row in [*earlier, *retired, *revived]]
    assert len(all_rows) == 5


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('entity_type', 'data', 'display_name'),
    [
        ('Task', {'content': 't1'}, 't1'),
        ('Version', {'code': 'v1'}, 'v1'),
        ('Playlist', {'code': 'pl'}, 'pl'),
        ('Note', {'subject': 'Sub', 'content': 'Body'}, 'Sub - Body'),
        ('Sequence', {'code': 'q1'}, 'q1'),
        ('Asset', {'code': 'a1'}, 'a1'),
        ('PublishedFile', {'code': 'pf1'}, 'pf1'),
        ('CutItem', {'code': 'ci1'}, 'ci1'),
    ],
)
async def test_lifecycle_events_observed_display_names(
    effects_site: FakeSiteCase, entity_type: str, data: dict[str, object], display_name: str
) -> None:
    seed(effects_site, 'Project', {'name': 'Alpha'})
    link = seed(effects_site, entity_type, data)
    before = await cursor(effects_site)
    assert await effects_site.call('delete', entity_type, link['id']) is True
    retired = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [row['event_type'] for row in retired] == [
        f'Shotgun_{entity_type}_Change',
        f'Shotgun_{entity_type}_Retirement',
    ]
    assert [row['attribute_name'] for row in retired] == ['retirement_date', None]
    for row in retired:
        found = cast(
            'dict[str, object]',
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['meta']
            ),
        )
        meta = cast('dict[str, object]', found['meta'])
        assert (
            meta['new_value' if row['attribute_name'] else 'retirement_date']
            == '2026-07-15 12:00:00 UTC'
        )
    before = await cursor(effects_site)
    assert await effects_site.call('revive', entity_type, link['id']) is True
    revived = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [row['event_type'] for row in revived] == [
        f'Shotgun_{entity_type}_Change',
        f'Shotgun_{entity_type}_Revival',
    ]
    assert [row['attribute_name'] for row in revived] == ['retirement_date', None]
    assert await effects_site.call(
        'find_one', 'EventLogEntry', [['id', 'is', revived[1]['id']]], ['meta']
    ) == {
        'type': 'EventLogEntry',
        'id': revived[1]['id'],
        'meta': {
            'class_name': entity_type,
            'display_name': display_name,
            'id': link['id'],
            'entity_type': entity_type,
            'entity_id': link['id'],
            'type': 'entity_revival',
        },
    }


@pytest.mark.anyio
async def test_lifecycle_events_revive_active_control_adds_no_row(
    effects_site: FakeSiteCase,
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
    before = await cursor(effects_site)
    assert await effects_site.call('revive', 'Shot', shot['id']) is False
    assert await delta(effects_site, before, ['event_type']) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('parent_type', 'holders'),
    [
        ('Shot', [('PublishedFile', 'entity'), ('CutItem', 'shot'), ('TimeLog', 'entity')]),
        ('Task', [('PublishedFile', 'task'), ('TimeLog', 'entity'), ('Version', 'sg_task')]),
        ('Asset', [('PublishedFile', 'entity')]),
        ('Sequence', [('PublishedFile', 'entity')]),
        ('Cut', [('CutItem', 'cut')]),
    ],
)
async def test_lifecycle_events_single_link_holders_add_no_rows(
    effects_site: FakeSiteCase, parent_type: str, holders: list[tuple[str, str]]
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    name = 'content' if parent_type == 'Task' else 'code'
    parent = seed(effects_site, parent_type, {name: 'p1', 'project': project})
    for holder_type, field in holders:
        seed(effects_site, holder_type, {field: parent})
    for method, kind in (('delete', 'Retirement'), ('revive', 'Revival')):
        before = await cursor(effects_site)
        assert await effects_site.call(method, parent_type, parent['id']) is True
        rows = await delta(effects_site, before, ['event_type', 'attribute_name'])
        assert [row['event_type'] for row in rows] == [
            f'Shotgun_{parent_type}_Change',
            f'Shotgun_{parent_type}_{kind}',
        ]
        assert [row['attribute_name'] for row in rows] == ['retirement_date', None]


@pytest.mark.anyio
async def test_project_create_explains_its_unsettled_event_rows(
    effects_site: FakeSiteCase,
) -> None:
    project = cast(
        'dict[str, object]', await effects_site.call('create', 'Project', {'name': 'Alpha'})
    )
    with pytest.raises(
        mockgun.MockgunError,
        match=rf'Project {project["id"]} create has unsettled event rows for the pages',
    ):
        await effects_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
@pytest.mark.parametrize(('child_type', 'name'), [('Task', 'content'), ('Version', 'code')])
async def test_lifecycle_events_holder_revived_before_its_target_adds_no_rows(
    effects_site: FakeSiteCase, child_type: str, name: str
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
    child = seed(effects_site, child_type, {name: 'c1', 'project': project, 'entity': shot})
    before = await cursor(effects_site)
    for method, row in (('delete', child), ('delete', shot), ('revive', child), ('revive', shot)):
        assert await effects_site.call(method, row['type'], row['id']) is True
    rows = await delta(effects_site, before, ['event_type'])
    assert [row['event_type'] for row in rows] == [
        f'Shotgun_{child_type}_Change',
        f'Shotgun_{child_type}_Retirement',
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Retirement',
        f'Shotgun_{child_type}_Change',
        f'Shotgun_{child_type}_Revival',
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Revival',
    ]


@pytest.mark.anyio
async def test_lifecycle_events_sudo_retirement_has_human_actor(
    effects_site: FakeSiteCase,
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's3', 'project': project})
    human = seed(effects_site, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    before = await cursor(effects_site)
    result = raw_call(
        effects_site.client._server,
        'delete',
        shot,
        auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
    )
    assert 'exception' not in result
    rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'user'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Shot_Change',
        'Shotgun_Shot_Retirement',
    ]
    assert [row['attribute_name'] for row in rows] == ['retirement_date', None]
    assert [row['user'] for row in rows] == [{**human, 'name': 'artist'}] * 2
    for row in rows:
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['meta']
            )


@pytest.mark.anyio
async def test_lifecycle_events_sudo_update_guard(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    shot = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
    human = seed(effects_site, 'HumanUser', {'login': 'artist', 'name': 'artist'})
    before = await cursor(effects_site)
    result = raw_call(
        effects_site.client._server,
        'update',
        {**shot, 'fields': [{'field_name': 'description', 'value': 'changed'}]},
        auth={'script_name': 'queries', 'script_key': 'key', 'sudo_as_login': 'artist'},
    )
    assert 'exception' not in result
    rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'user'])
    assert [row['event_type'] for row in rows] == ['Shotgun_Shot_Change']
    assert [row['attribute_name'] for row in rows] == ['description']
    assert rows[0]['user'] == {**human, 'name': 'artist'}
    with pytest.raises(mockgun.MockgunError, match='meta'):
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    'variant',
    [
        'playlist-version',
        'shot-task',
        'asset-shots',
        'sequence-shots',
        'version-published-files',
        'project-asset',
    ],
)
async def test_lifecycle_events_unsettled_holders_guard(
    effects_site: FakeSiteCase, variant: str
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    if variant == 'playlist-version':
        target = seed(effects_site, 'Version', {'code': 'v1', 'project': project})
        seed(effects_site, 'Playlist', {'code': 'pl', 'versions': [target]})
    elif variant == 'version-published-files':
        target = seed(effects_site, 'PublishedFile', {'code': 'pf1', 'project': project})
        seed(effects_site, 'Version', {'code': 'v1', 'published_files': [target]})
    elif variant == 'project-asset':
        target = project
        seed(effects_site, 'Asset', {'code': 'a1', 'project': project})
    else:
        target = seed(effects_site, 'Shot', {'code': 's1', 'project': project})
        if variant == 'shot-task':
            seed(effects_site, 'Task', {'content': 't1', 'entity': target})
        elif variant == 'asset-shots':
            seed(effects_site, 'Asset', {'code': 'a1', 'shots': [target]})
        else:
            seed(effects_site, 'Sequence', {'code': 'q1', 'shots': [target]})
    assert await effects_site.call('delete', target['type'], target['id']) is True
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await effects_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['plain-note'], indirect=True)
async def test_session_events_note_reading_precedes_create_rows(
    create_event_site: FakeSiteCase,
) -> None:
    project = seed(create_event_site, 'Project', {'name': 'Alpha'})
    before = await cursor(create_event_site)
    result = raw_call(
        create_event_site.client._server,
        'create',
        {
            'type': 'Note',
            'fields': [
                {'field_name': 'subject', 'value': 'n'},
                {'field_name': 'project', 'value': project},
            ],
            'return_fields': ['id'],
        },
        auth={'session_token': 'unknown'},
    )
    assert 'exception' not in result
    humans = cast(
        'list[dict[str, object]]', await create_event_site.call('find', 'HumanUser', [], ['name'])
    )
    assert len(humans) == 1
    rows = await delta(create_event_site, before, ['event_type', 'attribute_name', 'user'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Reading_Change',
        'Shotgun_Note_New',
        'Shotgun_Note_Change',
    ]
    assert [row['attribute_name'] for row in rows] == ['read_by_current_user', None, 'subject']
    assert [row['user'] for row in rows] == [humans[0]] * 3
    for field in ['entity', 'project', 'meta']:
        with pytest.raises(mockgun.MockgunError, match=field):
            await create_event_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], [field]
            )


@pytest.mark.anyio
async def test_scalar_updates_write_ordered_rows_with_common_fields(
    effects_site: FakeSiteCase,
) -> None:
    scene = _scene(effects_site)
    shot = scene['Shot']
    await effects_site.call(
        'update', 'Shot', shot['id'], {'description': 'new', 'sg_status_list': 'ip'}
    )
    rows = await _rows(effects_site, shot, _COMMON_FIELDS)
    assert len(rows) == 2
    assert [row['event_type'] for row in rows] == ['Shotgun_Shot_Change'] * 2
    assert [row['attribute_name'] for row in rows] == ['description', 'sg_status_list']
    assert rows[0]['meta'] == {
        'type': 'attribute_change',
        'attribute_name': 'description',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'field_data_type': 'text',
        'old_value': 'old',
        'new_value': 'new',
    }
    for row in rows:
        assert row['user'] == {'type': 'ApiUser', 'id': 1, 'name': 'queries'}
        assert row['project'] == {**scene['Project'], 'name': 'Alpha'}
        assert row['entity'] == {**shot, 'name': 's1'}
        assert row['created_at'] == QUERY_NOW
        assert row['session_uuid'] is None
    await effects_site.call(
        'update', 'Shot', shot['id'], {'sg_status_list': 'fin', 'description': 'x'}
    )
    all_rows = await _rows(effects_site, shot, ['attribute_name'])
    assert [row['attribute_name'] for row in all_rows] == [
        'description',
        'sg_status_list',
        'sg_status_list',
        'description',
    ]
    assert [row['id'] for row in all_rows] == sorted(entity_id(row) for row in all_rows)


@pytest.mark.anyio
@pytest.mark.parametrize('value', [True, False])
async def test_number_event_encodes_boolean_as_integer_without_changing_storage(
    effects_site: FakeSiteCase, value: bool
) -> None:
    version = _scene(effects_site)['Version']
    result = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Version', version['id'], {'sg_first_frame': value}),
    )
    assert result['sg_first_frame'] is value
    stored = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'Version', [['id', 'is', version['id']]], ['sg_first_frame']
        ),
    )
    assert stored['sg_first_frame'] is value
    rows = await _rows(effects_site, version, ['meta'])
    assert len(rows) == 1
    meta = cast('dict[str, object]', rows[0]['meta'])
    assert meta['field_data_type'] == 'number'
    assert meta['old_value'] is None
    assert type(meta['new_value']) is int
    assert meta['new_value'] == int(value)
    await effects_site.call('update', 'Version', version['id'], {'sg_first_frame': None})
    rows = await _rows(effects_site, version, ['meta'])
    assert len(rows) == 2
    meta = cast('dict[str, object]', rows[1]['meta'])
    assert type(meta['old_value']) is int
    assert meta['old_value'] == int(value)
    assert meta['new_value'] is None


@pytest.mark.anyio
@pytest.mark.parametrize('value', [True, False])
async def test_duration_clear_event_encodes_boolean_old_value_as_integer(
    effects_site: FakeSiteCase, value: bool
) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    task = seed(effects_site, 'Task', {'content': 't1', 'project': project, 'duration': value})
    stored = cast(
        'dict[str, object]',
        await effects_site.call('find_one', 'Task', [['id', 'is', task['id']]], ['duration']),
    )
    assert stored['duration'] is value
    result = cast(
        'dict[str, object]',
        await effects_site.call('update', 'Task', task['id'], {'duration': None}),
    )
    assert result['duration'] is None
    rows = await _rows(effects_site, task, ['meta'])
    assert len(rows) == 1
    meta = cast('dict[str, object]', rows[0]['meta'])
    assert meta['field_data_type'] == 'duration'
    assert type(meta['old_value']) is int
    assert meta['old_value'] == int(value)
    assert meta['new_value'] is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('entity_type', 'field', 'changes'),
    [
        (
            'Version',
            'sg_movie_aspect_ratio',
            [
                (1.3333334, None, '1.333333'),
                (2.0, '1.333333', '2.0'),
                (-1.3333338, '2.0', '-1.333334'),
            ],
        ),
        ('Version', 'sg_first_frame', [(7, None, 7), (None, 7, None)]),
        (
            'Version',
            'media_center_import_time',
            [(datetime(2026, 7, 1, 12, tzinfo=UTC), None, '2026-07-01 12:00:00 UTC')],
        ),
        ('Version', 'sg_movie_has_slate', [(True, False, True)]),
        ('Task', 'start_date', [(None, '2026-07-01', None)]),
        ('Asset', 'sg_asset_type', [(None, 'Prop', None)]),
        ('Asset', 'sg_status_list', [(None, 'wtg', None)]),
    ],
)
async def test_scalar_event_encodings(
    effects_site: FakeSiteCase,
    entity_type: str,
    field: str,
    changes: list[tuple[object, object, object]],
) -> None:
    record = _scene(effects_site)[entity_type]
    for count, (value, old, new) in enumerate(changes, 1):
        await effects_site.call('update', entity_type, record['id'], {field: value})
        rows = await _rows(effects_site, record, ['meta'])
        assert len(rows) == count
        meta = cast('dict[str, object]', rows[-1]['meta'])
        assert meta['attribute_name'] == field
        assert meta['old_value'] == old
        assert meta['new_value'] == new
        assert type(meta['new_value']) is type(new)
        assert type(meta['old_value']) is type(old)


@pytest.mark.anyio
async def test_unsettled_float_encodings_keep_event_type_readable(
    effects_site: FakeSiteCase,
) -> None:
    version = _scene(effects_site)['Version']
    for count, value in enumerate([0.0078125, 1e-07], 1):
        await effects_site.call(
            'update', 'Version', version['id'], {'sg_movie_aspect_ratio': value}
        )
        rows = await _rows(effects_site, version, ['event_type'])
        assert len(rows) == count
        assert rows[-1]['event_type'] == 'Shotgun_Version_Change'
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[-1]['id']]], ['meta']
            )


@pytest.mark.anyio
async def test_same_value_guard_adds_no_event(effects_site: FakeSiteCase) -> None:
    shot = _scene(effects_site)['Shot']
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'same'})
    before = await _rows(effects_site, shot, ['event_type'])
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'same'})
    assert await _rows(effects_site, shot, ['event_type']) == before


@pytest.mark.anyio
async def test_batch_events_follow_request_order(effects_site: FakeSiteCase) -> None:
    scene = _scene(effects_site)
    before = await cursor(effects_site)
    await effects_site.call(
        'batch',
        [
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': scene['Shot']['id'],
                'data': {'description': 'new'},
            },
            {
                'request_type': 'update',
                'entity_type': 'Version',
                'entity_id': scene['Version']['id'],
                'data': {'code': 'v2'},
            },
        ],
    )
    rows = await delta(effects_site, before, ['event_type', 'entity'])
    assert [row['event_type'] for row in rows] == ['Shotgun_Shot_Change', 'Shotgun_Version_Change']
    assert [row['entity'] for row in rows] == [
        {**scene['Shot'], 'name': 's1'},
        {**scene['Version'], 'name': 'v2'},
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('failure', ['missing', 'invalid-status', 'unsettled-batch'])
async def test_failed_writes_guard_restore_event_rows_and_unknown_log(
    effects_site: FakeSiteCase, failure: str
) -> None:
    scene = _scene(effects_site)
    before = await effects_site.call('find', 'EventLogEntry', [], ['event_type'])
    if failure == 'invalid-status':
        with pytest.raises(Fault, match=r'bogus.*not a valid status'):
            await effects_site.call(
                'update', 'Asset', scene['Asset']['id'], {'sg_status_list': 'bogus'}
            )
        assert await effects_site.call(
            'find_one', 'Asset', [['id', 'is', scene['Asset']['id']]], ['sg_status_list']
        ) == {**scene['Asset'], 'sg_status_list': 'wtg'}
    else:
        batch = [
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': scene['Shot']['id'],
                'data': {'description': 'new'},
            }
        ]
        if failure == 'unsettled-batch':
            batch.append(
                {
                    'request_type': 'update',
                    'entity_type': 'Project',
                    'entity_id': scene['Project']['id'],
                    'data': {'name': 'Beta'},
                }
            )
        batch.append(
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': 99999,
                'data': {'description': 'missing'},
            }
        )
        with pytest.raises(Fault, match='99999'):
            await effects_site.call('batch', batch)
        assert await effects_site.call(
            'find_one', 'Shot', [['id', 'is', scene['Shot']['id']]], ['description']
        ) == {**scene['Shot'], 'description': 'old'}
        assert await effects_site.call(
            'find_one', 'Project', [['id', 'is', scene['Project']['id']]], ['name']
        ) == {**scene['Project'], 'name': 'Alpha'}
    assert await effects_site.call('find', 'EventLogEntry', [], ['event_type']) == before
    assert await _rows(effects_site, scene['Shot'], ['event_type']) == []


@pytest.mark.anyio
async def test_event_session_uuid_from_raw_and_client_auth(effects_site: FakeSiteCase) -> None:
    shot = _scene(effects_site)['Shot']
    result = raw_call(
        effects_site.client._server,
        'update',
        {**shot, 'fields': [{'field_name': 'description', 'value': 'raw'}]},
        auth={'script_name': 'queries', 'script_key': 'key', 'session_uuid': 'u-1'},
    )
    assert 'exception' not in result
    rows = await _rows(effects_site, shot, ['session_uuid'])
    assert [row['session_uuid'] for row in rows] == ['u-1']
    await effects_site.call('set_session_uuid', 'u-2')
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'client'})
    rows = await _rows(effects_site, shot, ['session_uuid'])
    assert [row['session_uuid'] for row in rows] == ['u-1', 'u-2']


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['projection', 'filter', 'order'])
async def test_written_description_is_unknown(effects_site: FakeSiteCase, operation: str) -> None:
    shot = _scene(effects_site)['Shot']
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'new'})
    if operation == 'projection':
        with pytest.raises(mockgun.MockgunError, match='description'):
            await _rows(effects_site, shot, ['description'])
    elif operation == 'filter':
        with pytest.raises(mockgun.MockgunError, match='description'):
            await effects_site.call('find', 'EventLogEntry', [['description', 'contains', 'x']])
    else:
        with pytest.raises(mockgun.MockgunError, match='description'):
            await effects_site.call(
                'find',
                'EventLogEntry',
                [],
                order=[{'field_name': 'description', 'direction': 'asc'}],
            )
    assert await effects_site.call(
        'find_one', 'EventLogEntry', [['id', 'is', 1]], ['description']
    ) == {'type': 'EventLogEntry', 'id': 1, 'description': 'Mockgun was born. Yay.'}


@pytest.mark.anyio
async def test_written_event_null_fields_and_omitted_audit_trail(
    effects_site: FakeSiteCase,
) -> None:
    shot = _scene(effects_site)['Shot']
    await effects_site.call('update', 'Shot', shot['id'], {'description': 'new'})
    fields = [
        'cached_display_name',
        'image',
        'filmstrip_image',
        'image_blur_hash',
        'image_source_entity',
    ]
    rows = await _rows(effects_site, shot, [*fields, 'audit_trail'])
    assert len(rows) == 1
    assert rows[0] == {'type': 'EventLogEntry', 'id': rows[0]['id'], **dict.fromkeys(fields)}


@pytest.mark.anyio
async def test_reply_event_project_comes_from_note(effects_site: FakeSiteCase) -> None:
    scene = _scene(effects_site)
    await effects_site.call('update', 'Reply', scene['Reply']['id'], {'content': 'r2'})
    rows = await _rows(effects_site, scene['Reply'], ['project'])
    assert len(rows) == 1
    assert rows[0]['project'] == {**scene['Project'], 'name': 'Alpha'}


@pytest.mark.anyio
async def test_unsettled_project_update_blocks_log_reads(effects_site: FakeSiteCase) -> None:
    project = _scene(effects_site)['Project']
    assert await effects_site.call('update', 'Project', project['id'], {'name': 'Beta'}) == {
        **project,
        'name': 'Beta',
    }
    for method in ('find', 'find_one'):
        with pytest.raises(mockgun.MockgunError, match=r'(?i)(Project.*update|update.*Project)'):
            await effects_site.call(method, 'EventLogEntry', [])


@pytest.mark.anyio
@pytest.mark.parametrize('method', ['create', 'update', 'delete', 'revive'])
async def test_unknown_log_rejects_direct_event_writes_without_mutation(
    effects_site: FakeSiteCase, method: str
) -> None:
    project = _scene(effects_site)['Project']
    await effects_site.call('update', 'Project', project['id'], {'name': 'Beta'})
    store = effects_site.client._server.store
    before = copy.deepcopy(store.rows('EventLogEntry', retired=False))
    args: tuple[object, ...] = (
        ({'event_type': 'x'},)
        if method == 'create'
        else (1, {'event_type': 'y'})
        if method == 'update'
        else (1,)
    )
    with pytest.raises(mockgun.MockgunError, match=r'(?i)(Project.*update|update.*Project)'):
        await effects_site.call(method, 'EventLogEntry', *args)
    assert store.rows('EventLogEntry', retired=False) == before
    assert store.rows('EventLogEntry', retired=True) == []
    assert store.require('EventLogEntry', 1)['description'] == 'Mockgun was born. Yay.'


@pytest.mark.anyio
async def test_known_log_allows_direct_event_create_then_becomes_unknown(
    effects_site: FakeSiteCase,
) -> None:
    result = cast(
        'dict[str, object]',
        await effects_site.call('create', 'EventLogEntry', {'event_type': 'x'}),
    )
    assert result['id'] == 2
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await effects_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
@pytest.mark.parametrize(
    'auth', [{}, {'session_token': 'unknown'}], ids=['no-credentials', 'unknown-token']
)
async def test_unsettled_auth_update_succeeds_and_blocks_log(
    effects_site: FakeSiteCase, auth: dict[str, object]
) -> None:
    shot = _scene(effects_site)['Shot']
    result = raw_call(
        effects_site.client._server,
        'update',
        {**shot, 'fields': [{'field_name': 'description', 'value': 'new'}]},
        auth=auth,
    )
    assert 'exception' not in result
    assert await effects_site.call(
        'find_one', 'Shot', [['id', 'is', shot['id']]], ['description']
    ) == {**shot, 'description': 'new'}
    with pytest.raises(mockgun.MockgunError):
        await effects_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
async def test_noncommon_declared_event_field_is_unknown(fake_site: FakeSiteCase) -> None:
    shot = seed(fake_site, 'Shot', {'code': 's1'})
    before = await cursor(fake_site)
    await fake_site.call('update', 'Shot', shot['id'], {'code': 's2'})
    rows = await delta(fake_site, before, ['event_type'])
    assert len(rows) == 1
    assert rows[0]['event_type'] == 'Shotgun_Shot_Change'
    with pytest.raises(mockgun.MockgunError, match='metadata'):
        await fake_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['metadata']
        )


@pytest.mark.anyio
async def test_schema_without_event_type_control_writes_no_events(
    effects_site: FakeSiteCase, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = {name: fields for name, fields in EFFECTS_SCHEMA.items() if name != 'EventLogEntry'}
    path = tmp_path / 'without-events.pickle'
    path.write_bytes(pickle.dumps(schema, protocol=2))
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', str(path))
    client = (mockgun.Shotgun if effects_site.kind == 'sync' else mockgun.AsyncShotgun)(
        'http://unit-test.example', script_name='queries', api_key='key'
    )
    case = FakeSiteCase(effects_site.kind, client)
    try:
        shot = seed(case, 'Shot', {'code': 's1'})
        assert await case.call('update', 'Shot', shot['id'], {'code': 's2'}) == {
            **shot,
            'code': 's2',
        }
        assert 'EventLogEntry' not in client._server.store._db
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_create_events_sequence_values_and_named_links(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    result = cast(
        'dict[str, object]',
        await effects_site.call(
            'create', 'Sequence', {'code': 'q1', 'project': project, 'description': None}
        ),
    )
    sequence = {'type': 'Sequence', 'id': result['id']}
    rows = await _rows(effects_site, sequence, _COMMON_FIELDS)
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Sequence_New',
        'Shotgun_Sequence_Change',
    ]
    assert [row['attribute_name'] for row in rows] == [None, 'code']
    assert rows[0]['meta'] == {
        'type': 'new_entity',
        'entity_type': 'Sequence',
        'entity_id': result['id'],
    }
    assert rows[1]['meta'] == {
        'type': 'attribute_change',
        'attribute_name': 'code',
        'entity_type': 'Sequence',
        'entity_id': result['id'],
        'field_data_type': 'text',
        'old_value': None,
        'new_value': 'q1',
        'in_create': True,
    }
    for row in rows:
        assert row['entity'] == {**sequence, 'name': 'q1'}
        assert row['project'] == {**project, 'name': 'Alpha'}
        assert row['user'] == {'type': 'ApiUser', 'id': 1, 'name': 'queries'}
        assert row['created_at'] == QUERY_NOW
    assert (
        await effects_site.call('find', 'EventLogEntry', [['entity', 'type_is', 'ApiUser']]) == []
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('create_event_site', 'entity_type', 'field', 'value'),
    [
        ('effects', 'Playlist', 'code', 'p1'),
        ('effects', 'Cut', 'code', 'c1'),
        ('effects', 'CutItem', 'code', 'ci1'),
        ('effects', 'PublishedFile', 'code', 'pf1'),
        ('plain-note', 'Note', 'subject', 'n'),
    ],
    indirect=['create_event_site'],
)
async def test_create_events_single_populated_field(
    create_event_site: FakeSiteCase, entity_type: str, field: str, value: str
) -> None:
    seed(create_event_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]', await create_event_site.call('create', entity_type, {field: value})
    )
    rows = await _rows(
        create_event_site,
        {'type': entity_type, 'id': record['id']},
        ['event_type', 'attribute_name', 'meta'],
    )
    assert [row['event_type'] for row in rows] == [
        f'Shotgun_{entity_type}_New',
        f'Shotgun_{entity_type}_Change',
    ]
    assert [row['attribute_name'] for row in rows] == [None, field]
    assert rows[0]['meta'] == {
        'type': 'new_entity',
        'entity_type': entity_type,
        'entity_id': record['id'],
    }
    assert rows[1]['meta'] == {
        'type': 'attribute_change',
        'attribute_name': field,
        'entity_type': entity_type,
        'entity_id': record['id'],
        'field_data_type': 'text',
        'old_value': None,
        'new_value': value,
        'in_create': True,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('create_event_site', 'entity_type', 'code', 'fields', 'values'),
    [
        (
            'shot-defaults',
            'Shot',
            's1',
            ['code', 'sg_status_list', 'sg_shot_type'],
            ['s1', 'wtg', 'Feature'],
        ),
        ('effects', 'Asset', 'a1', ['code', 'sg_status_list'], ['a1', 'wtg']),
    ],
    indirect=['create_event_site'],
)
async def test_create_events_observed_defaults_order(
    create_event_site: FakeSiteCase,
    entity_type: str,
    code: str,
    fields: list[str],
    values: list[str],
) -> None:
    project = seed(create_event_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]',
        await create_event_site.call('create', entity_type, {'code': code, 'project': project}),
    )
    rows = await _rows(
        create_event_site,
        {'type': entity_type, 'id': record['id']},
        ['event_type', 'attribute_name', 'meta'],
    )
    assert [row['event_type'] for row in rows] == [
        f'Shotgun_{entity_type}_New',
        *([f'Shotgun_{entity_type}_Change'] * len(fields)),
    ]
    assert [row['attribute_name'] for row in rows] == [None, *fields]
    assert [cast('dict[str, object]', row['meta'])['new_value'] for row in rows[1:]] == values
    assert all(cast('dict[str, object]', row['meta'])['in_create'] is True for row in rows[1:])


@pytest.mark.anyio
@pytest.mark.parametrize(
    'supply_order',
    [
        ['description', 'sg_first_frame', 'code'],
        ['code', 'sg_first_frame', 'description'],
        ['sg_first_frame', 'code', 'description'],
    ],
)
async def test_create_events_version_observed_order(
    effects_site: FakeSiteCase, supply_order: list[str]
) -> None:
    seed(effects_site, 'Project', {'name': 'Alpha'})
    values = {'code': 'v1', 'description': 'd', 'sg_first_frame': 7}
    record = cast(
        'dict[str, object]',
        await effects_site.call(
            'create', 'Version', {field: values[field] for field in supply_order}
        ),
    )
    rows = await _rows(
        effects_site, {'type': 'Version', 'id': record['id']}, ['event_type', 'attribute_name']
    )
    expected = [
        'code',
        'sg_status_list',
        'user',
        *[field for field in supply_order if field != 'code'],
    ]
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Version_New',
        *(['Shotgun_Version_Change'] * 5),
    ]
    assert [row['attribute_name'] for row in rows] == [None, *expected]
    with pytest.raises(mockgun.MockgunError, match='meta'):
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', rows[3]['id']]], ['meta']
        )
    for row, field in zip(rows[1:], expected, strict=True):
        if field != 'user':
            found = cast(
                'dict[str, object]',
                await effects_site.call(
                    'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['meta']
                ),
            )
            meta = cast('dict[str, object]', found['meta'])
            assert meta['new_value'] == ('rev' if field == 'sg_status_list' else values[field])
            assert meta['in_create'] is True


# C4 EV-04's Version creates omitted the checkbox that C5-04 read back as its False default.
@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['slate-default'], indirect=True)
async def test_create_events_version_default_checkbox_adds_no_row(
    create_event_site: FakeSiteCase,
) -> None:
    seed(create_event_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]',
        await create_event_site.call(
            'create', 'Version', {'code': 'v1', 'description': 'd', 'sg_first_frame': 7}
        ),
    )
    rows = await _rows(
        create_event_site, {'type': 'Version', 'id': record['id']}, ['attribute_name']
    )
    assert [row['attribute_name'] for row in rows] == [
        None,
        'code',
        'sg_status_list',
        'user',
        'description',
        'sg_first_frame',
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['slate-default'], indirect=True)
async def test_create_events_version_supplied_checkbox_stays_unsettled(
    create_event_site: FakeSiteCase,
) -> None:
    seed(create_event_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]',
        await create_event_site.call(
            'create', 'Version', {'code': 'v1', 'sg_movie_has_slate': False}
        ),
    )
    with pytest.raises(mockgun.MockgunError):
        await _rows(create_event_site, {'type': 'Version', 'id': record['id']}, ['attribute_name'])


# 2026-11-10 trial capture: no create logs a number, duration or text default, whatever the
# entity type.
@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['site-defaults'], indirect=True)
@pytest.mark.parametrize(
    ('kind', 'data', 'expected'),
    [
        ('Task', {'content': 't'}, ['Task:', 'Task:color', 'Task:content', 'Task:sg_status_list']),
        (
            'Note',
            {'subject': 's', 'content': 'c'},
            ['Note:', 'Note:user', 'Note:sg_status_list', 'Note:subject', 'Note:content'],
        ),
        (
            'Reply',
            {'entity': 'note', 'content': 'c'},
            ['Reply:', 'Reply:user', 'Reply:entity', 'Note:replies', 'Reply:content'],
        ),
        ('Shot', {'code': 's2'}, ['Shot:', 'Shot:code']),
    ],
)
async def test_create_events_under_generated_schema_defaults(
    create_event_site: FakeSiteCase, kind: str, data: dict[str, str], expected: list[str]
) -> None:
    project = seed(create_event_site, 'Project', {'name': 'Alpha'})
    links = {
        'shot': [seed(create_event_site, 'Shot', {'code': 's1', 'project': project})],
        'note': seed(create_event_site, 'Note', {'subject': 'n', 'project': project}),
    }
    values = {name: links.get(value, value) for name, value in data.items()}
    before = await cursor(create_event_site)
    await create_event_site.call(
        'create', kind, {**({} if kind == 'Reply' else {'project': project}), **values}
    )
    rows = await delta(create_event_site, before, ['event_type', 'attribute_name'])
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        (
            f'Shotgun_{item.split(":")[0]}_{"Change" if item.split(":")[1] else "New"}',
            item.split(':')[1] or None,
        )
        for item in expected
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['site-defaults'], indirect=True)
async def test_create_events_supplied_unlogged_default_stays_unsettled(
    create_event_site: FakeSiteCase,
) -> None:
    project = seed(create_event_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]',
        await create_event_site.call(
            'create', 'Note', {'project': project, 'subject': 's', 'publish_status': 'published'}
        ),
    )
    with pytest.raises(mockgun.MockgunError, match=rf'Note {record["id"]} create has unsettled'):
        await cursor(create_event_site)


@pytest.mark.anyio
async def test_create_events_supplied_audit_times_are_unknown(effects_site: FakeSiteCase) -> None:
    project = seed(effects_site, 'Project', {'name': 'Alpha'})
    record = cast(
        'dict[str, object]',
        await effects_site.call(
            'create',
            'Sequence',
            {
                'code': 'q3',
                'project': project,
                'created_at': QUERY_NOW - timedelta(hours=2),
                'updated_at': QUERY_NOW - timedelta(hours=1),
            },
        ),
    )
    sequence = {'type': 'Sequence', 'id': record['id']}
    rows = await _rows(effects_site, sequence, ['id', 'entity', 'event_type'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Sequence_New',
        'Shotgun_Sequence_Change',
    ]
    for row in rows:
        assert row['entity'] == {**sequence, 'name': 'q3'}
        with pytest.raises(mockgun.MockgunError, match='created_at') as error:
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['created_at']
            )
        assert 'EventLogEntry' in str(error.value)
        assert f'record {row["id"]}' in str(error.value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('create_event_site', 'variant'),
    [
        ('shot-defaults', 'shot-description'),
        ('effects', 'version-code'),
        ('effects', 'task-color'),
        ('effects', 'version-link'),
        ('effects', 'shot-assets'),
    ],
    indirect=['create_event_site'],
)
async def test_create_events_unsettled_guard(
    create_event_site: FakeSiteCase, variant: str
) -> None:
    scene = _scene(create_event_site)
    creates = {
        'shot-description': ('Shot', {'code': 's2', 'description': 'd'}),
        'version-code': ('Version', {'code': 'v1'}),
        'task-color': ('Task', {'content': 't'}),
        'version-link': ('Version', {'code': 'v2', 'entity': scene['Shot']}),
        'shot-assets': ('Shot', {'code': 's3', 'assets': [scene['Asset']]}),
    }
    entity_type, data = creates[variant]
    await create_event_site.call('create', entity_type, data)
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await create_event_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
async def test_link_events_asset_add_remove_and_historical_meta(
    effects_site: FakeSiteCase,
) -> None:
    scene = _scene(effects_site)
    shot, asset = scene['Shot'], scene['Asset']
    before = await cursor(effects_site)
    await effects_site.call('update', 'Shot', shot['id'], {'assets': [asset]}, {'assets': 'add'})
    added = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [row['event_type'] for row in added] == [
        'Shotgun_AssetShotConnection_New',
        'Shotgun_Shot_Change',
        'Shotgun_Asset_Change',
    ]
    assert [row['attribute_name'] for row in added] == [None, 'assets', 'shots']
    forward = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', added[1]['id']]], ['entity', 'meta']
        ),
    )
    assert forward['entity'] == {**shot, 'name': 's1'}
    meta = cast('dict[str, object]', forward['meta'])
    elements = cast('list[dict[str, object]]', meta['added'])
    uuid = cast('str', elements[0]['uuid'])
    assert str(UUID(uuid)) == uuid
    expected_element = {**asset, 'name': 'a1', 'valid': 'valid', 'status': 'wtg', 'uuid': uuid}
    expected_meta = {
        'type': 'attribute_change',
        'attribute_name': 'assets',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'field_data_type': 'multi_entity',
        'added': [expected_element],
        'removed': [],
    }
    assert meta == expected_meta
    inverse = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', added[2]['id']]], ['entity']
        ),
    )
    assert inverse['entity'] == {**asset, 'name': 'a1'}
    with pytest.raises(mockgun.MockgunError, match='meta'):
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', added[2]['id']]], ['meta']
        )
    for field in ('entity', 'project', 'created_at'):
        with pytest.raises(mockgun.MockgunError):
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', added[0]['id']]], [field]
            )
    before = await cursor(effects_site)
    await effects_site.call(
        'update', 'Shot', shot['id'], {'assets': [asset]}, {'assets': 'remove'}
    )
    removed = await delta(effects_site, before, ['event_type', 'attribute_name'])
    assert [row['event_type'] for row in removed] == [
        'Shotgun_Shot_Change',
        'Shotgun_Asset_Change',
    ]
    assert [row['attribute_name'] for row in removed] == ['assets', 'shots']
    forward_removed = cast(
        'dict[str, object]',
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', removed[0]['id']]], ['meta']
        ),
    )
    assert forward_removed['meta'] == {**expected_meta, 'added': [], 'removed': [expected_element]}
    await effects_site.call('update', 'Asset', asset['id'], {'code': 'a2'})
    with pytest.raises(mockgun.MockgunError, match='meta'):
        await effects_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', added[1]['id']]], ['meta']
        )


@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['partial-asset'], indirect=True)
@pytest.mark.parametrize('mode', ['add', 'remove'])
@pytest.mark.parametrize('sudo', [False, True], ids=['script', 'sudo'])
async def test_link_events_shot_assets_without_inverse_declaration(
    create_event_site: FakeSiteCase, mode: str, *, sudo: bool
) -> None:
    case = FakeSiteCase(
        create_event_site.kind,
        create_event_site.client.new_client(
            **(
                {'sudo_as_login': 'artist'}
                if sudo
                else {'script_name': 'queries', 'api_key': 'key'}
            )
        ),
    )
    try:
        asset = seed(case, 'Asset', {'code': 'a1', 'sg_asset_type': 'Prop'})
        shot = seed(case, 'Shot', {'code': 's1', 'assets': [asset] if mode == 'remove' else []})
        before = await cursor(case)
        await case.call('update', 'Shot', shot['id'], {'assets': [asset]}, {'assets': mode})
        rows = await delta(case, before, ['event_type', 'attribute_name', 'entity', 'user'])
        assert [(row['event_type'], row['attribute_name']) for row in rows] == [
            ('Shotgun_Shot_Change', 'assets'),
            ('Shotgun_Asset_Change', 'shots'),
        ]
        assert [row['entity'] for row in rows] == [
            {**shot, 'name': 's1'},
            {**asset, 'name': 'a1'},
        ]
        assert [cast('dict[str, object]', row['user'])['type'] for row in rows] == [
            'HumanUser' if sudo else 'ApiUser'
        ] * 2
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await case.call('find_one', 'EventLogEntry', [['id', 'is', rows[1]['id']]], ['meta'])
        if sudo:
            with pytest.raises(mockgun.MockgunError, match='meta'):
                await case.call(
                    'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
                )
        else:
            forward = cast(
                'dict[str, object]',
                await case.call(
                    'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
                ),
            )
            meta = cast('dict[str, object]', forward['meta'])
            elements = cast(
                'list[dict[str, object]]', meta['added' if mode == 'add' else 'removed']
            )
            assert len(elements) == 1
            uuid = cast('str', elements[0]['uuid'])
            assert str(UUID(uuid)) == uuid
            assert meta == {
                'type': 'attribute_change',
                'attribute_name': 'assets',
                'entity_type': 'Shot',
                'entity_id': shot['id'],
                'field_data_type': 'multi_entity',
                'added': elements if mode == 'add' else [],
                'removed': elements if mode == 'remove' else [],
            }
            assert elements == [
                {**asset, 'name': 'a1', 'valid': 'valid', 'status': 'wtg', 'uuid': uuid}
            ]
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_workflow_link_update_missing_inverse_stays_unsettled(
    workflow_site: FakeSiteCase,
) -> None:
    store = workflow_site.client._server.store
    store.schema.fields['Shot'].pop('assets')
    shot = seed(workflow_site, 'Shot', {'code': 's1'})
    asset = seed(workflow_site, 'Asset', {'code': 'a1'})
    await workflow_site.call('update', 'Asset', asset['id'], {'shots': [shot]}, {'shots': 'add'})
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await workflow_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
async def test_link_events_note_has_forward_rows_only(effects_site: FakeSiteCase) -> None:
    scene = _scene(effects_site)
    for mode in ('add', 'remove'):
        before = await cursor(effects_site)
        await effects_site.call(
            'update',
            'Note',
            scene['Note']['id'],
            {'note_links': [scene['Shot']]},
            {'note_links': mode},
        )
        rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'entity'])
        assert len(rows) == 1
        assert rows[0]['event_type'] == 'Shotgun_Note_Change'
        assert rows[0]['attribute_name'] == 'note_links'
        assert rows[0]['entity'] == {**scene['Note'], 'name': 'n'}
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await effects_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
            )


@pytest.mark.anyio
@pytest.mark.parametrize('variant', ['asset-shots', 'shot-sequence', 'two-assets', 'version-task'])
async def test_link_events_unsettled_guard(effects_site: FakeSiteCase, variant: str) -> None:
    scene = _scene(effects_site)
    shot2 = seed(effects_site, 'Shot', {'code': 's2', 'project': scene['Project']})
    asset2 = seed(effects_site, 'Asset', {'code': 'a2', 'project': scene['Project']})
    sequence = seed(effects_site, 'Sequence', {'code': 'q', 'project': scene['Project']})
    writes: dict[str, tuple[str, object, dict[str, object], dict[str, str]]] = {
        'asset-shots': ('Asset', scene['Asset']['id'], {'shots': [shot2]}, {'shots': 'add'}),
        'shot-sequence': ('Shot', scene['Shot']['id'], {'sg_sequence': sequence}, {}),
        'two-assets': (
            'Shot',
            scene['Shot']['id'],
            {'assets': [scene['Asset'], asset2]},
            {'assets': 'add'},
        ),
        'version-task': ('Version', scene['Version']['id'], {'sg_task': scene['Task']}, {}),
    }
    before = await cursor(effects_site)
    await effects_site.call('update', *writes[variant])
    if variant == 'two-assets':
        with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
            await effects_site.call('find', 'EventLogEntry', [])
    else:
        rows = await delta(effects_site, before, ['attribute_name'])
        expected = {
            'asset-shots': [None, 'shots', 'assets'],
            'shot-sequence': ['sg_sequence', 'shots'],
            'version-task': ['sg_task', 'sg_versions'],
        }
        assert [row['attribute_name'] for row in rows] == expected[variant]


@pytest.mark.anyio
async def test_create_events_batch_precedes_update_rows(effects_site: FakeSiteCase) -> None:
    scene = _scene(effects_site)
    before = await cursor(effects_site)
    results = cast(
        'list[dict[str, object]]',
        await effects_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Sequence', 'data': {'code': 'q2'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': scene['Shot']['id'],
                    'data': {'description': 'new'},
                },
            ],
        ),
    )
    rows = await delta(effects_site, before, ['event_type', 'attribute_name', 'entity'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_Sequence_New',
        'Shotgun_Sequence_Change',
        'Shotgun_Shot_Change',
    ]
    assert [row['attribute_name'] for row in rows] == [None, 'code', 'description']
    assert [row['entity'] for row in rows] == [
        {'type': 'Sequence', 'id': results[0]['id'], 'name': 'q2'},
        {'type': 'Sequence', 'id': results[0]['id'], 'name': 'q2'},
        {**scene['Shot'], 'name': 's1'},
    ]


@pytest.mark.anyio
async def test_seed_events_control_adds_no_rows(effects_site: FakeSiteCase) -> None:
    before = await effects_site.call('find', 'EventLogEntry', [], ['event_type'])
    _scene(effects_site)
    assert await effects_site.call('find', 'EventLogEntry', [], ['event_type']) == before


@pytest.mark.anyio
async def test_link_events_uuid_survives_snapshot_restore(effects_site: FakeSiteCase) -> None:
    scene = _scene(effects_site)
    store = effects_site.client._server.store
    snapshot = store.snapshot()
    uuids: list[object] = []
    for _ in range(2):
        await effects_site.call(
            'update',
            'Shot',
            scene['Shot']['id'],
            {'assets': [scene['Asset']]},
            {'assets': 'add'},
        )
        rows = await _rows(effects_site, scene['Shot'], ['meta'])
        meta = cast('dict[str, object]', rows[0]['meta'])
        added = cast('list[dict[str, object]]', meta['added'])
        uuids.append(added[0]['uuid'])
        store.restore(snapshot)
    assert uuids[0] == uuids[1]


@pytest.mark.anyio
async def test_link_events_preexisting_connection_and_noop_add_no_new_rows(
    effects_site: FakeSiteCase,
) -> None:
    scene = _scene(effects_site)
    seed(effects_site, 'AssetShotConnection', {'asset': scene['Asset'], 'shot': scene['Shot']})
    before = await cursor(effects_site)
    await effects_site.call(
        'update', 'Shot', scene['Shot']['id'], {'assets': [scene['Asset']]}, {'assets': 'add'}
    )
    rows = await delta(effects_site, before, ['event_type'])
    assert [row['event_type'] for row in rows] == ['Shotgun_Shot_Change', 'Shotgun_Asset_Change']
    before = await cursor(effects_site)
    await effects_site.call(
        'update', 'Shot', scene['Shot']['id'], {'assets': [scene['Asset']]}, {'assets': 'add'}
    )
    assert await delta(effects_site, before, ['event_type']) == []
    assert effects_site.client._server.store.created_connections == []


@pytest.mark.anyio
async def test_link_events_failed_batch_clears_connection_provenance(
    effects_site: FakeSiteCase,
) -> None:
    scene = _scene(effects_site)
    before = await cursor(effects_site)
    with pytest.raises(Fault, match='99999'):
        await effects_site.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': scene['Shot']['id'],
                    'data': {'assets': [scene['Asset']]},
                    'multi_entity_update_modes': {'assets': 'add'},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'description': 'missing'},
                },
            ],
        )
    assert await delta(effects_site, before, ['event_type']) == []
    assert effects_site.client._server.store.created_connections == []
    assert await effects_site.call('find', 'AssetShotConnection', []) == []
    await effects_site.call(
        'update', 'Shot', scene['Shot']['id'], {'assets': [scene['Asset']]}, {'assets': 'add'}
    )
    rows = await delta(effects_site, before, ['event_type'])
    assert [row['event_type'] for row in rows] == [
        'Shotgun_AssetShotConnection_New',
        'Shotgun_Shot_Change',
        'Shotgun_Asset_Change',
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('kind', 'order', 'expected'),
    [
        (
            'Task',
            ['project', 'content', 'entity'],
            [
                'Task:',
                'Task:color',
                'Task:content',
                'Task:sg_status_list',
                'Task:entity',
                'Shot:tasks',
            ],
        ),
        (
            'Task',
            ['entity', 'content', 'project'],
            [
                'Task:',
                'Task:color',
                'Task:content',
                'Task:sg_status_list',
                'Task:entity',
                'Shot:tasks',
            ],
        ),
        (
            'Note',
            ['project', 'subject', 'content', 'note_links'],
            [
                'Note:',
                'Note:user',
                'Note:sg_status_list',
                'Note:subject',
                'Note:content',
                'Note:note_links',
            ],
        ),
        (
            'Note',
            ['note_links', 'content', 'subject', 'project'],
            [
                'Note:',
                'Note:user',
                'Note:sg_status_list',
                'Note:note_links',
                'Note:content',
                'Note:subject',
            ],
        ),
        (
            'Reply',
            ['entity', 'content'],
            ['Reply:', 'Reply:user', 'Reply:entity', 'Note:replies', 'Reply:content'],
        ),
        (
            'Reply',
            ['content', 'entity'],
            ['Reply:', 'Reply:user', 'Reply:content', 'Reply:entity', 'Note:replies'],
        ),
        (
            'Sequence',
            ['project', 'code', 'description'],
            ['Sequence:', 'Sequence:code', 'Sequence:sg_status_list', 'Sequence:description'],
        ),
        ('Cut', ['project', 'code'], ['Cut:', 'Cut:code', 'Cut:sg_status_list']),
        (
            'CutItem',
            ['project', 'code', 'cut'],
            ['CutItem:', 'CutItem:code', 'CutItem:cut', 'Cut:cut_items'],
        ),
        (
            'CutItem',
            ['shot', 'version', 'cut', 'project', 'code'],
            [
                'CutItem:',
                'CutItem:code',
                'CutItem:shot',
                'CutItem:version',
                'CutItem:cut',
                'Cut:cut_items',
            ],
        ),
        (
            'Playlist',
            ['project', 'code', 'versions', 'description'],
            [
                'Playlist:',
                'PlaylistVersionConnection:',
                'Playlist:code',
                'Playlist:versions',
                'Version:playlists',
                'Playlist:description',
            ],
        ),
    ],
)
async def test_workflow_create_event_forms(
    workflow_site: FakeSiteCase, kind: str, order: list[str], expected: list[str]
) -> None:
    scene = _workflow_scene(workflow_site)
    if kind == 'Reply':
        note = cast(
            'dict[str, object]',
            await workflow_site.call(
                'create',
                'Note',
                {'project': scene['Project'], 'subject': 's', 'content': 'c'},
            ),
        )
        scene['Note'] = {'type': 'Note', 'id': note['id']}
    values: dict[str, object] = {
        'project': scene['Project'],
        'content': 't',
        'subject': 's',
        'code': 'c',
        'description': 'd',
        'entity': scene['Note' if kind == 'Reply' else 'Shot'],
        'note_links': [scene['Shot']],
        'cut': scene['Cut'],
        'shot': scene['Shot'],
        'version': scene['Version'],
        'versions': [scene['Version']],
    }
    before = await cursor(workflow_site)
    created = cast(
        'dict[str, object]',
        await workflow_site.call('create', kind, {field: values[field] for field in order}),
    )
    rows = await delta(workflow_site, before, ['event_type', 'attribute_name'])
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        (
            f'Shotgun_{item.split(":")[0]}_{"Change" if item.split(":")[1] else "New"}',
            item.split(':')[1] or None,
        )
        for item in expected
    ]
    if kind == 'Task':
        content = await workflow_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', rows[2]['id']]], ['meta']
        )
        assert cast('dict[str, object]', content)['meta'] == {
            'type': 'attribute_change',
            'attribute_name': 'content',
            'entity_type': 'Task',
            'entity_id': created['id'],
            'field_data_type': 'text',
            'old_value': None,
            'new_value': 't',
            'in_create': True,
        }
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await workflow_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[-1]['id']]], ['meta']
            )
        inverse = cast(
            'dict[str, object]',
            await workflow_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[-1]['id']]], ['entity']
            ),
        )
        assert inverse['entity'] == {**scene['Shot'], 'name': 's1'}
    if kind == 'CutItem':
        with pytest.raises(mockgun.MockgunError, match='meta'):
            await workflow_site.call(
                'find_one', 'EventLogEntry', [['id', 'is', rows[1]['id']]], ['meta']
            )


@pytest.mark.anyio
@pytest.mark.parametrize('variant', ['sudo-note', 'followed-reply'])
async def test_workflow_reading_rows(workflow_site: FakeSiteCase, variant: str) -> None:
    scene = _workflow_scene(workflow_site)
    sudo_client = workflow_site.client.new_client(sudo_as_login='artist')
    case = FakeSiteCase(workflow_site.kind, sudo_client)
    try:
        await case.call('find', 'HumanUser', [])
        artist = workflow_site.client._server.store.rows('HumanUser', retired=False)[0]
        before = await cursor(workflow_site)
        if variant == 'sudo-note':
            await case.call(
                'create',
                'Note',
                {'note_links': [scene['Shot']], 'subject': 's', 'project': scene['Project']},
            )
            expected = [
                'read_by_current_user',
                None,
                'user',
                'sg_status_list',
                'note_links',
                'subject',
            ]
        else:
            workflow_site.client._server.store.side_set(
                'follows',
                (entity_id(artist), 'Note', entity_id(scene['Note'])),
                value=True,
            )
            await workflow_site.call('create', 'Reply', {'entity': scene['Note'], 'content': 'r'})
            expected = ['read_by_current_user', None, 'user', 'entity', 'replies', 'content']
        rows = await delta(workflow_site, before, ['event_type', 'attribute_name'])
        assert [row['attribute_name'] for row in rows] == expected
        assert rows[0]['event_type'] == 'Shotgun_Reading_Change'
        if variant == 'sudo-note':
            for row in rows:
                found = cast(
                    'dict[str, object]',
                    await workflow_site.call(
                        'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['user']
                    ),
                )
                assert found['user'] == {'type': 'HumanUser', 'id': artist['id'], 'name': 'artist'}
                with pytest.raises(mockgun.MockgunError, match='meta'):
                    await workflow_site.call(
                        'find_one', 'EventLogEntry', [['id', 'is', row['id']]], ['meta']
                    )
        else:
            with pytest.raises(mockgun.MockgunError, match='user'):
                await workflow_site.call(
                    'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['user']
                )
    finally:
        await case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize(
    'variant', ['task-asset', 'note-subject', 'reply-order', 'cut-entity', 'sudo-order']
)
async def test_workflow_create_event_guards(workflow_site: FakeSiteCase, variant: str) -> None:
    scene = _workflow_scene(workflow_site)
    sudo_client = workflow_site.client.new_client(sudo_as_login='artist')
    sudo_case = FakeSiteCase(workflow_site.kind, sudo_client)
    try:
        await sudo_case.call('find', 'HumanUser', [])
        artist = workflow_site.client._server.store.rows('HumanUser', retired=False)[0]
        workflow_site.client._server.store.side_set(
            'follows',
            (entity_id(artist), 'Note', entity_id(scene['Note'])),
            value=True,
        )
        forms: dict[str, tuple[str, dict[str, object]]] = {
            'task-asset': (
                'Task',
                {'project': scene['Project'], 'content': 't', 'entity': scene['Asset']},
            ),
            'note-subject': ('Note', {'project': scene['Project'], 'subject': 's'}),
            'reply-order': ('Reply', {'content': 'r', 'entity': scene['Note']}),
            'cut-entity': (
                'Cut',
                {'project': scene['Project'], 'code': 'c', 'entity': scene['Sequence']},
            ),
            'sudo-order': (
                'Note',
                {'subject': 's', 'note_links': [scene['Shot']], 'project': scene['Project']},
            ),
        }
        case = sudo_case if variant == 'sudo-order' else workflow_site
        await case.call('create', *forms[variant])
        with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
            await workflow_site.call('find', 'EventLogEntry', [])
    finally:
        await sudo_case.call('close')


@pytest.mark.anyio
@pytest.mark.parametrize(
    'form',
    [
        ('Shot', 'sg_sequence', 'Sequence', 0, 1, None, 'shots'),
        ('Shot', 'sg_sequence', 'Sequence', 1, 1, None, 'shots'),
        ('Sequence', 'shots', 'Shot', 0, 1, None, 'sg_sequence'),
        ('Task', 'entity', 'Shot', 0, 1, None, 'tasks'),
        ('Task', 'entity', 'Shot', 1, 1, None, 'tasks'),
        ('Version', 'playlists', 'Playlist', 0, 2, 'PlaylistVersionConnection', 'versions'),
        ('Playlist', 'versions', 'Version', 0, 1, 'PlaylistVersionConnection', 'playlists'),
        ('Playlist', 'versions', 'Version', 2, 0, None, 'playlists'),
        ('Asset', 'parents', 'Asset', 0, 1, 'AssetAssetConnection', 'assets'),
        ('Asset', 'parents', 'Asset', 1, 1, 'AssetAssetConnection', 'assets'),
        ('Asset', 'shots', 'Shot', 0, 1, 'AssetShotConnection', 'assets'),
        ('Version', 'sg_task', 'Task', 0, 1, None, 'sg_versions'),
        ('CutItem', 'cut', 'Cut', 1, 1, None, 'cut_items'),
        ('Note', 'note_links', 'Shot', 0, 2, None, None),
        ('Note', 'note_links', 'Shot', 2, 1, None, None),
    ],
)
async def test_workflow_link_update_forms(
    workflow_site: FakeSiteCase,
    form: tuple[str, str, str, int, int, str | None, str | None],
) -> None:
    kind, field, target, old_count, new_count, connection, inverse = form
    project = seed(workflow_site, 'Project', {'name': 'Alpha'})
    targets = [
        seed(
            workflow_site,
            'Asset' if kind == 'Note' and index == 1 else target,
            {('content' if target == 'Task' else 'code'): f'target{index}', 'project': project},
        )
        for index in range(old_count + new_count)
    ]
    old, new = targets[:old_count], targets[old_count:]
    multi = workflow_site.client._server.store.schema.field_type(kind, field) == 'multi_entity'
    row = seed(
        workflow_site,
        kind,
        {
            ('subject' if kind == 'Note' else 'content' if kind == 'Task' else 'code'): 'writer',
            'project': project,
            field: old if multi else (old[0] if old else None),
        },
    )
    before = await cursor(workflow_site)
    mode = 'remove' if new_count == 0 else 'add' if old_count == 0 else 'set'
    value = (old if mode == 'remove' else new) if multi else new[0]
    await workflow_site.call(
        'update', kind, row['id'], {field: value}, {field: mode} if multi else {}
    )
    rows = await delta(workflow_site, before, ['event_type', 'attribute_name'])
    expected = ([(f'Shotgun_{connection}_New', None)] * new_count if connection else []) + [
        (f'Shotgun_{kind}_Change', field)
    ]
    if inverse:
        expected += [(f'Shotgun_{target}_Change', inverse)] * len(targets)
    assert [(event['event_type'], event['attribute_name']) for event in rows] == expected
    if inverse:
        entities = [
            cast(
                'dict[str, object]',
                await workflow_site.call(
                    'find_one', 'EventLogEntry', [['id', 'is', event['id']]], ['entity']
                ),
            )['entity']
            for event in rows[-len(targets) :]
        ]
        assert [cast('dict[str, object]', entity)['id'] for entity in entities] == [
            link['id'] for link in targets
        ]
    if kind == 'Asset' and field == 'parents':
        forward_id = rows[new_count]['id']
        for event in rows[-len(targets) :]:
            found = cast(
                'dict[str, object]',
                await workflow_site.call(
                    'find_one', 'EventLogEntry', [['id', 'is', event['id']]], ['meta']
                ),
            )
            meta = cast('dict[str, object]', found['meta'])
            assert meta['original_event_log_entry_id'] == forward_id
            assert meta['actual_attribute_changed'] == 'Asset.parents'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'variant', ['sequence-remove', 'version-one', 'version-reparent', 'mixed']
)
async def test_workflow_link_update_guards(workflow_site: FakeSiteCase, variant: str) -> None:
    scene = _workflow_scene(workflow_site)
    if variant == 'sequence-remove':
        row = seed(workflow_site, 'Sequence', {'shots': [scene['Shot']]})
        await workflow_site.call('update', 'Sequence', row['id'], {'shots': []})
    elif variant == 'version-one':
        await workflow_site.call(
            'update', 'Version', scene['Version']['id'], {'playlists': [scene['Playlist']]}
        )
    elif variant == 'version-reparent':
        row = seed(workflow_site, 'Version', {'sg_task': scene['Task']})
        other = seed(workflow_site, 'Task', {'content': 'other'})
        await workflow_site.call('update', 'Version', row['id'], {'sg_task': other})
    else:
        await workflow_site.call(
            'update',
            'Shot',
            scene['Shot']['id'],
            {'sg_sequence': scene['Sequence'], 'description': 'changed'},
        )
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await workflow_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
@pytest.mark.parametrize('create_event_site', ['plain-note'], indirect=True)
async def test_sudo_plain_note_reading_fidelity(create_event_site: FakeSiteCase) -> None:
    case = FakeSiteCase(
        create_event_site.kind, create_event_site.client.new_client(sudo_as_login='artist')
    )
    try:
        before = await cursor(create_event_site)
        await case.call('create', 'Note', {'subject': 'n'})
        rows = await delta(create_event_site, before, ['event_type', 'attribute_name'])
        assert [row['attribute_name'] for row in rows] == ['read_by_current_user', None, 'subject']
    finally:
        await case.call('close')


@pytest.mark.anyio
async def test_event_planner_independent_of_log(workflow_site: FakeSiteCase) -> None:
    store = workflow_site.client._server.store
    project = seed(workflow_site, 'Project', {'name': 'Alpha'})
    shot = seed(workflow_site, 'Shot', {'code': 's'})
    context = RequestContext(_Actor('ApiUser', 1, 'queries'), QUERY_NOW)
    store.schema.fields.pop('EventLogEntry')
    row = store.create(
        'Task', {'project': project, 'content': 't', 'entity': shot}, context=context
    )
    plans = _events._plan_create(store, row, ['project', 'content', 'entity'], context=context)
    assert plans is not None
    assert len(plans) == 6
    assert (
        _events.record_create(store, row, ['project', 'content', 'entity'], context=context)
        is True
    )
    store.events_unknown = 'test tainted log'
    assert (
        _events._plan_create(store, row, ['project', 'content', 'entity'], context=context)
        == plans
    )
    row = store.create('Task', {'content': 't'}, context=context)
    assert _events._plan_create(store, row, ['content'], context=context) is None


@pytest.mark.anyio
async def test_workflow_lifecycle_retirement_metadata(workflow_site: FakeSiteCase) -> None:
    shot = seed(workflow_site, 'Shot', {'code': 's1'})
    before = await cursor(workflow_site)
    await workflow_site.call('delete', 'Shot', shot['id'])
    rows = await delta(workflow_site, before, ['meta'])
    expected = {
        'type': 'attribute_change',
        'attribute_name': 'retirement_date',
        'entity_type': 'Shot',
        'entity_id': shot['id'],
        'old_value': None,
        'new_value': '2026-07-15 12:00:00 UTC',
    }
    assert rows[0]['meta'] == expected
    assert cast('dict[str, object]', rows[1]['meta'])['retirement_date'] == expected['new_value']
    assert cast('dict[str, object]', rows[1]['meta'])['display_name'] == 's1'
    workflow_site.client.site.set_time(QUERY_NOW + timedelta(hours=1))
    before = await cursor(workflow_site)
    await workflow_site.call('revive', 'Shot', shot['id'])
    rows = await delta(workflow_site, before, ['meta'])
    assert rows[0]['meta'] == {**expected, 'old_value': expected['new_value'], 'new_value': None}


@pytest.mark.anyio
async def test_sequence_event_provenance_rejects_reparent(workflow_site: FakeSiteCase) -> None:
    previous = seed(workflow_site, 'Sequence', {'code': 'previous'})
    shot = seed(workflow_site, 'Shot', {'code': 's', 'sg_sequence': previous})
    sequence = seed(workflow_site, 'Sequence', {'code': 'next'})
    await workflow_site.call('update', 'Sequence', sequence['id'], {'shots': [shot]})
    with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
        await workflow_site.call('find', 'EventLogEntry', [])


@pytest.mark.anyio
async def test_sequence_event_provenance_rollback_and_stale_marker(
    workflow_site: FakeSiteCase,
) -> None:
    shot = seed(workflow_site, 'Shot', {'code': 's'})
    sequence = seed(workflow_site, 'Sequence', {'code': 'q'})
    store = workflow_site.client._server.store
    row = store.require('Sequence', entity_id(sequence))
    before = await cursor(workflow_site)
    with pytest.raises(Fault, match='99999'):
        await workflow_site.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Sequence',
                    'entity_id': sequence['id'],
                    'data': {'shots': [shot]},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': 99999,
                    'data': {'description': 'missing'},
                },
            ],
        )
    row = store.require('Sequence', entity_id(sequence))
    assert '__sequence_shots_from_null' not in row
    assert row['shots'] == []
    assert await delta(workflow_site, before, ['event_type']) == []
    await workflow_site.call('update', 'Sequence', sequence['id'], {'shots': [shot]})
    assert row['__sequence_shots_from_null'] is True
    before = await cursor(workflow_site)
    await workflow_site.call('update', 'Sequence', sequence['id'], {'description': 'changed'})
    assert '__sequence_shots_from_null' not in row
    rows = await delta(workflow_site, before, ['attribute_name'])
    assert [event['attribute_name'] for event in rows] == ['description']


@pytest.mark.anyio
async def test_update_planner_independent_of_tainted_log(workflow_site: FakeSiteCase) -> None:
    scene = _workflow_scene(workflow_site)
    store = workflow_site.client._server.store
    context = RequestContext(_Actor('ApiUser', 1, 'queries'), QUERY_NOW)
    before = copy.deepcopy(store.require('Shot', entity_id(scene['Shot'])))
    after = store.update(
        'Shot',
        entity_id(scene['Shot']),
        {'sg_sequence': scene['Sequence']},
        {},
        context=context,
    )
    store.taint_events('test tainted log')
    plans = _events._plan_update(store, before, after, {'sg_sequence': 'set'}, context=context)
    assert plans is not None
    assert [plan.kind for plan in plans] == ['change', 'inverse']
    assert plans[1].forward == 0
    assert (
        _events.record_update(store, before, after, {'sg_sequence': 'set'}, context=context)
        is True
    )
    store.schema.fields.pop('EventLogEntry')
    assert (
        _events._plan_update(store, before, after, {'sg_sequence': 'set'}, context=context)
        == plans
    )


@pytest.mark.anyio
async def test_lifecycle_without_retained_instant_keeps_metadata_unknown(
    workflow_site: FakeSiteCase,
) -> None:
    shot = seed(workflow_site, 'Shot', {'code': 's'})
    store = workflow_site.client._server.store
    store.retire('Shot', entity_id(shot))
    before = await cursor(workflow_site)
    await workflow_site.call('revive', 'Shot', shot['id'])
    rows = await delta(workflow_site, before, ['attribute_name'])
    assert [row['attribute_name'] for row in rows] == ['retirement_date', None]
    with pytest.raises(mockgun.MockgunError, match='meta'):
        await workflow_site.call(
            'find_one', 'EventLogEntry', [['id', 'is', rows[0]['id']]], ['meta']
        )


@pytest.mark.anyio
@pytest.mark.parametrize('event_log', [True, False])
async def test_sequence_event_provenance_preserves_missing_target_write(
    workflow_site: FakeSiteCase, event_log: bool
) -> None:
    sequence = seed(workflow_site, 'Sequence', {'code': 'q'})
    store = workflow_site.client._server.store
    if not event_log:
        store.schema.fields.pop('EventLogEntry')
    missing = {'type': 'Shot', 'id': 99999}
    result = await workflow_site.call('update', 'Sequence', sequence['id'], {'shots': [missing]})
    assert result == {**sequence, 'shots': [missing]}
    assert await workflow_site.call(
        'find_one', 'Sequence', [['id', 'is', sequence['id']]], ['shots']
    ) == {**sequence, 'shots': [missing]}
    row = store.require('Sequence', entity_id(sequence))
    assert row.get('__sequence_shots_from_null') is not True
    if event_log:
        with pytest.raises(mockgun.MockgunError, match='EventLogEntry'):
            await workflow_site.call('find', 'EventLogEntry', [])
