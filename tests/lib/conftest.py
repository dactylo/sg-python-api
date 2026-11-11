import copy
import pickle
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, cast

import pytest

from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun._server import FakeServer
from shotgun_api3.lib.mockgun._sessions import RequestContext
from tests._sockets import refuse_connections
from tests.lib._requests import raw_call, raw_request
from tests.lib._schemas import schema
from tests.lib._site import FakeSiteCase, SiteDefinition, site_case

__all__ = ['FakeSiteCase', 'raw_call', 'raw_request']

SCHEMA: dict[str, object] = {
    'EventLogEntry': {
        'description': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'event_type': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'metadata': {
            'data_type': {'value': 'serializable'},
            'properties': {'default_value': {'value': {'source': 'schema'}}},
        },
        'related_entities': {
            'data_type': {'value': 'multi_entity'},
            'properties': {'default_value': {'value': None}},
        },
    },
    'HumanUser': {
        'login': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'project': {
            'data_type': {'value': 'entity'},
            'properties': {
                'default_value': {'value': None},
                'valid_types': {'value': ['Project']},
            },
        },
    },
    'Project': {
        'name': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
    },
    'ScalarTypes': {
        'checkbox': {
            'data_type': {'value': 'checkbox'},
            'properties': {'default_value': {'value': None}},
        },
        'date': {
            'data_type': {'value': 'date'},
            'properties': {'default_value': {'value': None}},
        },
        'date_time': {
            'data_type': {'value': 'date_time'},
            'properties': {'default_value': {'value': None}},
        },
        'duration': {
            'data_type': {'value': 'duration'},
            'properties': {'default_value': {'value': None}},
        },
        'entity_type': {
            'data_type': {'value': 'entity_type'},
            'properties': {'default_value': {'value': None}},
        },
        'float': {
            'data_type': {'value': 'float'},
            'properties': {'default_value': {'value': None}},
        },
        'list': {
            'data_type': {'value': 'list'},
            'properties': {'default_value': {'value': None}},
        },
        'number': {
            'data_type': {'value': 'number'},
            'properties': {'default_value': {'value': None}},
        },
        'percent': {
            'data_type': {'value': 'percent'},
            'properties': {'default_value': {'value': None}},
        },
        'serializable': {
            'data_type': {'value': 'serializable'},
            'properties': {'default_value': {'value': None}},
        },
        'status_list': {
            'data_type': {'value': 'status_list'},
            'properties': {'default_value': {'value': None}},
        },
        'text': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'unsupported': {
            'data_type': {'value': 'unsupported'},
            'properties': {'default_value': {'value': None}},
        },
        'url': {
            'data_type': {'value': 'url'},
            'properties': {'default_value': {'value': None}},
        },
    },
    'Shot': {
        'code': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'project': {
            'data_type': {'value': 'entity'},
            'properties': {
                'default_value': {'value': None},
                'valid_types': {'value': ['Project']},
            },
        },
        'reviewers': {
            'data_type': {'value': 'multi_entity'},
            'properties': {
                'default_value': {'value': None},
                'valid_types': {'value': ['HumanUser']},
            },
        },
        'sg_status_list': {
            'data_type': {'value': 'status_list'},
            'properties': {'default_value': {'value': None}},
        },
    },
}

SCHEMA_ENTITY: dict[str, object] = {
    'EventLogEntry': {'name': {'value': 'Event Log Entry'}},
    'HumanUser': {'name': {'value': 'Person'}},
    'Project': {'name': {'value': 'Project'}},
    'ScalarTypes': {'name': {'value': 'Scalar Types'}},
    'Shot': {'name': {'value': 'Shot'}},
}

_QUERY_FIELDS = {
    'ApiUser': {'firstname': 'text', 'lastname': 'text'},
    'HumanUser': {'login': 'text', 'name': 'text', 'sg_status_list': 'status_list'},
    'Project': {'name': 'text', 'archived': 'checkbox'},
    'Step': {'code': 'text', 'list_order': 'number'},
    'TaskTemplate': {'code': 'text'},
    'PublishedFileType': {'code': 'text'},
    'CustomEntity01': {'code': 'text'},
    'CustomEntity03': {'code': 'text', 'cached_display_name': 'text'},
    'Department': {'code': 'text', 'name': 'text', 'cached_display_name': 'text'},
    'Group': {'code': 'text', 'cached_display_name': 'text'},
    'Ticket': {'title': 'text', 'cached_display_name': 'text'},
    'EventLogEntry': {'event_type': 'text', 'entity': 'entity', 'project': 'entity'},
    'Sequence': {'code': 'text', 'project': 'entity', 'shots': 'multi_entity'},
    'Shot': {
        'code': 'text',
        'description': 'text',
        'cached_display_name': 'text',
        'project': 'entity',
        'sg_sequence': 'entity',
        'created_by': 'entity',
        'assets': 'multi_entity',
        'sg_status_list': 'status_list',
        'sg_cut_order': 'number',
        'created_at': 'date_time',
        'image': 'image',
    },
    'Asset': {
        'code': 'text',
        'description': 'text',
        'project': 'entity',
        'sg_asset_type': 'list',
        'shots': 'multi_entity',
        'parents': 'multi_entity',
        'assets': 'multi_entity',
    },
    'Task': {
        'content': 'text',
        'sg_description': 'text',
        'project': 'entity',
        'entity': 'entity',
        'step': 'entity',
        'start_date': 'date',
    },
    'Version': {
        'code': 'text',
        'description': 'text',
        'project': 'entity',
        'entity': 'entity',
        'sg_task': 'entity',
        'created_at': 'date_time',
        'playlists': 'multi_entity',
        'published_files': 'multi_entity',
    },
    'Playlist': {'code': 'text', 'project': 'entity', 'versions': 'multi_entity'},
    'Note': {
        'subject': 'text',
        'content': 'text',
        'cached_display_name': 'text',
        'project': 'entity',
        'note_links': 'multi_entity',
    },
    'Reply': {'content': 'text', 'entity': 'entity'},
    'PublishedFile': {
        'code': 'text',
        'name': 'text',
        'project': 'entity',
        'entity': 'entity',
        'task': 'entity',
        'version': 'entity',
        'published_file_type': 'entity',
        'path': 'url',
        'version_number': 'number',
        'created_at': 'date_time',
    },
    'Cut': {
        'code': 'text',
        'project': 'entity',
        'entity': 'entity',
        'revision_number': 'number',
        'created_at': 'date_time',
        'cut_items': 'multi_entity',
    },
    'CutItem': {
        'code': 'text',
        'project': 'entity',
        'cut': 'entity',
        'shot': 'entity',
        'version': 'entity',
    },
    'Scalar': {
        'sg_number': 'number',
        'sg_float': 'float',
        'sg_date': 'date',
        'sg_date_time': 'date_time',
        'sg_duration': 'duration',
        'sg_percent': 'percent',
        'sg_checkbox': 'checkbox',
        'sg_text': 'text',
        'sg_list': 'list',
        'sg_status': 'status_list',
        'sg_review': 'status_list',
        'sg_url': 'url',
        'sg_currency': 'currency',
        'sg_timecode': 'timecode',
        'image': 'image',
        'sg_serializable': 'serializable',
        'project': 'entity',
        'sg_link': 'entity',
        'sg_links': 'multi_entity',
    },
}
_QUERY_VALID_TYPES = {
    ('Sequence', 'shots'): ['Shot'],
    ('Shot', 'assets'): ['Asset'],
    ('Asset', 'shots'): ['Shot'],
    ('Asset', 'parents'): ['Asset'],
    ('Asset', 'assets'): ['Asset'],
    ('Version', 'playlists'): ['Playlist'],
    ('Version', 'published_files'): ['PublishedFile'],
    ('Playlist', 'versions'): ['Version'],
    ('Note', 'note_links'): ['Shot', 'Asset'],
    ('Cut', 'cut_items'): ['CutItem'],
    ('Scalar', 'sg_links'): ['Shot', 'Asset'],
}
_QUERY_VALID_VALUES = {
    ('Shot', 'sg_status_list'): ['wtg', 'ip', 'fin'],
    ('Asset', 'sg_asset_type'): ['Prop', 'Character', 'Environment'],
    ('Scalar', 'sg_list'): ['Prop', 'Character', 'Environment'],
    ('Scalar', 'sg_status'): ['wtg', 'ip', 'fin'],
    ('Scalar', 'sg_review'): ['hld', 'na'],
}
_QUERY_DISPLAY_VALUES = {('Scalar', 'sg_review'): {'hld': 'On Hold', 'na': 'N/A'}}
QUERY_SCHEMA = schema(
    _QUERY_FIELDS,
    defaults={('Project', 'archived'): False, ('Scalar', 'sg_checkbox'): False},
    valid_types=_QUERY_VALID_TYPES,
    valid_values=_QUERY_VALID_VALUES,
    display_values=_QUERY_DISPLAY_VALUES,
)
QUERY_SCHEMA_ENTITY = {
    entity_type: {'name': {'value': entity_type}} for entity_type in QUERY_SCHEMA
}
QUERY_NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)

_EFFECTS_AUDIT_FIELDS = {
    'created_at': 'date_time',
    'created_by': 'entity',
    'updated_at': 'date_time',
    'updated_by': 'entity',
}
_EFFECTS_AUDIT_TYPES = {
    'Shot',
    'Asset',
    'Sequence',
    'Task',
    'Version',
    'Note',
    'Playlist',
    'PublishedFile',
    'Cut',
    'CutItem',
    'TimeLog',
}
_EFFECTS_FIELDS = {
    'EventLogEntry': {
        'description': 'text',
        'attribute_name': 'text',
        'session_uuid': 'text',
        'cached_display_name': 'text',
        'image_blur_hash': 'text',
        'meta': 'serializable',
        'audit_trail': 'serializable',
        'user': 'entity',
        'image_source_entity': 'entity',
        'created_at': 'date_time',
        'image': 'image',
        'filmstrip_image': 'image',
    },
    'Shot': {'tasks': 'multi_entity', 'notes': 'multi_entity', 'open_notes_count': 'summary'},
    'Sequence': {'description': 'text'},
    'Asset': {'sg_status_list': 'status_list'},
    'Version': {
        'user': 'entity',
        'description': 'text',
        'sg_first_frame': 'number',
        'sg_movie_aspect_ratio': 'float',
        'media_center_import_time': 'date_time',
        'sg_movie_has_slate': 'checkbox',
        'sg_status_list': 'status_list',
    },
    'Task': {
        'color': 'color',
        'due_date': 'date',
        'duration': 'duration',
        'sg_versions': 'multi_entity',
    },
    'Note': {
        'user': 'entity',
        'reply_content': 'text',
        'tasks': 'multi_entity',
        'note_links': 'multi_entity',
    },
    'Reply': {'user': 'entity'},
    'AssetShotConnection': {'asset': 'entity', 'shot': 'entity', 'created_at': 'date_time'},
    'AssetAssetConnection': {'asset': 'entity', 'parent': 'entity'},
    'PlaylistVersionConnection': {'playlist': 'entity', 'version': 'entity'},
    'AssetSequenceConnection': {'asset': 'entity', 'sequence': 'entity'},
    'TimeLog': {'entity': 'entity', 'project': 'entity', 'duration': 'duration'},
}
_EFFECTS_VALID_TYPES = {
    ('Shot', 'tasks'): ['Task'],
    ('Shot', 'notes'): ['Note'],
    ('Task', 'sg_versions'): ['Version'],
    ('Note', 'tasks'): ['Task'],
    ('Note', 'note_links'): ['Shot', 'Asset', 'Sequence', 'Version'],
    ('TimeLog', 'entity'): ['Shot', 'Task'],
}
_EFFECTS_VALID_VALUES = {
    ('Asset', 'sg_status_list'): ['wtg', 'ip', 'fin'],
    ('Version', 'sg_status_list'): ['rev', 'apr'],
}
_EFFECTS_DEFAULTS = {
    ('Asset', 'sg_status_list'): 'wtg',
    ('Version', 'sg_status_list'): 'rev',
}
EFFECTS_SCHEMA = {
    entity_type: {
        **copy.deepcopy(QUERY_SCHEMA.get(entity_type, {})),
        **schema(
            {
                entity_type: {
                    field: data_type
                    for field, data_type in {
                        **(_EFFECTS_AUDIT_FIELDS if entity_type in _EFFECTS_AUDIT_TYPES else {}),
                        **_EFFECTS_FIELDS.get(entity_type, {}),
                    }.items()
                    if field not in QUERY_SCHEMA.get(entity_type, {})
                    or (entity_type, field) == ('Note', 'note_links')
                }
            },
            defaults=_EFFECTS_DEFAULTS,
            valid_types=_EFFECTS_VALID_TYPES,
            valid_values=_EFFECTS_VALID_VALUES,
        )[entity_type],
    }
    for entity_type in dict.fromkeys((*QUERY_SCHEMA, *_EFFECTS_FIELDS))
}
EFFECTS_SCHEMA_ENTITY = {
    entity_type: {'name': {'value': entity_type}} for entity_type in EFFECTS_SCHEMA
}


_WORKFLOW_AUDIT_TYPES = {'Attachment', 'Reply', 'WorkDayRule'}
_WORKFLOW_DEFAULTS = {
    ('Cut', 'sg_status_list'): 'act',
    ('Note', 'sg_status_list'): 'opn',
    ('Sequence', 'sg_status_list'): 'ip',
    ('Task', 'sg_status_list'): 'wtg',
    ('WorkDayRule', 'working'): False,
}
_WORKFLOW_FIELDS = {
    'Attachment': {
        'filename': 'text',
        'original_fname': 'text',
        'display_name': 'text',
        'description': 'text',
        'this_file': 'url',
        'attachment_links': 'multi_entity',
        'project': 'entity',
        'cached_display_name': 'text',
    },
    'Note': {
        'attachments': 'multi_entity',
        'replies': 'multi_entity',
        'sg_status_list': 'status_list',
    },
    'Playlist': {'description': 'text'},
    'Reply': {},
    'Version': {
        'sg_uploaded_movie': 'url',
        'sg_uploaded_movie_transcoding_status': 'number',
        'image': 'image',
        'filmstrip_image': 'image',
    },
    'Shot': {'filmstrip_image': 'image', 'sg_cut_in': 'number', 'sg_shot_type': 'list'},
    'Asset': {'image': 'image', 'filmstrip_image': 'image'},
    'Task': {'sg_status_list': 'status_list', 'est_in_mins': 'duration'},
    'Sequence': {'sg_status_list': 'status_list'},
    'Cut': {'sg_status_list': 'status_list'},
    'HumanUser': {'image': 'image'},
    'Project': {'last_accessed_by_current_user': 'date_time'},
    'WorkDayRule': {
        'start_date': 'date',
        'end_date': 'date',
        'working': 'checkbox',
        'description': 'text',
        'project': 'entity',
        'user': 'entity',
        'precedence': 'number',
        'day_of_week': 'number',
    },
}
_WORKFLOW_VALID_TYPES = {
    ('Attachment', 'attachment_links'): ['Note', 'Reply', 'Version', 'Shot', 'Asset', 'Task'],
    ('Attachment', 'project'): ['Project'],
    ('Note', 'attachments'): ['Attachment'],
    ('Note', 'replies'): ['Reply'],
    ('WorkDayRule', 'project'): ['Project'],
    ('WorkDayRule', 'user'): ['HumanUser'],
}
_WORKFLOW_VALID_VALUES = {
    ('Cut', 'sg_status_list'): ['act', 'na'],
    ('Note', 'sg_status_list'): ['opn', 'clsd'],
    ('Sequence', 'sg_status_list'): ['ip', 'fin'],
    ('Shot', 'sg_shot_type'): ['Feature', 'Commercial'],
    ('Task', 'sg_status_list'): ['wtg', 'ip', 'fin'],
}

WORKFLOW_SCHEMA = {
    entity_type: {
        **copy.deepcopy(EFFECTS_SCHEMA.get(entity_type, {})),
        **schema(
            {
                entity_type: {
                    **(_EFFECTS_AUDIT_FIELDS if entity_type in _WORKFLOW_AUDIT_TYPES else {}),
                    **_WORKFLOW_FIELDS.get(entity_type, {}),
                }
            },
            defaults=_WORKFLOW_DEFAULTS,
            valid_types=_WORKFLOW_VALID_TYPES,
            valid_values=_WORKFLOW_VALID_VALUES,
        )[entity_type],
    }
    for entity_type in dict.fromkeys((*EFFECTS_SCHEMA, *_WORKFLOW_FIELDS))
}
WORKFLOW_SCHEMA_ENTITY = {
    entity_type: {'name': {'value': entity_type}} for entity_type in WORKFLOW_SCHEMA
}


@pytest.fixture
def mockgun_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> mockgun.Shotgun:
    schema_path = tmp_path / 'schema.pickle'
    schema_entity_path = tmp_path / 'schema_entity.pickle'
    schema_path.write_bytes(pickle.dumps(SCHEMA, protocol=2))
    schema_entity_path.write_bytes(pickle.dumps(SCHEMA_ENTITY, protocol=2))
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', None)
    monkeypatch.setattr(mockgun.Shotgun, '_schema_entity_path', None)
    mockgun.Shotgun.set_schema_paths(str(schema_path), str(schema_entity_path))
    return mockgun.Shotgun('https://mockgun.example', script_name='fixture', api_key='secret')


@pytest.fixture
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail() -> NoReturn:
        pytest.fail('network connection attempted')

    refuse_connections(fail, monkeypatch.setattr)


@pytest.fixture(params=['sync', 'async'])
async def fake_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    case = site_case(
        request,
        tmp_path,
        monkeypatch,
        SiteDefinition(SCHEMA, SCHEMA_ENTITY, 'site', positional_credentials=True),
    )
    yield case
    await case.call('close')


@pytest.fixture(params=['sync', 'async'])
async def query_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    case = site_case(
        request,
        tmp_path,
        monkeypatch,
        SiteDefinition(QUERY_SCHEMA, QUERY_SCHEMA_ENTITY, 'query', QUERY_NOW),
    )
    yield case
    await case.call('close')


@pytest.fixture(params=['sync', 'async'])
async def effects_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    case = site_case(
        request,
        tmp_path,
        monkeypatch,
        SiteDefinition(EFFECTS_SCHEMA, EFFECTS_SCHEMA_ENTITY, 'effects', QUERY_NOW),
    )
    yield case
    await case.call('close')


def raw_read(
    server: FakeServer,
    entity_type: str,
    conditions: list[tuple[str, str, list[object]]],
    **params: object,
) -> dict[str, object]:
    auth = cast('dict[str, object] | None', params.pop('auth', None))
    return raw_call(
        server,
        'read',
        {
            'type': entity_type,
            'filters': {
                'logical_operator': 'and',
                'conditions': [
                    {'path': path, 'relation': relation, 'values': values}
                    for path, relation, values in conditions
                ],
            },
            'return_fields': ['id'],
            'paging': {'entities_per_page': 500, 'current_page': 1},
            **params,
        },
        auth=auth,
    )


def seed(case: FakeSiteCase, entity_type: str, data: dict[str, object]) -> dict[str, object]:
    row = case.client._server.store.create(
        entity_type, data, context=RequestContext(None, case.client.site.now())
    )
    return {'type': entity_type, 'id': row['id']}


@pytest.fixture(params=['sync', 'async'])
async def workflow_site(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[FakeSiteCase]:
    case = site_case(
        request,
        tmp_path,
        monkeypatch,
        SiteDefinition(WORKFLOW_SCHEMA, WORKFLOW_SCHEMA_ENTITY, 'workflow', QUERY_NOW),
    )
    yield case
    await case.call('close')


def write_upload(directory: Path, name: str, data: bytes) -> str:
    path = directory / name
    path.write_bytes(data)
    return str(path)
