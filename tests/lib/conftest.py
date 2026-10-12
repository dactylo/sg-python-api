import pickle
from pathlib import Path

import pytest

from shotgun_api3.lib import mockgun

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
