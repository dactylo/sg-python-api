import copy
import inspect
import pickle
import socket
from pathlib import Path
from typing import cast

import pytest

from shotgun_api3 import Fault
from shotgun_api3 import Shotgun as ClientShotgun
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun._errors import ServerFaultError
from shotgun_api3.lib.mockgun._schema import SiteSchema

from .conftest import SCHEMA


def test_constructor_accepts_compatibility_arguments_without_network_io(
    mockgun_client: mockgun.Shotgun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError('mockgun attempted network I/O')

    monkeypatch.setattr(socket, 'create_connection', fail_network)

    client = mockgun.Shotgun(
        'HTTPS://Mockgun.Example/path/',
        script_name='script',
        api_key='key',
        convert_datetimes_to_utc=False,
        http_proxy='proxy.example:8080',
        connect=True,
        ca_certs='/not/read/ca.pem',
        sudo_as_login='delegate',
    )

    assert mockgun_client.base_url == 'https://mockgun.example'
    assert client.base_url == 'HTTPS://Mockgun.Example/path'
    assert client.config.scheme == 'https'
    assert client.config.records_per_page == 500


@pytest.mark.usefixtures('mockgun_client')
@pytest.mark.parametrize(
    ('base_url', 'expected_scheme', 'expected_server', 'expected_api_path'),
    [
        ('http://mockgun.example', 'http', 'mockgun.example', '/api3/json'),
        ('https://mockgun.example', 'https', 'mockgun.example', '/api3/json'),
        (
            'HTTPS://Mockgun.Example/path/',
            'https',
            'mockgun.example',
            '/path/api3/json',
        ),
    ],
)
def test_constructor_publishes_parsed_server_fields(
    base_url: str,
    expected_scheme: str,
    expected_server: str,
    expected_api_path: str,
) -> None:
    client = mockgun.Shotgun(base_url, script_name='fixture', api_key='secret')

    assert client.base_url == base_url.rstrip('/')
    assert client.config.scheme == expected_scheme
    assert client.config.server == expected_server
    assert client.config.api_path == expected_api_path


@pytest.mark.parametrize('protocol', [2, pickle.HIGHEST_PROTOCOL])
def test_constructor_reads_existing_pickle_protocols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protocol: int,
) -> None:
    schema: dict[str, object] = {
        'EventLogEntry': {
            'description': {
                'data_type': {'value': 'text'},
                'properties': {'default_value': {'value': None}},
            },
            'event_type': {
                'data_type': {'value': 'text'},
                'properties': {'default_value': {'value': None}},
            },
        }
    }
    schema_entity: dict[str, object] = {'EventLogEntry': {'name': {'value': 'Event Log Entry'}}}
    schema_path = tmp_path / 'schema.pickle'
    schema_entity_path = tmp_path / 'schema_entity.pickle'
    schema_path.write_bytes(pickle.dumps(schema, protocol=protocol))
    schema_entity_path.write_bytes(pickle.dumps(schema_entity, protocol=protocol))
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', None)
    monkeypatch.setattr(mockgun.Shotgun, '_schema_entity_path', None)
    mockgun.Shotgun.set_schema_paths(str(schema_path), str(schema_entity_path))

    client = mockgun.Shotgun('https://existing.example', script_name='fixture', api_key='secret')
    loaded_schema = client.schema_read()
    loaded_schema_entity = client.schema_entity_read()

    assert loaded_schema == schema
    assert loaded_schema_entity == schema_entity


@pytest.mark.parametrize('missing_file', ['schema', 'schema_entity'])
def test_constructor_reports_each_missing_schema_file_with_mockgun_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_file: str,
) -> None:
    schema_path = tmp_path / 'schema.pickle'
    schema_entity_path = tmp_path / 'schema_entity.pickle'
    if missing_file != 'schema':
        schema_path.write_bytes(pickle.dumps({}))
    if missing_file != 'schema_entity':
        schema_entity_path.write_bytes(pickle.dumps({}))
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', None)
    monkeypatch.setattr(mockgun.Shotgun, '_schema_entity_path', None)
    mockgun.Shotgun.set_schema_paths(str(schema_path), str(schema_entity_path))

    with pytest.raises(mockgun.MockgunError, match=missing_file) as caught:
        mockgun.Shotgun('https://missing.example')

    assert type(caught.value) is mockgun.MockgunError


def test_constructor_reports_unset_schema_paths_with_mockgun_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mockgun.Shotgun, '_schema_path', None)
    monkeypatch.setattr(mockgun.Shotgun, '_schema_entity_path', None)

    with pytest.raises(mockgun.MockgunError, match=r'schema.*set_schema_paths') as caught:
        mockgun.Shotgun('https://unset.example')

    assert type(caught.value) is mockgun.MockgunError


def test_constructor_seeds_tables_and_initial_event_log(mockgun_client: mockgun.Shotgun) -> None:
    assert mockgun_client.finds == 0
    assert set(mockgun_client.schema_read()) == {
        'EventLogEntry',
        'HumanUser',
        'Project',
        'ScalarTypes',
        'Shot',
    }
    for entity_type in ('HumanUser', 'Project', 'ScalarTypes', 'Shot'):
        assert mockgun_client.find(entity_type, []) == []
    fields = list(mockgun_client.schema_field_read('EventLogEntry'))
    assert mockgun_client.find('EventLogEntry', [], fields) == [
        {
            'description': 'Mockgun was born. Yay.',
            'event_type': 'Hello_Mockgun_World',
            'id': 1,
            'metadata': {'source': 'schema'},
            'related_entities': [],
            'type': 'EventLogEntry',
        }
    ]


def test_generate_schema_writes_bare_protocol_two_pickles_without_mutation(tmp_path: Path) -> None:
    schema: dict[str, object] = {'Shot': {'code': {'data_type': {'value': 'text'}}}}
    entity_schema: dict[str, object] = {'Shot': {'name': {'value': 'Shot'}}}
    original_schema = copy.deepcopy(schema)
    original_entity_schema = copy.deepcopy(entity_schema)
    calls: list[str] = []

    class SchemaClient:
        def schema_entity_read(self) -> dict[str, object]:
            calls.append('schema_entity_read')
            return entity_schema

        def schema_read(self) -> dict[str, object]:
            calls.append('schema_read')
            return schema

    schema_path = tmp_path / 'schema.pickle'
    entity_schema_path = tmp_path / 'schema_entity.pickle'

    result = mockgun.generate_schema(
        SchemaClient(),
        str(schema_path),
        str(entity_schema_path),
    )

    assert result is None
    assert calls == ['schema_read', 'schema_entity_read']
    assert schema_path.read_bytes() == pickle.dumps(schema, protocol=2)
    assert entity_schema_path.read_bytes() == pickle.dumps(entity_schema, protocol=2)
    assert schema == original_schema
    assert entity_schema == original_entity_schema


def test_loaded_schema_and_database_are_independent_per_instance(
    mockgun_client: mockgun.Shotgun,
) -> None:
    second = mockgun.Shotgun('https://second.example', script_name='fixture', api_key='secret')
    first_schema = mockgun_client.schema_read()
    second_schema = second.schema_read()
    assert first_schema == second_schema
    assert first_schema is not second_schema
    first_schema['OnlyFirst'] = {}
    mockgun_client.update(
        'EventLogEntry',
        1,
        {
            'metadata': {'source': 'first-only'},
        },
    )
    mockgun_client.create(
        'Shot',
        {'code': 'first-only', 'reviewers': [{'type': 'HumanUser', 'id': 1}]},
    )
    mockgun_client.site.update_server_metadata({'api_max_entities_per_page': 25})
    mockgun_client.info()

    assert 'OnlyFirst' not in second.schema_read()
    assert second.find('Shot', []) == []
    assert second.find_one('EventLogEntry', [], ['metadata', 'related_entities']) == {
        'type': 'EventLogEntry',
        'id': 1,
        'metadata': {'source': 'schema'},
        'related_entities': [],
    }
    assert mockgun_client.config.records_per_page == 25
    assert second.config.records_per_page == 500


def test_mockgun_error_has_exception_base() -> None:
    assert mockgun.MockgunError.__bases__ == (Exception,)


def test_schema_field_types_resolve_links_and_identity() -> None:
    schema = SiteSchema(cast('dict[str, dict[str, object]]', SCHEMA), {})
    assert schema.declares('Project')
    assert schema.declares('Shot', 'project')
    assert not schema.declares('Missing')
    assert not schema.declares('Shot', 'missing')
    assert schema.field_type('Shot', 'project.Project.name') == 'text'
    assert schema.field_type('Shot', 'project.Project.id') == 'number'
    assert schema.field_type('Shot', 'type') == 'entity_type'
    with pytest.raises(ServerFaultError, match='code is not a valid linked field for entity Shot'):
        schema.field_type('Shot', 'code.Project.name')
    with pytest.raises(ServerFaultError, match='Missing is not a valid entity'):
        schema.field_type('Missing', 'id')
    with pytest.raises(ServerFaultError, match='missing is not a valid field for entity Shot'):
        schema.field_type('Shot', 'missing')


def test_schema_path_methods_are_bound_classmethods_with_exact_signatures() -> None:
    set_descriptor = inspect.getattr_static(mockgun.Shotgun, 'set_schema_paths')
    get_descriptor = inspect.getattr_static(mockgun.Shotgun, 'get_schema_paths')

    assert isinstance(set_descriptor, classmethod)
    assert isinstance(get_descriptor, classmethod)
    assert mockgun.Shotgun.set_schema_paths.__self__ is mockgun.Shotgun
    assert mockgun.Shotgun.get_schema_paths.__self__ is mockgun.Shotgun
    assert tuple(inspect.signature(mockgun.Shotgun.set_schema_paths).parameters) == (
        'schema_path',
        'schema_entity_path',
    )
    assert tuple(inspect.signature(mockgun.Shotgun.get_schema_paths).parameters) == ()


def test_schema_paths_bind_to_subclasses() -> None:
    class SiteMockgun(mockgun.Shotgun):
        pass

    SiteMockgun.set_schema_paths('/site/schema', '/site/entities')

    assert SiteMockgun.set_schema_paths.__self__ is SiteMockgun
    assert SiteMockgun.get_schema_paths() == ('/site/schema', '/site/entities')
    assert mockgun.Shotgun.get_schema_paths() == (None, None)


@pytest.mark.parametrize('method_name', ['schema_entity_read', 'schema_field_read', 'schema_read'])
def test_schema_reads_have_project_compatible_signatures(
    method_name: str,
) -> None:
    assert inspect.signature(getattr(mockgun.Shotgun, method_name)) == inspect.signature(
        getattr(ClientShotgun, method_name)
    )


@pytest.mark.parametrize('project', [None, {'type': 'Project', 'id': 1}])
def test_schema_field_read_detaches_nested_metadata(
    mockgun_client: mockgun.Shotgun, project: dict[str, object] | None
) -> None:
    fields = mockgun_client.schema_field_read('Shot', project_entity=project)
    original = copy.deepcopy(fields['code'])
    metadata = cast('dict[str, object]', fields['code'])
    cast('dict[str, object]', metadata['data_type'])['value'] = 'changed'
    assert mockgun_client.schema_field_read('Shot', project_entity=project)['code'] == original
    schema = cast('dict[str, object]', mockgun_client.schema_read(project)['Shot'])
    assert schema['code'] == original


@pytest.mark.parametrize('path', ['missing.Project', 'project.Project'])
def test_partial_link_paths_report_the_original_invalid_field(path: str) -> None:
    schema = SiteSchema(cast('dict[str, dict[str, object]]', SCHEMA), {})
    with pytest.raises(ServerFaultError) as raised:
        schema.field_type('Shot', path)
    assert str(raised.value) == f'{path} is not a valid field for entity Shot'


def test_schema_reads_return_independent_copies_and_preserve_shapes(
    mockgun_client: mockgun.Shotgun,
) -> None:
    project = {'type': 'Project', 'id': 1, 'nested': {'value': True}}
    original_project = copy.deepcopy(project)

    schema = mockgun_client.schema_read(project)
    entities = mockgun_client.schema_entity_read()
    with pytest.raises(mockgun.MockgunError, match='schema_entity_read'):
        mockgun_client.schema_entity_read(project)
    shot_fields = mockgun_client.schema_field_read('Shot', project_entity=project)
    code_field = mockgun_client.schema_field_read('Shot', 'code', project)

    assert schema == mockgun_client.schema_read()
    assert entities == mockgun_client.schema_entity_read()
    assert shot_fields == schema['Shot']
    assert shot_fields is not schema['Shot']
    assert code_field == {'code': shot_fields['code']}
    assert code_field['code'] is not shot_fields['code']
    code_field['code'] = {}
    schema['Shot'] = {}
    entities['Shot'] = {}
    assert mockgun_client.schema_field_read('Shot', 'code') == {'code': shot_fields['code']}
    assert mockgun_client.schema_entity_read()['Shot'] != {}
    with pytest.raises(Fault, match=r'Shot\.missing'):
        mockgun_client.schema_field_read('Shot', 'missing')
    with pytest.raises(Fault, match='MissingEntity'):
        mockgun_client.schema_field_read('MissingEntity')
    assert project == original_project
