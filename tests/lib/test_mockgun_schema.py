import copy
import importlib
import inspect
import pickle
import socket
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from shotgun_api3 import Shotgun as ClientShotgun
from shotgun_api3.lib import mockgun

if TYPE_CHECKING:
    from collections.abc import Callable

_CREDENTIAL = 'test-credential'


def _client_state(
    client: mockgun.Shotgun,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[int, dict[str, Any]]]]:
    return client._schema, client._schema_entity, client._db


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
        login='person',
        password=_CREDENTIAL,
        sudo_as_login='delegate',
        session_token=_CREDENTIAL,
        auth_token=_CREDENTIAL,
    )

    assert mockgun_client.base_url == 'https://mockgun.example'
    assert client.base_url == 'HTTPS://Mockgun.Example/path/'
    assert client.config.scheme == 'https'
    assert client.config.records_per_page == 500


def test_constructor_has_exact_compatibility_parameter_shape() -> None:
    parameters = inspect.signature(mockgun.Shotgun).parameters
    method_parameters = inspect.signature(mockgun.Shotgun.__init__).parameters
    expected = (
        ('base_url', inspect.Parameter.empty),
        ('script_name', None),
        ('api_key', None),
        ('convert_datetimes_to_utc', True),
        ('http_proxy', None),
        ('connect', True),
        ('ca_certs', None),
        ('login', None),
        ('password', None),
        ('sudo_as_login', None),
        ('session_token', None),
        ('auth_token', None),
    )

    assert (
        tuple((parameter.name, parameter.default) for parameter in parameters.values()) == expected
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in parameters.values()
    )
    assert tuple(method_parameters) == ('self', *(name for name, _default in expected))
    assert (
        tuple(
            (parameter.name, parameter.default)
            for parameter in tuple(method_parameters.values())[1:]
        )
        == expected
    )
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for parameter in tuple(method_parameters.values())[1:]
    )


@pytest.mark.usefixtures('mockgun_client')
@pytest.mark.parametrize(
    ('base_url', 'expected_scheme', 'expected_server', 'expected_api_path'),
    [
        ('http://mockgun.example', 'http', 'mockgun.example', '/api3/json'),
        ('https://mockgun.example', 'https', 'mockgun.example', '/api3/json'),
        (
            'HTTPS://Mockgun.Example/path/',
            'https',
            'Mockgun.Example',
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
    client = mockgun.Shotgun(base_url)

    assert client.base_url == base_url
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

    client = mockgun.Shotgun('https://existing.example')
    loaded_schema, loaded_schema_entity, _database = _client_state(client)

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


def test_constructor_seeds_tables_and_initial_event_log(
    mockgun_client: mockgun.Shotgun,
) -> None:
    database = _client_state(mockgun_client)[2]

    assert mockgun_client.finds == 0
    assert set(database) == {'EventLogEntry', 'HumanUser', 'Project', 'ScalarTypes', 'Shot'}
    assert database['HumanUser'] == {}
    assert database['Project'] == {}
    assert database['ScalarTypes'] == {}
    assert database['Shot'] == {}
    assert database['EventLogEntry'] == {
        1: {
            '__retired': False,
            'description': 'Mockgun was born. Yay.',
            'event_type': 'Hello_Mockgun_World',
            'id': 1,
            'metadata': {'source': 'schema'},
            'related_entities': [],
            'type': 'EventLogEntry',
        }
    }


@pytest.mark.parametrize(
    ('args', 'kwargs', 'message'),
    [
        ((), {}, "Mockgun Shotgun.__init__() missing a required argument: 'base_url'"),
        (
            ('https://x.example',) * 13,
            {},
            'Mockgun Shotgun.__init__() too many positional arguments',
        ),
        (
            ('https://x.example',),
            {'unexpected': True},
            "Mockgun Shotgun.__init__() got an unexpected keyword argument 'unexpected'",
        ),
        (
            ('https://x.example', 's'),
            {'script_name': 'other'},
            "Mockgun Shotgun.__init__() multiple values for argument 'script_name'",
        ),
    ],
)
def test_constructor_type_errors_identify_mockgun_shotgun(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    constructor = cast('Callable[..., mockgun.Shotgun]', mockgun.Shotgun)
    with pytest.raises(TypeError) as caught:
        constructor(*args, **kwargs)

    assert str(caught.value) == message


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
    second = mockgun.Shotgun('https://second.example')
    first_schema, first_schema_entity, first_db = _client_state(mockgun_client)
    second_schema, second_schema_entity, second_db = _client_state(second)

    assert second_schema is not first_schema
    assert second_schema_entity is not first_schema_entity
    assert second_db is not first_db
    assert all(second_db[name] is not first_db[name] for name in second_db)
    first_event = first_db['EventLogEntry'][1]
    first_metadata_default = first_schema['EventLogEntry']['metadata']['properties'][
        'default_value'
    ]['value']
    second_event = second_db['EventLogEntry'][1]
    assert (
        first_event['metadata']
        == first_metadata_default
        == second_event['metadata']
        == {'source': 'schema'}
    )
    assert first_event['metadata'] is not first_metadata_default
    assert first_event['metadata'] is not second_event['metadata']
    assert first_event['related_entities'] == second_event['related_entities'] == []
    assert first_event['related_entities'] is not second_event['related_entities']

    first_schema['OnlyFirst'] = {}
    first_schema_entity['OnlyFirst'] = {}
    first_db['Shot'][99] = {'type': 'Shot', 'id': 99}
    first_event['metadata']['source'] = 'first-only'
    first_event['related_entities'].append({'type': 'Shot', 'id': 99})
    mockgun_client.config.records_per_page = 25

    assert 'OnlyFirst' not in second_schema
    assert 'OnlyFirst' not in second_schema_entity
    assert second_db['Shot'] == {}
    assert first_metadata_default == {'source': 'schema'}
    assert second_event['metadata'] == {'source': 'schema'}
    assert second_event['related_entities'] == []
    assert mockgun_client.config.scheme == 'https'
    assert second.config.scheme == 'https'
    assert mockgun_client.config.records_per_page == 25
    assert second.config.records_per_page == 500


def test_public_exports_include_the_approved_async_extension() -> None:
    assert mockgun.__all__ == ['AsyncShotgun', 'MockgunError', 'Shotgun', 'generate_schema']

    async_implementation = importlib.import_module('shotgun_api3.lib.mockgun._async')
    implementation = importlib.import_module('shotgun_api3.lib.mockgun._mockgun')

    assert mockgun.AsyncShotgun is async_implementation.AsyncShotgun
    assert mockgun.MockgunError is implementation.MockgunError
    assert mockgun.Shotgun is implementation.Shotgun
    assert mockgun.generate_schema is implementation.generate_schema
    assert mockgun.MockgunError.__bases__ == (Exception,)


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


def test_schema_reads_preserve_source_aliases_shapes_and_errors(
    mockgun_client: mockgun.Shotgun,
) -> None:
    project = {'type': 'Project', 'id': 1, 'nested': {'value': True}}
    original_project = copy.deepcopy(project)

    schema = mockgun_client.schema_read(project)
    entities = mockgun_client.schema_entity_read(project)
    shot_fields = mockgun_client.schema_field_read('Shot', project_entity=project)
    code_field = mockgun_client.schema_field_read('Shot', 'code', project)

    assert schema is mockgun_client._schema
    assert entities is mockgun_client._schema_entity
    assert shot_fields is schema['Shot']
    assert code_field == {'code': shot_fields['code']}
    assert code_field['code'] is shot_fields['code']
    assert mockgun_client.schema_field_read('Shot', 'missing') == {}
    with pytest.raises(KeyError, match='MissingEntity'):
        mockgun_client.schema_field_read('MissingEntity')
    assert project == original_project
