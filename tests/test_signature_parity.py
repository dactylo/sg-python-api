import inspect
import json
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Literal, NamedTuple, TypeAlias, cast

import pytest

from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._client import Shotgun
from shotgun_api3.lib import mockgun
from tests._constructor_cases import CONSTRUCTOR_TYPE_ERROR_CASES
from tests._pyright import run_pyright

pytestmark = pytest.mark.async_required

_DescriptorKind: TypeAlias = Literal['classmethod', 'method', 'property']

# Source: ../_github/python-api, commit
# cf5eb6a000d34f0ae0d447ff8725763d2692967a:shotgun_api3/shotgun.py.
_UPSTREAM_SHOTGUN_SIGNATURES: dict[str, str] = {
    '__init__': (
        '(self, base_url, script_name=None, api_key=None, '
        'convert_datetimes_to_utc=True, http_proxy=None, connect=True, '
        'ca_certs=None, login=None, password=None, sudo_as_login=None, '
        'session_token=None, auth_token=None)'
    ),
    'server_info': 'property',
    'server_caps': 'property',
    'connect': '(self)',
    'close': '(self)',
    'info': '(self)',
    'find_one': (
        '(self, entity_type, filters, fields=None, order=None, '
        'filter_operator=None, retired_only=False, '
        'include_archived_projects=True, additional_filter_presets=None)'
    ),
    'find': (
        '(self, entity_type, filters, fields=None, order=None, '
        'filter_operator=None, limit=0, retired_only=False, page=0, '
        'include_archived_projects=True, additional_filter_presets=None)'
    ),
    'summarize': (
        '(self, entity_type, filters, summary_fields, '
        'filter_operator=None, grouping=None, '
        'include_archived_projects=True)'
    ),
    'create': '(self, entity_type, data, return_fields=None)',
    'update': '(self, entity_type, entity_id, data, multi_entity_update_modes=None)',
    'delete': '(self, entity_type, entity_id)',
    'revive': '(self, entity_type, entity_id)',
    'batch': '(self, requests)',
    'work_schedule_read': '(self, start_date, end_date, project=None, user=None)',
    'work_schedule_update': (
        '(self, date, working, description=None, project=None, user=None, recalculate_field=None)'
    ),
    'export_page': '(self, page_id, format, layout_name=None)',
    'follow': '(self, user, entity)',
    'unfollow': '(self, user, entity)',
    'followers': '(self, entity)',
    'following': '(self, user, project=None, entity_type=None)',
    'schema_entity_read': '(self, project_entity=None)',
    'schema_read': '(self, project_entity=None)',
    'schema_field_read': '(self, entity_type, field_name=None, project_entity=None)',
    'schema_field_create': '(self, entity_type, data_type, display_name, properties=None)',
    'schema_field_update': '(self, entity_type, field_name, properties, project_entity=None)',
    'schema_field_delete': '(self, entity_type, field_name)',
    'add_user_agent': '(self, agent)',
    'reset_user_agent': '(self)',
    'set_session_uuid': '(self, session_uuid)',
    'share_thumbnail': (
        '(self, entities, thumbnail_path=None, source_entity=None, '
        'filmstrip_thumbnail=False, **kwargs)'
    ),
    'upload_thumbnail': '(self, entity_type, entity_id, path, **kwargs)',
    'upload_filmstrip_thumbnail': '(self, entity_type, entity_id, path, **kwargs)',
    'upload': (
        '(self, entity_type, entity_id, path, field_name=None, '
        'display_name=None, tag_list=None, created_at=None)'
    ),
    'download_attachment': '(self, attachment=False, file_path=None, attachment_id=None)',
    'get_auth_cookie_handler': '(self)',
    'get_attachment_download_url': '(self, attachment)',
    'authenticate_human_user': '(self, user_login, user_password, auth_token=None)',
    'update_project_last_accessed': '(self, project, user=None)',
    'note_thread_read': '(self, note_id, entity_fields=None)',
    'text_search': '(self, text, entity_types, project_ids=None, limit=None)',
    'activity_stream_read': (
        '(self, entity_type, entity_id, entity_fields=None, min_id=None, max_id=None, limit=None)'
    ),
    'nav_expand': '(self, path, seed_entity_field=None, entity_fields=None)',
    'nav_search_string': '(self, root_path, search_string, seed_entity_field=None)',
    'nav_search_entity': '(self, root_path, entity, seed_entity_field=None)',
    'get_session_token': '(self)',
    'preferences_read': '(self, prefs=None)',
    'user_subscriptions_read': '(self)',
    'user_subscriptions_create': '(self, users)',
    'schema': '(self, entity_type)',
    'entity_types': '(self)',
}
# These methods mutate only the locked client configuration and perform no I/O,
# so both clients intentionally expose them as plain methods.
_NON_COROUTINE_CLIENT_METHODS = frozenset(
    {'add_user_agent', 'reset_user_agent', 'set_session_uuid'}
)
# mockgun's new_client() only builds a client, so it is plain on async too.
_NON_COROUTINE_MOCKGUN_METHODS = _NON_COROUTINE_CLIENT_METHODS | {'new_client'}


class _PublicDescriptor(NamedTuple):
    kind: _DescriptorKind
    member: Callable[..., object]


def _public_descriptors(class_: type[object]) -> dict[str, _PublicDescriptor]:
    descriptors: dict[str, _PublicDescriptor] = {}
    for base in reversed(class_.__mro__):
        for name, member in vars(base).items():
            if name.startswith('_'):
                continue
            descriptors.pop(name, None)
            if isinstance(member, classmethod):
                raw_member = cast('object', member)
                descriptors[name] = _PublicDescriptor(
                    'classmethod',
                    cast(
                        'Callable[..., object]',
                        object.__getattribute__(raw_member, '__func__'),
                    ),
                )
            elif isinstance(member, property):
                getter = member.fget
                if getter is None:
                    raise AssertionError(f'public property has no getter: {name}')
                descriptors[name] = _PublicDescriptor('property', getter)
            elif inspect.isfunction(member):
                descriptors[name] = _PublicDescriptor(
                    'method',
                    cast('Callable[..., object]', member),
                )
    return descriptors


def _assert_inventory_matches(
    sync_class: type[object],
    async_class: type[object],
    *,
    non_coroutine_methods: frozenset[str],
) -> None:
    sync_descriptors = _public_descriptors(sync_class)
    async_descriptors = _public_descriptors(async_class)

    assert async_descriptors.keys() == sync_descriptors.keys()
    for name, sync_descriptor in sync_descriptors.items():
        async_descriptor = async_descriptors[name]
        assert async_descriptor.kind == sync_descriptor.kind, name
        assert not inspect.iscoroutinefunction(sync_descriptor.member), name
        if async_descriptor.kind == 'property' or name in non_coroutine_methods:
            assert not inspect.iscoroutinefunction(async_descriptor.member), name
        else:
            assert inspect.iscoroutinefunction(async_descriptor.member), name


@pytest.mark.parametrize('client', [Shotgun, AsyncShotgun, mockgun.Shotgun, mockgun.AsyncShotgun])
def test_argument_errors_name_the_called_method(
    client: type[Shotgun] | type[AsyncShotgun],
) -> None:
    find = cast('Callable[..., object]', client.find)
    with pytest.raises(TypeError) as error:
        find(None, filters=[], unexpected=True)

    message = str(error.value)
    assert f'{client.__name__}.find()' in message
    assert 'unexpected' in message


def test_client_public_descriptor_parity() -> None:
    _assert_inventory_matches(
        Shotgun,
        AsyncShotgun,
        non_coroutine_methods=_NON_COROUTINE_CLIENT_METHODS,
    )


@pytest.mark.parametrize(
    ('client', 'label'),
    [
        pytest.param(Shotgun, 'Shotgun', id='Shotgun'),
        pytest.param(AsyncShotgun, 'AsyncShotgun', id='AsyncShotgun'),
        pytest.param(mockgun.Shotgun, 'Shotgun', id='Mockgun Shotgun'),
        pytest.param(mockgun.AsyncShotgun, 'AsyncShotgun', id='Mockgun AsyncShotgun'),
    ],
)
@pytest.mark.parametrize(('args', 'kwargs', 'message'), CONSTRUCTOR_TYPE_ERROR_CASES)
def test_constructor_type_errors_identify_client(
    client: type[Shotgun] | type[AsyncShotgun],
    label: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    constructor = cast('Callable[..., object]', client)
    with pytest.raises(TypeError) as caught:
        constructor(*args, **kwargs)

    assert f'{label}.__init__()' in str(caught.value)
    assert message in str(caught.value)


def test_mockgun_public_descriptor_parity() -> None:
    _assert_inventory_matches(
        mockgun.Shotgun,
        mockgun.AsyncShotgun,
        non_coroutine_methods=_NON_COROUTINE_MOCKGUN_METHODS,
    )


def test_result_annotations_preserve_only_validated_topology() -> None:
    lines = [
        'from typing import assert_type',
        'from shotgun_api3 import Shotgun as RealSync, AsyncShotgun as RealAsync',
        'from shotgun_api3.lib.mockgun import Shotgun as MockSync, AsyncShotgun as MockAsync',
    ]
    negative_lines: set[int] = set()
    for mock, asynchronous, client in (
        (False, False, 'RealSync'),
        (False, True, 'RealAsync'),
        (True, False, 'MockSync'),
        (True, True, 'MockAsync'),
    ):
        await_prefix = 'await ' if asynchronous else ''
        definition = 'async def' if asynchronous else 'def'
        contracts = {
            'batch([])': 'list[object]',
            "create('Shot', {})": 'dict[str, object]',
            "find('Shot', [])": 'list[dict[str, object]]',
            "find_one('Shot', [])": 'dict[str, object] | None',
            "update('Shot', 1, {})": 'dict[str, object]',
            'schema_entity_read()': 'dict[str, object]',
            "schema_field_read('Shot')": 'dict[str, object]',
            'schema_read()': 'dict[str, object]',
            "delete('Shot', 1)": 'bool',
            "revive('Shot', 1)": 'bool',
        }
        if not mock:
            contracts.update(
                {
                    "activity_stream_read('Shot', 1)": 'dict[str, object]',
                    "authenticate_human_user('user', 'password')": 'dict[str, object] | None',
                    'follow({}, {})': 'dict[str, object]',
                    'followers({})': 'list[dict[str, object]]',
                    'following({})': 'list[dict[str, object]]',
                    "nav_expand('/')": 'dict[str, object]',
                    "nav_search_entity('/', {})": 'list[dict[str, object]]',
                    "nav_search_string('/', 'shot')": 'list[dict[str, object]]',
                    'note_thread_read(1)': 'list[dict[str, object]]',
                    'preferences_read()': 'dict[str, object]',
                    "summarize('Shot', [], [])": 'dict[str, object]',
                    "text_search('shot', {'Shot': []})": 'dict[str, object]',
                    'unfollow({}, {})': 'dict[str, object]',
                    'user_subscriptions_read()': 'list[dict[str, object]]',
                    "work_schedule_read('2026-07-01', '2026-07-31')": 'dict[str, object]',
                    "work_schedule_update('2026-07-04', False)": 'dict[str, object]',
                    'info()': 'dict[str, object]',
                    "schema_field_create('Shot', 'text', 'Custom')": 'str',
                }
            )
        lines.append(f'{definition} check_{client}(client: {client}) -> None:')
        lines.extend(
            f'    assert_type({await_prefix}client.{call}, {expected})'
            for call, expected in contracts.items()
        )
        lines.append(f"    assert_type(({await_prefix}client.create('Shot', {{}}))['id'], object)")
        negative_lines.add(len(lines))
        lines.append(f"    ({await_prefix}client.create('Shot', {{}}))['id'].nonexistent()")
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
        probe = Path(directory) / 'result_types.py'
        probe.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        result = run_pyright(probe, cwd=Path(__file__).parents[1])

    report = json.loads(result.stdout)
    diagnostics = report['generalDiagnostics']
    assert result.returncode == 1, result.stdout
    assert {item['rule'] for item in diagnostics} == {
        'reportAttributeAccessIssue',
        'reportUnknownMemberType',
    }, result.stdout
    observed_lines = {item['range']['start']['line'] for item in diagnostics}
    assert observed_lines == negative_lines, result.stdout
    assert all('nonexistent' in item['message'] for item in diagnostics), result.stdout
    assert len(diagnostics) == 2 * len(negative_lines), result.stdout


def test_shotgun_signatures_match_shotgun_api3() -> None:
    for name, upstream in _UPSTREAM_SHOTGUN_SIGNATURES.items():
        member = inspect.getattr_static(Shotgun, name)
        if upstream == 'property':
            assert isinstance(member, property), name
            continue
        assert not isinstance(member, property), name
        signature = inspect.signature(member)
        parameters = tuple(
            parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in signature.parameters.values()
        )
        actual = str(
            signature.replace(parameters=parameters, return_annotation=inspect.Signature.empty)
        )
        assert actual == upstream, name


def test_signatures_match_across_runtimes() -> None:
    classes = (Shotgun, AsyncShotgun, mockgun.Shotgun, mockgun.AsyncShotgun)
    common_methods = {name for name in dir(Shotgun) if not name.startswith('_')}
    for cls in classes[1:]:
        assert common_methods <= set(dir(cls))
    for name in (*sorted(common_methods), '__init__'):
        members = [getattr(cls, name) for cls in classes]
        if all(callable(member) for member in members):
            assert all(
                inspect.signature(member) == inspect.signature(members[0])
                for member in members[1:]
            ), name
    mockgun_only = {
        name for name in dir(mockgun.Shotgun) if not name.startswith('_')
    } - common_methods
    assert mockgun_only <= set(dir(mockgun.AsyncShotgun))
    for name in sorted(mockgun_only):
        sync_member = getattr(mockgun.Shotgun, name)
        async_member = getattr(mockgun.AsyncShotgun, name)
        if callable(sync_member):
            assert callable(async_member), name
            assert inspect.signature(async_member) == inspect.signature(sync_member), name


def test_single_any_argument_call_is_rejected_by_type_checker(tmp_path: Path) -> None:
    probe = tmp_path / 'single_any_find.py'
    probe.write_text(
        """from typing import Any

from shotgun_api3 import AsyncShotgun, Shotgun

def probe(sync: Shotgun, async_: AsyncShotgun, value: Any) -> None:
    sync.find(value)
    async_.find(value)
""",
        encoding='utf-8',
    )

    result = run_pyright(probe, cwd=Path(__file__).parents[1])

    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    errors = [
        diagnostic
        for diagnostic in report['generalDiagnostics']
        if diagnostic['severity'] == 'error'
    ]
    assert len(errors) == 2, errors
    assert all(error['rule'] == 'reportCallIssue' for error in errors)
    assert all('filters' in error['message'] for error in errors)
    assert {error['range']['start']['line'] for error in errors} == {5, 6}
