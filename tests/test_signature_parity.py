import ast
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Literal, NamedTuple, TypeAlias, cast, get_type_hints

import pytest

from shotgun_api3._async_client import AsyncShotgun
from shotgun_api3._client import Shotgun
from shotgun_api3.lib import mockgun

pytestmark = pytest.mark.async_required

_DescriptorKind: TypeAlias = Literal['classmethod', 'method', 'property']

_DOCUMENTED_TWIN_METHODS = (
    'activity_stream_read',
    'export_page',
    'find',
    'find_one',
    'summarize',
    'upload',
    'work_schedule_update',
)
_MOCKGUN_DOCUMENTED_TWIN_METHODS = ('find', 'find_one')
# These methods mutate only the locked client configuration and perform no I/O,
# so both clients intentionally expose them as plain methods.
_NON_COROUTINE_CLIENT_METHODS = frozenset(
    {'add_user_agent', 'reset_user_agent', 'set_session_uuid'}
)


class _PublicDescriptor(NamedTuple):
    kind: _DescriptorKind
    member: Callable[..., object]


def _assert_signatures_match_exactly(
    expected: inspect.Signature,
    actual: inspect.Signature,
) -> None:
    assert tuple(actual.parameters) == tuple(expected.parameters)
    for name, expected_parameter in expected.parameters.items():
        actual_parameter = actual.parameters[name]
        assert actual_parameter.kind is expected_parameter.kind
        assert actual_parameter.annotation == expected_parameter.annotation
        assert actual_parameter.default is expected_parameter.default
    assert actual.return_annotation == expected.return_annotation


def _documentation_problems(
    sync_class: type[object],
    async_class: type[object],
    method_names: tuple[str, ...],
    runtime_name: str,
) -> list[str]:
    problems: list[str] = []
    for name in method_names:
        sync_method = getattr(sync_class, name)
        async_method = getattr(async_class, name)
        sync_docstring = inspect.getdoc(sync_method)
        async_docstring = inspect.getdoc(async_method)

        if sync_docstring != async_docstring:
            problems.append(f'{runtime_name} {name}: sync and async docstrings differ')

        for runtime, method, docstring in (
            ('sync', sync_method, sync_docstring),
            ('async', async_method, async_docstring),
        ):
            if docstring is None:
                problems.append(f'{runtime_name} {name}: {runtime} docstring is missing')
                continue
            problems.extend(
                f'{runtime_name} {name}: {runtime} docstring omits {parameter_name}'
                for parameter_name in inspect.signature(method).parameters
                if parameter_name != 'self' and f'{parameter_name}:' not in docstring
            )
            problems.extend(
                f'{runtime_name} {name}: {runtime} docstring exposes {implementation_parameter}'
                for implementation_parameter in ('*args', '**kwargs')
                if implementation_parameter in docstring
            )
    return problems


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
        assert inspect.signature(async_descriptor.member) == inspect.signature(
            sync_descriptor.member
        ), name
        assert not inspect.iscoroutinefunction(sync_descriptor.member), name
        if async_descriptor.kind == 'property' or name in non_coroutine_methods:
            assert not inspect.iscoroutinefunction(async_descriptor.member), name
        else:
            assert inspect.iscoroutinefunction(async_descriptor.member), name


def test_client_public_descriptor_parity() -> None:
    _assert_inventory_matches(
        Shotgun,
        AsyncShotgun,
        non_coroutine_methods=_NON_COROUTINE_CLIENT_METHODS,
    )


def test_constructor_signatures_match_across_runtimes() -> None:
    expected_class = inspect.signature(Shotgun)
    expected_method = inspect.signature(Shotgun.__init__)
    expected_hints = get_type_hints(Shotgun.__init__)

    for class_ in (AsyncShotgun, mockgun.Shotgun, mockgun.AsyncShotgun):
        _assert_signatures_match_exactly(expected_class, inspect.signature(class_))
        _assert_signatures_match_exactly(
            expected_method,
            inspect.signature(class_.__init__),
        )
        assert get_type_hints(class_.__init__) == expected_hints
        assert class_.__init__.__annotations__ is not Shotgun.__init__.__annotations__


def test_decorated_public_method_documentation_matches_across_runtimes() -> None:
    assert (
        _documentation_problems(
            Shotgun,
            AsyncShotgun,
            _DOCUMENTED_TWIN_METHODS,
            'client',
        )
        == []
    )


@pytest.mark.parametrize('method_name', ['activity_stream_read', 'find', 'upload'])
@pytest.mark.parametrize(
    ('class_', 'source_name'),
    [(Shotgun, '_client.py'), (AsyncShotgun, '_async_client.py')],
)
def test_decorated_source_contract_matches_runtime(
    class_: type[object], source_name: str, method_name: str
) -> None:
    source = Path(__file__).resolve().parents[1] / 'src' / 'shotgun_api3' / source_name
    module = ast.parse(source.read_text())
    class_node = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == class_.__name__
    )
    method_node = next(
        node
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
        and ast.get_docstring(node) is not None
    )
    source_doc = inspect.cleandoc(ast.get_docstring(method_node) or '')
    runtime_doc = inspect.getdoc(getattr(class_, method_name)) or ''

    def contract(docstring: str) -> tuple[str, str, str]:
        summary = docstring.splitlines()[0]
        sections: dict[str, str] = {}
        for name in ('Returns:', 'Raises:'):
            start = docstring.find(f'\n{name}\n')
            if start < 0:
                sections[name] = ''
                continue
            section = docstring[start + len(name) + 2 :].split('\n\n', 1)[0]
            sections[name] = ' '.join(section.split())
        return summary, sections['Returns:'], sections['Raises:']

    assert contract(source_doc) == contract(runtime_doc)


def test_mockgun_public_descriptor_parity() -> None:
    _assert_inventory_matches(
        mockgun.Shotgun,
        mockgun.AsyncShotgun,
        non_coroutine_methods=frozenset(),
    )


def test_mockgun_query_documentation_matches_across_runtimes() -> None:
    assert (
        _documentation_problems(
            mockgun.Shotgun,
            mockgun.AsyncShotgun,
            _MOCKGUN_DOCUMENTED_TWIN_METHODS,
            'mockgun',
        )
        == []
    )
