from __future__ import annotations

import importlib
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pytest

import shotgun_api3

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import Self

_MODULE_NAMES = ('examples.async_client', 'examples.sync_client')
_PROJECT_NAME = 'Project With Spaces'
_SCRIPT_NAME = 'example-script'
_SENTINEL = 'sentinel-value-for-output-safety'
_SERVER_VERSION = [8, 87, 0]
_SITE = 'https://example.shotgrid.autodesk.com'


class _Arguments(Protocol):
    credentials_file: Path
    project: str
    site: str


class _ExampleModule(Protocol):
    main: Callable[[Sequence[str] | None], int]


class _LegacyTerminalClientError(Exception):
    pass


def _arguments(credentials_file: Path) -> list[str]:
    return [
        '--site',
        _SITE,
        '--project',
        _PROJECT_NAME,
        '--credentials-file',
        str(credentials_file),
    ]


def _import_example(module_name: str) -> _ExampleModule:
    return cast('_ExampleModule', importlib.import_module(module_name))


def _load_credentials(module_name: str, path: Path) -> tuple[str, str]:
    module = importlib.import_module(module_name)
    loader = cast('Callable[[Path], tuple[str, str]]', vars(module)['_load_credentials'])
    return loader(path)


def _module_path() -> Path:
    module_path = shotgun_api3.__file__
    assert module_path is not None
    return Path(module_path).resolve()


def _package_identity_lines() -> list[str]:
    return [
        f'shotgun_api3 version: {shotgun_api3.__version__}',
        f'shotgun_api3 module: {_module_path()}',
    ]


def _parse_args(module_name: str, argv: Sequence[str]) -> _Arguments:
    module = importlib.import_module(module_name)
    parser = cast(
        'Callable[[Sequence[str] | None], _Arguments]',
        vars(module)['_parse_args'],
    )
    return parser(argv)


def test_async_main_queries_project_and_prints_allowlisted_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _import_example('examples.async_client')
    credentials_file = tmp_path / 'credentials.txt'
    credentials_file.write_text(f'{_SCRIPT_NAME}\n{_SENTINEL}\n', encoding='utf-8')
    calls: list[tuple[object, ...]] = []

    class FakeAsyncShotgun:
        def __init__(self, *args: object, **kwargs: object) -> None:
            calls.append(('construct', args, kwargs))

        async def __aenter__(self) -> Self:
            calls.append(('enter',))
            return self

        async def __aexit__(self, *exc: object) -> None:
            calls.append(('exit', *exc))

        async def find_one(
            self,
            entity_type: str,
            filters: list[list[object]],
            fields: list[str],
        ) -> dict[str, object]:
            calls.append(('find_one', entity_type, filters, fields))
            return {
                'id': 41,
                'name': _PROJECT_NAME,
                'raw-project-secret': _SENTINEL,
                'type': 'Project',
            }

        async def info(self) -> dict[str, object]:
            calls.append(('info',))
            return {
                'full_version': [*_SERVER_VERSION, 927],
                'raw-server-secret': _SENTINEL,
                'version': _SERVER_VERSION,
            }

    monkeypatch.setattr(module, 'AsyncShotgun', FakeAsyncShotgun)

    status = module.main(_arguments(credentials_file))
    captured = capsys.readouterr()

    assert status == 0
    assert calls[:4] == [
        ('construct', (_SITE,), {'api_key': _SENTINEL, 'script_name': _SCRIPT_NAME}),
        ('enter',),
        ('info',),
        ('find_one', 'Project', [['name', 'is', _PROJECT_NAME]], ['name']),
    ]
    assert len(calls) == 5
    assert calls[-1][0] == 'exit'
    assert calls[-1][1:] == (None, None, None)
    assert captured.out.splitlines() == [
        f'shotgun_api3 version: {shotgun_api3.__version__}',
        f'shotgun_api3 module: {_module_path()}',
        f'server version: {_SERVER_VERSION}',
        'project id: 41',
        f'project name: {_PROJECT_NAME}',
    ]
    assert captured.err == ''
    assert _SENTINEL not in captured.out
    assert 'full_version' not in captured.out
    assert 'raw-' not in captured.out


def test_async_request_failure_from_legacy_terminal_exception_exits_once_and_is_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _import_example('examples.async_client')
    credentials_file = tmp_path / 'credentials.txt'
    credentials_file.write_text(f'{_SCRIPT_NAME}\n{_SENTINEL}\n', encoding='utf-8')
    calls: list[tuple[object, ...]] = []
    identity_before_request: list[str] = []

    class FailingAsyncShotgun:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            calls.append(('construct',))

        async def __aenter__(self) -> Self:
            calls.append(('enter',))
            return self

        async def __aexit__(self, *exc: object) -> None:
            calls.append(('exit', *exc))

        async def info(self) -> dict[str, object]:
            calls.append(('info',))
            identity_before_request.extend(capsys.readouterr().out.splitlines())
            raise _LegacyTerminalClientError(f'request exposed {_SENTINEL}')

    monkeypatch.setattr(module, 'AsyncShotgun', FailingAsyncShotgun)

    status = module.main(_arguments(credentials_file))
    captured = capsys.readouterr()

    assert status == 1
    assert [call[0] for call in calls] == ['construct', 'enter', 'info', 'exit']
    assert len([call for call in calls if call[0] == 'exit']) == 1
    assert calls[-1][1] is _LegacyTerminalClientError
    assert identity_before_request == _package_identity_lines()
    assert captured.out == ''
    assert captured.err == 'Unable to run example.\n'
    assert _SENTINEL not in captured.err


@pytest.mark.parametrize('module_name', _MODULE_NAMES)
@pytest.mark.parametrize('terminal_newline', ['', '\n'])
def test_credential_loaders_accept_exactly_two_nonempty_lines(
    tmp_path: Path,
    module_name: str,
    terminal_newline: str,
) -> None:
    credentials_file = tmp_path / 'credentials.txt'
    credentials_file.write_text(
        f'{_SCRIPT_NAME}\n{_SENTINEL}{terminal_newline}',
        encoding='utf-8',
    )

    assert _load_credentials(module_name, credentials_file) == (_SCRIPT_NAME, _SENTINEL)


@pytest.mark.parametrize('module_name', _MODULE_NAMES)
@pytest.mark.parametrize(
    'content',
    [
        pytest.param(None, id='missing'),
        pytest.param(f'{_SCRIPT_NAME}\n   \n', id='blank'),
        pytest.param(f'{_SCRIPT_NAME}\n{_SENTINEL}\nextra', id='extra'),
        pytest.param(b'example-script\n\xff', id='undecodable'),
    ],
)
def test_credential_loaders_reject_invalid_content_without_exposing_secret(
    tmp_path: Path,
    module_name: str,
    content: bytes | str | None,
) -> None:
    credentials_file = tmp_path / 'credentials.txt'
    if isinstance(content, bytes):
        credentials_file.write_bytes(content)
    elif content is not None:
        credentials_file.write_text(content, encoding='utf-8')

    with pytest.raises((OSError, UnicodeError, ValueError)) as raised:
        _load_credentials(module_name, credentials_file)

    assert _SENTINEL not in str(raised.value)


@pytest.mark.parametrize('module_name', _MODULE_NAMES)
def test_parsers_preserve_spaces_in_project_name(module_name: str) -> None:
    credentials_file = Path('credentials.txt')

    parsed = _parse_args(module_name, _arguments(credentials_file))

    assert parsed.site == _SITE
    assert parsed.project == _PROJECT_NAME
    assert parsed.credentials_file == credentials_file


@pytest.mark.parametrize('module_name', _MODULE_NAMES)
@pytest.mark.parametrize('missing_option', ['--credentials-file', '--project', '--site'])
def test_parsers_require_all_options(module_name: str, missing_option: str) -> None:
    credentials_file = Path('credentials.txt')
    arguments = _arguments(credentials_file)
    option_index = arguments.index(missing_option)
    del arguments[option_index : option_index + 2]

    with pytest.raises(SystemExit) as raised:
        _parse_args(module_name, arguments)

    assert raised.value.code == 2


def test_sync_main_queries_project_and_prints_allowlisted_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _import_example('examples.sync_client')
    credentials_file = tmp_path / 'credentials.txt'
    credentials_file.write_text(f'{_SCRIPT_NAME}\n{_SENTINEL}\n', encoding='utf-8')
    calls: list[tuple[object, ...]] = []

    class FakeShotgun:
        def __init__(self, *args: object, **kwargs: object) -> None:
            calls.append(('construct', args, kwargs))

        def close(self) -> None:
            calls.append(('close',))

        def find_one(
            self,
            entity_type: str,
            filters: list[list[object]],
            fields: list[str],
        ) -> dict[str, object]:
            calls.append(('find_one', entity_type, filters, fields))
            return {
                'id': 41,
                'name': _PROJECT_NAME,
                'raw-project-secret': _SENTINEL,
                'type': 'Project',
            }

        def info(self) -> dict[str, object]:
            calls.append(('info',))
            return {
                'full_version': [*_SERVER_VERSION, 927],
                'raw-server-secret': _SENTINEL,
                'version': _SERVER_VERSION,
            }

    monkeypatch.setattr(module, 'Shotgun', FakeShotgun)

    status = module.main(_arguments(credentials_file))
    captured = capsys.readouterr()

    assert status == 0
    assert calls == [
        (
            'construct',
            (_SITE,),
            {'api_key': _SENTINEL, 'connect': False, 'script_name': _SCRIPT_NAME},
        ),
        ('info',),
        ('find_one', 'Project', [['name', 'is', _PROJECT_NAME]], ['name']),
        ('close',),
    ]
    assert captured.out.splitlines() == [
        f'shotgun_api3 version: {shotgun_api3.__version__}',
        f'shotgun_api3 module: {_module_path()}',
        f'server version: {_SERVER_VERSION}',
        'project id: 41',
        f'project name: {_PROJECT_NAME}',
    ]
    assert captured.err == ''
    assert _SENTINEL not in captured.out
    assert 'full_version' not in captured.out
    assert 'raw-' not in captured.out


def test_sync_request_failure_from_legacy_terminal_exception_closes_once_and_is_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _import_example('examples.sync_client')
    credentials_file = tmp_path / 'credentials.txt'
    credentials_file.write_text(f'{_SCRIPT_NAME}\n{_SENTINEL}\n', encoding='utf-8')
    calls: list[str] = []
    identity_before_request: list[str] = []

    class FailingShotgun:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            calls.append('construct')

        def close(self) -> None:
            calls.append('close')

        def info(self) -> dict[str, object]:
            calls.append('info')
            identity_before_request.extend(capsys.readouterr().out.splitlines())
            raise _LegacyTerminalClientError(f'request exposed {_SENTINEL}')

    monkeypatch.setattr(module, 'Shotgun', FailingShotgun)

    status = module.main(_arguments(credentials_file))
    captured = capsys.readouterr()

    assert status == 1
    assert calls == ['construct', 'info', 'close']
    assert identity_before_request == _package_identity_lines()
    assert captured.out == ''
    assert captured.err == 'Unable to run example.\n'
    assert _SENTINEL not in captured.err
