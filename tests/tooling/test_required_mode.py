import asyncio
import os
import runpy
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

import pytest

import shotgun_api3
from tests._required_mode import REQUIRED_LIVE_ENV

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakeBootstrapClient:
    instances: ClassVar[list['_FakeBootstrapClient']] = []
    project: ClassVar[dict[str, object] | None] = None

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.closed = False
        self.__class__.instances.append(self)

    def close(self) -> None:
        self.closed = True

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        assert entity_type == 'Project'
        self.__class__.project = {'type': 'Project', 'id': 17, **data}
        return self.__class__.project

    def find_one(
        self,
        entity_type: str,
        filters: list[list[object]],
    ) -> dict[str, object] | None:
        assert entity_type == 'Project'
        assert filters == [['name', 'is', 'sg-python-api live test project']]
        return self.__class__.project


class _FakeLiveClient:
    def __init__(
        self,
        *,
        residual_ids: Iterable[int] = (),
        attachments: dict[int, str] | None = None,
    ) -> None:
        self.attachments = {} if attachments is None else attachments
        self.calls: list[tuple[object, ...]] = []
        self.residual_ids = set(residual_ids)

    def delete(self, entity_type: str, entity_id: int) -> bool:
        self.calls.append(('delete', entity_type, entity_id))
        if entity_id not in self.residual_ids and entity_type == 'Attachment':
            self.attachments.pop(entity_id, None)
        return entity_id not in self.residual_ids

    def find(
        self,
        entity_type: str,
        filters: list[list[object]],
        fields: list[str],
    ) -> list[dict[str, object]]:
        self.calls.append(('find', entity_type, filters, fields))
        if entity_type == 'Attachment':
            assert filters[1][:2] == ['filename', 'starts_with']
            prefix = filters[1][2]
            assert isinstance(prefix, str)
            return [
                {'type': 'Attachment', 'id': identity}
                for identity, filename in self.attachments.items()
                if filename.startswith(prefix)
            ]
        return []

    def find_one(
        self,
        entity_type: str,
        filters: list[list[object]],
    ) -> dict[str, object] | None:
        self.calls.append(('find_one', entity_type, filters))
        entity_id = filters[0][2]
        assert isinstance(entity_id, int)
        if entity_id in self.residual_ids or (
            entity_type == 'Attachment' and entity_id in self.attachments
        ):
            return {'type': entity_type, 'id': entity_id}
        return None


def _configure_isolated_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    present: Iterable[str] = (),
) -> None:
    for name in REQUIRED_LIVE_ENV:
        monkeypatch.delenv(name, raising=False)
    for name in present:
        monkeypatch.setenv(name, 'configured-for-isolated-test')
    inherited = os.environ.get('PYTHONPATH')
    pythonpath = str(_REPO_ROOT) if inherited is None else f'{_REPO_ROOT}{os.pathsep}{inherited}'
    monkeypatch.setenv('PYTHONPATH', pythonpath)


def _install_plugin(pytester: pytest.Pytester) -> None:
    pytester.makeconftest("pytest_plugins = ('tests._required_mode',)")


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_live_cleanup_propagates_delete_cancellation() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_async.py'))
    resources_class = namespace['_AsyncLiveResources']
    cancellation = asyncio.CancelledError('delete cancellation')

    class CancellingClient:
        async def delete(self, entity_type: str, entity_id: int) -> bool:
            del entity_type, entity_id
            raise cancellation

    resources = resources_class(
        CancellingClient(),
        {'type': 'Project', 'id': 9},
        'namespace',
    )
    resources.track('Shot', 10)

    with pytest.raises(asyncio.CancelledError) as raised:
        await resources.cleanup()

    assert raised.value is cancellation


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_live_cleanup_recovers_tracked_and_namespace_residuals() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_async.py'))
    resources_class = namespace['_AsyncLiveResources']
    project = {'type': 'Project', 'id': 9}
    run_name = 'sg-python-api-async-live-run'

    class FakeAsyncClient:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []
            self.failed_deletes = {('Attachment', 11)}
            self.present = {
                ('Attachment', 11),
                ('Attachment', 12),
                ('Shot', 10),
                ('Shot', 13),
            }

        async def delete(self, entity_type: str, entity_id: int) -> bool:
            identity = (entity_type, entity_id)
            self.calls.append(('delete', *identity))
            if identity in self.failed_deletes:
                self.failed_deletes.remove(identity)
                raise RuntimeError('credential-bearing server failure')
            if identity not in self.present:
                return False
            self.present.remove(identity)
            return True

        async def find(
            self,
            entity_type: str,
            filters: list[list[object]],
            fields: list[str],
        ) -> list[dict[str, object]]:
            self.calls.append(('find', entity_type, filters, fields))
            expected_field = 'filename' if entity_type == 'Attachment' else 'code'
            assert filters == [
                ['project', 'is', project],
                [expected_field, 'starts_with', run_name],
            ]
            return [
                {'type': found_type, 'id': entity_id}
                for found_type, entity_id in sorted(self.present)
                if found_type == entity_type
            ]

        async def find_one(
            self,
            entity_type: str,
            filters: list[list[object]],
        ) -> dict[str, object] | None:
            self.calls.append(('find_one', entity_type, filters))
            entity_id = filters[0][2]
            assert isinstance(entity_id, int)
            if (entity_type, entity_id) in self.present:
                return {'type': entity_type, 'id': entity_id}
            return None

    client = FakeAsyncClient()
    resources = resources_class(client, project, run_name)
    resources.track('Shot', 10)
    resources.track('Attachment', 11)

    with pytest.raises(RuntimeError, match='delete raised for Attachment:11') as raised:
        await resources.cleanup()

    assert 'credential-bearing' not in str(raised.value)
    assert [call for call in client.calls if call[0] == 'delete'] == [
        ('delete', 'Attachment', 11),
        ('delete', 'Shot', 10),
        ('delete', 'Attachment', 11),
        ('delete', 'Attachment', 12),
        ('delete', 'Shot', 13),
    ]
    assert client.present == set()
    attachment_scan = (
        'find',
        'Attachment',
        [
            ['project', 'is', project],
            ['filename', 'starts_with', run_name],
        ],
        ['id'],
    )
    assert client.calls.count(attachment_scan) == 2


@pytest.mark.async_required
def test_async_live_failure_capture_propagates_process_base_exceptions() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_async.py'))
    capture_class = namespace['_FailureCapture']
    interrupt = KeyboardInterrupt('stop')

    with pytest.raises(KeyboardInterrupt) as raised, capture_class():
        raise interrupt

    assert raised.value is interrupt


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_live_finalizers_drain_and_aggregate_across_cancellation() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_async.py'))
    assert '_finalize_live_test' in namespace
    finalize = namespace['_finalize_live_test']
    operation_failure = ValueError('operation failed')

    class BlockingResources:
        def __init__(self) -> None:
            self.completed = False
            self.release = asyncio.Event()
            self.started = asyncio.Event()

        async def cleanup(self) -> None:
            self.started.set()
            await self.release.wait()
            self.completed = True
            raise RuntimeError('cleanup failed')

    class BlockingClient:
        def __init__(self) -> None:
            self.completed = False
            self.release = asyncio.Event()
            self.started = asyncio.Event()

        async def close(self) -> None:
            self.started.set()
            await self.release.wait()
            self.completed = True
            raise RuntimeError('close failed')

    resources = BlockingResources()
    client = BlockingClient()
    baseline_tasks = asyncio.all_tasks()
    finalization = asyncio.create_task(finalize(resources, client, operation_failure))
    await asyncio.wait_for(resources.started.wait(), timeout=1)

    finalization.cancel('first cancellation')
    await asyncio.sleep(0)
    resources.release.set()
    await asyncio.wait_for(client.started.wait(), timeout=1)

    finalization.cancel('second cancellation')
    await asyncio.sleep(0)
    client.release.set()
    with pytest.raises(BaseExceptionGroup) as raised:
        await asyncio.wait_for(finalization, timeout=1)

    failures = raised.value.exceptions
    assert failures[0] is operation_failure
    assert isinstance(failures[1], asyncio.CancelledError)
    assert failures[1].args == ('first cancellation',)
    assert isinstance(failures[2], RuntimeError)
    assert str(failures[2]) == 'cleanup failed'
    assert isinstance(failures[3], RuntimeError)
    assert str(failures[3]) == 'close failed'
    assert resources.completed
    assert client.completed
    assert not asyncio.all_tasks() - baseline_tasks


@pytest.mark.anyio
@pytest.mark.async_required
async def test_async_live_roundtrip_namespaces_uploaded_source(tmp_path: Path) -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_async.py'))
    exercise_roundtrip = namespace['_exercise_roundtrip']
    project = {'type': 'Project', 'id': 9}
    run_name = 'sg-python-api-async-live-run'
    payload: bytes | None = None

    class FakeResources:
        def track(self, entity_type: str, entity_id: object) -> int:
            assert entity_type == 'Attachment'
            assert isinstance(entity_id, int)
            return entity_id

        def track_created(self, entity_type: str, entity: dict[str, object]) -> int:
            assert entity_type == 'Shot'
            entity_id = entity['id']
            assert isinstance(entity_id, int)
            return entity_id

    class FakeClient:
        async def create(
            self,
            entity_type: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            assert entity_type == 'Shot'
            return {'type': entity_type, 'id': 10, **data}

        async def download_attachment(
            self,
            attachment: object,
            file_path: str | None = None,
        ) -> str:
            assert attachment == {'type': 'Attachment', 'id': 11}
            assert file_path is not None
            assert payload is not None
            await asyncio.to_thread(Path(file_path).write_bytes, payload)
            return file_path

        async def find_one(
            self,
            entity_type: str,
            filters: list[list[object]],
            fields: list[str],
        ) -> dict[str, object]:
            del filters, fields
            assert entity_type == 'Shot'
            return {'type': entity_type, 'id': 10, 'code': f'{run_name}-roundtrip'}

        async def update(
            self,
            entity_type: str,
            entity_id: int,
            data: dict[str, object],
        ) -> dict[str, object]:
            assert (entity_type, entity_id) == ('Shot', 10)
            return data

        async def upload(
            self,
            entity_type: str,
            entity_id: int,
            path: str,
            *,
            display_name: str,
        ) -> int:
            nonlocal payload
            assert (entity_type, entity_id) == ('Shot', 10)
            assert Path(path).name == f'{run_name}-payload.bin'
            assert display_name == f'{run_name}-payload.bin'
            payload = await asyncio.to_thread(Path(path).read_bytes)
            return 11

    await exercise_roundtrip(FakeClient(), project, FakeResources(), run_name, tmp_path)


@pytest.mark.async_required
def test_async_unit_selector_deselects_live_async_tests(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_isolated_environment(monkeypatch)
    _install_plugin(pytester)
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.async_required
        def test_async_unit():
            pass

        @pytest.mark.async_required
        @pytest.mark.live
        def test_async_live():
            raise AssertionError('live test was selected')
        """
    )

    result = pytester.runpytest_subprocess(
        '-q',
        '-m',
        'async_required and not live',
        '--require-no-skips',
    )

    result.assert_outcomes(passed=1, deselected=1)
    assert result.ret == pytest.ExitCode.OK


def test_bootstrap_is_idempotent_and_closes_each_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = runpy.run_path(str(_REPO_ROOT / 'scripts/bootstrap_test_site.py'))
    main = namespace['main']
    _FakeBootstrapClient.instances = []
    _FakeBootstrapClient.project = None
    main.__globals__['_load_client_class'] = lambda: _FakeBootstrapClient
    monkeypatch.setenv('SG_TEST_SITE', 'https://example.invalid')
    monkeypatch.setenv('SG_TEST_SCRIPT', 'script')
    monkeypatch.setenv('SG_TEST_KEY', 'secret')
    monkeypatch.setenv('SG_TEST_PROJECT', 'sg-python-api live test project')

    assert main() == 0
    assert main() == 0

    assert _FakeBootstrapClient.project == {
        'type': 'Project',
        'id': 17,
        'name': 'sg-python-api live test project',
    }
    assert len(_FakeBootstrapClient.instances) == 2
    assert all(client.closed for client in _FakeBootstrapClient.instances)


def test_bootstrap_loads_client_from_this_checkout() -> None:
    namespace = runpy.run_path(str(_REPO_ROOT / 'scripts/bootstrap_test_site.py'))

    client_class = namespace['_load_client_class']()

    client_module = Path(client_class.__module__.replace('.', '/'))
    assert client_module == Path('shotgun_api3/_client')
    assert Path(shotgun_api3.__file__).resolve().is_relative_to(_REPO_ROOT / 'src')


def test_bootstrap_preserves_operation_and_close_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = runpy.run_path(str(_REPO_ROOT / 'scripts/bootstrap_test_site.py'))
    main = namespace['main']

    class FailingClient(_FakeBootstrapClient):
        def close(self) -> None:
            raise RuntimeError('close failed')

        def find_one(
            self,
            entity_type: str,
            filters: list[list[object]],
        ) -> dict[str, object] | None:
            del entity_type, filters
            raise RuntimeError('lookup failed')

    main.__globals__['_load_client_class'] = lambda: FailingClient
    for name in REQUIRED_LIVE_ENV:
        monkeypatch.setenv(name, 'configured')

    with pytest.raises(BaseExceptionGroup) as raised:
        main()

    assert [str(error) for error in raised.value.exceptions] == [
        'lookup failed',
        'close failed',
    ]


def test_live_resources_clean_in_reverse_order_and_scan_namespace() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))
    resources_class = namespace['_LiveResources']
    client = _FakeLiveClient(
        attachments={12: 'sg-python-api-live-run-payload.bin', 13: 'another-run-payload.bin'}
    )
    project = {'type': 'Project', 'id': 9}
    resources = resources_class(client, project, 'sg-python-api-live-run')
    resources.track('Shot', 10)
    resources.track('Attachment', 11)

    resources.cleanup()

    assert [call for call in client.calls if call[0] == 'delete'] == [
        ('delete', 'Attachment', 12),
        ('delete', 'Attachment', 11),
        ('delete', 'Shot', 10),
    ]
    assert client.attachments == {13: 'another-run-payload.bin'}
    assert (
        'find',
        'Attachment',
        [['project', 'is', project], ['filename', 'starts_with', 'sg-python-api-live-run']],
        ['id'],
    ) in client.calls
    assert ('find_one', 'Attachment', [['id', 'is', 11]]) in client.calls
    assert ('find_one', 'Shot', [['id', 'is', 10]]) in client.calls
    assert any(call[:2] == ('find', 'Shot') for call in client.calls)
    assert any(call[:2] == ('find', 'Note') for call in client.calls)


def test_live_resources_report_residuals_after_attempting_all_cleanup() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))
    resources_class = namespace['_LiveResources']
    client = _FakeLiveClient(residual_ids=(10, 11))
    resources = resources_class(client, {'type': 'Project', 'id': 9}, 'namespace')
    resources.track('Shot', 10)
    resources.track('Attachment', 11)

    with pytest.raises(RuntimeError, match='Attachment:11, Shot:10'):
        resources.cleanup()

    assert [call for call in client.calls if call[0] == 'delete'] == [
        ('delete', 'Attachment', 11),
        ('delete', 'Shot', 10),
    ]


def test_live_run_names_are_unique_and_stable_prefix() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))

    first = namespace['_new_run_namespace']()
    second = namespace['_new_run_namespace']()

    assert first.startswith('sg-python-api-live-')
    assert second.startswith('sg-python-api-live-')
    assert first != second


def test_optional_live_test_skips_without_configuration(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_isolated_environment(monkeypatch)
    _install_plugin(pytester)
    pytester.makepyfile(
        """
        import os
        import pytest

        pytestmark = pytest.mark.live

        def test_optional_live():
            if not all(os.environ.get(name) for name in (
                'SG_TEST_SITE', 'SG_TEST_SCRIPT', 'SG_TEST_KEY', 'SG_TEST_PROJECT'
            )):
                pytest.skip('live-site configuration not present')
        """
    )

    result = pytester.runpytest_subprocess('-q')

    result.assert_outcomes(skipped=1)
    assert result.ret == pytest.ExitCode.OK


def test_require_live_fails_setup_when_configuration_is_missing(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_isolated_environment(monkeypatch)
    _install_plugin(pytester)
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.live

        def test_live_gate():
            pass
        """
    )

    result = pytester.runpytest_subprocess('-q', '--require-live')

    result.assert_outcomes(errors=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(['*required live configuration missing:*'])


def test_require_no_skips_fails_on_selected_skip(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_isolated_environment(monkeypatch)
    _install_plugin(pytester)
    pytester.makepyfile(
        """
        import pytest

        def test_selected_skip():
            pytest.skip('selected skip')
        """
    )

    result = pytester.runpytest_subprocess('-q', '--require-no-skips')

    result.assert_outcomes(skipped=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(['*required mode rejected 1 selected skip*'])


def test_require_no_skips_preserves_zero_collection_exit(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_isolated_environment(monkeypatch)
    _install_plugin(pytester)
    pytester.makepyfile(helper='VALUE = 1')

    result = pytester.runpytest_subprocess('-q', '--require-no-skips')

    assert result.ret == pytest.ExitCode.NO_TESTS_COLLECTED


def test_sync_live_cleanup_collects_ordinary_failures() -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))

    class FailingClient(_FakeLiveClient):
        def delete(self, entity_type: str, entity_id: int) -> bool:
            del entity_type, entity_id
            raise RuntimeError('sensitive cleanup error')

    resources = namespace['_LiveResources'](
        FailingClient(), {'type': 'Project', 'id': 9}, 'namespace'
    )
    resources.track('Shot', 10)
    with pytest.raises(RuntimeError, match='delete raised for Shot:10') as cleanup_error:
        resources.cleanup()
    assert 'sensitive cleanup error' not in str(cleanup_error.value)


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, SystemExit])
def test_sync_live_cleanup_preserves_process_control(error_type: type[BaseException]) -> None:
    namespace: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))
    error = error_type('stop')

    class InterruptingClient(_FakeLiveClient):
        def delete(self, entity_type: str, entity_id: int) -> bool:
            del entity_type, entity_id
            raise error

    resources = namespace['_LiveResources'](
        InterruptingClient(), {'type': 'Project', 'id': 9}, 'namespace'
    )
    resources.track('Shot', 10)
    with pytest.raises(error_type) as raised:
        resources.cleanup()

    assert raised.value is error


@pytest.mark.parametrize('cleanup_fails', [False, True])
def test_sync_live_roundtrip_recovers_upload_with_lost_response(
    tmp_path: Path,
    *,
    cleanup_fails: bool,
) -> None:
    fixtures: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/conftest.py'))
    tests: dict[str, Any] = runpy.run_path(str(_REPO_ROOT / 'tests/live/test_roundtrip.py'))
    project = {'type': 'Project', 'id': 9}
    run_name = 'sg-python-api-live-run'

    class Client(_FakeLiveClient):
        def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
            return {'type': entity_type, 'id': 10, **data}

        def upload(
            self,
            entity_type: str,
            entity_id: int,
            path: str,
            *,
            display_name: str,
        ) -> int:
            assert (entity_type, entity_id) == ('Shot', 10)
            assert display_name.startswith(run_name)
            self.attachments[12] = Path(path).name
            raise TimeoutError('upload response lost')

    client = Client(residual_ids=(12,) if cleanup_fails else ())
    resources = fixtures['_LiveResources'](client, project, run_name)
    with pytest.raises(TimeoutError, match='upload response lost'):
        tests['test_upload_download_roundtrip'](client, project, run_name, resources, tmp_path)

    assert client.attachments[12].startswith(f'{run_name}-')
    if cleanup_fails:
        with pytest.raises(RuntimeError, match='Attachment:12'):
            resources.cleanup()
        assert 12 in client.attachments
    else:
        resources.cleanup()
        assert client.attachments == {}
    assert ('delete', 'Shot', 10) in client.calls
