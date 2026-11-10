import runpy
from pathlib import Path
from typing import ClassVar

import pytest

import shotgun_api3
from tests._required_mode import REQUIRED_LIVE_ENV

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakeBootstrapClient:
    instances: ClassVar[list['_FakeBootstrapClient']] = []
    project: ClassVar[dict[str, object] | None] = None
    creations: ClassVar[list[tuple[str, dict[str, object]]]] = []

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.closed = False
        self.__class__.instances.append(self)

    def close(self) -> None:
        self.closed = True

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        assert entity_type == 'Project'
        self.__class__.creations.append((entity_type, data.copy()))
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


def test_bootstrap_is_idempotent_and_closes_each_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = runpy.run_path(str(_REPO_ROOT / 'scripts/bootstrap_test_site.py'))
    main = namespace['_main']
    _FakeBootstrapClient.instances = []
    _FakeBootstrapClient.project = None
    _FakeBootstrapClient.creations = []
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
    assert _FakeBootstrapClient.creations == [
        ('Project', {'name': 'sg-python-api live test project'})
    ]
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
    main = namespace['_main']

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
