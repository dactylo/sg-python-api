import pytest

from tests.tooling._network import block_connections


@pytest.fixture
def block_live_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    block_connections(monkeypatch)
