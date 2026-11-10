from typing import NoReturn

import pytest

from tests._sockets import refuse_connections


def block_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse() -> NoReturn:
        raise AssertionError('network forbidden')

    refuse_connections(refuse, monkeypatch.setattr)
