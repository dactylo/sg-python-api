from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Literal, cast

import pytest

from shotgun_api3.lib.mockgun import AsyncShotgun, Shotgun
from shotgun_api3.lib.mockgun import _transport as mockgun_transport
from tests._live_safety import LiveStop
from tests._live_vocabulary import SESSION_NAMES
from tests._parity import (
    EXTENDED_SCENARIOS,
    SCENARIOS,
    MockgunCase,
    parity_schema,
    parity_schema_entity,
)
from tests.live._session import session_budget, session_nodes
from tests.live.conftest import upload_size

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from shotgun_api3._transport import TransportRequest
    from tests._live_safety import LiveBudget

_REGISTRY = {scenario.__name__: scenario for scenario in (*SCENARIOS, *EXTENDED_SCENARIOS)}


def _guard_transports(monkeypatch: pytest.MonkeyPatch, current: list[LiveBudget]) -> None:
    def charge(request: TransportRequest) -> None:
        if current:
            current[0].before(body=request.body, upload_bytes=upload_size(request))

    for owner in (mockgun_transport.AsyncInMemoryTransport, mockgun_transport.InMemoryTransport):
        for name in ('send', 'send_streaming'):
            original = getattr(owner, name)
            if owner is mockgun_transport.AsyncInMemoryTransport:

                async def guarded_async(
                    self: object,
                    request: TransportRequest,
                    original: Callable[..., Awaitable[object]] = original,
                ) -> object:
                    charge(request)
                    return await original(self, request)

                monkeypatch.setattr(owner, name, guarded_async)
            else:

                def guarded_sync(
                    self: object,
                    request: TransportRequest,
                    original: Callable[..., object] = original,
                ) -> object:
                    charge(request)
                    return original(self, request)

                monkeypatch.setattr(owner, name, guarded_sync)


# Every RPC the real client sends for a session's scenarios, including RPCs a public
# method makes internally, must pass that session's wire allowlist.
@pytest.mark.anyio
@pytest.mark.parametrize('session', [name for name in SESSION_NAMES if session_nodes(name)])
async def test_session_scenarios_pass_their_wire_allowlist(
    session: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema_path = tmp_path / 'schema.pickle'
    entity_path = tmp_path / 'entities.pickle'
    schema_path.write_bytes(pickle.dumps(parity_schema(), protocol=2))
    entity_path.write_bytes(pickle.dumps(parity_schema_entity(), protocol=2))
    monkeypatch.setattr(Shotgun, '_schema_path', str(schema_path))
    monkeypatch.setattr(Shotgun, '_schema_entity_path', str(entity_path))
    monkeypatch.setattr(AsyncShotgun, '_schema_paths', None)
    current: list[LiveBudget] = []
    _guard_transports(monkeypatch, current)
    refused: list[str] = []
    for node in session_nodes(session):
        kind, name = node.split('[', 1)[1].rstrip(']').split('-', 1)
        case = MockgunCase(
            cast("Literal['sync', 'async']", kind), 'sg-python-api-live-0123456789ab'
        )
        await case.initialize()
        current[:] = [session_budget(session)]
        try:
            await _REGISTRY[name](case)
        except LiveStop:
            refused.append(name)
        finally:
            current.clear()
            await case.call('close')
    assert refused == []
