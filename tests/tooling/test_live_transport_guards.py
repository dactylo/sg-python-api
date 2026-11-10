from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
import urllib3

from shotgun_api3._transport import TransportRequest, Urllib3Transport
from tests._live_safety import LiveBudget, LiveStop
from tests._live_vocabulary import SESSION_NAMES
from tests.live.conftest import patch_live_transports
from tests.tooling._network import block_connections

if TYPE_CHECKING:
    from shotgun_api3._async_httpx2 import Httpx2Transport
    from shotgun_api3._transport import TransportResponse


@pytest.mark.anyio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('session', SESSION_NAMES)
@pytest.mark.parametrize('operation', ['schema_field_create', 'export_page', 'batch'])
async def test_forbidden_wire_payloads_never_reach_either_transport(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    session: str,
    operation: str,
) -> None:
    sent: list[object] = []

    def sync_send(*args: object, **_kwargs: object) -> urllib3.response.HTTPResponse:
        sent.append(args)
        raise AssertionError('forbidden transport call')

    async def async_send(*args: object, **_kwargs: object) -> TransportResponse:
        sent.append(args)
        raise AssertionError('forbidden transport call')

    block_connections(monkeypatch)
    transport = cast(
        'type[Httpx2Transport]',
        importlib.import_module('shotgun_api3._async_httpx2').Httpx2Transport,
    )
    monkeypatch.setattr(Urllib3Transport, '_start_request', sync_send)
    monkeypatch.setattr(transport, '_send_once', async_send)
    budget = LiveBudget(session=session)
    patch_live_transports(budget, monkeypatch)
    body = json.dumps(
        {
            'method_name': operation,
            'params': [{}, [{'request_type': 'update', 'type': 'HumanUser', 'id': 1}]],
        }
    ).encode()
    request = cast('TransportRequest', SimpleNamespace(method='POST', body=body, headers={}))
    if kind == 'sync':
        with pytest.raises(LiveStop, match='refused'):
            Urllib3Transport._start_request(
                cast('Urllib3Transport', SimpleNamespace()), request, preload_content=True
            )
    else:
        with pytest.raises(LiveStop, match='refused'):
            await transport._send_once(cast('Httpx2Transport', SimpleNamespace()), request)
    assert sent == []
    assert budget.counts['requests'] == 0


def test_transport_carries_retry_after_to_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    def sync_send(*_args: object, **_kwargs: object) -> urllib3.response.HTTPResponse:
        return urllib3.response.HTTPResponse(status=503, headers={'Retry-After': '7'})

    monkeypatch.setattr(Urllib3Transport, '_start_request', sync_send)
    budget = LiveBudget(cleanup_requests=2)
    patch_live_transports(budget, monkeypatch)

    transport = cast('Urllib3Transport', SimpleNamespace())
    request = cast('TransportRequest', SimpleNamespace(method='POST', body=b'{}', headers={}))
    with pytest.raises(LiveStop):
        Urllib3Transport._start_request(transport, request, preload_content=True)
    assert budget.retry_after == 7
