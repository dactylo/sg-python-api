from __future__ import annotations

import inspect
import json
import pickle
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, cast

from anyio import Path as AsyncPath

from shotgun_api3.lib.mockgun import AsyncShotgun, Shotgun
from tests._parity import MockgunCase, parity_schema, parity_schema_entity
from tests.live._session import block_connections
from tests.live._verification import LiveCase, LiveTarget, baseline_equal

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests._live_safety import LiveBudget


class Client:
    def __init__(self, client: Shotgun | AsyncShotgun, budget: LiveBudget, *, sudo: bool) -> None:
        self.client = client
        self._budget = budget
        self._sudo = sudo

    def __getattr__(self, method: str) -> Callable[..., object]:
        async def invoke(*args: object, **kwargs: object) -> object:
            if method != 'close':
                wire = {
                    'find': 'read',
                    'find_one': 'read',
                    'download_attachment': 'read',
                    'get_attachment_download_url': 'read',
                }.get(method, method)
                params: dict[str, object] = {}
                if args and method in {'create', 'update', 'delete', 'revive'}:
                    params['type'] = args[0]
                body = json.dumps(
                    {
                        'method_name': wire,
                        'params': [{'sudo_as_login': 'standin'} if self._sudo else {}, params],
                    }
                ).encode()
                size = (await AsyncPath(str(args[2])).stat()).st_size if method == 'upload' else 0
                self._budget.before(body=body, upload_bytes=size)
            result = cast('Callable[..., object]', getattr(self.client, method))(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        return invoke


class _StandinCase(LiveCase):
    def verification_case(self) -> LiveCase:
        assert self.target is not None
        client = cast('Client', self.client).client
        # These are local mock-state assertions, not simulated live RPCs. Reuse
        # the final-scan logic without consuming or changing live request budgets.
        return LiveCase(
            cast("Literal['sync', 'async']", self.kind),
            self.project,
            self.namespace,
            script_name=self.script_name,
            target=replace(
                self.target,
                factory=partial(client.new_client, script_name='parity', api_key='secret'),
            ),
        )

    async def cleanup(self) -> None:
        await super().cleanup()
        assert self.target is not None
        checking = self.verification_case()
        try:
            current = await checking.read_baselines()
            prefix_count = await checking.prefix_count()
            if prefix_count or any(
                not baseline_equal(label, current[label], self.target.baselines[label])
                for label in ('F', 'S', 'W', 'P')
            ):
                self.target.budget.stopped = 'live cleanup or restoration failed'
                raise RuntimeError('live cleanup failed: stand-in final verification')
        finally:
            await checking.call('close')


async def _seed_case(kind: str) -> MockgunCase:
    with TemporaryDirectory() as directory:
        schema = Path(directory) / 'schema.pickle'
        entities = Path(directory) / 'entities.pickle'
        fields = parity_schema()
        fields['Tag'] = {
            'name': {
                'data_type': {'value': 'text'},
                'properties': {'default_value': {'value': None}},
            }
        }
        entity_fields = parity_schema_entity()
        entity_fields['Tag'] = {'name': {'value': 'Tag'}}
        schema.write_bytes(pickle.dumps(fields, protocol=2))
        entities.write_bytes(pickle.dumps(entity_fields, protocol=2))
        schema_path = Shotgun._schema_path
        schema_entity_path = Shotgun._schema_entity_path
        async_schema_paths = AsyncShotgun._schema_paths
        try:
            Shotgun._schema_path = str(schema)
            Shotgun._schema_entity_path = str(entities)
            AsyncShotgun._schema_paths = None
            original = MockgunCase(kind, 'standin')
            await original.initialize()
        finally:
            Shotgun._schema_path = schema_path
            Shotgun._schema_entity_path = schema_entity_path
            AsyncShotgun._schema_paths = async_schema_paths
    original.client.site.set_time(datetime.now(tz=UTC))
    return original


async def make_case(kind: str, namespace: str, budget: LiveBudget) -> LiveCase:
    original = await _seed_case(kind)
    run_date = original.client.site.now().date()
    human = cast(
        'dict[str, object]',
        await original.call(
            'find_one', 'HumanUser', [['login', 'is', 'standin']], ['name', 'login']
        ),
    )

    def factory(**options: object) -> object:
        client = original.client.new_client(**options) if options else original.client
        return Client(client, budget, sudo=bool(options))

    def advance(seconds: int) -> None:
        original.client.site.set_time(original.client.site.now() + timedelta(seconds=seconds))

    target = LiveTarget(
        factory, budget, human=human, human_login='standin', run_date=run_date, advance=advance
    )
    case = _StandinCase(
        cast("Literal['sync', 'async']", kind),
        original.project,
        namespace,
        script_name='parity',
        target=target,
    )
    checking = case.verification_case()
    try:
        target.baselines = await checking.read_baselines()
    finally:
        await checking.call('close')
    return case


def pytest_configure() -> None:

    block_connections()
