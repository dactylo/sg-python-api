#!/usr/bin/env python
"""Measure a repeatable offline mockgun CRUD and schema workload.

Run from the project environment to compare the old mockgun with later
implementations. Each measured run constructs a fresh client and discards its
results. The schema is stored in temporary pickle files for that invocation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import pickle
import statistics
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from shotgun_api3.lib import mockgun

if TYPE_CHECKING:
    from collections.abc import Sequence

_SCHEMA: dict[str, dict[str, dict[str, object]]] = {
    'EventLogEntry': {
        'description': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'event_type': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
    },
    'Project': {
        'name': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
    },
    'Shot': {
        'code': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'description': {
            'data_type': {'value': 'text'},
            'properties': {'default_value': {'value': None}},
        },
        'project': {
            'data_type': {'value': 'entity'},
            'properties': {
                'default_value': {'value': None},
                'valid_types': {'value': ['Project']},
            },
        },
        'sg_status_list': {
            'data_type': {'value': 'status_list'},
            'properties': {'default_value': {'value': None}},
        },
    },
}
_SCHEMA_ENTITY: dict[str, dict[str, dict[str, object]]] = {
    'EventLogEntry': {'name': {'value': 'Event Log Entry'}},
    'Project': {'name': {'value': 'Project'}},
    'Shot': {'name': {'value': 'Shot'}},
}
_URL = 'https://mockgun.example'


def _count(base: int, scale: float) -> int:
    """Keep every operation represented at small positive scales."""
    return max(1, round(base * scale))


async def _run_async(scale: float) -> float:
    """Time a fresh asynchronous client's matching workload."""
    start = time.perf_counter()
    client = mockgun.AsyncShotgun(_URL, 'bench', 'key')
    project = await client.create('Project', {'name': 'Benchmark'})
    project_link = {'type': 'Project', 'id': project['id']}
    shot_ids: list[int] = []
    shot_count = _count(300, scale)
    for index in range(shot_count):
        created = await client.create(
            'Shot',
            {
                'code': f'Shot-{index:04d}',
                'description': 'baseline',
                'project': project_link,
                'sg_status_list': 'ip',
            },
        )
        shot_id = created['id']
        if not isinstance(shot_id, int):
            raise TypeError('mockgun create returned a non-integer Shot ID')
        shot_ids.append(shot_id)
    for index in range(_count(300, scale)):
        await client.find_one('Shot', [['code', 'is', f'Shot-{index % shot_count:04d}']])
    for _ in range(_count(30, scale)):
        await client.find(
            'Shot',
            [['project', 'is', project_link], ['sg_status_list', 'is', 'ip']],
            ['code'],
            order=[{'field_name': 'code', 'direction': 'asc'}],
        )
    for index in range(_count(150, scale)):
        await client.update('Shot', shot_ids[index % shot_count], {'description': 'updated'})
    for batch_index in range(_count(30, scale)):
        await client.batch(
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {
                        'code': f'Batch-{batch_index:04d}-{offset}',
                        'project': project_link,
                    },
                }
                for offset in range(5)
            ]
        )
    for index in range(_count(50, scale)):
        await client.delete('Shot', shot_ids[index % shot_count])
    for _ in range(_count(10, scale)):
        await client.schema_field_read('Shot', 'code')
    return time.perf_counter() - start


def _run_sync(scale: float) -> float:
    """Time a fresh synchronous client's workload."""
    start = time.perf_counter()
    client = mockgun.Shotgun(_URL, 'bench', 'key')
    project = client.create('Project', {'name': 'Benchmark'})
    project_link = {'type': 'Project', 'id': project['id']}
    shot_ids: list[int] = []
    shot_count = _count(300, scale)
    for index in range(shot_count):
        created = client.create(
            'Shot',
            {
                'code': f'Shot-{index:04d}',
                'description': 'baseline',
                'project': project_link,
                'sg_status_list': 'ip',
            },
        )
        shot_id = created['id']
        if not isinstance(shot_id, int):
            raise TypeError('mockgun create returned a non-integer Shot ID')
        shot_ids.append(shot_id)
    for index in range(_count(300, scale)):
        client.find_one('Shot', [['code', 'is', f'Shot-{index % shot_count:04d}']])
    for _ in range(_count(30, scale)):
        client.find(
            'Shot',
            [['project', 'is', project_link], ['sg_status_list', 'is', 'ip']],
            ['code'],
            order=[{'field_name': 'code', 'direction': 'asc'}],
        )
    for index in range(_count(150, scale)):
        client.update('Shot', shot_ids[index % shot_count], {'description': 'updated'})
    for batch_index in range(_count(30, scale)):
        client.batch(
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {
                        'code': f'Batch-{batch_index:04d}-{offset}',
                        'project': project_link,
                    },
                }
                for offset in range(5)
            ]
        )
    for index in range(_count(50, scale)):
        client.delete('Shot', shot_ids[index % shot_count])
    for _ in range(_count(10, scale)):
        client.schema_field_read('Shot', 'code')
    return time.perf_counter() - start


def _write_schema(directory: Path) -> tuple[str, str]:
    """Write the small schema consumed by both mockgun clients."""
    schema_path = directory / 'schema.pickle'
    entity_path = directory / 'schema_entity.pickle'
    schema_path.write_bytes(pickle.dumps(_SCHEMA, protocol=2))
    entity_path.write_bytes(pickle.dumps(_SCHEMA_ENTITY, protocol=2))
    return str(schema_path), str(entity_path)


def _main(argv: Sequence[str] | None = None) -> int:
    """Run selected clients and print their seconds and medians as JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', choices=('sync', 'async', 'both'), default='both')
    parser.add_argument('--repeat', type=int, default=5)
    parser.add_argument('--scale', type=float, default=1.0)
    args = parser.parse_args(argv)
    if args.repeat < 1:
        parser.error('--repeat must be positive')
    if not math.isfinite(args.scale) or args.scale <= 0:
        parser.error('--scale must be finite and greater than zero')

    report: dict[str, dict[str, list[float] | float]] = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        schema_paths = _write_schema(Path(temp_dir))
        if args.client in ('sync', 'both'):
            mockgun.Shotgun.set_schema_paths(*schema_paths)
            sync_runs = [_run_sync(args.scale) for _ in range(args.repeat)]
            report['sync'] = {'runs': sync_runs, 'median': statistics.median(sync_runs)}
        if args.client in ('async', 'both'):
            asyncio.run(mockgun.AsyncShotgun.set_schema_paths(*schema_paths))
            async_runs = [asyncio.run(_run_async(args.scale)) for _ in range(args.repeat)]
            report['async'] = {'runs': async_runs, 'median': statistics.median(async_runs)}
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
