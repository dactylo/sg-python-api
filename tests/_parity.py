from __future__ import annotations

import inspect
import struct
import zlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from itertools import combinations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, TypeAlias, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pytest

from shotgun_api3 import Fault, ProtocolError, ShotgunError, ShotgunFileDownloadError
from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._async_client import AsyncShotgun as MockAsyncShotgun
from shotgun_api3.lib.mockgun._query import check_collation, collation_key
from shotgun_api3.lib.mockgun._sessions import RequestContext
from shotgun_api3.lib.mockgun._sync_client import Shotgun as MockShotgun

Scenario: TypeAlias = Callable[['ParityCase'], Awaitable[None]]
EXTENDED_SCENARIOS: list[Scenario] = []
_EVENT_CONNECTION_FIELDS = {
    'AssetAssetConnection': ('asset', 'parent'),
    'AssetShotConnection': ('asset', 'shot'),
    'PlaylistVersionConnection': ('playlist', 'version'),
}
SCENARIOS: list[Scenario] = []


class MockgunCase:
    def __init__(self, kind: str, namespace: str) -> None:
        self.script_name = 'parity'
        self.client = (MockShotgun if kind == 'sync' else MockAsyncShotgun)(
            'https://mockgun.example', script_name=self.script_name, api_key='secret'
        )
        self.namespace = namespace
        self.human_login = namespace
        self.project: dict[str, object] = {}
        self.run_date: date | None = None

    async def advance(self, seconds: int) -> None:
        self.client.site.set_time(self.client.site.now() + timedelta(seconds=seconds))

    async def call(self, method: str, *args: object, **kwargs: object) -> object:
        result = getattr(self.client, method)(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    async def create(
        self, entity_type: str, data: dict[str, object], **kwargs: object
    ) -> dict[str, object]:
        return cast('dict[str, object]', await self.call('create', entity_type, data, **kwargs))

    async def initialize(self) -> None:
        self.client.site.set_time(datetime(2026, 7, 15, 12, tzinfo=UTC))
        context = RequestContext(None, self.client.site.now())
        self.project = _link(
            self.client._server.store.create('Project', {'name': self.namespace}, context=context)
        )
        self.client._server.store.create(
            'HumanUser',
            {'login': self.namespace, 'name': self.namespace, 'sg_status_list': 'act'},
            context=context,
        )

    async def sudo_call(self, method: str, *args: object, **kwargs: object) -> object:
        client = self.client.new_client(sudo_as_login=self.human_login)
        try:
            result = getattr(client, method)(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result
        finally:
            result = client.close()
            if inspect.isawaitable(result):
                await result


def _event_instant(value: object) -> datetime:
    assert isinstance(value, str)
    return datetime.strptime(value, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=UTC)


def _extended_scenario(scenario: Scenario) -> Scenario:
    EXTENDED_SCENARIOS.append(scenario)
    return scenario


def _field(
    data_type: str,
    valid_types: list[str] | None = None,
    *,
    valid_values: list[str] | None = None,
    default: object = None,
) -> dict[str, object]:
    properties: dict[str, object] = {'default_value': {'value': default}}
    if valid_types is not None:
        properties['valid_types'] = {'value': valid_types}
    if valid_values is not None:
        properties['valid_values'] = {'value': valid_values}
    return {'data_type': {'value': data_type}, 'properties': properties}


class ParityCase(Protocol):
    run_date: date | None
    project: dict[str, object]
    namespace: str
    script_name: str
    human_login: str

    async def advance(self, seconds: int) -> None: ...

    async def call(self, method: str, *args: object, **kwargs: object) -> object: ...

    async def create(
        self, entity_type: str, data: dict[str, object], **kwargs: object
    ) -> dict[str, object]: ...

    async def sudo_call(self, method: str, *args: object, **kwargs: object) -> object: ...


async def _file_note(case: ParityCase, label: str) -> dict[str, object]:
    return await case.create(
        'Note',
        {'project': case.project, 'subject': f'{case.namespace}-{label}', 'content': 'body'},
    )


def _id(record: dict[str, object]) -> int:
    identity = record.get('id')
    assert isinstance(identity, int)
    assert not isinstance(identity, bool)
    return identity


def _items(value: object) -> list[object]:
    assert isinstance(value, list)
    return cast('list[object]', value)


def _metadata_events(
    rows: list[dict[str, object]], record: dict[str, object]
) -> list[dict[str, object]]:
    scoped: list[dict[str, object]] = []
    for row in rows:
        value = row.get('meta')
        if isinstance(value, dict):
            meta = cast('dict[str, object]', value)
            if meta.get('entity_type') == record['type'] and meta.get('entity_id') == _id(record):
                scoped.append(row)
    return scoped


def _parity_data(case: ParityCase, label: str, suffix: str) -> dict[str, object]:
    return {'code': f'{case.namespace}-{label}-{suffix}', 'project': case.project}


def _parity_scope(case: ParityCase, label: str) -> list[list[object]]:
    return [
        ['project', 'is', case.project],
        ['code', 'starts_with', f'{case.namespace}-{label}-'],
    ]


def _png(width: int, height: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack('!I', len(data)) + kind + data + struct.pack('!I', zlib.crc32(kind + data))
        )

    row = b'\0' + b'\xff\0\0' * (width // 2) + b'\0\0\xff' * (width - width // 2)
    return (
        b'\x89PNG\r\n\x1a\n'
        + chunk(b'IHDR', struct.pack('!2I5B', width, height, 8, 2, 0, 0, 0))
        + chunk(b'IDAT', zlib.compress(row * height))
        + chunk(b'IEND', b'')
    )


def _record(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast('dict[str, object]', value)


async def _cursor(case: ParityCase) -> int:
    row = await case.call(
        'find_one',
        'EventLogEntry',
        [],
        ['id'],
        order=[{'field_name': 'id', 'direction': 'desc'}],
    )
    return 0 if row is None else _id(_record(row))


async def _fields(
    case: ParityCase, record: dict[str, object], *fields: str, retired_only: bool = False
) -> dict[str, object]:
    return _record(
        await case.call(
            'find_one',
            record['type'],
            [['id', 'is', _id(record)]],
            list(fields),
            retired_only=retired_only,
        )
    )


async def _human(case: ParityCase) -> dict[str, object]:
    return _record(
        await case.call(
            'find_one',
            'HumanUser',
            [['login', 'is', case.human_login]],
            ['name', 'sg_status_list'],
        )
    )


def _link(value: object) -> dict[str, object]:
    record = _record(value)
    return {'type': record['type'], 'id': _id(record)}


async def _parity_search(
    case: ParityCase,
    text: str,
    scopes: Mapping[str, Sequence[Sequence[object]]],
    **options: object,
) -> dict[str, object]:
    return _record(await case.call('text_search', text, scopes, **options))


async def _ready_image(case: ParityCase, record: dict[str, object], field: str) -> str:
    for attempt in range(12):
        value = (await _fields(case, record, field))[field]
        if (
            isinstance(value, str)
            and value.startswith(('http://', 'https://'))
            and '/images/status/transient/' not in value
        ):
            return value
        if attempt < 11:
            await case.advance(5)
    pytest.fail(f'{record["type"]}.{field} did not become ready after 12 reads')


def _records(value: object) -> list[dict[str, object]]:
    items = _items(value)
    assert all(isinstance(item, dict) for item in items)
    return cast('list[dict[str, object]]', items)


def _links(value: object) -> list[dict[str, object]]:
    return [_link(record) for record in _records(value)]


async def _connection_event_records(
    case: ParityCase, rows: list[dict[str, object]], records: list[dict[str, object]]
) -> list[dict[str, object]]:
    identities = _links(records)
    connections: list[dict[str, object]] = []
    for kind, fields in _EVENT_CONNECTION_FIELDS.items():
        candidates = [
            _record(row['entity'])
            for row in rows
            if isinstance(row.get('entity'), dict)
            and cast('dict[str, object]', row['entity']).get('type') == kind
        ]
        missing = list(dict.fromkeys(_id(record) for record in candidates))
        for retired in (False, True):
            if not missing:
                break
            found = _records(
                await case.call(
                    'find', kind, [['id', 'in', *missing]], list(fields), retired_only=retired
                )
            )
            connections.extend(
                connection
                for connection in found
                if all(
                    isinstance(connection[field], dict) and _link(connection[field]) in identities
                    for field in fields
                )
            )
            missing = [
                identity for identity in missing if identity not in [_id(row) for row in found]
            ]
    return connections


def _record_events(
    rows: list[dict[str, object]], records: list[dict[str, object]]
) -> list[dict[str, object]]:
    identities = _links(records)
    return [
        row
        for row in rows
        if isinstance(row.get('entity'), dict) and _link(row['entity']) in identities
    ]


def _row_pairs(
    rows: list[dict[str, object]], record: dict[str, object]
) -> list[tuple[object, object]]:
    return [(row['event_type'], row['attribute_name']) for row in _record_events(rows, [record])]


def _scenario(scenario: Scenario) -> Scenario:
    SCENARIOS.append(scenario)
    return scenario


def _search_token() -> str:
    return ''.join(chr(ord('g') + int(digit, 16)) for digit in uuid4().hex[:8])


def _shot_data(case: ParityCase, suffix: str) -> dict[str, object]:
    return {'code': f'{case.namespace}-{suffix}', 'project': case.project}


def _shot_filters(case: ParityCase) -> list[list[object]]:
    return [
        ['project', 'is', case.project],
        ['code', 'starts_with', case.namespace],
    ]


async def _stream(
    case: ParityCase, record: dict[str, object], **options: object
) -> dict[str, object]:
    await case.advance(2)
    return _record(await case.call('activity_stream_read', record['type'], _id(record), **options))


def _stream_ids(stream: dict[str, object]) -> list[int]:
    return [_id(row) for row in _records(stream['updates'])]


def _strings(value: object) -> list[str]:
    strings: list[str] = []
    for item in _items(value):
        assert isinstance(item, str)
        strings.append(item)
    return strings


async def _schema_values(case: ParityCase, entity_type: str, field: str) -> list[str]:
    schema = _record(await case.call('schema_field_read', entity_type, field))
    properties = _record(_record(schema[field])['properties'])
    if 'valid_values' not in properties:
        return []
    values = _record(properties['valid_values'])['value']
    return _strings(values)


async def _activity_shot(
    case: ParityCase,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    await case.create('Shot', _shot_data(case, 'activity-s0'))
    shot = await case.create('Shot', _shot_data(case, 'activity-s1'))
    prior_status = (await _fields(case, shot, 'sg_status_list'))['sg_status_list']
    created = await _stream(case, shot)
    first = _records(created['updates'])[0]
    first_id = _id(first)
    assert _stream_ids(created) == [first_id]
    assert created['entity_type'] == 'Shot'
    assert created['entity_id'] == _id(shot)
    assert created['latest_update_id'] == first_id
    assert created['earliest_update_id'] == 0
    assert first['update_type'] == 'create'
    assert first['read'] is False
    assert first['meta'] == {'type': 'new_entity', 'entity_type': 'Shot', 'entity_id': _id(shot)}
    assert _record(first['primary_entity']) == {
        **_link(shot),
        'name': shot['code'],
        'status': prior_status,
    }
    audit = await _fields(case, shot, 'created_at', 'created_by')
    assert first['created_at'] == audit['created_at']
    assert first['created_by'] == {**_record(audit['created_by']), 'status': None, 'image': None}
    assert _record(first['created_by'])['type'] == 'ApiUser'
    status = next(
        value
        for value in await _schema_values(case, 'Shot', 'sg_status_list')
        if value != prior_status
    )
    await case.call('update', 'Shot', _id(shot), {'sg_status_list': status})
    changed = await _stream(case, shot)
    update = _records(changed['updates'])[0]
    update_id = _id(update)
    assert update_id > first_id
    assert _stream_ids(changed) == [update_id, first_id]
    assert update['update_type'] == 'update'
    assert update['meta'] == {
        'type': 'attribute_change',
        'attribute_name': 'sg_status_list',
        'entity_type': 'Shot',
        'entity_id': _id(shot),
        'field_data_type': 'status_list',
        'old_value': prior_status,
        'new_value': status,
    }
    assert all(
        _record(row['primary_entity'])['status'] == status for row in _records(changed['updates'])
    )
    return shot, first, update


def _summary_entry(field: str, kind: str, **options: object) -> dict[str, object]:
    return {'field': field, 'type': kind, **options}


async def _parity_groups(
    case: ParityCase,
    entity_type: str,
    filters: Sequence[Sequence[object]],
    summary_field: str,
    groupings: list[dict[str, str]],
    **options: object,
) -> list[dict[str, object]]:
    result = _record(
        await case.call(
            'summarize',
            entity_type,
            filters,
            [_summary_entry(summary_field, 'count')],
            grouping=groupings,
            **options,
        )
    )
    return _records(result['groups'])


async def _parity_summary(
    case: ParityCase,
    entity_type: str,
    filters: Sequence[Sequence[object]],
    field: str,
    kind: str,
    **options: object,
) -> object:
    result = _record(
        await case.call(
            'summarize', entity_type, filters, [_summary_entry(field, kind, **options)]
        )
    )
    return _record(result['summaries'])[field]


def _unique_search_token(used: set[str], *, exclude_q: bool = False) -> str:
    token = _search_token()
    while token in used or (exclude_q and 'q' in token):
        token = _search_token()
    used.add(token)
    return token


async def _select(
    case: ParityCase,
    rows: list[dict[str, object]],
    filters: list[list[object]] | None = None,
    **kwargs: object,
) -> list[int]:
    found = _records(
        await case.call(
            'find',
            rows[0]['type'],
            [['id', 'in', *[_id(row) for row in rows]], *(filters or [])],
            ['id'],
            **kwargs,
        )
    )
    identities = [_id(row) for row in rows]
    return [identities.index(_id(row)) for row in found]


async def _events(
    case: ParityCase,
    bounds: tuple[int, int],
    fields: list[str],
    filters: list[list[object]] | None = None,
) -> list[dict[str, object]]:
    return _records(
        await case.call(
            'find',
            'EventLogEntry',
            [
                ['id', 'greater_than', bounds[0]],
                ['id', 'less_than', bounds[1] + 1],
                *(filters or []),
            ],
            fields,
            order=[{'field_name': 'id', 'direction': 'asc'}],
        )
    )


async def _event_delta(
    case: ParityCase, before: int, records: list[dict[str, object]]
) -> list[dict[str, object]]:
    bounds = (before, await _cursor(case))
    connection_types = [f'Shotgun_{kind}_New' for kind in _EVENT_CONNECTION_FIELDS]
    rows = await _events(
        case,
        bounds,
        ['event_type', 'attribute_name', 'entity'],
        [['event_type', 'not_in', *connection_types]],
    )
    connection_rows = await _events(
        case,
        bounds,
        ['event_type', 'attribute_name', 'meta'],
        [['event_type', 'in', *connection_types]],
    )
    for row in connection_rows:
        value = row.get('meta')
        if isinstance(value, dict):
            meta = cast('dict[str, object]', value)
            if meta.get('entity_type') in _EVENT_CONNECTION_FIELDS:
                rows.append(
                    {**row, 'entity': {'type': meta['entity_type'], 'id': meta['entity_id']}}
                )
    connections = await _connection_event_records(case, rows, records)
    return sorted(_record_events(rows, [*records, *connections]), key=_id)


async def _paired_batch(
    case: ParityCase,
    operations: list[tuple[dict[str, object], str, object, str | None]],
    expected: list[tuple[dict[str, object] | str, str | None]],
) -> None:
    before = await _cursor(case)
    requests: list[dict[str, object]] = []
    for record, field, value, mode in operations:
        request: dict[str, object] = {
            'request_type': 'update',
            'entity_type': record['type'],
            'entity_id': _id(record),
            'data': {field: value},
        }
        if mode is not None:
            request['multi_entity_update_modes'] = {field: mode}
        requests.append(request)
    await case.call('batch', requests)
    records = [target for target, _ in expected if isinstance(target, dict)]
    rows = await _event_delta(case, before, records)
    assert len(rows) == len(expected)
    for row, (target, field) in zip(rows, expected, strict=True):
        kind = target if isinstance(target, str) else str(target['type'])
        assert (row['event_type'], row['attribute_name']) == (
            f'Shotgun_{kind}_{"New" if field is None else "Change"}',
            field,
        )
        if isinstance(target, dict):
            assert _link(row['entity']) == _link(target)


async def _relation(
    case: ParityCase,
    rows: list[dict[str, object]],
    field: str,
    relation: str,
    *values: object,
) -> list[int]:
    return await _select(case, rows, [[field, relation, *values]])


async def _scoped_state(case: ParityCase, record: dict[str, object], *, retired: bool) -> None:
    assert await _select(case, [record]) == ([] if retired else [0])
    assert await _select(case, [record], retired_only=True) == ([0] if retired else [])


def _upload_file(directory: Path, case: ParityCase, label: str, data: bytes) -> str:
    extension = 'png' if data.startswith(b'\x89PNG') else 'txt'
    path = directory / f'{case.namespace}-{label}.{extension}'
    path.write_bytes(data)
    return str(path)


async def _upload_note(case: ParityCase, note: dict[str, object], label: str) -> dict[str, object]:
    with TemporaryDirectory() as directory:
        identity = await case.call(
            'upload', 'Note', _id(note), _upload_file(Path(directory), case, label, b'x' * 40)
        )
    assert isinstance(identity, int)
    return {'type': 'Attachment', 'id': identity}


async def _valid_values(case: ParityCase, field: str) -> list[str]:
    schema = _record(await case.call('schema_field_read', 'Asset', field))
    properties = _record(_record(schema[field])['properties'])
    values = _record(properties['valid_values'])['value']
    return _strings(values)


def _whole_second(value: object) -> datetime:
    assert isinstance(value, datetime)
    assert value.tzinfo is not None
    return value.astimezone(UTC).replace(microsecond=0)


async def _anchor(case: ParityCase) -> datetime:
    shot = await case.create('Shot', _shot_data(case, 'clock-anchor'))
    return _whole_second((await _fields(case, shot, 'created_at'))['created_at'])


async def _calendar_anchor(case: ParityCase, *, months: bool = False) -> datetime:
    anchor = await _anchor(case)
    midnight = anchor.replace(hour=0, minute=0, second=0)
    if min(anchor - midnight, midnight + timedelta(days=1) - anchor) < timedelta(minutes=5):
        pytest.skip('relative-date anchor is within five minutes of a UTC day boundary')
    if months and anchor.day > 28:
        pytest.skip('calendar-month relative-date anchor is after day 28')
    return anchor


@_scenario
async def asset_parents_mirror(case: ParityCase) -> None:
    parents = [await case.create('Asset', _shot_data(case, f'parent-{i}')) for i in range(2)]
    child = await case.create(
        'Asset', {**_shot_data(case, 'child'), 'parents': [_link(parent) for parent in parents]}
    )
    for parent in parents:
        assert _links((await _fields(case, parent, 'assets'))['assets']) == [_link(child)]
    await case.call('update', 'Asset', _id(child), {'parents': [_link(parents[1])]})
    assert _links((await _fields(case, parents[0], 'assets'))['assets']) == []
    assert _links((await _fields(case, parents[1], 'assets'))['assets']) == [_link(child)]


@_scenario
async def create_keeps_supplied_audit_fields(case: ParityCase) -> None:
    human = _record(await case.call('find_one', 'HumanUser', [['sg_status_list', 'is', 'act']]))
    now = datetime.now(tz=UTC).replace(microsecond=0)
    supplied = {
        'created_at': now - timedelta(days=1),
        'updated_at': now - timedelta(days=2),
        'created_by': _link(human),
    }
    asset = await case.create('Asset', {**_shot_data(case, 'audit'), **supplied})
    found = await _fields(case, asset, *supplied)
    assert _whole_second(found['created_at']) == supplied['created_at']
    assert _whole_second(found['updated_at']) == supplied['updated_at']
    assert _link(found['created_by']) == _link(human)
    overridden = await case.create(
        'Asset', {**_shot_data(case, 'actor'), 'updated_by': _link(human)}
    )
    assert (
        _link((await _fields(case, overridden, 'updated_by'))['updated_by'])['type'] == 'ApiUser'
    )


@_scenario
async def create_returns_supplied_and_requested_fields(case: ParityCase) -> None:
    for index, requested in enumerate((['sg_status_list'], ['description'])):
        data = {**_shot_data(case, f'explicit-{index}'), 'description': f'{case.namespace}-text'}
        created = await case.create('Asset', data, return_fields=requested)
        assert set(created) == {'type', 'id', *data, *requested}
        assert created['type'] == 'Asset'
        assert _id(created) > 0
        assert created['code'] == data['code']
        assert created['description'] == data['description']
        assert _link(created['project']) == _link(case.project)
        if 'sg_status_list' in requested:
            stored = await _fields(case, created, 'sg_status_list')
            assert created['sg_status_list'] == stored['sg_status_list']
    schema = _record(await case.call('schema_field_read', 'Asset'))
    unknown = f'{case.namespace}_unknown_field'
    assert unknown not in schema
    created = await case.create(
        'Asset', _shot_data(case, 'unknown-result'), return_fields=['code', unknown]
    )
    assert set(created) == {'type', 'id', 'code', 'project'}
    assert unknown not in created


@_scenario
async def create_returns_supplied_fields(case: ParityCase) -> None:
    data = {**_shot_data(case, 'created'), 'description': f'{case.namespace}-description'}
    created = await case.create('Shot', data)
    assert created['type'] == 'Shot'
    identity = _id(created)
    assert created['code'] == data['code']
    assert created['description'] == data['description']
    found = _record(
        await case.call(
            'find_one',
            'Shot',
            [['id', 'is', identity], *_shot_filters(case)],
            ['code', 'description'],
        )
    )
    assert found['id'] == identity
    assert found['code'] == created['code']
    assert found['description'] == created['description']


@_scenario
async def default_order_is_ascending_id(case: ParityCase) -> None:
    created = [await case.create('Shot', _shot_data(case, suffix)) for suffix in ('z', 'y', 'x')]
    records = _records(await case.call('find', 'Shot', _shot_filters(case), ['code']))
    assert [_id(row) for row in records] == sorted(_id(row) for row in created)
    ordered = _records(
        await case.call(
            'find',
            'Shot',
            _shot_filters(case),
            ['code'],
            order=[{'field_name': 'code', 'direction': 'asc'}],
        )
    )
    codes = _strings([row['code'] for row in created])
    assert [row['code'] for row in ordered] == sorted(codes)


async def delete_fields(case: ParityCase, entity_type: str, fields: Sequence[str]) -> None:
    failed_fields: list[str] = []
    first_error: Exception | None = None
    for field in reversed(fields):
        try:
            result = await case.call('schema_field_delete', entity_type, field)
        except (Fault, ShotgunError, ProtocolError, MockgunError) as error:
            failed_fields.append(field)
            if first_error is None:
                first_error = error
        else:
            if result is not True:
                failed_fields.append(field)
    if failed_fields:
        raise AssertionError(
            f'Failed to delete {entity_type} fields: {", ".join(failed_fields)}'
        ) from first_error


@_scenario
async def failed_batch_changes_nothing(case: ParityCase) -> None:
    assets = [await case.create('Asset', _shot_data(case, f'batch-asset-{i}')) for i in range(2)]
    shot = await case.create(
        'Shot', {**_shot_data(case, 'batch-existing'), 'assets': [_link(assets[0])]}
    )
    shot_before = _links((await _fields(case, shot, 'assets'))['assets'])
    assets_before = [_links((await _fields(case, asset, 'shots'))['shots']) for asset in assets]
    created_data = _shot_data(case, 'batch-rolled-back')
    with pytest.raises(Fault):
        await case.call(
            'batch',
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': _id(shot),
                    'data': {'assets': [_link(assets[1])]},
                },
                {'request_type': 'create', 'entity_type': 'Shot', 'data': created_data},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': -1,
                    'data': {'description': case.namespace},
                },
            ],
        )
    assert (
        await case.call(
            'find_one', 'Shot', [*_shot_filters(case), ['code', 'is', created_data['code']]]
        )
        is None
    )
    assert _links((await _fields(case, shot, 'assets'))['assets']) == shot_before
    assert [
        _links((await _fields(case, asset, 'shots'))['shots']) for asset in assets
    ] == assets_before


@_scenario
async def invalid_list_values_fault(case: ParityCase) -> None:
    schema = _record(await case.call('schema_field_read', 'Asset'))
    asset = await case.create('Asset', _shot_data(case, 'invalid-values'))
    for field in ('sg_status_list', 'sg_asset_type'):
        properties = _record(_record(schema[field])['properties'])
        valid = _record(properties['valid_values'])['value']
        assert isinstance(valid, list)
        invalid = f'{case.namespace}-invalid-{field}'
        assert invalid not in valid
        before = await _fields(case, asset, field)
        with pytest.raises(Fault, match=rf'API create\(\).*Update failed for \[Asset\.{field}\]'):
            await case.create('Asset', {**_shot_data(case, field), field: invalid})
        with pytest.raises(Fault, match=rf'API update\(\).*Update failed for \[Asset\.{field}\]'):
            await case.call('update', 'Asset', _id(asset), {field: invalid})
        assert await _fields(case, asset, field) == before


async def lifecycle_rows(
    case: ParityCase,
    record: dict[str, object],
    operation: str,
) -> list[dict[str, object]]:
    before = await _cursor(case)
    assert await case.call(operation, record['type'], _id(record)) is True
    after = await _cursor(case)
    kind = 'Retirement' if operation == 'delete' else 'Revival'
    rows = await _events(
        case,
        (before, after),
        ['event_type', 'attribute_name'],
        [
            [
                'event_type',
                'in',
                f'Shotgun_{record["type"]}_Change',
                f'Shotgun_{record["type"]}_{kind}',
            ],
        ],
    )
    rows = [row for row in rows if row['attribute_name'] in ('retirement_date', None)]
    assert [(row['event_type'], row['attribute_name']) for row in rows] == [
        (f'Shotgun_{record["type"]}_Change', 'retirement_date'),
        (f'Shotgun_{record["type"]}_{kind}', None),
    ]
    if operation == 'revive':
        meta = _record((await _fields(case, rows[1], 'meta'))['meta'])
        assert meta['class_name'] == record['type']
        assert meta['display_name'] == record['code']
    return rows


@_scenario
async def lifecycle_touches_updated_at(case: ParityCase) -> None:
    asset = await case.create('Asset', _shot_data(case, 'lifecycle-asset'))
    shot = await case.create('Shot', _shot_data(case, 'lifecycle-shot'))
    previous = _whole_second((await _fields(case, asset, 'updated_at'))['updated_at'])
    for method in ('delete', 'revive', 'update'):
        await case.advance(2)
        if method == 'update':
            await case.call('update', 'Shot', _id(shot), {'assets': [_link(asset)]})
        else:
            assert await case.call(method, 'Asset', _id(asset)) is True
        found = await _fields(
            case, asset, 'updated_at', 'updated_by', retired_only=method == 'delete'
        )
        current = _whole_second(found['updated_at'])
        assert current > previous
        assert _link(found['updated_by'])['type'] == 'ApiUser'
        previous = current


@_scenario
async def limit_and_page(case: ParityCase) -> None:
    created = [await case.create('Shot', _shot_data(case, f'page-{index}')) for index in range(5)]
    identities = [_id(row) for row in created]
    for page, expected in ((1, identities[:2]), (2, identities[2:4]), (3, identities[4:])):
        kwargs = {'limit': 2} if page == 1 else {'limit': 2, 'page': page}
        rows = _records(await case.call('find', 'Shot', _shot_filters(case), ['id'], **kwargs))
        assert [_id(row) for row in rows] == expected


@_scenario
async def paired_assets_and_shots(case: ParityCase) -> None:
    asset = await case.create('Asset', _shot_data(case, 'paired-asset'))
    shot = await case.create('Shot', _shot_data(case, 'paired-shot'))
    for source, field, target, mirror in (
        (shot, 'assets', asset, 'shots'),
        (asset, 'shots', shot, 'assets'),
    ):
        await case.call('update', source['type'], _id(source), {field: [_link(target)]})
        assert _links((await _fields(case, target, mirror))[mirror]) == [_link(source)]
        await case.call('update', source['type'], _id(source), {field: []})
        assert _links((await _fields(case, target, mirror))[mirror]) == []


def parity_schema() -> dict[str, object]:
    audit = {
        'created_at': _field('date_time'),
        'updated_at': _field('date_time'),
        'created_by': _field('entity', ['HumanUser', 'ApiUser']),
        'updated_by': _field('entity', ['HumanUser', 'ApiUser']),
    }
    schema = {
        'ApiUser': {'firstname': _field('text')},
        'Asset': {
            'code': _field('text'),
            'description': _field('text'),
            'project': _field('entity', ['Project']),
            'sg_status_list': _field(
                'status_list', valid_values=['wtg', 'ip', 'fin'], default='wtg'
            ),
            'sg_asset_type': _field('list', valid_values=['Character', 'Prop', 'Environment']),
            'shots': _field('multi_entity', ['Shot']),
            'parents': _field('multi_entity', ['Asset']),
            'assets': _field('multi_entity', ['Asset']),
            **audit,
        },
        'Cut': {
            'cut_items': _field('multi_entity', ['CutItem']),
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'entity': _field('entity', ['Sequence']),
            'revision_number': _field('number'),
            **audit,
        },
        'CutItem': {
            **audit,
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'cut': _field('entity', ['Cut']),
            'shot': _field('entity', ['Shot']),
            'version': _field('entity', ['Version']),
        },
        'Note': {
            **audit,
            'tasks': _field('multi_entity', ['Task']),
            'subject': _field('text'),
            'content': _field('text'),
            'cached_display_name': _field('text'),
            'project': _field('entity', ['Project']),
            'note_links': _field(
                'multi_entity',
                ['Shot', 'Asset', 'Sequence', 'Version', 'Department', 'Group', 'CustomEntity03'],
            ),
        },
        'Reply': {'content': _field('text'), 'entity': _field('entity', ['Note'])},
        'Task': {
            **audit,
            'content': _field('text'),
            'project': _field('entity', ['Project']),
            'entity': _field('entity', ['Shot', 'Asset', 'Sequence']),
            'start_date': _field('date'),
            'est_in_mins': _field('duration'),
        },
        'Version': {
            'user': _field('entity', ['HumanUser', 'ApiUser']),
            'playlists': _field('multi_entity', ['Playlist']),
            'published_files': _field('multi_entity', ['PublishedFile']),
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'entity': _field('entity', ['Shot', 'Asset', 'Sequence']),
            'sg_task': _field('entity', ['Task']),
            'sg_movie_aspect_ratio': _field('float'),
            'media_center_import_time': _field('date_time'),
            **audit,
        },
        'EventLogEntry': {
            'attribute_name': _field('text'),
            'meta': _field('serializable'),
            'entity': _field('entity'),
            'project': _field('entity', ['Project']),
            'user': _field('entity'),
            'created_at': _field('date_time'),
            'session_uuid': _field('text'),
            'cached_display_name': _field('text'),
            'image': _field('image'),
            'audit_trail': _field('serializable'),
            'description': _field('text'),
            'event_type': _field('text'),
        },
        'HumanUser': {
            'login': _field('text'),
            'name': _field('text'),
            'sg_status_list': _field('status_list'),
        },
        'Project': {'name': _field('text'), 'archived': _field('checkbox', default=False)},
        'Sequence': {
            'description': _field('text'),
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'shots': _field('multi_entity', ['Shot']),
            **audit,
        },
        'Shot': {
            'tasks': _field('multi_entity', ['Task']),
            'notes': _field('multi_entity', ['Note']),
            'open_notes_count': _field('summary'),
            'sg_cut_order': _field('number'),
            'cached_display_name': _field('text'),
            'assets': _field('multi_entity', ['Asset']),
            'code': _field('text'),
            'description': _field('text'),
            'project': _field('entity', ['Project']),
            'sg_status_list': _field('status_list'),
            'sg_sequence': _field('entity', ['Sequence']),
            **audit,
        },
        'AssetShotConnection': {
            'asset': _field('entity', ['Asset']),
            'shot': _field('entity', ['Shot']),
        },
        'PublishedFile': {
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'entity': _field('entity', ['Shot', 'Asset']),
            'version': _field('entity', ['Version']),
            'task': _field('entity', ['Task']),
            **audit,
        },
        'Playlist': {
            'code': _field('text'),
            'project': _field('entity', ['Project']),
            'versions': _field('multi_entity', ['Version']),
            **audit,
        },
    }
    additions = {
        'Attachment': {
            **audit,
            'filename': _field('text'),
            'original_fname': _field('text'),
            'display_name': _field('text'),
            'cached_display_name': _field('text'),
            'this_file': _field('url'),
            'attachment_links': _field(
                'multi_entity', ['Note', 'Reply', 'Version', 'Asset', 'Shot']
            ),
            'project': _field('entity', ['Project']),
        },
        'Note': {
            'user': _field('entity', ['HumanUser', 'ApiUser']),
            'sg_status_list': _field(
                'status_list', valid_values=['opn', 'ip', 'clsd'], default='opn'
            ),
            'attachments': _field('multi_entity', ['Attachment']),
            'replies': _field('multi_entity', ['Reply']),
        },
        'Reply': {**audit, 'user': _field('entity', ['HumanUser', 'ApiUser'])},
        'Task': {
            'sg_status_list': _field(
                'status_list', valid_values=['wtg', 'ip', 'fin'], default='wtg'
            ),
            'color': _field('color'),
            'due_date': _field('date'),
            'sg_versions': _field('multi_entity', ['Version']),
        },
        'Sequence': {
            'sg_status_list': _field('status_list', valid_values=['ip', 'fin'], default='ip')
        },
        'Cut': {
            'sg_status_list': _field('status_list', valid_values=['act', 'na'], default='act')
        },
        'Shot': {
            'image': _field('image'),
            'filmstrip_image': _field('image'),
            'sg_cut_in': _field('number'),
            'sg_shot_type': _field('list', valid_values=['Feature', 'Commercial']),
            'sg_status_list': _field('status_list', valid_values=['wtg', 'ip', 'fin']),
        },
        'Asset': {'image': _field('image'), 'filmstrip_image': _field('image')},
        'Version': {
            'image': _field('image'),
            'filmstrip_image': _field('image'),
            'sg_uploaded_movie': _field('url'),
            'sg_uploaded_movie_transcoding_status': _field('number'),
            'sg_movie_has_slate': _field('checkbox', default=False),
        },
        'Playlist': {'description': _field('text')},
        'Project': {'last_accessed_by_current_user': _field('date_time')},
        'HumanUser': {'image': _field('image')},
        'AssetAssetConnection': {
            'asset': _field('entity', ['Asset']),
            'parent': _field('entity', ['Asset']),
        },
        'PlaylistVersionConnection': {
            'playlist': _field('entity', ['Playlist']),
            'version': _field('entity', ['Version']),
        },
        'WorkDayRule': {
            **audit,
            'start_date': _field('date'),
            'end_date': _field('date'),
            'working': _field('checkbox', default=False),
            'description': _field('text'),
            'project': _field('entity', ['Project']),
            'user': _field('entity', ['HumanUser']),
            'precedence': _field('number'),
            'day_of_week': _field('number'),
        },
        'Department': {
            'code': _field('text'),
            'name': _field('text'),
            'cached_display_name': _field('text'),
        },
        'Group': {'code': _field('text'), 'cached_display_name': _field('text')},
        'CustomEntity03': {
            'code': _field('text'),
            'description': _field('text'),
            'project': _field('entity', ['Project']),
            'cached_display_name': _field('text'),
        },
        'Ticket': {
            'title': _field('text'),
            'description': _field('text'),
            'project': _field('entity', ['Project']),
            'tickets': _field('multi_entity', ['Ticket']),
            'cached_display_name': _field('text'),
        },
    }
    for entity_type, fields in additions.items():
        schema.setdefault(entity_type, {}).update(fields)
    return cast('dict[str, object]', schema)


def parity_schema_entity() -> dict[str, object]:
    return {entity_type: {'name': {'value': entity_type}} for entity_type in parity_schema()}


@_scenario
async def remove_mode_removes_given_entities(case: ParityCase) -> None:
    assets = [await case.create('Asset', _shot_data(case, f'remove-{i}')) for i in range(3)]
    shot = await case.create(
        'Shot', {**_shot_data(case, 'remove-shot'), 'assets': [_link(asset) for asset in assets]}
    )
    await case.call(
        'update',
        'Shot',
        _id(shot),
        {'assets': [_link(asset) for asset in assets[:2]]},
        multi_entity_update_modes={'assets': 'remove'},
    )
    assert _links((await _fields(case, shot, 'assets'))['assets']) == [_link(assets[2])]
    for index, asset in enumerate(assets):
        assert _links((await _fields(case, asset, 'shots'))['shots']) == (
            [_link(shot)] if index == 2 else []
        )


@_scenario
async def script_actor_is_api_user(case: ParityCase) -> None:
    asset = await case.create('Asset', _shot_data(case, 'script-actor'))
    actor = _link((await _fields(case, asset, 'created_by'))['created_by'])
    assert actor['type'] == 'ApiUser'
    assert (await _fields(case, actor, 'firstname'))['firstname'] == case.script_name


@_scenario
async def sequence_and_shots_mirror(case: ParityCase) -> None:
    sequences = [
        await case.create('Sequence', _shot_data(case, f'sequence-{i}')) for i in range(2)
    ]
    shot = await case.create(
        'Shot', {**_shot_data(case, 'sequence-shot'), 'sg_sequence': _link(sequences[0])}
    )
    assert _links((await _fields(case, sequences[0], 'shots'))['shots']) == [_link(shot)]
    second_shot = await case.create('Shot', _shot_data(case, 'sequence-second'))
    await case.call(
        'update',
        'Sequence',
        _id(sequences[0]),
        {'shots': [_link(second_shot)]},
        multi_entity_update_modes={'shots': 'add'},
    )
    assert _link((await _fields(case, second_shot, 'sg_sequence'))['sg_sequence']) == _link(
        sequences[0]
    )
    await case.call('update', 'Shot', _id(shot), {'sg_sequence': _link(sequences[1])})
    assert _links((await _fields(case, sequences[0], 'shots'))['shots']) == [_link(second_shot)]
    assert _links((await _fields(case, sequences[1], 'shots'))['shots']) == [_link(shot)]
    await case.call('update', 'Shot', _id(shot), {'sg_sequence': None})
    assert _links((await _fields(case, sequences[1], 'shots'))['shots']) == []
    assert (await _fields(case, shot, 'sg_sequence'))['sg_sequence'] is None


@_scenario
async def unlisted_relation_faults(case: ParityCase) -> None:
    with pytest.raises(Fault):
        await case.call('find', 'Shot', [*_shot_filters(case), ['code', 'between', 'a', 'b']])


@_scenario
async def update_rejects_audit_fields(case: ParityCase) -> None:
    human = _record(await case.call('find_one', 'HumanUser', [['sg_status_list', 'is', 'act']]))
    asset = await case.create('Asset', _shot_data(case, 'immutable-audit'))
    fields = ('created_at', 'updated_at', 'created_by', 'updated_by')
    before = await _fields(case, asset, *fields)
    for field in fields:
        replacement = (
            _whole_second(before[field]) - timedelta(days=1)
            if field.endswith('_at')
            else _link(human)
        )
        if field.endswith('_by'):
            assert _link(before[field]) != replacement
        with pytest.raises(Fault, match=rf'Asset\.{field} is editable on create only'):
            await case.call('update', 'Asset', _id(asset), {field: replacement})
        after = await _fields(case, asset, *fields)
        for checked in fields:
            if checked.endswith('_at'):
                assert _whole_second(after[checked]) == _whole_second(before[checked])
            else:
                assert _link(after[checked]) == _link(before[checked])


@_scenario
async def update_returns_dictionary(case: ParityCase) -> None:
    created = await case.create('Shot', _shot_data(case, 'update'))
    description = f'{case.namespace}-updated'
    updated = _record(
        await case.call('update', 'Shot', _id(created), {'description': description})
    )
    assert updated['type'] == 'Shot'
    assert updated['id'] == created['id']
    assert updated['description'] == description
    found = _record(
        await case.call(
            'find_one',
            'Shot',
            [['id', 'is', _id(created)], *_shot_filters(case)],
            ['description'],
        )
    )
    assert found['description'] == description


@_scenario
async def retire_and_revive(case: ParityCase) -> None:
    created = await case.create('Shot', _shot_data(case, 'retire'))
    identity = _id(created)
    filters = [['id', 'is', identity], *_shot_filters(case)]
    assert await case.call('delete', 'Shot', identity) is True
    assert _records(await case.call('find', 'Shot', filters, ['id'])) == []
    retired = _records(await case.call('find', 'Shot', filters, ['id'], retired_only=True))
    assert [_id(row) for row in retired] == [identity]
    assert await case.call('revive', 'Shot', identity) is True
    revived = _records(await case.call('find', 'Shot', filters, ['id']))
    assert [_id(row) for row in revived] == [identity]


@_scenario
async def batch_results(case: ParityCase) -> None:
    updated = await case.create('Shot', _shot_data(case, 'batch-update'))
    deleted = await case.create('Shot', _shot_data(case, 'batch-delete'))
    code = f'{case.namespace}-batch-create'
    description = f'{case.namespace}-batch-updated'
    results = await case.call(
        'batch',
        [
            {
                'request_type': 'create',
                'entity_type': 'Shot',
                'data': {'code': code, 'project': case.project},
                'return_fields': ['code'],
            },
            {
                'request_type': 'update',
                'entity_type': 'Shot',
                'entity_id': _id(updated),
                'data': {'description': description},
            },
            {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': _id(deleted)},
        ],
    )
    assert isinstance(results, list)
    items = cast('list[object]', results)
    assert len(items) == 3
    created_result = _record(items[0])
    assert created_result['type'] == 'Shot'
    assert created_result['code'] == code
    assert _id(created_result) > 0
    updated_result = _record(items[1])
    assert updated_result['id'] == updated['id']
    assert updated_result['description'] == description
    assert items[2] is True


@_scenario
async def multi_entity_update_modes(case: ParityCase) -> None:
    assets = [
        await case.create(
            'Asset',
            {
                'code': f'{case.namespace}-asset-{index}',
                'project': case.project,
            },
        )
        for index in range(2)
    ]
    shot = await case.create(
        'Shot',
        {
            **_shot_data(case, 'assets'),
            'assets': [assets[0]],
        },
    )
    filters = [['id', 'is', _id(shot)], *_shot_filters(case)]

    async def asset_ids() -> set[int]:
        found = _record(await case.call('find_one', 'Shot', filters, ['assets']))
        linked = _records(found['assets'])
        return {_id(asset) for asset in linked}

    assert await asset_ids() == {_id(assets[0])}
    await case.call(
        'update',
        'Shot',
        _id(shot),
        {'assets': [assets[1]]},
        multi_entity_update_modes={'assets': 'add'},
    )
    assert await asset_ids() == {_id(assets[0]), _id(assets[1])}
    await case.call(
        'update',
        'Shot',
        _id(shot),
        {'assets': [assets[0]]},
        multi_entity_update_modes={'assets': 'remove'},
    )
    assert await asset_ids() == {_id(assets[1])}


@_scenario
async def missing_entity_update_faults(case: ParityCase) -> None:
    with pytest.raises(Fault):
        await case.call('update', 'Shot', -1, {'description': case.namespace})


@_scenario
async def unknown_filter_field_faults(case: ParityCase) -> None:
    created = await case.create('Shot', _shot_data(case, 'unknown-field'))
    unknown_field = f'{case.namespace}_unknown_field'
    filters = [['id', 'is', _id(created)], *_shot_filters(case)]
    with pytest.raises(Fault):
        await case.call('find', 'Shot', [*filters, [unknown_field, 'is', 'invalid']], ['id'])


@_scenario
async def link_names_follow_display_fields(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'named-shot'))
    version = await case.create(
        'Version', {**_shot_data(case, 'named-version'), 'entity': _link(shot)}
    )
    note = await case.create(
        'Note', {'subject': case.namespace, 'project': case.project, 'note_links': [_link(shot)]}
    )
    for renamed in (False, True):
        code = f'{case.namespace}-renamed' if renamed else shot['code']
        if renamed:
            await case.call('update', 'Shot', _id(shot), {'code': code})
        assert _record((await _fields(case, version, 'entity'))['entity'])['name'] == code
        assert _records((await _fields(case, note, 'note_links'))['note_links'])[0]['name'] == code


@_scenario
async def note_display_names(case: ParityCase) -> None:
    subject = case.namespace
    content = 'body'
    long_content = (case.namespace + '-').ljust(300, 'g')
    combined_content = 'g' * (83 - len(subject) - 3)
    samples = [
        ({'subject': subject}, subject),
        ({'content': content}, content),
        ({'subject': subject, 'content': content}, subject + ' - ' + content),
        (
            {'subject': (case.namespace + '-').ljust(120, 'g')},
            (case.namespace + '-').ljust(120, 'g'),
        ),
        ({'subject': subject, 'content': combined_content}, subject + ' - ' + combined_content),
        ({'content': long_content}, long_content[:255]),
        ({'subject': subject + '\nline'}, subject + '\nline'),
        ({'content': subject + '\nline'}, subject + '\nline'),
    ]
    for index, (data, expected) in enumerate(samples):
        note = await case.create('Note', {'project': case.project, **data})
        found = await _fields(case, note, 'cached_display_name', 'content')
        assert found['cached_display_name'] == expected
        assert found['content'] == data.get('content')
        reply = await case.create(
            'Reply', {'content': f'{case.namespace}-reply-{index}', 'entity': _link(note)}
        )
        assert _record((await _fields(case, reply, 'entity'))['entity'])['name'] == expected


@_scenario
async def task_null_content_name(case: ParityCase) -> None:
    task = await case.create('Task', {'content': case.namespace, 'project': case.project})
    await case.call('update', 'Task', _id(task), {'content': None})
    assert (await _fields(case, task, 'content'))['content'] is None
    version = await case.create(
        'Version', {**_shot_data(case, 'null-task'), 'sg_task': _link(task)}
    )
    assert _record((await _fields(case, version, 'sg_task'))['sg_task'])['name'] == '-'


@_scenario
async def supplied_cached_display_name_is_ignored(case: ParityCase) -> None:
    shot = await case.create(
        'Shot', {**_shot_data(case, 'cache'), 'cached_display_name': 'supplied-create'}
    )
    assert (await _fields(case, shot, 'cached_display_name'))['cached_display_name'] == shot[
        'code'
    ]
    await case.call('update', 'Shot', _id(shot), {'cached_display_name': 'supplied-update'})
    assert (await _fields(case, shot, 'cached_display_name'))['cached_display_name'] == shot[
        'code'
    ]


@_scenario
async def empty_text_is_stored_as_null(case: ParityCase) -> None:
    rows = [
        await case.create('Asset', {**_shot_data(case, f'empty-{i}'), **data})
        for i, data in enumerate(({'description': ''}, {}, {'description': 'populated'}))
    ]
    assert (await _fields(case, rows[0], 'description'))['description'] is None
    assert await _relation(case, rows, 'description', 'is', '') == [0, 1]


@_scenario
async def deep_return_fields(case: ParityCase) -> None:
    sequence = await case.create('Sequence', _shot_data(case, 'deep-sequence'))
    shot = await case.create(
        'Shot', {**_shot_data(case, 'deep-shot'), 'sg_sequence': _link(sequence)}
    )
    absent = await case.create('Shot', _shot_data(case, 'no-sequence'))
    invalid = [
        'sg_sequence.Sequence.unknown_leaf',
        'code.Shot.code',
        'assets.Asset.code',
        'sg_sequence.Asset.code',
        'sg_sequence.UnknownType.code',
    ]
    found = await _fields(case, shot, *invalid)
    assert all(field not in found for field in invalid)
    assert (await _fields(case, absent, 'sg_sequence.Sequence.code'))[
        'sg_sequence.Sequence.code'
    ] is None
    version = await case.create(
        'Version', {**_shot_data(case, 'deep-version'), 'entity': _link(shot)}
    )
    path = 'entity.Shot.sg_sequence.Sequence.code'
    assert (await _fields(case, version, path))[path] == sequence['code']


@_scenario
async def ordered_relations_skip_nulls(case: ParityCase) -> None:
    rows = [
        await case.create('Shot', {**_shot_data(case, f'ordered-{i}'), 'sg_cut_order': value})
        for i, value in enumerate((None, 1, 5, 10))
    ]
    for relation, operands, expected in (
        ('greater_than', [5], [3]),
        ('less_than', [5], [1]),
        ('between', [1, 5], [1, 2]),
        ('between', [5, 1], [1, 2]),
        ('between', [None, 5], []),
        ('between', [5, None], []),
    ):
        assert await _relation(case, rows, 'sg_cut_order', relation, *operands) == expected


@_scenario
async def between_endpoints(case: ParityCase) -> None:
    float_values = [1.5, -0.5, 2.25, 10.0, 0.1, 0.2, 1.5, None]
    times = [
        datetime.fromisoformat(value).replace(tzinfo=UTC)
        for value in (
            '2025-12-31T23:30',
            '2026-01-03T23:30',
            '2026-01-04T00:30',
            '2026-01-05T00:30',
            '2026-01-05T12:00',
            '2026-03-31T23:30',
            '2026-04-01T00:30',
        )
    ]
    versions = [
        await case.create(
            'Version',
            {
                **_shot_data(case, f'endpoints-{i}'),
                'sg_movie_aspect_ratio': value,
                'media_center_import_time': times[i] if i < len(times) else None,
            },
        )
        for i, value in enumerate(float_values)
    ]
    tasks = [
        await case.create(
            'Task',
            {
                'content': f'{case.namespace}-duration-{i}',
                'project': case.project,
                'est_in_mins': value,
            },
        )
        for i, value in enumerate((30, 60, 90, 120, None))
    ]
    end = datetime(2026, 1, 4, 12, tzinfo=UTC)
    assert await _relation(case, versions, 'sg_movie_aspect_ratio', 'between', 2.0, 0.15) == [
        0,
        5,
        6,
    ]
    assert await _relation(
        case,
        versions,
        'media_center_import_time',
        'between',
        end,
        datetime(2026, 1, 1, tzinfo=UTC),
    ) == [1, 2]
    for rows, field, value in (
        (versions, 'sg_movie_aspect_ratio', 1.0),
        (versions, 'media_center_import_time', end),
        (tasks, 'est_in_mins', 75),
    ):
        assert await _relation(case, rows, field, 'between', None, value) == []
        assert await _relation(case, rows, field, 'between', value, None) == []


@_scenario
async def membership_lists(case: ParityCase) -> None:
    rows = [
        await case.create('Shot', {**_shot_data(case, f'member-{i}'), 'sg_cut_order': value})
        for i, value in enumerate((None, 1, 5, 10))
    ]
    assert await _relation(case, rows, 'sg_cut_order', 'in', 1, 5) == [1, 2]
    assert await _relation(case, rows, 'sg_cut_order', 'not_in', 1, 5) == [0, 3]
    for relation, values in (('in', [1, None]), ('not_in', [1, None]), ('in', [])):
        with pytest.raises(Fault):
            await _relation(case, rows, 'sg_cut_order', relation, *values)


@_scenario
async def operand_type_mismatches_fault(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'operand'))
    for field, value in (('sg_cut_order', 5.0), ('description', 5)):
        with pytest.raises(Fault):
            await _relation(case, [shot], field, 'is', value)


@_scenario
async def text_and_list_case(case: ParityCase) -> None:
    for field in ('description', 'sg_asset_type', 'sg_status_list'):
        value = (
            f'{case.namespace}-MiXeD'
            if field == 'description'
            else (await _valid_values(case, field))[0]
        )
        assert value.swapcase() != value
        rows = [
            await case.create('Asset', {**_shot_data(case, f'case-{field}-{i}'), field: current})
            for i, current in enumerate((value, None))
        ]
        assert await _relation(case, rows, field, 'is', value.swapcase()) == [0]
        for relation, expected in (('is_not', [1]), ('in', [0]), ('not_in', [1])):
            assert await _relation(case, rows, field, relation, value.swapcase()) == expected


@_scenario
async def entity_relations(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'entity-shot'))
    asset = await case.create('Asset', _shot_data(case, 'entity-asset'))
    rows = [
        await case.create(
            'Task',
            {'content': f'{case.namespace}-entity-{i}', 'project': case.project, 'entity': target},
        )
        for i, target in enumerate((_link(shot), _link(asset), None))
    ]
    for relation, values, expected in (
        ('in', [_link(shot), _link(asset)], [0, 1]),
        ('not_in', [_link(shot), _link(asset)], [2]),
        ('name_is', [str(shot['code']).swapcase()], [0]),
        ('name_not_contains', ['ENTITY-SHOT'], [1, 2]),
        ('type_is', ['Shot'], [0]),
        ('type_is_not', ['Shot'], [1, 2]),
    ):
        assert await _relation(case, rows, 'entity', relation, *values) == expected


@_scenario
async def multi_entity_relations(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'multi-shot'))
    asset = await case.create('Asset', _shot_data(case, 'multi-asset'))
    members = [[_link(shot)], [_link(asset)], [_link(shot), _link(asset)], []]
    rows = [
        await case.create(
            'Note',
            {
                'subject': f'{case.namespace}-multi-{i}',
                'project': case.project,
                **({'note_links': members[i]} if i < 4 else {}),
            },
        )
        for i in range(5)
    ]
    for relation, values, expected in (
        ('is', [_link(shot)], [0, 2]),
        ('is_not', [_link(shot)], [1, 3, 4]),
        ('is', [None], [3, 4]),
        ('is_not', [None], [0, 1, 2]),
        ('in', [_link(shot)], [0, 2]),
        ('not_in', [_link(shot)], [1, 3, 4]),
        ('name_is', [str(shot['code']).swapcase()], [0, 2]),
        ('name_contains', ['MULTI-SHOT'], [0, 2]),
        ('name_not_contains', ['MULTI-SHOT'], [1, 3, 4]),
        ('type_is', ['Shot'], [0, 2]),
        ('type_is_not', ['Shot'], [1, 3, 4]),
        ('in', [_link(shot), _link(asset)], [0, 1, 2]),
        ('not_in', [_link(shot), _link(asset)], [3, 4]),
    ):
        assert await _relation(case, rows, 'note_links', relation, *values) == expected
    with pytest.raises(Fault):
        await _relation(case, rows, 'note_links', 'is', _link(shot), _link(asset))


@_scenario
async def deep_filters(case: ParityCase) -> None:
    sequence = await case.create('Sequence', _shot_data(case, 'filter-sequence'))
    shot = await case.create(
        'Shot', {**_shot_data(case, 'filter-shot'), 'sg_sequence': _link(sequence)}
    )
    asset = await case.create('Asset', _shot_data(case, 'filter-asset'))
    note = await case.create(
        'Note',
        {
            'subject': case.namespace,
            'project': case.project,
            'note_links': [_link(asset), _link(shot)],
        },
    )
    assert await _relation(case, [note], 'note_links.Shot.code', 'is', shot['code']) == [0]
    tasks = [
        await case.create(
            'Task',
            {'content': f'{case.namespace}-filter-{i}', 'project': case.project, 'entity': target},
        )
        for i, target in enumerate((_link(shot), None, _link(asset)))
    ]
    assert await _relation(case, tasks, 'entity.Shot.code', 'is', shot['code']) == [0]
    assert await _relation(
        case, tasks, 'entity.Shot.sg_sequence.Sequence.code', 'is', sequence['code']
    ) == [0]


@_scenario
async def hidden_links_after_retirement(case: ParityCase) -> None:
    sequence = await case.create('Sequence', _shot_data(case, 'hidden-sequence'))
    shot = await case.create(
        'Shot', {**_shot_data(case, 'hidden-shot'), 'sg_sequence': _link(sequence)}
    )
    asset = await case.create(
        'Asset', {**_shot_data(case, 'hidden-asset'), 'shots': [_link(shot)]}
    )
    await case.call('delete', 'Sequence', _id(sequence))
    assert (await _fields(case, shot, 'sg_sequence'))['sg_sequence'] is None
    assert await _relation(case, [shot], 'sg_sequence', 'is', _link(sequence)) == []
    await case.call('revive', 'Sequence', _id(sequence))
    assert _link((await _fields(case, shot, 'sg_sequence'))['sg_sequence']) == _link(sequence)
    assert await _relation(case, [shot], 'sg_sequence', 'is', _link(sequence)) == [0]
    await case.call('delete', 'Shot', _id(shot))
    assert _links((await _fields(case, asset, 'shots'))['shots']) == []
    await case.call('revive', 'Shot', _id(shot))
    assert _links((await _fields(case, asset, 'shots'))['shots']) == [_link(shot)]


@_scenario
async def relative_dates_on_date_time(case: ParityCase) -> None:
    anchor = await _calendar_anchor(case, months=True)
    month_index = anchor.year * 12 + anchor.month - 1 - 2
    month_start = anchor.replace(year=month_index // 12, month=month_index % 12 + 1)
    next_month_index = anchor.year * 12 + anchor.month - 1 + 2
    month_end = anchor.replace(year=next_month_index // 12, month=next_month_index % 12 + 1)
    sunday = anchor.replace(hour=0, minute=0, second=0) - timedelta(
        days=(anchor.weekday() + 1) % 7
    )
    times = [
        anchor - timedelta(hours=25),
        anchor - timedelta(hours=23),
        anchor - timedelta(hours=2),
        anchor + timedelta(hours=2),
        anchor + timedelta(hours=23),
        anchor + timedelta(hours=25),
        month_start - timedelta(hours=2),
        month_start + timedelta(hours=2),
        month_end - timedelta(hours=2),
        month_end + timedelta(hours=2),
    ]
    rows = [
        await case.create('Shot', {**_shot_data(case, f'relative-{i}'), 'created_at': value})
        for i, value in enumerate(times)
    ]
    assert await _relation(case, rows, 'created_at', 'in_last', 1, 'DAY') == [1, 2]
    assert await _relation(case, rows, 'created_at', 'in_next', 1, 'DAY') == [3, 4]
    assert await _relation(case, rows, 'created_at', 'in_last', 2, 'MONTH') == [0, 1, 2, 7]
    assert await _relation(case, rows, 'created_at', 'in_next', 2, 'MONTH') == [3, 4, 5, 8]
    week_rows = [
        await case.create('Shot', {**_shot_data(case, f'week-{i}'), 'created_at': value})
        for i, value in enumerate(
            (
                sunday - timedelta(hours=2),
                sunday + timedelta(hours=2),
                sunday + timedelta(days=7, hours=-2),
                sunday + timedelta(days=7, hours=2),
            )
        )
    ]
    assert await _relation(case, week_rows, 'created_at', 'in_calendar_week', 0) == [1, 2]
    versions = [
        await case.create('Version', {**_shot_data(case, f'deep-date-{i}'), 'entity': target})
        for i, target in enumerate((_link(rows[2]), _link(rows[3]), None))
    ]
    path = 'entity.Shot.created_at'
    for relation, expected in (
        ('in_last', [0]),
        ('not_in_last', [1, 2]),
        ('in_next', [1]),
        ('not_in_next', [0, 2]),
    ):
        assert await _relation(case, versions, path, relation, 1, 'DAY') == expected


@_scenario
async def relative_dates_on_date(case: ParityCase) -> None:
    today = (await _calendar_anchor(case, months=True)).date()
    offsets = [-3, -2, -1, 0, 1, 2, 3]
    rows = [
        await case.create(
            'Task',
            {
                'content': f'{case.namespace}-date-{i}',
                'project': case.project,
                'start_date': (today + timedelta(days=offset)).isoformat(),
            },
        )
        for i, offset in enumerate(offsets)
    ]
    for relation, amount, unit, expected in (
        ('in_last', 1, 'DAY', [2, 3]),
        ('in_next', 1, 'DAY', [3, 4]),
        ('in_last', 24, 'HOUR', [2, 3]),
        ('in_next', 24, 'HOUR', [3, 4]),
        ('in_last', 48, 'HOUR', [1, 2, 3]),
        ('in_next', 48, 'HOUR', [3, 4, 5]),
        ('in_last', 1, 'MONTH', [0, 1, 2, 3]),
        ('in_next', 1, 'MONTH', [3, 4, 5, 6]),
    ):
        assert await _relation(case, rows, 'start_date', relation, amount, unit) == expected

    previous_index = today.year * 12 + today.month - 2
    next_index = today.year * 12 + today.month
    previous = today.replace(year=previous_index // 12, month=previous_index % 12 + 1)
    following = today.replace(year=next_index // 12, month=next_index % 12 + 1)
    month_dates = [
        previous - timedelta(days=1),
        previous,
        previous + timedelta(days=1),
        following - timedelta(days=1),
        following,
        following + timedelta(days=1),
    ]
    month_rows = [
        await case.create(
            'Task',
            {
                'content': f'{case.namespace}-month-{i}',
                'project': case.project,
                'start_date': value.isoformat(),
            },
        )
        for i, value in enumerate(month_dates)
    ]
    assert await _relation(case, month_rows, 'start_date', 'in_last', 1, 'MONTH') == [1, 2]
    assert await _relation(case, month_rows, 'start_date', 'in_next', 1, 'MONTH') == [3, 4]


@_scenario
async def order_precedence_nulls_and_ties(case: ParityCase) -> None:
    values = [(2, 'a'), (1, 'b'), (1, 'a'), (None, 'z'), (1, 'A'), (3, '10'), (3, '2')]
    rows = [
        await case.create(
            'Shot', {**_shot_data(case, f'order-{i}'), 'sg_cut_order': number, 'description': text}
        )
        for i, (number, text) in enumerate(values)
    ]
    assert await _select(
        case,
        rows,
        order=[
            {'field_name': 'sg_cut_order', 'direction': 'asc'},
            {'field_name': 'description', 'direction': 'desc'},
        ],
    ) == [1, 2, 4, 0, 6, 5, 3]
    assert await _select(
        case,
        rows,
        order=[
            {'field_name': 'sg_cut_order', 'direction': 'desc'},
            {'field_name': 'description', 'direction': 'asc'},
        ],
    ) == [5, 6, 0, 2, 4, 1, 3]
    assert await _select(
        case, rows, order=[{'field_name': 'unknown_order_field', 'direction': 'desc'}]
    ) == list(range(len(rows)))


@_scenario
async def text_order_punctuation(case: ParityCase) -> None:
    suffixes = ['ab', 'a_c', 'a-d', 'a.e', 'a f', 'a1', 'a10', 'a2', 'aB', 'A_a']
    rows = [await case.create('Shot', _shot_data(case, suffix)) for suffix in suffixes]
    assert await _select(case, rows, order=[{'field_name': 'code', 'direction': 'asc'}]) == [
        5,
        6,
        7,
        9,
        0,
        8,
        1,
        2,
        3,
        4,
    ]
    assert await _select(case, rows, order=[{'field_name': 'code', 'direction': 'desc'}]) == [
        4,
        3,
        2,
        1,
        0,
        8,
        9,
        7,
        6,
        5,
    ]


@_scenario
async def status_and_list_order(case: ParityCase) -> None:
    statuses = await _valid_values(case, 'sg_status_list')
    lists = await _valid_values(case, 'sg_asset_type')
    assert len(statuses) >= 3
    assert len(lists) >= 3
    # Keep schema order, choosing an inversion so lexical sorting cannot pass.
    inversion = next(
        (
            (i, j)
            for i in range(len(statuses))
            for j in range(i + 1, len(statuses))
            if statuses[i] > statuses[j]
        ),
        None,
    )
    assert inversion is not None, 'status schema must offer a nonlexical ordering'
    indices = sorted({*inversion, next(i for i in range(len(statuses)) if i not in inversion)})
    selected_statuses = [statuses[i] for i in indices]
    selected_lists = lists[:3]
    rows = [
        await case.create(
            'Asset',
            {
                **_shot_data(case, f'schema-order-{i}'),
                'sg_status_list': status,
                'sg_asset_type': selected_lists[i],
            },
        )
        for i, status in enumerate(selected_statuses)
    ]
    for direction, expected in (('asc', [0, 1, 2]), ('desc', [2, 1, 0])):
        assert (
            await _select(
                case, rows, order=[{'field_name': 'sg_status_list', 'direction': direction}]
            )
            == expected
        )
    for direction in ('asc', 'desc'):
        expected = sorted(range(3), key=lambda i: selected_lists[i], reverse=direction == 'desc')
        assert (
            await _select(
                case, rows, order=[{'field_name': 'sg_asset_type', 'direction': direction}]
            )
            == expected
        )


@_scenario
async def entity_and_deep_order(case: ParityCase) -> None:
    sequences = [
        await case.create('Sequence', _shot_data(case, suffix))
        for suffix in ('z-sequence', 'a-sequence')
    ]
    rows = [
        await case.create('Shot', {**_shot_data(case, f'link-order-{i}'), 'sg_sequence': target})
        for i, target in enumerate((_link(sequences[0]), None, _link(sequences[1])))
    ]
    for field in ('sg_sequence', 'sg_sequence.Sequence.code'):
        for direction, expected in (('asc', [2, 0, 1]), ('desc', [0, 2, 1])):
            assert (
                await _select(case, rows, order=[{'field_name': field, 'direction': direction}])
                == expected
            )


@_scenario
async def version_latest_by_created_at(case: ParityCase) -> None:
    shots = [await case.create('Shot', _shot_data(case, f'latest-shot-{i}')) for i in range(2)]
    rows = [
        await case.create(
            'Version',
            {
                **_shot_data(case, f'latest-version-{i}'),
                'entity': _link(shots[i // 2]),
                'created_at': datetime(2026, 1, i + 1, tzinfo=UTC),
            },
        )
        for i in range(4)
    ]
    presets = [{'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'}]
    assert await _select(case, rows, additional_filter_presets=presets) == [1, 3]
    assert await _select(
        case, rows, [['id', 'is_not', _id(rows[1])]], additional_filter_presets=presets
    ) == [0, 3]
    assert await _select(case, rows, additional_filter_presets=presets, limit=1, page=2) == [3]
    await case.call('delete', 'Version', _id(rows[1]))
    assert await _select(case, rows, additional_filter_presets=presets) == [0, 3]
    assert await _select(case, rows, additional_filter_presets=presets, retired_only=True) == []


@_scenario
async def cut_latest_by_revision(case: ParityCase) -> None:
    sequence = await case.create('Sequence', _shot_data(case, 'cut-sequence'))
    rows = [
        await case.create(
            'Cut',
            {
                **_shot_data(case, 'revision-cut'),
                'entity': _link(sequence),
                'revision_number': revision,
                'created_at': datetime(2026, 1, i + 1, tzinfo=UTC),
            },
        )
        for i, revision in enumerate((1, 2, 2, None))
    ]
    presets = [{'preset_name': 'LATEST', 'latest_by': 'REVISION_NUMBER'}]
    assert await _select(case, rows, additional_filter_presets=presets) == [2]
    assert await _select(
        case, rows, [['id', 'is_not', _id(rows[2])]], additional_filter_presets=presets
    ) == [1]


@_scenario
async def cut_shot_versions(case: ParityCase) -> None:
    shots = [await case.create('Shot', _shot_data(case, f'cut-shot-{i}')) for i in range(3)]
    versions = [
        await case.create(
            'Version',
            {
                **_shot_data(case, f'cut-version-{i}'),
                'entity': _link(shots[i // 2]),
                'created_at': datetime(2026, 1, i + 1, tzinfo=UTC),
            },
        )
        for i in range(6)
    ]
    cut = await case.create('Cut', _shot_data(case, 'items-cut'))
    for index, shot in enumerate(shots[:2]):
        await case.create(
            'CutItem',
            {
                **_shot_data(case, f'cut-item-{index}'),
                'cut': _link(cut),
                'shot': _link(shot),
                'version': _link(versions[index * 2]),
            },
        )
    presets = [{'preset_name': 'CUT_SHOT_VERSIONS', 'cut_id': _id(cut)}]
    assert await _select(case, versions, additional_filter_presets=presets) == [0, 1, 2, 3]
    assert await _select(
        case,
        versions,
        additional_filter_presets=[
            *presets,
            {'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'},
        ],
    ) == [1, 3]


@_scenario
async def preset_validation(case: ParityCase) -> None:
    for entity_type, preset in (
        ('Version', {'preset_name': 'UNKNOWN_PRESET'}),
        ('Shot', {'preset_name': 'LATEST', 'latest_by': 'ENTITIES_CREATED_AT'}),
        ('Version', {'preset_name': 'LATEST'}),
    ):
        with pytest.raises(Fault):
            await case.call(
                'find', entity_type, [['id', 'is', -1]], ['id'], additional_filter_presets=[preset]
            )
    version = await case.create('Version', _shot_data(case, 'missing-cut-id'))
    assert await _select(case, [version]) == [0]
    assert (
        await _select(
            case, [version], additional_filter_presets=[{'preset_name': 'CUT_SHOT_VERSIONS'}]
        )
        == []
    )


@_extended_scenario
async def extended_archive_scope(case: ParityCase) -> None:
    project = await case.create('Project', {'name': f'{case.namespace}-archive'})
    shot = await case.create(
        'Shot', {'code': f'{case.namespace}-archive-shot', 'project': _link(project)}
    )
    version = await case.create(
        'Version', {**_shot_data(case, 'archive-link'), 'entity': _link(shot)}
    )
    try:
        await case.call('update', 'Project', _id(project), {'archived': True})
        for archived in (True, False):
            assert await _select(case, [project], include_archived_projects=archived) == (
                [0] if archived else []
            )
            assert await _select(case, [shot], include_archived_projects=archived) == (
                [0] if archived else []
            )
            assert await _select(
                case,
                [version],
                [['entity.Shot.code', 'is', shot['code']]],
                include_archived_projects=archived,
            ) == [0]
            assert _link((await _fields(case, version, 'entity'))['entity']) == _link(shot)
        await case.call('delete', 'Shot', _id(shot))
        for archived in (True, False):
            assert await _select(
                case, [shot], retired_only=True, include_archived_projects=archived
            ) == ([0] if archived else [])
        await case.call('revive', 'Shot', _id(shot))
        await case.call('delete', 'Version', _id(version))
        for archived in (True, False):
            assert await _select(
                case,
                [version],
                [['entity.Shot.code', 'is', shot['code']]],
                retired_only=True,
                include_archived_projects=archived,
            ) == [0]
    finally:
        await case.call('update', 'Project', _id(project), {'archived': False})


@_scenario
async def asset_and_sequence_parents_cascade(case: ParityCase) -> None:
    for entity_type in ('Asset', 'Sequence'):
        parent = await case.create(entity_type, _shot_data(case, f'{entity_type}-parent'))
        children = [
            await case.create(
                'Task',
                {
                    'content': f'{case.namespace}-{entity_type}-task',
                    'project': case.project,
                    'entity': _link(parent),
                },
            ),
            await case.create(
                'Version',
                {
                    **_shot_data(case, f'{entity_type}-version'),
                    'entity': _link(parent),
                },
            ),
            await case.create(
                'Note',
                {
                    'subject': f'{case.namespace}-{entity_type}-note',
                    'project': case.project,
                    'note_links': [_link(parent)],
                },
            ),
        ]
        field = 'assets' if entity_type == 'Asset' else 'sg_sequence'
        shot = await case.create(
            'Shot',
            {
                **_shot_data(case, f'{entity_type}-shot'),
                field: [_link(parent)] if entity_type == 'Asset' else _link(parent),
            },
        )
        holders = [(shot, field)]
        if entity_type == 'Asset':
            published = await case.create(
                'PublishedFile',
                {
                    **_shot_data(case, 'asset-publish'),
                    'entity': _link(parent),
                },
            )
            ancestor = await case.create(
                'Asset',
                {
                    **_shot_data(case, 'asset-ancestor'),
                    'assets': [_link(parent)],
                },
            )
            holders.extend([(published, 'entity'), (ancestor, 'assets')])
            unchanged = published
        else:
            unchanged = shot
        before = (await _fields(case, unchanged, 'updated_at'))['updated_at']
        for operation in ('delete', 'revive'):
            await case.advance(2)
            assert await case.call(operation, entity_type, _id(parent)) is True
            retired = operation == 'delete'
            for child in children:
                await _scoped_state(case, child, retired=retired)
                if not retired:
                    child_field = 'note_links' if child['type'] == 'Note' else 'entity'
                    value = (await _fields(case, child, child_field))[child_field]
                    if child_field == 'note_links':
                        assert _links(value) == [_link(parent)]
                    else:
                        assert _link(value) == _link(parent)
            for holder, link_field in holders:
                value = (await _fields(case, holder, link_field))[link_field]
                if link_field == 'assets':
                    assert _links(value) == ([] if retired else [_link(parent)])
                else:
                    assert (None if value is None else _link(value)) == (
                        None if retired else _link(parent)
                    )
            assert (await _fields(case, unchanged, 'updated_at'))['updated_at'] == before


@_scenario
async def batch_rows_follow_request_order(case: ParityCase) -> None:
    updated = await case.create('Shot', _shot_data(case, 'batch-updated'))
    retired = await case.create('Shot', _shot_data(case, 'batch-retired'))
    before = await _cursor(case)
    results = _items(
        await case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': _shot_data(case, 'batch-new'),
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': _id(updated),
                    'data': {'description': 'changed'},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': _id(retired)},
            ],
        ),
    )
    bounds = (before, await _cursor(case))
    created_rows = await _events(
        case, bounds, ['event_type'], [['entity', 'is', _link(results[0])]]
    )
    updated_rows = await _events(case, bounds, ['event_type'], [['entity', 'is', updated]])
    lifecycle = await _events(
        case,
        bounds,
        ['event_type', 'attribute_name'],
        [
            ['event_type', 'in', 'Shotgun_Shot_Change', 'Shotgun_Shot_Retirement'],
        ],
    )
    lifecycle = [row for row in lifecycle if row['attribute_name'] in ('retirement_date', None)]
    assert created_rows
    assert len(updated_rows) == 1
    assert [(row['event_type'], row['attribute_name']) for row in lifecycle] == [
        ('Shotgun_Shot_Change', 'retirement_date'),
        ('Shotgun_Shot_Retirement', None),
    ]
    assert max(map(_id, created_rows)) < _id(updated_rows[0]) < min(map(_id, lifecycle))
    newest = _record(
        await case.call(
            'find_one', 'Shot', [], ['id'], order=[{'field_name': 'id', 'direction': 'desc'}]
        )
    )
    before = await _cursor(case)
    with pytest.raises(Fault):
        await case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': _shot_data(case, 'rollback'),
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': _id(newest) + 1_000_000,
                    'data': {'description': 'missing'},
                },
            ],
        )
    assert (
        await _events(
            case,
            (before, await _cursor(case)),
            ['event_type'],
            [
                ['event_type', 'starts_with', 'Shotgun_Shot_'],
            ],
        )
        == []
    )


@_scenario
async def cascade_retires_children(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'cascade-shot'))
    asset = await case.create('Asset', _shot_data(case, 'cascade-asset'))
    versions = [
        await case.create(
            'Version',
            {
                **_shot_data(case, f'cascade-version-{i}'),
                'entity': _link(shot),
            },
        )
        for i in range(3)
    ]
    task = await case.create(
        'Task',
        {
            'content': f'{case.namespace}-cascade-task',
            'project': case.project,
            'entity': _link(shot),
        },
    )
    sole_note = await case.create(
        'Note',
        {
            'subject': f'{case.namespace}-task-note',
            'project': case.project,
            'tasks': [_link(task)],
        },
    )
    shared_note = await case.create(
        'Note',
        {
            'subject': f'{case.namespace}-shared-note',
            'project': case.project,
            'note_links': [_link(shot), _link(asset)],
        },
    )
    await case.call('delete', 'Version', _id(versions[0]))
    await case.call('delete', 'Shot', _id(shot))
    for child in [*versions, task, sole_note]:
        await _scoped_state(case, child, retired=True)
    await _scoped_state(case, shared_note, retired=False)
    assert _links((await _fields(case, shared_note, 'note_links'))['note_links']) == [_link(asset)]
    await case.call('revive', 'Version', _id(versions[2]))
    assert (await _fields(case, versions[2], 'entity'))['entity'] is None
    await case.call('revive', 'Shot', _id(shot))
    for child in [*versions[1:], task, sole_note]:
        await _scoped_state(case, child, retired=False)
    await _scoped_state(case, versions[0], retired=True)
    assert _links((await _fields(case, shared_note, 'note_links'))['note_links']) == [
        _link(asset),
        _link(shot),
    ]
    assert _link((await _fields(case, versions[1], 'entity'))['entity']) == _link(shot)
    assert _link((await _fields(case, task, 'entity'))['entity']) == _link(shot)
    assert _links((await _fields(case, sole_note, 'tasks'))['tasks']) == [_link(task)]


@_scenario
async def create_rows_start_with_new(case: ParityCase) -> None:
    schema = _record(await case.call('schema_field_read', 'Sequence'))
    allowed = {'code'}
    for name, entry in schema.items():
        properties = _record(_record(entry)['properties'])
        if _record(properties.get('default_value', {'value': None}))['value'] is not None:
            allowed.add(name)
    before = await _cursor(case)
    sequence = await case.create(
        'Sequence',
        {
            **_shot_data(case, 'create-events'),
            'description': None,
        },
    )
    rows = await _events(
        case,
        (before, await _cursor(case)),
        ['event_type', 'attribute_name'],
        [
            ['entity', 'is', sequence],
        ],
    )
    assert rows[0]['event_type'] == 'Shotgun_Sequence_New'
    assert (await _fields(case, rows[0], 'meta'))['meta'] == {
        'type': 'new_entity',
        'entity_type': 'Sequence',
        'entity_id': _id(sequence),
    }
    changes = rows[1:]
    assert all(row['event_type'] == 'Shotgun_Sequence_Change' for row in changes)
    assert {row['attribute_name'] for row in changes} <= allowed
    assert not {'project', 'description'} & {row['attribute_name'] for row in changes}
    code_rows = [row for row in changes if row['attribute_name'] == 'code']
    assert len(code_rows) == 1
    assert _record((await _fields(case, code_rows[0], 'meta'))['meta'])['in_create'] is True


@_scenario
async def hidden_members_survive_edits(case: ParityCase) -> None:
    for sequence in range(3):
        shots = [
            await case.create('Shot', _shot_data(case, f'hidden-{sequence}-{i}')) for i in range(3)
        ]
        asset = await case.create(
            'Asset',
            {
                **_shot_data(case, f'hidden-asset-{sequence}'),
                'shots': [_link(row) for row in shots[:2]],
            },
        )
        await case.call('delete', 'Shot', _id(shots[0]))
        if sequence == 0:
            await case.call('update', 'Asset', _id(asset), {'shots': [_link(shots[1])]})
            await case.call(
                'update',
                'Asset',
                _id(asset),
                {'shots': [_link(shots[2])]},
                multi_entity_update_modes={'shots': 'add'},
            )
            expected = [True, True, True]
        elif sequence == 1:
            await case.call(
                'update',
                'Asset',
                _id(asset),
                {'shots': [_link(shots[2])]},
                multi_entity_update_modes={'shots': 'add'},
            )
            expected = [True, True, True]
        else:
            await case.call(
                'update',
                'Asset',
                _id(asset),
                {'shots': [_link(shots[1])]},
                multi_entity_update_modes={'shots': 'remove'},
            )
            expected = [True, False, False]
        await case.call('revive', 'Shot', _id(shots[0]))
        for shot, present in zip(shots, expected, strict=True):
            assert await _select(case, [asset], [['shots', 'is', shot]]) == (
                [0] if present else []
            )


@_scenario
async def inverse_holders_hide_retired_members(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'inverse-shot'))
    sequence = await case.create('Sequence', _shot_data(case, 'inverse-sequence'))
    await case.call('update', 'Shot', _id(shot), {'sg_sequence': _link(sequence)})
    task = await case.create(
        'Task',
        {
            'content': f'{case.namespace}-inverse-task',
            'project': case.project,
            'entity': _link(shot),
        },
    )
    note = await case.create(
        'Note',
        {
            'subject': f'{case.namespace}-inverse-note',
            'project': case.project,
            'note_links': [_link(shot)],
        },
    )
    version = await case.create('Version', _shot_data(case, 'inverse-version'))
    published = await case.create(
        'PublishedFile',
        {
            **_shot_data(case, 'inverse-publish'),
            'version': _link(version),
            'task': _link(task),
        },
    )
    cut = await case.create('Cut', _shot_data(case, 'inverse-cut'))
    cut_item = await case.create(
        'CutItem',
        {
            **_shot_data(case, 'inverse-cut-item'),
            'cut': _link(cut),
        },
    )
    playlist = await case.create(
        'Playlist',
        {
            **_shot_data(case, 'inverse-playlist'),
            'versions': [_link(version)],
        },
    )
    for member, holder, field in (
        (shot, sequence, 'shots'),
        (task, shot, 'tasks'),
        (note, shot, 'notes'),
        (published, version, 'published_files'),
        (cut_item, cut, 'cut_items'),
        (playlist, version, 'playlists'),
    ):
        baseline = (
            None
            if field == 'playlists'
            else (await _fields(case, holder, 'updated_at'))['updated_at']
        )
        published_time = (await _fields(case, published, 'updated_at'))['updated_at']
        for operation in ('delete', 'revive'):
            await case.advance(2)
            await case.call(operation, member['type'], _id(member))
            retired = operation == 'delete'
            assert _links((await _fields(case, holder, field))[field]) == (
                [] if retired else [_link(member)]
            )
            if field != 'playlists':
                assert (await _fields(case, holder, 'updated_at'))['updated_at'] == baseline
            if member is task:
                value = (await _fields(case, published, 'task'))['task']
                assert (None if value is None else _link(value)) == (
                    None if retired else _link(task)
                )
                assert (await _fields(case, published, 'updated_at'))[
                    'updated_at'
                ] == published_time


@_scenario
async def missing_and_repeated_lifecycle_calls(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'repeat-lifecycle'))
    assert await case.call('revive', 'Shot', _id(shot)) is False
    assert await case.call('delete', 'Shot', _id(shot)) is True
    assert await case.call('delete', 'Shot', _id(shot)) is False
    assert await case.call('revive', 'Shot', _id(shot)) is True
    newest = _record(
        await case.call(
            'find_one', 'Shot', [], ['id'], order=[{'field_name': 'id', 'direction': 'desc'}]
        )
    )
    for method in ('delete', 'revive'):
        with pytest.raises(Fault):
            await case.call(method, 'Shot', _id(newest) + 1_000_000)


@_scenario
async def missing_schema_field_faults(case: ParityCase) -> None:
    with pytest.raises(Fault):
        await case.call('schema_field_read', 'Shot', f'sg_{case.namespace}_missing')


@_scenario
async def multi_link_holders_touched_single_kept(case: ParityCase) -> None:
    parent = await case.create('Asset', _shot_data(case, 'audit-parent'))
    child = await case.create(
        'Asset',
        {
            **_shot_data(case, 'audit-child'),
            'parents': [_link(parent)],
        },
    )
    sequence = await case.create('Sequence', _shot_data(case, 'audit-sequence'))
    shot = await case.create(
        'Shot',
        {
            **_shot_data(case, 'audit-shot'),
            'sg_sequence': _link(sequence),
        },
    )
    baseline = (await _fields(case, shot, 'updated_at'))['updated_at']
    for operation in ('delete', 'revive'):
        await case.advance(2)
        await case.call(operation, 'Asset', _id(parent))
        assert _links((await _fields(case, child, 'parents'))['parents']) == (
            [] if operation == 'delete' else [_link(parent)]
        )
        await case.advance(2)
        await case.call(operation, 'Sequence', _id(sequence))
        assert (await _fields(case, shot, 'updated_at'))['updated_at'] == baseline


@_scenario
async def retirement_rows_and_entity_links(case: ParityCase) -> None:
    before = await _cursor(case)
    shot = await case.create('Shot', _shot_data(case, 'retirement-events'))
    created = await _events(
        case, (before, await _cursor(case)), ['entity'], [['entity', 'is', shot]]
    )
    assert created
    retired = await lifecycle_rows(case, shot, 'delete')
    for row in [*created, *retired]:
        assert (await _fields(case, row, 'entity'))['entity'] is None
    await lifecycle_rows(case, shot, 'revive')
    for row in [*created, *retired]:
        assert _link((await _fields(case, row, 'entity'))['entity']) == _link(shot)
    for entity_type in ('Asset', 'Sequence'):
        record = await case.create(entity_type, _shot_data(case, f'{entity_type}-events'))
        await lifecycle_rows(case, record, 'delete')
        await lifecycle_rows(case, record, 'revive')


@_scenario
async def same_value_updates_change_nothing(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'no-op'))
    await case.call('update', 'Shot', _id(shot), {'description': 'same', 'sg_status_list': 'ip'})
    before_audit = await _fields(case, shot, 'updated_at', 'updated_by')
    await case.advance(2)
    before = await _cursor(case)
    await case.call('update', 'Shot', _id(shot), {'description': 'same', 'sg_status_list': 'ip'})
    assert (
        await _events(
            case, (before, await _cursor(case)), ['event_type'], [['entity', 'is', shot]]
        )
        == []
    )
    assert await _fields(case, shot, 'updated_at', 'updated_by') == before_audit
    sequence = await case.create('Sequence', _shot_data(case, 'no-op-sequence'))
    asset = await case.create('Asset', _shot_data(case, 'no-op-asset'))
    await case.call(
        'update', 'Shot', _id(shot), {'sg_sequence': _link(sequence), 'assets': [_link(asset)]}
    )
    before_audit = await _fields(case, shot, 'updated_at', 'updated_by')
    for data, modes in (
        ({'sg_sequence': _link(sequence)}, {}),
        ({'assets': [_link(asset)]}, {'assets': 'add'}),
        ({'assets': [_link(asset)]}, {'assets': 'set'}),
    ):
        await case.advance(2)
        await case.call('update', 'Shot', _id(shot), data, multi_entity_update_modes=modes)
        assert await _fields(case, shot, 'updated_at', 'updated_by') == before_audit


@_scenario
async def session_uuid_and_common_fields(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'session-events'))
    session = str(uuid4())
    await case.call('set_session_uuid', session)
    try:
        before = await _cursor(case)
        await case.call('update', 'Shot', _id(shot), {'description': 'session'})
        rows = await _events(
            case,
            (before, await _cursor(case)),
            [
                'session_uuid',
                'created_at',
                'cached_display_name',
                'image',
                'audit_trail',
            ],
            [['entity', 'is', shot]],
        )
        assert len(rows) == 1
        assert rows[0]['session_uuid'] == session
        assert rows[0]['created_at'] == (await _fields(case, shot, 'updated_at'))['updated_at']
        assert rows[0]['cached_display_name'] is None
        assert rows[0]['image'] is None
        assert 'audit_trail' not in rows[0]
    finally:
        await case.call('set_session_uuid', None)


@_scenario
async def shot_assets_rows_and_connections(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'link-events-shot'))
    asset = await case.create('Asset', _shot_data(case, 'link-events-asset'))
    status = (await _fields(case, asset, 'sg_status_list'))['sg_status_list']
    for mode in ('add', 'remove'):
        before = await _cursor(case)
        await case.call(
            'update',
            'Shot',
            _id(shot),
            {'assets': [_link(asset)]},
            multi_entity_update_modes={'assets': mode},
        )
        bounds = (before, await _cursor(case))
        connection_rows = await _events(
            case,
            bounds,
            ['event_type', 'attribute_name'],
            [
                ['event_type', 'starts_with', 'Shotgun_AssetShotConnection_'],
            ],
        )
        forward = await _events(
            case,
            bounds,
            ['event_type', 'attribute_name', 'meta'],
            [
                ['entity', 'is', shot],
                ['event_type', 'is', 'Shotgun_Shot_Change'],
            ],
        )
        inverse = await _events(
            case,
            bounds,
            ['event_type', 'attribute_name', 'entity'],
            [
                ['entity', 'is', asset],
                ['event_type', 'is', 'Shotgun_Asset_Change'],
            ],
        )
        assert len(forward) == len(inverse) == 1
        assert forward[0]['attribute_name'] == 'assets'
        assert inverse[0]['attribute_name'] == 'shots'
        assert _link(inverse[0]['entity']) == _link(asset)
        assert _id(forward[0]) < _id(inverse[0])
        meta = _record(forward[0]['meta'])
        elements = _records(meta['added' if mode == 'add' else 'removed'])
        assert meta['removed' if mode == 'add' else 'added'] == []
        assert len(elements) == 1
        element = elements[0]
        assert set(element) == {'type', 'id', 'name', 'valid', 'status', 'uuid'}
        assert _link(element) == _link(asset)
        assert element['name'] == asset['code']
        assert element['valid'] == 'valid'
        assert element['status'] == status
        assert str(UUID(str(element['uuid']))) == element['uuid']
        connections = _records(
            await case.call(
                'find', 'AssetShotConnection', [['shot', 'is', shot]], ['asset', 'shot']
            )
        )
        if mode == 'add':
            assert len(connection_rows) == len(connections) == 1
            assert connection_rows[0]['event_type'] == 'Shotgun_AssetShotConnection_New'
            assert connection_rows[0]['attribute_name'] is None
            assert _id(connection_rows[0]) < _id(forward[0])
            assert _link(connections[0]['asset']) == _link(asset)
            assert _link(connections[0]['shot']) == _link(shot)
        else:
            assert connection_rows == connections == []
            assert (
                await case.call(
                    'find',
                    'AssetShotConnection',
                    [['shot', 'is', shot]],
                    ['id'],
                    retired_only=True,
                )
                == []
            )


@_scenario
async def summary_field_is_read_only(case: ParityCase) -> None:
    schema = _record(await case.call('schema_field_read', 'Shot'))
    summary = next(
        (
            name
            for name, entry in schema.items()
            if _record(_record(entry)['data_type'])['value'] == 'summary'
        ),
        None,
    )
    if summary is None:
        pytest.skip('Shot schema has no summary field')
    shot = await case.create('Shot', _shot_data(case, 'summary-readonly'))
    before = await _fields(case, shot, summary)
    with pytest.raises(Fault):
        await case.call('update', 'Shot', _id(shot), {summary: 42})
    assert await _fields(case, shot, summary) == before


@_scenario
async def update_rows_follow_supply_order(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'ordered-events'))
    previous = await _fields(case, shot, 'description', 'sg_status_list')
    for data in (
        {'description': 'first', 'sg_status_list': 'ip'},
        {'sg_status_list': 'fin', 'description': 'second'},
        {'description': None},
    ):
        before = await _cursor(case)
        await case.call('update', 'Shot', _id(shot), data)
        rows = await _events(
            case,
            (before, await _cursor(case)),
            ['event_type', 'attribute_name', 'meta', 'user'],
            [['entity', 'is', shot]],
        )
        assert [row['attribute_name'] for row in rows] == list(data)
        for row, (field, value) in zip(rows, data.items(), strict=True):
            assert row['event_type'] == 'Shotgun_Shot_Change'
            assert _link(row['user'])['type'] == 'ApiUser'
            assert row['meta'] == {
                'type': 'attribute_change',
                'attribute_name': field,
                'entity_type': 'Shot',
                'entity_id': _id(shot),
                'field_data_type': 'text' if field == 'description' else 'status_list',
                'old_value': previous[field],
                'new_value': value,
            }
        previous.update(data)


@_scenario
async def version_user_is_actor(case: ParityCase) -> None:
    version = await case.create('Version', _shot_data(case, 'version-actor'))
    user = _link((await _fields(case, version, 'user'))['user'])
    assert user['type'] == 'ApiUser'
    assert (await _fields(case, user, 'firstname'))['firstname'] == case.script_name


@_extended_scenario
async def extended_project_retirement_cascades(case: ParityCase) -> None:
    project = await case.create('Project', {'name': f'{case.namespace}-retirement'})
    # LiveCase.create tracks this Project immediately, before any child can fail.
    try:
        children = [
            await case.create(
                entity_type,
                {
                    field: f'{case.namespace}-project-{entity_type}',
                    'project': _link(project),
                },
            )
            for entity_type, field in (
                ('Shot', 'code'),
                ('Asset', 'code'),
                ('Sequence', 'code'),
                ('Task', 'content'),
                ('Version', 'code'),
                ('Note', 'subject'),
            )
        ]
        assert await case.call('delete', 'Project', _id(project)) is True
        for child in children:
            await _scoped_state(case, child, retired=True)
        assert await case.call('revive', 'Project', _id(project)) is True
        for child in children:
            await _scoped_state(case, child, retired=False)
            assert _link((await _fields(case, child, 'project'))['project']) == _link(project)
    finally:
        await case.call('delete', 'Project', _id(project))


@_extended_scenario
async def extended_schema_field_lifecycle(case: ParityCase) -> None:
    display = f'{case.namespace}-Text'
    expected = 'sg_' + display.lower().replace('-', '_')
    fields: list[str] = []
    try:
        for suffix in ('', '_1'):
            before = await _cursor(case)
            field = await case.call('schema_field_create', 'Asset', 'text', display)
            assert isinstance(field, str)
            fields.append(field)
            assert field == expected + suffix
            rows = await _events(
                case,
                (before, await _cursor(case)),
                [
                    'event_type',
                    'attribute_name',
                    'user',
                    'project',
                ],
                [['event_type', 'is', 'Shotgun_DisplayColumn_New']],
            )
            assert len(rows) == 1
            assert rows[0]['attribute_name'] is None
            assert rows[0]['project'] is None
            assert _link(rows[0]['user'])['type'] == 'ApiUser'
            entry = _record(await case.call('schema_field_read', 'Asset', field))[field]
            assert entry == {
                'name': {'value': display, 'editable': True},
                'description': {'value': '', 'editable': True},
                'custom_metadata': {'value': '', 'editable': True},
                'entity_type': {'value': 'Asset', 'editable': False},
                'data_type': {'value': 'text', 'editable': False},
                'editable': {'value': True, 'editable': False},
                'mandatory': {'value': False, 'editable': False},
                'unique': {'value': False, 'editable': False},
                'properties': {
                    'default_value': {'value': None, 'editable': False},
                    'summary_default': {'value': 'none', 'editable': True},
                },
                'visible': {'value': True, 'editable': True},
                'ui_value_displayable': {'value': True, 'editable': False},
            }
        first = fields[0]
        assert (
            await case.call(
                'schema_field_update',
                'Asset',
                first,
                {'visible': False},
                project_entity=case.project,
            )
            is True
        )
        scoped = _record(
            await case.call('schema_field_read', 'Asset', first, project_entity=case.project)
        )
        unscoped = _record(await case.call('schema_field_read', 'Asset', first))
        assert _record(_record(scoped[first])['visible'])['value'] is False
        assert _record(_record(unscoped[first])['visible'])['value'] is True
        all_scoped = _record(await case.call('schema_read', project_entity=case.project))
        assert _record(all_scoped['Asset'])[first] == scoped[first]
        asset = await case.create('Asset', {**_shot_data(case, 'hidden-schema'), first: 'initial'})
        assert (await _fields(case, asset, first))[first] == 'initial'
        await case.call('update', 'Asset', _id(asset), {first: 'changed'})
        assert (await _fields(case, asset, first))[first] == 'changed'
        assert await _select(case, [asset], [[first, 'is', 'changed']]) == [0]
        assert (
            await case.call(
                'schema_field_update',
                'Asset',
                first,
                {'description': 'updated'},
                project_entity=case.project,
            )
            is True
        )
        entry = _record(_record(await case.call('schema_field_read', 'Asset', first))[first])
        assert _record(entry['description'])['value'] == 'updated'
        with pytest.raises(Fault):
            await case.call('schema_field_update', 'Asset', first, {'visible': True})
        assert _record(await case.call('schema_field_read', 'Asset', first))[first] == entry
    finally:
        await delete_fields(case, 'Asset', fields)
    schema = _record(await case.call('schema_field_read', 'Asset'))
    assert all(field not in schema for field in fields)
    projected = await _fields(case, asset, *fields)
    assert all(field not in projected for field in fields)
    with pytest.raises(Fault):
        await case.call('find', 'Asset', [[fields[0], 'is', 'changed']])
    with pytest.raises(Fault):
        await case.call('schema_field_create', 'Asset', 'text', display)


def _group_entry(field: str, kind: str, direction: str = 'asc') -> dict[str, str]:
    return {'field': field, 'type': kind, 'direction': direction}


@_scenario
async def summary_counts_and_numbers(case: ParityCase) -> None:
    label = 'summary-numbers'
    shots = [
        await case.create(
            'Shot',
            {
                **_parity_data(case, label, str(index)),
                'sg_cut_order': order,
                'description': f'description {index}' if index < 2 else None,
            },
        )
        for index, order in enumerate((1, 2, None))
    ]
    scope = _parity_scope(case, label)
    for field, kind, expected in (
        ('id', 'record_count', 3),
        ('description', 'count', 2),
        ('sg_cut_order', 'sum', 3),
        ('sg_cut_order', 'maximum', 2),
        ('sg_cut_order', 'minimum', 1),
        ('sg_cut_order', 'average', 1.5),
    ):
        value = await _parity_summary(case, 'Shot', scope, field, kind)
        assert value == expected
        assert type(value) is type(expected)
    for index, ratio in enumerate((0.1, 0.2)):
        await case.create(
            'Version',
            {**_parity_data(case, label, f'version-{index}'), 'sg_movie_aspect_ratio': ratio},
        )
    assert await _parity_summary(case, 'Version', scope, 'sg_movie_aspect_ratio', 'sum') == 0.3
    empty = _parity_scope(case, f'{label}-empty')
    for entity_type, field, kind, expected in (
        ('Shot', 'sg_cut_order', 'sum', 0),
        ('Shot', 'sg_cut_order', 'average', 0.0),
        ('Shot', 'description', 'count', 0),
        ('Version', 'sg_movie_aspect_ratio', 'sum', 0.0),
        ('Version', 'sg_movie_aspect_ratio', 'maximum', 0.0),
        ('Version', 'sg_movie_aspect_ratio', 'minimum', 0.0),
        ('Version', 'sg_movie_aspect_ratio', 'average', 0.0),
    ):
        value = await _parity_summary(case, entity_type, empty, field, kind)
        assert value == expected
        assert type(value) is type(expected)
    task_empty = [
        ['project', 'is', case.project],
        ['content', 'starts_with', f'{case.namespace}-{label}-empty'],
    ]
    for kind, expected in (('sum', 0), ('average', 0.0)):
        value = await _parity_summary(case, 'Task', task_empty, 'est_in_mins', kind)
        assert value == expected
        assert type(value) is type(expected)
    project_filter = [['id', 'is', _id(case.project)]]
    assert await _parity_summary(case, 'Project', project_filter, 'archived', 'checked') == 0
    assert await _parity_summary(case, 'Project', project_filter, 'archived', 'unchecked') == 1
    assert await case.call('delete', 'Shot', _id(shots[0])) is True
    assert await _parity_summary(case, 'Shot', scope, 'id', 'record_count') == 2


@_scenario
async def summary_type_faults(case: ParityCase) -> None:
    label = 'summary-faults'
    shot = await case.create('Shot', {**_parity_data(case, label, 'shot'), 'sg_cut_order': 1})
    await case.create('Asset', _parity_data(case, label, 'asset'))
    await case.create(
        'Task',
        {
            'content': f'{case.namespace}-{label}-task',
            'project': case.project,
            'start_date': '2026-07-01',
        },
    )
    shot_scope = _parity_scope(case, label)
    asset_scope = _parity_scope(case, label)
    task_scope = [
        ['project', 'is', case.project],
        ['content', 'starts_with', f'{case.namespace}-{label}-'],
    ]
    calls = [
        ('Shot', shot_scope, [_summary_entry('sg_cut_order', 'max')], None),
        ('Shot', shot_scope, [_summary_entry('description', 'sum')], None),
        (
            'Shot',
            shot_scope,
            [_summary_entry('sg_cut_order', 'sum'), _summary_entry('sg_cut_order', 'average')],
            None,
        ),
        ('Shot', shot_scope, [_summary_entry('sg_sequence', 'count')], None),
        (
            'Asset',
            asset_scope,
            [_summary_entry('sg_status_list', 'status_percentage', value=['ip'])],
            None,
        ),
        ('Asset', asset_scope, [_summary_entry('sg_asset_type', 'percentage')], None),
        ('Shot', shot_scope, [_summary_entry('no_such_field', 'count')], None),
        (
            'Shot',
            shot_scope,
            [_summary_entry('id', 'count')],
            [{'field': 'sg_cut_order', 'direction': 'asc'}],
        ),
        (
            'Shot',
            shot_scope,
            [_summary_entry('id', 'count')],
            [{'field': 'sg_cut_order', 'type': 'exact'}],
        ),
        (
            'Task',
            task_scope,
            [_summary_entry('id', 'count')],
            [_group_entry('start_date', 'oneday')],
        ),
        (
            'Task',
            task_scope,
            [_summary_entry('id', 'count')],
            [_group_entry('start_date', 'fivedays')],
        ),
    ]
    assert _id(shot) > 0
    for entity_type, filters, summaries, grouping in calls:
        with pytest.raises(Fault):
            await case.call('summarize', entity_type, filters, summaries, grouping=grouping)


@_scenario
async def entity_groups(case: ParityCase) -> None:
    label = 'entity-groups'
    zeta = await case.create('Sequence', _parity_data(case, label, 'zeta'))
    alpha = await case.create('Sequence', _parity_data(case, label, 'alpha'))
    for index, sequence in enumerate((zeta, zeta, alpha, None)):
        await case.create(
            'Shot',
            {
                **_parity_data(case, label, str(index)),
                'sg_sequence': None if sequence is None else _link(sequence),
            },
        )
    scope = _parity_scope(case, label)
    for direction, ordered in (('asc', (alpha, zeta)), ('desc', (zeta, alpha))):
        groups = await _parity_groups(
            case, 'Shot', scope, 'id', [_group_entry('sg_sequence', 'exact', direction)]
        )
        assert [
            (
                _link(_record(group['group_value'])),
                group['group_name'],
                _record(group['summaries'])['id'],
            )
            for group in groups[:-1]
        ] == [(_link(row), row['code'], 1 if row is alpha else 2) for row in ordered]
        for group in groups[:-1]:
            value = _record(group['group_value'])
            assert value['name'] == group['group_name']
            assert value['valid'] == 'valid'
        assert groups[-1]['group_value'] is None
        assert _record(groups[-1]['summaries'])['id'] == 1
    null_groups = await _parity_groups(
        case, 'Shot', scope, 'sg_sequence.Sequence.id', [_group_entry('sg_sequence', 'exact')]
    )
    assert _record(null_groups[-1]['summaries'])['sg_sequence.Sequence.id'] == 0
    shot = await case.create('Shot', _parity_data(case, 'entity-target', 'shot'))
    asset = await case.create('Asset', _parity_data(case, 'entity-target', 'asset'))
    for index, target in enumerate((shot, asset, None)):
        await case.create(
            'Version',
            {
                **_parity_data(case, label, f'version-{index}'),
                'entity': None if target is None else _link(target),
            },
        )
    names = _record(await case.call('schema_entity_read'))
    if all(_record(_record(names[kind])['name'])['value'] == kind for kind in ('Asset', 'Shot')):
        for kind in ('exact', 'entitytype'):
            for direction, types in (('asc', ('Asset', 'Shot')), ('desc', ('Shot', 'Asset'))):
                groups = await _parity_groups(
                    case, 'Version', scope, 'id', [_group_entry('entity', kind, direction)]
                )
                assert [
                    (
                        _record(group['group_value'])['type']
                        if kind == 'exact'
                        else group['group_value']
                    )
                    for group in groups[:-1]
                ] == list(types)
                assert groups[-1]['group_value'] is None


@_scenario
async def status_and_list_percentages(case: ParityCase) -> None:
    label = 'status-percentages'
    statuses = list(dict.fromkeys(await _schema_values(case, 'Shot', 'sg_status_list')))
    choices = statuses or ['ip', 'fin']
    assert len(choices) >= 2, 'Shot.sg_status_list requires two declared statuses'
    first, second = choices[:2]
    sequences = [
        await case.create('Sequence', _parity_data(case, label, f'sequence-{i}')) for i in range(2)
    ]
    for index, (sequence, status) in enumerate(
        ((0, first), (0, first), (0, second), (1, first), (1, second))
    ):
        await case.create(
            'Shot',
            {
                **_parity_data(case, label, str(index)),
                'sg_sequence': _link(sequences[sequence]),
                'sg_status_list': status,
            },
        )
    scope = _parity_scope(case, label)
    for kind, expected in (
        ('status_percentage', (67, 50)),
        ('status_percentage_as_float', (66, 50)),
    ):
        result = _record(
            await case.call(
                'summarize',
                'Shot',
                scope,
                [_summary_entry('sg_status_list', kind, value=first)],
                grouping=[_group_entry('sg_sequence', 'exact')],
            )
        )
        assert _record(result['summaries'])['sg_status_list'] == 60
        assert [
            _record(group['summaries'])['sg_status_list'] for group in _records(result['groups'])
        ] == list(expected)
    assert (
        await _parity_summary(
            case,
            'Shot',
            [*scope, ['sg_status_list', 'is', first]],
            'sg_status_list',
            'status_list',
        )
        == first
    )
    values = await _schema_values(case, 'Asset', 'sg_asset_type')
    assert len(values) >= 2
    for index, value in enumerate((values[0], values[0], values[1])):
        await case.create(
            'Asset', {**_parity_data(case, label, str(index)), 'sg_asset_type': value}
        )
    assert (
        await _parity_summary(case, 'Asset', scope, 'sg_asset_type', 'percentage', value=values[0])
        == 67
    )
    fourth = await case.create(
        'Asset', {**_parity_data(case, label, 'fourth'), 'sg_asset_type': values[1]}
    )
    await case.call('update', 'Asset', _id(fourth), {'sg_asset_type': None})
    assert (
        await _parity_summary(case, 'Asset', scope, 'sg_asset_type', 'percentage', value=values[0])
        == 50
    )
    empty = _parity_scope(case, f'{label}-empty')
    assert (
        await _parity_summary(
            case, 'Shot', empty, 'sg_status_list', 'status_percentage_as_float', value=first
        )
        == -1
    )
    assert (
        await _parity_summary(case, 'Asset', empty, 'sg_asset_type', 'percentage', value=values[0])
        == 0
    )


@_scenario
async def tens_and_nested_groups(case: ParityCase) -> None:
    label = 'tens-groups'
    sequences = [
        await case.create('Sequence', _parity_data(case, label, f'sequence-{i}')) for i in range(2)
    ]
    for index, order in enumerate((1, 9, 12, 105, 12345, None)):
        await case.create(
            'Shot',
            {
                **_parity_data(case, label, str(index)),
                'sg_cut_order': order,
                'sg_sequence': _link(sequences[index % 2]),
            },
        )
    scope = _parity_scope(case, label)
    for direction, expected in (
        ('asc', [(0, 2), (10, 1), (100, 1), (12340, 1)]),
        ('desc', [(12340, 1), (100, 1), (10, 1), (0, 2)]),
    ):
        groups = await _parity_groups(
            case, 'Shot', scope, 'id', [_group_entry('sg_cut_order', 'tens', direction)]
        )
        assert [
            (group['group_name'], _record(group['summaries'])['id']) for group in groups[:-1]
        ] == expected
        assert all(
            type(group['group_name']) is int and group['group_value'] == group['group_name']
            for group in groups[:-1]
        )
        assert groups[-1]['group_value'] is None
        assert _record(groups[-1]['summaries'])['id'] == 1
    hundreds = await _parity_groups(
        case, 'Shot', scope, 'id', [_group_entry('sg_cut_order', 'hundreds')]
    )
    assert [group['group_value'] for group in hundreds] == [0, 100, 12300, None]
    nested = await _parity_groups(
        case,
        'Shot',
        scope,
        'id',
        [
            _group_entry('sg_sequence', 'exact'),
            _group_entry('project', 'exact'),
            _group_entry('sg_cut_order', 'tens'),
        ],
    )
    assert len(nested) == 2
    assert [_link(_record(group['group_value'])) for group in nested] == [
        _link(sequence) for sequence in sequences
    ]
    expected_by_sequence = (
        [(0, 0, 1), (10, 10, 1), (12340, 12340, 1)],
        [(0, 0, 1), (100, 100, 1), ('', None, 1)],
    )
    for sequence_group, expected_leaves in zip(nested, expected_by_sequence, strict=True):
        assert _record(sequence_group['summaries'])['id'] == 3
        projects = _records(sequence_group['groups'])
        assert len(projects) == 1
        assert _link(projects[0]['group_value']) == _link(case.project)
        assert _record(projects[0]['summaries'])['id'] == 3
        leaves = _records(projects[0]['groups'])
        assert [
            (leaf['group_name'], leaf['group_value'], _record(leaf['summaries'])['id'])
            for leaf in leaves
        ] == expected_leaves
        for leaf in leaves:
            assert 'groups' not in leaf


@_scenario
async def date_extrema(case: ParityCase) -> None:
    label = 'date-extrema'
    for index, day in enumerate(('2026-07-01', '2026-07-10', None)):
        await case.create(
            'Task',
            {
                'content': f'{case.namespace}-{label}-{index}',
                'project': case.project,
                'start_date': day,
            },
        )
    scope = [
        ['project', 'is', case.project],
        ['content', 'starts_with', f'{case.namespace}-{label}-'],
    ]
    assert await _parity_summary(case, 'Task', scope, 'start_date', 'earliest') == '2026-07-01'
    assert await _parity_summary(case, 'Task', scope, 'start_date', 'latest') == '2026-07-10'
    shots = [
        await case.create('Shot', _parity_data(case, label, str(index))) for index in range(2)
    ]
    stored = [(await _fields(case, shot, 'created_at'))['created_at'] for shot in shots]
    expected = [_whole_second(value) for value in stored]
    for kind, value in (('earliest', min(expected)), ('latest', max(expected))):
        result = await _parity_summary(
            case, 'Shot', _parity_scope(case, label), 'created_at', kind
        )
        assert isinstance(result, str)
        assert result == value.strftime('%Y-%m-%d %H:%M:%S UTC')


@_scenario
async def exact_value_groups(case: ParityCase) -> None:
    label = 'exact-groups'
    scope = _parity_scope(case, label)
    text_scope = [*scope, ['code', 'contains', f'{label}-text-']]
    for index, value in enumerate(('1x', 'alpha', 'alpha', '_x', 'Zed', None)):
        await case.create(
            'Shot', {**_parity_data(case, label, f'text-{index}'), 'description': value}
        )
    for direction, expected in (
        ('asc', [('1x', 1), ('alpha', 2), ('_x', 1), ('Zed', 1), ('', 1)]),
        ('desc', [('Zed', 1), ('_x', 1), ('alpha', 2), ('1x', 1), ('', 1)]),
    ):
        groups = await _parity_groups(
            case, 'Shot', text_scope, 'id', [_group_entry('description', 'exact', direction)]
        )
        assert [
            (group['group_name'], _record(group['summaries'])['id']) for group in groups
        ] == expected
        assert all(group['group_name'] == group['group_value'] for group in groups[:-1])
        assert groups[-1]['group_value'] is None
    for index, number in enumerate((-15, 5, 5, 1000)):
        await case.create(
            'Shot', {**_parity_data(case, label, f'number-{index}'), 'sg_cut_order': number}
        )
    numbered = [*scope, ['code', 'contains', f'{label}-number-']]
    groups = await _parity_groups(
        case, 'Shot', numbered, 'id', [_group_entry('sg_cut_order', 'exact')]
    )
    assert [(group['group_value'], _record(group['summaries'])['id']) for group in groups] == [
        ('-15', 1),
        ('5', 2),
        ('1000', 1),
    ]
    for index, ratio in enumerate((1.5, 1.5, -0.5, 10.0)):
        await case.create(
            'Version',
            {**_parity_data(case, label, f'ratio-{index}'), 'sg_movie_aspect_ratio': ratio},
        )
    ratio_groups = await _parity_groups(
        case, 'Version', scope, 'id', [_group_entry('sg_movie_aspect_ratio', 'exact')]
    )
    assert [
        (group['group_value'], _record(group['summaries'])['id']) for group in ratio_groups
    ] == [('-0.5', 1), ('1.5', 2), ('10.0', 1)]
    for index, hour in enumerate((0, 12)):
        await case.create(
            'Version',
            {
                **_parity_data(case, label, f'time-{index}'),
                'media_center_import_time': datetime(
                    2026, 1, 5, hour, 30 if hour == 0 else 0, tzinfo=UTC
                ),
            },
        )
    time_scope = [*scope, ['code', 'contains', f'{label}-time-']]
    time_groups = await _parity_groups(
        case, 'Version', time_scope, 'id', [_group_entry('media_center_import_time', 'exact')]
    )
    assert [group['group_name'] for group in time_groups] == [
        '2026-01-05 00:30:00',
        '2026-01-05 12:00:00',
    ]
    values = await _schema_values(case, 'Asset', 'sg_asset_type')
    ordered = sorted(values)
    pair: tuple[str, str] | None = None
    for first, second in combinations(ordered, 2):
        if values.index(first) >= values.index(second) or collation_key(first) >= collation_key(
            second
        ):
            continue
        try:
            check_collation((first, second), entity_type='Asset', field_name='sg_asset_type')
        except MockgunError:
            continue
        pair = (first, second)
        break
    assert pair is not None, 'Asset.sg_asset_type has no two values with agreeing orders'
    for index, value in enumerate(pair):
        await case.create(
            'Asset', {**_parity_data(case, label, str(index)), 'sg_asset_type': value}
        )
    list_groups = await _parity_groups(
        case, 'Asset', scope, 'id', [_group_entry('sg_asset_type', 'exact')]
    )
    assert [group['group_value'] for group in list_groups] == list(pair)
    project_groups = await _parity_groups(
        case,
        'Project',
        [['id', 'is', _id(case.project)]],
        'id',
        [_group_entry('archived', 'exact')],
    )
    assert [(group['group_name'], group['group_value']) for group in project_groups] == [
        ('[ ]', 'f')
    ]


@_scenario
async def multi_entity_and_letter_groups(case: ParityCase) -> None:
    label = 'multi-letter-groups'
    assets = [
        await case.create('Asset', _parity_data(case, label, name))
        for name in ('Zeta', 'alpha', 'Beta')
    ]
    for index, members in enumerate(((0, 1), (1,), (1, 2), ())):
        await case.create(
            'Shot',
            {
                **_parity_data(case, label, f'links-{index}'),
                'assets': [_link(assets[i]) for i in members],
            },
        )
    scope = _parity_scope(case, label)
    member_scope = [*scope, ['code', 'contains', f'{label}-links-']]
    for direction, indices in (('asc', (1, 2, 0)), ('desc', (0, 2, 1))):
        groups = await _parity_groups(
            case, 'Shot', member_scope, 'id', [_group_entry('assets', 'exact', direction)]
        )
        assert [
            (group['group_name'], group['group_value'], _record(group['summaries'])['id'])
            for group in groups[:-1]
        ] == [
            (
                assets[i]['code'],
                [{**_link(assets[i]), 'name': assets[i]['code'], 'valid': 'valid'}],
                3 if i == 1 else 1,
            )
            for i in indices
        ]
        assert groups[-1]['group_name'] == ''
        assert groups[-1]['group_value'] == []
        assert _record(groups[-1]['summaries'])['id'] == 1
    for index, description in enumerate(('Zed', 'beta', 'alpha', '_x', '1x', 'Alpha')):
        await case.create(
            'Shot', {**_parity_data(case, label, f'letter-{index}'), 'description': description}
        )
    letter_scope = [*scope, ['code', 'contains', f'{label}-letter-']]
    groups = await _parity_groups(
        case, 'Shot', letter_scope, 'id', [_group_entry('description', 'firstletter')]
    )
    assert [(group['group_value'], _record(group['summaries'])['id']) for group in groups] == [
        ('_', 1),
        ('1', 1),
        ('A', 2),
        ('B', 1),
        ('Z', 1),
    ]


@_scenario
async def date_interval_groups(case: ParityCase) -> None:
    label = 'interval-groups'
    dates = (
        datetime(2025, 12, 31, 23, 30, tzinfo=UTC),
        datetime(2026, 1, 3, 23, 30, tzinfo=UTC),
        datetime(2026, 1, 4, 0, 30, tzinfo=UTC),
        datetime(2026, 1, 5, 0, 30, tzinfo=UTC),
        datetime(2026, 1, 5, 12, tzinfo=UTC),
        datetime(2026, 3, 31, 23, 30, tzinfo=UTC),
        datetime(2026, 4, 1, 0, 30, tzinfo=UTC),
        None,
    )
    for index, value in enumerate(dates):
        await case.create(
            'Task',
            {
                'content': f'{case.namespace}-{label}-{index}',
                'project': case.project,
                'start_date': None if value is None else value.date().isoformat(),
            },
        )
        await case.create(
            'Version',
            {
                **_parity_data(case, label, str(index)),
                'media_center_import_time': value,
            },
        )
    task_scope = [
        ['project', 'is', case.project],
        ['content', 'starts_with', f'{case.namespace}-{label}-'],
    ]
    expected = {
        'day': [
            ('December 31, 2025', '2025-12-31', 1),
            ('January 03, 2026', '2026-01-03', 1),
            ('January 04, 2026', '2026-01-04', 1),
            ('January 05, 2026', '2026-01-05', 2),
            ('March 31, 2026', '2026-03-31', 1),
            ('April 01, 2026', '2026-04-01', 1),
            ('', None, 1),
        ],
        'month': [
            ('December 2025', '2025-12-01', 1),
            ('January 2026', '2026-01-01', 4),
            ('March 2026', '2026-03-01', 1),
            ('April 2026', '2026-04-01', 1),
            ('', None, 1),
        ],
        'quarter': [
            ('Q4 2025', '2025-10-01', 1),
            ('Q1 2026', '2026-01-01', 5),
            ('Q2 2026', '2026-04-01', 1),
            ('', None, 1),
        ],
        'year': [('2025', '2025-01-01', 1), ('2026', '2026-01-01', 6), ('', None, 1)],
    }
    for entity_type, filters, field in (
        ('Task', task_scope, 'start_date'),
        ('Version', _parity_scope(case, label), 'media_center_import_time'),
    ):
        for kind, rows in expected.items():
            groups = await _parity_groups(
                case, entity_type, filters, 'id', [_group_entry(field, kind)]
            )
            assert [
                (group['group_name'], group['group_value'], _record(group['summaries'])['id'])
                for group in groups
            ] == rows
        descending = await _parity_groups(
            case, entity_type, filters, 'id', [_group_entry(field, 'day', 'desc')]
        )
        assert [
            (group['group_name'], group['group_value'], _record(group['summaries'])['id'])
            for group in descending
        ] == [*reversed(expected['day'][:-1]), expected['day'][-1]]


@_scenario
async def text_search_single_shot(case: ParityCase) -> None:
    label = 'search-single'
    used: set[str] = set()
    token = _unique_search_token(used, exclude_q=True)
    asset_token = _unique_search_token(used)
    renamed = _unique_search_token(used)
    code = f'{case.namespace}-{label}-{token} wy x_y a.b,c'
    status_schema = await _schema_values(case, 'Shot', 'sg_status_list')
    status = status_schema[0] if status_schema else 'ip'
    shot = await case.create('Shot', {'code': code, 'project': case.project})
    await case.call('update', 'Shot', _id(shot), {'sg_status_list': status})
    asset = await case.create('Asset', _parity_data(case, label, 'asset'))
    await case.call('update', 'Asset', _id(asset), {'description': asset_token})
    shot_schema = _record(await case.call('schema_field_read', 'Shot'))
    if 'image' in shot_schema:
        assert (await _fields(case, shot, 'image'))['image'] is None
    scopes = {'Shot': _parity_scope(case, label), 'EventLogEntry': [['id', 'is', -1]]}
    result = await _parity_search(
        case, f'{token.upper()}  WY X_Y A.B,C', scopes, project_ids=[_id(case.project)], limit=10
    )
    assert result['terms'] == [token, 'wy', 'x_y', 'a.b,c']
    matches = _records(result['matches'])
    assert len(matches) == 1
    assert matches[0] == {
        'id': _id(shot),
        'type': 'Shot',
        'name': code,
        'project_id': _id(case.project),
        'links': ['', ''],
        'status': status,
        'image': None,
    }
    assert _records((await _parity_search(case, f'{token} q', scopes))['matches']) == []
    assert _records((await _parity_search(case, token[1:7], scopes))['matches']) == matches
    assert (
        _records(
            (await _parity_search(case, asset_token, {'Asset': _parity_scope(case, label)}))[
                'matches'
            ]
        )
        == []
    )
    new_code = code.replace(token, renamed)
    await case.call('update', 'Shot', _id(shot), {'code': new_code})
    assert _records((await _parity_search(case, token, scopes))['matches']) == []
    assert [
        _record(row)['name']
        for row in _records((await _parity_search(case, renamed, scopes))['matches'])
    ] == [new_code]
    await case.call('delete', 'Shot', _id(shot))
    assert _records((await _parity_search(case, renamed, scopes))['matches']) == []


@_scenario
async def text_search_order(case: ParityCase) -> None:
    label = 'search-order'
    used: set[str] = set()
    token = _unique_search_token(used)
    scope = _parity_scope(case, label)
    names = [
        f'{case.namespace}-{label}-{prefix}{token}' for prefix in (' wwwwww ', ' yy ', ' ', ' g ')
    ]
    for name in names:
        await case.create('Shot', {'code': name, 'project': case.project})
    matches = _records((await _parity_search(case, token, {'Shot': scope}))['matches'])
    assert [match['name'] for match in matches] == [names[i] for i in (2, 3, 1, 0)]
    token2 = _unique_search_token(used)
    combined: dict[str, list[list[object]]] = {'Asset': scope, 'Shot': scope}
    expected = [f'{case.namespace}-{label}-{token2} {letter}' for letter in 'abcd']
    for entity_type, letter in (('Shot', 'd'), ('Asset', 'a'), ('Shot', 'b'), ('Asset', 'c')):
        await case.create(
            entity_type, {'code': expected[ord(letter) - ord('a')], 'project': case.project}
        )
    for scopes in (combined, {'Shot': scope, 'Asset': scope}):
        assert [
            match['name']
            for match in _records((await _parity_search(case, token2, scopes))['matches'])
        ] == expected
        assert [
            match['name']
            for match in _records((await _parity_search(case, token2, scopes, limit=2))['matches'])
        ] == expected[:2]


@_scenario
async def text_search_match_fields(case: ParityCase) -> None:
    label = 'search-fields'
    used: set[str] = set()
    shot = await case.create('Shot', _parity_data(case, label, _unique_search_token(used)))
    sequence = await case.create('Sequence', _parity_data(case, label, _unique_search_token(used)))
    asset = await case.create('Asset', _parity_data(case, label, _unique_search_token(used)))
    task = await case.create(
        'Task',
        {
            'content': f'{case.namespace}-{label}-{_unique_search_token(used)}',
            'project': case.project,
            'entity': _link(shot),
        },
    )
    version = await case.create(
        'Version', {**_parity_data(case, label, _unique_search_token(used)), 'entity': _link(shot)}
    )
    note = await case.create(
        'Note',
        {
            'subject': f'{case.namespace}-{label}-{_unique_search_token(used)}',
            'project': case.project,
            'note_links': [_link(shot)],
        },
    )
    cut = await case.create(
        'Cut', {**_parity_data(case, label, _unique_search_token(used)), 'entity': _link(sequence)}
    )
    cut_item = await case.create(
        'CutItem',
        {
            **_parity_data(case, label, _unique_search_token(used)),
            'cut': _link(cut),
            'shot': _link(shot),
        },
    )
    for record, field in (
        (shot, 'code'),
        (sequence, 'code'),
        (asset, 'code'),
        (task, 'content'),
        (version, 'code'),
        (note, 'subject'),
        (cut, 'code'),
        (cut_item, 'code'),
    ):
        token = str(record[field]).rsplit('-', 1)[-1]
        filters = [
            ['project', 'is', case.project],
            [field, 'starts_with', f'{case.namespace}-{label}-'],
        ]
        matches = _records(
            (await _parity_search(case, token, {str(record['type']): filters}))['matches']
        )
        if record is task:
            assert matches == []
            continue
        assert len(matches) == 1
        match = matches[0]
        assert _link(match) == _link(record)
        assert match['project_id'] == _id(case.project)
        assert match['links'] == (
            ['Shot', shot['code']] if record is task or record is version else ['', '']
        )
        schema = _record(await case.call('schema_field_read', record['type']))
        if 'image' in schema:
            assert (await _fields(case, record, 'image'))['image'] is None
        status = (
            (await _fields(case, record, 'sg_status_list'))['sg_status_list']
            if 'sg_status_list' in schema
            else None
        )
        assert match['status'] == status
        assert match['image'] is None
    reply_token = _unique_search_token(used)
    await case.create('Reply', {'content': reply_token, 'entity': _link(note)})
    await case.call('update', 'Note', _id(note), {'content': reply_token})
    note_scope = [
        ['project', 'is', case.project],
        ['subject', 'starts_with', f'{case.namespace}-{label}-'],
    ]
    found = _records(
        (
            await _parity_search(
                case,
                reply_token,
                {'Reply': [['entity', 'is', note]], 'Note': note_scope},
            )
        )['matches']
    )
    assert len(found) == 1
    assert _link(found[0]) == _link(note)


@_scenario
async def text_search_short_text_faults(case: ParityCase) -> None:
    for text in ('', 'a', 'ab', 'wy'):
        with pytest.raises(Fault):
            await _parity_search(case, text, {'Shot': _parity_scope(case, 'search-short')})


@_extended_scenario
async def extended_archive_summary_and_search(case: ParityCase) -> None:
    label = 'archive-query'
    token = _search_token()
    project = await case.create('Project', {'name': f'{case.namespace}-{label}'})
    try:
        archived = await case.create(
            'Shot', {'code': f'{case.namespace}-{label}-{token}-a', 'project': _link(project)}
        )
        active = await case.create(
            'Shot', {'code': f'{case.namespace}-{label}-{token}-b', 'project': case.project}
        )
        await case.call('update', 'Project', _id(project), {'archived': True})
        filters = [['code', 'starts_with', f'{case.namespace}-{label}-']]
        all_groups = await _parity_groups(
            case, 'Shot', filters, 'id', [_group_entry('project', 'exact')]
        )
        assert {_id(_record(group['group_value'])) for group in all_groups} == {
            _id(project),
            _id(case.project),
        }
        assert len(all_groups) == 2
        assert all(_record(group['summaries'])['id'] == 1 for group in all_groups)
        visible_groups = await _parity_groups(
            case,
            'Shot',
            filters,
            'id',
            [_group_entry('project', 'exact')],
            include_archived_projects=False,
        )
        assert [_id(_record(group['group_value'])) for group in visible_groups] == [
            _id(case.project)
        ]
        assert len(visible_groups) == 1
        assert _record(visible_groups[0]['summaries'])['id'] == 1
        scope = {'Shot': filters}
        for ids, expected in (
            (None, [active]),
            ([_id(project)], [archived]),
            ([_id(case.project), _id(project)], [archived, active]),
        ):
            options = {} if ids is None else {'project_ids': ids}
            matches = _records((await _parity_search(case, token, scope, **options))['matches'])
            assert [_link(match) for match in matches] == [_link(row) for row in expected]
    finally:
        await case.call('update', 'Project', _id(project), {'archived': False})


@_scenario
async def plain_upload_attachment_fields(case: ParityCase) -> None:
    instant = await _anchor(case) - timedelta(days=1)
    note = await _file_note(case, 'plain-fields')
    display = f'{case.namespace}-Shown'
    with TemporaryDirectory() as directory:
        path = _upload_file(Path(directory), case, 'plain-fields', b'x' * 40)
        identity = await case.call(
            'upload', 'Note', _id(note), path, display_name=display, created_at=instant
        )
    attachment = {'type': 'Attachment', 'id': identity}
    row = await _fields(
        case,
        attachment,
        'filename',
        'original_fname',
        'display_name',
        'cached_display_name',
        'created_at',
        'created_by',
        'project',
        'attachment_links',
    )
    assert row['filename'] == row['original_fname'] == Path(path).name
    assert row['display_name'] == row['cached_display_name'] == display
    assert _whole_second(row['created_at']) == instant
    assert _record(row['created_by'])['type'] == 'ApiUser'
    assert _link(row['project']) == case.project
    assert _links(row['attachment_links']) == [_link(note)]
    assert (await _fields(case, note, 'attachments'))['attachments'] == [
        {**attachment, 'name': display}
    ]
    await case.call('delete', 'Note', _id(note))
    assert await _fields(case, attachment, 'filename') == {
        **attachment,
        'filename': Path(path).name,
    }


@_scenario
async def plain_upload_event_rows(case: ParityCase) -> None:
    note = await _file_note(case, 'plain-events')
    for display in (None, f'{case.namespace}-Shown'):
        before = await _cursor(case)
        with TemporaryDirectory() as directory:
            identity = await case.call(
                'upload',
                'Note',
                _id(note),
                _upload_file(Path(directory), case, 'plain-events', b'x' * 40),
                display_name=display,
            )
        attachment = {'type': 'Attachment', 'id': identity}
        rows = await _events(
            case, (before, await _cursor(case)), ['entity', 'event_type', 'attribute_name']
        )
        assert [
            (row['event_type'], row['attribute_name'])
            for row in rows
            if isinstance(row['entity'], dict)
            and _link(cast('object', row['entity'])) in (attachment, _link(note))
        ] == [
            ('Shotgun_Attachment_New', None),
            ('Shotgun_Attachment_Change', 'attachment_links'),
            ('Shotgun_Note_Change', 'attachments'),
            *([('Shotgun_Attachment_Change', 'display_name')] if display else []),
            ('Shotgun_Attachment_Change', 'project'),
        ]


@_scenario
async def field_upload_links_version(case: ParityCase) -> None:
    version = await case.create('Version', _shot_data(case, 'field-upload'))
    with TemporaryDirectory() as directory:
        identity = await case.call(
            'upload',
            'Version',
            _id(version),
            _upload_file(Path(directory), case, 'field-upload', b'x' * 40),
            field_name='sg_uploaded_movie',
        )
    attachment = {'type': 'Attachment', 'id': identity}
    assert _links((await _fields(case, attachment, 'attachment_links'))['attachment_links']) == [
        _link(version)
    ]


@_scenario
async def upload_to_missing_record_fails(case: ParityCase) -> None:
    highest = await case.call(
        'find_one', 'Asset', [], order=[{'field_name': 'id', 'direction': 'desc'}]
    )
    missing = (0 if highest is None else _id(_record(highest))) + 10_000_000
    with TemporaryDirectory() as directory:
        path = _upload_file(Path(directory), case, 'missing-upload', b'x' * 40)
        with pytest.raises(ShotgunError):
            await case.call('upload', 'Asset', missing, path)
    assert await case.call('find', 'Attachment', [['filename', 'is', Path(path).name]]) == []


@_scenario
async def attachment_downloads(case: ParityCase) -> None:
    attachment = await _upload_note(case, await _file_note(case, 'downloads'), 'downloads')
    assert await case.call('download_attachment', _id(attachment)) == b'x' * 40
    assert await case.call('download_attachment', attachment) == b'x' * 40
    assert await case.call('download_attachment', attachment_id=_id(attachment)) == b'x' * 40
    await case.call('delete', 'Attachment', _id(attachment))
    for identity in (_id(attachment), _id(attachment) + 10_000_000):
        with pytest.raises(ShotgunFileDownloadError):
            await case.call('download_attachment', identity)


@_scenario
async def thumbnail_identities_sharing_and_clearing(case: ParityCase) -> None:
    source, target, empty = [
        await case.create('Asset', _shot_data(case, f'thumbnail-{index}')) for index in range(3)
    ]
    with TemporaryDirectory() as directory:
        image = await case.call(
            'upload_thumbnail',
            'Asset',
            _id(source),
            _upload_file(Path(directory), case, 'thumb', _png(256, 256)),
        )
        filmstrip = await case.call(
            'upload_filmstrip_thumbnail',
            'Asset',
            _id(source),
            _upload_file(Path(directory), case, 'filmstrip', _png(512, 256)),
        )
    source_url = await _ready_image(case, source, 'image')
    for identity in (image, filmstrip):
        for retired in (False, True):
            assert (
                await case.call(
                    'find_one', 'Attachment', [['id', 'is', identity]], retired_only=retired
                )
                is None
            )
    for _ in range(2):
        assert (
            await case.call('share_thumbnail', [_link(target)], source_entity=_link(source))
            == image
        )
    target_url = await _ready_image(case, target, 'image')
    assert urlsplit(target_url).path == urlsplit(source_url).path
    assert target_url != source_url
    with pytest.raises(ShotgunError):
        await case.call('share_thumbnail', [_link(target)], source_entity=_link(empty))
    assert (await _fields(case, target, 'image'))['image'] is None
    await case.call('update', 'Asset', _id(source), {'image': None})
    assert await _fields(case, source, 'image', 'filmstrip_image') == {
        **_link(source),
        'image': None,
        'filmstrip_image': None,
    }


@_scenario
async def note_thread_contents(case: ParityCase) -> None:
    note = await _file_note(case, 'thread')
    await case.advance(2)
    reply = await case.create(
        'Reply', {'entity': _link(note), 'content': f'{case.namespace}-reply'}
    )
    await case.advance(2)
    attachment = await _upload_note(case, note, 'thread')
    await case.advance(2)
    replies = _records(
        await case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Reply',
                    'data': {'entity': _link(note), 'content': f'{case.namespace}-batch-{index}'},
                }
                for index in range(2)
            ],
        )
    )
    rows = _records(await case.call('note_thread_read', _id(note)))
    assert _links(rows) == [
        _link(note),
        _link(reply),
        attachment,
        *sorted(_links(replies), key=_id),
    ]
    for row in rows:
        stored = await _fields(case, row, *[key for key in row if key not in {'type', 'id'}])
        for key, value in row.items():
            if key in {'user', 'created_by'}:
                assert _link(value) == _link(stored[key])
            else:
                assert value == stored[key]
        if row['type'] == 'Reply':
            assert _record(row['user'])['type'] == 'ApiUser'
            assert _record(row['user'])['image'] is None
    selected = _records(
        await case.call(
            'note_thread_read',
            _id(note),
            {
                'Note': ['subject', 'user'],
                'Reply': ['created_by', 'entity'],
                'Attachment': ['filename', 'project'],
            },
        )
    )
    assert selected[0]['subject'] == note['subject']
    assert _link(selected[0]['user']) == _link((await _fields(case, note, 'user'))['user'])
    assert set(_record(selected[0]['user'])) == {'id', 'type', 'name'}
    assert selected[2]['filename'] == f'{case.namespace}-thread.txt'
    assert 'entity' not in selected[1]
    assert _link(selected[2]['project']) == case.project
    assert set(_record(selected[2]['project'])) == {'id', 'type', 'name'}
    assert all('created_by' not in row for row in selected if row['type'] == 'Reply')
    with TemporaryDirectory() as directory:
        ignored = await case.call(
            'upload', 'Reply', _id(reply), _upload_file(Path(directory), case, 'reply-file', b'x')
        )
    assert {'type': 'Attachment', 'id': ignored} not in _links(
        await case.call('note_thread_read', _id(note))
    )
    other = await _file_note(case, 'thread-other')
    await case.call('update', 'Note', _id(other), {'attachments': [attachment]})
    assert attachment in _links(await case.call('note_thread_read', _id(other)))
    await case.call('delete', 'Reply', _id(reply))
    await case.call('delete', 'Attachment', _id(attachment))
    remaining = _links(await case.call('note_thread_read', _id(note)))
    assert _link(reply) not in remaining
    assert attachment not in remaining
    assert attachment not in _links(await case.call('note_thread_read', _id(other)))
    await case.call('delete', 'Note', _id(note))
    for identity in (_id(note), _id(note) + 10_000_000):
        with pytest.raises(Fault):
            await case.call('note_thread_read', identity)


@_extended_scenario
async def extended_sudo_users_and_automatic_follows(case: ParityCase) -> None:
    human = _link(await _human(case))
    shot = await case.create('Shot', _shot_data(case, 'sudo-shot'))
    script = await _file_note(case, 'sudo-script')
    assert await case.call('followers', _link(script)) == []
    notes = [script]
    try:
        note = _record(
            await case.sudo_call(
                'create',
                'Note',
                {
                    'note_links': [_link(shot)],
                    'subject': f'{case.namespace}-sudo',
                    'project': case.project,
                },
            )
        )
        notes.append(note)
        await case.advance(2)
        reply = _record(
            await case.sudo_call(
                'create',
                'Reply',
                {'entity': _link(script), 'content': f'{case.namespace}-sudo-reply'},
            )
        )
        for record in (note, reply):
            assert _link((await _fields(case, record, 'user'))['user']) == human
        for record in notes:
            assert human in _links(await case.call('followers', _link(record)))
    finally:
        for record in notes:
            await case.call('unfollow', human, _link(record))


@_scenario
async def activity_actions_and_views(case: ParityCase) -> None:
    shot, first, update = await _activity_shot(case)
    first_id, update_id = _id(first), _id(update)
    status = _record(update['meta'])['new_value']
    note = await case.create(
        'Note',
        {
            'project': case.project,
            'subject': f'{case.namespace}-activity',
            'content': 'body',
            'note_links': [_link(shot)],
        },
    )
    with_note = await _stream(case, shot)
    note_entry = _records(with_note['updates'])[0]
    note_id = _id(note_entry)
    assert note_id > update_id
    assert _stream_ids(with_note) == [note_id, update_id, first_id]
    assert _stream_ids(await _stream(case, note)) == [note_id]
    note_status = (await _fields(case, note, 'sg_status_list'))['sg_status_list']
    assert note_entry['primary_entity'] == {
        **_link(note),
        'name': f'{note["subject"]} - body',
        'status': note_status,
    }
    reply = await case.create(
        'Reply', {'entity': _link(note), 'content': f'{case.namespace}-activity-reply'}
    )
    complete = await _stream(case, shot)
    reply_entry = _records(complete['updates'])[0]
    reply_id = _id(reply_entry)
    expected = [reply_id, update_id, first_id]
    assert reply_id > note_id
    assert _stream_ids(complete) == expected
    assert reply_entry['update_type'] == 'create_reply'
    assert reply_entry['primary_entity'] == note_entry['primary_entity']
    assert reply_entry['meta'] == {
        'type': 'new_entity',
        'entity_type': 'Reply',
        'entity_id': _id(reply),
        'content': reply['content'],
    }
    note_stream = await _stream(case, note)
    assert _stream_ids(note_stream) == [reply_id]
    for stream in (complete, note_stream):
        assert stream['latest_update_id'] == reply_id
        assert stream['earliest_update_id'] == 0
    for options, ids, latest, earliest in (
        ({'limit': 1}, [reply_id], reply_id, reply_id),
        ({'limit': 2}, [reply_id, update_id], reply_id, update_id),
        ({'min_id': first_id}, [reply_id, update_id], reply_id, update_id),
        ({'max_id': reply_id}, [note_id, update_id, first_id], note_id, 0),
    ):
        view = await _stream(case, shot, **options)
        assert _stream_ids(view) == ids
        assert view['latest_update_id'] == latest
        assert view['earliest_update_id'] == earliest
    project = await _stream(case, case.project, min_id=first_id - 1)
    assert [identity for identity in _stream_ids(project) if identity in expected] == expected
    assert project['earliest_update_id'] == first_id
    latest_update_id = project['latest_update_id']
    assert isinstance(latest_update_id, int)
    assert latest_update_id >= reply_id
    selected = _records(
        (await _stream(case, shot, entity_fields={'Shot': ['code', 'sg_status_list']}))['updates']
    )
    assert selected[0]['primary_entity'] == reply_entry['primary_entity']
    for row in selected[1:]:
        assert _record(row['primary_entity']) == {
            **_record(first['primary_entity']),
            'status': status,
            'code': shot['code'],
            'sg_status_list': status,
        }
    await case.call('delete', 'Shot', _id(shot))
    retired = await _stream(case, shot)
    assert retired['updates'] == []
    assert retired['latest_update_id'] == reply_id
    assert retired['earliest_update_id'] == 0
    await case.call('revive', 'Shot', _id(shot))
    assert _stream_ids(await _stream(case, shot)) == expected


@_scenario
async def activity_silent_field_changes(case: ParityCase) -> None:
    asset = await case.create('Asset', _shot_data(case, 'silent-asset'))
    shot = await case.create('Shot', _shot_data(case, 'silent-shot'))
    task = await case.create(
        'Task',
        {
            'project': case.project,
            'content': f'{case.namespace}-silent-task',
            'entity': _link(shot),
        },
    )
    baselines = [await _stream(case, record) for record in (asset, shot, task)]
    task_statuses = await _schema_values(case, 'Task', 'sg_status_list')
    current_task = (await _fields(case, task, 'sg_status_list'))['sg_status_list']
    shot_types = await _schema_values(case, 'Shot', 'sg_shot_type')
    for record, data in (
        (shot, {'code': f'{case.namespace}-silent-renamed'}),
        (shot, {'sg_cut_in': 1001}),
        (shot, {'sg_shot_type': shot_types[0]}),
        (task, {'start_date': '2026-09-01'}),
        (task, {'est_in_mins': 60}),
        (
            task,
            {'sg_status_list': next(value for value in task_statuses if value != current_task)},
        ),
    ):
        await case.call('update', record['type'], _id(record), data)
    current_asset = (await _fields(case, asset, 'sg_status_list'))['sg_status_list']
    asset_status = next(
        value
        for value in await _schema_values(case, 'Asset', 'sg_status_list')
        if value != current_asset
    )
    await case.call('update', 'Asset', _id(asset), {'sg_status_list': asset_status})
    await case.advance(30)
    assert _stream_ids(await _stream(case, shot)) == _stream_ids(baselines[1])
    assert _stream_ids(await _stream(case, task)) == _stream_ids(baselines[2])
    entries = _records((await _stream(case, asset))['updates'])
    assert len(entries) == len(_records(baselines[0]['updates'])) + 1
    assert entries[0]['update_type'] == 'update'
    assert [_id(row) for row in entries[1:]] == _stream_ids(baselines[0])


@_extended_scenario
async def extended_follow_roundtrip_and_filters(case: ParityCase) -> None:
    human = await _human(case)
    first, second = [
        await case.create('Asset', _shot_data(case, f'follow-{index}')) for index in range(2)
    ]
    shot = await case.create('Shot', _shot_data(case, 'follow-shot'))
    targets = [first, second, shot]
    before = await _cursor(case)
    try:
        for record in (second, first, shot, first):
            result = _record(await case.call('follow', _link(human), _link(record)))
            assert result == {'followed': True, 'user': _link(human), 'entity': _link(record)}
        followers = _records(await case.call('followers', _link(first)))
        assert len(followers) == 1
        follower = followers[0]
        assert set(follower) == {'name', 'type', 'id', 'valid', 'status', 'uuid'}
        assert {key: follower[key] for key in ('name', 'type', 'id', 'valid', 'status')} == {
            **_link(human),
            'name': human['name'],
            'valid': 'valid',
            'status': human['sg_status_list'],
        }
        UUID(str(follower['uuid']))
        for options, expected in (
            ({'entity_type': 'Asset'}, [first, second]),
            ({'entity_type': 'Shot'}, [shot]),
            ({'entity_type': 'Asset', 'project': case.project}, [first, second]),
        ):
            rows = _links(await case.call('following', _link(human), **options))
            assert [row for row in rows if row in _links(targets)] == sorted(
                _links(expected), key=_id
            )
        events = await _events(case, (before, await _cursor(case)), ['entity'])
        assert not [
            row
            for row in events
            if isinstance(row['entity'], dict)
            and _link(cast('object', row['entity'])) in _links(targets)
        ]
    finally:
        for record in (*targets, first):
            assert await case.call('unfollow', _link(human), _link(record)) == {
                'unfollowed': True,
                'user': _link(human),
                'entity': _link(record),
            }


@_extended_scenario
async def extended_work_schedule_project_exception(case: ParityCase) -> None:
    start = (case.run_date or (await _anchor(case)).date()) + timedelta(days=45)
    start -= timedelta(days=start.weekday())
    end = start + timedelta(days=6)
    schedule = _record(
        await case.call(
            'work_schedule_read', start.isoformat(), end.isoformat(), project=case.project
        )
    )
    for index in range(7):
        assert schedule[(start + timedelta(days=index)).isoformat()] == {
            'working': index < 5,
            'description': None,
            'reason': 'STUDIO_WORK_WEEK',
        }
    date = start.isoformat()
    rules: list[dict[str, object]] = []
    try:
        for index, working in enumerate((False, False, True)):
            description = f'{case.namespace}-schedule-{index}'
            result = _record(
                await case.call(
                    'work_schedule_update', date, working, description, project=case.project
                )
            )
            assert {key: result[key] for key in ('date', 'working', 'description', 'user')} == {
                'date': date,
                'working': working,
                'description': description,
                'user': None,
            }
            assert _link(result['project']) == case.project
            assert await case.call('work_schedule_read', date, date, project=case.project) == {
                date: {
                    'working': working,
                    'description': description,
                    'reason': 'PROJECT_EXCEPTION',
                }
            }
            found = _records(
                await case.call(
                    'find',
                    'WorkDayRule',
                    [['project', 'is', case.project], ['start_date', 'is', date]],
                    ['start_date', 'end_date', 'working', 'description', 'project', 'user'],
                )
            )
            assert len(found) == 1
            rule = found[0]
            assert rule['start_date'] == rule['end_date'] == date
            assert rule['working'] is working
            assert rule['description'] == description
            assert rule['user'] is None
            assert _link(rule['project']) == case.project
            assert _id(rule) not in [_id(previous) for previous in rules]
            for previous in rules:
                assert (
                    await case.call('find_one', 'WorkDayRule', [['id', 'is', _id(previous)]])
                    is None
                )
            rules.append(rule)
    finally:
        # Also recover a rule created by a successful update before a later assertion failed.
        active = _records(
            await case.call(
                'find',
                'WorkDayRule',
                [
                    ['project', 'is', case.project],
                    ['description', 'starts_with', f'{case.namespace}-schedule-'],
                ],
            )
        )
        for rule in active:
            await case.call('delete', 'WorkDayRule', _id(rule))


@_scenario
async def preferences_unknown_name(case: ParityCase) -> None:
    assert await case.call('preferences_read', [f'{case.namespace}_no_such_pref']) == {}


@_extended_scenario
async def extended_subscription_invalid_user_faults(case: ParityCase) -> None:
    highest = _record(
        await case.call(
            'find_one', 'HumanUser', [], order=[{'field_name': 'id', 'direction': 'desc'}]
        )
    )
    before = await case.call('user_subscriptions_read')
    missing = _id(highest) + 10_000_000
    with pytest.raises(Fault, match='Invalid humanUserId'):
        await case.call(
            'user_subscriptions_create', [{'humanUserId': missing, 'subscription': 'dummy'}]
        )
    assert await case.call('user_subscriptions_read') == before


@_extended_scenario
async def extended_last_accessed_per_actor(case: ParityCase) -> None:
    start = await _anchor(case)
    assert await case.call('update_project_last_accessed', case.project) is None
    field = 'last_accessed_by_current_user'
    script = _whole_second((await _fields(case, case.project, field))[field])
    end = await _anchor(case)
    assert start <= script <= end
    await case.advance(2)
    start = await _anchor(case)
    assert await case.sudo_call('update_project_last_accessed', case.project) is None
    row = _record(
        await case.sudo_call('find_one', 'Project', [['id', 'is', _id(case.project)]], [field])
    )
    human = _whole_second(row[field])
    end = await _anchor(case)
    assert start <= human <= end
    assert human > script
    assert _whole_second((await _fields(case, case.project, field))[field]) == script


@_scenario
async def cascade_and_lifecycle_timestamps(case: ParityCase) -> None:
    lone = await case.create('Shot', _shot_data(case, 'lifecycle-lone'))
    await case.advance(2)
    before = await _cursor(case)
    await case.call('delete', 'Shot', _id(lone))
    rows = await _events(
        case, (before, await _cursor(case)), ['event_type', 'attribute_name', 'meta']
    )
    rows = _metadata_events(rows, lone)
    retired = _whole_second(
        (await _fields(case, lone, 'updated_at', retired_only=True))['updated_at']
    )
    change = next(row for row in rows if row['attribute_name'] == 'retirement_date')
    retirement = next(row for row in rows if row['event_type'] == 'Shotgun_Shot_Retirement')
    assert _event_instant(_record(change['meta'])['new_value']) == retired
    assert _event_instant(_record(retirement['meta'])['retirement_date']) == retired
    await case.advance(2)
    before = await _cursor(case)
    await case.call('revive', 'Shot', _id(lone))
    rows = await _events(case, (before, await _cursor(case)), ['attribute_name', 'meta'])
    rows = _metadata_events(rows, lone)
    change = next(row for row in rows if row['attribute_name'] == 'retirement_date')
    assert _event_instant(_record(change['meta'])['old_value']) == retired
    shot = await case.create('Shot', _shot_data(case, 'cascade-root'))
    version = await case.create(
        'Version', {**_shot_data(case, 'cascade-version'), 'entity': _link(shot)}
    )
    task = await case.create(
        'Task',
        {
            'project': case.project,
            'content': f'{case.namespace}-cascade-task',
            'entity': _link(shot),
        },
    )
    asset = await case.create(
        'Asset', {**_shot_data(case, 'cascade-holder'), 'shots': [_link(shot)]}
    )
    instants: list[datetime] = []
    for operation in ('delete', 'revive'):
        await case.advance(2)
        await case.call(operation, 'Shot', _id(shot))
        values = [
            _whole_second(
                (
                    await _fields(
                        case,
                        record,
                        'updated_at',
                        retired_only=operation == 'delete' and record is not asset,
                    )
                )['updated_at']
            )
            for record in (shot, version, task, asset)
        ]
        assert len(set(values)) == 1
        instants.append(values[0])
    assert instants[1] > instants[0]


@_scenario
async def entity_order_across_types(case: ParityCase) -> None:
    targets = [
        await case.create(kind, _shot_data(case, f'order-{letter}'))
        for kind, letter in (('Shot', 'b'), ('Asset', 'c'), ('Sequence', 'a'))
    ]
    versions = [
        await case.create(
            'Version', {**_shot_data(case, f'order-version-{index}'), 'entity': _link(target)}
        )
        for index, target in enumerate(targets)
    ]
    versions.append(await case.create('Version', _shot_data(case, 'order-version-none')))
    for direction, expected in (('asc', [2, 0, 1, 3]), ('desc', [1, 0, 2, 3])):
        assert (
            await _select(case, versions, order=[{'field_name': 'entity', 'direction': direction}])
            == expected
        )


@_scenario
async def week_exact_and_checkbox_group_names(case: ParityCase) -> None:
    base = (await _anchor(case)).date() + timedelta(days=40)
    sunday = base + timedelta(days=(6 - base.weekday()) % 7)
    dates = [sunday, sunday + timedelta(days=6), sunday + timedelta(days=7)]
    tasks = [
        await case.create(
            'Task',
            {
                'project': case.project,
                'content': f'{case.namespace}-week-{index}',
                'start_date': date.isoformat(),
            },
        )
        for index, date in enumerate(dates)
    ]
    versions = [
        await case.create(
            'Version',
            {
                **_shot_data(case, f'week-{index}'),
                'media_center_import_time': datetime.combine(date, datetime.min.time(), UTC),
                'sg_movie_has_slate': index == 0,
            },
        )
        for index, date in enumerate(dates)
    ]
    expected = [
        (f'{date:%B %d, %Y} to {date + timedelta(days=6):%B %d, %Y}', date.isoformat(), count)
        for date, count in ((sunday, 2), (sunday + timedelta(days=7), 1))
    ]
    for records, field in ((tasks, 'start_date'), (versions, 'media_center_import_time')):
        scope = [['id', 'in', *[_id(row) for row in records]]]
        for direction in ('asc', 'desc'):
            groups = await _parity_groups(
                case,
                str(records[0]['type']),
                scope,
                'id',
                [_group_entry(field, 'week', direction)],
            )
            assert [
                (group['group_name'], group['group_value'], _record(group['summaries'])['id'])
                for group in groups
            ] == (expected if direction == 'asc' else list(reversed(expected)))
    exact = await _parity_groups(
        case,
        'Task',
        [['id', 'in', *[_id(row) for row in tasks]]],
        'id',
        [_group_entry('start_date', 'exact')],
    )
    assert [(group['group_name'], group['group_value']) for group in exact] == [
        (date.strftime('%b %d %y'), date.isoformat()) for date in dates
    ]
    for direction in ('asc', 'desc'):
        checkbox = await _parity_groups(
            case,
            'Version',
            [['id', 'in', *[_id(row) for row in versions]]],
            'id',
            [_group_entry('sg_movie_has_slate', 'exact', direction)],
        )
        values = [('[ ]', 'f'), ('[x]', 't')]
        assert [(group['group_name'], group['group_value']) for group in checkbox] == (
            values if direction == 'asc' else list(reversed(values))
        )


@_scenario
async def clustered_date_groups(case: ParityCase) -> None:
    today = (await _calendar_anchor(case)).date()
    offsets = [(-7, 'last_week')]
    if today.weekday() != 6:
        offsets.append((-1, 'yesterday'))
    offsets.append((0, 'today'))
    if today.weekday() != 5:
        offsets.append((1, 'tomorrow'))
    if (today.weekday() + 1) % 7 < 5:
        offsets.append((2, 'later_this_week'))
    offsets.append((7, 'next_week'))
    tasks = [
        await case.create(
            'Task',
            {
                'project': case.project,
                'content': f'{case.namespace}-cluster-{label}',
                'start_date': (today + timedelta(days=offset)).isoformat(),
            },
        )
        for offset, label in offsets
    ]
    for direction in ('asc', 'desc'):
        groups = await _parity_groups(
            case,
            'Task',
            [['id', 'in', *[_id(row) for row in tasks]]],
            'id',
            [_group_entry('start_date', 'clustered_date', direction)],
        )
        expected = [(label, label, 1) for _, label in offsets]
        assert [
            (group['group_name'], group['group_value'], _record(group['summaries'])['id'])
            for group in groups
        ] == (expected if direction == 'asc' else list(reversed(expected)))


@_scenario
async def computed_task_note_and_reply_values(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'computed-shot'))
    task = await case.create(
        'Task',
        {'project': case.project, 'content': f'{case.namespace}-computed', 'entity': _link(shot)},
    )
    assert await _fields(case, task, 'color', 'due_date') == {
        **_link(task),
        'color': 'pipeline_step',
        'due_date': None,
    }
    note = await _file_note(case, 'computed-note')
    before = await _cursor(case)
    reply = await case.create(
        'Reply', {'entity': _link(note), 'content': f'{case.namespace}-computed-reply'}
    )
    events = await _events(
        case, (before, await _cursor(case)), ['event_type', 'attribute_name', 'entity']
    )
    events = _record_events(events, [note, reply])
    assert [(row['event_type'], row['attribute_name']) for row in events] == [
        ('Shotgun_Reply_New', None),
        ('Shotgun_Reply_Change', 'user'),
        ('Shotgun_Reply_Change', 'entity'),
        ('Shotgun_Note_Change', 'replies'),
        ('Shotgun_Reply_Change', 'content'),
    ]
    for row in (note, reply):
        fields = await _fields(case, row, 'user', 'created_by')
        schema = _record(await case.call('schema_field_read', row['type']))
        if 'created_by' in schema:
            assert fields['user'] == fields['created_by']
        else:
            assert 'created_by' not in fields


@_scenario
async def create_rows_for_recorded_forms(case: ParityCase) -> None:
    shot = await case.create('Shot', _shot_data(case, 'create-forms-shot'))
    version = await case.create(
        'Version',
        {**_shot_data(case, 'create-forms-version'), 'user': None},
    )
    cut = await case.create('Cut', _shot_data(case, 'create-forms-cut'))
    note = await _file_note(case, 'create-forms-note')
    forms: list[tuple[str, dict[str, object], list[str | None]]] = [
        (
            'Task',
            {
                'project': case.project,
                'content': f'{case.namespace}-form-task',
                'entity': _link(shot),
            },
            [None, 'color', 'content', 'sg_status_list', 'entity'],
        ),
        (
            'Note',
            {
                'project': case.project,
                'subject': f'{case.namespace}-form-note',
                'content': 'body',
                'note_links': [_link(shot)],
            },
            [None, 'user', 'sg_status_list', 'subject', 'content', 'note_links'],
        ),
        (
            'Sequence',
            {**_shot_data(case, 'form-sequence'), 'description': 'description'},
            [None, 'code', 'sg_status_list', 'description'],
        ),
        ('Cut', _shot_data(case, 'form-cut'), [None, 'code', 'sg_status_list']),
        (
            'CutItem',
            {
                **_shot_data(case, 'form-item'),
                'cut': _link(cut),
                'shot': _link(shot),
                'version': _link(version),
            },
            [None, 'code', 'cut', 'shot', 'version'],
        ),
        (
            'Playlist',
            {
                **_shot_data(case, 'form-playlist'),
                'description': 'description',
                'versions': [_link(version)],
            },
            [None, 'code', 'description', 'versions'],
        ),
        (
            'Reply',
            {'entity': _link(note), 'content': f'{case.namespace}-form-reply'},
            [None, 'user', 'entity', 'content'],
        ),
    ]
    before = await _cursor(case)
    created = _records(
        await case.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': kind, 'data': data}
                for kind, data, _ in forms
            ],
        )
    )
    rows = await _event_delta(case, before, [*created, shot, version, cut, note])
    for record, (kind, _, fields) in zip(created, forms, strict=True):
        assert _row_pairs(rows, record) == [
            (f'Shotgun_{kind}_{"New" if field is None else "Change"}', field) for field in fields
        ]
    assert _row_pairs(rows, shot) == [('Shotgun_Shot_Change', 'tasks')]
    assert _row_pairs(rows, cut) == [('Shotgun_Cut_Change', 'cut_items')]
    assert _row_pairs(rows, version) == [('Shotgun_Version_Change', 'playlists')]
    assert _row_pairs(rows, note) == [('Shotgun_Note_Change', 'replies')]
    connection = [
        row for row in rows if row['event_type'] == 'Shotgun_PlaylistVersionConnection_New'
    ]
    assert len(connection) == 1
    assert len(rows) == sum(len(fields) for _, _, fields in forms) + 5


@_scenario
async def paired_update_rows(case: ParityCase) -> None:
    shots = [await case.create('Shot', _shot_data(case, f'paired-shot-{i}')) for i in range(3)]
    assets = [await case.create('Asset', _shot_data(case, f'paired-asset-{i}')) for i in range(3)]
    sequences = [
        await case.create('Sequence', _shot_data(case, f'paired-sequence-{i}')) for i in range(2)
    ]
    versions = [
        await case.create(
            'Version',
            {**_shot_data(case, f'paired-version-{i}'), 'user': None},
        )
        for i in range(2)
    ]
    playlists = [
        await case.create('Playlist', _shot_data(case, f'paired-playlist-{i}')) for i in range(2)
    ]
    cuts = [await case.create('Cut', _shot_data(case, f'paired-cut-{i}')) for i in range(2)]
    task = await case.create(
        'Task', {'project': case.project, 'content': f'{case.namespace}-paired-task'}
    )
    note = await _file_note(case, 'paired-note')
    item = await case.create('CutItem', {**_shot_data(case, 'paired-item'), 'cut': _link(cuts[0])})
    await _paired_batch(
        case,
        [
            (shots[0], 'sg_sequence', _link(sequences[0]), None),
            (sequences[1], 'shots', [_link(shots[1])], 'add'),
            (task, 'entity', _link(shots[0]), None),
            (versions[0], 'playlists', _links(playlists), 'add'),
            (playlists[1], 'versions', [_link(versions[1])], 'add'),
            (assets[0], 'parents', [_link(assets[1])], 'add'),
            (assets[2], 'shots', [_link(shots[2])], 'add'),
            (versions[1], 'sg_task', _link(task), None),
            (note, 'note_links', [_link(shots[0]), _link(assets[0])], 'add'),
        ],
        [
            (shots[0], 'sg_sequence'),
            (sequences[0], 'shots'),
            (sequences[1], 'shots'),
            (shots[1], 'sg_sequence'),
            (task, 'entity'),
            (shots[0], 'tasks'),
            ('PlaylistVersionConnection', None),
            ('PlaylistVersionConnection', None),
            (versions[0], 'playlists'),
            (playlists[0], 'versions'),
            (playlists[1], 'versions'),
            ('PlaylistVersionConnection', None),
            (playlists[1], 'versions'),
            (versions[1], 'playlists'),
            ('AssetAssetConnection', None),
            (assets[0], 'parents'),
            (assets[1], 'assets'),
            ('AssetShotConnection', None),
            (assets[2], 'shots'),
            (shots[2], 'assets'),
            (versions[1], 'sg_task'),
            (task, 'sg_versions'),
            (note, 'note_links'),
        ],
    )
    await _paired_batch(
        case,
        [
            (shots[0], 'sg_sequence', _link(sequences[1]), None),
            (task, 'entity', _link(shots[1]), None),
            (playlists[1], 'versions', _links(versions), 'remove'),
            (assets[0], 'parents', [_link(assets[2])], 'set'),
            (item, 'cut', _link(cuts[1]), None),
            (note, 'note_links', [_link(shots[2])], 'set'),
        ],
        [
            (shots[0], 'sg_sequence'),
            (sequences[0], 'shots'),
            (sequences[1], 'shots'),
            (task, 'entity'),
            (shots[0], 'tasks'),
            (shots[1], 'tasks'),
            (playlists[1], 'versions'),
            (versions[0], 'playlists'),
            (versions[1], 'playlists'),
            ('AssetAssetConnection', None),
            (assets[0], 'parents'),
            (assets[1], 'assets'),
            (assets[2], 'assets'),
            (item, 'cut'),
            (cuts[0], 'cut_items'),
            (cuts[1], 'cut_items'),
            (note, 'note_links'),
        ],
    )


@_scenario
async def version_task_mirror(case: ParityCase) -> None:
    task = await case.create(
        'Task', {'project': case.project, 'content': f'{case.namespace}-mirror-task'}
    )
    version = await case.create('Version', _shot_data(case, 'mirror-version'))
    await case.call('update', 'Version', _id(version), {'sg_task': _link(task)})
    assert _links((await _fields(case, task, 'sg_versions'))['sg_versions']) == [_link(version)]


@_scenario
async def attachment_file_dictionary(case: ParityCase) -> None:
    note = await _file_note(case, 'file-dictionary')
    display = f'{case.namespace}-Displayed'
    with TemporaryDirectory() as directory:
        identity = await case.call(
            'upload',
            'Note',
            _id(note),
            _upload_file(Path(directory), case, 'file-dictionary', b'x' * 40),
            display_name=display,
        )
    assert isinstance(identity, int)
    attachment: dict[str, object] = {'type': 'Attachment', 'id': identity}
    direct = _record((await _fields(case, attachment, 'this_file'))['this_file'])
    thread = _records(
        await case.call('note_thread_read', _id(note), {'Attachment': ['this_file']})
    )
    selected = [row for row in thread if _link(row) == attachment]
    assert len(selected) == 1
    file_values = [direct, _record(selected[0]['this_file'])]
    for value in file_values:
        assert set(value) == {'url', 'name', 'content_type', 'link_type', 'type', 'id'}
        assert value['name'] == display
        assert value['content_type'] == 'text/plain'
        assert value['link_type'] == 'upload'
        assert value['type'] == 'Attachment'
        assert value['id'] == identity
        raw_url = value['url']
        assert isinstance(raw_url, str)
        url = urlsplit(raw_url)
        assert url.scheme == 'https'
        assert str(identity) not in url.path


@_extended_scenario
async def extended_sudo_activity_creator(case: ParityCase) -> None:
    human = await _human(case)
    shot = await case.create('Shot', _shot_data(case, 'sudo-activity'))
    note = _record(
        await case.sudo_call(
            'create',
            'Note',
            {
                'note_links': [_link(shot)],
                'subject': f'{case.namespace}-sudo-activity',
                'project': case.project,
            },
        )
    )
    try:
        await case.advance(2)
        entries = _records((await _stream(case, note))['updates'])
        entry = next(row for row in entries if row['update_type'] == 'create')
        creator = _record(entry['created_by'])
        assert creator == {
            **_link(human),
            'name': human['name'],
            'status': human['sg_status_list'],
            'image': None,
        }
    finally:
        await case.call('unfollow', _link(human), _link(note))


@_extended_scenario
async def extended_other_type_display_names(case: ParityCase) -> None:
    department = await case.create(
        'Department', {'code': f'{case.namespace}-dept-code', 'name': f'{case.namespace}-dept'}
    )
    group = await case.create('Group', {'code': f'{case.namespace}-group'})
    custom = await case.create(
        'CustomEntity03', {'code': f'{case.namespace}-ce03', 'project': case.project}
    )
    # C9: Department names by `name`, Group and CustomEntity03 by `code`, and a Ticket as
    # `#<id>: <title>`.
    expected = {
        'Department': f'{case.namespace}-dept',
        'Group': f'{case.namespace}-group',
        'CustomEntity03': f'{case.namespace}-ce03',
    }
    note = await case.create(
        'Note',
        {
            'subject': f'{case.namespace}-display-names',
            'project': case.project,
            'note_links': [_link(department), _link(group), _link(custom)],
        },
    )
    links = _records((await _fields(case, note, 'note_links'))['note_links'])
    assert {str(_record(link)['type']): _record(link)['name'] for link in links} == expected
    for record in (department, group, custom):
        fields = await _fields(case, record, 'cached_display_name')
        assert fields['cached_display_name'] == expected[str(record['type'])]
    # Note links do not accept Ticket; link one Ticket from another instead.
    title = f'{case.namespace}-ticket'
    ticket = await case.create('Ticket', {'title': title, 'project': case.project})
    holder = await case.create(
        'Ticket',
        {'title': f'{title}-holder', 'project': case.project, 'tickets': [_link(ticket)]},
    )
    linked = _records((await _fields(case, holder, 'tickets'))['tickets'])
    ticket_name = f'#{_id(ticket)}: {title}'
    assert [_record(link)['name'] for link in linked] == [ticket_name]
    assert (await _fields(case, ticket, 'cached_display_name'))[
        'cached_display_name'
    ] == ticket_name
