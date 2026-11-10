from datetime import UTC, datetime, timedelta, timezone
from typing import cast

import anyio
import pytest

from shotgun_api3 import Fault
from shotgun_api3.lib.mockgun._async_client import AsyncShotgun
from shotgun_api3.lib.mockgun._schema import SiteSchema
from shotgun_api3.lib.mockgun._sessions import RequestContext
from shotgun_api3.lib.mockgun._store import Store
from shotgun_api3.lib.mockgun._sync_client import Shotgun
from tests._threads import run_concurrently

from .conftest import SCHEMA, FakeSiteCase

_TEST_CREDENTIAL = 'offline'


async def _create_shots(case: FakeSiteCase, count: int = 5) -> list[dict[str, object]]:
    return [
        cast('dict[str, object]', await case.call('create', 'Shot', {'code': f'shot-{index}'}))
        for index in range(count)
    ]


@pytest.mark.anyio
async def test_datetime_round_trip_and_filter(fake_site: FakeSiteCase) -> None:
    when = datetime(2025, 2, 3, 10, 11, 12, 345678, tzinfo=timezone(timedelta(hours=3)))
    await fake_site.call('create', 'ScalarTypes', {'date_time': when, 'date': '2025-02-03'})
    found = cast(
        'list[dict[str, object]]',
        await fake_site.call(
            'find',
            'ScalarTypes',
            [['date_time', 'greater_than', when - timedelta(seconds=1)]],
            ['date_time', 'date'],
        ),
    )
    assert len(found) == 1
    actual = cast('datetime', found[0]['date_time'])
    assert actual.utcoffset() is not None
    assert actual == when.replace(microsecond=0)
    assert found[0]['date'] == '2025-02-03'
    fake_site.client.config.convert_datetimes_to_utc = False
    found = cast(
        'dict[str, object]',
        await fake_site.call('find_one', 'ScalarTypes', [], ['date_time']),
    )
    assert found['date_time'] == when.astimezone(UTC).replace(tzinfo=None, microsecond=0)


@pytest.mark.anyio
async def test_create_projection_update_and_find_count(fake_site: FakeSiteCase) -> None:
    data = {'code': 'original', 'sg_status_list': 'ip'}
    default = cast('dict[str, object]', await fake_site.call('create', 'Shot', data))
    empty = cast(
        'dict[str, object]', await fake_site.call('create', 'Shot', data, return_fields=[])
    )
    selected = cast(
        'dict[str, object]', await fake_site.call('create', 'Shot', data, return_fields=['code'])
    )
    assert default.keys() == empty.keys() == {'code', 'sg_status_list', 'type', 'id'}
    assert selected.keys() == {'code', 'sg_status_list', 'type', 'id'}
    updated = await fake_site.call('update', 'Shot', default['id'], {'code': 'updated'})
    assert updated == {'type': 'Shot', 'id': default['id'], 'code': 'updated'}
    fake_site.client.finds = 0
    found = await fake_site.call('find_one', 'Shot', [['id', 'is', default['id']]], ['code'])
    assert found == updated
    assert fake_site.client.finds == 1


@pytest.mark.anyio
async def test_retirement_batch_and_missing_entity_fault(fake_site: FakeSiteCase) -> None:
    first, second = await _create_shots(fake_site, 2)
    results = cast(
        'list[object]',
        await fake_site.call(
            'batch',
            [
                {'request_type': 'create', 'entity_type': 'Shot', 'data': {'code': 'batch'}},
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': first['id'],
                    'data': {'code': 'changed'},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': second['id']},
            ],
        ),
    )
    assert cast('dict[str, object]', results[0])['code'] == 'batch'
    assert results[1] == {'type': 'Shot', 'id': first['id'], 'code': 'changed'}
    assert results[2] is True
    assert await fake_site.call('delete', 'Shot', second['id']) is False
    assert await fake_site.call('find_one', 'Shot', [['id', 'is', second['id']]]) is None
    assert await fake_site.call('find_one', 'Shot', [], retired_only=True) == {
        'type': 'Shot',
        'id': second['id'],
    }
    assert await fake_site.call('revive', 'Shot', second['id']) is True
    assert await fake_site.call('revive', 'Shot', second['id']) is False
    with pytest.raises(Fault, match='No entity of type Shot exists'):
        await fake_site.call('update', 'Shot', 99999, {'code': 'missing'})


@pytest.mark.anyio
async def test_concurrent_creates_and_reads(fake_site: FakeSiteCase) -> None:
    identities: list[object] = []
    client = fake_site.client
    if isinstance(client, Shotgun):

        def sync_worker(index: int) -> object:
            record = client.create('Shot', {'code': f'concurrent-{index}'})
            assert client.find_one('Shot', [['id', 'is', record['id']]], ['code']) == record
            return record['id']

        identities = run_concurrently(
            [lambda index=index: sync_worker(index) for index in range(8)]
        )
    else:
        assert isinstance(client, AsyncShotgun)

        async def async_worker(index: int) -> None:
            record = await client.create('Shot', {'code': f'concurrent-{index}'})
            assert await client.find_one('Shot', [['id', 'is', record['id']]], ['code']) == record
            identities.append(record['id'])

        async with anyio.create_task_group() as group:
            for index in range(8):
                group.start_soon(async_worker, index)
    assert len(set(identities)) == 8
    assert len(cast('list[object]', await fake_site.call('find', 'Shot', []))) == 8


@pytest.mark.anyio
async def test_invalid_datetime_values_keep_validation_fault(fake_site: FakeSiteCase) -> None:
    with pytest.raises(Fault, match=r'date_time.*not a datetime'):
        await fake_site.call('create', 'ScalarTypes', {'date_time': 'invalid'})


@pytest.mark.anyio
@pytest.mark.parametrize('shot_first', [True, False])
async def test_note_shot_asset_read_order_survives_shot_revival(
    workflow_site: FakeSiteCase, shot_first: bool
) -> None:
    case = workflow_site
    shot = cast('dict[str, object]', await case.call('create', 'Shot', {'code': 'S'}))
    asset = cast('dict[str, object]', await case.call('create', 'Asset', {'code': 'A'}))
    members = [shot, asset] if shot_first else [asset, shot]
    note = cast(
        'dict[str, object]',
        await case.call('create', 'Note', {'subject': 'shared', 'note_links': members}),
    )
    expected = [
        {'type': 'Asset', 'id': asset['id'], 'name': 'A'},
        {'type': 'Shot', 'id': shot['id'], 'name': 'S'},
    ]
    found = cast('dict[str, object]', await case.call('find_one', 'Note', [], ['note_links']))
    assert found['note_links'] == expected
    await case.call('delete', 'Shot', shot['id'])
    found = cast('dict[str, object]', await case.call('find_one', 'Note', [], ['note_links']))
    assert found['note_links'] == expected[:1]
    await case.call('revive', 'Shot', shot['id'])
    found = cast('dict[str, object]', await case.call('find_one', 'Note', [], ['note_links']))
    assert found['note_links'] == expected
    assert found['id'] == note['id']


def test_store_default_values_are_owned_by_each_row() -> None:
    schema = SiteSchema(cast('dict[str, dict[str, object]]', SCHEMA), {})
    store = Store(schema)
    context = RequestContext(None, datetime.now(UTC))
    first = store.create('Shot', {}, context=context)
    second = store.create('Shot', {}, context=context)
    first_reviewers = cast('list[object]', first['reviewers'])
    second_reviewers = cast('list[object]', second['reviewers'])
    first_reviewers.append({'type': 'HumanUser', 'id': 1})
    assert second_reviewers == []
    assert first_reviewers is not second_reviewers

    reviewers_field = cast('dict[str, object]', schema.fields['Shot']['reviewers'])
    reviewers_properties = cast('dict[str, object]', reviewers_field['properties'])
    reviewers_default = cast('dict[str, object]', reviewers_properties['default_value'])
    assert reviewers_default['value'] is None

    startup_event = store.row('EventLogEntry', 1)
    assert startup_event is not None
    metadata = cast('dict[str, object]', startup_event['metadata'])
    metadata['source'] = 'changed'
    metadata_field = cast('dict[str, object]', schema.fields['EventLogEntry']['metadata'])
    metadata_properties = cast('dict[str, object]', metadata_field['properties'])
    metadata_default = cast('dict[str, object]', metadata_properties['default_value'])
    assert metadata_default['value'] == {'source': 'schema'}
