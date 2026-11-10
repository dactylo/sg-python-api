from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import Shotgun
from shotgun_api3._errors import Fault
from tests.live._shapes import dictionary, list_items, record_id, require

if TYPE_CHECKING:
    from ._verification import LiveCase

pytestmark = [pytest.mark.live, pytest.mark.async_required, pytest.mark.anyio]


def _entities(value: object, message: str) -> list[dict[str, object]]:
    items = list_items(value, message)
    if not all(isinstance(item, dict) for item in items):
        pytest.fail(message)
    return cast('list[dict[str, object]]', items)


async def test_batch_create_update_delete(api_case: LiveCase) -> None:
    updated = await api_case.create(
        'Shot', {'code': f'{api_case.namespace}-batch-update', 'project': api_case.project}
    )
    deleted = await api_case.create(
        'Shot', {'code': f'{api_case.namespace}-batch-delete', 'project': api_case.project}
    )
    created_code = f'{api_case.namespace}-batch-create'
    description = f'{api_case.namespace}-batch-updated'
    results = list_items(
        await api_case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {'code': created_code, 'project': api_case.project},
                    'return_fields': ['code'],
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': updated['id'],
                    'data': {'description': description},
                },
                {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': deleted['id']},
            ],
        ),
        'batch returned no result list',
    )
    require(len(results) == 3, 'batch returned wrong result count')
    created = dictionary(results[0], 'batch create failed')
    api_case.track('Shot', record_id(created))
    require(created.get('code') == created_code, 'batch create return_fields was incorrect')
    require(
        record_id(dictionary(results[1], 'batch update failed')) == record_id(updated),
        'batch update returned a different Shot',
    )
    require(results[2] is True, 'batch delete did not succeed')
    fetched = dictionary(
        await api_case.call(
            'find_one', 'Shot', [['id', 'is', record_id(updated)]], ['description']
        ),
        'batch-updated Shot was not found',
    )
    require(
        fetched.get('description') == description,
        'batch update did not persist',
    )
    absent = await api_case.call('find_one', 'Shot', [['id', 'is', record_id(deleted)]], ['id'])
    require(absent is None, 'batch delete left an active Shot')


async def test_batch_rolls_back_first_create_on_second_request_failure(api_case: LiveCase) -> None:
    code = f'{api_case.namespace}-atomicity'
    target = await api_case.create(
        'Shot', {'code': f'{api_case.namespace}-atomicity-target', 'project': api_case.project}
    )
    raised_fault = False
    try:
        await api_case.call(
            'batch',
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {'code': code, 'project': api_case.project},
                },
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': record_id(target),
                    'data': {'sg_python_api_missing_field': 'invalid'},
                },
            ],
        )
    except Fault:
        raised_fault = True
    found = _entities(
        await api_case.call('find', 'Shot', [['code', 'is', code]], ['id']),
        'rollback query returned invalid records',
    )
    for entity in found:
        api_case.track('Shot', record_id(entity))
    require(raised_fault, 'invalid second batch request did not raise Fault')
    require(not found, 'failed batch committed its first create')


async def test_delete_revive_and_retired_only(api_case: LiveCase) -> None:
    shot = await api_case.create(
        'Shot', {'code': f'{api_case.namespace}-retire', 'project': api_case.project}
    )
    shot_id = record_id(shot)
    require(await api_case.call('delete', 'Shot', shot_id) is True, 'delete did not succeed')
    filters = [['id', 'is', shot_id]]
    active = await api_case.call('find_one', 'Shot', filters, ['id'])
    retired = dictionary(
        await api_case.call('find_one', 'Shot', filters, ['id'], retired_only=True),
        'retired_only missed Shot',
    )
    require(active is None, 'deleted Shot remained active')
    require(record_id(retired) == shot_id, 'retired_only returned a different Shot')
    require(await api_case.call('revive', 'Shot', shot_id) is True, 'revive did not succeed')
    revived = dictionary(
        await api_case.call('find_one', 'Shot', filters, ['id']), 'revived Shot is absent'
    )
    require(record_id(revived) == shot_id, 'revived Shot has a different ID')


async def test_eight_concurrent_reads_share_client(api_case: LiveCase) -> None:
    shot = await api_case.create(
        'Shot', {'code': f'{api_case.namespace}-concurrent', 'project': api_case.project}
    )
    shot_id = record_id(shot)
    filters = [['id', 'is', shot_id]]
    client = api_case.client
    if isinstance(client, Shotgun):
        reads = await asyncio.gather(
            *(asyncio.to_thread(client.find_one, 'Shot', filters, ['id']) for _ in range(8))
        )
    else:
        reads = await asyncio.gather(*(client.find_one('Shot', filters, ['id']) for _ in range(8)))
    require(
        len(reads) == 8
        and all(isinstance(result, dict) and result.get('id') == shot_id for result in reads),
        'one or more concurrent reads returned the wrong Shot',
    )


async def test_event_log_prefetch_matches_serial_paging(api_case: LiveCase) -> None:
    total = dictionary(
        await api_case.call('summarize', 'EventLogEntry', [], [{'field': 'id', 'type': 'count'}]),
        'EventLogEntry summary returned no dictionary',
    )
    summaries = dictionary(total.get('summaries'), 'EventLogEntry summary has no values')
    count = summaries.get('id')
    if not isinstance(count, int):
        pytest.fail('EventLogEntry count was not an integer')
    page_size = api_case.client.config.records_per_page
    if page_size <= 0:
        pytest.fail('server page size is unavailable')
    limit = 3 * page_size
    if count < limit:
        pytest.fail('site has too few EventLogEntry records for three-page prefetch verification')

    original = api_case.client.config.page_prefetch
    filters: list[list[object]] = []
    order = [{'field_name': 'id', 'direction': 'asc'}]
    try:
        api_case.client.config.page_prefetch = 0
        serial = _entities(
            await api_case.call(
                'find', 'EventLogEntry', filters, ['id'], order=order, limit=limit
            ),
            'serial EventLogEntry query returned invalid records',
        )
        api_case.client.config.page_prefetch = 3
        prefetched = _entities(
            await api_case.call(
                'find', 'EventLogEntry', filters, ['id'], order=order, limit=limit
            ),
            'prefetched EventLogEntry query returned invalid records',
        )
    finally:
        api_case.client.config.page_prefetch = original
    require(len(serial) == limit, 'serial EventLogEntry query returned fewer than three pages')
    require(
        [record_id(item) for item in prefetched] == [record_id(item) for item in serial],
        'prefetched EventLogEntry IDs differed from serial paging',
    )


async def test_nested_deep_link_and_recent_filters(api_case: LiveCase) -> None:
    code = f'{api_case.namespace}-linked-shot'
    shot = await api_case.create('Shot', {'code': code, 'project': api_case.project})
    version = await api_case.create(
        'Version',
        {
            'code': f'{api_case.namespace}-linked-version',
            'project': api_case.project,
            'entity': shot,
        },
    )
    filters = {
        'filter_operator': 'all',
        'filters': [
            ['project', 'is', api_case.project],
            {
                'filter_operator': 'any',
                'filters': [
                    ['entity.Shot.code', 'is', code],
                    ['code', 'is', f'{api_case.namespace}-no-match'],
                ],
            },
            ['created_at', 'in_last', 1, 'DAY'],
        ],
    }
    found = _entities(
        await api_case.call('find', 'Version', filters, ['id']), 'query returned invalid Versions'
    )
    require(
        [record_id(item) for item in found] == [record_id(version)],
        'nested recent deep-link query missed Version',
    )
    for include_archived_projects in (True, False):
        visible = dictionary(
            await api_case.call(
                'find_one',
                'Version',
                [['id', 'is', record_id(version)]],
                ['id'],
                include_archived_projects=include_archived_projects,
            ),
            'Version was not found with archive option',
        )
        require(
            record_id(visible) == record_id(version),
            'active Project result changed with archive option',
        )


async def test_record_fields_and_multi_entity_modes(api_case: LiveCase) -> None:
    first = await api_case.create(
        'Asset', {'code': f'{api_case.namespace}-asset-a', 'project': api_case.project}
    )
    second = await api_case.create(
        'Asset', {'code': f'{api_case.namespace}-asset-b', 'project': api_case.project}
    )
    code = f'{api_case.namespace}-shot-assets'
    shot = dictionary(
        await api_case.call(
            'create', 'Shot', {'code': code, 'project': api_case.project}, return_fields=['code']
        ),
        'create did not return a Shot',
    )
    shot_id = record_id(shot)
    api_case.track('Shot', shot_id)
    require(shot.get('code') == code, 'create return_fields did not include code')

    added = dictionary(
        await api_case.call(
            'update',
            'Shot',
            shot_id,
            {'assets': [first, second]},
            multi_entity_update_modes={'assets': 'add'},
        ),
        'multi-entity add returned no Shot',
    )
    require(added.get('id') == shot_id, 'multi-entity add returned a different Shot')
    fetched = dictionary(
        await api_case.call('find_one', 'Shot', [['id', 'is', shot_id]], ['assets']),
        'updated Shot was not found',
    )
    assets = _entities(fetched.get('assets'), 'multi-entity add returned no Assets')
    require(
        {record_id(asset) for asset in assets} == {record_id(first), record_id(second)},
        'multi-entity add did not persist both Assets',
    )

    removed = dictionary(
        await api_case.call(
            'update',
            'Shot',
            shot_id,
            {'assets': [first]},
            multi_entity_update_modes={'assets': 'remove'},
        ),
        'multi-entity remove returned no Shot',
    )
    require(removed.get('id') == shot_id, 'multi-entity remove returned a different Shot')
    fetched = dictionary(
        await api_case.call('find_one', 'Shot', [['id', 'is', shot_id]], ['assets']),
        'updated Shot was not found',
    )
    assets = _entities(fetched.get('assets'), 'multi-entity remove returned no Assets')
    require(
        [record_id(asset) for asset in assets] == [record_id(second)],
        'multi-entity remove did not leave the expected Asset',
    )


async def test_sorted_limit_and_page(api_case: LiveCase) -> None:
    prefix = f'{api_case.namespace}-ordered-'
    expected_ids: list[tuple[int, int]] = []
    for index in (2, 0, 1):
        shot = await api_case.create(
            'Shot', {'code': f'{prefix}{index}', 'project': api_case.project}
        )
        expected_ids.append((index, record_id(shot)))
    ordered_ids = [entity_id for _, entity_id in sorted(expected_ids)]
    filters = [['project', 'is', api_case.project], ['code', 'starts_with', prefix]]
    order = [{'field_name': 'code', 'direction': 'asc'}]
    first = _entities(
        await api_case.call('find', 'Shot', filters, ['id'], order=order, limit=2, page=1),
        'first sorted page returned invalid records',
    )
    second = _entities(
        await api_case.call('find', 'Shot', filters, ['id'], order=order, limit=2, page=2),
        'second sorted page returned invalid records',
    )
    require(
        [record_id(item) for item in first] == ordered_ids[:2], 'first sorted page was incorrect'
    )
    require(
        [record_id(item) for item in second] == ordered_ids[2:], 'second sorted page was incorrect'
    )


async def test_summary_types_and_grouping_depth(api_case: LiveCase) -> None:
    prefix = f'{api_case.namespace}-summary-'
    for index in range(2):
        await api_case.create('Shot', {'code': f'{prefix}{index}', 'project': api_case.project})
    filters = [['project', 'is', api_case.project], ['code', 'starts_with', prefix]]
    summary_fields = [
        {'field': 'id', 'type': 'count'},
        {'field': 'created_at', 'type': 'latest'},
    ]
    ungrouped = dictionary(
        await api_case.call('summarize', 'Shot', filters, summary_fields),
        'ungrouped summary returned no dictionary',
    )
    summaries = dictionary(ungrouped.get('summaries'), 'ungrouped summary has no values')
    require(
        summaries.get('id') == 2 and summaries.get('created_at') is not None,
        'count or latest summary was incorrect',
    )
    for grouping in (
        [{'field': 'project', 'type': 'exact', 'direction': 'asc'}],
        [
            {'field': 'project', 'type': 'exact', 'direction': 'asc'},
            {'field': 'code', 'type': 'exact', 'direction': 'asc'},
        ],
    ):
        grouped = dictionary(
            await api_case.call('summarize', 'Shot', filters, summary_fields, grouping=grouping),
            'grouped summary returned no dictionary',
        )
        groups = _entities(grouped.get('groups'), 'summary grouping returned invalid groups')
        require(bool(groups), 'summary grouping returned no groups')
        if len(grouping) == 2:
            require(
                any(
                    isinstance(group.get('groups'), list) and bool(group['groups'])
                    for group in groups
                ),
                'two-level summary did not return nested groups',
            )


async def test_text_search_with_and_without_project(api_case: LiveCase) -> None:
    term = api_case.namespace.rsplit('-', maxsplit=1)[-1][:12]
    shot = await api_case.create(
        'Shot', {'code': f'{api_case.namespace} {term}', 'project': api_case.project}
    )
    project_id = api_case.project['id']
    require(isinstance(project_id, int), 'test Project has no ID')
    for project_ids in (None, [project_id]):
        matched = False
        for _ in range(12):
            result = dictionary(
                await api_case.call(
                    'text_search', term, {'Shot': []}, project_ids=project_ids, limit=20
                ),
                'text_search returned no dictionary',
            )
            matches = _entities(result.get('matches'), 'text_search returned invalid matches')
            if any(record_id(item) == record_id(shot) for item in matches):
                matched = True
                break
            await asyncio.sleep(1)
        require(matched, 'text_search did not find the disposable Shot')
