from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit

from shotgun_api3 import ShotgunError
from shotgun_api3.lib.mockgun import MockgunError
from tests._live_vocabulary import PROBE_KEYS
from tests.live._evidence import exception_label

if TYPE_CHECKING:
    from tests.live._verification import LiveCase

_FILE_KEYS = {'url', 'name', 'content_type', 'link_type', 'type', 'id'}
_TYPE_VALUES = {'Attachment', 'attachment', 'Upload', 'upload', 'File', 'file', 'Note', ''}


def _shape(value: dict[str, object], keys: set[str]) -> dict[str, object]:
    types: dict[type[object], str] = {
        str: 'str',
        int: 'int',
        float: 'float',
        bool: 'bool',
        dict: 'dict',
        list: 'list',
        type(None): 'null',
    }
    return {
        'keys': [key if key in keys else 'unexpected-key' for key in value],
        'types': [types.get(type(item), 'other') for item in value.values()],
    }


async def _stable_stream(
    case: LiveCase, record: dict[str, object]
) -> tuple[dict[str, object], int]:
    previous = None
    for attempt in range(1, 6):
        current = cast(
            'dict[str, object]',
            await case.call('activity_stream_read', record['type'], record['id']),
        )
        if current == previous:
            return current, attempt
        previous = current
        if attempt < 5:
            await case.advance(2)
    raise MockgunError('probe activity stream stability is unsettled')


def creator_observation(
    entry: dict[str, object], human: dict[str, object], script: dict[str, object]
) -> dict[str, object]:
    creator = cast('dict[str, object]', entry.get('created_by', {}))
    meta = cast('dict[str, object]', entry.get('meta', {}))
    sudo = cast('dict[str, object]', meta.get('sudo_actual_user', {}))
    return {
        'creator': _shape(creator, {'type', 'id', 'name', 'status', 'image'}),
        'type_equal': creator.get('type') == 'HumanUser',
        'id_equal': creator.get('id') == human['id'],
        'name_equal': creator.get('name') == human.get('name'),
        'status_equal': creator.get('status') == human.get('sg_status_list'),
        'status_null': creator.get('status') is None,
        'image_null': creator.get('image') is None,
        'human_image_null': human.get('image') is None,
        'status_value': creator.get('status')
        if creator.get('status') in ('act', 'dis')
        else 'other',
        'meta': _shape(meta, {'type', 'entity_type', 'entity_id', 'sudo_actual_user'}),
        'sudo': _shape(sudo, {'type', 'id', 'name', 'valid', 'uuid'}),
        'sudo_type_equal': sudo.get('type') == 'ApiUser',
        'sudo_id_equal': sudo.get('id') == script.get('id'),
    }


def file_observation(
    value: dict[str, object], identity: int, display: str, site_host: str
) -> dict[str, object]:
    url = urlsplit(str(value.get('url', '')))
    kind = value.get('type')
    return {
        **_shape(value, _FILE_KEYS),
        'type_value': kind if isinstance(kind, str) and kind in _TYPE_VALUES else 'none',
        'type_length': len(kind) if isinstance(kind, str) else 0,
        'name_equal': value.get('name') == display,
        'id_equal': value.get('id') == identity,
        'content_type_equal': value.get('content_type') == 'text/plain',
        'link_type_equal': value.get('link_type') == 'upload',
        'host': 'site'
        if url.hostname == site_host
        else 'storage'
        if (url.hostname or '').endswith(('.amazonaws.com', '.cloudfront.net'))
        else 'other',
        'path_has_id': str(identity) in url.path,
    }


def settled_creators(observations: list[dict[str, object]]) -> bool:
    required = ('type_equal', 'id_equal', 'name_equal', 'image_null', 'human_image_null')
    return len(observations) == 2 and all(
        set(cast('list[str]', cast('dict[str, object]', row['creator'])['keys']))
        == {'type', 'id', 'name', 'status', 'image'}
        and all(row[key] is True for key in required)
        and row['status_equal'] != row['status_null']
        and row['status_equal'] == observations[0]['status_equal']
        for row in observations
    )


def settled_files(observations: list[dict[str, object]]) -> bool:
    required = ('name_equal', 'id_equal', 'content_type_equal', 'link_type_equal')
    return len(observations) == 2 and all(
        set(cast('list[str]', row['keys'])) == _FILE_KEYS
        and row['type_value'] in _TYPE_VALUES
        and row['type_value'] == observations[0]['type_value']
        and all(row[key] is True for key in required)
        for row in observations
    )


async def attachment(case: LiveCase) -> dict[str, object]:
    note = await case.create(
        'Note', {'project': case.project, 'subject': f'{case.namespace}-o1', 'content': 'body'}
    )
    display = f'{case.namespace}-Shown'
    with TemporaryDirectory() as directory:
        path = Path(directory) / f'{case.namespace}-o1.txt'
        path.write_bytes(b'a' * 40)
        identity = cast(
            'int', await case.call('upload', 'Note', note['id'], str(path), display_name=display)
        )
    direct = cast(
        'dict[str, object]',
        await case.call('find_one', 'Attachment', [['id', 'is', identity]], ['this_file']),
    )
    thread = cast(
        'list[dict[str, object]]',
        await case.call(
            'note_thread_read', note['id'], entity_fields={'Attachment': ['this_file']}
        ),
    )
    attachments = [
        row for row in thread if row.get('type') == 'Attachment' and row.get('id') == identity
    ]
    if not attachments:
        raise MockgunError('probe Attachment thread observation is unsettled')
    first, second = (
        cast('dict[str, object]', direct['this_file']),
        cast('dict[str, object]', attachments[0]['this_file']),
    )
    target = getattr(case, 'target', None)
    host = target.site_host if target is not None else ''
    observations = [file_observation(row, identity, display, host) for row in (first, second)]
    return {
        'status': 'Settled' if settled_files(observations) else 'Unsettled',
        'error': '',
        'reads': observations,
        'equal': first == second,
    }


def sudo_matches(observations: list[dict[str, object]]) -> bool:
    return bool(observations) and all(
        set(cast('list[str]', cast('dict[str, object]', row['sudo'])['keys']))
        == {'type', 'id', 'name', 'valid', 'uuid'}
        and row['sudo_type_equal'] is True
        and row['sudo_id_equal'] is True
        for row in observations
    )


async def _creator(case: LiveCase) -> dict[str, object]:
    assert case.target is not None
    human = cast(
        'dict[str, object]',
        await case.call(
            'find_one',
            'HumanUser',
            [['id', 'is', case.target.human['id']]],
            ['name', 'sg_status_list', 'image'],
        ),
    )
    script = cast(
        'dict[str, object]',
        await case.call('find_one', 'ApiUser', [['firstname', 'is', case.script_name]], ['id']),
    )
    shot = await case.create('Shot', {'project': case.project, 'code': f'{case.namespace}-o2'})
    note = cast(
        'dict[str, object]',
        await case.sudo_call(
            'create',
            'Note',
            {'note_links': [shot], 'subject': f'{case.namespace}-o2', 'project': case.project},
        ),
    )
    observations: list[dict[str, object]] = []
    for record in (note, shot):
        stream, attempt = await _stable_stream(case, record)
        entries = cast('list[dict[str, object]]', stream.get('updates', []))
        matches = [
            entry
            for entry in entries
            if entry.get('update_type') == 'create'
            and cast('dict[str, object]', entry.get('primary_entity', {})).get('id') == note['id']
        ]
        if not matches or not isinstance(matches[0].get('created_by'), dict):
            raise MockgunError('probe HumanUser or absent created_by is unsettled')
        observations.append({**creator_observation(matches[0], human, script), 'attempt': attempt})
    sudo_passed = sudo_matches(observations)
    return {
        'status': 'Settled' if settled_creators(observations) else 'Unsettled',
        'error': '',
        'streams': observations,
        'sudo_metadata': sudo_passed,
    }


async def run_probe(case: LiveCase) -> dict[str, object]:
    results: dict[str, object] = {}
    first_key, second_key = PROBE_KEYS
    try:
        results[first_key] = await attachment(case)
    except (MockgunError, ShotgunError) as error:
        results[first_key] = {
            'status': 'Unsettled',
            'error': exception_label(error),
            'reads': [],
            'equal': False,
        }
    try:
        results[second_key] = await _creator(case)
    except (MockgunError, ShotgunError) as error:
        results[second_key] = {
            'status': 'Unsettled',
            'error': exception_label(error),
            'streams': [],
            'sudo_metadata': False,
        }
    return results
