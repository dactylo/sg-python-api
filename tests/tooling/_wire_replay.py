from __future__ import annotations

import copy
from typing import TYPE_CHECKING, cast

from scripts.probe_wire import _FixtureError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from shotgun_api3._async_client import AsyncShotgun
    from shotgun_api3._client import Shotgun


_REPLAY_RECIPES: Mapping[
    str,
    Callable[[Shotgun, Mapping[str, object] | None], object],
] = {
    'activity_stream_read': lambda sg, body: sg.activity_stream_read(
        'Project',
        _replay_int(body, 'id', 1),
    ),
    'batch': lambda sg, body: sg.batch(_replay_batch_requests(body)),
    'create': lambda sg, body: sg.create('Note', _replay_create_data(body)),
    'fault_bad_auth': lambda sg, _body: sg.find_one('Project', [], ['id']),
    'following': lambda sg, body: sg.following(
        _replay_entity(body, 'user', {'type': 'HumanUser', 'id': 1})
    ),
    'get_session_token': lambda sg, _body: sg.get_session_token(),
    'info': lambda sg, _body: sg.info(),
    'nav_expand': lambda sg, _body: sg.nav_expand('/'),
    'nav_search_entity': lambda sg, body: sg.nav_search_entity(
        _replay_str(body, 'root_path', '/Project/1'),
        _replay_nav_entity(body),
        _replay_str(body, 'seed_entity_field', 'Asset.project'),
    ),
    'nav_search_string': lambda sg, body: sg.nav_search_string(
        _replay_str(body, 'root_path', '/Project/1'),
        _replay_nav_text(body),
        _replay_str(body, 'seed_entity_field', 'Asset.project'),
    ),
    'note_thread_read': lambda sg, body: sg.note_thread_read(_replay_int(body, 'note_id', 1)),
    'preferences_read': lambda sg, _body: sg.preferences_read(),
    'read': lambda sg, _body: sg.find('Project', [], ['name'], limit=2),
    'schema_entity_read': lambda sg, _body: sg.schema_entity_read(),
    'schema_read': lambda sg, _body: sg.schema_read(),
    'summarize': lambda sg, _body: sg.summarize(
        'Project',
        [],
        [{'field': 'id', 'type': 'count'}],
    ),
    'text_search': lambda sg, body: sg.text_search(
        _replay_str(body, 'text', 'wire probe'),
        {'Project': []},
    ),
    'update_project_last_accessed': lambda sg, body: sg.update_project_last_accessed(
        {'type': 'Project', 'id': _replay_int(body, 'project_id', 1)},
        user={'type': 'HumanUser', 'id': _replay_int(body, 'user_id', 1)},
    ),
    'user_subscriptions_read': lambda sg, _body: sg.user_subscriptions_read(),
    'work_schedule_read': lambda sg, _body: sg.work_schedule_read(
        '2026-01-05',
        '2026-01-09',
    ),
}

_ASYNC_REPLAY_RECIPES: Mapping[
    str,
    Callable[[AsyncShotgun, Mapping[str, object] | None], Awaitable[object]],
] = {
    'nav_search_entity': lambda sg, body: sg.nav_search_entity(
        _replay_str(body, 'root_path', '/Project/1'),
        _replay_nav_entity(body),
        _replay_str(body, 'seed_entity_field', 'Asset.project'),
    ),
    'nav_search_string': lambda sg, body: sg.nav_search_string(
        _replay_str(body, 'root_path', '/Project/1'),
        _replay_nav_text(body),
        _replay_str(body, 'seed_entity_field', 'Asset.project'),
    ),
    'note_thread_read': lambda sg, body: sg.note_thread_read(_replay_int(body, 'note_id', 1)),
    'user_subscriptions_read': lambda sg, _body: sg.user_subscriptions_read(),
}


def _replay_field_data(value: Mapping[str, object]) -> dict[str, object]:
    fields = value.get('fields')
    if not isinstance(fields, list):
        raise _FixtureError('recorded field data is malformed')
    data: dict[str, object] = {}
    for item in cast('list[object]', fields):
        if not isinstance(item, dict):
            raise _FixtureError('recorded field data is malformed')
        field = cast('dict[str, object]', item)
        name = field.get('field_name')
        if not isinstance(name, str) or 'value' not in field:
            raise _FixtureError('recorded field data is malformed')
        data[name] = copy.deepcopy(field['value'])
    return data


def _replay_wire_params(body: Mapping[str, object]) -> object:
    params = body.get('params')
    if not isinstance(params, list) or not params:
        raise _FixtureError('recorded RPC params are malformed')
    return cast('list[object]', params)[-1]


def _replay_batch_requests(
    body: Mapping[str, object] | None,
) -> list[dict[str, object]]:
    if body is None:
        return [
            {
                'request_type': 'create',
                'entity_type': 'Note',
                'data': {
                    'subject': 'sg-python-api-wire-probe-replay-batch',
                    'project': {'type': 'Project', 'id': 1},
                },
            }
        ]
    requests = _replay_wire_params(body)
    if not isinstance(requests, list):
        raise _FixtureError('recorded batch params are malformed')
    rebuilt: list[dict[str, object]] = []
    for value in cast('list[object]', requests):
        if not isinstance(value, dict):
            raise _FixtureError('recorded batch request is malformed')
        request = cast('dict[str, object]', value)
        request_type = request.get('request_type')
        entity_type = request.get('type')
        if not isinstance(request_type, str) or not isinstance(entity_type, str):
            raise _FixtureError('recorded batch request is malformed')
        rebuilt.append(
            {
                'request_type': request_type,
                'entity_type': entity_type,
                'data': _replay_field_data(request),
            }
        )
    return rebuilt


def _replay_create_data(body: Mapping[str, object] | None) -> dict[str, object]:
    if body is None:
        return {
            'subject': 'sg-python-api-wire-probe-replay',
            'project': {'type': 'Project', 'id': 1},
        }
    params = _replay_wire_params(body)
    if not isinstance(params, dict):
        raise _FixtureError('recorded create params are malformed')
    return _replay_field_data(cast('dict[str, object]', params))


def _replay_param_mapping(body: Mapping[str, object]) -> dict[str, object]:
    value = _replay_wire_params(body)
    if not isinstance(value, dict):
        raise _FixtureError('recorded operation params are malformed')
    return cast('dict[str, object]', value)


def _replay_entity(
    body: Mapping[str, object] | None,
    key: str,
    default: dict[str, object],
) -> dict[str, object]:
    if body is None:
        return copy.deepcopy(default)
    value = _replay_param_mapping(body).get(key)
    if not isinstance(value, dict):
        raise _FixtureError(f'recorded {key} entity is malformed')
    return copy.deepcopy(cast('dict[str, object]', value))


def _replay_int(body: Mapping[str, object] | None, key: str, default: int) -> int:
    if body is None:
        return default
    value = _replay_param_mapping(body).get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise _FixtureError(f'recorded {key} argument is malformed')
    return value


def _replay_nav_entity(body: Mapping[str, object] | None) -> dict[str, object]:
    if body is None:
        return {'type': 'Asset', 'id': 1}
    criteria = _replay_param_mapping(body).get('search_criteria')
    if not isinstance(criteria, dict):
        raise _FixtureError('recorded navigation criteria are malformed')
    entity = cast('dict[str, object]', criteria).get('entity')
    if not isinstance(entity, dict):
        raise _FixtureError('recorded navigation entity is malformed')
    return copy.deepcopy(cast('dict[str, object]', entity))


def _replay_nav_text(body: Mapping[str, object] | None) -> str:
    if body is None:
        return 'Fixture asset'
    criteria = _replay_param_mapping(body).get('search_criteria')
    if not isinstance(criteria, dict):
        raise _FixtureError('recorded navigation criteria are malformed')
    text = cast('dict[str, object]', criteria).get('search_string')
    if not isinstance(text, str):
        raise _FixtureError('recorded navigation text is malformed')
    return text


def _replay_str(body: Mapping[str, object] | None, key: str, default: str) -> str:
    if body is None:
        return default
    value = _replay_param_mapping(body).get(key)
    if not isinstance(value, str):
        raise _FixtureError(f'recorded {key} argument is malformed')
    return value
