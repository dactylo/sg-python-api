"""Dispatch the RPC methods currently emulated by the fake site."""

from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING, TypeAlias, cast

from . import _site_data
from ._activity import (
    read_stream,
    record_create_activity,
    record_lifecycle_activity,
    record_update_activity,
    taint_activity,
)
from ._errors import MockgunError, ServerFaultError
from ._events import record_create, record_lifecycle, record_schema_write, record_update
from ._follows import follow, follow_created_note, followers, following, unfollow
from ._query import ReadScope, page, project_rows, select
from ._retirement import kept_members, retire, revive
from ._schema_writes import create_field, delete_field, update_field
from ._search import search
from ._sessions import RequestContext
from ._store import CONNECTION_FIELDS
from ._summaries import summarize
from ._threads import read_thread
from ._wire import RpcCall

if TYPE_CHECKING:
    from ._server import FakeServer
    from ._store import FieldValue

_Handler: TypeAlias = Callable[['FakeServer', RpcCall, RequestContext], object]
HANDLERS: dict[str, _Handler] = {}
UNWRAPPED_METHODS: set[str] = set()
_CONNECTION_ENDPOINTS = {
    entity_type: frozenset({'id', side_a, side_b})
    for entity_type, side_a, side_b in CONNECTION_FIELDS.values()
}


def _check_connection_path(entity_type: str, path: str, allowed: frozenset[str]) -> None:
    """Reject a path whose first segment is outside a connection's surface."""
    if path.split('.', 1)[0] not in allowed:
        raise MockgunError(f'mockgun cannot read {entity_type}.{path}')


def _check_connection_tree(
    entity_type: str, tree: dict[str, object], allowed: frozenset[str]
) -> None:
    """Check all filter paths, including nested filter groups."""
    for condition in cast('list[dict[str, object]]', tree.get('conditions', [])):
        if 'path' in condition:
            _check_connection_path(entity_type, cast('str', condition['path']), allowed)
        elif 'conditions' in condition:
            _check_connection_tree(entity_type, condition, allowed)


def _check_connection_read(
    entity_type: str,
    return_fields: list[str],
    filters: dict[str, object],
    order: list[dict[str, str]],
) -> None:
    """Reject paths outside the observed identity and endpoint surface."""
    if not entity_type.endswith('Connection'):
        return
    allowed = _CONNECTION_ENDPOINTS.get(entity_type)
    if allowed is None:
        raise MockgunError(f'mockgun cannot read unmodeled {entity_type}')
    for path in return_fields:
        _check_connection_path(entity_type, path, allowed)
    _check_connection_tree(entity_type, filters, allowed)
    for entry in order:
        _check_connection_path(entity_type, entry['field_name'], allowed)


def _check_event_write(server: 'FakeServer', entity_type: str) -> None:
    """Reject direct event mutation once event identities and contents are unknown."""
    if entity_type == 'EventLogEntry' and server.store.events_unknown is not None:
        raise MockgunError(f'EventLogEntry is unknown: {server.store.events_unknown}')


def _fields(params: dict[str, object]) -> tuple[dict[str, object], dict[str, str]]:
    """Split wire field entries into values and per-field update modes."""
    fields: dict[str, object] = {}
    modes: dict[str, str] = {}
    for entry in cast('list[dict[str, object]]', params['fields']):
        name = cast('str', entry['field_name'])
        fields[name] = entry['value']
        if 'multi_entity_update_mode' in entry:
            modes[name] = cast('str', entry['multi_entity_update_mode'])
    return fields, modes


def _follow_params(
    method: str, raw: object, required: tuple[str, ...], optional: tuple[str, ...] = ()
) -> dict[str, object]:
    """Check the exact JSON object accepted by one follow RPC."""
    if not isinstance(raw, dict):
        raise MockgunError(f'mockgun cannot {method} parameter params: expected object')
    params = cast('dict[str, object]', raw)
    missing = set(required) - params.keys()
    if missing:
        raise MockgunError(f'mockgun cannot {method} parameter {min(missing)}: missing')
    extra = params.keys() - set(required) - set(optional)
    if extra:
        raise MockgunError(f'mockgun cannot {method} parameter {min(extra)}: unexpected')
    for key, value in params.items():
        valid = isinstance(value, str) if key == 'entity_type' else isinstance(value, dict)
        if not valid:
            raise MockgunError(f'mockgun cannot {method} parameter {key}: wrong JSON type')
    return params


def _register(name: str, *, wrapped: bool = True) -> Callable[[_Handler], _Handler]:
    """Register one handler under its wire method name."""

    def decorate(handler: _Handler) -> _Handler:
        HANDLERS[name] = handler
        if not wrapped:
            UNWRAPPED_METHODS.add(name)
        return handler

    return decorate


@_register('export_page')
def _export_page(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated export_page site data parameters."""
    return _site_data.export_page(server, call.params, context=context)


@_register('nav_expand', wrapped=False)
def _nav_expand(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated nav_expand site data parameters."""
    return _site_data.nav_expand(server, call.params, context=context)


@_register('nav_search', wrapped=False)
def _nav_search(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated nav_search site data parameters."""
    return _site_data.nav_search(server, call.params, context=context)


@_register('preferences_read', wrapped=False)
def _preferences_read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated preferences_read site data parameters."""
    return _site_data.preferences_read(server, call.params, context=context)


def _schema_project_id(params: object) -> int | None:
    """Extract a Project ID from optional schema-read parameters."""
    if not isinstance(params, dict):
        return None
    project = cast('dict[str, object]', params).get('project')
    if not isinstance(project, dict):
        return None
    link = cast('dict[str, object]', project)
    identity = link.get('id')
    return (
        identity
        if link.get('type') == 'Project'
        and isinstance(identity, int)
        and not isinstance(identity, bool)
        else None
    )


@_register('user_subscriptions_read')
def _user_subscriptions_read(
    server: 'FakeServer', call: RpcCall, context: RequestContext
) -> object:
    """Dispatch validated user_subscriptions_read site data parameters."""
    return _site_data.user_subscriptions_read(server, call.params, context=context)


@_register('user_subscriptions_create')
def _user_subscriptions_create(
    server: 'FakeServer', call: RpcCall, context: RequestContext
) -> object:
    """Dispatch validated user_subscriptions_create site data parameters."""
    return _site_data.user_subscriptions_create(server, call.params, context=context)


@_register('update_project_last_accessed_by_current_user')
def _update_project_last_accessed_by_current_user(
    server: 'FakeServer', call: RpcCall, context: RequestContext
) -> object:
    """Dispatch validated update_project_last_accessed_by_current_user site data parameters."""
    return _site_data.update_project_last_accessed_by_current_user(
        server, call.params, context=context
    )


@_register('work_schedule_read')
def _work_schedule_read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated work_schedule_read site data parameters."""
    return _site_data.work_schedule_read(server, call.params, context=context)


@_register('work_schedule_update')
def _work_schedule_update(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Dispatch validated work_schedule_update site data parameters."""
    return _site_data.work_schedule_update(server, call.params, context=context)


@_register('activity_stream', wrapped=False)
def _activity_stream(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return the unwrapped observed activity view."""
    params: object = call.params
    if not isinstance(params, dict):
        raise MockgunError('mockgun cannot activity_stream parameter params')
    return read_stream(server.store, cast('dict[str, object]', params), context=context)


@_register('create')
def _create(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Create a record and return requested or supplied fields plus its identity."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    _check_event_write(server, entity_type)
    fields, _ = _fields(params)
    return_fields = cast('list[str]', params['return_fields'])
    result_fields = list(fields)
    for name in return_fields:
        if '.' in name:
            raise MockgunError(f'mockgun create() cannot return linked field path {name}')
        if server.schema.declares(entity_type, name) and name not in result_fields:
            result_fields.append(name)

    candidate = server.store.prepare_create(entity_type, fields, context=context)
    project_rows(server.store.preview(candidate), server.schema, [candidate], result_fields)
    with server.store.transaction():
        row = server.store.create(entity_type, fields, context=context)
        result = project_rows(server.store, server.schema, [row], result_fields)[0]
        settled = record_create(server.store, row, list(fields), context=context)
        follow_created_note(server.store, row, context=context)
        record_create_activity(server.store, row, settled=settled, context=context)
    return result


@_register('delete')
def _delete(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Retire one existing record."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    _check_event_write(server, entity_type)
    with server.store.transaction():
        effects = retire(server.store, entity_type, cast('int', params['id']), context=context)
        record_lifecycle(server.store, effects, revival=False, context=context)
        record_lifecycle_activity(server.store, effects, context=context)
    return effects.changed


@_register('follow')
def _follow(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Follow an active entity for an active HumanUser."""
    del context
    return follow(server.store, _follow_params('follow', call.params, ('user', 'entity')))


@_register('followers')
def _followers(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Read the observed follower shape for one active entity."""
    del context
    return followers(server.store, _follow_params('followers', call.params, ('entity',)))


@_register('following')
def _following(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Read a HumanUser's followed entities with optional filters."""
    del context
    return following(
        server.store,
        _follow_params('following', call.params, ('user',), ('project', 'entity_type')),
    )


@_register('get_session_token', wrapped=False)
def _get_session_token(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Issue an unwrapped token response retaining the current credentials' actor."""
    del call
    actor = context.actor
    if actor is None:
        raise ServerFaultError('Authentication failed.', error_code=102)
    return {
        'session_id': server.sessions.issue_token(actor),
        'user_type': actor.entity_type,
        'user_id': actor.entity_id,
    }


@_register('note_thread_contents', wrapped=False)
def _note_thread_contents(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return an unwrapped Note thread for validated wire parameters."""
    params: object = call.params
    if not isinstance(params, dict):
        raise MockgunError('mockgun cannot note_thread_contents parameter params')
    return read_thread(server.store, cast('dict[str, object]', params), context=context)


@_register('read')
def _read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Select, page, and project a query using the site's current maximum width."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    return_fields = cast('list[str]', params['return_fields'])
    if server.schema.declares(entity_type):
        _check_connection_read(
            entity_type,
            return_fields,
            cast('dict[str, object]', params['filters']),
            cast('list[dict[str, str]]', params.get('sorts', [])),
        )
    presets = params.get('additional_filter_presets', [])
    # ReadScope's None means omitted; preserve explicit wire null as a non-list.
    if presets is None:
        presets = ()
    rows = select(
        server.store,
        entity_type,
        cast('dict[str, object]', params['filters']),
        cast('list[dict[str, str]]', params.get('sorts', [])),
        scope=ReadScope(
            retired=params.get('return_only') == 'retired',
            context=context,
            include_archived_projects=params.get('include_archived_projects') is not False,
            presets=presets,
        ),
    )
    paging = cast('dict[str, int]', params['paging'])
    selected, paging_info = page(
        rows,
        entities_per_page=paging['entities_per_page'],
        current_page=paging['current_page'],
        max_page_size=cast('int', server.server_metadata()['api_max_entities_per_page']),
    )
    return {
        'entities': project_rows(
            server.store, server.schema, selected, return_fields, context=context
        ),
        'paging_info': paging_info,
    }


@_register('query_display_name_cache', wrapped=False)
def _search(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return matches and terms for a supported text search."""
    params: object = call.params
    if not isinstance(params, dict):
        raise MockgunError('mockgun cannot query_display_name_cache this request yet')
    return search(server.store, server.schema, cast('dict[str, object]', params), context=context)


@_register('summarize')
def _summarize(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return totals and groups for a supported summary request."""
    params: object = call.params
    if not isinstance(params, dict):
        raise MockgunError('mockgun cannot summarize this request yet')
    return summarize(
        server.store, server.schema, cast('dict[str, object]', params), context=context
    )


@_register('revive')
def _revive(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Restore one existing retired record."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    _check_event_write(server, entity_type)
    with server.store.transaction():
        effects = revive(server.store, entity_type, cast('int', params['id']), context=context)
        record_lifecycle(server.store, effects, revival=True, context=context)
        record_lifecycle_activity(server.store, effects, context=context)
    return effects.changed


@_register('update')
def _update(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Update a record and return the submitted fields plus its identity."""
    params = cast('dict[str, object]', call.params)
    fields, modes = _fields(params)

    entity_type = cast('str', params['type'])
    entity_id = cast('int', params['id'])
    _check_event_write(server, entity_type)
    kept = kept_members(server.store, entity_type, entity_id, fields)
    effective = server.store.kept_fields(
        server.store.require(entity_type, entity_id), fields, modes, kept
    )
    effective_modes = {**modes, **dict.fromkeys(kept, 'set')}
    candidate = server.store.prepare_update(
        entity_type, entity_id, effective, effective_modes, context=context
    )
    result = project_rows(
        server.store.preview(candidate), server.schema, [candidate], list(fields)
    )[0]
    with server.store.transaction():
        before = deepcopy(server.store.require(entity_type, entity_id))
        row = server.store.update(
            entity_type, entity_id, effective, effective_modes, context=context
        )
        for field in kept:
            if len(cast('list[FieldValue]', row[field])) > 1:
                server.store.mark_unknown(
                    row, field, 'retained hidden members have unsettled order', order_only=True
                )
        settled = record_update(
            server.store,
            before,
            row,
            {field: modes.get(field, 'set') for field in fields},
            context=context,
        )
        record_update_activity(server.store, before, row, settled=settled, context=context)
        if {'image', 'filmstrip_image'} & fields.keys():
            taint_activity(
                server.store,
                [before, row],
                f'update {entity_type} {entity_id} image fields: activity is unsettled',
            )
    return result


@_register('unfollow')
def _unfollow(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Remove a HumanUser follow of an active entity."""
    del context
    return unfollow(server.store, _follow_params('unfollow', call.params, ('user', 'entity')))


@_register('batch')
def _batch(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Apply writes in order, restoring all tables if any request fails."""
    results: list[object] = []
    with server.store.transaction():
        for params in cast('list[dict[str, object]]', call.params):
            method = cast('str', params['request_type'])
            results.append(HANDLERS[method](server, RpcCall(method, call.auth, params), context))
    return results


@_register('info', wrapped=False)
def _info(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return the site's current readiness metadata."""
    del call, context
    return server.server_metadata()


@_register('schema_entity_read')
def _schema_entity_read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return entity metadata without Project-specific visibility changes."""
    del context
    project_id = _schema_project_id(call.params)
    if project_id is not None:
        raise MockgunError(
            f'schema_entity_read Project {project_id} project-specific entity schema is unsettled'
        )
    return server.schema.entities


@_register('schema_field_create')
def _schema_field_create(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Create a field in the site's mutable schema and existing records."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    data_type = cast('str', params['data_type'])
    properties = cast('list[dict[str, object]]', params['properties'])
    field_name, _ = create_field(server.schema, entity_type, data_type, properties)
    server.store.add_field(entity_type, field_name)
    record_schema_write(server.store, 'create', data_type, properties, context=context)
    return field_name


@_register('schema_field_delete')
def _schema_field_delete(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Delete one custom field and reserve its internal name."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    field_name = cast('str', params['field_name'])
    field = server.schema.fields.get(entity_type, {}).get(field_name)
    data_type = ''
    if isinstance(field, dict):
        metadata = cast('dict[str, object]', field)
        data_type = cast('str', cast('dict[str, object]', metadata['data_type'])['value'])
    delete_field(server.schema, entity_type, field_name)
    server.store.drop_field(entity_type, field_name)
    record_schema_write(server.store, 'delete', data_type, [], context=context)
    return True


@_register('schema_field_read')
def _schema_field_read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Select field metadata for an entity and optional field name."""
    del context
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    if not server.schema.declares(entity_type):
        raise ServerFaultError(f'{entity_type} is not a valid entity')
    fields = server.schema.scoped(_schema_project_id(params), entity_type)[entity_type]
    field_name = params.get('field_name')
    if field_name is None:
        return fields
    if field_name not in fields:
        raise ServerFaultError(
            f"API schema_field_read() {entity_type}.{field_name} doesn't exist:\n"
            f'{{"type" => "{entity_type}", "field_name" => "{field_name}"}}',
            error_code=103,
        )
    return {cast('str', field_name): fields[cast('str', field_name)]}


@_register('schema_field_update')
def _schema_field_update(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Validate Project scope and update one custom field's metadata."""
    params = cast('dict[str, object]', call.params)
    entity_type = cast('str', params['type'])
    field_name = cast('str', params['field_name'])
    project = params.get('project')
    project_id: int | None = None
    if project is not None:
        if not isinstance(project, dict):
            raise MockgunError(
                f'schema_field_update {entity_type}.{field_name}: project link is unsettled'
            )
        project_link = cast('dict[str, object]', project)
        if project_link.get('type') != 'Project':
            raise MockgunError(
                f'schema_field_update {entity_type}.{field_name}: project link is unsettled'
            )
        identity = project_link.get('id')
        if (
            not isinstance(identity, int)
            or isinstance(identity, bool)
            or not server.schema.declares('Project')
            or server.store.row('Project', identity) is None
        ):
            raise MockgunError(
                f'schema_field_update {entity_type}.{field_name}: project link is unsettled'
            )
        project_id = identity
    properties = cast('list[dict[str, object]]', params['properties'])
    field = server.schema.fields.get(entity_type, {}).get(field_name)
    data_type = ''
    if isinstance(field, dict):
        metadata = cast('dict[str, object]', field)
        data_type = cast('str', cast('dict[str, object]', metadata['data_type'])['value'])
    update_field(server.schema, entity_type, field_name, properties, project_id=project_id)
    record_schema_write(server.store, 'update', data_type, properties, context=context)
    return True


@_register('schema_read')
def _schema_read(server: 'FakeServer', call: RpcCall, context: RequestContext) -> object:
    """Return the loaded field metadata without added declarations."""
    del context
    return server.schema.scoped(_schema_project_id(call.params))
