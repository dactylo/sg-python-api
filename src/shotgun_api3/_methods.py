"""Shared pure plans for synchronous and asynchronous client methods."""

import contextvars
import dataclasses
import datetime
import inspect
import warnings
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, NotRequired, TypedDict, cast

from . import _crud, _protocol
from ._config import CompatibilityWarningCode
from ._errors import ShotgunError
from ._signatures import bind_arguments, expose_signature

_ACTIVITY_STREAM_READ_DOCSTRING = """Read the activity stream for one entity.

Args:
    entity_type: Entity type whose activity stream to read.
    entity_id: Numeric identifier of the entity.
    entity_fields: Optional fields to return for linked entity types.
    min_id: Optional minimum activity identifier.
    max_id: Optional maximum activity identifier.
    limit: Optional maximum number of activity records.

Returns:
    Activity stream data returned by the server.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if the server result has the wrong type.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_ACTIVITY_STREAM_READ_PARAMETERS = (
    inspect.Parameter('entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter('entity_id', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=int),
    inspect.Parameter(
        'entity_fields',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=dict[str, list[str]] | None,
    ),
    inspect.Parameter(
        'min_id',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=int | None,
    ),
    inspect.Parameter(
        'max_id',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=int | None,
    ),
    inspect.Parameter(
        'limit',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=int | None,
    ),
)
_ACTIVITY_STREAM_READ_SIGNATURE = inspect.Signature(
    _ACTIVITY_STREAM_READ_PARAMETERS,
    return_annotation=dict[str, object],
)
_EXPORT_PAGE_DOCSTRING = """Export a page using the documented arguments.

Args:
    page_id: Numeric page identifier.
    format: Required nonempty export format name.
    layout_name: Optional layout name, including an empty name.

Returns:
    Exported page text returned by the server.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if `page_id`, `format`, or the server result is invalid.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_EXPORT_PAGE_PARAMETERS = (
    inspect.Parameter('page_id', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=int),
    inspect.Parameter('format', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter(
        'layout_name',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
)
_EXPORT_PAGE_SIGNATURE = inspect.Signature(
    _EXPORT_PAGE_PARAMETERS,
    return_annotation=str,
)
_METHOD_ACTIVITY_STREAM_READ_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_ACTIVITY_STREAM_READ_PARAMETERS,
    ),
    return_annotation=dict[str, object],
)
_METHOD_EXPORT_PAGE_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_EXPORT_PAGE_PARAMETERS,
    ),
    return_annotation=str,
)
_PENDING_COMPATIBILITY_WARNINGS = tuple[
    object,
    tuple[CompatibilityWarningCode, ...],
]
_SCHEDULE_DATE_LENGTH = 10
_SUMMARIZE_DOCSTRING = """Summarize field data returned by a query.

Args:
    entity_type: Entity type to summarize.
    filters: Filters in list or dict syntax.
    summary_fields: List of `{'field': ..., 'type': ...}` summaries.
    filter_operator: `'all'` or `'any'` for list-syntax filters.
    grouping: Optional grouping specs.
    include_archived_projects: Include archived projects when `True`.

Returns:
    Dict with `summaries` and (when grouped) `groups`.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if `filters`, `summary_fields`, or the server result has the
        wrong type.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_SUMMARIZE_PARAMETERS = (
    inspect.Parameter('entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter(
        'filters',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=list[Any] | tuple[Any, ...] | dict[str, Any],
    ),
    inspect.Parameter(
        'summary_fields',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=list[dict[str, Any]],
    ),
    inspect.Parameter(
        'filter_operator',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'grouping',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=list[dict[str, Any]] | None,
    ),
    inspect.Parameter(
        'include_archived_projects',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=True,
        annotation=bool,
    ),
)
_METHOD_SUMMARIZE_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_SUMMARIZE_PARAMETERS,
    ),
    return_annotation=dict[str, object],
)
_SUMMARIZE_SIGNATURE = inspect.Signature(
    _SUMMARIZE_PARAMETERS,
    return_annotation=dict[str, object],
)
_WORK_SCHEDULE_UPDATE_DOCSTRING = """Update a work schedule.

Args:
    date: Date or exact `YYYY-MM-DD` string to update.
    working: Whether the date is a working day.
    description: Optional schedule description.
    project: Optional project entity restriction.
    user: Optional user entity restriction.
    recalculate_field: Optional field to recalculate.

Returns:
    Updated work schedule data returned by the server.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if the date, project/user exclusivity, or server result is
        invalid.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_WORK_SCHEDULE_UPDATE_PARAMETERS = (
    inspect.Parameter(
        'date', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=datetime.date | str
    ),
    inspect.Parameter('working', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=bool),
    inspect.Parameter(
        'description',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'project',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=dict[str, Any] | None,
    ),
    inspect.Parameter(
        'user',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=dict[str, Any] | None,
    ),
    inspect.Parameter(
        'recalculate_field',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
)
_METHOD_WORK_SCHEDULE_UPDATE_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_WORK_SCHEDULE_UPDATE_PARAMETERS,
    ),
    return_annotation=dict[str, object],
)
_WORK_SCHEDULE_UPDATE_SIGNATURE = inspect.Signature(
    _WORK_SCHEDULE_UPDATE_PARAMETERS,
    return_annotation=dict[str, object],
)
SCHEMA_ENTITY_READ_WARNINGS: contextvars.ContextVar[_PENDING_COMPATIBILITY_WARNINGS | None] = (
    contextvars.ContextVar('SCHEMA_ENTITY_READ_WARNINGS', default=None)
)
SCHEMA_FIELD_READ_WARNINGS: contextvars.ContextVar[_PENDING_COMPATIBILITY_WARNINGS | None] = (
    contextvars.ContextVar('SCHEMA_FIELD_READ_WARNINGS', default=None)
)


@dataclasses.dataclass(frozen=True)
class _ActivityStreamReadOptions:
    """Collect exact public activity stream arguments for validation."""

    entity_type: str
    entity_id: int
    entity_fields: dict[str, list[str]] | None = None
    min_id: int | None = None
    max_id: int | None = None
    limit: int | None = None


@dataclasses.dataclass(frozen=True)
class _AuthenticationPlan:
    """Carry one human-authentication request and its credentials."""

    auth_spec: _protocol.AuthSpec
    params: dict[str, object]


class ExportPageKeywordArguments(TypedDict):
    """Describe supported keyword calls to `export_page`.

    Attributes:
        format: Required nonempty export format.
        layout_name: Optional page layout name.
    """

    format: str
    layout_name: NotRequired[str | None]


@dataclasses.dataclass(frozen=True)
class _ExportPageOptions:
    """Collect exact public export-page arguments for validation."""

    page_id: int
    format: str
    layout_name: str | None = None


@dataclasses.dataclass(frozen=True)
class _SummarizeOptions:
    """Collect exact public `summarize` arguments for runtime validation."""

    entity_type: str
    filters: object
    summary_fields: object
    filter_operator: str | None = None
    grouping: list[dict[str, object]] | None = None
    include_archived_projects: bool = True


@dataclasses.dataclass(frozen=True)
class _WorkScheduleUpdateOptions:
    """Collect exact public work-schedule update arguments for validation."""

    date: object
    working: bool
    description: str | None = None
    project: dict[str, object] | None = None
    user: dict[str, object] | None = None
    recalculate_field: str | None = None


def _date_str(value: object, argument: str) -> str:
    """Return an exact schedule date string."""
    if isinstance(value, datetime.datetime):
        raise ShotgunError(f'{argument} must be a date, not a datetime')
    if isinstance(value, datetime.date):
        return f'{value.year:04d}-{value.month:02d}-{value.day:02d}'
    if not isinstance(value, str):
        raise ShotgunError(f'{argument} must be a datetime.date or YYYY-MM-DD string')
    if (
        len(value) != _SCHEDULE_DATE_LENGTH
        or value[4] != '-'
        or value[7] != '-'
        or not value.isascii()
        or not (value[:4] + value[5:7] + value[8:]).isdigit()
    ):
        raise ShotgunError(f'{argument} must be YYYY-MM-DD')
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        raise ShotgunError(f'{argument} must be YYYY-MM-DD') from None
    return value


def _entity(value: object) -> object:
    """Optimize an entity value for client request preparation."""
    return _protocol.optimize_entity(value)


def _project_entity(value: object) -> object:
    """Optimize an optional project entity."""
    return None if value is None else _entity(value)


def _property_pairs(properties: dict[str, object]) -> list[dict[str, object]]:
    """Build schema property entries in caller order."""
    return [{'property_name': name, 'value': value} for name, value in properties.items()]


def _reject_project_and_user(
    project: dict[str, object] | None,
    user: dict[str, object] | None,
) -> None:
    """Reject simultaneous project and user restrictions."""
    if project is not None and user is not None:
        raise ShotgunError('project and user can only be used exclusively of each other')


def _require_nonempty_string(value: object, message: str) -> str:
    """Validate a nonempty string."""
    if not isinstance(value, str) or not value:
        raise ShotgunError(message)
    return value


def bind_activity_stream_read(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    owner_name: str,
) -> _ActivityStreamReadOptions:
    """Bind runtime activity-stream arguments.

    Args:
        args: Positional arguments supplied by the caller.
        kwargs: Keyword arguments supplied by the caller.
        owner_name: Public client class name used in errors.

    Returns:
        Bound activity-stream options.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(
        _ACTIVITY_STREAM_READ_SIGNATURE, args, kwargs, f'{owner_name}.activity_stream_read()'
    )
    options = cast('Callable[..., _ActivityStreamReadOptions]', _ActivityStreamReadOptions)
    return options(**bound.arguments)


def bind_export_page(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    owner_name: str,
) -> _ExportPageOptions:
    """Bind runtime export-page arguments.

    Args:
        args: Positional arguments supplied by the caller.
        kwargs: Keyword arguments supplied by the caller.
        owner_name: Public client class name used in errors.

    Returns:
        Bound export-page options.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(_EXPORT_PAGE_SIGNATURE, args, kwargs, f'{owner_name}.export_page()')
    options = cast('Callable[..., _ExportPageOptions]', _ExportPageOptions)
    return options(**bound.arguments)


def bind_summarize(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    owner_name: str,
) -> _SummarizeOptions:
    """Bind runtime summarize arguments.

    Args:
        args: Positional arguments supplied by the caller.
        kwargs: Keyword arguments supplied by the caller.
        owner_name: Public client class name used in errors.

    Returns:
        Bound summarize options.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(_SUMMARIZE_SIGNATURE, args, kwargs, f'{owner_name}.summarize()')
    options = cast('Callable[..., _SummarizeOptions]', _SummarizeOptions)
    return options(**bound.arguments)


def bind_work_schedule_update(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    owner_name: str,
) -> _WorkScheduleUpdateOptions:
    """Bind runtime work-schedule update arguments.

    Args:
        args: Positional arguments supplied by the caller.
        kwargs: Keyword arguments supplied by the caller.
        owner_name: Public client class name used in errors.

    Returns:
        Bound schedule-update options.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(
        _WORK_SCHEDULE_UPDATE_SIGNATURE, args, kwargs, f'{owner_name}.work_schedule_update()'
    )
    options = cast('Callable[..., _WorkScheduleUpdateOptions]', _WorkScheduleUpdateOptions)
    return options(**bound.arguments)


def build_activity_stream_read_params(options: _ActivityStreamReadOptions) -> dict[str, object]:
    """Build activity-stream parameters.

    Args:
        options: Bound activity-stream options.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    fields = options.entity_fields
    return {
        'type': options.entity_type,
        'id': options.entity_id,
        'max_id': options.max_id,
        'min_id': options.min_id,
        'limit': options.limit,
        'entity_fields': {} if fields is None else fields,
    }


def build_authentication_plan(
    user_login: str,
    user_password: str,
    auth_token: str | None,
) -> _AuthenticationPlan:
    """Build one human-authentication operation.

    Args:
        user_login: Login to authenticate.
        user_password: Password to authenticate.
        auth_token: Optional two-factor token.

    Returns:
        Request parameters and per-request authentication specification.
    """
    params = _crud.build_find_plan(
        _crud.prepare_find(
            _crud.FindOptions(
                entity_type='HumanUser',
                filters=[
                    ['sg_status_list', 'is', 'act'],
                    ['login', 'is', user_login],
                ],
                fields=['id', 'login'],
                order=None,
                filter_operator=None,
                limit=1,
                retired_only=False,
                page=1,
                include_archived_projects=True,
                additional_filter_presets=None,
            )
        ),
        1,
    ).params
    return _AuthenticationPlan(
        auth_spec=_protocol.AuthSpec(
            script_name=None,
            api_key=None,
            user_login=user_login,
            user_password=user_password,
            session_token=None,
            auth_token=auth_token,
            sudo_as_login=None,
            session_uuid=None,
            extra_auth_params=None,
        ),
        params=params,
    )


def build_export_page_params(options: _ExportPageOptions) -> dict[str, object]:
    """Build validated page-export parameters.

    Args:
        options: Bound export-page options.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If the page identifier or format is invalid.
    """
    page_id = _crud.require_entity_id(
        options.page_id,
        'export_page page_id must be an integer',
    )
    page_format = _require_nonempty_string(
        options.format,
        'export_page format must be a nonempty string',
    )
    params: dict[str, object] = {'page_id': page_id, 'format': page_format}
    if options.layout_name is not None:
        params['layout_name'] = options.layout_name
    return params


def build_follow_params(user: object, target: object) -> dict[str, object]:
    """Build follow or unfollow parameters.

    Args:
        user: User entity.
        target: Target entity.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    return {'user': _entity(user), 'entity': _entity(target)}


def build_followers_params(target: object) -> dict[str, object]:
    """Build follower-query parameters.

    Args:
        target: Target entity.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    return {'entity': _entity(target)}


def build_following_params(
    user: object,
    project: object,
    entity_type: object,
) -> dict[str, object]:
    """Build following-query parameters.

    Args:
        user: User entity.
        project: Optional project entity.
        entity_type: Optional entity type restriction.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    params: dict[str, object] = {'user': _entity(user)}
    if project is not None:
        params['project'] = _entity(project)
    if entity_type is not None:
        params['entity_type'] = entity_type
    return params


def build_nav_expand_params(
    path: object,
    seed_entity_field: object,
    requested_entity_fields: object,
) -> dict[str, object]:
    """Build navigation expansion parameters.

    Args:
        path: Navigation path.
        seed_entity_field: Optional seed field.
        requested_entity_fields: Optional linked-entity fields.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    fields = requested_entity_fields
    params: dict[str, object] = {'path': path}
    if seed_entity_field is not None:
        params['seed_entity_field'] = seed_entity_field
    if fields is not None:
        params['entity_fields'] = fields
    return params


def build_nav_search_entity_params(
    root_path: object,
    searched_entity: object,
    seed_entity_field: object,
) -> dict[str, object]:
    """Build entity navigation-search parameters.

    Args:
        root_path: Navigation root path.
        searched_entity: Entity to find.
        seed_entity_field: Optional seed field.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    params: dict[str, object] = {
        'root_path': root_path,
        'search_criteria': {'entity': _entity(searched_entity)},
    }
    if seed_entity_field is not None:
        params['seed_entity_field'] = seed_entity_field
    return params


def build_nav_search_string_params(
    root_path: object,
    search_string: object,
    seed_entity_field: object,
) -> dict[str, object]:
    """Build string navigation-search parameters.

    Args:
        root_path: Navigation root path.
        search_string: Search text.
        seed_entity_field: Optional seed field.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    params: dict[str, object] = {
        'root_path': root_path,
        'search_criteria': {'search_string': search_string},
    }
    if seed_entity_field is not None:
        params['seed_entity_field'] = seed_entity_field
    return params


def build_note_thread_read_params(
    note_id: object,
    requested_entity_fields: object,
) -> dict[str, object]:
    """Build note-thread parameters.

    Args:
        note_id: Note identifier.
        requested_entity_fields: Optional linked-entity fields.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    fields = requested_entity_fields
    params: dict[str, object] = {'note_id': note_id}
    if fields is not None:
        params['entity_fields'] = fields
    return params


def build_preferences_read_params(prefs: object) -> dict[str, object]:
    """Build preference-read parameters.

    Args:
        prefs: Optional preference names.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    return {'prefs': prefs or []}


def build_schema_entity_read_params(project: object) -> dict[str, object] | None:
    """Build entity-schema parameters.

    Args:
        project: Optional project entity.

    Returns:
        RPC parameters or `None`, copied by the client before dispatch.
    """
    project_entity = _project_entity(project)
    return {'project': project_entity} if project_entity is not None else None


def build_schema_field_create_params(
    entity_type: object,
    data_type: object,
    display_name: object,
    properties: dict[str, object] | None,
) -> dict[str, object]:
    """Build field-creation parameters.

    Args:
        entity_type: Owning entity type.
        data_type: New field data type.
        display_name: New field display name.
        properties: Optional field properties.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    field_properties = {} if properties is None else properties
    return {
        'type': entity_type,
        'data_type': data_type,
        'properties': [
            {'property_name': 'name', 'value': display_name},
            *_property_pairs(field_properties),
        ],
    }


def build_schema_field_delete_params(
    entity_type: object,
    field_name: object,
) -> dict[str, object]:
    """Build field-deletion parameters.

    Args:
        entity_type: Owning entity type.
        field_name: Internal field name.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    return {
        'type': entity_type,
        'field_name': field_name,
    }


def build_schema_field_read_params(
    entity_type: object,
    field_name: object,
    project: object,
) -> dict[str, object]:
    """Build field-schema parameters.

    Args:
        entity_type: Owning entity type.
        field_name: Optional internal field name.
        project: Optional project entity.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    project_entity = _project_entity(project)
    params: dict[str, object] = {'type': entity_type}
    if field_name is not None:
        params['field_name'] = field_name
    if project_entity is not None:
        params['project'] = project_entity
    return params


def build_schema_field_update_params(
    entity_type: object,
    field_name: object,
    properties: dict[str, object],
    project: object,
) -> dict[str, object]:
    """Build field-update parameters.

    Args:
        entity_type: Owning entity type.
        field_name: Internal field name.
        properties: Field properties to update.
        project: Optional project entity.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    project_entity = _project_entity(project)
    params: dict[str, object] = {
        'type': entity_type,
        'field_name': field_name,
        'properties': _property_pairs(properties),
    }
    if project_entity is not None:
        params['project'] = project_entity
    return params


def build_summarize_params(options: _SummarizeOptions) -> dict[str, object]:
    """Build validated summarize parameters.

    Args:
        options: Bound summarize options.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If filters or summary fields are invalid.
    """
    raw_filters: object = options.filters
    if not isinstance(raw_filters, (list, tuple, dict)):
        raise ShotgunError('summarize() requires list or dict filters')
    filters = cast(
        'list[object] | tuple[object, ...] | dict[str, object]',
        raw_filters,
    )
    summary_fields = cast(
        'list[dict[str, object]]',
        _crud.require_list(
            options.summary_fields,
            'summarize() requires a list of summary fields',
        ),
    )
    params: dict[str, object] = {
        'type': options.entity_type,
        'filters': _protocol.translate_filters(filters, options.filter_operator),
        'summaries': summary_fields,
    }
    if options.grouping is not None:
        params['grouping'] = options.grouping
    if not options.include_archived_projects:
        params['include_archived_projects'] = False
    return params


def build_text_search_params(
    text: str,
    entity_types: object,
    project_ids: object,
    limit: object,
) -> dict[str, object]:
    """Build validated text-search parameters.

    Args:
        text: Search text.
        entity_types: Entity-type filter mapping.
        project_ids: Optional project identifiers.
        limit: Optional maximum result count.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If `entity_types` is not a dictionary.
    """
    validated_entity_types = _crud.require_dictionary(
        entity_types,
        'text_search() entity_types must be a dict',
    )
    translated = {
        entity_type: _protocol.translate_filters(_protocol.require_filter_container(filters))
        for entity_type, filters in validated_entity_types.items()
    }
    return {
        'text': text,
        'entity_types': translated,
        'project_ids': [] if project_ids is None else project_ids,
        'max_results': limit,
    }


def build_update_project_last_accessed_params(
    project: dict[str, object],
    user: dict[str, object] | None,
) -> dict[str, object]:
    """Build last-accessed parameters.

    Args:
        project: Accessed project entity.
        user: Optional attributed user entity.

    Returns:
        RPC parameters copied by the client before dispatch.
    """
    params: dict[str, object] = {'project_id': project['id']}
    if user is not None:
        params['user_id'] = user['id']
    return params


def build_user_subscriptions_create_params(users: object) -> dict[str, object]:
    """Build validated subscription parameters.

    Args:
        users: User subscription entries.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If the entries are invalid.
    """
    validated = _crud.require_list(
        users,
        'user_subscriptions_create users must be a list',
    )
    message = 'user_subscriptions_create users must contain dictionaries with a user_id key'
    for member in validated:
        if not isinstance(member, dict) or 'user_id' not in member:
            raise ShotgunError(message)
    return {'users': cast('list[dict[str, object]]', validated)}


def build_work_schedule_read_params(
    start_date: object,
    end_date: object,
    project: dict[str, object] | None,
    user: dict[str, object] | None,
) -> dict[str, object]:
    """Build validated schedule-read parameters.

    Args:
        start_date: Inclusive `datetime.date` or exact `YYYY-MM-DD` string;
            `datetime.datetime` is not accepted.
        end_date: Inclusive `datetime.date` or exact `YYYY-MM-DD` string;
            `datetime.datetime` is not accepted.
        project: Optional project restriction.
        user: Optional user restriction.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If dates or restrictions are invalid.
    """
    params: dict[str, object] = {
        'start_date': _date_str(start_date, 'start_date'),
        'end_date': _date_str(end_date, 'end_date'),
    }
    if project is not None:
        params['project'] = _entity(project)
    if user is not None:
        params['user'] = _entity(user)
    return params


def build_work_schedule_update_params(
    options: _WorkScheduleUpdateOptions,
) -> dict[str, object]:
    """Build validated schedule-update parameters.

    Args:
        options: Bound schedule-update options. The date must be a
            `datetime.date` or exact `YYYY-MM-DD` string, not a
            `datetime.datetime`. Project and user restrictions are mutually
            exclusive.

    Returns:
        RPC parameters copied by the client before dispatch.

    Raises:
        ShotgunError: If the date is invalid or both restrictions are supplied.
    """
    _reject_project_and_user(options.project, options.user)
    params: dict[str, object] = {
        'date': _date_str(options.date, 'date'),
        'working': options.working,
    }
    if options.description is not None:
        params['description'] = options.description
    if options.project is not None:
        params['project'] = _entity(options.project)
    if options.user is not None:
        params['user'] = _entity(options.user)
    if options.recalculate_field is not None:
        params['recalculate_field'] = options.recalculate_field
    return params


def consume_compatibility_warnings(
    warning_context: contextvars.ContextVar[_PENDING_COMPATIBILITY_WARNINGS | None],
    owner: object,
) -> tuple[CompatibilityWarningCode, ...]:
    """Consume operation-local compatibility warnings exactly once.

    Args:
        warning_context: Context-local warning state.
        owner: Client instance expected to own the warning.

    Returns:
        Compatibility warning codes for the target call.
    """
    pending = warning_context.get()
    if pending is None or pending[0] is not owner:
        return ()
    warning_context.set(None)
    return pending[1]


@contextmanager
def entity_types_alias(owner: object) -> Generator[None, None, None]:
    """Scope the deprecated `entity_types` compatibility notice.

    Args:
        owner: Client instance invoking the alias.

    Yields:
        Control while the public target method consumes the warning.
    """
    token = SCHEMA_ENTITY_READ_WARNINGS.set(
        (owner, ('deprecated-entity-types-alias',)),
    )
    try:
        yield
    finally:
        SCHEMA_ENTITY_READ_WARNINGS.reset(token)


def expose_activity_stream_read_signature(
    function: Callable[..., object],
) -> Callable[..., object]:
    """Expose the documented activity-stream signature.

    Args:
        function: Variadic implementation.

    Returns:
        The implementation with public reflection metadata.
    """
    return expose_signature(
        function, _METHOD_ACTIVITY_STREAM_READ_SIGNATURE, _ACTIVITY_STREAM_READ_DOCSTRING
    )


def expose_export_page_signature(
    function: Callable[..., object],
) -> Callable[..., object]:
    """Expose the documented page-export signature.

    Args:
        function: Variadic implementation.

    Returns:
        The implementation with public reflection metadata.
    """
    return expose_signature(function, _METHOD_EXPORT_PAGE_SIGNATURE, _EXPORT_PAGE_DOCSTRING)


def expose_summarize_signature(
    function: Callable[..., object],
) -> Callable[..., object]:
    """Expose the documented summarize signature.

    Args:
        function: Variadic implementation.

    Returns:
        The implementation with public reflection metadata.
    """
    return expose_signature(function, _METHOD_SUMMARIZE_SIGNATURE, _SUMMARIZE_DOCSTRING)


def expose_work_schedule_update_signature(
    function: Callable[..., object],
) -> Callable[..., object]:
    """Expose the documented schedule-update signature.

    Args:
        function: Variadic implementation.

    Returns:
        The implementation with public reflection metadata.
    """
    return expose_signature(
        function, _METHOD_WORK_SCHEDULE_UPDATE_SIGNATURE, _WORK_SCHEDULE_UPDATE_DOCSTRING
    )


def require_dictionary_list_result(
    value: object,
    method_name: str,
) -> list[dict[str, object]]:
    """Validate a list of dictionary RPC results.

    Args:
        value: Raw RPC result.
        method_name: Public method name used in errors.

    Returns:
        Validated dictionary list.

    Raises:
        ShotgunError: If the result is invalid.
    """
    result = _crud.require_list_result(value, method_name)
    if any(not isinstance(member, dict) for member in result):
        raise ShotgunError(f'{method_name} returned a non-dictionary member')
    return cast('list[dict[str, object]]', result)


def require_human_authentication_result(value: object) -> dict[str, object] | None:
    """Validate and select the first human-authentication result.

    Args:
        value: Raw read result.

    Returns:
        First matching user or `None`.

    Raises:
        ShotgunError: If the result is invalid.
    """
    result = _crud.require_dictionary_result(value, 'authenticate_human_user')
    entities = _crud.require_list(
        result.get('entities', []),
        'authenticate_human_user did not return an entities list',
    )
    if any(not isinstance(member, dict) for member in entities):
        raise ShotgunError('authenticate_human_user returned a non-dictionary entity')
    return cast('dict[str, object]', entities[0]) if entities else None


def require_string_result(value: object, method_name: str) -> str:
    """Validate a string RPC result.

    Args:
        value: Raw RPC result.
        method_name: Public method name used in errors.

    Returns:
        Validated string.

    Raises:
        ShotgunError: If the result is not a string.
    """
    if not isinstance(value, str):
        raise ShotgunError(f'{method_name} did not return a string')
    return value


@contextmanager
def schema_alias(owner: object) -> Generator[None, None, None]:
    """Scope the deprecated `schema` compatibility notice.

    Args:
        owner: Client instance invoking the alias.

    Yields:
        Control while the public target method consumes the warning.
    """
    token = SCHEMA_FIELD_READ_WARNINGS.set(
        (owner, ('deprecated-schema-alias',)),
    )
    try:
        yield
    finally:
        SCHEMA_FIELD_READ_WARNINGS.reset(token)


def warn_entity_types_alias() -> None:
    """Warn at the caller of the deprecated `entity_types` method."""
    warnings.warn(
        'entity_types() is deprecated; use schema_entity_read()',
        DeprecationWarning,
        stacklevel=3,
    )


def warn_schema_alias() -> None:
    """Warn at the caller of the deprecated `schema` method."""
    warnings.warn(
        'schema() is deprecated; use schema_field_read()',
        DeprecationWarning,
        stacklevel=3,
    )
