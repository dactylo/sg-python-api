"""Shared pure plans for synchronous and asynchronous client methods."""

import dataclasses
import datetime
from collections.abc import Mapping
from typing import cast

from . import _crud, _protocol
from ._errors import ShotgunError

_SCHEDULE_DATE_LENGTH = 10


@dataclasses.dataclass(frozen=True)
class ActivityStreamReadOptions:
    """Collect exact public activity stream arguments for validation.

    Attributes:
        entity_type: Entity type whose activity stream to read.
        entity_id: Numeric identifier of the entity.
        entity_fields: Optional fields to return for linked entity types.
        min_id: Optional minimum activity identifier.
        max_id: Optional maximum activity identifier.
        limit: Optional maximum number of activity records.
    """

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


@dataclasses.dataclass(frozen=True)
class ExportPageOptions:
    """Collect exact public export-page arguments for validation.

    Attributes:
        page_id: Numeric page identifier.
        format: Required nonempty export format name.
        layout_name: Optional layout name, including an empty name.
    """

    page_id: int
    format: str
    layout_name: str | None = None


@dataclasses.dataclass(frozen=True)
class SummarizeOptions:
    """Collect exact public `summarize` arguments for runtime validation.

    Attributes:
        entity_type: Entity type to summarize.
        filters: Filters in list or dict syntax.
        summary_fields: List of `{'field': ..., 'type': ...}` summaries.
        filter_operator: `'all'` or `'any'` for list-syntax filters.
        grouping: Optional grouping specs.
        include_archived_projects: Include archived projects when `True`.
    """

    entity_type: str
    filters: object
    summary_fields: object
    filter_operator: str | None = None
    grouping: list[dict[str, object]] | None = None
    include_archived_projects: bool = True


@dataclasses.dataclass(frozen=True)
class WorkScheduleUpdateOptions:
    """Collect exact public work-schedule update arguments for validation.

    Attributes:
        date: Date or exact `YYYY-MM-DD` string to update.
        working: Whether the date is a working day.
        description: Optional schedule description.
        project: Optional project entity restriction.
        user: Optional user entity restriction.
        recalculate_field: Optional field to recalculate.
    """

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


def _is_subscription_value(value: object) -> bool:
    """Check the supported subscription value forms."""
    return (
        value is None
        or isinstance(value, str)
        or (
            isinstance(value, list)
            and all(isinstance(item, str) for item in cast('list[object]', value))
        )
    )


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


def build_activity_stream_read_params(options: ActivityStreamReadOptions) -> dict[str, object]:
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


def build_export_page_params(options: ExportPageOptions) -> dict[str, object]:
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


def build_summarize_params(options: SummarizeOptions) -> dict[str, object]:
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
        ShotgunError: If `entity_types` is not a dictionary or an entity filter
            has invalid container or filter syntax.
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

    Raises:
        ShotgunError: If a project or user ID is missing or invalid.
    """
    params: dict[str, object] = {
        'project_id': _crud.require_entity_id(
            project.get('id'), 'update_project_last_accessed project id must be an integer'
        )
    }
    if user is not None:
        params['user_id'] = _crud.require_entity_id(
            user.get('id'), 'update_project_last_accessed user id must be an integer'
        )
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
    entry_message = (
        'user_subscriptions_create users must contain dictionaries with '
        'humanUserId and subscription keys'
    )
    params: dict[str, object] = {}
    for member in validated:
        if not isinstance(member, dict):
            raise ShotgunError(entry_message)
        entry = cast('dict[str, object]', member)
        if 'humanUserId' not in entry or 'subscription' not in entry:
            raise ShotgunError(entry_message)
        user_id = entry['humanUserId']
        if isinstance(user_id, bool) or not isinstance(user_id, (str, int)):
            raise ShotgunError(
                'user_subscriptions_create users must contain string or integer humanUserId values'
            )
        if not _is_subscription_value(entry['subscription']):
            raise ShotgunError(
                'user_subscriptions_create users must contain valid subscription values'
            )
        key = str(user_id)
        if key in params:
            raise ShotgunError(
                'user_subscriptions_create users must not contain duplicate user IDs'
            )
        subscription = entry['subscription']
        params[key] = (
            cast('list[str]', subscription).copy()
            if isinstance(subscription, list)
            else subscription
        )
    return params


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
    options: WorkScheduleUpdateOptions,
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


def entity_types_alias_error() -> ShotgunError:
    """Build the error raised by the deprecated `entity_types` alias.

    Returns:
        A `ShotgunError` naming `schema_entity_read()` as the replacement.
    """
    return ShotgunError('Deprecated: use schema_entity_read() instead')


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


def require_user_subscriptions_create_result(value: object) -> bool:
    """Interpret a mapping of per-user subscription assignment reports.

    Args:
        value: Raw result returned by the server.

    Returns:
        `True` if every report is `None`, otherwise `False`.

    Raises:
        ShotgunError: If the result is not a mapping with string keys.
    """
    message = 'user_subscriptions_create returned an invalid result mapping'
    if not isinstance(value, Mapping):
        raise ShotgunError(message)
    reports = cast('Mapping[object, object]', value)
    if any(not isinstance(key, str) for key in reports):
        raise ShotgunError(message)
    return all(report is None for report in reports.values())


def require_user_subscriptions_result(value: object) -> list[dict[str, object]]:
    """Convert a subscription mapping to ordered public entries.

    Args:
        value: Raw subscription mapping returned by the server.

    Returns:
        Entries with unchanged string `humanUserId` and `subscription` values
        of `None`, string, or list of strings. Lists are copied.

    Raises:
        ShotgunError: If the result is not a valid subscription mapping.
    """
    if not isinstance(value, Mapping):
        raise ShotgunError('user_subscriptions_read did not return a dictionary')
    subscriptions: list[dict[str, object]] = []
    for user_id, subscription in cast('Mapping[object, object]', value).items():
        if not isinstance(user_id, str) or not user_id:
            raise ShotgunError('user_subscriptions_read returned an invalid user ID')
        if not _is_subscription_value(subscription):
            raise ShotgunError('user_subscriptions_read returned an invalid subscription')
        public_subscription = (
            cast('list[str]', subscription).copy()
            if isinstance(subscription, list)
            else subscription
        )
        subscriptions.append({'humanUserId': user_id, 'subscription': public_subscription})
    return subscriptions


def schema_alias_error(entity_type: object) -> ShotgunError:
    """Build the error raised by the deprecated `schema` alias.

    Args:
        entity_type: Caller-supplied entity type, rendered with `str()`.

    Returns:
        A `ShotgunError` naming the equivalent `schema_field_read()` call.
    """
    return ShotgunError(f"Deprecated: use schema_field_read('{entity_type}') instead")
