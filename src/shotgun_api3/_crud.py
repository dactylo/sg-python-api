"""Shared pure builders and validators for CRUD and query operations."""

import copy
import dataclasses
import inspect
from collections.abc import Callable
from typing import Any, Literal, cast

from . import _protocol
from ._errors import ShotgunError
from ._signatures import bind_arguments, expose_signature

_FIND_DOCSTRING = """Find entities matching the supplied filters.

Args:
    entity_type: Entity type to find.
    filters: Filters in list or dict syntax.
    fields: Optional fields to return; defaults to `id`.
    order: Optional field and direction sort specifications.
    filter_operator: `'all'` or `'any'` for list-syntax filters.
    limit: Maximum entities to return, or `0` for no limit.
    retired_only: Return retired entities instead of active entities.
    page: Specific page to return, or `0` to read all pages.
    include_archived_projects: Include entities from archived projects.
    additional_filter_presets: Optional server-side filter presets.

Returns:
    Matching entities in server order.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if filters, paging, ordering, or a server result is invalid.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_FIND_ONE_DOCSTRING = """Return the first entity matching the supplied filters.

Args:
    entity_type: Entity type to find.
    filters: Filters in list or dict syntax.
    fields: Optional fields to return; defaults to `id`.
    order: Optional field and direction sort specifications.
    filter_operator: `'all'` or `'any'` for list-syntax filters.
    retired_only: Return a retired entity instead of an active entity.
    include_archived_projects: Include entities from archived projects.
    additional_filter_presets: Optional server-side filter presets.

Returns:
    The first matching entity, or `None` when no entity matches.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If an RPC fault or terminal transport failure
        occurs, or if filters, paging, ordering, or a server result is invalid.
    ProtocolError: If a server request returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_FIND_ONE_PARAMETERS = (
    inspect.Parameter('entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter(
        'filters',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=list[Any] | tuple[Any, ...] | dict[str, Any],
    ),
    inspect.Parameter(
        'fields',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=list[str] | None,
    ),
    inspect.Parameter(
        'order',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=list[dict[str, str]] | None,
    ),
    inspect.Parameter(
        'filter_operator',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'retired_only',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=False,
        annotation=bool,
    ),
    inspect.Parameter(
        'include_archived_projects',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=True,
        annotation=bool,
    ),
    inspect.Parameter(
        'additional_filter_presets',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=object,
    ),
)
_FIND_ONE_SIGNATURE = inspect.Signature(
    _FIND_ONE_PARAMETERS,
    return_annotation=dict[str, object] | None,
)
_FIND_PARAMETERS = (
    inspect.Parameter('entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter(
        'filters',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=list[Any] | tuple[Any, ...] | dict[str, Any],
    ),
    inspect.Parameter(
        'fields',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=list[str] | None,
    ),
    inspect.Parameter(
        'order',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=list[dict[str, str]] | None,
    ),
    inspect.Parameter(
        'filter_operator',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'limit',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=0,
        annotation=int,
    ),
    inspect.Parameter(
        'retired_only',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=False,
        annotation=bool,
    ),
    inspect.Parameter(
        'page',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=0,
        annotation=int,
    ),
    inspect.Parameter(
        'include_archived_projects',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=True,
        annotation=bool,
    ),
    inspect.Parameter(
        'additional_filter_presets',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=object,
    ),
)
_FIND_SIGNATURE = inspect.Signature(
    _FIND_PARAMETERS,
    return_annotation=list[dict[str, object]],
)
_METHOD_FIND_ONE_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_FIND_ONE_PARAMETERS,
    ),
    return_annotation=dict[str, object] | None,
)
_METHOD_FIND_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_FIND_PARAMETERS,
    ),
    return_annotation=list[dict[str, object]],
)


@dataclasses.dataclass(frozen=True)
class _FindOneOptions:
    """Collect exact public `find_one` arguments for validation."""

    entity_type: str
    filters: list[Any] | tuple[Any, ...] | dict[str, Any]
    fields: list[str] | None = None
    order: list[dict[str, str]] | None = None
    filter_operator: str | None = None
    retired_only: bool = False
    include_archived_projects: bool = True
    additional_filter_presets: object = None


@dataclasses.dataclass(frozen=True)
class FindOptions:
    """Collect the exact public `find` arguments for runtime validation.

    Attributes:
        entity_type: Type of entity to query.
        filters: Query filters or filter preset structure.
        fields: Optional fields to return.
        order: Optional field ordering directives.
        filter_operator: Optional logical operator joining filters.
        limit: Maximum number of records, or zero for no limit.
        retired_only: Whether to return only retired entities.
        page: One-based page number, or zero for automatic paging.
        include_archived_projects: Whether results may include archived
            projects.
        additional_filter_presets: Optional server-side filter presets.
    """

    entity_type: str
    filters: list[Any] | tuple[Any, ...] | dict[str, Any]
    fields: list[str] | None = None
    order: list[dict[str, str]] | None = None
    filter_operator: str | None = None
    limit: int = 0
    retired_only: bool = False
    page: int = 0
    include_archived_projects: bool = True
    additional_filter_presets: object = None


@dataclasses.dataclass(frozen=True)
class _FindPlan:
    """Carry paging decisions for one read operation."""

    limit: int
    multi_page: bool
    paging: dict[str, int]
    params: dict[str, object]
    single_page: bool


@dataclasses.dataclass(frozen=True)
class _PreparedFind:
    """Carry detached find inputs until server paging is known."""

    limit: int
    page: int
    params: dict[str, object]


@dataclasses.dataclass(frozen=True)
class _ReadPage:
    """Carry validated page entities and a stop decision."""

    entities: list[dict[str, object]]
    stop: bool


def _build_fields(
    data: dict[str, object],
    modes: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    """Build independent wire field entries without mutating caller data."""
    fields: list[dict[str, object]] = []
    for name, value in data.items():
        field = {
            'field_name': name,
            'value': _protocol.project_entities(value),
        }
        if modes is not None and name in modes:
            field['multi_entity_update_mode'] = modes[name]
        fields.append(field)
    return fields


def _read_sorts(order: list[dict[str, str]] | None) -> list[dict[str, str]]:
    """Validate and copy read ordering into wire sort entries."""
    sorts: list[dict[str, str]] = []
    for index, item in enumerate(cast('list[object]', order or [])):
        if not isinstance(item, dict):
            raise ShotgunError(f'order item {index} must be a dictionary')
        member = cast('dict[str, object]', item)
        field_name = member.get('field_name')
        if not isinstance(field_name, str) or not field_name:
            raise ShotgunError(f"order item {index} is missing 'field_name'")
        direction = member.get('direction', 'asc')
        if not isinstance(direction, str) or direction not in {'asc', 'desc'}:
            raise ShotgunError(f'order item {index} has invalid direction {direction!r}')
        sorts.append({'field_name': field_name, 'direction': direction})
    return sorts


def _require_modes(value: object, message: str) -> dict[str, str] | None:
    """Return dynamically validated optional update modes."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ShotgunError(message)
    mapping = cast('dict[object, object]', value)
    if any(not isinstance(key, str) or not isinstance(item, str) for key, item in mapping.items()):
        raise ShotgunError(message)
    return cast('dict[str, str]', value)


def _require_return_fields(value: object, message: str) -> list[str]:
    """Return copied string fields without substituting a default."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ShotgunError(message)
    fields = cast('list[object]', value)
    if any(not isinstance(field, str) for field in fields):
        raise ShotgunError(message)
    return cast('list[str]', list(fields))


def _validate_field_names(data: dict[object, object], message: str) -> None:
    """Require nonempty string field-name keys."""
    if any(not isinstance(name, str) or not name for name in data):
        raise ShotgunError(message)


def _batch_create_call(index: int, member: dict[str, object]) -> dict[str, object]:
    """Build one validated batch create call."""
    if 'data' not in member:
        raise ShotgunError(f"batch create request {index} is missing 'data'")
    data = member['data']
    if not isinstance(data, dict):
        raise ShotgunError(f'batch create request {index} data must be a dictionary')
    validated_data = cast('dict[object, object]', data)
    _validate_field_names(
        validated_data,
        f'batch create request {index} field names must be nonempty strings',
    )
    return_fields = _require_return_fields(
        member.get('return_fields'),
        f'batch create request {index} return_fields must be a list of strings or None',
    ) or ['id']
    return {
        'request_type': 'create',
        'type': member['entity_type'],
        'fields': _build_fields(cast('dict[str, object]', validated_data)),
        'return_fields': return_fields,
    }


def _validate_nonnegative_integer(value: object, name: str) -> None:
    """Require a non-boolean, nonnegative integer argument."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ShotgunError(f'{name} must be a nonnegative integer')


def bind_find(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    client_name: str,
) -> FindOptions:
    """Bind runtime arguments with client-specific `find` errors.

    Args:
        args: Positional public arguments.
        kwargs: Keyword public arguments.
        client_name: Public client class name for error attribution.

    Returns:
        Bound find options retaining the supplied argument values without
        copying mutable containers.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(_FIND_SIGNATURE, args, kwargs, f'{client_name}.find()')
    options = cast('Callable[..., FindOptions]', FindOptions)
    return options(**bound.arguments)


def bind_find_one(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    client_name: str,
) -> _FindOneOptions:
    """Bind runtime arguments with client-specific `find_one` errors.

    Args:
        args: Positional public arguments.
        kwargs: Keyword public arguments.
        client_name: Public client class name for error attribution.

    Returns:
        Bound find-one options retaining the supplied argument values without
        copying mutable containers.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(_FIND_ONE_SIGNATURE, args, kwargs, f'{client_name}.find_one()')
    options = cast('Callable[..., _FindOneOptions]', _FindOneOptions)
    return options(**bound.arguments)


def build_entity_action_params(entity_type: str, entity_id: int) -> dict[str, object]:
    """Build parameters for delete and revive operations.

    Args:
        entity_type: Entity type to mutate.
        entity_id: Numeric entity identifier.

    Returns:
        Detached entity identity parameters.
    """
    return {'type': entity_type, 'id': entity_id}


def build_find_plan(prepared: _PreparedFind, per_page: int) -> _FindPlan:
    """Finalize one prepared find with the server page limit.

    Args:
        prepared: Validated and detached find inputs.
        per_page: Readiness-derived server page limit.

    Returns:
        A paging plan with mutable parameter and paging dictionaries.
        Nested parameter values are shared with the prepared inputs.
    """
    params = dict(prepared.params)
    paging = {
        'entities_per_page': per_page,
        'current_page': max(prepared.page, 1),
    }
    if prepared.limit and prepared.limit <= per_page:
        paging['entities_per_page'] = prepared.limit
    params['paging'] = paging
    single_page = prepared.page > 0
    multi_page = not single_page and (prepared.limit == 0 or prepared.limit > per_page)
    return _FindPlan(
        limit=prepared.limit,
        multi_page=multi_page,
        paging=paging,
        params=params,
        single_page=single_page,
    )


def build_page_params(plan: _FindPlan, page_number: int, convert_utc: bool) -> object:
    """Build transformed parameters for one independent read page.

    Args:
        plan: Shared find plan.
        page_number: Page number to request.
        convert_utc: Captured outbound datetime conversion mode.

    Returns:
        Detached transformed page parameters.
    """
    built = dict(plan.params)
    built['paging'] = {**plan.paging, 'current_page': page_number}
    return _protocol.transform_outbound(built, convert_utc)


def expose_find_one_signature(function: Callable[..., object]) -> Callable[..., object]:
    """Expose the documented `find_one` signature and documentation.

    Args:
        function: Variadic method implementation.

    Returns:
        The same function with its public runtime contract attached.
    """
    return expose_signature(function, _METHOD_FIND_ONE_SIGNATURE, _FIND_ONE_DOCSTRING)


def expose_find_signature(function: Callable[..., object]) -> Callable[..., object]:
    """Expose the documented `find` signature and documentation.

    Args:
        function: Variadic method implementation.

    Returns:
        The same function with its public runtime contract attached.
    """
    return expose_signature(function, _METHOD_FIND_SIGNATURE, _FIND_DOCSTRING)


def finalize_find(
    records: list[dict[str, object]],
    options: FindOptions,
    plan: _FindPlan,
    local_path_field: str | None,
) -> list[dict[str, object]]:
    """Apply shared limit and local-path result normalization.

    Args:
        records: Validated records accumulated in wire order.
        options: Bound public find arguments.
        plan: Shared find plan.
        local_path_field: Platform-specific local path source key.

    Returns:
        Final normalized result list.
    """
    if options.limit and not plan.single_page:
        records = records[: options.limit]
    return _protocol.inject_local_paths(records, local_path_field)


def find_options_from_one(options: _FindOneOptions) -> FindOptions:
    """Convert bound find-one arguments into a one-result find plan.

    Args:
        options: Bound public find-one arguments.

    Returns:
        Equivalent one-page find options.
    """
    return FindOptions(
        entity_type=options.entity_type,
        filters=options.filters,
        fields=options.fields,
        order=options.order,
        filter_operator=options.filter_operator,
        limit=1,
        retired_only=options.retired_only,
        page=1,
        include_archived_projects=options.include_archived_projects,
        additional_filter_presets=options.additional_filter_presets,
    )


def first_result(results: list[dict[str, object]]) -> dict[str, object] | None:
    """Return the first find result when present.

    Args:
        results: Validated find result list.

    Returns:
        First entity or `None`.
    """
    return results[0] if results else None


def prepare_find(options: FindOptions) -> _PreparedFind:
    """Validate and detach one find before operation I/O.

    Args:
        options: Bound public find arguments.

    Returns:
        Detached inputs ready for server-aware paging.

    Raises:
        ShotgunError: If paging or ordering is invalid.
        Fault: If filter syntax is invalid.
    """
    _validate_nonnegative_integer(options.limit, 'limit')
    _validate_nonnegative_integer(options.page, 'page')
    translated_filters = copy.deepcopy(
        _protocol.translate_filters(
            _protocol.require_filter_container(options.filters, options.filter_operator),
            options.filter_operator,
        )
    )
    params: dict[str, object] = {
        'type': copy.deepcopy(options.entity_type),
        'return_fields': copy.deepcopy(list(options.fields)) if options.fields else ['id'],
        'filters': translated_filters,
        'return_only': 'retired' if options.retired_only else 'active',
        'paging': {'current_page': max(options.page, 1)},
        'api_return_image_urls': True,
    }
    sorts = _read_sorts(options.order)
    if sorts:
        params['sorts'] = sorts
    if not options.include_archived_projects:
        params['include_archived_projects'] = False
    if options.additional_filter_presets is not None:
        params['additional_filter_presets'] = copy.deepcopy(options.additional_filter_presets)
    return _PreparedFind(limit=options.limit, page=options.page, params=params)


def build_thumbnail_read_params(
    entity_type: str,
    entity_id: int,
    field_name: Literal['filmstrip_image', 'image'],
    records_per_page: int,
) -> dict[str, object]:
    """Build the single-page read for a source thumbnail field.

    Args:
        entity_type: Entity type owning the source thumbnail.
        entity_id: Identifier of the source entity.
        field_name: Thumbnail field to check.
        records_per_page: Server page limit from readiness.

    Returns:
        Parameters for a one-entity read.
    """
    return build_find_plan(
        prepare_find(
            FindOptions(
                entity_type=entity_type,
                filters=[['id', 'is', entity_id]],
                fields=[field_name],
                limit=1,
                page=1,
            )
        ),
        records_per_page,
    ).params


def require_boolean_result(value: object, method_name: str) -> bool:
    """Return an exact boolean RPC result or raise a value-free error.

    Args:
        value: RPC result.
        method_name: Logical method name for the error.

    Returns:
        Validated boolean.

    Raises:
        ShotgunError: If the result is not a boolean.
    """
    if not isinstance(value, bool):
        raise ShotgunError(f'{method_name} did not return a boolean')
    return value


def require_dictionary(value: object, message: str) -> dict[object, object]:
    """Return a dictionary after validating its container type.

    Args:
        value: Candidate mapping.
        message: Value-free validation error.

    Returns:
        The original dictionary. Key and value types are not checked.

    Raises:
        ShotgunError: If the value is not a dictionary.
    """
    if not isinstance(value, dict):
        raise ShotgunError(message)
    return cast('dict[object, object]', value)


def build_create_params(
    entity_type: str,
    data: object,
    return_fields: list[str] | None,
) -> dict[str, object]:
    """Build validated detached create parameters.

    Args:
        entity_type: Entity type to create.
        data: Public field mapping.
        return_fields: Optional requested return fields.

    Returns:
        Create RPC parameters.

    Raises:
        ShotgunError: If the data mapping, field names, or return fields are
            invalid.
    """
    validated_data = require_dictionary(data, 'create data must be a dictionary')
    _validate_field_names(validated_data, 'create field names must be nonempty strings')
    message = 'create return_fields must be an iterable of strings or None'
    if isinstance(return_fields, (str, bytes)):
        raise ShotgunError(message)
    use_default = not return_fields
    try:
        collected_fields = [] if return_fields is None else list(return_fields)
    except TypeError as error:
        raise ShotgunError(message) from error
    fields = _require_return_fields(collected_fields, message)
    return {
        'type': entity_type,
        'fields': _build_fields(cast('dict[str, object]', validated_data)),
        'return_fields': ['id'] if use_default else fields,
    }


def build_update_params(
    entity_type: str,
    entity_id: int,
    data: object,
    multi_entity_update_modes: object,
) -> dict[str, object]:
    """Build validated detached update parameters.

    Args:
        entity_type: Entity type to update.
        entity_id: Numeric entity identifier.
        data: Public field mapping.
        multi_entity_update_modes: Optional per-field update modes.

    Returns:
        Update RPC parameters.

    Raises:
        ShotgunError: If data, field names, or modes are invalid.
    """
    validated_data = require_dictionary(data, 'update data must be a dictionary')
    _validate_field_names(validated_data, 'update field names must be nonempty strings')
    validated_modes = _require_modes(
        multi_entity_update_modes,
        'multi_entity_update_modes must map strings to strings',
    )
    return {
        'type': entity_type,
        'id': entity_id,
        'fields': _build_fields(cast('dict[str, object]', validated_data), validated_modes),
    }


def require_dictionary_result(value: object, method_name: str) -> dict[str, object]:
    """Return a dictionary RPC result or raise a value-free error.

    Args:
        value: RPC result.
        method_name: Logical method name for the error.

    Returns:
        Validated dictionary.

    Raises:
        ShotgunError: If the result is not a dictionary.
    """
    if not isinstance(value, dict):
        raise ShotgunError(f'{method_name} did not return a dictionary')
    return cast('dict[str, object]', value)


def require_entity_id(value: object, message: str) -> int:
    """Return a non-boolean integer entity identifier.

    Args:
        value: Candidate identifier.
        message: Value-free validation error.

    Returns:
        Validated identifier.

    Raises:
        ShotgunError: If the value is not an integer identifier.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise ShotgunError(message)
    return value


def _batch_delete_call(index: int, member: dict[str, object]) -> dict[str, object]:
    """Build one validated batch delete call."""
    if 'entity_id' not in member:
        raise ShotgunError(f"batch delete request {index} is missing 'entity_id'")
    entity_id = require_entity_id(
        member['entity_id'],
        f'batch delete request {index} entity_id must be an integer',
    )
    return {
        'request_type': 'delete',
        'type': member['entity_type'],
        'id': entity_id,
    }


def _batch_update_call(index: int, member: dict[str, object]) -> dict[str, object]:
    """Build one validated batch update call."""
    if 'entity_id' not in member:
        raise ShotgunError(f"batch update request {index} is missing 'entity_id'")
    entity_id = require_entity_id(
        member['entity_id'],
        f'batch update request {index} entity_id must be an integer',
    )
    if 'data' not in member:
        raise ShotgunError(f"batch update request {index} is missing 'data'")
    data = member['data']
    if not isinstance(data, dict):
        raise ShotgunError(f'batch update request {index} data must be a dictionary')
    validated_data = cast('dict[object, object]', data)
    _validate_field_names(
        validated_data,
        f'batch update request {index} field names must be nonempty strings',
    )
    modes = _require_modes(
        member.get('multi_entity_update_modes'),
        f'batch update request {index} modes must map strings to strings',
    )
    return {
        'request_type': 'update',
        'type': member['entity_type'],
        'id': entity_id,
        'fields': _build_fields(cast('dict[str, object]', validated_data), modes),
    }


def _batch_call(index: int, request: object) -> dict[str, object]:
    """Validate and build one batch member without exposing its values."""
    if not isinstance(request, dict):
        raise ShotgunError(f'batch request {index} must be a dictionary')
    member = cast('dict[str, object]', request)
    if 'request_type' not in member:
        raise ShotgunError(f"batch request {index} is missing 'request_type'")
    if 'entity_type' not in member:
        raise ShotgunError(f"batch request {index} is missing 'entity_type'")
    entity_type = member['entity_type']
    if not isinstance(entity_type, str) or not entity_type:
        raise ShotgunError(f'batch request {index} has invalid entity_type')
    request_type = member['request_type']
    if not isinstance(request_type, str) or not request_type:
        raise ShotgunError(f'batch request {index} has invalid request_type')
    if request_type == 'create':
        return _batch_create_call(index, member)
    if request_type == 'update':
        return _batch_update_call(index, member)
    if request_type == 'delete':
        return _batch_delete_call(index, member)
    raise ShotgunError(f'batch request {index} has invalid request_type')


def require_list(value: object, message: str) -> list[object]:
    """Return a dynamically validated list.

    Args:
        value: Candidate list.
        message: Value-free validation error.

    Returns:
        Validated list.

    Raises:
        ShotgunError: If the value is not a list.
    """
    if not isinstance(value, list):
        raise ShotgunError(message)
    return cast('list[object]', value)


def build_batch_calls(requests: object) -> list[dict[str, object]]:
    """Validate and build detached calls for one batch request.

    Args:
        requests: Public batch request value.

    Returns:
        Validated wire calls in caller order.

    Raises:
        ShotgunError: If the batch or a member is malformed.
    """
    validated_requests = require_list(requests, 'batch requests must be a list')
    return [_batch_call(index, request) for index, request in enumerate(validated_requests)]


def require_list_result(value: object, method_name: str) -> list[object]:
    """Return a list RPC result or raise a value-free error.

    Args:
        value: RPC result.
        method_name: Logical method name for the error.

    Returns:
        Validated list.

    Raises:
        ShotgunError: If the result is not a list.
    """
    if not isinstance(value, list):
        raise ShotgunError(f'{method_name} did not return a list')
    return cast('list[object]', value)


def validate_read_page(
    raw_result: object,
    *,
    convert_utc: bool,
    records_count: int,
    plan: _FindPlan,
) -> _ReadPage:
    """Normalize and validate one read page with its stop decision.

    Args:
        raw_result: Raw driver result.
        convert_utc: Captured inbound datetime conversion mode.
        records_count: Count accumulated before this page.
        plan: Shared find plan.

    Returns:
        Validated entities and stop decision.

    Raises:
        ShotgunError: If the read result is malformed.
    """
    inbound = _protocol.transform_inbound(raw_result, convert_utc)
    if not isinstance(inbound, dict):
        raise ShotgunError('read did not return a dictionary')
    result = cast('dict[str, object]', inbound)
    raw_entities = result.get('entities', [])
    if not isinstance(raw_entities, list):
        raise ShotgunError('read did not return an entities list')
    entities = cast('list[object]', raw_entities)
    if any(not isinstance(entity, dict) for entity in entities):
        raise ShotgunError('read returned a non-dictionary entity')
    raw_paging_info = result.get('paging_info', {})
    if not isinstance(raw_paging_info, dict):
        raise ShotgunError('read did not return a paging_info dictionary')
    paging_info = cast('dict[str, object]', raw_paging_info)
    next_count = records_count + len(entities)
    verdict = _protocol.page_verdict(
        paging_info,
        len(entities),
        next_count,
        plan.paging['entities_per_page'],
        plan.limit,
    )
    return _ReadPage(
        entities=cast('list[dict[str, object]]', entities),
        stop=plan.single_page or verdict == 'stop',
    )
