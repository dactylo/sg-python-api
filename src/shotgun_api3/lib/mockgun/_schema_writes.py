"""Apply observed field-schema mutations to one in-memory fake site."""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._schema import SiteSchema

_CREATE_ACCEPTED: dict[str, frozenset[str]] = {
    'checkbox': frozenset({'default_value'}),
    'color': frozenset(),
    'entity': frozenset({'valid_types'}),
    'float': frozenset({'description'}),
    'list': frozenset({'valid_values'}),
    'multi_entity': frozenset({'valid_types'}),
    'number': frozenset({'summary_default'}),
    'status_list': frozenset({'valid_values'}),
    'text': frozenset(),
}
_KNOWN_TYPES = frozenset(
    {
        'addressing',
        'calculated',
        'checkbox',
        'color',
        'currency',
        'date',
        'date_time',
        'duration',
        'entity',
        'entity_type',
        'float',
        'footage',
        'image',
        'jsonb',
        'list',
        'multi_entity',
        'number',
        'password',
        'percent',
        'pivot_column',
        'serializable',
        'status_list',
        'summary',
        'tag_list',
        'text',
        'timecode',
        'url',
        'uuid',
    }
)
_MAX_DISPLAY_NAME_LENGTH = 80
_NAME_CHARACTERS = frozenset('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .-!?')
_NAME_REPLACEMENTS = str.maketrans({' ': '_', '-': '_', '.': '_', '!': '_', '?': '_'})
_SUPPORTED_TYPES = frozenset(
    {
        'checkbox',
        'entity',
        'float',
        'list',
        'multi_entity',
        'number',
        'text',
    }
)


def _create_properties(data_type: str, supplied: dict[str, object]) -> dict[str, object]:
    """Build the observed property wrappers after validation succeeds."""
    result: dict[str, object] = {
        'default_value': {
            'value': False if data_type == 'checkbox' else None,
            'editable': data_type in {'list', 'checkbox'},
        },
        'summary_default': {
            'value': 'sum' if data_type == 'number' else 'none',
            'editable': True,
        },
    }
    if data_type == 'list' and 'valid_values' in supplied:
        result['valid_values'] = {'value': deepcopy(supplied['valid_values']), 'editable': True}
    if data_type in {'entity', 'multi_entity'} and 'valid_types' in supplied:
        result['valid_types'] = {
            'value': deepcopy(supplied['valid_types']),
            'editable': data_type == 'entity',
        }
    return result


def _entry(
    display_name: str,
    entity_type: str,
    data_type: str,
    properties: dict[str, object],
    supplied: dict[str, object],
) -> dict[str, object]:
    """Build a detached schema entry in the observed metadata order."""
    return {
        'name': {'value': display_name, 'editable': True},
        'description': {'value': supplied.get('description', ''), 'editable': True},
        'custom_metadata': {'value': '', 'editable': True},
        'entity_type': {'value': entity_type, 'editable': False},
        'data_type': {'value': data_type, 'editable': False},
        'editable': {'value': True, 'editable': False},
        'mandatory': {'value': False, 'editable': False},
        'unique': {'value': False, 'editable': False},
        'properties': properties,
        'visible': {'value': True, 'editable': True},
        'ui_value_displayable': {'value': True, 'editable': False},
    }


def _field_for_write(schema: SiteSchema, entity_type: str, field_name: str) -> dict[str, object]:
    """Require an existing custom field before updating or deleting it."""
    if not schema.declares(entity_type, field_name):
        raise ServerFaultError(
            f'{field_name} is not a valid field for entity {entity_type}', error_code=103
        )
    if not (field_name.startswith('sg_') or (entity_type, field_name) in schema.site_created):
        raise MockgunError(f'{entity_type}.{field_name} built-in schema write is unsettled')
    return cast('dict[str, object]', schema.fields[entity_type][field_name])


def _field_name(
    schema: SiteSchema, entity_type: str, display_name: object, problems: list[Exception]
) -> str:
    """Derive one observed name, considering declared and retired collisions."""
    if not isinstance(display_name, str) or not display_name:
        problems.append(ServerFaultError('display name is empty', error_code=103))
        return ''
    if len(display_name) > _MAX_DISPLAY_NAME_LENGTH or any(
        char not in _NAME_CHARACTERS for char in display_name
    ):
        problems.append(MockgunError('schema field display name contains unsettled characters'))
        return ''
    base = 'sg_' + display_name.lower().translate(_NAME_REPLACEMENTS)
    if (entity_type, base) in schema.retired_fields:
        problems.append(
            ServerFaultError(
                'API schema_field_create() failed, there is a retired field with the same '
                f'field_name: {base}. Delete the retired field forever from the Trash Page in '
                'Shotgun and try again.',
                error_code=105,
            )
        )
        return ''
    for candidate in (base, f'{base}_1', f'{base}_2'):
        if (entity_type, candidate) in schema.retired_fields:
            problems.append(
                MockgunError(f'{entity_type}.{candidate} retired collision is unsettled')
            )
            return ''
        if not schema.declares(entity_type, candidate):
            return candidate
    problems.append(MockgunError(f'{entity_type}.{base} third collision is unsettled'))
    return ''


def _properties(properties: Sequence[Mapping[str, object]]) -> tuple[dict[str, object], bool]:
    """Collect wire property pairs while identifying duplicate names."""
    result: dict[str, object] = {}
    repeated = False
    for property_entry in properties:
        name = property_entry.get('property_name')
        if not isinstance(name, str):
            raise MockgunError('schema field property name is unsettled')
        if name in result:
            repeated = True
        result[name] = property_entry.get('value')
    return result, repeated


def _raise_problems(problems: list[Exception]) -> None:
    """Choose the settled single error, or reject a competing-error request."""
    if len(problems) > 1:
        raise MockgunError('schema field request has several errors')
    if problems:
        raise problems[0]


def _updated_entry(entry: dict[str, object], supplied: dict[str, object]) -> dict[str, object]:
    """Apply site-wide property values to a detached schema entry."""
    updated = deepcopy(entry)
    for name, value in supplied.items():
        if name == 'visible':
            continue
        if name in {'name', 'description'}:
            wrapper = updated.get(name)
            if isinstance(wrapper, dict):
                cast('dict[str, object]', wrapper)['value'] = value
            else:
                updated[name] = {'value': value, 'editable': True}
        else:
            cast('dict[str, dict[str, object]]', updated['properties'])[name]['value'] = deepcopy(
                value
            )
    return updated


def _valid_names(values: object) -> bool:
    """Recognize a nonempty ordered list of type or option names."""
    if not isinstance(values, list) or not values:
        return False
    return all(isinstance(value, str) and bool(value) for value in cast('list[object]', values))


def _validate_required_create(
    data_type: str, supplied: dict[str, object], problems: list[Exception]
) -> None:
    """Check the one required value of each observed field type."""
    if data_type == 'checkbox':
        if 'default_value' not in supplied:
            problems.append(ServerFaultError('checkbox requires default_value', error_code=105))
        elif supplied['default_value'] is not False:
            problems.append(MockgunError('checkbox default_value is unsettled'))
    elif data_type == 'number':
        if supplied.get('summary_default') != 'sum' and (
            'summary_default' in supplied or 'default_value' not in supplied
        ):
            problems.append(MockgunError('number summary_default is unsettled'))
    elif data_type in {'list', 'entity', 'multi_entity'}:
        key = 'valid_values' if data_type == 'list' else 'valid_types'
        values: object = supplied.get(key)
        if (
            data_type == 'multi_entity'
            and isinstance(values, list)
            and len(cast('list[object]', values)) > 1
        ):
            problems.append(
                ServerFaultError('multi_entity valid_types is invalid', error_code=103)
            )
        elif not _valid_names(cast('object', values)) or (
            data_type == 'entity' and len(cast('list[str]', values)) > 1
        ):
            problems.append(MockgunError(f'{data_type} {key} is unsettled'))


def _validate_create_type(
    data_type: str, supplied: dict[str, object], problems: list[Exception]
) -> bool:
    """Record create faults and indicate whether an entry can be assembled."""
    accepted = _CREATE_ACCEPTED.get(data_type)
    if accepted is not None:
        for name in supplied.keys() - accepted:
            if name == 'default_value' and data_type in {'number', 'float'}:
                problems.append(
                    ServerFaultError(f'{data_type} default_value is invalid', error_code=103)
                )
            else:
                problems.append(MockgunError(f'{data_type}.{name} schema property is unsettled'))
    if data_type == 'color':
        problems.append(ServerFaultError('color field creation failed', error_code=103))
        return False
    if data_type == 'status_list':
        if 'valid_values' not in supplied:
            problems.append(ServerFaultError('status_list requires valid_values', error_code=103))
        else:
            problems.append(MockgunError('status_list display_values are unsettled'))
        return False
    if data_type not in _SUPPORTED_TYPES:
        problems.append(MockgunError(f'{data_type} schema field creation is unsettled'))
        return False
    _validate_required_create(data_type, supplied, problems)
    return True


def _visible_problem(field_name: str, value: object, project_id: int | None) -> Exception | None:
    """Reject visibility updates without a Project or boolean setting."""
    if project_id is None:
        return ServerFaultError(f'{field_name}.visible requires a Project', error_code=104)
    return (
        None
        if isinstance(value, bool)
        else MockgunError(f'{field_name}.visible value is unsettled')
    )


def _update_problem(
    field_name: str,
    data_type: str,
    entry: dict[str, object],
    property_pair: tuple[str, object],
    project_id: int | None,
) -> Exception | None:
    """Return a rejection for one field-property update, if any."""
    name, value = property_pair
    if name == 'visible':
        return _visible_problem(field_name, value, project_id)
    if name in {'data_type', 'mandatory'}:
        return ServerFaultError(f'{field_name}.{name} cannot be updated', error_code=103)
    if name in {'name', 'description'}:
        return (
            None
            if isinstance(value, str)
            else MockgunError(f'{field_name}.{name} update value is unsettled')
        )
    if name == 'valid_values' and data_type == 'list':
        return (
            None
            if _valid_names(value)
            else MockgunError(f'{field_name}.valid_values update is unsettled')
        )
    if name == 'default_value' and data_type == 'list':
        values = cast('dict[str, dict[str, object]]', entry['properties'])['valid_values']['value']
        return (
            None
            if value is None or value in cast('list[str]', values)
            else MockgunError(f'{field_name}.default_value update is unsettled')
        )
    return MockgunError(f'{field_name}.{name} update is unsettled')


def create_field(
    schema: SiteSchema,
    entity_type: str,
    data_type: str,
    properties: Sequence[Mapping[str, object]],
) -> tuple[str, dict[str, object]]:
    """Create an observed schema field after checking all competing errors.

    Args:
        schema: Mutable site schema receiving the field.
        entity_type: Entity type receiving the field.
        data_type: Requested schema data type.
        properties: Wire entries containing `property_name` and `value`,
            including the field's display `name`.

    Returns:
        The generated internal field name and its live stored metadata entry.

    Raises:
        ServerFaultError: If the request has a single modeled schema fault.
        MockgunError: If requested behavior, a property, a name collision, or
            competing error precedence is unsupported.
    """
    supplied, repeated = _properties(properties)
    problems: list[Exception] = []
    if repeated:
        problems.append(MockgunError('schema_field_create repeated property is unsettled'))
    if not schema.declares(entity_type):
        problems.append(ServerFaultError(f'{entity_type} is not a valid entity', error_code=103))
    known = _KNOWN_TYPES | frozenset(
        cast('str', cast('dict[str, object]', entry['data_type'])['value'])
        for fields in schema.fields.values()
        for entry in cast('dict[str, dict[str, object]]', fields).values()
    )
    if data_type not in known:
        problems.append(ServerFaultError(f'{data_type} is not a valid data type', error_code=103))
    name = _field_name(schema, entity_type, supplied.get('name'), problems)
    supplied_properties = {name: value for name, value in supplied.items() if name != 'name'}
    supported = (
        _validate_create_type(data_type, supplied_properties, problems)
        if data_type in known
        else False
    )
    _raise_problems(problems)
    display_name = cast('str', supplied['name'])
    entry_properties = _create_properties(data_type, supplied_properties) if supported else {}
    entry = _entry(display_name, entity_type, data_type, entry_properties, supplied)
    schema.fields[entity_type][name] = entry
    schema.site_created.add((entity_type, name))
    return name, entry


def delete_field(schema: SiteSchema, entity_type: str, field_name: str) -> None:
    """Remove a custom field and reserve its name for this site's lifetime.

    Args:
        schema: Mutable site schema containing the field.
        entity_type: Entity type containing the field.
        field_name: Internal name of the custom field to remove.

    Raises:
        ServerFaultError: If the entity or field does not exist.
        MockgunError: If the field is a built-in field whose deletion is
            unsupported.
    """
    _field_for_write(schema, entity_type, field_name)
    del schema.fields[entity_type][field_name]
    schema.visibility.pop((entity_type, field_name), None)
    schema.site_created.discard((entity_type, field_name))
    schema.retired_fields.add((entity_type, field_name))


def update_field(
    schema: SiteSchema,
    entity_type: str,
    field_name: str,
    properties: Sequence[Mapping[str, object]],
    *,
    project_id: int | None,
) -> None:
    """Apply supported property updates atomically to a custom field.

    Args:
        schema: Mutable site schema containing the field.
        entity_type: Entity type containing the field.
        field_name: Internal name of the custom field to update.
        properties: Wire entries containing `property_name` and `value`.
        project_id: Project-specific visibility scope, or `None` for an
            unscoped update.

    Raises:
        ServerFaultError: If the field is absent or the request has a single
            modeled property fault.
        MockgunError: If the field, property update, or competing error
            precedence is unsupported.
    """
    entry = _field_for_write(schema, entity_type, field_name)
    supplied, repeated = _properties(properties)
    problems: list[Exception] = []
    if repeated:
        problems.append(MockgunError(f'{entity_type}.{field_name} repeated update property'))
    data_type = cast('str', cast('dict[str, object]', entry['data_type'])['value'])
    for name, value in supplied.items():
        problem = _update_problem(field_name, data_type, entry, (name, value), project_id)
        if problem is not None:
            problems.append(problem)
    _raise_problems(problems)
    schema.fields[entity_type][field_name] = _updated_entry(entry, supplied)
    if 'visible' in supplied and project_id is not None:
        schema.visibility.setdefault((entity_type, field_name), {})[project_id] = cast(
            'bool', supplied['visible']
        )
