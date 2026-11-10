from collections.abc import Mapping


def schema(
    fields: Mapping[str, Mapping[str, str]],
    *,
    defaults: Mapping[tuple[str, str], object] | None = None,
    valid_types: Mapping[tuple[str, str], list[str]] | None = None,
    valid_values: Mapping[tuple[str, str], list[str]] | None = None,
    display_values: Mapping[tuple[str, str], dict[str, str]] | None = None,
) -> dict[str, dict[str, dict[str, dict[str, object]]]]:
    defaults = {} if defaults is None else defaults
    options: tuple[tuple[str, Mapping[tuple[str, str], object]], ...] = (
        ('valid_types', {} if valid_types is None else valid_types),
        ('valid_values', {} if valid_values is None else valid_values),
        ('display_values', {} if display_values is None else display_values),
    )
    return {
        entity_type: {
            field: {
                'data_type': {'value': data_type},
                'properties': {
                    'default_value': {'value': defaults.get((entity_type, field))},
                    **{
                        name: {'value': values[(entity_type, field)]}
                        for name, values in options
                        if (entity_type, field) in values
                    },
                },
            }
            for field, data_type in entity_fields.items()
        }
        for entity_type, entity_fields in fields.items()
    }
