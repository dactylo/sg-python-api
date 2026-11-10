"""Load schema snapshots and resolve declared field paths."""

import pickle
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

from ._errors import MockgunError, ServerFaultError

_LINK_PATH_PARTS = 3


class _SchemaReader(Protocol):
    """Describe the schema export client surface."""

    def schema_entity_read(self) -> Mapping[str, object]:
        """Return entity schema metadata.

        Returns:
            Entity schema metadata keyed by entity type.
        """
        ...

    def schema_read(self) -> Mapping[str, object]:
        """Return field schema metadata.

        Returns:
            Field schema metadata keyed by entity type.
        """
        ...


@dataclass
class SiteSchema:
    """Hold schema metadata and site-local field changes.

    Attributes:
        fields: Field metadata keyed by entity type, then field name.
        entities: Entity metadata keyed by entity type.
        site_created: `(entity_type, field_name)` identities created on this
            fake site.
        retired_fields: Deleted field identities retained for subsequent
            field-creation validation.
        visibility: Per-field Project visibility overrides, keyed by
            `(entity_type, field_name)` and then Project ID.
    """

    fields: dict[str, dict[str, object]]
    entities: dict[str, dict[str, object]]
    site_created: set[tuple[str, str]] = field(default_factory=set[tuple[str, str]])
    retired_fields: set[tuple[str, str]] = field(default_factory=set[tuple[str, str]])
    visibility: dict[tuple[str, str], dict[int, bool]] = field(
        default_factory=dict[tuple[str, str], dict[int, bool]]
    )

    def declares(self, entity_type: str, field_name: str | None = None) -> bool:
        """Return whether an entity or field exists in the loaded field schema."""
        return entity_type in self.fields and (
            field_name is None or field_name in self.fields[entity_type]
        )

    def field_type(self, entity_type: str, path: str) -> str:
        """Resolve a direct or deep-linked field to its declared data type.

        Raises:
            ServerFaultError: If the entity, field, or linked path is invalid.
        """
        if entity_type not in self.fields:
            raise ServerFaultError(f'{entity_type} is not a valid entity')
        parts = path.split('.', 2)
        field = parts[0] if len(parts) == _LINK_PATH_PARTS else path
        if field not in self.fields[entity_type] and field not in {'id', 'type'}:
            raise ServerFaultError(f'{field} is not a valid field for entity {entity_type}')
        if field == 'id':
            field_type = 'number'
        elif field == 'type':
            field_type = 'entity_type'
        else:
            metadata = cast('dict[str, object]', self.fields[entity_type][field])
            field_type = cast('str', cast('dict[str, object]', metadata['data_type'])['value'])
        if len(parts) == _LINK_PATH_PARTS:
            if field_type not in {'entity', 'multi_entity'}:
                raise ServerFaultError(
                    f'{field} is not a valid linked field for entity {entity_type}'
                )
            return self.field_type(parts[1], parts[2])
        return field_type

    def scoped(
        self, project_id: int | None, entity_type: str | None = None
    ) -> dict[str, dict[str, object]]:
        """Return detached field metadata with one Project's visibility settings."""
        fields = (
            deepcopy(self.fields)
            if entity_type is None
            else {entity_type: deepcopy(self.fields[entity_type])}
        )
        if project_id is None:
            return fields
        for (kind, field_name), projects in self.visibility.items():
            if project_id in projects and field_name in fields.get(kind, {}):
                entry = cast('dict[str, object]', fields[kind][field_name])
                visible = cast(
                    'dict[str, object]',
                    entry.setdefault('visible', {'value': projects[project_id], 'editable': True}),
                )
                visible['value'] = projects[project_id]
        return fields


def generate_schema(
    sg: _SchemaReader,
    schema_file_path: str,
    schema_entity_file_path: str,
) -> None:
    """Write the schema dictionaries needed to initialize mockgun.

    Write the field schema first and the entity schema second, using pickle
    protocol 2. Existing destination files are replaced. A failure can leave
    incomplete output; the two files are not published atomically.

    Args:
        sg: Client that supplies the schema dictionaries.
        schema_file_path: Destination for the field schema.
        schema_entity_file_path: Destination for the entity schema.

    Raises:
        OSError: If a destination cannot be opened or written.
        ShotgunError: If a supplied Shotgun client's schema request fails.
        ProtocolError: If a supplied Shotgun client's schema request receives
            an HTTP error.
        ValueError: If a supplied Shotgun client has invalid configuration.
    """
    with Path(schema_file_path).open('wb') as schema_file:
        pickle.dump(sg.schema_read(), schema_file, protocol=2)
    with Path(schema_entity_file_path).open('wb') as schema_entity_file:
        pickle.dump(sg.schema_entity_read(), schema_entity_file, protocol=2)


def load_schema(schema_path: str | None, schema_entity_path: str | None) -> SiteSchema:
    """Load the configured trusted field and entity schema pickles.

    Args:
        schema_path: Field-schema pickle path, or `None` when unconfigured.
        schema_entity_path: Entity-schema pickle path, or `None` when
            unconfigured.

    Returns:
        Schema metadata loaded from the two files.

    Raises:
        MockgunError: If paths are unconfigured or either file is missing.
        OSError: If a schema file cannot be opened or read.
        EOFError: If a schema pickle is empty or truncated.
        pickle.UnpicklingError: If a schema file cannot be decoded as a pickle.
        ValueError: If a pickle requires an unsupported protocol.
    """
    if schema_path is None or schema_entity_path is None:
        raise MockgunError(
            'mockgun schema paths are not configured; call Shotgun.set_schema_paths() first'
        )
    if not Path(schema_path).is_file():
        raise MockgunError(f'mockgun schema file does not exist: {schema_path}')
    if not Path(schema_entity_path).is_file():
        raise MockgunError(f'mockgun schema_entity file does not exist: {schema_entity_path}')
    with Path(schema_path).open('rb') as schema_file:
        fields = cast('dict[str, dict[str, object]]', pickle.load(schema_file))  # noqa: S301
    with Path(schema_entity_path).open('rb') as schema_entity_file:
        entities = cast('dict[str, dict[str, object]]', pickle.load(schema_entity_file))  # noqa: S301
    return SiteSchema(fields, entities)
