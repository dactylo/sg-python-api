"""Awaitable delegation twin of the in-memory mockgun client."""

from typing import Any, ClassVar

from ._mockgun import Shotgun, expose_find_documentation, expose_find_one_documentation


class AsyncShotgun:
    """Provide the mockgun surface through awaitable methods.

    Each instance owns a synchronous in-memory delegate; awaitable operations
    execute against that delegate without network I/O. Each subclass receives
    its own delegate class, so changing its schema paths does not change those
    of sibling subclasses.

    Attributes:
        base_url: Constructor-supplied site URL, preserved unchanged.
        config: Mutable mock configuration.
        finds: Number of find operations performed.
    """

    _sync_type: ClassVar[type[Shotgun]] = Shotgun
    finds: int

    def __init__(  # noqa: PLR0913, PLR0917 - compatibility requires the exact sync signature.
        self,
        base_url: str,
        script_name: str | None = None,
        api_key: str | None = None,
        convert_datetimes_to_utc: bool = True,
        http_proxy: str | None = None,
        connect: bool = True,
        ca_certs: str | None = None,
        login: str | None = None,
        password: str | None = None,
        sudo_as_login: str | None = None,
        session_token: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        """Initialize an asynchronous wrapper around one mock client.

        Args:
            base_url: Site URL used to initialize mock configuration.
            script_name: Accepted compatibility script name.
            api_key: Accepted compatibility API key.
            convert_datetimes_to_utc: Accepted datetime-conversion setting.
            http_proxy: Accepted compatibility proxy setting.
            connect: Accepted compatibility connection setting.
            ca_certs: Accepted compatibility certificate path.
            login: Accepted compatibility user login.
            password: Accepted compatibility user password.
            sudo_as_login: Accepted compatibility sudo login.
            session_token: Accepted compatibility session token.
            auth_token: Accepted compatibility two-factor token.

        Raises:
            MockgunError: If schema paths are unset or files are missing.
            TypeError: If arguments do not match the compatibility signature.
            ValueError: If `base_url` has an unsupported or invalid origin.
        """
        self._sync = self._sync_type(
            base_url,
            script_name,
            api_key,
            convert_datetimes_to_utc,
            http_proxy,
            connect,
            ca_certs,
            login,
            password,
            sudo_as_login,
            session_token,
            auth_token,
        )
        self.config = self._sync.config
        self.base_url = self._sync.base_url

    def __getattr__(self, name: str) -> object:
        """Return the sync twin's live find counter."""
        if name == 'finds':
            return self._sync.finds
        raise AttributeError(f'{type(self).__name__!s} has no attribute {name!r}')

    def __init_subclass__(cls) -> None:
        """Give each async mock subclass its own synchronous delegate class."""
        super().__init_subclass__()
        cls._sync_type = type(
            f'_{cls.__name__}SyncDelegate',
            (cls._sync_type,),
            {'__module__': __name__},
        )

    async def batch(self, requests: list[dict[str, Any]]) -> list[object]:
        """Execute supported requests in order and return their result values.

        Requests apply sequentially. If a later request fails, earlier
        mutations remain applied.

        Args:
            requests: Create, update, and delete request dictionaries.

        Returns:
            Per-request results in input order.

        Raises:
            ShotgunError: If a request or delegated operation is invalid.
        """
        return self._sync.batch(requests)

    async def create(
        self,
        entity_type: str,
        data: dict[str, Any],
        return_fields: list[str] | None = None,
    ) -> dict[str, object]:
        """Create and return an entity in the in-memory database.

        Args:
            entity_type: Schema entity type to create.
            data: Field values for the new entity.
            return_fields: Fields to return instead of the fields in `data`.

        Returns:
            Created entity identity and the selected field values.

        Raises:
            ShotgunError: If the entity, fields, or values are invalid.
        """
        return self._sync.create(entity_type, data, return_fields)

    async def delete(self, entity_type: str, entity_id: int) -> bool:
        """Retire an active entity.

        Args:
            entity_type: Schema entity type to retire.
            entity_id: ID of the existing entity.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ShotgunError: If the entity type or ID is unknown.
        """
        return self._sync.delete(entity_type, entity_id)

    @expose_find_documentation
    async def find(  # noqa: PLR0913, PLR0917 - compatibility requires the exact sync signature.
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        limit: int = 0,
        retired_only: bool = False,
        page: int = 0,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> list[dict[str, object]]:
        return self._sync.find(
            entity_type,
            filters,
            fields,
            order,
            filter_operator,
            limit,
            retired_only,
            page,
            include_archived_projects,
            additional_filter_presets,
        )

    @expose_find_one_documentation
    async def find_one(  # noqa: PLR0913, PLR0917 - compatibility requires the exact sync signature.
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        retired_only: bool = False,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> dict[str, object] | None:
        return self._sync.find_one(
            entity_type,
            filters,
            fields,
            order,
            filter_operator,
            retired_only,
            include_archived_projects,
            additional_filter_presets,
        )

    @classmethod
    async def get_schema_paths(cls) -> tuple[str | None, str | None]:
        """Return the schema paths configured for mockgun clients.

        Returns:
            Field-schema and entity-schema paths, in that order.
        """
        return cls._sync_type.get_schema_paths()

    async def revive(self, entity_type: str, entity_id: int) -> bool:
        """Restore a retired entity to active state.

        Args:
            entity_type: Schema entity type to restore.
            entity_id: ID of the existing entity.

        Returns:
            `True` when the entity changed state, otherwise `False`.

        Raises:
            ShotgunError: If the entity type or ID is unknown.
        """
        return self._sync.revive(entity_type, entity_id)

    async def schema_entity_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded entity schema.

        Args:
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live entity-schema dictionary. Mutations affect this client.
        """
        return self._sync.schema_entity_read(project_entity)

    async def schema_field_read(
        self,
        entity_type: str,
        field_name: str | None = None,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded field schema for an entity or one field.

        Args:
            entity_type: Entity type whose fields to return.
            field_name: Optional field name to select.
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live entity field-schema dictionary when no field is selected.
            Otherwise, a new dictionary containing the selected field and its
            shared schema value, or an empty dictionary for an unknown field.
            Mutating shared schema values affects this client.

        Raises:
            KeyError: If `entity_type` is absent from the loaded schema.
        """
        return self._sync.schema_field_read(entity_type, field_name, project_entity)

    async def schema_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the loaded field schema.

        Args:
            project_entity: Accepted for public-client compatibility and
                ignored.

        Returns:
            The live field-schema dictionary. Mutations affect this client.
        """
        return self._sync.schema_read(project_entity)

    @classmethod
    async def set_schema_paths(
        cls,
        schema_path: str,
        schema_entity_path: str,
    ) -> None:
        """Configure the schema files loaded by future mockgun clients.

        Args:
            schema_path: Path to the field-schema pickle.
            schema_entity_path: Path to the entity-schema pickle.
        """
        cls._sync_type.set_schema_paths(schema_path, schema_entity_path)

    async def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, Any],
        multi_entity_update_modes: dict[str, str] | None = None,
    ) -> list[dict[str, object]]:
        """Update an entity and return the official one-item result list.

        Args:
            entity_type: Schema entity type to update.
            entity_id: ID of the existing entity.
            data: Replacement field values.
            multi_entity_update_modes: Per-field replacement modes.

        Returns:
            One updated entity containing identity and submitted fields.

        Raises:
            ShotgunError: If the entity, fields, or values are invalid.
        """
        return self._sync.update(
            entity_type,
            entity_id,
            data,
            multi_entity_update_modes,
        )
