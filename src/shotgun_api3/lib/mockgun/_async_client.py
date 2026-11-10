"""Run the real asynchronous client against an isolated in-memory site."""

import dataclasses
from typing import ClassVar, Self

from shotgun_api3 import _crud
from shotgun_api3._async_client import AsyncShotgun as RealAsyncShotgun
from shotgun_api3._async_transport import AsyncTransport
from shotgun_api3._client import ConstructorOptions
from shotgun_api3._config import RequestContext

from ._new_client import (
    SHARED_SERVER,
    copy_client_settings,
    default_credentials,
    new_client_options,
)
from ._schema import load_schema
from ._server import FakeServer
from ._site import Site
from ._sync_client import Shotgun
from ._transport import AsyncInMemoryTransport


class AsyncShotgun(RealAsyncShotgun):
    """A real client whose transport reaches only its own in-memory fake site.

    Configure schema paths before construction. Direct construction creates
    independent site state; `new_client()` shares the site with new
    credentials. Test controls are exposed through `site`.

    Attributes:
        finds: Number of find operations performed.
    """

    _in_process: ClassVar[bool] = True
    _schema_paths: ClassVar[tuple[str, str] | None] = None

    def __init__(
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
        """Initialize a fake site before the real client's readiness operation.

        Load the configured schema files synchronously for a new fake site.
        Readiness and session operations remain deferred to the async client's
        normal entry points.

        Args:
            base_url: Flow Production Tracking site URL.
            script_name: Script name for script authentication.
            api_key: API key for script authentication.
            convert_datetimes_to_utc: Whether to convert datetime values to
                UTC.
            http_proxy: Optional proxy authority in `[user:pass@]host[:port]`
                form.
            connect: Whether to request eager readiness during asynchronous
                context entry.
            ca_certs: Optional certificate-authority bundle path.
            login: Login for human-user authentication.
            password: Password for human-user authentication.
            sudo_as_login: Optional user login for sudo impersonation.
            session_token: Existing session token for authentication.
            auth_token: Optional two-factor authentication token.

        Raises:
            MockgunError: If schema paths are unset or files are missing.
            OSError: If a schema file cannot be opened or read.
            EOFError: If a schema pickle ends prematurely.
            pickle.UnpicklingError: If a schema pickle is invalid.
            ValueError: If a pickle protocol is unsupported, constructor values
                fail real-client validation, or the retry environment setting
                is invalid.
        """
        options = ConstructorOptions(
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
        shared = SHARED_SERVER.get()
        if shared is None:
            options = default_credentials(options)
        self._server = (
            shared
            if shared is not None
            else FakeServer(load_schema(*self._configured_schema_paths()))
        )
        self._options = options
        self._site = Site(self._server)
        if options.login is not None:
            self._server.admit_login(options.login)
        self.finds = 0
        super().__init__(
            options.base_url,
            options.script_name,
            options.api_key,
            options.convert_datetimes_to_utc,
            options.http_proxy,
            options.connect,
            options.ca_certs,
            options.login,
            options.password,
            options.sudo_as_login,
            options.session_token,
            options.auth_token,
        )

    @classmethod
    def _configured_schema_paths(cls) -> tuple[str | None, str | None]:
        """Resolve subclass paths or the base synchronous client's defaults."""
        return cls._schema_paths if cls._schema_paths is not None else Shotgun.get_schema_paths()

    async def _find(self, options: _crud.FindOptions) -> list[dict[str, object]]:
        """Count each query before dispatching through the real client."""
        self.finds += 1
        return await super()._find(options)

    def _new_transport(self, context: RequestContext) -> AsyncTransport:
        """Bind every operation to this client's in-memory site."""
        del context
        return AsyncInMemoryTransport(self._server)

    @classmethod
    async def get_schema_paths(cls) -> tuple[str | None, str | None]:
        """Return field-schema and entity-schema paths, in that order."""
        return cls._configured_schema_paths()

    def new_client(self, **credentials: object) -> Self:
        """Create a client with new credentials sharing this client's fake site.

        This is a plain method on both sync and async clients. The returned
        client has its own readiness and session state. Timeout, retry and
        user-agent settings are copied; other noncredential options use their
        original constructor values.

        Args:
            **credentials: Constructor credential arguments. A lone
                `sudo_as_login` preserves the original credentials; otherwise
                only the supplied credentials are used.

        Returns:
            A new instance of this client's class sharing schema and records.

        Raises:
            ValueError: If constructor credentials, retained configuration, or
                the retry environment setting fail validation.
        """
        options = new_client_options(self._options, credentials)
        token = SHARED_SERVER.set(self._server)
        try:
            client = type(self)(**dataclasses.asdict(options))
        finally:
            SHARED_SERVER.reset(token)
        copy_client_settings(self.config, client.config)
        return client

    @classmethod
    async def set_schema_paths(cls, schema_path: str, schema_entity_path: str) -> None:
        """Configure the trusted schema pickles loaded by future instances.

        On `AsyncShotgun` itself, these paths are shared with
        `mockgun.Shotgun`.

        Args:
            schema_path: Path to the field-schema pickle.
            schema_entity_path: Path to the entity-schema pickle.
        """
        if cls is AsyncShotgun:
            Shotgun.set_schema_paths(schema_path, schema_entity_path)
        else:
            cls._schema_paths = (schema_path, schema_entity_path)

    @property
    def site(self) -> Site:
        """The test-control facade for this client's in-memory site."""
        return self._site
