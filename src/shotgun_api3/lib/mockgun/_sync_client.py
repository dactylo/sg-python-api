"""Run the real synchronous client against an isolated in-memory site."""

import dataclasses
from typing import ClassVar, Self

from shotgun_api3 import Shotgun as RealShotgun
from shotgun_api3 import _crud
from shotgun_api3._client import ConstructorOptions
from shotgun_api3._config import RequestContext
from shotgun_api3._transport import Transport

from ._new_client import (
    SHARED_SERVER,
    copy_client_settings,
    default_credentials,
    new_client_options,
)
from ._schema import load_schema
from ._server import FakeServer
from ._site import Site
from ._transport import InMemoryTransport


class Shotgun(RealShotgun):
    """A real client whose transport reaches only its own in-memory fake site.

    Configure schema paths before construction. Direct construction creates
    independent site state; `new_client()` shares the site with new
    credentials. Test controls are exposed through `site`.

    Attributes:
        finds: Number of find operations performed.
    """

    _in_process: ClassVar[bool] = True
    _schema_entity_path: ClassVar[str | None] = None
    _schema_path: ClassVar[str | None] = None

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
        Default construction performs fake-site readiness; supplying
        `auth_token` also performs the normal session-token exchange.

        Args:
            base_url: Flow Production Tracking site URL.
            script_name: Script name for script authentication.
            api_key: API key for script authentication.
            convert_datetimes_to_utc: Whether to convert datetime values to
                UTC.
            http_proxy: Optional proxy authority in `[user:pass@]host[:port]`
                form.
            connect: Whether to perform eager readiness during construction.
            ca_certs: Optional certificate-authority bundle path.
            login: Login for human-user authentication.
            password: Password for human-user authentication.
            sudo_as_login: Optional user login for sudo impersonation.
            session_token: Existing session token for authentication.
            auth_token: Optional two-factor authentication token.

        Raises:
            MockgunError: If schema paths are unset, files are missing, or
                readiness or session behavior is unsupported.
            OSError: If a schema file cannot be opened or read.
            EOFError: If a schema pickle ends prematurely.
            pickle.UnpicklingError: If a schema pickle is invalid.
            ValueError: If a pickle protocol is unsupported, constructor values
                fail real-client validation, or the retry environment setting
                is invalid.
            ShotgunError: If eager readiness or session exchange returns a
                modeled fault or invalid metadata.
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
            shared if shared is not None else FakeServer(load_schema(*self.get_schema_paths()))
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

    def _find(self, options: _crud.FindOptions) -> list[dict[str, object]]:
        """Count each query before dispatching through the real client."""
        self.finds += 1
        return super()._find(options)

    def _new_transport(self, context: RequestContext) -> Transport:
        """Bind every operation to this client's in-memory site."""
        del context
        return InMemoryTransport(self._server)

    @classmethod
    def get_schema_paths(cls) -> tuple[str | None, str | None]:
        """Return field-schema and entity-schema paths, in that order."""
        return cls._schema_path, cls._schema_entity_path

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
            MockgunError: If eager readiness or session behavior is
                unsupported.
            ShotgunError: If eager readiness or session exchange returns a
                modeled fault or invalid metadata.
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
    def set_schema_paths(cls, schema_path: str, schema_entity_path: str) -> None:
        """Configure the trusted schema pickles loaded by future instances.

        Args:
            schema_path: Path to the field-schema pickle.
            schema_entity_path: Path to the entity-schema pickle.
        """
        cls._schema_path = schema_path
        cls._schema_entity_path = schema_entity_path

    @property
    def site(self) -> Site:
        """The test-control facade for this client's in-memory site."""
        return self._site
