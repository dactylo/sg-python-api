"""Own the fake site's state and serialized RPC and file dispatch."""

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from threading import RLock
from typing import cast
from urllib.parse import urlsplit

from shotgun_api3._transport import TransportRequest, TransportResponse

from ._errors import MockgunError, ServerFaultError
from ._handlers import HANDLERS, UNWRAPPED_METHODS
from ._schema import SiteSchema
from ._sessions import RequestContext, Sessions
from ._site_data import SiteData
from ._storage import Storage
from ._store import Store
from ._uploads import handle_file_request
from ._wire import decode_call, fault_response, result_response

_DEFAULT_METADATA: dict[str, object] = {
    'version': [7, 10, 0],
    'full_version': [7, 10, 0, 0],
    's3_uploads_enabled': True,
    's3_direct_uploads_enabled': True,
    's3_enabled_upload_types': {'*': ['*']},
    'shotgunlocalhost_browser_integration_enabled': False,
    'unified_login_flow_enabled': False,
    'authentication_app_session_launcher_enabled': False,
    'analytics_site_id': 'mockgun',
    'analytics_site_name': 'mockgun',
    'user_authentication_method': 'default',
    'api_max_entities_per_page': 500,
}


class FakeServer:
    """An isolated site's state and synchronous, serialized request dispatcher.

    Attributes:
        schema: Loaded declarations used by RPC handlers.
        store: Records shared by every operation on this site.
        storage: Uploaded bytes retained independently of record transactions.
        site_data: Explicit preferences, work week, subscriptions and canned
            responses.
        site_host: First successful RPC hostname, or `None` before one
            succeeds.
        sessions: Credential admission and token identities shared by this
            site.
    """

    def __init__(self, schema: SiteSchema) -> None:
        """Create an independent site using the supplied schema snapshot."""
        self.schema = schema
        self.store = Store(schema)
        self.storage = Storage()
        self.site_data = SiteData()
        self.site_host: str | None = None
        self.sessions = Sessions(self.store, schema)
        self._lock = RLock()
        self._time: datetime | None = None
        self._metadata = deepcopy(_DEFAULT_METADATA)

    def admit_login(self, login: str) -> None:
        """Admit a constructor login for later session resolution."""
        with self._lock:
            self.sessions.admit_login(login)

    def now(self) -> datetime:
        """Return the site's aware UTC time with whole-second precision."""
        with self._lock:
            return self._time or datetime.now(UTC).replace(microsecond=0)

    def context_for(
        self, auth: Mapping[str, object] | None, *, method_name: str
    ) -> RequestContext:
        """Capture the clock and resolve identities once for one request.

        Call under the server's request lock for RPC or HTTP dispatch.

        Args:
            auth: Wire credentials, or `None` for an anonymous request.
            method_name: RPC or route name controlling anonymous readiness.

        Returns:
            The request's actor, time, session UUID and sudo identities.

        Raises:
            ServerFaultError: If authentication fails.
            MockgunError: If a credential combination is not emulated.
        """
        now = self.now()
        credentials = auth or {}
        actor = (
            self.sessions.resolve(credentials, now=now)
            if method_name != 'info' and (method_name == 'get_session_token' or auth is not None)
            else None
        )
        sudo = isinstance(credentials.get('sudo_as_login'), str)
        actual = self.sessions.script_actor(credentials, now=now) if sudo else None
        return RequestContext(
            actor,
            now,
            cast('str | None', credentials.get('session_uuid')),
            sudo=sudo,
            actual=actual,
        )

    def handle(self, request: TransportRequest) -> TransportResponse:
        """Dispatch one complete request while holding the site's lock.

        Raises:
            MockgunError: If the request or RPC method is not emulated.
        """
        with self._lock:
            parsed = urlsplit(request.url)
            if request.method != 'POST' or not parsed.path.endswith('/api3/json'):
                return handle_file_request(self, request)
            try:
                call = decode_call(request)
                context = self.context_for(call.auth, method_name=call.method_name)
                self.store.context = context
                handler = HANDLERS.get(call.method_name)
                if handler is None:
                    raise MockgunError(
                        f"mockgun does not emulate the server's {call.method_name!r} method yet"
                    )
                result = handler(self, call, context)
                if self.site_host is None:
                    self.site_host = parsed.hostname
                return result_response(
                    result,
                    wrapped=call.method_name not in UNWRAPPED_METHODS,
                )
            except ServerFaultError as fault:
                return fault_response(fault)

    def server_metadata(self) -> dict[str, object]:
        """Return a detached copy of the site's readiness metadata."""
        with self._lock:
            return deepcopy(self._metadata)

    def set_time(self, when: datetime | None) -> None:
        """Freeze at an aware time, or resume real UTC time with `None`.

        Raises:
            ValueError: If `when` is naive.
        """
        if when is not None and when.utcoffset() is None:
            raise ValueError('site time must be an aware datetime')
        with self._lock:
            self._time = when.astimezone(UTC).replace(microsecond=0) if when is not None else None

    @contextmanager
    def site_data_update(self) -> Generator[SiteData, None, None]:
        """Hold the server lock while configuring non-journaled site settings.

        Yields:
            The site's settings, accessible only while the lock is held.
        """
        with self._lock:
            yield self.site_data

    def update_server_metadata(self, values: Mapping[str, object]) -> None:
        """Merge detached values into the site's readiness metadata."""
        with self._lock:
            self._metadata.update(deepcopy(values))
