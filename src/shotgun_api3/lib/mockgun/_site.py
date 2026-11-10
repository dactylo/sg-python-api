"""Expose test controls for the in-memory site behind a mockgun client."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import cast

from ._errors import ServerFaultError
from ._server import FakeServer
from ._site_data import json_copy

_DAYS_PER_WEEK = 7


class Site:
    """The in-memory site behind one mockgun client.

    The site holds the records, schema, sessions, clock and server metadata
    that the client reaches through its in-memory transport. Each directly
    constructed client has its own site.
    """

    def __init__(self, server: FakeServer) -> None:
        """Bind a facade to its server; tests obtain it from `client.site`."""
        self._server = server

    def now(self) -> datetime:
        """Return the site's current time as an aware UTC datetime in whole seconds."""
        return self._server.now()

    @property
    def server_metadata(self) -> dict[str, object]:
        """A copy of the metadata the site returns from `info`."""
        return self._server.server_metadata()

    def set_canned_fault(
        self, method: str, params: Mapping[str, object], message: str, code: int | None = None
    ) -> None:
        """Configure a fault for exactly matching navigation or export parameters.

        Args:
            method: `nav_expand`, `nav_search_string`, `nav_search_entity` or
                `export_page`.
            params: Exact wire parameters, detached through JSON; key order is
                ignored.
            message: Fault message returned by the server.
            code: Optional wire error code. A known code selects the matching
                `Fault` subclass (for example, 102 selects
                `AuthenticationFault`); the code is not otherwise exposed.

        Raises:
            ValueError: If the method is unsupported.
            TypeError: If parameters, message or code have invalid types.
        """
        with self._server.site_data_update() as data:
            if type(message) is not str or (code is not None and type(code) is not int):
                raise TypeError('canned fault needs a string message and integer or null code')
            data.set_canned(method, params, None, ServerFaultError(message, error_code=code))

    def set_canned_response(
        self, method: str, params: Mapping[str, object], response: object
    ) -> None:
        """Configure an exact response for navigation or page export.

        Args:
            method: `nav_expand`, `nav_search_string`, `nav_search_entity` or
                `export_page`.
            params: Exact wire parameters, compared after JSON decoding without
                key order.
            response: JSON response value, detached from the caller.

        Raises:
            ValueError: If the method is unsupported.
            TypeError: If parameters or response cannot be represented as JSON.
        """
        with self._server.site_data_update() as data:
            data.set_canned(method, params, response)

    def set_preferences(self, values: Mapping[str, object]) -> None:
        """Merge explicitly known preferences into the site settings.

        Args:
            values: String preference names and JSON-serializable values,
                deep-copied.

        Raises:
            TypeError: If a key is not a string or a value is not
                JSON-serializable.
        """
        with self._server.site_data_update() as data:
            if any(type(key) is not str for key in values):
                raise TypeError('preference keys must be strings')
            data.preferences.update(cast('dict[str, object]', json_copy(dict(values))))

    def set_subscriptions(self, values: Mapping[int, str]) -> None:
        """Replace configured HumanUser subscriptions in the supplied order.

        Args:
            values: Positive integer HumanUser IDs (excluding booleans) and
                string values.
                Record existence is checked when subscriptions are read.

        Raises:
            TypeError: If an ID or subscription has an unsupported type or
                value.
        """
        with self._server.site_data_update() as data:
            if any(
                type(key) is not int or key <= 0 or type(value) is not str
                for key, value in values.items()
            ):
                raise TypeError('subscriptions require positive integer IDs and string values')
            data.subscriptions = {
                int(key): value
                for key, value in cast('dict[str, str]', json_copy(dict(values))).items()
            }

    def set_time(self, when: datetime | None) -> None:
        """Freeze the clock at `when`, or follow real UTC time again with `None`.

        Args:
            when: A timezone-aware instant to freeze, or `None` to resume the
                real UTC clock.

        Raises:
            ValueError: If `when` is naive.
        """
        self._server.set_time(when)

    def set_work_week(self, working: Sequence[bool]) -> None:
        """Replace the default studio working week from Monday to Sunday.

        Args:
            working: Exactly seven booleans, deep-copied into the site.

        Raises:
            ValueError: If the input does not contain exactly seven booleans.
        """
        with self._server.site_data_update() as data:
            if len(working) != _DAYS_PER_WEEK or any(type(value) is not bool for value in working):
                raise ValueError('work week requires seven booleans from Monday to Sunday')
            data.work_week = cast('list[bool]', json_copy(list(working)))

    def update_server_metadata(self, values: Mapping[str, object]) -> None:
        """Merge values into the metadata the site returns from `info`.

        Clients cache metadata at readiness, as they do with a real server. A
        change reaches a client at its next readiness: its first request if it
        is not yet ready, or an explicit `info()` if it is already ready.
        """
        self._server.update_server_metadata(values)
