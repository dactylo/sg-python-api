"""Construct independent credential sessions against a shared fake site."""

from __future__ import annotations

import dataclasses
from contextvars import ContextVar
from typing import TYPE_CHECKING

from shotgun_api3._client import ConstructorOptions

if TYPE_CHECKING:
    from collections.abc import Mapping

    from shotgun_api3._config import Config

    from ._server import FakeServer

_CREDENTIAL_FIELDS = (
    'script_name',
    'api_key',
    'login',
    'password',
    'sudo_as_login',
    'session_token',
    'auth_token',
)
_DEFAULT_SCRIPT = 'mockgun'
SHARED_SERVER: ContextVar[FakeServer | None] = ContextVar('mockgun_shared_server', default=None)


def copy_client_settings(source: Config, target: Config) -> None:
    """Copy timeout, retry and user-agent settings into a newly constructed client.

    Args:
        source: Original client's current configuration.
        target: New client's configuration, updated atomically.
    """
    target.update_fields(
        timeout_secs=source.timeout_secs,
        max_rpc_attempts=source.max_rpc_attempts,
        rpc_attempt_interval=source.rpc_attempt_interval,
        user_agents=source.user_agents,
    )


def default_credentials(options: ConstructorOptions) -> ConstructorOptions:
    """Give a directly constructed client without credentials the default script's.

    Earlier mockgun versions accepted a base URL alone, so tests written for
    them keep working. A partial or combined credential form still fails as the
    real client's validation decides.

    Args:
        options: Bound constructor arguments of a client that owns a new site.

    Returns:
        The same options, or a copy acting as the `mockgun` script when no
        credential other than `sudo_as_login` was supplied.
    """
    if any(
        getattr(options, name) is not None
        for name in _CREDENTIAL_FIELDS
        if name != 'sudo_as_login'
    ):
        return options
    return dataclasses.replace(options, script_name=_DEFAULT_SCRIPT, api_key=_DEFAULT_SCRIPT)


def new_client_options(
    options: ConstructorOptions, credentials: Mapping[str, object]
) -> ConstructorOptions:
    """Replace credentials while retaining original noncredential constructor values.

    Args:
        options: Original bound constructor arguments.
        credentials: New credentials; a lone `sudo_as_login` keeps the original
            credentials and replaces only the impersonated login.

    Returns:
        Bound constructor arguments for the new client.

    Raises:
        TypeError: If a key is not a credential argument.
    """
    for name in credentials:
        if name not in _CREDENTIAL_FIELDS:
            raise TypeError(f"new_client() got an unexpected keyword argument '{name}'")
    values = dataclasses.asdict(options)
    if set(credentials) != {'sudo_as_login'}:
        values.update(dict.fromkeys(_CREDENTIAL_FIELDS))
    values.update(credentials)
    return ConstructorOptions(**values)
