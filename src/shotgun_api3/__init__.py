"""Provide compatible synchronous and optional asynchronous Shotgun clients.

Import `Shotgun` directly from this package. Import `AsyncShotgun` explicitly
after installing the `async` extra; it is loaded lazily and excluded from star
imports.

Close synchronous clients with `close()`. Use asynchronous clients through an
async context manager or call `await close()`. The public `sg_timezone` object
is the shared timezone configuration used for datetime conversion.
"""

import importlib
from typing import TYPE_CHECKING

from .shotgun import SG_TIMEZONE as _SG_TIMEZONE
from .shotgun import (
    AuthenticationFault,
    Error,
    Fault,
    MissingTwoFactorAuthenticationFault,
    ProtocolError,
    ResponseError,
    Shotgun,
    ShotgunError,
    ShotgunFileDownloadError,
    ShotgunThumbnailNotReady,
    UserCredentialsNotAllowedForSSOAuthenticationFault,
    __version__,
)

if TYPE_CHECKING:
    from ._async_client import AsyncShotgun as AsyncShotgun

sg_timezone = _SG_TIMEZONE

__all__ = [
    'AuthenticationFault',
    'Error',
    'Fault',
    'MissingTwoFactorAuthenticationFault',
    'ProtocolError',
    'ResponseError',
    'Shotgun',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
    '__version__',
    'sg_timezone',
]


def __getattr__(name: str) -> object:
    """Resolve optional package exports without loading them on base import."""
    if name != 'AsyncShotgun':
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    try:
        importlib.import_module('httpx2')
    except ModuleNotFoundError as exc:
        if exc.name != 'httpx2':
            raise
        raise ImportError(
            "AsyncShotgun requires httpx2 from the 'async' extra: install sg-python-api[async]"
        ) from exc
    module = importlib.import_module(f'{__name__}._async_client')
    exported = getattr(module, name)
    globals()[name] = exported
    return exported
