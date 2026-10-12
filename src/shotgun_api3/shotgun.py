"""Expose the synchronous `shotgun_api3.shotgun` compatibility surface.

Import this module for the synchronous client and public compatibility names.
Import initializes the `shotgun_api3` logger and reads
`SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION`; the environment value `1` disables
entity-link optimization.

Assigning the module's `SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION` flag later
changes the setting used by subsequent request construction.
"""

import logging
import os

from ._capabilities import ClientCapabilities, ServerCapabilities
from ._client import Shotgun, __version__
from ._errors import (
    AuthenticationFault,
    Error,
    Fault,
    MissingTwoFactorAuthenticationFault,
    ProtocolError,
    ResponseError,
    ShotgunError,
    ShotgunFileDownloadError,
    ShotgunThumbnailNotReady,
    UserCredentialsNotAllowedForOxygenAuthenticationFault,
    UserCredentialsNotAllowedForSSOAuthenticationFault,
)
from ._sg_timezone import SG_TIMEZONE
from .lib.sgtimezone import SgTimezone

LOG = logging.getLogger('shotgun_api3')
SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION = (
    os.environ.get('SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION', '0') == '1'
)

__all__ = [
    'LOG',
    'SG_TIMEZONE',
    'SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION',
    'AuthenticationFault',
    'ClientCapabilities',
    'Error',
    'Fault',
    'MissingTwoFactorAuthenticationFault',
    'ProtocolError',
    'ResponseError',
    'ServerCapabilities',
    'SgTimezone',
    'Shotgun',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForOxygenAuthenticationFault',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
    '__version__',
]
