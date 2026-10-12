"""Exception hierarchy for the `shotgun_api3` compatibility surface."""

from xmlrpc.client import Error, ProtocolError, ResponseError

__all__ = [
    'AuthenticationFault',
    'Error',
    'Fault',
    'MissingTwoFactorAuthenticationFault',
    'ProtocolError',
    'ResponseError',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForOxygenAuthenticationFault',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
]


class ShotgunError(Exception):
    """Base exception for errors in the `ShotgunError` family."""


class Fault(ShotgunError):  # noqa: N818
    """Raised when the server reports an exception in an RPC response."""


class AuthenticationFault(Fault):
    """Raised when the server rejects the provided credentials."""


class MissingTwoFactorAuthenticationFault(Fault):
    """Raised when two-factor authentication is required but missing."""


class ShotgunFileDownloadError(ShotgunError):
    """Raised when downloading an attachment fails."""


class ShotgunThumbnailNotReady(ShotgunError):  # noqa: N818
    """Raised when a requested thumbnail has not finished processing."""


class UserCredentialsNotAllowedForOxygenAuthenticationFault(Fault):
    """Raised when username/password auth is used against an Autodesk Identity site."""


class UserCredentialsNotAllowedForSSOAuthenticationFault(Fault):
    """Raised when username/password auth is used against an SSO-enabled site."""
