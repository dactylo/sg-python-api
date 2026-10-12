"""Server and client capability containers."""

import copy
import ssl
import sys
from typing import Literal, TypeAlias, cast

from ._errors import ShotgunError

_Platform: TypeAlias = Literal['linux', 'mac', 'windows']

_DEV_VERSION_INDEX = 3
_SERVER_VERSION_FLOOR = (7, 10, 0)
_VERSION_PARTS = 3


class ClientCapabilities:
    """Represent local platform details used by compatibility helpers.

    Attributes:
        platform: Operating-system name.
        py_version: Python version.
        ssl_version: SSL library version.
        local_path_field: Platform-specific local-file field name.
    """

    def __init__(self) -> None:
        """Capture local platform capabilities for compatibility checks."""
        system = sys.platform
        if system == 'darwin':
            platform: _Platform | None = 'mac'
        elif system.startswith('linux'):
            platform = 'linux'
        elif system == 'win32':
            platform = 'windows'
        else:
            platform = None
        self.platform = platform
        self.local_path_field = f'local_path_{platform}' if platform is not None else None
        self.py_version = f'{sys.version_info.major}.{sys.version_info.minor}'
        self.ssl_version = ssl.OPENSSL_VERSION


class ServerCapabilities:
    """Represent parsed server version and capability metadata.

    Attributes:
        host: Server host name.
        version: Parsed server version.
        is_dev: Whether the server reports a development build.
        server_info: Detached raw server metadata.
    """

    def __init__(self, host: str, meta: dict[str, object]) -> None:
        """Parse one server's capability metadata.

        Args:
            host: Server host name represented by the metadata.
            meta: Server metadata containing a version sequence.

        Raises:
            ShotgunError: If server version metadata is missing, short,
                malformed, or below the supported floor.
        """
        owned_meta = copy.deepcopy(meta)
        self.host = host
        self._server_info = owned_meta
        raw = owned_meta.get('version')
        if isinstance(raw, list | tuple):
            sequence = cast('list[object] | tuple[object, ...]', raw)
            self.is_dev = (
                len(sequence) > _DEV_VERSION_INDEX and sequence[_DEV_VERSION_INDEX] == 'Dev'
            )
        else:
            self.is_dev = False
        self.version = _version(owned_meta)
        if self.version < _SERVER_VERSION_FLOOR:
            floor = '.'.join(str(part) for part in _SERVER_VERSION_FLOOR)
            current = '.'.join(str(part) for part in self.version)
            raise ShotgunError(
                f'This client requires server version {floor} or higher; server is {current}.'
            )

    def __str__(self) -> str:
        """Return a concise capability summary."""
        return (
            f'ServerCapabilities: host {self.host}, version {self.version}, is_dev {self.is_dev}'
        )

    def ensure_include_archived_projects(self) -> bool:
        """Report archived-project filtering support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    def ensure_paging_info_without_counts_support(self) -> bool:
        """Report count-free paging-info support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    def ensure_per_project_customization(self) -> bool:
        """Report per-project customization support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    def ensure_return_image_urls_support(self) -> bool:
        """Report image-URL return support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    def ensure_support_for_additional_filter_presets(self) -> bool:
        """Report additional-filter-preset support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    def ensure_user_following_support(self) -> bool:
        """Report user-following support.

        Returns:
            Always `True`; the server floor guarantees this feature.
        """
        return True

    @property
    def server_info(self) -> dict[str, object]:
        """Return detached raw server metadata.

        Returns:
            A deep copy of the metadata owned by this capability container.
        """
        return copy.deepcopy(self._server_info)


def _version(meta: dict[str, object]) -> tuple[int, int, int]:
    """Parse the supported three-part server version."""
    raw = meta.get('version')
    if not isinstance(raw, list | tuple):
        raise ShotgunError(
            'The Flow Production Tracking server did not report a version; '
            'it may be too old for this client.'
        )
    sequence = cast('list[object] | tuple[object, ...]', raw)
    if len(sequence) < _VERSION_PARTS:
        raise ShotgunError(
            'The Flow Production Tracking server did not report a version; '
            'it may be too old for this client.'
        )
    parts = sequence[:_VERSION_PARTS]
    if any(not isinstance(part, int) or isinstance(part, bool) for part in parts):
        raise ShotgunError('The Flow Production Tracking server reported an invalid version.')
    return cast('tuple[int, int, int]', tuple(parts))
