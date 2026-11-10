"""Retain issued upload allocations and bytes independently of record transactions."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit
from uuid import uuid4

from ._errors import MockgunError
from ._wire import format_wire_datetime

STORAGE_HOST = 'mockgun-storage.invalid'
_LETTERS = str.maketrans('0123456789abcdef', 'abcdefghijklmnop')


@dataclass(frozen=True)
class Allocation:
    """One issued allocation whose encoded response is the link-step credential.

    Attributes:
        upload_id: Site-local upload identity, independent of record IDs.
        filename: Client-supplied basename.
        upload_type: Attachment or thumbnail allocation kind.
        multipart: Whether the allocation requires part completion.
        key: Opaque storage path component.
        encoded: Exact allocation answer sent back by the link step.
    """

    upload_id: str
    filename: str
    upload_type: str
    multipart: bool
    key: str
    encoded: str


class Storage:
    """An in-memory byte store whose uploads survive failed record transactions.

    Only issued URLs accept writes. Attachment bindings identify completed
    files; reading a URL ignores its query token.
    """

    def __init__(self) -> None:
        """Create empty allocations, bytes, parts and Attachment bindings."""
        self._allocations: dict[str, Allocation] = {}
        self._encoded: dict[str, Allocation] = {}
        self._blobs: dict[str, bytes] = {}
        self._parts: dict[str, tuple[str, int]] = {}
        self._etags: dict[str, str] = {}
        self._etag_serial = 0
        self._bindings: dict[int, str] = {}

    def allocate(
        self, filename: str, upload_type: str, *, multipart: bool, now: datetime
    ) -> Allocation:
        """Issue an upload URL and retain its exact encoded answer.

        Args:
            filename: Basename supplied by the client.
            upload_type: Allocation kind sent by the client.
            multipart: Whether parts must be completed before linking.
            now: Aware request instant used for the opaque timestamp.

        Returns:
            The retained allocation, including its upload URL and exact encoded
            link-step credential.
        """
        key = _letters()
        upload_id = f'mockgun-upload-{len(self._allocations) + 1}'
        fields = [
            '1',
            f'https://{STORAGE_HOST}/{key}',
            format_wire_datetime(now),
            upload_type,
            upload_id,
        ]
        if multipart:
            fields.append('mockgun-multipart')
        allocation = Allocation(
            upload_id, filename, upload_type, multipart, key, '\n'.join(fields)
        )
        self._allocations[upload_id] = allocation
        self._encoded[allocation.encoded] = allocation
        return allocation

    def allocation(self, encoded: str) -> Allocation:
        """Resolve the exact answer returned by allocation.

        Raises:
            MockgunError: If the link form names an unissued allocation.
        """
        allocation = self._encoded.get(encoded)
        if allocation is None:
            raise MockgunError(
                'mockgun POST /upload/api_link_file upload_link_info: allocation was not issued'
            )
        return allocation

    def bind(self, attachment_id: int, key: str) -> None:
        """Associate an Attachment identity with its uploaded storage key."""
        self._bindings[attachment_id] = key

    def bound_key(self, attachment_id: int) -> str | None:
        """Return the Attachment's storage key, or `None` when unbound."""
        return self._bindings.get(attachment_id)

    def complete(self, upload_id: str, etags: Sequence[str]) -> int:
        """Join contiguous uploaded parts after checking their ordered ETags.

        Args:
            upload_id: Identity issued by a multipart allocation.
            etags: Quoted ETags in one-based part order.

        Returns:
            The number of completed parts.

        Raises:
            MockgunError: If the allocation, parts or ETags cannot be
                validated.
        """
        allocation = self._allocations.get(upload_id)
        parts = sorted(
            (number, path)
            for path, (identity, number) in self._parts.items()
            if identity == upload_id
        )
        if (
            allocation is None
            or not allocation.multipart
            or not parts
            or [number for number, _ in parts] != list(range(1, len(parts) + 1))
            or [self._etags.get(path) for _, path in parts] != list(etags)
            or any(path not in self._blobs for _, path in parts)
        ):
            raise MockgunError(
                f'mockgun POST /upload/api_complete_multipart_upload upload_id {upload_id}: '
                'parts or ETags are unsettled'
            )
        self._blobs['/' + allocation.key] = b''.join(self._blobs[path] for _, path in parts)
        return len(parts)

    def has_bytes(self, key: str) -> bool:
        """Return whether a completed file exists under the allocation key."""
        return '/' + key in self._blobs

    def image_url(self, key: str) -> str:
        """Return the key's URL with a fresh opaque query token."""
        return f'https://{STORAGE_HOST}/{key}?{_letters()[:16]}'

    def part_url(self, upload_id: str, part_number: int) -> str:
        """Issue a storage URL for a positive, one-based multipart part.

        Raises:
            MockgunError: If the upload is not multipart or the part is
                invalid.
        """
        allocation = self._allocations.get(upload_id)
        if allocation is None or not allocation.multipart or part_number < 1:
            raise MockgunError(
                f'mockgun POST /upload/api_get_upload_link_for_part upload_id {upload_id}: '
                f'part_number {part_number} was not issued'
            )
        for path, identity in self._parts.items():
            if identity == (upload_id, part_number):
                return f'https://{STORAGE_HOST}{path}'
        path = f'/{allocation.key}/part-{_letters()}'
        self._parts[path] = (upload_id, part_number)
        return f'https://{STORAGE_HOST}{path}'

    def put(self, url: str, data: bytes) -> str | None:
        """Store bytes at an issued URL, returning a quoted ETag for a part.

        Raises:
            MockgunError: If the URL was not issued for storage writes.
        """
        parsed = urlsplit(url)
        path = parsed.path
        direct = any(
            '/' + item.key == path and not item.multipart for item in self._allocations.values()
        )
        if parsed.hostname != STORAGE_HOST or (path not in self._parts and not direct):
            raise MockgunError(f'mockgun does not emulate PUT {path}: storage URL was not issued')
        self._blobs[path] = data
        if path not in self._parts:
            return None
        self._etag_serial += 1
        etag = f'"mockgun-etag-{self._etag_serial}"'
        self._etags[path] = etag
        return etag

    def read(self, url: str) -> bytes:
        """Return uploaded bytes, ignoring a storage URL's query token.

        Raises:
            MockgunError: If the URL has no stored bytes.
        """
        parsed = urlsplit(url)
        if parsed.hostname != STORAGE_HOST or parsed.path not in self._blobs:
            raise MockgunError(
                f'mockgun does not emulate GET {parsed.path}: storage bytes are unknown'
            )
        return self._blobs[parsed.path]


def _letters() -> str:
    """Return an opaque path component containing no decimal digits."""
    return uuid4().hex.translate(_LETTERS)
