"""Pure file-transfer routing helpers and observed endpoint paths."""

import copy
import dataclasses
import datetime
import http.cookiejar
import os
import tempfile
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Literal, TypeAlias, TypedDict, cast

from . import _protocol
from ._config import RequestContext
from ._errors import ShotgunError, ShotgunFileDownloadError
from ._transport import TransportRequest

COMPLETE_MULTIPART = '/upload/api_complete_multipart_upload'
GET_PART_LINK = '/upload/api_get_upload_link_for_part'
GET_UPLOAD_LINK_INFO = '/upload/api_get_upload_link_info'
LINK_FILE = '/upload/api_link_file'
SHARE_THUMBNAIL = '/upload/share_thumbnail'
SHARE_THUMBNAIL_OPERATION = _protocol.OperationDescriptor(
    category='upload',
    replay_safe=False,
    body_restartable=True,
    endpoint_role=None,
)
_FILE_SERVE_ATTACHMENT = '/file_serve/attachment/'
_MULTIPART_CHUNK_SIZE = 20_000_000
_Origin: TypeAlias = tuple[str, str, int]
_PART_LINK_FIELD_COUNT = 2
_THUMBNAIL_UPLOAD_FIELDS = ('image', 'filmstrip_image')


@dataclasses.dataclass(frozen=True)
class StorageRange:
    """Describe one restartable file range for a storage PUT.

    Attributes:
        length: Number of bytes in the range.
        offset: Zero-based file offset.
        path: Source file path.
    """

    length: int
    offset: int
    path: str


@dataclasses.dataclass(frozen=True)
class _ThumbnailShare:
    """Carry validated detached thumbnail-sharing inputs."""

    entities: list[Mapping[str, object]]
    filmstrip_thumbnail: bool
    source_entity: Mapping[str, object]
    thumbnail_path: str | None
    source_entity_type: str
    source_entity_id: int


class ThumbnailUploadKeywordArguments(TypedDict, total=False):
    """Describe optional upload arguments accepted by thumbnail wrappers.

    Attributes:
        display_name: Optional Attachment display name.
        tag_list: Optional comma-delimited tags.
        created_at: Optional Attachment creation timestamp.
    """

    display_name: str | None
    tag_list: str | None
    created_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class UploadArguments:
    """Collect the exact public upload arguments.

    Attributes:
        entity_type: Entity type receiving the upload.
        entity_id: Numeric identifier of the receiving entity.
        path: Source file path; byte values are decoded during preparation.
        field_name: Optional file field.
        display_name: Optional Attachment display name.
        tag_list: Optional comma-delimited tags.
        created_at: Optional Attachment creation timestamp.
    """

    entity_type: str
    entity_id: int
    path: str
    field_name: str | None = None
    display_name: str | None = None
    tag_list: str | None = None
    created_at: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True)
class UploadExchange:
    """Describe one upload exchange and its fresh body factory.

    Attributes:
        body_factory: Build a fresh request body for every attempt.
        headers: Request headers copied at the send boundary.
        method: HTTP request method.
        operation: Replay and route classification.
        url: Complete request URL.
    """

    body_factory: Callable[[], bytes | Iterator[bytes] | None]
    headers: dict[str, str]
    method: str
    operation: _protocol.OperationDescriptor
    url: str


@dataclasses.dataclass(frozen=True, init=False)
class UploadLinkInfo:
    """Preserve and expose one observed upload-allocation response.

    Attributes:
        encoded: Original space-delimited allocation response.
        timestamp: Parsed allocation timestamp.
        upload_id: Upload allocation identifier.
        upload_type: Upload allocation type.
        upload_url: Upload destination URL.
    """

    encoded: str
    timestamp: str
    upload_id: str
    upload_type: str
    upload_url: str

    def __init__(self, encoded: str, multipart: bool) -> None:
        """Parse one upload-allocation response.

        Args:
            encoded: Space-delimited allocation response from the server.
            multipart: Whether the allocation is for a multipart upload.

        Raises:
            ShotgunError: If the allocation response is unsuccessful or
                malformed.
        """
        parts = encoded.split()
        expected_count = 6 if multipart else 5
        try:
            success = int(parts[0])
        except (IndexError, ValueError):
            success = 0
        if len(parts) != expected_count or success != 1:
            raise ShotgunError('upload allocation returned an invalid response')
        object.__setattr__(self, 'encoded', encoded)
        object.__setattr__(self, 'upload_url', parts[1])
        object.__setattr__(self, 'timestamp', parts[2])
        object.__setattr__(self, 'upload_type', parts[3])
        object.__setattr__(self, 'upload_id', parts[4])


@dataclasses.dataclass(frozen=True)
class UploadOptions:
    """Collect one upload call and its thumbnail-routing state.

    Attributes:
        arguments: Bound public upload arguments.
        upload_type: Attachment or thumbnail endpoint type.
        filmstrip: Whether the upload is a filmstrip thumbnail.
    """

    arguments: UploadArguments
    upload_type: Literal['Attachment', 'Thumbnail']
    filmstrip: bool


@dataclasses.dataclass(frozen=True)
class PreparedUpload:
    """Carry validated file metadata with one upload call.

    Attributes:
        filename: File basename used by upload endpoints.
        file_size: Positive file size captured before network I/O.
        options: Immutable public-call and thumbnail-routing options.
    """

    filename: str
    file_size: int
    options: 'UploadOptions'


class _ExactOriginCookiePolicy(http.cookiejar.DefaultCookiePolicy):
    """Restrict session-cookie returns to one canonical HTTP origin."""

    def __init__(self, origin: _Origin) -> None:
        """Initialize a cookie policy for one exact origin."""
        super().__init__()
        self._origin = origin

    def domain_return_ok(
        self,
        domain: str,
        request: urllib.request.Request,
    ) -> bool:
        """Return whether a cookie domain may serve the request origin.

        Args:
            domain: Candidate cookie-jar domain.
            request: Stdlib request receiving cookie headers.

        Returns:
            Whether exact-origin matching and configured domain lists allow the
            domain.
        """
        return (
            domain == self._origin[1]
            and needs_session_cookie(request.get_full_url(), self._origin)
            and not self.is_blocked(domain)
            and not self.is_not_allowed(domain)
        )

    def return_ok_domain(
        self,
        cookie: http.cookiejar.Cookie,
        request: urllib.request.Request,
    ) -> bool:
        """Return whether a cookie belongs to the exact request origin.

        Args:
            cookie: Candidate cookie from the jar.
            request: Stdlib request receiving cookie headers.

        Returns:
            Whether the canonical cookie domain and request origin match the
            captured origin.
        """
        return cookie.domain == self._origin[1] and needs_session_cookie(
            request.get_full_url(),
            self._origin,
        )


def _entity_reference(entity: Mapping[str, object]) -> str:
    """Serialize one entity as the upload endpoint reference grammar."""
    entity_type = entity.get('type')
    entity_id = entity.get('id')
    if not isinstance(entity_type, str) or not entity_type:
        raise ShotgunError('thumbnail entities must contain a nonempty string type')
    if not isinstance(entity_id, int) or isinstance(entity_id, bool):
        raise ShotgunError('thumbnail entities must contain an integer id')
    return f'{entity_type}_{entity_id}'


def _iter_file(
    path: str,
    chunk_size: int = 65536,
    *,
    offset: int = 0,
    length: int | None = None,
) -> Generator[bytes, None, None]:
    """Stream a bounded file range while owning its handle."""
    if chunk_size <= 0:
        raise ValueError('file chunk size must be positive')
    if offset < 0:
        raise ValueError('file offset must be nonnegative')
    if length is not None and length < 0:
        raise ValueError('file length must be nonnegative')
    remaining = length
    with Path(path).open('rb') as handle:
        handle.seek(offset)
        while remaining is None or remaining > 0:
            read_size = chunk_size if remaining is None else min(chunk_size, remaining)
            chunk = handle.read(read_size)
            if not chunk:
                break
            yield chunk
            if remaining is not None:
                remaining -= len(chunk)


def _requires_direct_upload(
    server_info: Mapping[str, object],
    entity_type: str,
    field_name: str | None,
) -> bool:
    """Return whether server capabilities route a field to direct storage."""
    if not server_info.get('s3_direct_uploads_enabled'):
        return False

    raw_allowed_types = server_info.get('s3_enabled_upload_types')
    if isinstance(raw_allowed_types, Mapping):
        allowed_types = cast('Mapping[object, object]', raw_allowed_types)
        for key in (entity_type, '*'):
            allowed_fields = allowed_types.get(key)
            if allowed_fields == '*':
                return True
            if isinstance(allowed_fields, list) and (
                '*' in allowed_fields or field_name in allowed_fields
            ):
                return True

    return entity_type == 'Version' and field_name == 'sg_uploaded_movie'


def add_auth_fields(
    fields: Mapping[str, str],
    auth: Mapping[str, object],
) -> dict[str, str]:
    """Copy form fields and append captured authentication values.

    Args:
        fields: Endpoint-specific form fields.
        auth: Immutable authentication snapshot.

    Returns:
        Independent form fields with non-`None` auth values appended.
    """
    result = dict(fields)
    result.update({name: str(value) for name, value in auth.items() if value is not None})
    return result


def build_auth_cookie_handler(
    token: str,
    origin: _Origin,
) -> urllib.request.HTTPCookieProcessor:
    """Build a stdlib cookie handler restricted to one captured origin.

    Args:
        token: Captured session token.
        origin: Canonical scheme, hostname, and effective port.

    Returns:
        Handler containing one host-only `_session_id` cookie.
    """
    scheme, hostname, _port = origin
    cookie = http.cookiejar.Cookie(
        version=0,
        name='_session_id',
        value=token,
        port=None,
        port_specified=False,
        domain=hostname,
        domain_specified=False,
        domain_initial_dot=False,
        path='/',
        path_specified=True,
        secure=scheme == 'https',
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={},
        rfc2109=False,
    )
    jar = http.cookiejar.LWPCookieJar(policy=_ExactOriginCookiePolicy(origin))
    jar.set_cookie(cookie)
    return urllib.request.HTTPCookieProcessor(jar)


def build_complete_fields(
    upload_info: UploadLinkInfo,
    filename: str,
    etags: Sequence[str],
) -> dict[str, str]:
    """Build the observed multipart-finalization form fields.

    Args:
        upload_info: Validated multipart allocation response.
        filename: Uploaded file basename.
        etags: Storage ETags in part order.

    Returns:
        Independent multipart-finalization fields.
    """
    return {
        'etags': ','.join(etags),
        'filename': filename,
        'timestamp': upload_info.timestamp,
        'upload_id': upload_info.upload_id,
        'upload_type': upload_info.upload_type,
    }


def build_download_request(
    context: RequestContext,
    request_url: str,
    request_headers: Mapping[str, str],
) -> TransportRequest:
    """Build a fresh GET request using captured timeout and detached headers."""
    return TransportRequest(
        method='GET',
        url=request_url,
        headers=dict(request_headers),
        body=None,
        timeout=context.timeout,
        token=uuid.uuid4().hex,
    )


def build_link_entity_fields(prepared: PreparedUpload) -> dict[str, str]:
    """Build entity metadata for an upload-link completion.

    Args:
        prepared: Validated upload metadata and arguments.

    Returns:
        Independent entity fields excluding opaque allocation information.
    """
    options = prepared.options
    arguments = options.arguments
    fields = {
        'entity_type': arguments.entity_type,
        'entity_id': str(arguments.entity_id),
    }
    if options.upload_type == 'Attachment':
        if arguments.field_name is not None:
            fields['field_name'] = arguments.field_name
        fields['display_name'] = arguments.display_name or prepared.filename
    elif arguments.display_name is not None:
        fields['display_name'] = arguments.display_name
    if arguments.tag_list is not None:
        fields['tag_list'] = arguments.tag_list
    if arguments.created_at is not None:
        fields['created_at'] = str(arguments.created_at)
    if options.filmstrip:
        fields['filmstrip'] = 'true'
    return fields


def build_link_file_fields(
    entity_fields: Mapping[str, str],
    encoded_upload_link_info: str,
) -> dict[str, str]:
    """Build link-completion fields without expanding allocation data.

    Args:
        entity_fields: Entity, display, and optional metadata fields.
        encoded_upload_link_info: Original allocation response text.

    Returns:
        A copy with the single opaque `upload_link_info` field appended.
    """
    return {**entity_fields, 'upload_link_info': encoded_upload_link_info}


def build_part_fields(
    upload_info: UploadLinkInfo,
    filename: str,
    part_number: int,
) -> dict[str, str]:
    """Build the observed multipart part-link form fields.

    Args:
        upload_info: Validated multipart allocation response.
        filename: Uploaded file basename.
        part_number: One-based part number.

    Returns:
        Independent part-link fields.
    """
    return {
        'filename': filename,
        'part_number': str(part_number),
        'timestamp': upload_info.timestamp,
        'upload_id': upload_info.upload_id,
        'upload_type': upload_info.upload_type,
    }


def build_share_thumbnail_fields(
    entities: Sequence[Mapping[str, object]],
    source_entity: Mapping[str, object],
    filmstrip_thumbnail: bool,
) -> dict[str, str]:
    """Build the current source-sharing form fields.

    Args:
        entities: Target entities receiving the source thumbnail.
        source_entity: Entity whose thumbnail is shared.
        filmstrip_thumbnail: Whether to share the filmstrip thumbnail.

    Returns:
        Exact endpoint fields without authentication values.

    Raises:
        ShotgunError: If an entity lacks a valid type or integer ID.
    """
    return {
        'entities': ','.join(_entity_reference(entity) for entity in entities),
        'filmstrip_thumbnail': '1' if filmstrip_thumbnail else 'False',
        'source_entity': _entity_reference(source_entity),
    }


def build_upload_link_fields(
    filename: str,
    upload_type: Literal['Attachment', 'Thumbnail'],
    multipart: bool,
    created_at: datetime.datetime | None = None,
) -> dict[str, str]:
    """Build the observed allocation fields before authentication.

    Args:
        filename: Uploaded file basename.
        upload_type: Attachment or thumbnail allocation type.
        multipart: Whether the allocation is multipart.
        created_at: Optional caller-supplied creation timestamp.

    Returns:
        Independent upload-allocation fields.
    """
    fields = {
        'filename': filename,
        'upload_type': upload_type,
        'multipart_upload': str(multipart).lower(),
    }
    if created_at is not None:
        fields['created_at'] = created_at.isoformat()
    return fields


def build_storage_upload_exchange(
    user_agent: str,
    operation: _protocol.OperationDescriptor,
    url: str,
    storage_range: StorageRange,
    chunk_size: int = 65536,
) -> UploadExchange:
    """Plan one restartable streamed storage PUT.

    Args:
        user_agent: Captured user-agent header value.
        operation: Replay and route classification.
        url: Signed storage destination URL.
        storage_range: File range to stream for every attempt.
        chunk_size: Maximum file bytes read per iteration.

    Returns:
        Complete restartable storage exchange.
    """
    return UploadExchange(
        body_factory=lambda: _iter_file(
            storage_range.path,
            chunk_size,
            offset=storage_range.offset,
            length=storage_range.length,
        ),
        headers={
            'content-length': str(storage_range.length),
            'content-type': 'application/octet-stream',
            'user-agent': user_agent,
        },
        method='PUT',
        operation=operation,
        url=url,
    )


def canonical_origin(url: str) -> _Origin:
    """Return the canonical HTTP origin for a URL.

    Args:
        url: Absolute HTTP or HTTPS URL without user information.

    Returns:
        Lowercase scheme, IDNA hostname, and effective port.

    Raises:
        ShotgunError: If the URL does not contain a safe HTTP origin.
    """
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ShotgunError('URL must contain a valid HTTP origin') from exc
    scheme = parsed.scheme.lower()
    if (
        scheme not in {'http', 'https'}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ShotgunError('URL must contain a valid HTTP origin')
    try:
        hostname = parsed.hostname.encode('idna').decode('ascii').lower()
    except UnicodeError as exc:
        raise ShotgunError('URL must contain a valid HTTP origin') from exc
    return scheme, hostname, port if port is not None else (443 if scheme == 'https' else 80)


def _build_urlencoded_upload_exchange(
    canonical_origin: str,
    user_agent: str,
    operation: _protocol.OperationDescriptor,
    route: str,
    fields: Mapping[str, str],
) -> UploadExchange:
    """Plan one restartable URL-encoded upload-endpoint exchange."""
    body = urllib.parse.urlencode(dict(fields)).encode()
    return UploadExchange(
        body_factory=lambda: body,
        headers={
            'content-length': str(len(body)),
            'content-type': 'application/x-www-form-urlencoded',
            'user-agent': user_agent,
        },
        method='POST',
        operation=operation,
        url=f'{canonical_origin}{route}',
    )


def build_download_redirect(
    context: RequestContext,
    response_headers: Mapping[str, str],
) -> TransportRequest:
    """Validate a redirect location and build a credential-free download hop.

    Only the captured user-agent is forwarded to the redirect destination.
    The `location` key is looked up in lowercase or in a case-insensitive
    response header mapping.

    Raises:
        ShotgunFileDownloadError: If the response has no valid redirect
            location.
    """
    location = response_headers.get('location')
    if location is None:
        raise ShotgunFileDownloadError('attachment download failed')
    try:
        canonical_origin(location)
    except ShotgunError:
        raise ShotgunFileDownloadError('attachment download failed') from None
    return build_download_request(context, location, {'user-agent': context.user_agent})


def build_upload_endpoint_exchange(
    canonical_origin: str | None,
    user_agent: str,
    operation: _protocol.OperationDescriptor,
    route: str,
    fields: Mapping[str, str],
) -> UploadExchange:
    """Plan one configured URL-encoded upload-endpoint exchange.

    Args:
        canonical_origin: Captured site origin, or `None` when unconfigured.
        user_agent: Captured user-agent header value.
        operation: Replay and route classification.
        route: Upload endpoint path.
        fields: Complete request form fields.

    Returns:
        Complete restartable upload-endpoint exchange.

    Raises:
        ValueError: If server parameters are not configured.
    """
    if canonical_origin is None:
        raise ValueError('server parameters must be configured before upload I/O')
    return _build_urlencoded_upload_exchange(
        canonical_origin,
        user_agent,
        operation,
        route,
        fields,
    )


def choose_upload_path(
    server_info: Mapping[str, object],
    entity_type: str,
    field_name: str | None,
    file_size: int,
) -> Literal['form', 'direct', 'multipart']:
    """Choose the observed upload path for a file.

    Args:
        server_info: Immutable server capability data.
        entity_type: Entity type receiving the file.
        field_name: Target field, if the upload specifies one.
        file_size: File size in bytes.

    Returns:
        `form`, `direct`, or `multipart` for the selected upload path.
    """
    if not _requires_direct_upload(server_info, entity_type, field_name):
        return 'form'
    if file_size <= _MULTIPART_CHUNK_SIZE:
        return 'direct'
    return 'multipart'


@contextmanager
def download_temporary(destination: Path) -> Generator[BinaryIO]:
    """Yield an adjacent named binary temporary file without deleting it on close."""
    with tempfile.NamedTemporaryFile(
        dir=destination.parent,
        prefix=f'.{destination.name}.',
        suffix='.tmp',
        delete=False,
    ) as handle:
        yield cast('BinaryIO', handle)


def multipart_ranges(prepared: PreparedUpload) -> tuple[StorageRange, ...]:
    """Plan ordered storage ranges for one multipart upload.

    Args:
        prepared: Validated upload metadata and arguments.

    Returns:
        Ordered contiguous ranges covering the complete file.
    """
    path = prepared.options.arguments.path
    return tuple(
        StorageRange(
            length=min(_MULTIPART_CHUNK_SIZE, prepared.file_size - offset),
            offset=offset,
            path=path,
        )
        for offset in range(0, prepared.file_size, _MULTIPART_CHUNK_SIZE)
    )


def needs_session_cookie(url: str, origin: _Origin) -> bool:
    """Return whether a URL exactly matches the captured session origin.

    Args:
        url: Candidate attachment URL.
        origin: Captured configured origin.

    Returns:
        Whether scheme, normalized hostname, and effective port all match.
    """
    try:
        candidate = canonical_origin(url)
    except ShotgunError:
        return False
    return candidate == origin


def parse_link_completion(encoded: str) -> int:
    """Parse an Attachment ID from the observed link success envelope.

    Args:
        encoded: Link-completion response text.

    Returns:
        Created Attachment ID.

    Raises:
        ShotgunError: If the response does not match the observed grammar.
    """
    marker, separator, payload = encoded.partition(':')
    parts = payload.split()
    if marker != '1' or separator != ':' or not parts:
        raise ShotgunError('upload link returned an invalid response')
    try:
        return int(parts[0])
    except ValueError:
        raise ShotgunError('upload link returned an invalid response') from None


def parse_multipart_completion(encoded: str) -> int:
    """Parse the observed integer-like multipart completion payload.

    Args:
        encoded: Multipart-completion response text.

    Returns:
        Parsed integer payload.

    Raises:
        ShotgunError: If the response is not integer-like.
    """
    try:
        return int(encoded.strip())
    except ValueError:
        raise ShotgunError('multipart completion returned an invalid response') from None


def parse_part_etag(_text: str, headers: Mapping[str, str]) -> str:
    """Return the required ETag from one multipart part response.

    Args:
        _text: Unused response body retained for upload-parser compatibility.
        headers: Response headers from the storage PUT.

    Returns:
        Multipart part ETag.

    Raises:
        ShotgunError: If the response omits its ETag.
    """
    etag = headers.get('Etag')
    if etag is None:
        raise ShotgunError('multipart part response did not include an ETag')
    return etag


def parse_part_link(encoded: str) -> str:
    """Parse a signed URL from the observed part-link response.

    Args:
        encoded: Part-link response text.

    Returns:
        Signed storage URL.

    Raises:
        ShotgunError: If the response does not match the observed grammar.
    """
    parts = encoded.split()
    try:
        success = int(parts[0])
    except (IndexError, ValueError):
        success = 0
    if len(parts) != _PART_LINK_FIELD_COUNT or success != 1:
        raise ShotgunError('multipart part link returned an invalid response')
    return parts[1]


def prepare_thumbnail_share(
    entities: object,
    thumbnail_path: str | None,
    source_entity: object,
    filmstrip_thumbnail: bool,
) -> _ThumbnailShare:
    """Validate and detach one thumbnail-sharing request.

    Supply exactly one of `thumbnail_path` and `source_entity`.

    Args:
        entities: Nonempty list of target entity mappings with a nonempty
            string `type` and an integer, non-boolean `id`.
        thumbnail_path: Optional local image path.
        source_entity: Optional source entity mapping with the same identity
            requirements as a target.
        filmstrip_thumbnail: Whether the filmstrip image is shared.

    Returns:
        Detached validated sharing inputs. For a local path, the first target
        is selected as the source entity.

    Raises:
        ShotgunError: If source selection or entity values are invalid.
    """
    if (thumbnail_path is None) == (source_entity is None):
        raise ShotgunError('exactly one of thumbnail_path and source_entity is required')
    if not isinstance(entities, list) or not entities:
        raise ShotgunError('entities must be a nonempty list of entity dictionaries')
    owned_entities = copy.deepcopy(cast('list[object]', entities))
    if any(not isinstance(entity, Mapping) for entity in owned_entities):
        raise ShotgunError('entities must be a nonempty list of entity dictionaries')
    typed_entities = cast('list[Mapping[str, object]]', owned_entities)
    if source_entity is None:
        owned_source = typed_entities[0]
    else:
        if not isinstance(source_entity, Mapping):
            raise ShotgunError('source_entity must be an entity dictionary')
        typed_source = cast('Mapping[str, object]', source_entity)
        owned_source = copy.deepcopy(typed_source)
    build_share_thumbnail_fields(
        typed_entities,
        owned_source,
        filmstrip_thumbnail,
    )
    return _ThumbnailShare(
        entities=typed_entities,
        filmstrip_thumbnail=filmstrip_thumbnail,
        source_entity=owned_source,
        thumbnail_path=thumbnail_path,
        source_entity_type=cast('str', owned_source['type']),
        source_entity_id=cast('int', owned_source['id']),
    )


def prepare_upload(options: UploadOptions) -> PreparedUpload:
    """Validate an upload path and capture its file metadata.

    Args:
        options: Bound upload and thumbnail-routing options.

    Returns:
        Captured filename, size, and options for one upload.

    Raises:
        ShotgunError: If the path does not name a file, the file is empty, or
            `created_at` is not a datetime.
    """
    created_at = cast('object', options.arguments.created_at)
    if created_at is not None and not isinstance(created_at, datetime.datetime):
        raise ShotgunError('created_at must be a datetime.datetime instance')
    raw_path = options.arguments.path
    if isinstance(raw_path, bytes):
        options = dataclasses.replace(
            options,
            arguments=dataclasses.replace(options.arguments, path=os.fsdecode(raw_path)),
        )
    path = Path(options.arguments.path)
    try:
        is_file = path.is_file()
        file_size = path.stat().st_size if is_file else 0
    except OSError:
        is_file, file_size = False, 0
    if not is_file:
        raise ShotgunError(f"Path must be a valid file, got '{options.arguments.path}'")
    if file_size <= 0:
        raise ShotgunError(f"Path cannot be an empty file: '{options.arguments.path}'")
    return PreparedUpload(filename=path.name, file_size=file_size, options=options)


def require_supported_upload_path(
    upload_path: Literal['form', 'direct', 'multipart'],
) -> Literal['direct', 'multipart']:
    """Reject the unsupported legacy form route before upload I/O.

    Args:
        upload_path: Route selected from server capabilities.

    Returns:
        The selected supported route.

    Raises:
        ShotgunError: If the server requires legacy form uploads.
    """
    if upload_path == 'form':
        raise ShotgunError(
            'server requires legacy form uploads, which this client does not support'
        )
    return upload_path


def resolve_attachment_url(attachment: object, base_url: str) -> str | None:
    """Resolve a supported attachment reference to its download URL.

    Args:
        attachment: Attachment ID, Attachment dictionary, URL dictionary, or
            `None`.
        base_url: Configured site base URL.

    Returns:
        The supplied URL, a site attachment URL, or `None`.

    Raises:
        TypeError: If `attachment` is a string.
        ValueError: If an entity dictionary references a different type.
        ShotgunError: If another `attachment` value is unsupported.
    """
    if attachment is None:
        return None
    if isinstance(attachment, str):
        raise TypeError('attachment must be an id or dictionary')
    if isinstance(attachment, Mapping):
        mapping = cast('Mapping[object, object]', attachment)
        url = mapping.get('url')
        if isinstance(url, str):
            return url
        attachment_id = mapping.get('id')
        if type(attachment_id) is int:
            if mapping.get('type') != 'Attachment':
                raise ValueError('attachment dictionary must reference an Attachment')
            return f'{base_url.rstrip("/")}{_FILE_SERVE_ATTACHMENT}{attachment_id}'
    elif type(attachment) is int:
        return f'{base_url.rstrip("/")}{_FILE_SERVE_ATTACHMENT}{attachment}'
    raise ShotgunError('attachment must be an id or dictionary')


def split_thumbnail_uploads(
    data: object,
    *,
    keep_image_none: bool,
) -> tuple[object, dict[str, object]]:
    """Separate `image` and `filmstrip_image` paths from entity field data.

    Autodesk's client uploads these paths after the entity write instead of
    sending them as field values.

    Args:
        data: Public field mapping. A non-dictionary is returned unchanged so
            that the caller's own validation reports it.
        keep_image_none: Whether an `image` value of `None` stays in the field
            data, which lets an update clear both thumbnails.

    Returns:
        A copy of the field data without the separated fields, and the
        non-empty paths to upload, with `image` before `filmstrip_image`.
    """
    if not isinstance(data, dict):
        return data, {}
    mapping = cast('dict[object, object]', data)
    fields: dict[object, object] = {}
    paths: dict[str, object] = {}
    for name, value in mapping.items():
        if name not in _THUMBNAIL_UPLOAD_FIELDS or (
            keep_image_none and name == 'image' and value is None
        ):
            fields[name] = value
        elif value:
            paths[cast('str', name)] = value
    return fields, {name: paths[name] for name in _THUMBNAIL_UPLOAD_FIELDS if name in paths}


def thumbnail_source_ready(
    result: object,
    field_name: Literal['filmstrip_image', 'image'],
) -> bool:
    """Validate and classify one thumbnail-readiness result.

    Args:
        result: Transformed `read` RPC result.
        field_name: Thumbnail field selected for readiness polling.

    Returns:
        Whether the first thumbnail is present and no longer transient.

    Raises:
        ShotgunError: If the result has an invalid response shape.
    """
    if not isinstance(result, dict):
        raise ShotgunError('thumbnail readiness read did not return a dictionary')
    raw_entities = cast('dict[str, object]', result).get('entities')
    if not isinstance(raw_entities, list):
        raise ShotgunError('thumbnail readiness read did not return an entities list')
    if not raw_entities:
        return False
    first = cast('list[object]', raw_entities)[0]
    if not isinstance(first, dict):
        raise ShotgunError('thumbnail readiness read returned a non-dictionary entity')
    value = cast('dict[str, object]', first).get(field_name)
    return isinstance(value, str) and '/images/status/transient/' not in value


def validate_download_url(url: str) -> None:
    """Reject unusable attachment URLs before download I/O.

    Args:
        url: Resolved attachment URL to validate.

    Raises:
        ShotgunFileDownloadError: If the URL has no safe HTTP origin.
    """
    try:
        canonical_origin(url)
    except ShotgunError:
        raise ShotgunFileDownloadError('attachment download failed') from None
