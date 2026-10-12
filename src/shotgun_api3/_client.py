"""Shotgun compatibility facade and client lifecycle."""

import copy
import dataclasses
import datetime as _dt_module
import inspect
import os
import threading
import urllib.request
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping
from contextlib import ExitStack, contextmanager, suppress
from pathlib import Path
from typing import (
    Any,
    Literal,
    Never,
    TypeAlias,
    TypeVar,
    Unpack,
    cast,
    overload,
)

from . import _config as config_module
from . import _crud, _errors, _files, _methods, _protocol, _transport
from ._capabilities import ClientCapabilities, ServerCapabilities
from ._config import (
    CompatibilityWarningCode,
    Config,
    RequestContext,
    advance_transport_generation,
    constructor_http_proxy_fields,
    readiness_key,
    require_secure_rpc,
    server_host,
    server_page_limit,
)
from ._driver import (
    AttemptControl,
    Driver,
    ExecutionOptions,
    LiveResponse,
)
from ._errors import ShotgunError
from ._execution import (
    ExceptionCapture,
    SuppressedExceptionCapture,
    release_lease,
    retire_transport,
)
from ._signatures import bind_arguments, expose_signature
from ._transport import (
    SendError,
    StreamingResponse,
    Transport,
    TransportError,
    TransportRequest,
    TransportResponse,
    Urllib3Transport,
)
from ._version import PACKAGE_VERSION

_ReadinessKey: TypeAlias = tuple[str, int]
_UploadResult = TypeVar('_UploadResult')

__version__ = PACKAGE_VERSION

_CONSTRUCTOR_PARAMETERS = (
    inspect.Parameter('base_url', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter(
        'script_name',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'api_key',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'convert_datetimes_to_utc',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=True,
        annotation=bool,
    ),
    inspect.Parameter(
        'http_proxy',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'connect',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=True,
        annotation=bool,
    ),
    inspect.Parameter(
        'ca_certs',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'login',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'password',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'sudo_as_login',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'session_token',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'auth_token',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
)
_UPLOAD_DOCSTRING = """Upload a file or thumbnail and return its Attachment ID.

A successful HTTP response from the legacy form-upload route still
raises `ShotgunError`; no Attachment ID is established for that
route. The form request has already been sent when this error is
raised.

Args:
    entity_type: Entity type receiving the upload.
    entity_id: Numeric identifier of the receiving entity.
    path: Existing non-empty file path as text or filesystem-encoded bytes.
    field_name: Optional file field receiving an Attachment.
    display_name: Optional Attachment display name.
    tag_list: Optional comma-delimited Attachment tags.
    created_at: Optional Attachment creation timestamp.

Returns:
    Created Attachment ID.

Raises:
    TypeError: If arguments do not match the documented signature.
    ShotgunError: If the file or an upload response is invalid, an RPC
        fault or terminal transport failure occurs, or the legacy
        form-upload route succeeds.
    ProtocolError: If an upload endpoint returns an HTTP error.
    ValueError: If authenticated HTTP violates the configured origin policy.
"""
_UPLOAD_PARAMETERS = (
    inspect.Parameter('entity_type', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
    inspect.Parameter('entity_id', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=int),
    inspect.Parameter('path', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str | bytes),
    inspect.Parameter(
        'field_name',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'display_name',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'tag_list',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        'created_at',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=None,
        annotation=_dt_module.datetime | None,
    ),
)
CONSTRUCTOR_SIGNATURE = inspect.Signature(_CONSTRUCTOR_PARAMETERS, return_annotation=None)
_METHOD_CONSTRUCTOR_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_CONSTRUCTOR_PARAMETERS,
    ),
    return_annotation=None,
)
_METHOD_UPLOAD_SIGNATURE = inspect.Signature(
    (
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        *_UPLOAD_PARAMETERS,
    ),
    return_annotation=int,
)
_UPLOAD_SIGNATURE = inspect.Signature(_UPLOAD_PARAMETERS, return_annotation=int)


@dataclasses.dataclass(frozen=True)
class ConstructorOptions:
    """Collect the exact public constructor arguments for runtime validation.

    Attributes:
        base_url: Flow Production Tracking site URL.
        script_name: Script name for script authentication.
        api_key: API key for script authentication.
        convert_datetimes_to_utc: Whether to convert datetime values to UTC.
        http_proxy: Optional proxy authority in `[user:pass@]host[:port]` form.
        connect: Whether to request eager readiness during synchronous
            construction or asynchronous context entry.
        ca_certs: Optional certificate-authority bundle path.
        login: Login for human-user authentication.
        password: Password for human-user authentication.
        sudo_as_login: Optional user login for sudo impersonation.
        session_token: Existing session token for authentication.
        auth_token: Optional two-factor authentication token.
    """

    base_url: str
    script_name: str | None = None
    api_key: str | None = None
    convert_datetimes_to_utc: bool = True
    http_proxy: str | None = None
    connect: bool = True
    ca_certs: str | None = None
    login: str | None = None
    password: str | None = None
    sudo_as_login: str | None = None
    session_token: str | None = None
    auth_token: str | None = None


class _DownloadCleanupError(Exception):
    """Keep required download cleanup failures outside transport retry policy."""


@dataclasses.dataclass(frozen=True)
class _DownloadExchange:
    """Own one restartable streaming download workflow."""

    context: RequestContext
    file_path: str | None
    headers: Mapping[str, str] = dataclasses.field(repr=False)
    url: str = dataclasses.field(repr=False)

    def _redirect(self, response: LiveResponse) -> TransportRequest:
        """Build one credential-free explicit redirect request."""
        return _files.build_download_redirect(self.context, response.headers)

    def _write_file(
        self,
        chunks: Iterator[bytes],
        control: AttemptControl,
        total_bytes: int | None,
    ) -> str:
        """Publish one fully consumed attempt through an adjacent temp file."""
        if self.file_path is None:
            raise AssertionError('file download requires a destination')
        destination = Path(self.file_path)
        stack: ExitStack | None = ExitStack()
        temporary: Path | None = None
        transfer = ExceptionCapture()
        try:
            with transfer:
                handle = stack.enter_context(_files.download_temporary(destination))
                temporary = Path(handle.name)
                control.consume_into(
                    chunks,
                    handle.write,
                    total_bytes=total_bytes,
                )
                opened_stack = stack
                stack = None
                opened_cleanup = SuppressedExceptionCapture()
                with opened_cleanup:
                    opened_stack.close()
                if isinstance(opened_cleanup.exception, Exception):
                    raise _DownloadCleanupError from None
                if opened_cleanup.exception is not None:
                    raise opened_cleanup.exception
                temporary.replace(destination)
        finally:
            close_cleanup = SuppressedExceptionCapture()
            unlink_cleanup = SuppressedExceptionCapture()
            if stack is not None:
                with close_cleanup:
                    stack.close()
            if temporary is not None:
                with unlink_cleanup:
                    temporary.unlink(missing_ok=True)
            if transfer.exception is None:
                for cleanup_error in (close_cleanup.exception, unlink_cleanup.exception):
                    if isinstance(cleanup_error, Exception):
                        raise _DownloadCleanupError from None
                    if cleanup_error is not None:
                        raise cleanup_error
        return self.file_path

    def _consume(self, response: LiveResponse, control: AttemptControl) -> bytes | str:
        """Consume a successful streaming response into attempt-local output."""
        if not _protocol.is_http_success(response.status):
            raise _errors.ShotgunFileDownloadError('attachment download failed')
        streaming = cast('StreamingResponse', response)
        chunks = streaming.iter_chunks()
        total_bytes = _transport.content_length(response.headers)
        if self.file_path is None:
            return control.consume(chunks, total_bytes=total_bytes)
        return self._write_file(chunks, control, total_bytes)

    def _execute(
        self,
        driver: Driver,
        operation: _protocol.OperationDescriptor,
    ) -> bytes | str:
        """Execute the workflow through the common streaming attempt loop."""
        try:
            return driver.execute(
                operation,
                self.context,
                lambda _attempt: _files.build_download_request(
                    self.context, self.url, self.headers
                ),
                self._consume,
                options=ExecutionOptions(
                    redirect_factory=self._redirect,
                    streaming=True,
                ),
            )
        except _DownloadCleanupError:
            raise SendError('download cleanup failed') from None


@dataclasses.dataclass(frozen=True)
class _InvocationOptions:
    """Collect non-payload controls for one client invocation."""

    include_auth: bool = True
    compatibility_warning_codes: tuple[CompatibilityWarningCode, ...] = ()
    logical_method_name: str | None = None
    auth_spec: _protocol.AuthSpec | None = None


def _bind_upload(
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> _files.UploadArguments:
    """Bind runtime arguments with upload-specific errors."""
    bound = bind_arguments(_UPLOAD_SIGNATURE, args, kwargs, 'Shotgun.upload()')
    arguments = cast('Callable[..., _files.UploadArguments]', _files.UploadArguments)
    return arguments(**bound.arguments)


def _execute_upload_exchange(
    driver: Driver,
    context: RequestContext,
    exchange: _files.UploadExchange,
    parser: Callable[[str, Mapping[str, str]], _UploadResult],
) -> _UploadResult:
    """Run one upload exchange through the common attempt executor."""

    def request_factory(_attempt: int) -> TransportRequest:
        return TransportRequest(
            method=exchange.method,
            url=exchange.url,
            headers=dict(exchange.headers),
            body=exchange.body_factory(),
            timeout=context.timeout,
            token=uuid.uuid4().hex,
        )

    def consume(
        response: LiveResponse,
        control: AttemptControl,
    ) -> _UploadResult:
        buffered = cast('TransportResponse', response)
        _protocol.check_http_status(
            buffered.status,
            buffered.reason,
            context.canonical_origin or '',
            buffered.headers,
            buffered.content,
        )
        body = control.consume((buffered.content,), total_bytes=len(buffered.content))
        return parser(body.decode('utf-8', errors='replace'), buffered.headers)

    return driver.execute(
        exchange.operation,
        context,
        request_factory,
        consume,
    )


def _expose_upload_signature(function: Callable[..., object]) -> Callable[..., object]:
    """Expose the observed upload signature on its variadic implementation."""
    return expose_signature(function, _METHOD_UPLOAD_SIGNATURE, _UPLOAD_DOCSTRING)


def bind_constructor(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    client_name: str,
) -> ConstructorOptions:
    """Bind constructor arguments with client-specific errors.

    Args:
        args: Positional public arguments.
        kwargs: Keyword public arguments.
        client_name: Public client class name for error attribution.

    Returns:
        Bound immutable constructor options.

    Raises:
        TypeError: If the arguments do not match the public signature.
    """
    bound = bind_arguments(CONSTRUCTOR_SIGNATURE, args, kwargs, f'{client_name}.__init__()')
    constructor = cast('Callable[..., ConstructorOptions]', ConstructorOptions)
    return constructor(**bound.arguments)


def build_config(options: ConstructorOptions) -> Config:
    """Initialize and validate network-free configuration, auth, and proxy fields.

    Raises:
        TypeError: If a configuration or proxy value has an unsupported type.
        ValueError: If authentication, site or proxy values, or
            `SHOTGUN_API_RETRY_INTERVAL` are invalid.
    """
    config = Config()
    config.set_server_params(options.base_url)
    config.update_fields(
        api_key=options.api_key,
        auth_token=options.auth_token,
        convert_datetimes_to_utc=options.convert_datetimes_to_utc,
        script_name=options.script_name,
        session_token=options.session_token,
        sudo_as_login=options.sudo_as_login,
        user_login=options.login,
        user_password=options.password,
    )
    _protocol.validate_auth(config_module.auth_spec(config))
    proxy_fields = constructor_http_proxy_fields(options.http_proxy)
    if proxy_fields is not None:
        config.update_fields(**proxy_fields)
    return config


def expose_constructor_signature(function: Callable[..., object]) -> Callable[..., object]:
    """Expose the documented constructor signature.

    Args:
        function: Variadic constructor implementation.

    Returns:
        The same function with its public runtime signature attached.
    """
    expose_signature(function, _METHOD_CONSTRUCTOR_SIGNATURE, None)
    annotations = {
        name: parameter.annotation
        for name, parameter in CONSTRUCTOR_SIGNATURE.parameters.items()
        if parameter.annotation is not inspect.Parameter.empty
    }
    annotations['return'] = CONSTRUCTOR_SIGNATURE.return_annotation
    object.__setattr__(function, '__annotations__', annotations)
    return function


class Shotgun:
    """Provide the synchronous Flow Production Tracking API client surface.

    Instances may be shared across threads. Call `close()` to retire the
    current transport; in-flight work retains its leases until completion.
    A later operation reconnects lazily.

    Attributes:
        base_url: The constructor's normalized base URL.
        client_caps: Capabilities of this Python client.
        config: The writable client configuration.
        server_caps: Validated capabilities for the configured origin.
        server_info: Raw validated server metadata.
    """

    __signature__ = CONSTRUCTOR_SIGNATURE

    @overload
    def __init__(self, base_url: Never) -> None: ...

    @overload
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
    ) -> None: ...

    @expose_constructor_signature
    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize a client for one site and authentication mode.

        Supply exactly one authentication mode: `script_name` with `api_key`,
        `login` with `password`, or `session_token`. An optional one-use
        `auth_token` requires `login` and `password`.

        `base_url` must identify an HTTP or HTTPS site without embedded
        credentials. Authenticated remote operations require HTTPS.
        `http_proxy` accepts `[user:pass@]host[:port]`, with port 8080 when
        omitted. A truthy `ca_certs` selects a CA bundle; otherwise
        `SHOTGUN_API_CACERTS` is consulted before using system trust roots.

        Construction performs server-readiness I/O by default. Supplying
        `auth_token` also performs a session-token exchange during
        construction, even when `connect` is `False`.

        Args:
            *args: Positional values from the documented constructor signature.
            **kwargs: Keyword values from the documented constructor signature.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ValueError: If authentication, URL, or proxy values are invalid.
            ShotgunError: If authentication, server metadata, or an RPC result
                is invalid, or a transport attempt fails.
            ProtocolError: If eager readiness or token exchange receives an
                HTTP error.
        """
        self._initialize(bind_constructor(args, kwargs, 'Shotgun'))

    @property
    def _connection(self) -> Transport | None:
        """Return the current private connection compatibility view."""
        with self._transport_lock:
            return self._transport

    def _invoke(
        self,
        driver: Driver,
        method_name: str,
        params: object,
        context: RequestContext,
        options: _InvocationOptions,
    ) -> object:
        """Run one transformed RPC using only its immutable snapshot."""
        require_secure_rpc(context, options.include_auth)
        outbound = _protocol.transform_outbound(params, context.convert_datetimes_to_utc)
        result = driver.call(
            method_name,
            outbound,
            context,
            include_auth=options.include_auth,
            compatibility_warning_codes=options.compatibility_warning_codes,
            _logical_method_name=options.logical_method_name,
        )
        return _protocol.transform_inbound(result, context.convert_datetimes_to_utc)

    def _new_transport(self, context: RequestContext) -> Transport:
        """Create a transport for one captured generation."""
        return Urllib3Transport(
            ca_certs=self._ca_certs,
            proxy_headers=dict(context.proxy_headers) or None,
            proxy_url=context.proxy_url,
            pool_maxsize=context.pool_maxsize,
        )

    def _publish_capabilities(
        self,
        context: RequestContext,
        capabilities: ServerCapabilities,
    ) -> bool:
        """Publish metadata only while its captured origin remains current."""
        return config_module.publish_server_info(self._config, context, capabilities.server_info)

    def _release_transport(self, transport: Transport) -> None:
        """Release a lease and close a retired transport at its safe point."""
        with self._transport_lock:
            close_after_unlock = release_lease(
                self._lease_counts, self._retired_transports, transport
            )
        if close_after_unlock is not None:
            close_after_unlock.close()

    def _release_transport_if_retired_close_fails(
        self,
        transport: Transport,
        retired_transports: list[Transport],
    ) -> None:
        """Close retired transports or roll back the replacement lease."""
        try:
            for retired in retired_transports:
                retired.close()
        except BaseException:
            with suppress(BaseException):
                self._release_transport(transport)
            raise

    def _acquire_driver(self, context: RequestContext) -> tuple[Driver, Transport]:
        """Lease the driver and transport for one complete logical operation."""
        close_after_unlock: list[Transport] = []
        with self._transport_lock:
            if self._known_transport is not self._transport:
                if self._known_transport is not None:
                    retired = retire_transport(
                        self._lease_counts, self._retired_transports, self._known_transport
                    )
                    if retired is not None:
                        close_after_unlock.append(retired)
                self._known_transport = self._transport
                self._driver = None
                self._driver_generation = None
                self._transport_owned = False

            transport = self._transport
            if transport is None:
                transport = self._new_transport(context)
                self._transport = transport
                self._known_transport = transport
                self._transport_owned = True
            elif self._driver_generation not in {None, context.transport_generation}:
                if self._transport_owned:
                    retired = retire_transport(
                        self._lease_counts, self._retired_transports, transport
                    )
                    if retired is not None:
                        close_after_unlock.append(retired)
                    transport = self._new_transport(context)
                    self._transport = transport
                    self._known_transport = transport
                self._driver = None

            if self._driver is None:
                self._driver = Driver(transport)
            self._driver_generation = context.transport_generation
            key = id(transport)
            self._lease_counts[key] = self._lease_counts.get(key, 0) + 1
            driver = self._driver
        self._release_transport_if_retired_close_fails(transport, close_after_unlock)
        return driver, transport

    @contextmanager
    def _driver_lease(self, context: RequestContext) -> Generator[Driver, None, None]:
        """Keep one transport alive across readiness and operation I/O."""
        driver, transport = self._acquire_driver(context)
        try:
            yield driver
        finally:
            self._release_transport(transport)

    def _run_info(
        self,
        context: RequestContext,
        driver: Driver,
        *,
        accept_ready: bool,
    ) -> tuple[dict[str, object], ServerCapabilities]:
        """Own, validate, and publish one readiness metadata exchange."""
        key = readiness_key(context)
        owner = threading.get_ident()
        with self._readiness:
            while key in self._readiness_in_progress:
                if self._readiness_owner[key] == owner:
                    raise RuntimeError('readiness cannot recursively wait on itself')
                self._readiness.wait()
            cached = self._ready_caps.get(key)
            if accept_ready and cached is not None:
                return cached.server_info, cached
            self._readiness_in_progress.add(key)
            self._readiness_owner[key] = owner

        capabilities: ServerCapabilities | None = None
        published = False
        try:
            result = self._invoke(
                driver,
                'info',
                None,
                context,
                _InvocationOptions(include_auth=False),
            )
            if not isinstance(result, dict):
                raise ShotgunError(
                    'The Flow Production Tracking server returned invalid metadata.'
                )
            metadata = cast('dict[str, object]', result)
            server_page_limit(metadata)
            capabilities = ServerCapabilities(server_host(context), metadata)
            published = self._publish_capabilities(context, capabilities)
            return metadata, capabilities
        finally:
            with self._readiness:
                if self._readiness_owner.get(key) == owner:
                    self._readiness_in_progress.discard(key)
                    self._readiness_owner.pop(key)
                    current_key = config_module.readiness_identity(self._config)
                    if capabilities is not None and published and key == current_key:
                        self._ready_caps.clear()
                        self._ready_caps[key] = capabilities
                self._readiness.notify_all()

    def _ensure_readiness(
        self,
        context: RequestContext,
        driver: Driver,
    ) -> ServerCapabilities:
        """Validate one origin while coalescing concurrent callers."""
        _meta, capabilities = self._run_info(context, driver, accept_ready=True)
        return capabilities

    def _call(
        self,
        method_name: str,
        params: object,
        invocation_options: _InvocationOptions | None = None,
    ) -> object:
        """Run one RPC from a coherent request snapshot."""
        owned_params = copy.deepcopy(params)
        options = invocation_options or _InvocationOptions()
        context = config_module.snapshot(self._config, options.auth_spec)
        if method_name != 'info':
            require_secure_rpc(context, options.include_auth)
        with self._driver_lease(context) as driver:
            if method_name == 'info':
                result, _capabilities = self._run_info(
                    context,
                    driver,
                    accept_ready=False,
                )
                return result
            self._ensure_readiness(context, driver)
            return self._invoke(
                driver,
                method_name,
                owned_params,
                context,
                options,
            )

    def _get_session_token_for(self, context: RequestContext, driver: Driver) -> str:
        """Return a token obtained entirely from one captured request state."""
        if context.session_token is not None:
            return context.session_token
        self._ensure_readiness(context, driver)
        result = self._invoke(
            driver,
            'get_session_token',
            None,
            context,
            _InvocationOptions(),
        )
        if not isinstance(result, dict):
            raise ShotgunError('get_session_token did not return a session_id')
        token_result = cast('dict[str, object]', result)
        session_id = token_result.get('session_id')
        if not isinstance(session_id, str):
            raise ShotgunError('get_session_token did not return a session_id')
        config_module.publish_session_token(self._config, context, session_id)
        return session_id

    @staticmethod
    def _share_thumbnail_source(
        driver: Driver,
        context: RequestContext,
        entities: list[Mapping[str, object]],
        source_entity: Mapping[str, object],
        filmstrip_thumbnail: bool,
    ) -> int:
        """Share one captured source thumbnail through the upload endpoint."""
        fields = _files.add_auth_fields(
            _files.build_share_thumbnail_fields(
                entities,
                source_entity,
                filmstrip_thumbnail,
            ),
            context.auth,
        )
        return _execute_upload_exchange(
            driver,
            context,
            _files.build_upload_endpoint_exchange(
                context.canonical_origin,
                context.user_agent,
                _files.SHARE_THUMBNAIL_OPERATION,
                _files.SHARE_THUMBNAIL,
                fields,
            ),
            lambda text, _headers: _files.parse_link_completion(text),
        )

    def _thumbnail_source_ready(
        self,
        driver: Driver,
        context: RequestContext,
        source_entity_type: str,
        source_entity_id: int,
        field_name: Literal['filmstrip_image', 'image'],
    ) -> bool:
        """Return whether one captured thumbnail field is no longer transient."""
        params = _crud.build_thumbnail_read_params(
            source_entity_type, source_entity_id, field_name, context.records_per_page
        )
        result = self._invoke(
            driver,
            'read',
            params,
            context,
            _InvocationOptions(),
        )
        return _files.thumbnail_source_ready(result, field_name)

    @staticmethod
    def _upload_allocation(
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        multipart: bool,
    ) -> _files.UploadLinkInfo:
        """Allocate one direct or multipart upload."""
        options = prepared.options
        arguments = options.arguments
        fields = _files.add_auth_fields(
            _files.build_upload_link_fields(
                prepared.filename,
                options.upload_type,
                multipart,
                arguments.created_at,
            ),
            context.auth,
        )
        return _execute_upload_exchange(
            driver,
            context,
            _files.build_upload_endpoint_exchange(
                context.canonical_origin,
                context.user_agent,
                _protocol.classify_endpoint('allocation'),
                _files.GET_UPLOAD_LINK_INFO,
                fields,
            ),
            lambda text, _headers: _files.UploadLinkInfo(text, multipart=multipart),
        )

    @staticmethod
    def _upload_direct(
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        upload_info: _files.UploadLinkInfo,
    ) -> None:
        """Stream one complete file to stable object storage."""
        _execute_upload_exchange(
            driver,
            context,
            _files.build_storage_upload_exchange(
                context.user_agent,
                _protocol.classify_endpoint('object_put'),
                upload_info.upload_url,
                _files.StorageRange(
                    length=prepared.file_size,
                    offset=0,
                    path=prepared.options.arguments.path,
                ),
            ),
            lambda _text, _headers: None,
        )

    @staticmethod
    def _upload_form(
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
    ) -> Never:
        """Send the observed form request without guessing a success result."""
        if context.canonical_origin is None:
            raise ValueError('server parameters must be configured before upload I/O')
        exchange = _files.build_form_upload_exchange(
            context.canonical_origin,
            context.user_agent,
            prepared,
            context.auth,
        )

        def reject_success(_text: str, _headers: Mapping[str, str]) -> Never:
            raise ShotgunError('form upload success response is not established')

        _execute_upload_exchange(driver, context, exchange, reject_success)
        raise AssertionError('form upload response parser returned')

    @staticmethod
    def _upload_link(
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        upload_info: _files.UploadLinkInfo,
    ) -> int:
        """Link a stored object to its receiving entity."""
        fields = _files.add_auth_fields(
            _files.build_link_file_fields(
                _files.build_link_entity_fields(prepared),
                upload_info.encoded,
            ),
            context.auth,
        )
        return _execute_upload_exchange(
            driver,
            context,
            _files.build_upload_endpoint_exchange(
                context.canonical_origin,
                context.user_agent,
                _protocol.classify_endpoint('link'),
                _files.LINK_FILE,
                fields,
            ),
            lambda text, _headers: _files.parse_link_completion(text),
        )

    @staticmethod
    def _upload_multipart(
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        upload_info: _files.UploadLinkInfo,
    ) -> None:
        """Stream ordered parts and finalize one multipart allocation."""
        etags: list[str] = []
        for part_number, storage_range in enumerate(
            _files.multipart_ranges(prepared),
            start=1,
        ):
            fields = _files.add_auth_fields(
                _files.build_part_fields(upload_info, prepared.filename, part_number),
                context.auth,
            )
            part_url = _execute_upload_exchange(
                driver,
                context,
                _files.build_upload_endpoint_exchange(
                    context.canonical_origin,
                    context.user_agent,
                    _protocol.classify_endpoint('allocation'),
                    _files.GET_PART_LINK,
                    fields,
                ),
                lambda text, _headers: _files.parse_part_link(text),
            )
            etag = _execute_upload_exchange(
                driver,
                context,
                _files.build_storage_upload_exchange(
                    context.user_agent,
                    _protocol.classify_endpoint('part_put'),
                    part_url,
                    storage_range,
                ),
                _files.parse_part_etag,
            )
            etags.append(etag)

        fields = _files.add_auth_fields(
            _files.build_complete_fields(upload_info, prepared.filename, etags),
            context.auth,
        )
        _execute_upload_exchange(
            driver,
            context,
            _files.build_upload_endpoint_exchange(
                context.canonical_origin,
                context.user_agent,
                _protocol.classify_endpoint('finalize'),
                _files.COMPLETE_MULTIPART,
                fields,
            ),
            lambda text, _headers: _files.parse_multipart_completion(text),
        )

    def _upload_for(
        self,
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
    ) -> int:
        """Execute one prepared upload through a captured driver lease."""
        arguments = prepared.options.arguments
        capabilities = self._ensure_readiness(context, driver)
        upload_path = _files.choose_upload_path(
            capabilities.server_info,
            arguments.entity_type,
            arguments.field_name,
            prepared.file_size,
        )
        if upload_path == 'form':
            return self._upload_form(driver, context, prepared)
        multipart = upload_path == 'multipart'
        upload_info = self._upload_allocation(driver, context, prepared, multipart)
        if multipart:
            self._upload_multipart(driver, context, prepared, upload_info)
        else:
            self._upload_direct(driver, context, prepared, upload_info)
        return self._upload_link(driver, context, prepared, upload_info)

    def _upload(self, options: _files.UploadOptions) -> int:
        """Execute one evidence-backed upload workflow."""
        prepared = _files.prepare_upload(options)
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        with self._driver_lease(context) as driver:
            return self._upload_for(driver, context, prepared)

    def _upload_thumbnail_for(
        self,
        driver: Driver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
    ) -> int:
        """Execute the exact direct-storage thumbnail workflow."""
        self._ensure_readiness(context, driver)
        upload_info = self._upload_allocation(driver, context, prepared, multipart=False)
        self._upload_direct(driver, context, prepared, upload_info)
        return self._upload_link(driver, context, prepared, upload_info)

    @overload
    def activity_stream_read(self, entity_type: Never) -> dict[str, object]: ...

    @overload
    def activity_stream_read(
        self,
        entity_type: str,
        entity_id: int,
        entity_fields: dict[str, list[str]] | None = None,
        min_id: int | None = None,
        max_id: int | None = None,
        limit: int | None = None,
    ) -> dict[str, object]: ...

    @_methods.expose_activity_stream_read_signature
    def activity_stream_read(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Read the activity stream for one entity.

        Args:
            *args: Positional arguments from the documented activity-stream
                signature.
            **kwargs: Keyword arguments from the documented activity-stream
                signature.

        Returns:
            Activity stream data returned by the server.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_activity_stream_read(args, kwargs, 'Shotgun')
        return _crud.require_dictionary_result(
            self._call(
                'activity_stream',
                _methods.build_activity_stream_read_params(options),
                invocation_options=_InvocationOptions(logical_method_name='activity_stream_read'),
            ),
            'activity_stream_read',
        )

    def add_user_agent(self, agent: str) -> None:
        """Append a user-agent fragment.

        Args:
            agent: User-agent fragment visible to subsequent requests.
        """
        self._config.append_user_agent(agent)

    def authenticate_human_user(
        self,
        user_login: str,
        user_password: str,
        auth_token: str | None = None,
    ) -> dict[str, object] | None:
        """Authenticate a human user without changing the client's own auth.

        Args:
            user_login: Login of the user to authenticate.
            user_password: Password for `user_login`.
            auth_token: Optional two-factor authentication token.

        Returns:
            The matching `HumanUser` entity, or `None` when no active user
            matches or the server rejects the credentials.

        Raises:
            MissingTwoFactorAuthenticationFault: If two-factor authentication
                is required but missing.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        plan = _methods.build_authentication_plan(
            user_login,
            user_password,
            auth_token,
        )
        try:
            raw_result = self._call(
                'read',
                plan.params,
                invocation_options=_InvocationOptions(
                    logical_method_name='authenticate_human_user',
                    auth_spec=plan.auth_spec,
                ),
            )
        except _errors.AuthenticationFault:
            return None
        return _methods.require_human_authentication_result(raw_result)

    @property
    def base_url(self) -> str:
        """Return the constructor's normalized base URL.

        Returns:
            The site URL without a trailing slash.
        """
        return self._base_url

    def batch(self, requests: list[dict[str, Any]]) -> list[object]:
        """Run create, update, and delete requests in one transaction.

        Args:
            requests: Ordered request dictionaries to validate and send.

        Returns:
            Results in the same order as the supplied requests.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if a request is malformed or has an unsupported request
                type, or if the server result is not a list.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        calls = _crud.build_batch_calls(requests)
        if not calls:
            return []
        return _crud.require_list_result(self._call('batch', calls), 'batch')

    @property
    def client_caps(self) -> ClientCapabilities:
        """Return capabilities of this Python client.

        Returns:
            Local platform capability metadata.
        """
        return self._client_caps

    def _find(self, options: _crud.FindOptions) -> list[dict[str, object]]:
        """Run one paged read operation from one immutable context."""
        prepared = _crud.prepare_find(options)
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        records: list[dict[str, object]] = []

        with self._driver_lease(context) as driver:
            capabilities = self._ensure_readiness(context, driver)
            operation_context = dataclasses.replace(
                context,
                records_per_page=server_page_limit(capabilities.server_info),
            )
            plan = _crud.build_find_plan(
                prepared,
                operation_context.records_per_page,
            )
            if not plan.multi_page:
                operation_context = dataclasses.replace(
                    operation_context,
                    page_prefetch=0,
                )

            def build_page_params(page_number: int) -> object:
                return _crud.build_page_params(
                    plan,
                    page_number,
                    operation_context.convert_datetimes_to_utc,
                )

            pages = driver.call_pages(
                build_page_params,
                plan.paging['current_page'],
                'read',
                operation_context,
            )
            try:
                for raw_result in pages:
                    page = _crud.validate_read_page(
                        raw_result,
                        convert_utc=operation_context.convert_datetimes_to_utc,
                        records_count=len(records),
                        plan=plan,
                    )
                    records.extend(page.entities)
                    if page.stop:
                        break
            finally:
                pages.close()
        return _crud.finalize_find(
            records,
            options,
            plan,
            self.client_caps.local_path_field,
        )

    def close(self) -> None:
        """Retire known and current transports after in-flight work releases them."""
        close_after_unlock: list[Transport] = []
        with self._transport_lock:
            advance_transport_generation(self._config)
            if self._known_transport is not None and self._known_transport is not self._transport:
                retired = retire_transport(
                    self._lease_counts, self._retired_transports, self._known_transport
                )
                if retired is not None:
                    close_after_unlock.append(retired)
            transport = self._transport
            self._driver = None
            self._driver_generation = None
            self._known_transport = None
            self._transport = None
            self._transport_owned = False
            if transport is not None:
                retired = retire_transport(self._lease_counts, self._retired_transports, transport)
                if retired is not None:
                    close_after_unlock.append(retired)
        pending_closes = iter(close_after_unlock)
        try:
            for retired in pending_closes:
                retired.close()
        except BaseException:
            for retired in pending_closes:
                with suppress(BaseException):
                    retired.close()
            raise
        with self._readiness:
            current_key = config_module.readiness_identity(self._config)
            current = self._ready_caps.get(current_key) if current_key is not None else None
            self._ready_caps.clear()
            if current_key is not None and current is not None:
                self._ready_caps[current_key] = current

    @property
    def config(self) -> Config:
        """Return the writable client configuration.

        Returns:
            The locked client configuration object.
        """
        return self._config

    def connect(self) -> None:
        """Validate readiness for the configured origin.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters are missing or the configured
                origin lacks a valid host.
        """
        context = config_module.snapshot(self._config)
        with self._driver_lease(context) as driver:
            self._ensure_readiness(context, driver)

    def create(
        self,
        entity_type: str,
        data: dict[str, Any],
        return_fields: list[str] | None = None,
    ) -> dict[str, object]:
        """Create an entity with the supplied field values.

        Args:
            entity_type: Entity type to create.
            data: Field values for the new entity.
            return_fields: Fields to return in addition to the entity identity.

        Returns:
            The created entity data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `data`, its field names, `return_fields`, or the server
                result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _crud.build_create_params(entity_type, data, return_fields)
        return _crud.require_dictionary_result(self._call('create', params), 'create')

    def delete(self, entity_type: str, entity_id: int) -> bool:
        """Retire an entity.

        Args:
            entity_type: Entity type to retire.
            entity_id: Numeric entity identifier.

        Returns:
            Whether the server retired the entity.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is not a boolean.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_boolean_result(
            self._call('delete', _crud.build_entity_action_params(entity_type, entity_id)),
            'delete',
        )

    @overload
    def download_attachment(
        self,
        attachment: int | Mapping[str, object] | None,
        file_path: str | None = None,
        attachment_id: int | None = None,
    ) -> bytes | str | None: ...

    @overload
    def download_attachment(
        self,
        *,
        file_path: str | None = None,
        attachment_id: int,
    ) -> bytes | str | None: ...

    def download_attachment(
        self,
        attachment: int | Mapping[str, object] | None = False,
        file_path: str | None = None,
        attachment_id: int | None = None,
    ) -> bytes | str | None:
        """Download an attachment to memory or an atomic destination path.

        Args:
            attachment: Attachment ID, Attachment dictionary, URL dictionary,
                or `None`.
            file_path: Optional destination path.
            attachment_id: Legacy attachment ID used when `attachment` is
                omitted.

        Returns:
            Downloaded bytes, the supplied destination path, or `None`.

        Raises:
            TypeError: If no attachment is given or the reference is a string.
            ValueError: If an entity dictionary references a different type.
            ShotgunError: If another attachment reference is unsupported or a
                setup RPC reports a fault; the fault keeps its specific type.
            ShotgunFileDownloadError: If the resolved URL is invalid or setup,
                transfer, or destination I/O fails.
            ValueError: If server settings are incomplete or authenticated HTTP
                violates the configured origin security policy.
        """
        if attachment is False:
            if attachment_id is None:
                raise TypeError('attachment or attachment_id is required')
            attachment = attachment_id
        context = config_module.snapshot(self._config)
        if context.canonical_origin is None or context.site_url is None:
            raise ValueError('server parameters must be configured before download I/O')
        url = _files.resolve_attachment_url(attachment, context.site_url)
        if url is None:
            return None
        _files.validate_download_url(url)
        origin = _files.canonical_origin(context.canonical_origin)
        use_session_cookie = _files.needs_session_cookie(url, origin)
        if use_session_cookie:
            require_secure_rpc(context, include_auth=True)
        operation = _protocol.classify_endpoint('download')

        try:
            with self._driver_lease(context) as driver:
                headers = {'user-agent': context.user_agent}
                if use_session_cookie:
                    if context.session_token is None:
                        token = self._get_session_token_for(context, driver)
                    else:
                        self._ensure_readiness(context, driver)
                        token = context.session_token
                    headers['cookie'] = f'_session_id={token}'
                exchange = _DownloadExchange(
                    context=context,
                    file_path=file_path,
                    headers=headers,
                    url=url,
                )
                return exchange._execute(driver, operation)  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        except _errors.ShotgunFileDownloadError:
            raise
        except (TransportError, _errors.ProtocolError, OSError):
            raise _errors.ShotgunFileDownloadError('attachment download failed') from None

    @overload
    def export_page(
        self,
        page_id: int,
        page_format: str,
        /,
        layout_name: str | None = None,
    ) -> str: ...

    @overload
    def export_page(
        self,
        page_id: int,
        **kwargs: Unpack[_methods.ExportPageKeywordArguments],
    ) -> str: ...

    @_methods.expose_export_page_signature
    def export_page(self, *args: object, **kwargs: object) -> str:
        """Export a page using the documented arguments.

        Args:
            *args: Positional values from the documented `export_page`
                signature.
            **kwargs: Keyword values from the documented `export_page`
                signature.

        Returns:
            Exported page text returned by the server.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if an argument or the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_export_page(args, kwargs, 'Shotgun')
        params = _methods.build_export_page_params(options)
        return _methods.require_string_result(self._call('export_page', params), 'export_page')

    @overload
    def find(self, entity_type: Never) -> list[dict[str, object]]: ...

    @overload
    def find(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        limit: int = 0,
        retired_only: bool = False,
        page: int = 0,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> list[dict[str, object]]: ...

    @_crud.expose_find_signature
    def find(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
        """Find entities matching the supplied filters.

        Args:
            *args: Positional values from the documented `find` signature.
            **kwargs: Keyword values from the documented `find` signature.

        Returns:
            Matching entities in server order.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if filters, paging, ordering, or a server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return self._find(_crud.bind_find(args, kwargs, 'Shotgun'))

    @overload
    def find_one(self, entity_type: Never) -> dict[str, object] | None: ...

    @overload
    def find_one(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        fields: list[str] | None = None,
        order: list[dict[str, str]] | None = None,
        filter_operator: str | None = None,
        retired_only: bool = False,
        include_archived_projects: bool = True,
        additional_filter_presets: object = None,
    ) -> dict[str, object] | None: ...

    @_crud.expose_find_one_signature
    def find_one(self, *args: object, **kwargs: object) -> dict[str, object] | None:
        """Return the first entity matching the supplied filters.

        Args:
            *args: Positional values from the documented `find_one` signature.
            **kwargs: Keyword values from the documented `find_one` signature.

        Returns:
            The first matching entity, or `None` when no entity matches.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if filters, paging, ordering, or a server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _crud.bind_find_one(args, kwargs, 'Shotgun')
        return _crud.first_result(self._find(_crud.find_options_from_one(options)))

    def follow(self, user: dict[str, Any], entity: dict[str, Any]) -> dict[str, object]:
        """Make a user follow an entity.

        Args:
            user: User entity that will follow the target.
            entity: Entity to follow.

        Returns:
            Follow result returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_follow_params(user, entity)
        return _crud.require_dictionary_result(self._call('follow', params), 'follow')

    def followers(self, entity: dict[str, Any]) -> list[dict[str, object]]:
        """List users following an entity.

        Args:
            entity: Entity whose followers to list.

        Returns:
            Following user entities returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_followers_params(entity)
        return _methods.require_dictionary_list_result(
            self._call('followers', params),
            'followers',
        )

    def following(
        self,
        user: dict[str, Any],
        project: dict[str, Any] | None = None,
        entity_type: str | None = None,
    ) -> list[dict[str, object]]:
        """List entities a user follows, optionally narrowed by project and type.

        Args:
            user: User entity whose followed entities to list.
            project: Optional project entity restriction.
            entity_type: Optional entity-type restriction.

        Returns:
            Followed entities returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_following_params(user, project, entity_type)
        return _methods.require_dictionary_list_result(
            self._call('following', params),
            'following',
        )

    def get_attachment_download_url(
        self, attachment: int | Mapping[str, object] | None
    ) -> str | None:
        """Resolve an attachment reference to its download URL.

        Args:
            attachment: Attachment ID, Attachment dictionary, URL dictionary,
                or `None`.

        Returns:
            The supplied URL, a site attachment URL, or `None`.

        Raises:
            TypeError: If the attachment reference is a string.
            ValueError: If an entity dictionary references a different type or
                server parameters have not been configured.
            ShotgunError: If the attachment reference is unsupported.
        """
        if attachment is None:
            return None
        if isinstance(attachment, Mapping):
            mapping = cast('Mapping[object, object]', attachment)
            url = mapping.get('url')
            if isinstance(url, str):
                return url
        context = config_module.snapshot(self._config)
        if context.site_url is None:
            raise ValueError('server parameters must be configured before URL generation')
        return _files.resolve_attachment_url(cast('object', attachment), context.site_url)

    def get_auth_cookie_handler(self) -> urllib.request.HTTPCookieProcessor:
        """Return a session-cookie handler bound to the captured origin.

        Returns:
            Stdlib handler containing one exact-origin session cookie.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if token exchange returns an invalid result.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters have not been configured or
                authenticated HTTP violates the configured origin policy.
        """
        context = config_module.snapshot(self._config)
        if context.canonical_origin is None:
            raise ValueError('server parameters must be configured before cookie creation')
        origin = _files.canonical_origin(context.canonical_origin)
        if context.session_token is None:
            require_secure_rpc(context, include_auth=True)
        with self._driver_lease(context) as driver:
            token = self._get_session_token_for(context, driver)
        return _files.build_auth_cookie_handler(token, origin)

    def get_session_token(self) -> str:
        """Return or obtain a session token for the current credentials.

        Returns:
            The existing or newly exchanged session token.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the RPC result lacks the expected session ID.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        context = config_module.snapshot(self._config)
        if context.session_token is not None:
            return context.session_token
        require_secure_rpc(context, include_auth=True)
        with self._driver_lease(context) as driver:
            return self._get_session_token_for(context, driver)

    def _initialize(self, options: ConstructorOptions) -> None:
        """Initialize client state from validated constructor options."""
        self._base_url = options.base_url.rstrip('/')
        self._ca_certs = options.ca_certs or os.environ.get('SHOTGUN_API_CACERTS') or None
        self._client_caps = ClientCapabilities()
        self._config = build_config(options)

        self._driver: Driver | None = None
        self._driver_generation: int | None = None
        self._known_transport: Transport | None = None
        self._lease_counts: dict[int, int] = {}
        self._retired_transports: dict[int, Transport] = {}
        self._transport: Transport | None = None
        self._transport_lock = threading.Lock()
        self._transport_owned = False

        self._readiness = threading.Condition()
        self._readiness_in_progress: set[_ReadinessKey] = set()
        self._readiness_owner: dict[_ReadinessKey, int] = {}
        self._ready_caps: dict[_ReadinessKey, ServerCapabilities] = {}

        if options.connect:
            self.connect()
        if options.auth_token is not None:
            self.get_session_token()

    def info(self) -> dict[str, object]:
        """Fetch and publish unauthenticated server metadata.

        Returns:
            Raw server metadata after version-floor validation.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters are missing or the configured
                origin lacks a valid host.
        """
        return cast(
            'dict[str, object]',
            self._call('info', None, _InvocationOptions(include_auth=False)),
        )

    def nav_expand(
        self,
        path: str,
        seed_entity_field: str | None = None,
        entity_fields: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        """Expand one level of the navigation hierarchy at `path`.

        Args:
            path: Navigation path to expand.
            seed_entity_field: Optional field used to seed the navigation path.
            entity_fields: Optional fields to return for linked entity types.

        Returns:
            Expanded navigation data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_nav_expand_params(
            path,
            seed_entity_field,
            entity_fields,
        )
        return _crud.require_dictionary_result(
            self._call('nav_expand', params),
            'nav_expand',
        )

    def nav_search_entity(
        self,
        root_path: str,
        entity: dict[str, Any],
        seed_entity_field: str | None = None,
    ) -> list[dict[str, object]]:
        """Search the navigation hierarchy under `root_path` for an entity.

        Args:
            root_path: Navigation path under which to search.
            entity: Entity to find in the navigation hierarchy.
            seed_entity_field: Optional field used to seed the navigation path.

        Returns:
            Matching navigation data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_nav_search_entity_params(
            root_path,
            entity,
            seed_entity_field,
        )
        return _methods.require_dictionary_list_result(
            self._call('nav_search_entity', params),
            'nav_search_entity',
        )

    def nav_search_string(
        self,
        root_path: str,
        search_string: str,
        seed_entity_field: str | None = None,
    ) -> list[dict[str, object]]:
        """Search the navigation hierarchy under `root_path` by string.

        Args:
            root_path: Navigation path under which to search.
            search_string: String to match in the navigation hierarchy.
            seed_entity_field: Optional field used to seed the navigation path.

        Returns:
            Matching navigation data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_nav_search_string_params(
            root_path,
            search_string,
            seed_entity_field,
        )
        return _methods.require_dictionary_list_result(
            self._call('nav_search_string', params),
            'nav_search_string',
        )

    def note_thread_read(
        self,
        note_id: int,
        entity_fields: dict[str, list[str]] | None = None,
    ) -> list[dict[str, object]]:
        """Read a note and its replies and attachments as a flat thread.

        Args:
            note_id: Numeric identifier of the note.
            entity_fields: Optional fields to return for linked entity types.

        Returns:
            Note thread records returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_note_thread_read_params(note_id, entity_fields)
        return _methods.require_dictionary_list_result(
            self._call('note_thread_read', params),
            'note_thread_read',
        )

    def preferences_read(self, prefs: list[str] | None = None) -> dict[str, object]:
        """Read site preferences, optionally restricted by name.

        Args:
            prefs: Preference names to read, or `None` to read all defaults.

        Returns:
            Preference values returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_dictionary_result(
            self._call('preferences_read', _methods.build_preferences_read_params(prefs)),
            'preferences_read',
        )

    def reset_user_agent(self) -> None:
        """Restore this client's default user-agent fragments."""
        self._config.reset_user_agent()

    def revive(self, entity_type: str, entity_id: int) -> bool:
        """Restore a retired entity.

        Args:
            entity_type: Entity type to restore.
            entity_id: Numeric entity identifier.

        Returns:
            Whether the server restored the entity.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is not a boolean.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_boolean_result(
            self._call('revive', _crud.build_entity_action_params(entity_type, entity_id)),
            'revive',
        )

    def schema_entity_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return entity-type schema, optionally for one project.

        Args:
            project_entity: Optional project entity restricting the schema.

        Returns:
            Entity-type schema data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_dictionary_result(
            self._call(
                'schema_entity_read',
                _methods.build_schema_entity_read_params(project_entity),
                _InvocationOptions(
                    compatibility_warning_codes=_methods.consume_compatibility_warnings(
                        _methods.SCHEMA_ENTITY_READ_WARNINGS,
                        self,
                    ),
                ),
            ),
            'schema_entity_read',
        )

    def entity_types(self) -> dict[str, object]:
        """Return entity schema through the deprecated alias.

        Returns:
            Entity-type schema data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is not a dictionary.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        _methods.warn_entity_types_alias()
        with _methods.entity_types_alias(self):
            return self.schema_entity_read()

    def schema_field_create(
        self,
        entity_type: str,
        data_type: str,
        display_name: str,
        properties: dict[str, Any] | None = None,
    ) -> str:
        """Create a field and return its internal name.

        Args:
            entity_type: Entity type on which to create the field.
            data_type: Data type for the new field.
            display_name: Display name for the new field.
            properties: Optional additional field properties.

        Returns:
            Internal name of the created field.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        result = self._call(
            'schema_field_create',
            _methods.build_schema_field_create_params(
                entity_type,
                data_type,
                display_name,
                properties,
            ),
        )
        return _methods.require_string_result(result, 'schema_field_create')

    def schema_field_delete(self, entity_type: str, field_name: str) -> bool:
        """Delete a field.

        Args:
            entity_type: Entity type containing the field.
            field_name: Internal field name to delete.

        Returns:
            Whether the server deleted the field.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_boolean_result(
            self._call(
                'schema_field_delete',
                _methods.build_schema_field_delete_params(entity_type, field_name),
            ),
            'schema_field_delete',
        )

    def schema_field_read(
        self,
        entity_type: str,
        field_name: str | None = None,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return field schema, optionally for one field or project.

        Args:
            entity_type: Entity type whose fields to return.
            field_name: Optional internal field name to return.
            project_entity: Optional project entity restricting the schema.

        Returns:
            Field schema data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_dictionary_result(
            self._call(
                'schema_field_read',
                _methods.build_schema_field_read_params(
                    entity_type,
                    field_name,
                    project_entity,
                ),
                _InvocationOptions(
                    compatibility_warning_codes=_methods.consume_compatibility_warnings(
                        _methods.SCHEMA_FIELD_READ_WARNINGS,
                        self,
                    ),
                ),
            ),
            'schema_field_read',
        )

    def schema(self, entity_type: str) -> dict[str, object]:
        """Return field schema through the deprecated alias.

        Args:
            entity_type: Entity type whose fields to return.

        Returns:
            Field schema data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        _methods.warn_schema_alias()
        with _methods.schema_alias(self):
            return self.schema_field_read(entity_type)

    def schema_field_update(
        self,
        entity_type: str,
        field_name: str,
        properties: dict[str, Any],
        project_entity: dict[str, Any] | None = None,
    ) -> bool:
        """Update field properties.

        Args:
            entity_type: Entity type containing the field.
            field_name: Internal field name to update.
            properties: Field properties to update.
            project_entity: Optional project entity restricting the update.

        Returns:
            Whether the server updated the field.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_boolean_result(
            self._call(
                'schema_field_update',
                _methods.build_schema_field_update_params(
                    entity_type,
                    field_name,
                    properties,
                    project_entity,
                ),
            ),
            'schema_field_update',
        )

    def schema_read(
        self,
        project_entity: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the full schema, optionally for one project.

        Args:
            project_entity: Optional project entity restricting the schema.

        Returns:
            Full schema data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_schema_entity_read_params(project_entity)
        return _crud.require_dictionary_result(self._call('schema_read', params), 'schema_read')

    @property
    def server_caps(self) -> ServerCapabilities:
        """Return validated capabilities for the configured origin.

        Returns:
            Server capability metadata at or above the supported floor.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters are missing or the configured
                origin lacks a valid host.
        """
        context = config_module.snapshot(self._config)
        with self._driver_lease(context) as driver:
            return self._ensure_readiness(context, driver)

    @property
    def server_info(self) -> dict[str, object]:
        """Return raw validated server metadata.

        Returns:
            The metadata that populated `server_caps`.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters are missing or the configured
                origin lacks a valid host.
        """
        return self.server_caps.server_info

    def set_session_uuid(self, session_uuid: str) -> None:
        """Set the browser session UUID for subsequent requests.

        Args:
            session_uuid: Browser session UUID to publish.
        """
        self._config.set_session_uuid(session_uuid)

    def share_thumbnail(
        self,
        entities: list[dict[str, Any]],
        thumbnail_path: str | None = None,
        source_entity: dict[str, Any] | None = None,
        filmstrip_thumbnail: bool = False,
        **kwargs: object,
    ) -> int:
        """Associate one thumbnail with one or more entities.

        Supply exactly one of `thumbnail_path` and `source_entity`. A local
        thumbnail is uploaded to the first target before it is shared with the
        remaining targets.

        Args:
            entities: Nonempty list of target entities with `type` and `id`.
            thumbnail_path: Optional local thumbnail path to upload once.
            source_entity: Optional entity whose existing thumbnail is shared.
            filmstrip_thumbnail: Whether to share a filmstrip thumbnail.
            **kwargs: Additional upload arguments for path-based sharing;
                `field_name` is not accepted. Ignored for an existing source.

        Returns:
            Attachment ID representing the source thumbnail.

        Raises:
            TypeError: If forwarded arguments do not match the upload signature
                or include `field_name`.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the source selection, entities, or transport fails.
            ShotgunThumbnailNotReady: If an uploaded source stays transient.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        share = _files.prepare_thumbnail_share(
            entities,
            thumbnail_path,
            source_entity,
            filmstrip_thumbnail,
        )
        if share.thumbnail_path is not None and 'field_name' in kwargs:
            raise TypeError("Shotgun.upload() got multiple values for argument 'field_name'")
        prepared: _files.PreparedUpload | None = None
        if share.thumbnail_path is not None:
            arguments = _bind_upload(
                (
                    share.source_entity_type,
                    share.source_entity_id,
                    share.thumbnail_path,
                ),
                {
                    **kwargs,
                    'field_name': (
                        'filmstrip_thumb_image' if filmstrip_thumbnail else 'thumb_image'
                    ),
                },
            )
            prepared = _files.prepare_upload(
                _files.UploadOptions(
                    arguments=arguments,
                    upload_type='Thumbnail',
                    filmstrip=filmstrip_thumbnail,
                )
            )
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        with self._driver_lease(context) as driver:
            if prepared is not None:
                attachment_id = self._upload_thumbnail_for(driver, context, prepared)
                if len(share.entities) == 1:
                    return attachment_id
                field_name: Literal['filmstrip_image', 'image'] = (
                    'filmstrip_image' if filmstrip_thumbnail else 'image'
                )
                ready = driver.poll(
                    lambda: self._thumbnail_source_ready(
                        driver,
                        context,
                        share.source_entity_type,
                        share.source_entity_id,
                        field_name,
                    ),
                    attempts=25,
                    interval=context.rpc_attempt_interval / 1000.0,
                )
                if not ready:
                    raise _errors.ShotgunThumbnailNotReady('thumbnail source did not become ready')
                return self._share_thumbnail_source(
                    driver,
                    context,
                    share.entities[1:],
                    share.source_entity,
                    filmstrip_thumbnail,
                )
            self._ensure_readiness(context, driver)
            return self._share_thumbnail_source(
                driver,
                context,
                share.entities,
                share.source_entity,
                filmstrip_thumbnail,
            )

    @overload
    def summarize(self, entity_type: Never) -> dict[str, object]: ...

    @overload
    def summarize(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        summary_fields: list[dict[str, Any]],
        filter_operator: str | None = None,
        grouping: list[dict[str, Any]] | None = None,
        include_archived_projects: bool = True,
    ) -> dict[str, object]: ...

    @_methods.expose_summarize_signature
    def summarize(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Summarize field data returned by a query.

        Args:
            *args: Positional values from the documented `summarize` signature.
            **kwargs: Keyword values from the documented `summarize` signature.

        Returns:
            Dict with `summaries` and (when grouped) `groups`.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `filters`, `summary_fields`, or the server result has the
                wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_summarize(args, kwargs, 'Shotgun')
        params = _methods.build_summarize_params(options)
        return _crud.require_dictionary_result(self._call('summarize', params), 'summarize')

    def text_search(
        self,
        text: str,
        entity_types: dict[str, Any],
        project_ids: list[int] | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        """Search entity display names matching `text` (min 3 chars server-side).

        Args:
            text: Text to match.
            entity_types: Mapping of entity type to filters restricting that
                type.
            project_ids: Optional project id allowlist.
            limit: Maximum results to return.

        Returns:
            Dict describing matches, as returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `entity_types` or the server result is not a dictionary.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_text_search_params(
            text,
            entity_types,
            project_ids,
            limit,
        )
        return _crud.require_dictionary_result(
            self._call(
                'query_display_name_cache',
                params,
                invocation_options=_InvocationOptions(logical_method_name='text_search'),
            ),
            'text_search',
        )

    def unfollow(self, user: dict[str, Any], entity: dict[str, Any]) -> dict[str, object]:
        """Make a user stop following an entity.

        Args:
            user: User entity that will stop following the target.
            entity: Entity to stop following.

        Returns:
            Unfollow result returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if an argument or the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_follow_params(user, entity)
        return _crud.require_dictionary_result(self._call('unfollow', params), 'unfollow')

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, Any],
        multi_entity_update_modes: dict[str, str] | None = None,
    ) -> dict[str, object]:
        """Update fields on an existing entity.

        Args:
            entity_type: Entity type to update.
            entity_id: Numeric entity identifier.
            data: Replacement field values.
            multi_entity_update_modes: Per-field multi-entity update modes.

        Returns:
            The updated entity data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `data`, its field names, `multi_entity_update_modes`, or
                the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _crud.build_update_params(
            entity_type,
            entity_id,
            data,
            multi_entity_update_modes,
        )
        return _crud.require_dictionary_result(self._call('update', params), 'update')

    def update_project_last_accessed(
        self,
        project: dict[str, Any],
        user: dict[str, Any] | None = None,
    ) -> None:
        """Record that a project was accessed by the current or given user.

        Args:
            project: Project entity whose access time to update.
            user: Optional user entity to attribute the access to.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        self._call(
            'update_project_last_accessed_by_current_user',
            _methods.build_update_project_last_accessed_params(project, user),
            invocation_options=_InvocationOptions(
                logical_method_name='update_project_last_accessed'
            ),
        )

    @overload
    def upload(self, entity_type: Never) -> int: ...

    @overload
    def upload(
        self,
        entity_type: str,
        entity_id: int,
        path: str | bytes,
        field_name: str | None = None,
        display_name: str | None = None,
        tag_list: str | None = None,
        created_at: _dt_module.datetime | None = None,
    ) -> int: ...

    @_expose_upload_signature
    def upload(self, *args: object, **kwargs: object) -> int:
        """Upload a file or thumbnail and return its Attachment ID.

        A successful HTTP response from the legacy form-upload route still
        raises `ShotgunError`; no Attachment ID is established for that route.
        The form request has already been sent when this error is raised.

        Args:
            *args: Positional arguments from the documented upload signature;
                the path may be text or filesystem-encoded bytes.
            **kwargs: Keyword arguments from the documented upload signature.

        Returns:
            Created Attachment ID.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If the file or an upload response is invalid, an RPC
                fault or terminal transport failure occurs, or the legacy
                form-upload route succeeds.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        arguments = _bind_upload(args, kwargs)
        thumbnail_fields = {
            'filmstrip_image',
            'filmstrip_thumb_image',
            'image',
            'thumb_image',
        }
        upload_type: Literal['Attachment', 'Thumbnail'] = (
            'Thumbnail' if arguments.field_name in thumbnail_fields else 'Attachment'
        )
        return self._upload(
            _files.UploadOptions(
                arguments=arguments,
                upload_type=upload_type,
                filmstrip=arguments.field_name in {'filmstrip_image', 'filmstrip_thumb_image'},
            )
        )

    def upload_filmstrip_thumbnail(
        self,
        entity_type: str,
        entity_id: int,
        path: str,
        **kwargs: Unpack[_files.ThumbnailUploadKeywordArguments],
    ) -> int:
        """Upload a filmstrip thumbnail through `upload`.

        Args:
            entity_type: Entity type receiving the thumbnail.
            entity_id: Numeric identifier of the receiving entity.
            path: Existing non-empty image path.
            **kwargs: Additional arguments forwarded to `upload`.

        Returns:
            Created Attachment ID.

        Raises:
            TypeError: If forwarded arguments do not match `upload`.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the file, upload response, or transport fails.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return self.upload(
            entity_type,
            entity_id,
            path,
            field_name='filmstrip_thumb_image',
            **kwargs,
        )

    def upload_thumbnail(
        self,
        entity_type: str,
        entity_id: int,
        path: str,
        **kwargs: Unpack[_files.ThumbnailUploadKeywordArguments],
    ) -> int:
        """Upload a static thumbnail through `upload`.

        Args:
            entity_type: Entity type receiving the thumbnail.
            entity_id: Numeric identifier of the receiving entity.
            path: Existing non-empty image path.
            **kwargs: Additional arguments forwarded to `upload`.

        Returns:
            Created Attachment ID.

        Raises:
            TypeError: If forwarded arguments do not match `upload`.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the file, upload response, or transport fails.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return self.upload(
            entity_type,
            entity_id,
            path,
            field_name='thumb_image',
            **kwargs,
        )

    def user_subscriptions_create(self, users: list[dict[str, Any]]) -> bool:
        """Assign subscriptions to users.

        Args:
            users: Subscription dictionaries that each contain a `user_id` key.

        Returns:
            Whether the server created the subscriptions.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `users` is not a list of dictionaries that each contain a
                `user_id` key, or the server result is not a boolean.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _crud.require_boolean_result(
            self._call(
                'user_subscriptions_create',
                _methods.build_user_subscriptions_create_params(users),
            ),
            'user_subscriptions_create',
        )

    def user_subscriptions_read(self) -> list[dict[str, object]]:
        """List the current user's subscriptions.

        Returns:
            User subscription dictionaries returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is not a list of dictionaries.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return _methods.require_dictionary_list_result(
            self._call('user_subscriptions_read', None),
            'user_subscriptions_read',
        )

    def work_schedule_read(
        self,
        start_date: _dt_module.date | str,
        end_date: _dt_module.date | str,
        project: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Read working-day rules between two dates.

        Args:
            start_date: Inclusive date or exact `YYYY-MM-DD` string.
            end_date: Inclusive date or exact `YYYY-MM-DD` string.
            project: Optional project entity restriction.
            user: Optional user entity restriction.

        Returns:
            Work schedule data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if dates, restrictions, or the server result are invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_work_schedule_read_params(
            start_date,
            end_date,
            project,
            user,
        )
        return _crud.require_dictionary_result(
            self._call('work_schedule_read', params),
            'work_schedule_read',
        )

    @overload
    def work_schedule_update(self, date: Never) -> dict[str, object]: ...

    @overload
    def work_schedule_update(
        self,
        date: _dt_module.date | str,
        working: bool,
        description: str | None = None,
        project: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
        recalculate_field: str | None = None,
    ) -> dict[str, object]: ...

    @_methods.expose_work_schedule_update_signature
    def work_schedule_update(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Update a work schedule using the documented arguments.

        Args:
            *args: Positional values from the documented `work_schedule_update`
                signature.
            **kwargs: Keyword values from the documented `work_schedule_update`
                signature.

        Returns:
            Updated work schedule data returned by the server.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if dates, exclusivity, or the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_work_schedule_update(args, kwargs, 'Shotgun')
        params = _methods.build_work_schedule_update_params(options)
        return _crud.require_dictionary_result(
            self._call('work_schedule_update', params),
            'work_schedule_update',
        )
