"""Native-async Shotgun client and lifecycle."""

# Same-file collaboration follows module-boundary privacy.
# pyright: reportPrivateUsage=false

import asyncio
import copy
import dataclasses
import datetime as _dt_module
import importlib
import inspect
import os
import threading
import urllib.request
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from contextlib import ExitStack, asynccontextmanager, suppress
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Literal,
    Never,
    ParamSpec,
    Self,
    TypeAlias,
    TypeVar,
    Unpack,
    cast,
    overload,
)

from . import _config as config_module
from . import _crud, _errors, _files, _methods, _protocol, _transport
from ._async_driver import (
    AsyncAttemptControl,
    AsyncDriver,
    AsyncExecutionOptions,
    AsyncLiveResponse,
)
from ._async_transport import AsyncStreamingResponse, AsyncTransport
from ._capabilities import ClientCapabilities, ServerCapabilities
from ._client import (
    CONSTRUCTOR_SIGNATURE,
    ConstructorOptions,
    Shotgun,
    bind_constructor,
    build_config,
    expose_constructor_signature,
)
from ._config import (
    Config,
    RequestContext,
    advance_transport_generation,
    readiness_key,
    require_secure_rpc,
    server_host,
    server_page_limit,
)
from ._execution import SuppressedExceptionCapture, release_lease, retire_transport
from ._signatures import bind_arguments, expose_signature
from ._transport import (
    ReadTimeoutError,
    SendError,
    TransportError,
    TransportRequest,
    TransportResponse,
)

if TYPE_CHECKING:
    from ._async_httpx2 import Httpx2Transport

_AuthExchangeKey: TypeAlias = tuple[str, int, int]
_ASYNC_FILE_BATCH_SIZE = 1024 * 1024
_ReadinessProgress: TypeAlias = tuple[asyncio.Task[object], asyncio.Event]
_ReadinessKey: TypeAlias = tuple[str, int]
_FilesystemParams = ParamSpec('_FilesystemParams')
_FilesystemResult = TypeVar('_FilesystemResult')
_UploadResult = TypeVar('_UploadResult')

_METHOD_UPLOAD_SIGNATURE = inspect.signature(Shotgun.upload)
_UPLOAD_SIGNATURE = inspect.signature(Shotgun.upload).replace(
    parameters=tuple(inspect.signature(Shotgun.upload).parameters.values())[1:]
)


class _AsyncDownloadCancellationScope:
    """Own and join every filesystem worker started by one transfer."""

    def __init__(self) -> None:
        """Initialize cancellation retention for one filesystem worker."""
        self._captured_cancellation: asyncio.CancelledError | None = None

    @property
    def _cancellation(self) -> asyncio.CancelledError | None:
        """Return the first cancellation captured while joining a worker."""
        return self._captured_cancellation

    def _capture_cancellation(self, error: BaseException) -> None:
        """Retain one actively delivered caller cancellation."""
        current = asyncio.current_task()
        if (
            not isinstance(error, asyncio.CancelledError)
            or current is None
            or current.cancelling() == 0
        ):
            return
        if self._captured_cancellation is None:
            self._captured_cancellation = error
        current.uncancel()

    def _raise_if_cancelled(self) -> None:
        """Propagate a captured cancellation only between worker phases."""
        if self._captured_cancellation is not None:
            raise self._captured_cancellation

    async def _run(
        self,
        function: Callable[_FilesystemParams, _FilesystemResult],
        *args: _FilesystemParams.args,
        **kwargs: _FilesystemParams.kwargs,
    ) -> _FilesystemResult:
        """Run one worker to completion while retaining cancellation."""
        task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        while not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError as error:
                self._capture_cancellation(error)
        try:
            return task.result()
        except BaseException:
            self._raise_if_cancelled()
            raise


class _AsyncDownloadCleanupCapture:
    """Capture one cleanup failure without replacing transfer state."""

    def __init__(self, workers: _AsyncDownloadCancellationScope) -> None:
        """Initialize an empty async download-cleanup capture."""
        self._workers = workers
        self.exception: BaseException | None = None

    def __enter__(self) -> Self:
        """Enter the async download-cleanup boundary."""
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool:
        """Capture and suppress one async download-cleanup exception."""
        self.exception = self._workers._cancellation or exception
        return exception is not None


class _AsyncDownloadCleanupError(Exception):
    """Mark a completed transfer whose required cleanup failed."""


@dataclasses.dataclass(frozen=True)
class _AsyncDownloadExchange:
    """Own one restartable async streaming download workflow."""

    context: RequestContext
    file_path: str | None
    headers: Mapping[str, str] = dataclasses.field(repr=False)
    url: str = dataclasses.field(repr=False)

    def _redirect(self, response: AsyncLiveResponse) -> TransportRequest:
        """Build one credential-free explicit redirect request."""
        return _files.build_download_redirect(self.context, response.headers)

    async def _write_file(
        self,
        chunks: AsyncIterator[bytes],
        control: AsyncAttemptControl,
        total_bytes: int | None,
    ) -> str:
        """Publish one complete attempt through an adjacent temp file."""
        if self.file_path is None:
            raise AssertionError('file download requires a destination')
        destination = Path(self.file_path)
        stack: ExitStack | None = None
        handle: BinaryIO | None = None
        operation_exception: BaseException | None = None
        temporary: Path | None = None
        workers = _AsyncDownloadCancellationScope()
        try:
            try:
                stack, handle, temporary = await workers._run(
                    _open_download_temporary,
                    destination,
                )
                workers._raise_if_cancelled()
                opened_handle = handle

                async def write(chunk: bytes) -> None:
                    await workers._run(opened_handle.write, chunk)
                    workers._raise_if_cancelled()

                await control.consume_into(chunks, write, total_bytes=total_bytes)
                await workers._run(handle.flush)
                workers._raise_if_cancelled()
                close_exception = await _capture_download_cleanup(workers, stack.close)
                stack = None
                handle = None
                workers._raise_if_cancelled()
                if close_exception is not None:
                    _raise_download_cleanup_error()
                # This is the final cancellation checkpoint before atomic commit.
                # Once `replace` starts, its worker is joined before cancellation
                # propagates, so the destination cannot change after return.
                await asyncio.sleep(0)
                await workers._run(temporary.replace, destination)
                workers._raise_if_cancelled()
            except BaseException as error:
                operation_exception = error
                workers._capture_cancellation(error)
                raise
        finally:
            close_exception: BaseException | None = None
            unlink_exception: BaseException | None = None
            if stack is not None:
                close_exception = await _capture_download_cleanup(workers, stack.close)
            if temporary is not None:
                unlink_exception = await _capture_download_cleanup(
                    workers,
                    temporary.unlink,
                    missing_ok=True,
                )
            workers._raise_if_cancelled()
            if operation_exception is None:
                if close_exception is not None:
                    raise _AsyncDownloadCleanupError from None
                if unlink_exception is not None:
                    raise _AsyncDownloadCleanupError from None
        return self.file_path

    async def _consume(
        self,
        response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> bytes | str:
        """Consume one successful streaming response attempt."""
        if not _protocol.is_http_success(response.status):
            raise _errors.ShotgunFileDownloadError('attachment download failed')
        streaming = cast('AsyncStreamingResponse', response)
        total_bytes = _transport.content_length(response.headers)
        if self.file_path is None:
            chunks = streaming.aiter_chunks()
            return await control.consume(chunks, total_bytes=total_bytes)
        chunks = streaming.aiter_chunks(_ASYNC_FILE_BATCH_SIZE)
        return await self._write_file(chunks, control, total_bytes)

    async def _execute(
        self,
        driver: AsyncDriver,
        operation: _protocol.OperationDescriptor,
    ) -> bytes | str:
        """Execute the workflow through the common async attempt loop."""
        try:
            return await driver.execute(
                operation,
                self.context,
                lambda _attempt: _files.build_download_request(
                    self.context, self.url, self.headers
                ),
                self._consume,
                options=AsyncExecutionOptions(
                    redirect_factory=self._redirect,
                    streaming=True,
                ),
            )
        except _AsyncDownloadCleanupError:
            raise SendError('download cleanup failed') from None


@dataclasses.dataclass(frozen=True)
class _InvocationOptions:
    """Collect non-payload controls for one client invocation."""

    auth_spec: _protocol.AuthSpec | None = None
    include_auth: bool = True
    logical_method_name: str | None = None


@dataclasses.dataclass(frozen=True)
class _RpcInvocation:
    """Bind one immutable context to one RPC invocation."""

    context: RequestContext
    method_name: str
    options: _InvocationOptions
    params: object


@dataclasses.dataclass
class _TokenExchangeProgress:
    """Carry one exchange result until captured auth-context users drain."""

    owner: asyncio.Task[object]
    event: asyncio.Event
    error: ReadTimeoutError | SendError | asyncio.CancelledError | None = None
    token: str | None = None


def _bind_upload(
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> _files.UploadArguments:
    """Bind runtime arguments with upload-specific errors."""
    bound = bind_arguments(_UPLOAD_SIGNATURE, args, kwargs, 'AsyncShotgun.upload()')
    arguments = cast('Callable[..., _files.UploadArguments]', _files.UploadArguments)
    return arguments(**bound.arguments)


async def _capture_download_cleanup(
    workers: _AsyncDownloadCancellationScope,
    function: Callable[_FilesystemParams, _FilesystemResult],
    *args: _FilesystemParams.args,
    **kwargs: _FilesystemParams.kwargs,
) -> BaseException | None:
    """Run one joined cleanup operation and capture its failure."""
    capture = _AsyncDownloadCleanupCapture(workers)
    with capture:
        await workers._run(function, *args, **kwargs)
    return capture.exception


def _expose_upload_signature(function: Callable[..., object]) -> Callable[..., object]:
    """Attach the implemented sync upload signature and annotations."""
    expose_signature(function, _METHOD_UPLOAD_SIGNATURE, Shotgun.upload.__doc__)
    object.__setattr__(function, '__annotations__', Shotgun.upload.__annotations__)
    return function


class AsyncShotgun:
    """Provide the asynchronous Flow Production Tracking API client surface.

    Instances are bound to one event loop and may be shared by tasks on that
    loop. Use an async context manager or call `await close()` to release the
    current transport. Closing drains in-flight work; a later operation
    reconnects lazily.

    Attributes:
        base_url: The constructor's normalized base URL.
        client_caps: Capabilities of this Python client.
        config: The writable client configuration.
        server_caps: Cached capabilities for the configured origin.
        server_info: Cached raw metadata for the configured origin.
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

        Constructor validation is synchronous; readiness and authentication
        exchange I/O begins only through an operation, `connect()`, or an async
        context manager. With `connect=False`, entering the context performs no
        I/O.

        Args:
            *args: Positional values from the documented constructor signature.
            **kwargs: Keyword values from the documented constructor signature.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ValueError: If authentication, URL, or proxy values are invalid.
        """
        self._initialize(bind_constructor(args, kwargs, 'AsyncShotgun'))

    async def __aenter__(self) -> Self:
        """Enter this client and perform any configured eager connection.

        Returns:
            This client.
        """
        if not self._connect_on_enter:
            return self
        entry_capture = SuppressedExceptionCapture()
        with entry_capture:
            await self.connect()
        if entry_capture.exception is not None:
            await self._close_failed_context_entry(entry_capture.exception)
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close this client's current transport on context exit.

        Args:
            *exc: Context-manager exception details, if any.
        """
        await self.close()

    def _checkin_transport(self, transport: AsyncTransport) -> AsyncTransport | None:
        """Release a lease on its owning task and return any idle retired transport."""
        with self._transport_lock:
            close_after_release = release_lease(
                self._lease_counts, self._retired_transports, transport
            )
        transport.checkin()
        return close_after_release

    def _complete_token_exchange(
        self,
        key: _AuthExchangeKey,
        progress: _TokenExchangeProgress,
        error: ReadTimeoutError | SendError | asyncio.CancelledError | None,
        token: str | None,
    ) -> None:
        """Publish one completed token exchange and wake its waiters."""
        with self._token_exchange_lock:
            active = self._token_exchanges.get(key)
            if active is not progress:
                return
            progress.error = error
            progress.token = token
            if (error is None and token is None) or not self._active_auth_contexts.get(key, 0):
                self._token_exchanges.pop(key)
            progress.event.set()

    async def _finalize_readiness(
        self,
        key: _ReadinessKey,
        owner: asyncio.Task[object],
        event: asyncio.Event,
        capabilities: ServerCapabilities | None,
        published: bool,
    ) -> None:
        """Publish current readiness and wake waiters for a matching owner."""
        async with self._readiness_lock:
            progress = self._readiness_in_progress.get(key)
            if progress is not None and progress[0] is owner:
                self._readiness_in_progress.pop(key)
                if (
                    capabilities is not None
                    and published
                    and config_module.readiness_identity(self._config) == key
                ):
                    self._ready_caps.clear()
                    self._ready_caps[key] = capabilities
            event.set()

    def _initialize(self, options: ConstructorOptions) -> None:
        """Initialize network-free state from validated constructor options."""
        self._base_url = options.base_url.rstrip('/')
        self._ca_certs = options.ca_certs or os.environ.get('SHOTGUN_API_CACERTS') or None
        self._client_caps = ClientCapabilities()
        self._connect_on_enter = options.connect
        self._config = build_config(options)

        self._driver: AsyncDriver | None = None
        self._driver_generation: int | None = None
        self._known_transport: AsyncTransport | None = None
        self._lease_counts: dict[int, int] = {}
        self._retired_transports: dict[int, AsyncTransport] = {}
        self._transport: AsyncTransport | None = None
        self._transport_lock = threading.Lock()
        self._transport_owned = False

        self._readiness_lock = asyncio.Lock()
        self._readiness_in_progress: dict[_ReadinessKey, _ReadinessProgress] = {}
        self._ready_caps: dict[_ReadinessKey, ServerCapabilities] = {}
        self._active_auth_contexts: dict[_AuthExchangeKey, int] = {}
        self._token_exchange_lock = threading.Lock()
        self._token_exchanges: dict[_AuthExchangeKey, _TokenExchangeProgress] = {}

    async def _invoke(
        self,
        driver: AsyncDriver,
        invocation: _RpcInvocation,
    ) -> object:
        """Transform and delegate one RPC to the common async executor."""
        context = invocation.context
        require_secure_rpc(context, invocation.options.include_auth)
        outbound = _protocol.transform_outbound(
            invocation.params,
            context.convert_datetimes_to_utc,
        )
        result = await driver.call(
            invocation.method_name,
            outbound,
            context,
            include_auth=invocation.options.include_auth,
            _logical_method_name=invocation.options.logical_method_name,
        )
        return _protocol.transform_inbound(result, context.convert_datetimes_to_utc)

    async def _join_readiness_finalizer(
        self,
        finalizer: asyncio.Task[None],
        *,
        preserve_cancellation: bool,
    ) -> None:
        """Finish readiness finalization before propagating cancellation."""
        cancellation: asyncio.CancelledError | None = None
        current = asyncio.current_task()
        while not finalizer.done():
            try:
                await asyncio.shield(finalizer)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
                if current is not None:
                    current.uncancel()
        finalizer.result()
        if cancellation is not None and not preserve_cancellation:
            raise cancellation

    def _new_transport(self, context: RequestContext) -> AsyncTransport:
        """Create an httpx2 transport from one immutable context."""
        module = importlib.import_module(f'{__package__}._async_httpx2')
        factory = cast(
            'type[Httpx2Transport]',
            module.Httpx2Transport,
        )
        return factory(
            ca_certs=self._ca_certs,
            proxy_headers=dict(context.proxy_headers) or None,
            proxy_url=context.proxy_url,
            pool_maxsize=context.pool_maxsize,
        )

    def _acquire_driver(
        self,
        context: RequestContext,
    ) -> tuple[AsyncDriver, AsyncTransport, tuple[AsyncTransport, ...]]:
        """Lease one generation's driver and return idle transports to close."""
        close_after_unlock: list[AsyncTransport] = []
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
                self._driver = AsyncDriver(transport)
            self._driver_generation = context.transport_generation
            transport.checkout()
            key = id(transport)
            self._lease_counts[key] = self._lease_counts.get(key, 0) + 1
            driver = self._driver
        return driver, transport, tuple(close_after_unlock)

    async def _release_failed_acquisition(
        self, transport: AsyncTransport, primary_exception: BaseException
    ) -> Never:
        """Join replacement rollback and raise the selected acquisition outcome."""
        cancellation: asyncio.CancelledError | None = None
        current = asyncio.current_task()
        if (
            isinstance(primary_exception, asyncio.CancelledError)
            and current is not None
            and current.cancelling() > 0
        ):
            cancellation = primary_exception
            current.uncancel()

        release_capture = SuppressedExceptionCapture()
        with release_capture:
            close_after_release = self._checkin_transport(transport)
            if close_after_release is not None:

                async def close_retired() -> None:
                    # Suppress inside the task so cancelled shields cannot report
                    # a secondary failure through the event-loop handler.
                    with suppress(BaseException):
                        await close_after_release.close()

                cleanup = asyncio.create_task(close_retired())
                while not cleanup.done():
                    wait_capture = SuppressedExceptionCapture()
                    with wait_capture:
                        await asyncio.shield(cleanup)
                    error = wait_capture.exception
                    if (
                        isinstance(error, asyncio.CancelledError)
                        and current is not None
                        and current.cancelling() > 0
                    ):
                        if cancellation is None:
                            cancellation = error
                        current.uncancel()
                cleanup.result()
        if cancellation is not None:
            raise cancellation
        raise primary_exception

    async def _release_transport(self, transport: AsyncTransport) -> None:
        """Release one client lease and close a fully retired transport."""
        close_after_release = self._checkin_transport(transport)
        if close_after_release is not None:
            await close_after_release.close()

    @asynccontextmanager
    async def _driver_lease(self, context: RequestContext) -> AsyncGenerator[AsyncDriver]:
        """Keep one transport alive across readiness and operation RPCs."""
        driver, transport, retired = self._acquire_driver(context)
        exchange_key: _AuthExchangeKey | None = None
        if (
            context.has_one_use_auth_token
            and context.auth_generation is not None
            and context.canonical_origin is not None
        ):
            exchange_key = (
                context.canonical_origin,
                context.transport_generation,
                context.auth_generation,
            )
            with self._token_exchange_lock:
                self._active_auth_contexts[exchange_key] = (
                    self._active_auth_contexts.get(exchange_key, 0) + 1
                )
        acquisition_capture = SuppressedExceptionCapture()
        try:
            with acquisition_capture:
                for retired_transport in retired:
                    await retired_transport.close()
            if acquisition_capture.exception is not None:
                return
            yield driver
        finally:
            if exchange_key is not None:
                with self._token_exchange_lock:
                    remaining = self._active_auth_contexts[exchange_key] - 1
                    if remaining:
                        self._active_auth_contexts[exchange_key] = remaining
                    else:
                        self._active_auth_contexts.pop(exchange_key)
                        progress = self._token_exchanges.get(exchange_key)
                        if progress is not None and (
                            progress.error is not None or progress.token is not None
                        ):
                            self._token_exchanges.pop(exchange_key)
            if acquisition_capture.exception is not None:
                await self._release_failed_acquisition(transport, acquisition_capture.exception)
            await self._release_transport(transport)

    async def _request_session_token(
        self,
        context: RequestContext,
        driver: AsyncDriver,
    ) -> str:
        """Request and conditionally publish a token for one captured state."""
        result = await self._invoke(
            driver,
            _RpcInvocation(
                context=context,
                method_name='get_session_token',
                options=_InvocationOptions(),
                params=None,
            ),
        )
        if not isinstance(result, dict):
            raise _errors.ShotgunError('get_session_token did not return a session_id')
        session_id = cast('dict[str, object]', result).get('session_id')
        if not isinstance(session_id, str):
            raise _errors.ShotgunError('get_session_token did not return a session_id')
        config_module.publish_session_token(self._config, context, session_id)
        return session_id

    async def _coalesce_session_token(
        self,
        context: RequestContext,
        driver: AsyncDriver,
    ) -> str:
        """Share one session exchange across one captured auth generation."""
        if context.auth_generation is None or context.canonical_origin is None:
            raise AssertionError('shared session exchange requires shared config state')
        key = (
            context.canonical_origin,
            context.transport_generation,
            context.auth_generation,
        )
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError('session exchange requires a running task')
        while True:
            with self._token_exchange_lock:
                progress = self._token_exchanges.get(key)
                if progress is None:
                    progress = _TokenExchangeProgress(owner, asyncio.Event())
                    self._token_exchanges[key] = progress
                    break
                if progress.error is not None:
                    raise progress.error
                if progress.token is not None:
                    return progress.token
                if progress.owner is owner:
                    raise RuntimeError('session exchange cannot recursively wait on itself')
            await progress.event.wait()

        error: ReadTimeoutError | SendError | asyncio.CancelledError | None = None
        token: str | None = None
        try:
            token = await self._request_session_token(context, driver)
        except (ReadTimeoutError, SendError, asyncio.CancelledError) as exc:
            error = exc
            raise
        finally:
            self._complete_token_exchange(key, progress, error, token)
        if token is None:
            raise AssertionError('session exchange completed without a token')
        return token

    async def _run_info(
        self,
        context: RequestContext,
        driver: AsyncDriver,
        *,
        accept_ready: bool,
    ) -> tuple[dict[str, object], ServerCapabilities]:
        """Own, validate, and conditionally publish one metadata exchange."""
        key = readiness_key(context)
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError('readiness requires a running task')
        while True:
            async with self._readiness_lock:
                cached = self._ready_caps.get(key)
                if accept_ready and cached is not None:
                    return cached.server_info, cached
                progress = self._readiness_in_progress.get(key)
                if progress is None:
                    event = asyncio.Event()
                    self._readiness_in_progress[key] = (owner, event)
                    break
                progress_owner, event = progress
                if progress_owner is owner:
                    raise RuntimeError('readiness cannot recursively wait on itself')
            await event.wait()

        capabilities: ServerCapabilities | None = None
        preserve_cancellation = False
        published = False
        try:
            result = await self._invoke(
                driver,
                _RpcInvocation(
                    context=context,
                    method_name='info',
                    options=_InvocationOptions(include_auth=False),
                    params=None,
                ),
            )
            if not isinstance(result, dict):
                raise _errors.ShotgunError(
                    'The Flow Production Tracking server returned invalid metadata.'
                )
            metadata = cast('dict[str, object]', result)
            server_page_limit(metadata)
            capabilities = ServerCapabilities(server_host(context), metadata)
            published = config_module.publish_server_info(
                self._config, context, capabilities.server_info
            )
        except asyncio.CancelledError:
            preserve_cancellation = True
            raise
        else:
            return metadata, capabilities
        finally:
            finalizer = asyncio.create_task(
                self._finalize_readiness(
                    key,
                    owner,
                    event,
                    capabilities,
                    published,
                )
            )
            await self._join_readiness_finalizer(
                finalizer,
                preserve_cancellation=preserve_cancellation,
            )

    async def _ensure_readiness(
        self,
        context: RequestContext,
        driver: AsyncDriver,
    ) -> ServerCapabilities:
        """Validate one origin while coalescing concurrent callers."""
        _metadata, capabilities = await self._run_info(context, driver, accept_ready=True)
        return capabilities

    async def _get_session_token_for(
        self,
        context: RequestContext,
        driver: AsyncDriver,
    ) -> str:
        """Return a token obtained entirely from one captured request state."""
        if context.session_token is not None:
            return context.session_token
        await self._ensure_readiness(context, driver)
        if context.auth_generation is None or context.canonical_origin is None:
            return await self._request_session_token(context, driver)
        return await self._coalesce_session_token(context, driver)

    async def _authenticated_context(
        self,
        context: RequestContext,
        driver: AsyncDriver,
    ) -> RequestContext:
        """Replace one-use credentials after their ordered token exchange."""
        if not context.has_one_use_auth_token:
            return context
        token = await self._get_session_token_for(context, driver)
        auth = dict(context.auth)
        for field in (
            'auth_token',
            'script_key',
            'script_name',
            'user_login',
            'user_password',
        ):
            auth.pop(field, None)
        auth['session_token'] = token
        extra_auth_params = dict(context.extra_auth_params)
        extra_auth_params.pop('auth_token', None)
        return dataclasses.replace(
            context,
            auth=MappingProxyType(auth),
            extra_auth_params=MappingProxyType(extra_auth_params),
            has_one_use_auth_token=False,
            session_token=token,
        )

    async def _call(
        self,
        method_name: str,
        params: object,
        invocation_options: _InvocationOptions | None = None,
    ) -> object:
        """Run one RPC from one coherent origin and transport snapshot."""
        owned_params = copy.deepcopy(params)
        options = invocation_options or _InvocationOptions()
        context = config_module.snapshot(self._config, options.auth_spec)
        if method_name != 'info':
            require_secure_rpc(context, options.include_auth)
        async with self._driver_lease(context) as driver:
            if method_name == 'info':
                result, _capabilities = await self._run_info(
                    context,
                    driver,
                    accept_ready=False,
                )
                return result
            await self._ensure_readiness(context, driver)
            operation_context = (
                context
                if options.auth_spec is not None
                else await self._authenticated_context(context, driver)
            )
            return await self._invoke(
                driver,
                _RpcInvocation(
                    context=operation_context,
                    method_name=method_name,
                    options=options,
                    params=owned_params,
                ),
            )

    @staticmethod
    async def _share_thumbnail_source(
        driver: AsyncDriver,
        context: RequestContext,
        entities: list[Mapping[str, object]],
        source_entity: Mapping[str, object],
        filmstrip_thumbnail: bool,
    ) -> int:
        """Share one captured source thumbnail through the common executor."""
        fields = _files.add_auth_fields(
            _files.build_share_thumbnail_fields(
                entities,
                source_entity,
                filmstrip_thumbnail,
            ),
            context.auth,
        )
        exchange = _files.build_upload_endpoint_exchange(
            context.canonical_origin,
            context.user_agent,
            _files.SHARE_THUMBNAIL_OPERATION,
            _files.SHARE_THUMBNAIL,
            fields,
        )
        return await _execute_upload_exchange(
            driver,
            context,
            exchange,
            lambda text, _headers: _files.parse_link_completion(text),
        )

    async def _thumbnail_source_ready(
        self,
        driver: AsyncDriver,
        context: RequestContext,
        source_entity_type: str,
        source_entity_id: int,
        field_name: Literal['filmstrip_image', 'image'],
    ) -> bool:
        """Return whether one thumbnail field is no longer transient."""
        params = _crud.build_thumbnail_read_params(
            source_entity_type, source_entity_id, field_name, context.records_per_page
        )
        result = await self._invoke(
            driver,
            _RpcInvocation(
                context=context,
                method_name='read',
                options=_InvocationOptions(),
                params=params,
            ),
        )
        return _files.thumbnail_source_ready(result, field_name)

    @staticmethod
    async def _upload_allocation(
        driver: AsyncDriver,
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
        exchange = _files.build_upload_endpoint_exchange(
            context.canonical_origin,
            context.user_agent,
            _protocol.classify_endpoint('allocation'),
            _files.GET_UPLOAD_LINK_INFO,
            fields,
        )
        return await _execute_upload_exchange(
            driver,
            context,
            exchange,
            lambda text, _headers: _files.UploadLinkInfo(text, multipart=multipart),
        )

    @staticmethod
    async def _upload_direct(
        driver: AsyncDriver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        upload_info: _files.UploadLinkInfo,
    ) -> None:
        """Stream one complete file to stable object storage."""
        exchange = _files.build_storage_upload_exchange(
            context.user_agent,
            _protocol.classify_endpoint('object_put'),
            upload_info.upload_url,
            _files.StorageRange(
                length=prepared.file_size,
                offset=0,
                path=prepared.options.arguments.path,
            ),
            chunk_size=_ASYNC_FILE_BATCH_SIZE,
        )
        await _execute_upload_exchange(
            driver,
            context,
            exchange,
            lambda _text, _headers: None,
        )

    @staticmethod
    async def _upload_link(
        driver: AsyncDriver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
        upload_info: _files.UploadLinkInfo,
    ) -> int:
        """Link one stored object to its receiving entity."""
        fields = _files.add_auth_fields(
            _files.build_link_file_fields(
                _files.build_link_entity_fields(prepared),
                upload_info.encoded,
            ),
            context.auth,
        )
        exchange = _files.build_upload_endpoint_exchange(
            context.canonical_origin,
            context.user_agent,
            _protocol.classify_endpoint('link'),
            _files.LINK_FILE,
            fields,
        )
        return await _execute_upload_exchange(
            driver,
            context,
            exchange,
            lambda text, _headers: _files.parse_link_completion(text),
        )

    @staticmethod
    async def _upload_multipart(
        driver: AsyncDriver,
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
            part_exchange = _files.build_upload_endpoint_exchange(
                context.canonical_origin,
                context.user_agent,
                _protocol.classify_endpoint('allocation'),
                _files.GET_PART_LINK,
                fields,
            )
            part_url = await _execute_upload_exchange(
                driver,
                context,
                part_exchange,
                lambda text, _headers: _files.parse_part_link(text),
            )
            put_exchange = _files.build_storage_upload_exchange(
                context.user_agent,
                _protocol.classify_endpoint('part_put'),
                part_url,
                storage_range,
                chunk_size=_ASYNC_FILE_BATCH_SIZE,
            )
            etag = await _execute_upload_exchange(
                driver,
                context,
                put_exchange,
                _files.parse_part_etag,
            )
            etags.append(etag)

        fields = _files.add_auth_fields(
            _files.build_complete_fields(upload_info, prepared.filename, etags),
            context.auth,
        )
        complete_exchange = _files.build_upload_endpoint_exchange(
            context.canonical_origin,
            context.user_agent,
            _protocol.classify_endpoint('finalize'),
            _files.COMPLETE_MULTIPART,
            fields,
        )
        await _execute_upload_exchange(
            driver,
            context,
            complete_exchange,
            lambda text, _headers: _files.parse_multipart_completion(text),
        )

    async def _upload_for(
        self,
        driver: AsyncDriver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
    ) -> int:
        """Execute one prepared upload through a captured driver lease."""
        arguments = prepared.options.arguments
        capabilities = await self._ensure_readiness(context, driver)
        operation_context = await self._authenticated_context(context, driver)
        upload_path = _files.require_supported_upload_path(
            _files.choose_upload_path(
                capabilities.server_info,
                arguments.entity_type,
                arguments.field_name,
                prepared.file_size,
            )
        )
        multipart = upload_path == 'multipart'
        upload_info = await self._upload_allocation(
            driver,
            operation_context,
            prepared,
            multipart,
        )
        if multipart:
            await self._upload_multipart(
                driver,
                operation_context,
                prepared,
                upload_info,
            )
        else:
            await self._upload_direct(driver, operation_context, prepared, upload_info)
        return await self._upload_link(
            driver,
            operation_context,
            prepared,
            upload_info,
        )

    async def _upload(self, options: _files.UploadOptions) -> int:
        """Execute one evidence-backed upload workflow."""
        prepared = await asyncio.to_thread(_files.prepare_upload, options)
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        async with self._driver_lease(context) as driver:
            return await self._upload_for(driver, context, prepared)

    async def _upload_thumbnail_for(
        self,
        driver: AsyncDriver,
        context: RequestContext,
        prepared: _files.PreparedUpload,
    ) -> int:
        """Execute the exact direct-storage thumbnail workflow."""
        capabilities = await self._ensure_readiness(context, driver)
        arguments = prepared.options.arguments
        _files.require_supported_upload_path(
            _files.choose_upload_path(
                capabilities.server_info,
                arguments.entity_type,
                arguments.field_name,
                prepared.file_size,
            )
        )
        upload_info = await self._upload_allocation(
            driver,
            context,
            prepared,
            multipart=False,
        )
        await self._upload_direct(driver, context, prepared, upload_info)
        return await self._upload_link(driver, context, prepared, upload_info)

    async def _upload_thumbnails_after_write(
        self,
        entity_type: str,
        entity_id: int,
        thumbnails: dict[str, object],
        result: dict[str, object],
    ) -> dict[str, object]:
        """Upload separated thumbnail paths and merge their current field values."""
        if not thumbnails:
            return result
        for field_name, path in thumbnails.items():
            upload = (
                self.upload_filmstrip_thumbnail
                if field_name == 'filmstrip_image'
                else self.upload_thumbnail
            )
            await upload(entity_type, entity_id, cast('str', path))
        current = await self.find_one(entity_type, [['id', 'is', entity_id]], list(thumbnails))
        if current is not None:
            result.update({field_name: current.get(field_name) for field_name in thumbnails})
        return result

    @overload
    async def activity_stream_read(self, entity_type: Never) -> dict[str, object]: ...

    @overload
    async def activity_stream_read(
        self,
        entity_type: str,
        entity_id: int,
        entity_fields: dict[str, list[str]] | None = None,
        min_id: int | None = None,
        max_id: int | None = None,
        limit: int | None = None,
    ) -> dict[str, object]: ...

    @_methods.expose_activity_stream_read_signature
    async def activity_stream_read(
        self,
        *args: object,
        **kwargs: object,
    ) -> dict[str, object]:
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
        options = _methods.bind_activity_stream_read(args, kwargs, 'AsyncShotgun')
        result = await self._call(
            'activity_stream',
            _methods.build_activity_stream_read_params(options),
            _InvocationOptions(logical_method_name='activity_stream_read'),
        )
        return _crud.require_dictionary_result(result, 'activity_stream_read')

    def add_user_agent(self, agent: str) -> None:
        """Append a user-agent fragment.

        Args:
            agent: User-agent fragment visible to subsequent requests.
        """
        self._config.append_user_agent(agent)

    async def authenticate_human_user(
        self,
        user_login: str,
        user_password: str,
        auth_token: str | None = None,
    ) -> dict[str, object] | None:
        """Authenticate a human user without changing client authentication.

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
            result = await self._call(
                'read',
                plan.params,
                _InvocationOptions(
                    auth_spec=plan.auth_spec,
                    logical_method_name='authenticate_human_user',
                ),
            )
        except _errors.AuthenticationFault:
            return None
        return _methods.require_human_authentication_result(result)

    @property
    def base_url(self) -> str:
        """Return the constructor's normalized base URL.

        Returns:
            The site URL without a trailing slash.
        """
        return self._base_url

    async def batch(self, requests: list[dict[str, Any]]) -> list[object]:
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
        return _crud.require_list_result(await self._call('batch', calls), 'batch')

    @property
    def client_caps(self) -> ClientCapabilities:
        """Return capabilities of this Python client.

        Returns:
            Local platform capability metadata.
        """
        return self._client_caps

    async def _find(self, options: _crud.FindOptions) -> list[dict[str, object]]:
        """Run one paged read operation from one immutable context."""
        prepared = _crud.prepare_find(options)
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        records: list[dict[str, object]] = []

        async with self._driver_lease(context) as driver:
            capabilities = await self._ensure_readiness(context, driver)
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
            operation_context = await self._authenticated_context(
                operation_context,
                driver,
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
                async for raw_result in pages:
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
                await pages.aclose()
        return _crud.finalize_find(
            records,
            options,
            plan,
            self.client_caps.local_path_field,
        )

    async def close(self) -> None:
        """Swap out and drain the current transport without aborting work."""
        close_after_unlock: list[AsyncTransport] = []
        with self._transport_lock:
            advance_transport_generation(self._config)
            if self._known_transport is not self._transport:
                if self._known_transport is not None:
                    self._retired_transports.pop(id(self._known_transport), None)
                    close_after_unlock.append(self._known_transport)
                self._known_transport = self._transport
            transport = self._transport
            self._driver = None
            self._driver_generation = None
            self._known_transport = None
            self._transport = None
            self._transport_owned = False
            if transport is not None:
                self._retired_transports.pop(id(transport), None)
                if transport not in close_after_unlock:
                    close_after_unlock.append(transport)
        pending_closes = iter(close_after_unlock)
        try:
            for retired_transport in pending_closes:
                await retired_transport.close()
        except BaseException:
            for retired_transport in pending_closes:
                with suppress(BaseException):
                    await retired_transport.close()
            raise
        async with self._readiness_lock:
            current_key = config_module.readiness_identity(self._config)
            current = self._ready_caps.get(current_key) if current_key is not None else None
            self._ready_caps.clear()
            if current_key is not None and current is not None:
                self._ready_caps[current_key] = current

    async def _close_failed_context_entry(self, entry_error: BaseException) -> Never:
        """Close after failed eager entry and raise the selected primary outcome."""
        cancellation: asyncio.CancelledError | None = None
        current = asyncio.current_task()
        if isinstance(entry_error, asyncio.CancelledError):
            cancellation = entry_error
            if current is not None and current.cancelling() > 0:
                current.uncancel()

        cleanup = asyncio.create_task(self.close())
        while not cleanup.done():
            wait_capture = SuppressedExceptionCapture()
            with wait_capture:
                await asyncio.shield(cleanup)
            error = wait_capture.exception
            if (
                isinstance(error, asyncio.CancelledError)
                and current is not None
                and current.cancelling() > 0
            ):
                if cancellation is None:
                    cancellation = error
                current.uncancel()

        cleanup_capture = SuppressedExceptionCapture()
        with cleanup_capture:
            cleanup.result()
        if cancellation is not None:
            raise cancellation
        raise entry_error

    @property
    def config(self) -> Config:
        """Return the writable client configuration.

        Returns:
            The locked client configuration object.
        """
        return self._config

    async def connect(self) -> None:
        """Validate readiness and complete deferred one-use authentication.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        context = config_module.snapshot(self._config)
        if context.has_one_use_auth_token:
            require_secure_rpc(context, include_auth=True)
        async with self._driver_lease(context) as driver:
            await self._ensure_readiness(context, driver)
            if context.has_one_use_auth_token:
                await self._get_session_token_for(context, driver)

    async def create(
        self,
        entity_type: str,
        data: dict[str, Any],
        return_fields: list[str] | None = None,
    ) -> dict[str, object]:
        """Create an entity with the supplied field values.

        Non-empty `image` and `filmstrip_image` values are file paths. They are
        uploaded as thumbnails after the entity is created, and the result
        includes their current field values.

        Args:
            entity_type: Entity type to create.
            data: Field values for the new entity.
            return_fields: Fields to return in addition to the entity identity.

        Returns:
            The created entity data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                if `data`, its field names, `return_fields`, or the server
                result is invalid, or if a thumbnail path is not a nonempty
                file. A thumbnail failure occurs after the entity is created.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        field_data, thumbnails = _files.split_thumbnail_uploads(data, keep_image_none=False)
        params = _crud.build_create_params(entity_type, field_data, return_fields)
        result = _crud.require_dictionary_result(await self._call('create', params), 'create')
        if not thumbnails:
            return result
        entity_id = _crud.require_entity_id(result.get('id'), 'create did not return an entity id')
        return await self._upload_thumbnails_after_write(
            entity_type, entity_id, thumbnails, result
        )

    async def delete(self, entity_type: str, entity_id: int) -> bool:
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
        params = _crud.build_entity_action_params(entity_type, entity_id)
        return _crud.require_boolean_result(await self._call('delete', params), 'delete')

    @overload
    async def download_attachment(
        self,
        attachment: int | Mapping[str, object] | None,
        file_path: str | None = None,
        attachment_id: int | None = None,
    ) -> bytes | str | None: ...

    @overload
    async def download_attachment(
        self,
        *,
        file_path: str | None = None,
        attachment_id: int,
    ) -> bytes | str | None: ...

    async def download_attachment(
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
            async with self._driver_lease(context) as driver:
                headers = {'user-agent': context.user_agent}
                if use_session_cookie:
                    if context.session_token is None:
                        token = await self._get_session_token_for(context, driver)
                    else:
                        await self._ensure_readiness(context, driver)
                        token = context.session_token
                    headers['cookie'] = f'_session_id={token}'
                exchange = _AsyncDownloadExchange(
                    context=context,
                    file_path=file_path,
                    headers=headers,
                    url=url,
                )
                return await exchange._execute(driver, operation)
        except _errors.ShotgunFileDownloadError:
            raise
        except (TransportError, _errors.ProtocolError, OSError):
            raise _errors.ShotgunFileDownloadError('attachment download failed') from None

    async def entity_types(self) -> Never:
        """Reject the deprecated alias of `schema_entity_read()` without I/O.

        Raises:
            ShotgunError: Always, naming `schema_entity_read()` as the
                replacement.
        """
        raise _methods.entity_types_alias_error()

    @overload
    async def export_page(
        self,
        page_id: int,
        page_format: str,
        /,
        layout_name: str | None = None,
    ) -> str: ...

    @overload
    async def export_page(
        self,
        page_id: int,
        **kwargs: Unpack[_methods.ExportPageKeywordArguments],
    ) -> str: ...

    @_methods.expose_export_page_signature
    async def export_page(self, *args: object, **kwargs: object) -> str:
        """Export a page using the documented arguments.

        The Page or its layout must be marked API Exportable in the web UI, or
        the server rejects the export.

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
        options = _methods.bind_export_page(args, kwargs, 'AsyncShotgun')
        result = await self._call(
            'export_page',
            _methods.build_export_page_params(options),
        )
        return _methods.require_string_result(result, 'export_page')

    @overload
    async def find(self, entity_type: Never) -> list[dict[str, object]]: ...

    @overload
    async def find(
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
    async def find(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
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
        return await self._find(_crud.bind_find(args, kwargs, 'AsyncShotgun'))

    @overload
    async def find_one(self, entity_type: Never) -> dict[str, object] | None: ...

    @overload
    async def find_one(
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
    async def find_one(self, *args: object, **kwargs: object) -> dict[str, object] | None:
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
        options = _crud.bind_find_one(args, kwargs, 'AsyncShotgun')
        return _crud.first_result(await self._find(_crud.find_options_from_one(options)))

    async def follow(
        self,
        user: dict[str, Any],
        entity: dict[str, Any],
    ) -> dict[str, object]:
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
        result = await self._call('follow', _methods.build_follow_params(user, entity))
        return _crud.require_dictionary_result(result, 'follow')

    async def followers(self, entity: dict[str, Any]) -> list[dict[str, object]]:
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
        result = await self._call('followers', _methods.build_followers_params(entity))
        return _methods.require_dictionary_list_result(result, 'followers')

    async def following(
        self,
        user: dict[str, Any],
        project: dict[str, Any] | None = None,
        entity_type: str | None = None,
    ) -> list[dict[str, object]]:
        """List entities a user follows with optional restrictions.

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
        result = await self._call('following', params)
        return _methods.require_dictionary_list_result(result, 'following')

    async def get_attachment_download_url(
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

    async def get_auth_cookie_handler(self) -> urllib.request.HTTPCookieProcessor:
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
        async with self._driver_lease(context) as driver:
            token = await self._get_session_token_for(context, driver)
        return _files.build_auth_cookie_handler(token, origin)

    async def get_session_token(self) -> str:
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
        async with self._driver_lease(context) as driver:
            return await self._get_session_token_for(context, driver)

    async def info(self) -> dict[str, object]:
        """Fetch and publish unauthenticated server metadata.

        Returns:
            Raw server metadata after version-floor validation.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If server parameters are missing or the configured
                origin lacks a valid host.
        """
        result = await self._call('info', None, _InvocationOptions(include_auth=False))
        return cast('dict[str, object]', result)

    async def nav_expand(
        self,
        path: str,
        seed_entity_field: str | None = None,
        entity_fields: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        """Expand one level of the navigation hierarchy at `path`.

        Args:
            path: Navigation path to expand.
            seed_entity_field: Optional field used to seed the path.
            entity_fields: Optional fields for linked entity types.

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
            await self._call('nav_expand', params),
            'nav_expand',
        )

    async def nav_search_entity(
        self,
        root_path: str,
        entity: dict[str, Any],
        seed_entity_field: str | None = None,
    ) -> list[dict[str, object]]:
        """Search the navigation hierarchy for an entity.

        Args:
            root_path: Navigation path under which to search.
            entity: Entity to find.
            seed_entity_field: Optional field used to seed the path.

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
        result = await self._call(
            'nav_search',
            params,
            _InvocationOptions(logical_method_name='nav_search_entity'),
        )
        return _methods.require_dictionary_list_result(result, 'nav_search_entity')

    async def nav_search_string(
        self,
        root_path: str,
        search_string: str,
        seed_entity_field: str | None = None,
    ) -> list[dict[str, object]]:
        """Search the navigation hierarchy by string.

        Args:
            root_path: Navigation path under which to search.
            search_string: String to match.
            seed_entity_field: Optional field used to seed the path.

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
        result = await self._call(
            'nav_search',
            params,
            _InvocationOptions(logical_method_name='nav_search_string'),
        )
        return _methods.require_dictionary_list_result(result, 'nav_search_string')

    async def note_thread_read(
        self,
        note_id: int,
        entity_fields: dict[str, list[str]] | None = None,
    ) -> list[dict[str, object]]:
        """Read a note and its replies and attachments as a flat thread.

        Args:
            note_id: Numeric identifier of the note.
            entity_fields: Optional fields for linked entity types.

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
        result = await self._call(
            'note_thread_contents',
            params,
            _InvocationOptions(logical_method_name='note_thread_read'),
        )
        return _methods.require_dictionary_list_result(result, 'note_thread_read')

    async def preferences_read(self, prefs: list[str] | None = None) -> dict[str, object]:
        """Read site preferences, optionally restricted by name.

        Args:
            prefs: Preference names to read, or `None` for all defaults.

        Returns:
            Preference values returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result has the wrong type.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_preferences_read_params(prefs)
        result = await self._call('preferences_read', params)
        return _crud.require_dictionary_result(result, 'preferences_read')

    def reset_user_agent(self) -> None:
        """Restore this client's shared default user-agent fragments."""
        self._config.reset_user_agent()

    async def revive(self, entity_type: str, entity_id: int) -> bool:
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
        params = _crud.build_entity_action_params(entity_type, entity_id)
        return _crud.require_boolean_result(await self._call('revive', params), 'revive')

    async def schema(self, entity_type: str) -> Never:
        """Reject the deprecated alias of `schema_field_read()` without I/O.

        Args:
            entity_type: Entity type named in the replacement suggestion.

        Raises:
            ShotgunError: Always, naming the equivalent `schema_field_read()`
                call.
        """
        raise _methods.schema_alias_error(entity_type)

    async def schema_entity_read(
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
        result = await self._call(
            'schema_entity_read',
            _methods.build_schema_entity_read_params(project_entity),
        )
        return _crud.require_dictionary_result(result, 'schema_entity_read')

    async def schema_field_create(
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
        params = _methods.build_schema_field_create_params(
            entity_type,
            data_type,
            display_name,
            properties,
        )
        result = await self._call('schema_field_create', params)
        return _methods.require_string_result(result, 'schema_field_create')

    async def schema_field_delete(self, entity_type: str, field_name: str) -> bool:
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
        params = _methods.build_schema_field_delete_params(entity_type, field_name)
        result = await self._call('schema_field_delete', params)
        return _crud.require_boolean_result(result, 'schema_field_delete')

    async def schema_field_read(
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
        result = await self._call(
            'schema_field_read',
            _methods.build_schema_field_read_params(
                entity_type,
                field_name,
                project_entity,
            ),
        )
        return _crud.require_dictionary_result(result, 'schema_field_read')

    async def schema_field_update(
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
        params = _methods.build_schema_field_update_params(
            entity_type,
            field_name,
            properties,
            project_entity,
        )
        result = await self._call('schema_field_update', params)
        return _crud.require_boolean_result(result, 'schema_field_update')

    async def schema_read(
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
        result = await self._call('schema_read', params)
        return _crud.require_dictionary_result(result, 'schema_read')

    @property
    def server_caps(self) -> ServerCapabilities:
        """Return cached capabilities for the configured origin.

        Returns:
            Server capability metadata at or above the supported floor.

        Raises:
            ShotgunError: If no awaitable operation has populated the cache.
        """
        capabilities = self._ready_caps.get(readiness_key(config_module.snapshot(self._config)))
        if capabilities is None:
            raise _errors.ShotgunError(
                'server_caps is not populated yet on AsyncShotgun: '
                'await connect() or any RPC first'
            )
        return capabilities

    @property
    def server_info(self) -> dict[str, object]:
        """Return cached raw metadata for the configured origin.

        Returns:
            The metadata that populated `server_caps`.

        Raises:
            ShotgunError: If no awaitable operation has populated the cache.
        """
        return self.server_caps.server_info

    def set_session_uuid(self, session_uuid: str) -> None:
        """Set the browser session UUID for subsequent requests.

        Args:
            session_uuid: Browser session UUID to publish.
        """
        self._config.set_session_uuid(session_uuid)

    async def share_thumbnail(
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
        remaining targets. A local thumbnail is rejected before any upload
        request if the server requires legacy form uploads.

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
                or if the source selection, entities, or transport fails, or a
                local upload requires the legacy form route.
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
            raise TypeError("AsyncShotgun.upload() got multiple values for argument 'field_name'")
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
            prepared = await asyncio.to_thread(
                _files.prepare_upload,
                _files.UploadOptions(
                    arguments=arguments,
                    upload_type='Thumbnail',
                    filmstrip=filmstrip_thumbnail,
                ),
            )
        context = config_module.snapshot(self._config)
        require_secure_rpc(context, include_auth=True)
        async with self._driver_lease(context) as driver:
            await self._ensure_readiness(context, driver)
            operation_context = await self._authenticated_context(context, driver)
            if prepared is not None:
                attachment_id = await self._upload_thumbnail_for(
                    driver,
                    operation_context,
                    prepared,
                )
                if len(share.entities) == 1:
                    return attachment_id
                field_name: Literal['filmstrip_image', 'image'] = (
                    'filmstrip_image' if filmstrip_thumbnail else 'image'
                )
                ready = await driver.poll(
                    lambda: self._thumbnail_source_ready(
                        driver,
                        operation_context,
                        share.source_entity_type,
                        share.source_entity_id,
                        field_name,
                    ),
                    attempts=25,
                    interval=operation_context.rpc_attempt_interval / 1000.0,
                )
                if not ready:
                    raise _errors.ShotgunThumbnailNotReady('thumbnail source did not become ready')
                return await self._share_thumbnail_source(
                    driver,
                    operation_context,
                    share.entities[1:],
                    share.source_entity,
                    filmstrip_thumbnail,
                )
            return await self._share_thumbnail_source(
                driver,
                operation_context,
                share.entities,
                share.source_entity,
                filmstrip_thumbnail,
            )

    @overload
    async def summarize(self, entity_type: Never) -> dict[str, object]: ...

    @overload
    async def summarize(
        self,
        entity_type: str,
        filters: list[Any] | tuple[Any, ...] | dict[str, Any],
        summary_fields: list[dict[str, Any]],
        filter_operator: str | None = None,
        grouping: list[dict[str, Any]] | None = None,
        include_archived_projects: bool = True,
    ) -> dict[str, object]: ...

    @_methods.expose_summarize_signature
    async def summarize(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Summarize field data returned by a query.

        Args:
            *args: Positional values from the documented `summarize` signature.
            **kwargs: Keyword values from the documented `summarize` signature.

        Returns:
            Dict with `summaries` and optional `groups`.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if arguments or the server result are invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_summarize(args, kwargs, 'AsyncShotgun')
        params = _methods.build_summarize_params(options)
        result = await self._call('summarize', params)
        return _crud.require_dictionary_result(result, 'summarize')

    async def text_search(
        self,
        text: str,
        entity_types: dict[str, Any],
        project_ids: list[int] | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        """Search entity display names matching `text`.

        Args:
            text: Text to match.
            entity_types: Mapping of entity type to filters.
            project_ids: Optional project identifier allowlist.
            limit: Maximum results to return.

        Returns:
            Match data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if arguments or the server result are invalid.
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
        result = await self._call(
            'query_display_name_cache',
            params,
            _InvocationOptions(logical_method_name='text_search'),
        )
        return _crud.require_dictionary_result(result, 'text_search')

    async def unfollow(
        self,
        user: dict[str, Any],
        entity: dict[str, Any],
    ) -> dict[str, object]:
        """Make a user stop following an entity.

        Args:
            user: User entity that will stop following the target.
            entity: Entity to stop following.

        Returns:
            Unfollow result returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if an argument or the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        result = await self._call('unfollow', _methods.build_follow_params(user, entity))
        return _crud.require_dictionary_result(result, 'unfollow')

    async def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, Any],
        multi_entity_update_modes: dict[str, str] | None = None,
    ) -> dict[str, object]:
        """Update fields on an existing entity.

        Non-empty `image` and `filmstrip_image` values are file paths. They are
        uploaded as thumbnails after the other fields are updated, and the
        result includes their current field values. An `image` of `None` clears
        both thumbnails. When no other fields remain, no update request is sent
        and the result contains only `id` and `type`.

        Args:
            entity_type: Entity type to update.
            entity_id: Numeric entity identifier.
            data: Replacement field values.
            multi_entity_update_modes: Per-field multi-entity update modes.

        Returns:
            The updated entity data returned by the server.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                if `data`, its field names, `multi_entity_update_modes`, or the
                server result is invalid, or if a thumbnail path is not a
                nonempty file. A thumbnail failure occurs after the update.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        field_data, thumbnails = _files.split_thumbnail_uploads(data, keep_image_none=True)
        params = _crud.build_update_params(
            entity_type,
            entity_id,
            field_data,
            multi_entity_update_modes,
        )
        result: dict[str, object] = {'id': entity_id, 'type': entity_type}
        if params['fields']:
            result = _crud.require_dictionary_result(
                await self._call('update', params),
                'update',
            )
        return await self._upload_thumbnails_after_write(
            entity_type, entity_id, thumbnails, result
        )

    async def update_project_last_accessed(
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
        await self._call(
            'update_project_last_accessed_by_current_user',
            _methods.build_update_project_last_accessed_params(project, user),
            _InvocationOptions(logical_method_name='update_project_last_accessed'),
        )

    @overload
    async def upload(self, entity_type: Never) -> int: ...

    @overload
    async def upload(
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
    async def upload(self, *args: object, **kwargs: object) -> int:
        """Upload a file or thumbnail and return its Attachment ID.

        A server requiring legacy form uploads raises `ShotgunError` before any
        upload request is sent. Server capability reads may occur.

        Args:
            *args: Positional arguments from the documented upload signature.
            **kwargs: Keyword arguments from the documented upload signature.

        Returns:
            Created Attachment ID.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If the file or an upload response is invalid, an RPC
                fault or terminal transport failure occurs, or the server
                requires legacy form uploads.
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
        return await self._upload(
            _files.UploadOptions(
                arguments=arguments,
                upload_type=upload_type,
                filmstrip=arguments.field_name in {'filmstrip_image', 'filmstrip_thumb_image'},
            )
        )

    async def upload_filmstrip_thumbnail(
        self,
        entity_type: str,
        entity_id: int,
        path: str,
        **kwargs: Unpack[_files.ThumbnailUploadKeywordArguments],
    ) -> int:
        """Upload a filmstrip thumbnail through `upload`.

        Servers requiring legacy form uploads are rejected before any upload
        request is sent.

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
                or if the file, upload response, or transport fails, or the
                server requires legacy form uploads.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return await self.upload(
            entity_type,
            entity_id,
            path,
            field_name='filmstrip_thumb_image',
            **kwargs,
        )

    async def upload_thumbnail(
        self,
        entity_type: str,
        entity_id: int,
        path: str,
        **kwargs: Unpack[_files.ThumbnailUploadKeywordArguments],
    ) -> int:
        """Upload a static thumbnail through `upload`.

        Servers requiring legacy form uploads are rejected before any upload
        request is sent.

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
                or if the file, upload response, or transport fails, or the
                server requires legacy form uploads.
            ProtocolError: If an upload endpoint returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        return await self.upload(
            entity_type,
            entity_id,
            path,
            field_name='thumb_image',
            **kwargs,
        )

    async def user_subscriptions_create(self, users: list[dict[str, Any]]) -> bool:
        """Assign subscriptions to users.

        The server accepts this call from scripts and from human users with
        permission to manage subscriptions, such as members of the Admin
        permission group. It refuses other human users.

        Args:
            users: Subscription dictionaries with a string or integer
                `humanUserId`; integers become decimal string keys and booleans
                are not accepted. Each user may appear only once.
                `subscription` is `None`, a string, or a list of strings.
                Lists are copied in order. Other entry keys are ignored.

        Returns:
            `True` when every per-user report is `None`, meaning the server
            reported no problem; `False` when any user has a non-`None` report.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if `users` contains invalid or duplicate entries, or the
                server result is not a mapping with string keys.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        params = _methods.build_user_subscriptions_create_params(users)
        result = await self._call('user_subscriptions_create', params)
        return _methods.require_user_subscriptions_create_result(result)

    async def user_subscriptions_read(self) -> list[dict[str, object]]:
        """List user subscriptions in server order.

        Returns:
            Subscription entries in server order, each with an unchanged string
            `humanUserId` and a `subscription` of `None`, a string, or a copied
            list of strings.

        Raises:
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if the server result is invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        result = await self._call('user_subscriptions_read', None)
        return _methods.require_user_subscriptions_result(result)

    async def work_schedule_read(
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
        result = await self._call('work_schedule_read', params)
        return _crud.require_dictionary_result(result, 'work_schedule_read')

    @overload
    async def work_schedule_update(self, date: Never) -> dict[str, object]: ...

    @overload
    async def work_schedule_update(
        self,
        date: _dt_module.date | str,
        working: bool,
        description: str | None = None,
        project: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
        recalculate_field: str | None = None,
    ) -> dict[str, object]: ...

    @_methods.expose_work_schedule_update_signature
    async def work_schedule_update(
        self,
        *args: object,
        **kwargs: object,
    ) -> dict[str, object]:
        """Update a work schedule using the documented arguments.

        Args:
            *args: Positional values from the documented public signature.
            **kwargs: Keyword values from the documented public signature.

        Returns:
            Updated work schedule data returned by the server.

        Raises:
            TypeError: If arguments do not match the documented signature.
            ShotgunError: If an RPC fault or terminal transport failure occurs,
                or if arguments or the server result are invalid.
            ProtocolError: If a server request returns an HTTP error.
            ValueError: If authenticated HTTP violates the configured origin
                policy.
        """
        options = _methods.bind_work_schedule_update(args, kwargs, 'AsyncShotgun')
        params = _methods.build_work_schedule_update_params(options)
        result = await self._call('work_schedule_update', params)
        return _crud.require_dictionary_result(result, 'work_schedule_update')


def _open_download_temporary(destination: Path) -> tuple[ExitStack, BinaryIO, Path]:
    """Open one adjacent download temporary file."""
    stack = ExitStack()
    handle = stack.enter_context(_files.download_temporary(destination))
    return stack, handle, Path(handle.name)


def _raise_download_cleanup_error() -> Never:
    """Raise the internal completed-transfer cleanup marker."""
    raise _AsyncDownloadCleanupError


async def _single_chunk(content: bytes) -> AsyncIterator[bytes]:
    """Yield one buffered response body through async consumption control."""
    yield content


async def _execute_upload_exchange(
    driver: AsyncDriver,
    context: RequestContext,
    exchange: _files.UploadExchange,
    parser: Callable[[str, Mapping[str, str]], _UploadResult],
) -> _UploadResult:
    """Run one upload exchange through the common async executor."""

    def request_factory(_attempt: int) -> TransportRequest:
        return TransportRequest(
            method=exchange.method,
            url=exchange.url,
            headers=dict(exchange.headers),
            body=exchange.body_factory(),
            timeout=context.timeout,
            token=uuid.uuid4().hex,
        )

    async def consume(
        response: AsyncLiveResponse,
        control: AsyncAttemptControl,
    ) -> _UploadResult:
        buffered = cast('TransportResponse', response)
        _protocol.check_http_status(
            buffered.status,
            buffered.reason,
            context.canonical_origin or '',
            buffered.headers,
            buffered.content,
        )
        body = await control.consume(
            _single_chunk(buffered.content),
            total_bytes=len(buffered.content),
        )
        return parser(body.decode('utf-8', errors='replace'), buffered.headers)

    return await driver.execute(
        exchange.operation,
        context,
        request_factory,
        consume,
    )
