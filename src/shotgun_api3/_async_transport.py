"""Async transport vocabulary and streaming response handling."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from typing import Protocol

from ._transport import (
    CaseInsensitiveHeaders,
    TransportError,
    TransportRequest,
    TransportResponse,
)


class AsyncStreamingResponse:
    """A response whose body is consumed incrementally exactly once.

    Fully consuming the body releases response resources. Call `await close()`
    when leaving the body unconsumed or stopping early. Explicit closure is
    idempotent.

    Attributes:
        status: HTTP status code.
        reason: HTTP reason phrase.
        headers: Response headers.
    """

    def __init__(
        self,
        status: int,
        reason: str,
        headers: CaseInsensitiveHeaders,
        stream: Callable[[int], AsyncIterator[bytes]],
        close_fn: Callable[[], Awaitable[None]],
    ) -> None:
        """Initialize a one-use asynchronous streaming response.

        Args:
            status: HTTP status code.
            reason: HTTP reason phrase.
            headers: Response headers.
            stream: Factory accepting a chunk size and returning the body
                iterator.
            close_fn: Awaitable callback that releases response resources.
        """
        self.status = status
        self.reason = reason
        self.headers = headers
        self._stream = stream
        self._close_fn: Callable[[], Awaitable[None]] | None = close_fn
        self._claimed = False

    def _require_open(self) -> None:
        """Raise when response resources were already released."""
        if self._close_fn is None:
            raise TransportError('streaming response is closed')

    async def close(self) -> None:
        """Release resources associated with the streaming response."""
        close_fn = self._close_fn
        self._close_fn = None
        if close_fn is not None:
            await close_fn()

    async def _iterate(self, size: int) -> AsyncIterator[bytes]:
        """Yield chunks while closing on every terminal iterator path."""
        try:
            async for chunk in self._stream(size):
                self._require_open()
                yield chunk
        except BaseException:
            with suppress(BaseException):
                await self.close()
            raise
        else:
            await self.close()

    def aiter_chunks(self, size: int = 65536) -> AsyncIterator[bytes]:
        """Claim and return the single-use response-body iterator.

        Args:
            size: Maximum bytes per yielded chunk.

        Returns:
            An async iterator over the response body.

        Raises:
            TransportError: If `size` is not positive, the response is closed,
                or its stream has already been claimed.
        """
        if size <= 0:
            raise TransportError('streaming chunk size must be positive')
        self._require_open()
        if self._claimed:
            raise TransportError('streaming response was already consumed')
        self._claimed = True
        return self._iterate(size)


class AsyncTransport(Protocol):
    """HTTP backend operations required by the async driver.

    Task cancellation replaces the sync transport's `abort` operation. The
    synchronous lease pair brackets one complete logical RPC so draining close
    operations cannot interrupt a retry loop between attempts.
    """

    def checkin(self) -> None:
        """Release one logical RPC lease."""
        ...

    def checkout(self) -> None:
        """Acquire one logical RPC lease."""
        ...

    async def close(self) -> None:
        """Release resources owned by the transport."""
        ...

    def reserve(self, task: asyncio.Task[object]) -> None:
        """Retain one child task until its done callback runs.

        Args:
            task: Child task whose scheduled work must survive close.

        Raises:
            TransportError: If close rejects the reserving owner or `task` is
                already reserved.
        """
        ...

    async def send(self, request: TransportRequest) -> TransportResponse:
        """Send one request and return its complete response.

        Args:
            request: Transport request to send.

        Returns:
            The complete transport response.

        Raises:
            TransportError: If the transport operation fails.
        """
        ...

    async def send_streaming(
        self,
        request: TransportRequest,
    ) -> AsyncStreamingResponse:
        """Send one request and return its single-use streaming response.

        Args:
            request: Transport request to send.

        Returns:
            The streaming transport response.

        Raises:
            TransportError: If the transport operation fails.
        """
        ...
