"""Decode in-memory RPC requests and encode server responses."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from urllib.parse import urlsplit

from shotgun_api3._transport import TransportRequest, TransportResponse

from ._errors import MockgunError, ServerFaultError

_AUTH_KEYS = frozenset(
    {'script_name', 'user_login', 'session_token', 'auth_token', 'sudo_as_login'}
)


@dataclass(frozen=True)
class RpcCall:
    """An RPC method with separated authentication and method parameters.

    Attributes:
        method_name: Server method to dispatch.
        auth: Authentication dictionary, or `None` for an anonymous call.
        params: Method payload, or `None` when omitted.
    """

    method_name: str
    auth: dict[str, object] | None
    params: object


def decode_call(request: TransportRequest) -> RpcCall:
    """Decode a JSON POST to an RPC path, including an optional site prefix.

    Raises:
        MockgunError: If the method, path, body, or call envelope is
            unsupported.
    """
    path = urlsplit(request.url).path
    message = f'mockgun does not emulate {request.method} {path}'
    if (
        request.method != 'POST'
        or not path.endswith('/api3/json')
        or not isinstance(request.body, bytes)
    ):
        raise MockgunError(message)
    try:
        body: object = json.loads(request.body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise MockgunError(message) from exc
    if not isinstance(body, dict):
        raise MockgunError(message)
    envelope = cast('dict[str, object]', body)
    method = envelope.get('method_name')
    values = envelope.get('params')
    if not isinstance(method, str) or not isinstance(values, list):
        raise MockgunError(message)
    params = cast('list[object]', values).copy()
    auth = None
    if params:
        first = params[0]
        if isinstance(first, dict) and _AUTH_KEYS.intersection(cast('dict[str, object]', first)):
            auth = cast('dict[str, object]', params.pop(0))
    if len(params) > 1:
        raise MockgunError(message)
    return RpcCall(method, auth, params[0] if params else None)


def format_wire_datetime(value: datetime) -> str:
    """Format a datetime as UTC with whole-second wire precision.

    Args:
        value: Datetime to convert; a naive value is interpreted in the
            system-local timezone.

    Returns:
        A UTC date and time separated by `T` and ending in `Z`, without
        microseconds. Year padding follows the platform
        `strftime("%Y-%m-%dT%H:%M:%SZ")` behavior.

    Raises:
        OverflowError: If UTC conversion exceeds the supported datetime range.
        ValueError: If the platform cannot convert a naive datetime.
        OSError: If local timezone conversion fails.
    """
    return value.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_wire_datetime(value: str) -> datetime:
    """Parse a wire timestamp into an aware UTC datetime.

    Args:
        value: Timestamp to parse using `datetime.fromisoformat`. A timestamp
            without an offset is interpreted as UTC.

    Raises:
        ValueError: If the timestamp cannot be parsed.
        OverflowError: If converting its offset to UTC exceeds the supported
            datetime range.
    """
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def result_response(result: object, *, wrapped: bool) -> TransportResponse:
    """Encode a result as JSON, optionally nested under `results`."""
    return TransportResponse(
        status=200,
        reason='OK',
        headers={'content-type': 'application/json'},
        content=json.dumps({'results': result} if wrapped else result).encode(),
    )


def fault_response(fault: ServerFaultError) -> TransportResponse:
    """Encode a server rejection in the recorded status-200 fault envelope."""
    return result_response(
        {'exception': True, 'message': str(fault), 'error_code': fault.error_code}, wrapped=False
    )
