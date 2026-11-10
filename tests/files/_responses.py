import urllib.parse
from collections.abc import Mapping

from shotgun_api3._transport import CaseInsensitiveHeaders, TransportRequest, TransportResponse
from tests.conftest import make_response


def forbidden_text_response(
    content: str = '',
    *,
    headers: Mapping[str, str] | None = None,
    status: int = 200,
) -> TransportResponse:
    return TransportResponse(
        status=status,
        reason='OK' if status == 200 else 'Forbidden',
        headers=CaseInsensitiveHeaders(headers or {'content-type': 'text/plain'}),
        content=content.encode(),
    )


def failed_text_response(
    content: str = '',
    *,
    headers: Mapping[str, str] | None = None,
    status: int = 200,
) -> TransportResponse:
    return TransportResponse(
        status=status,
        reason='OK' if status == 200 else 'Request failed',
        headers=CaseInsensitiveHeaders(headers or {'content-type': 'text/plain'}),
        content=content.encode(),
    )


def form_fields(body: bytes) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(body.decode(), keep_blank_values=True))


def raw_upload_transcript(
    requests: list[TransportRequest],
    bodies: list[bytes | None],
) -> list[tuple[str, str, dict[str, str], bytes | None]]:
    return [
        (request.method, request.url, dict(request.headers), body)
        for request, body in zip(requests, bodies, strict=True)
    ]


def read_response(entity: dict[str, object]) -> TransportResponse:
    return make_response({'entities': [entity], 'paging_info': {'entity_count': 1}})


def upload_headers(
    body: bytes,
    content_type: str,
) -> dict[str, str]:
    return {
        'content-length': str(len(body)),
        'content-type': content_type,
        'user-agent': 'upload-transcript',
    }
