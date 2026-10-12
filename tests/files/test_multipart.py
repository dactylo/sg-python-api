import io
from pathlib import Path

import pytest

from shotgun_api3 import _files
from shotgun_api3._files import _pump_multipart
from shotgun_api3._protocol import FileSlot, build_multipart_plan


def test_form_upload_exchange_forwards_file_chunk_size(tmp_path: Path) -> None:
    source = tmp_path / 'source.bin'
    source.write_bytes(b'abcdefg')
    prepared = _files.PreparedUpload(
        filename='source.bin',
        file_size=7,
        options=_files.UploadOptions(
            arguments=_files.UploadArguments('Version', 5, str(source)),
            upload_type='Attachment',
            filmstrip=False,
        ),
    )
    exchange = _files.build_form_upload_exchange(
        'https://x.example',
        'test-agent',
        prepared,
        {},
        chunk_size=3,
    )
    body = exchange.body_factory()
    assert body is not None
    assert not isinstance(body, bytes)

    chunks = list(body)

    file_header = next(
        index for index, chunk in enumerate(chunks) if b'filename="source.bin"' in chunk
    )
    assert chunks[file_header + 1 : file_header + 4] == [b'abc', b'def', b'g']


def test_generated_multipart_boundary_is_uuid_hex() -> None:
    plan = build_multipart_plan({}, [])
    boundary = plan.content_type.removeprefix('multipart/form-data; boundary=')

    assert len(boundary) == 32
    assert set(boundary) <= set('0123456789abcdef')
    assert plan.segments == [f'--{boundary}--\r\n'.encode()]


def test_multipart_plan_preserves_exact_wire_order_and_escapes_dispositions() -> None:
    slot = FileSlot(
        name='fi"le\n',
        filename='clip"\\\r.mov',
        content_type='video/quicktime',
    )

    plan = build_multipart_plan(
        {'entity"\r\n\\type': 'Version', 'entity_id': '5'},
        [slot],
        boundary='Boundary-09_.+',
    )

    assert plan.content_type == 'multipart/form-data; boundary=Boundary-09_.+'
    assert plan.segments == [
        (
            b'--Boundary-09_.+\r\nContent-Disposition: form-data; '
            b'name="entity%22\\\\type"\r\n\r\nVersion\r\n'
        ),
        (b'--Boundary-09_.+\r\nContent-Disposition: form-data; name="entity_id"\r\n\r\n5\r\n'),
        (
            b'--Boundary-09_.+\r\nContent-Disposition: form-data; '
            b'name="fi%22le"; filename="clip%22\\\\.mov"'
            b'\r\nContent-Type: video/quicktime\r\n\r\n'
        ),
        slot,
        b'\r\n',
        b'--Boundary-09_.+--\r\n',
    ]


@pytest.mark.parametrize(
    'boundary',
    [
        '',
        'contains space',
        'contains/slash',
        'header\r\nX-Injected: yes',
        'non-ascii-\u00e9',
        'x' * 71,
    ],
)
def test_multipart_plan_rejects_unsafe_explicit_boundary(boundary: str) -> None:
    with pytest.raises(
        ValueError,
        match=r'^multipart boundary must be 1 to 70 safe ASCII characters$',
    ):
        build_multipart_plan({}, [], boundary=boundary)


def test_pump_preserves_empty_file_framing() -> None:
    slot = FileSlot(name='file', filename='empty.txt', content_type='text/plain')
    plan = build_multipart_plan({}, [slot], boundary='B')

    chunks = list(_pump_multipart(plan, {'file': io.BytesIO()}))

    assert chunks == [
        (
            b'--B\r\nContent-Disposition: form-data; name="file"; '
            b'filename="empty.txt"\r\nContent-Type: text/plain\r\n\r\n'
        ),
        b'\r\n',
        b'--B--\r\n',
    ]


def test_pump_reads_file_incrementally_in_wire_order() -> None:
    slot = FileSlot(name='file', filename='a.txt', content_type='text/plain')
    plan = build_multipart_plan({}, [slot], boundary='B')

    chunks = list(_pump_multipart(plan, {'file': io.BytesIO(b'abcdefg')}, chunk_size=3))

    assert chunks == [
        (
            b'--B\r\nContent-Disposition: form-data; name="file"; '
            b'filename="a.txt"\r\nContent-Type: text/plain\r\n\r\n'
        ),
        b'abc',
        b'def',
        b'g',
        b'\r\n',
        b'--B--\r\n',
    ]


@pytest.mark.parametrize('chunk_size', [0, -1])
def test_pump_rejects_nonpositive_chunk_size_before_yielding(chunk_size: int) -> None:
    slot = FileSlot(name='file', filename='a.txt', content_type='text/plain')
    plan = build_multipart_plan({}, [slot], boundary='B')
    opened = io.BytesIO(b'FILEDATA')
    chunks = _pump_multipart(plan, {'file': opened}, chunk_size=chunk_size)

    with pytest.raises(ValueError, match=r'^multipart chunk size must be positive$'):
        next(chunks)

    assert opened.tell() == 0
