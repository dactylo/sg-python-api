from __future__ import annotations

import struct
import zlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import Shotgun, sg_timezone

if TYPE_CHECKING:
    from pathlib import Path

    from .conftest import _LiveResources

pytestmark = pytest.mark.live


def _local_now() -> datetime:
    return datetime.now(tz=UTC).astimezone().replace(microsecond=0)


def _png(width: int, height: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return struct.pack('>I', len(data)) + body + struct.pack('>I', zlib.crc32(body))

    header = struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0)
    pixels = zlib.compress((b'\x00' + b'\x80\x40\x20' * width) * height)
    return (
        b'\x89PNG\r\n\x1a\n'
        + chunk(b'IHDR', header)
        + chunk(b'IDAT', pixels)
        + chunk(b'IEND', b'')
    )


def test_crud_roundtrip(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
    live_resources: _LiveResources,
) -> None:
    code = f'{live_namespace}-crud'
    shot = live_resources.create('Shot', {'code': code, 'project': live_project})
    shot_id = shot['id']
    assert isinstance(shot_id, int)

    fetched = live_sg.find_one('Shot', [['id', 'is', shot_id]], ['code', 'description'])
    assert fetched is not None
    assert fetched['code'] == code

    updated = live_sg.update('Shot', shot_id, {'description': f'{live_namespace}-updated'})
    assert updated['description'] == f'{live_namespace}-updated'


def test_current_site_info_satisfies_readiness(live_sg: Shotgun) -> None:
    info = live_sg.info()

    version = info.get('version')
    assert isinstance(version, list)
    version = cast('list[object]', version)
    assert len(version) >= 3
    assert all(isinstance(part, int) for part in version[:3])
    major, minor, patch = version[:3]
    assert isinstance(major, int)
    assert isinstance(minor, int)
    assert isinstance(patch, int)
    assert (major, minor, patch) >= (7, 10, 0)
    assert live_sg.server_caps.version >= (7, 10, 0)


def test_datetime_roundtrip(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
    live_resources: _LiveResources,
) -> None:
    before = _local_now()
    note = live_resources.create(
        'Note',
        {'project': live_project, 'subject': f'{live_namespace}-datetime'},
    )
    note_id = note['id']
    assert isinstance(note_id, int)

    fetched = live_sg.find_one('Note', [['id', 'is', note_id]], ['created_at'])
    assert fetched is not None
    created_at = fetched['created_at']
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is sg_timezone.local
    after = _local_now()
    assert before <= created_at <= after


def test_paging_and_ordering_roundtrip(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
    live_resources: _LiveResources,
) -> None:
    prefix = f'{live_namespace}-paging-'
    expected_codes = [f'{prefix}{index:03d}' for index in range(3)]
    for code in reversed(expected_codes):
        live_resources.create('Shot', {'code': code, 'project': live_project})

    filters = [
        ['project', 'is', live_project],
        ['code', 'starts_with', prefix],
    ]
    order = [{'field_name': 'code', 'direction': 'asc'}]
    first_page = live_sg.find('Shot', filters, ['code'], order=order, limit=2, page=1)
    second_page = live_sg.find('Shot', filters, ['code'], order=order, limit=2, page=2)

    assert [entity['code'] for entity in first_page] == expected_codes[:2]
    assert [entity['code'] for entity in second_page] == expected_codes[2:]


def test_thumbnail_paths_upload_on_create_and_update(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
    live_resources: _LiveResources,
    tmp_path: Path,
) -> None:
    image = tmp_path / f'{live_namespace}-thumbnail.png'
    image.write_bytes(_png(64, 64))
    strip = tmp_path / f'{live_namespace}-filmstrip.png'
    strip.write_bytes(_png(480, 135))

    shot = live_resources.create(
        'Shot',
        {'code': f'{live_namespace}-thumbnail', 'project': live_project, 'image': str(image)},
    )
    shot_id = shot['id']
    assert isinstance(shot_id, int)
    assert isinstance(shot['image'], str)
    assert shot['image'].startswith('https://')

    updated = live_sg.update('Shot', shot_id, {'filmstrip_image': str(strip)})
    assert set(updated) == {'type', 'id', 'filmstrip_image'}
    assert isinstance(updated['filmstrip_image'], str)
    assert updated['filmstrip_image'].startswith('https://')

    cleared = live_sg.update('Shot', shot_id, {'image': None})
    assert cleared['image'] is None


def test_upload_download_roundtrip(
    live_sg: Shotgun,
    live_project: dict[str, object],
    live_namespace: str,
    live_resources: _LiveResources,
    tmp_path: Path,
) -> None:
    owner = live_resources.create(
        'Shot',
        {'code': f'{live_namespace}-upload-owner', 'project': live_project},
    )
    owner_id = owner['id']
    assert isinstance(owner_id, int)
    payload = b'sg-python-api tier-2 live payload\x00\xff'
    source = tmp_path / f'{live_namespace}-payload.bin'
    source.write_bytes(payload)

    attachment_id = live_sg.upload(
        'Shot',
        owner_id,
        str(source),
        display_name=f'{live_namespace}-payload.bin',
    )
    live_resources.track('Attachment', attachment_id)

    downloaded = live_sg.download_attachment({'type': 'Attachment', 'id': attachment_id})
    assert downloaded == payload
