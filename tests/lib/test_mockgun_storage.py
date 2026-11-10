from datetime import UTC, datetime
from urllib.parse import urlsplit

import pytest

from shotgun_api3.lib.mockgun import MockgunError
from shotgun_api3.lib.mockgun._storage import Storage

_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


def test_multipart_overwrite_etags_are_unique() -> None:
    storage = Storage()
    allocation = storage.allocate('a.txt', 'Attachment', multipart=True, now=_NOW)
    first = storage.part_url(allocation.upload_id, 1)
    second = storage.part_url(allocation.upload_id, 2)
    stale_etag = storage.put(first, b'old')
    current_etag = storage.put(first, b'new')
    second_etag = storage.put(second, b'part two')
    assert isinstance(stale_etag, str)
    assert isinstance(current_etag, str)
    assert isinstance(second_etag, str)
    assert len({stale_etag, current_etag, second_etag}) == 3
    with pytest.raises(MockgunError, match='parts or ETags'):
        storage.complete(allocation.upload_id, [stale_etag, second_etag])
    assert storage.complete(allocation.upload_id, [current_etag, second_etag]) == 2
    assert storage.read(storage.image_url(allocation.key)) == b'newpart two'


def test_storage_multipart_integrity_and_opaque_urls() -> None:
    storage = Storage()
    allocation = storage.allocate('a.txt', 'Attachment', multipart=True, now=_NOW)
    first = storage.part_url(allocation.upload_id, 1)
    second = storage.part_url(allocation.upload_id, 2)
    assert storage.part_url(allocation.upload_id, 1) == first
    assert not any(char.isdigit() for char in urlsplit(first).path)
    first_etag = storage.put(first, b'first')
    assert isinstance(first_etag, str)
    assert first_etag.startswith('"')
    assert first_etag.endswith('"')
    with pytest.raises(MockgunError, match='parts or ETags'):
        storage.complete(allocation.upload_id, [first_etag])
    second_etag = storage.put(second, b'second')
    assert isinstance(second_etag, str)
    with pytest.raises(MockgunError, match='parts or ETags'):
        storage.complete(allocation.upload_id, [second_etag, first_etag])
    assert not storage.has_bytes(allocation.key)
    assert storage.complete(allocation.upload_id, [first_etag, second_etag]) == 2
    left, right = storage.image_url(allocation.key), storage.image_url(allocation.key)
    assert left != right
    assert urlsplit(left).path == urlsplit(right).path
    assert storage.read(left) == storage.read(right) == b'firstsecond'
    with pytest.raises(MockgunError, match='upload_link_info'):
        storage.allocation(allocation.encoded + ' ')
    assert storage.allocation(allocation.encoded) == allocation
