from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar, cast

import pytest

from tests.live import _probe

if TYPE_CHECKING:
    from tests.live._verification import LiveCase


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        ('unexpected', True),
        ('type', 'unlisted'),
        ('name', 'different'),
        ('id', 99),
        ('content_type', 'application/octet-stream'),
        ('link_type', 'local'),
    ],
)
def test_attachment_successful_reads_with_wrong_shape_or_values_stay_unsettled(
    key: str, value: object
) -> None:
    row: dict[str, object] = {
        'url': 'https://storage.invalid/10',
        'name': 'Shown',
        'content_type': 'text/plain',
        'link_type': 'upload',
        'type': 'Attachment',
        'id': 10,
    }
    first = _probe.file_observation(row, 10, 'Shown', 'site.invalid')
    changed = _probe.file_observation({**row, key: value}, 10, 'Shown', 'site.invalid')
    assert _probe.settled_files([first, first])
    assert not _probe.settled_files([first, changed])


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        ('unexpected', True),
        ('type', 'ApiUser'),
        ('name', 'different'),
        ('id', 99),
        ('status', 'unexpected'),
        ('image', 'present'),
    ],
)
def test_creator_successful_reads_require_exact_shape_and_candidate(
    key: str, value: object
) -> None:
    human: dict[str, object] = {
        'type': 'HumanUser',
        'id': 2,
        'name': 'Human',
        'sg_status_list': 'act',
        'image': None,
    }
    creator: dict[str, object] = {
        'type': 'HumanUser',
        'id': 2,
        'name': 'Human',
        'status': 'act',
        'image': None,
    }
    script: dict[str, object] = {'type': 'ApiUser', 'id': 3}
    first = _probe.creator_observation({'created_by': creator}, human, script)
    changed = _probe.creator_observation({'created_by': {**creator, key: value}}, human, script)
    assert _probe.settled_creators([first, first])
    assert not _probe.settled_creators([first, changed])


@pytest.mark.anyio
async def test_attachment_probe_does_not_settle_successful_wrong_values() -> None:

    row: dict[str, object] = {
        'url': 'https://storage.invalid/10',
        'name': 'wrong',
        'content_type': 'text/plain',
        'link_type': 'upload',
        'type': 'Attachment',
        'id': 10,
    }

    class Case:
        project: ClassVar[dict[str, object]] = {'type': 'Project', 'id': 1}
        namespace = 'sg-python-api-live-123456789abc'
        client = SimpleNamespace()

        async def create(self, _entity_type: str, _data: dict[str, object]) -> dict[str, object]:
            return {'type': 'Note', 'id': 2}

        async def call(self, method: str, *_args: object, **_kwargs: object) -> object:
            if method == 'upload':
                return 10
            if method == 'find_one':
                return {'this_file': row}
            return [{'type': 'Attachment', 'id': 10, 'this_file': row}]

    observed = await _probe.attachment(cast('LiveCase', Case()))
    assert observed['status'] == 'Unsettled'


def test_sudo_metadata_requires_all_five_exact_keys() -> None:
    incomplete: dict[str, object] = {
        'sudo': {'keys': ['type', 'id'], 'types': ['str', 'int']},
        'sudo_type_equal': True,
        'sudo_id_equal': True,
    }
    complete: dict[str, object] = {
        **incomplete,
        'sudo': {
            'keys': ['type', 'id', 'name', 'valid', 'uuid'],
            'types': ['str', 'int', 'str', 'str', 'str'],
        },
    }
    assert not _probe.sudo_matches([incomplete])
    assert _probe.sudo_matches([complete])
