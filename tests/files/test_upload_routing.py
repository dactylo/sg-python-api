from collections.abc import Mapping
from types import MappingProxyType

import pytest

from shotgun_api3 import _files


@pytest.mark.parametrize(
    ('server_info', 'field_name'),
    [
        (
            {
                's3_direct_uploads_enabled': False,
                's3_enabled_upload_types': {'Version': ['sg_uploaded_movie']},
            },
            'sg_uploaded_movie',
        ),
        (
            {
                's3_direct_uploads_enabled': True,
                's3_enabled_upload_types': {'Version': ['sg_uploaded_movie']},
            },
            'sg_other',
        ),
    ],
    ids=['direct-disabled', 'nonmatching-field'],
)
def test_choose_upload_path_falls_back_to_form_upload(
    server_info: Mapping[str, object],
    field_name: str,
) -> None:
    assert _files.choose_upload_path(server_info, 'Version', field_name, 1) == 'form'


@pytest.mark.parametrize(
    ('file_size', 'expected'),
    [(20_000_000, 'direct'), (20_000_001, 'multipart')],
    ids=['boundary-direct', 'boundary-multipart'],
)
def test_choose_upload_path_uses_the_observed_multipart_boundary(
    file_size: int,
    expected: str,
) -> None:
    server_info: Mapping[str, object] = MappingProxyType(
        {
            's3_direct_uploads_enabled': True,
            's3_enabled_upload_types': {'Version': ['sg_uploaded_movie']},
        }
    )

    assert (
        _files.choose_upload_path(server_info, 'Version', 'sg_uploaded_movie', file_size)
        == expected
    )


def test_upload_routes_are_evidence_pinned() -> None:
    assert _files.COMPLETE_MULTIPART == '/upload/api_complete_multipart_upload'
    assert _files._FILE_SERVE_ATTACHMENT == '/file_serve/attachment/'
    assert _files.GET_PART_LINK == '/upload/api_get_upload_link_for_part'
    assert _files.GET_UPLOAD_LINK_INFO == '/upload/api_get_upload_link_info'
    assert _files.LINK_FILE == '/upload/api_link_file'
    assert _files._MULTIPART_CHUNK_SIZE == 20_000_000
    assert _files.SHARE_THUMBNAIL == '/upload/share_thumbnail'
    assert _files._UPLOAD_FILE == '/upload/upload_file'
