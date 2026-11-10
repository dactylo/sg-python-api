from typing import cast

from shotgun_api3 import _capabilities as capabilities_module


def test_capability_metadata_is_detached_from_caller_and_accessors() -> None:
    metadata: dict[str, object] = {
        'version': [9, 1, 2],
        's3_enabled_upload_types': {'*': ['*']},
    }
    caps = capabilities_module.ServerCapabilities('x.example', metadata)

    cast('list[int]', metadata['version'])[0] = 7
    cast('dict[str, list[str]]', metadata['s3_enabled_upload_types'])['*'].clear()
    exposed = caps.server_info
    cast('list[int]', exposed['version'])[1] = 0
    cast('dict[str, list[str]]', exposed['s3_enabled_upload_types'])['*'].clear()

    assert caps.server_info == {
        'version': [9, 1, 2],
        's3_enabled_upload_types': {'*': ['*']},
    }
