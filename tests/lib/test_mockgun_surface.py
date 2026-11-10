import importlib
import inspect

import pytest

from shotgun_api3 import AsyncShotgun, Shotgun
from shotgun_api3.lib import mockgun
from shotgun_api3.lib.mockgun import AsyncShotgun as SiteAsyncShotgun
from shotgun_api3.lib.mockgun import Shotgun as SiteShotgun

_SURFACE = frozenset(
    {
        'activity_stream_read',
        'add_user_agent',
        'authenticate_human_user',
        'base_url',
        'batch',
        'client_caps',
        'close',
        'config',
        'connect',
        'create',
        'delete',
        'download_attachment',
        'entity_types',
        'export_page',
        'find',
        'find_one',
        'follow',
        'followers',
        'following',
        'get_attachment_download_url',
        'get_auth_cookie_handler',
        'get_session_token',
        'info',
        'nav_expand',
        'nav_search_entity',
        'nav_search_string',
        'note_thread_read',
        'preferences_read',
        'reset_user_agent',
        'revive',
        'schema',
        'schema_entity_read',
        'schema_field_create',
        'schema_field_delete',
        'schema_field_read',
        'schema_field_update',
        'schema_read',
        'server_caps',
        'server_info',
        'set_session_uuid',
        'share_thumbnail',
        'summarize',
        'text_search',
        'unfollow',
        'update',
        'update_project_last_accessed',
        'upload',
        'upload_filmstrip_thumbnail',
        'upload_thumbnail',
        'user_subscriptions_create',
        'user_subscriptions_read',
        'work_schedule_read',
        'work_schedule_update',
    }
)


def _public_surface(cls: type[Shotgun] | type[AsyncShotgun]) -> set[str]:
    return {
        name
        for name in dir(cls)
        if not name.startswith('_')
        and (
            callable(getattr(cls, name)) or isinstance(inspect.getattr_static(cls, name), property)
        )
    }


def test_client_module_path_exports_the_package_api() -> None:
    # Code written for Autodesk's mockgun imports `shotgun_api3.lib.mockgun.mockgun`.
    client_module = importlib.import_module('shotgun_api3.lib.mockgun.mockgun')
    assert len(client_module.__all__) == len(set(client_module.__all__))
    assert len(mockgun.__all__) == len(set(mockgun.__all__))
    assert set(client_module.__all__) == set(mockgun.__all__)
    assert all(getattr(client_module, name) is getattr(mockgun, name) for name in mockgun.__all__)


def test_package_exports_have_supported_names_and_are_available() -> None:
    names = {'AsyncShotgun', 'MockgunError', 'Shotgun', 'Site', 'generate_schema'}
    assert len(mockgun.__all__) == len(set(mockgun.__all__))
    assert set(mockgun.__all__) == names
    assert all(getattr(mockgun, name) is not None for name in names)


@pytest.mark.parametrize(
    ('real', 'mock'), [(Shotgun, SiteShotgun), (AsyncShotgun, SiteAsyncShotgun)]
)
def test_surface_matrix_covers_real_client_and_mockgun(
    real: type[Shotgun] | type[AsyncShotgun],
    mock: type[SiteShotgun] | type[SiteAsyncShotgun],
) -> None:
    assert _public_surface(real) == _SURFACE
    assert _public_surface(mock) >= _SURFACE
