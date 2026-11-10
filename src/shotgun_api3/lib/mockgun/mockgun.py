"""Provide mockgun's clients under the module path Autodesk's package uses.

Code written for Autodesk's mockgun can import `Shotgun` from this module. The
names here are the same objects that `shotgun_api3.lib.mockgun` exports; see
that package for their behavior.
"""

from ._async_client import AsyncShotgun
from ._errors import MockgunError
from ._schema import generate_schema
from ._site import Site
from ._sync_client import Shotgun

__all__ = ['AsyncShotgun', 'MockgunError', 'Shotgun', 'Site', 'generate_schema']
