"""Provide in-memory sync and async Shotgun test doubles.

Configure the chosen client's `set_schema_paths()` with field-schema and
entity-schema pickle files before constructing instances. Await
`AsyncShotgun.set_schema_paths()` and `AsyncShotgun.get_schema_paths()`.
`generate_schema()` can create the files from an existing client.

Both clients operate without network connections. `AsyncShotgun` exposes
awaitable methods over a synchronous in-memory delegate and does not require
the real client's optional async transport.
"""

from ._async import AsyncShotgun
from ._mockgun import MockgunError, Shotgun, generate_schema

__all__ = ['AsyncShotgun', 'MockgunError', 'Shotgun', 'generate_schema']
