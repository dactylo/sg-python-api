"""Run real sync and async Shotgun clients against an in-memory fake site.

Site controls configure explicit preferences, a Monday-to-Sunday working week,
HumanUser subscriptions, and exact navigation/export responses or faults
through `set_preferences`, `set_work_week`, `set_subscriptions`,
`set_canned_response` and `set_canned_fault`. Inputs are detached under the
site lock. Unconfigured server-dependent values raise `MockgunError`.

Configure the chosen client's `set_schema_paths()` with field-schema and
entity-schema pickle files before constructing instances. Await
`AsyncShotgun.set_schema_paths()` and `AsyncShotgun.get_schema_paths()`.
`generate_schema()` can create the files from an existing client.

Constructor credentials are validated by the real client; a client constructed
without any credentials acts as the `mockgun` script. Both clients operate
without network connections, and `AsyncShotgun` needs no optional async
transport extra. Use each client's `site` facade to control its clock,
readiness metadata, and maximum page size. Call `new_client()` with new
credentials to share the same site's schema and records. It is a plain method
on both clients; directly constructed clients have independent sites.
"""

from ._async_client import AsyncShotgun
from ._errors import MockgunError
from ._schema import generate_schema
from ._site import Site
from ._sync_client import Shotgun

__all__ = ['AsyncShotgun', 'MockgunError', 'Shotgun', 'Site', 'generate_schema']
