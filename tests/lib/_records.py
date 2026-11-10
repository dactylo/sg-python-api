from typing import cast

from tests.lib._site import FakeSiteCase


async def create_link(case: FakeSiteCase, kind: str, data: dict[str, object]) -> dict[str, object]:
    result = cast('dict[str, object]', await case.call('create', kind, data))
    return {'type': kind, 'id': result['id']}
