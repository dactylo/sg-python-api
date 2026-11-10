from typing import cast

from tests.conftest import entity_id
from tests.lib._site import FakeSiteCase


async def cursor(case: FakeSiteCase) -> int:
    rows = cast(
        'list[dict[str, object]]',
        await case.call(
            'find',
            'EventLogEntry',
            [],
            ['id'],
            order=[{'field_name': 'id', 'direction': 'desc'}],
            limit=1,
        ),
    )
    return entity_id(rows[0]) if rows else 0


async def delta(case: FakeSiteCase, before: int, fields: list[str]) -> list[dict[str, object]]:
    return cast(
        'list[dict[str, object]]',
        await case.call(
            'find',
            'EventLogEntry',
            [['id', 'greater_than', before]],
            fields,
            order=[{'field_name': 'id', 'direction': 'asc'}],
        ),
    )
