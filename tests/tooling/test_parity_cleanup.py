from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from shotgun_api3 import ShotgunError
from shotgun_api3.lib.mockgun import MockgunError
from tests import _parity

if TYPE_CHECKING:
    from tests._parity import ParityCase


class _Case:
    def __init__(self, failures: dict[str, object]) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self._failures = failures

    async def call(self, method: str, entity_type: str, field: str) -> object:
        self.calls.append((method, entity_type, field))
        result = self._failures.get(field, True)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.anyio
async def test_field_cleanup_reports_every_failed_result_and_chains_first_error() -> None:
    error = MockgunError('delete failed')
    case = _Case({'fourth': None, 'third': False, 'second': error, 'first': ShotgunError('later')})

    with pytest.raises(AssertionError) as caught:
        await _parity.delete_fields(
            cast('ParityCase', case),
            'Asset',
            ['first', 'successful_first', 'second', 'successful_second', 'third', 'fourth'],
        )

    assert case.calls == [
        ('schema_field_delete', 'Asset', 'fourth'),
        ('schema_field_delete', 'Asset', 'third'),
        ('schema_field_delete', 'Asset', 'successful_second'),
        ('schema_field_delete', 'Asset', 'second'),
        ('schema_field_delete', 'Asset', 'successful_first'),
        ('schema_field_delete', 'Asset', 'first'),
    ]
    assert all(field in str(caught.value) for field in ('first', 'second', 'third', 'fourth'))
    assert 'successful_first' not in str(caught.value)
    assert 'successful_second' not in str(caught.value)
    assert caught.value.__cause__ is error


@pytest.mark.anyio
async def test_field_cleanup_propagates_unexpected_errors() -> None:
    error = RuntimeError('unexpected failure')
    case = _Case({'third': error})

    with pytest.raises(RuntimeError) as caught:
        await _parity.delete_fields(
            cast('ParityCase', case), 'Asset', ['first', 'second', 'third']
        )

    assert caught.value is error
    assert case.calls == [('schema_field_delete', 'Asset', 'third')]
