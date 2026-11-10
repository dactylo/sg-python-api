from typing import cast

import pytest


def dictionary(value: object, message: str, *, pytrace: bool = True) -> dict[str, object]:
    if not isinstance(value, dict):
        pytest.fail(message, pytrace=pytrace)
    return cast('dict[str, object]', value)


def list_items(value: object, message: str, *, pytrace: bool = True) -> list[object]:
    if not isinstance(value, list):
        pytest.fail(message, pytrace=pytrace)
    return cast('list[object]', value)


def positive_id(value: object, message: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        pytest.fail(message, pytrace=False)
    return value


def record_id(entity: dict[str, object]) -> int:
    return positive_id(entity.get('id'), 'record has no positive integer ID')


def require(condition: bool, message: str, *, pytrace: bool = True) -> None:
    if not condition:
        pytest.fail(message, pytrace=pytrace)
