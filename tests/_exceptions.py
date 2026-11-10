from collections.abc import Callable
from typing import Never, TypeVar

_AmbientResult = TypeVar('_AmbientResult')


def raise_ambient_exception() -> Never:
    raise RuntimeError('ambient exception state')


def call_inside_except(function: Callable[[], _AmbientResult]) -> _AmbientResult:
    try:
        raise_ambient_exception()
    except RuntimeError:
        return function()
