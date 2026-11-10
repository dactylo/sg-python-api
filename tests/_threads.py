import threading
from collections.abc import Callable, Sequence
from time import monotonic
from types import TracebackType
from typing import TypeVar, cast

T = TypeVar('T')


class _CaptureError:
    def __init__(self, errors: list[BaseException | None], index: int) -> None:
        self.errors = errors
        self.index = index

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exception_type, traceback
        self.errors[self.index] = error
        return error is not None


def run_concurrently(calls: Sequence[Callable[[], T]], *, timeout: float = 5.0) -> list[T]:
    results: list[T | None] = [None] * len(calls)
    errors: list[BaseException | None] = [None] * len(calls)
    finished = [threading.Event() for _ in calls]

    def run(index: int, call: Callable[[], T]) -> None:
        try:
            with _CaptureError(errors, index):
                results[index] = call()
        finally:
            finished[index].set()

    deadline = monotonic() + timeout
    for index, call in enumerate(calls):
        threading.Thread(target=run, args=(index, call), daemon=True).start()
    for done in finished:
        if not done.wait(max(0.0, deadline - monotonic())):
            raise TimeoutError('concurrent test calls did not finish before the deadline')
    for error in errors:
        if error is not None:
            raise error
    return [cast('T', result) for result in results]
