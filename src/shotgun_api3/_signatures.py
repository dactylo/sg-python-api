"""Shared argument binding and callable reflection metadata."""

import inspect
from collections.abc import Callable, Mapping


def bind_arguments(
    signature: inspect.Signature,
    args: tuple[object, ...],
    kwargs: Mapping[str, object],
    label: str,
) -> inspect.BoundArguments:
    """Bind arguments and defaults with a caller-specific error prefix.

    Args:
        signature: Public call signature to enforce.
        args: Supplied positional arguments.
        kwargs: Supplied keyword arguments.
        label: Public callable label, including parentheses, for errors.

    Returns:
        Bound arguments retaining the supplied values without copying them.

    Raises:
        TypeError: If binding fails, prefixed with the callable label.
    """
    try:
        bound = signature.bind(*args, **kwargs)
    except TypeError as exc:
        raise TypeError(f'{label} {exc}') from None
    bound.apply_defaults()
    return bound


def expose_signature(
    function: Callable[..., object],
    signature: inspect.Signature,
    doc: str | None,
) -> Callable[..., object]:
    """Attach a signature and optional documentation to the same callable.

    Args:
        function: Implementation receiving public reflection metadata.
        signature: Public signature to expose.
        doc: Replacement documentation, or `None` to preserve the docstring.

    Returns:
        The same callable with its reflection metadata updated.
    """
    if doc is not None:
        object.__setattr__(function, '__doc__', doc)
    object.__setattr__(function, '__signature__', signature)
    return function
