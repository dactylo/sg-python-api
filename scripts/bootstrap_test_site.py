#!/usr/bin/env python
"""Idempotently create the stable Project fixture for tier-2 live tests.

Run this script with `SG_TEST_SITE`, `SG_TEST_SCRIPT`, `SG_TEST_KEY`, and
`SG_TEST_PROJECT` configured. It loads and verifies this checkout's client,
creates the named Project only when absent, and closes the client.

The Project remains on the live site for subsequent tests.
"""

import importlib
import os
import sys
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from shotgun_api3 import Shotgun

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SOURCE_ROOT = _REPO_ROOT / 'src'


class _FailureCollector:
    """Collect and suppress one cleanup failure."""

    def __init__(self, failures: list[BaseException]) -> None:
        """Bind the caller-owned shared cleanup-failure list."""
        self.failures = failures

    def __enter__(self) -> None:
        """Enter the cleanup-failure capture boundary."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Capture and suppress one cleanup exception."""
        del exc_type, traceback
        if exc is None:
            return False
        self.failures.append(exc)
        return True


class _ProjectClient(Protocol):
    """Describe the project setup client surface."""

    def create(self, entity_type: str, data: dict[str, object]) -> object:
        """Create one entity.

        Returns:
            The client-specific creation result.
        """
        ...

    def find_one(self, entity_type: str, filters: list[list[object]]) -> object:
        """Find the first matching entity.

        Returns:
            The client-specific first-match result.
        """
        ...


def _ensure_project(client: _ProjectClient, name: str) -> bool:
    """Ensure the stable Project exists and report whether it was created."""
    project = client.find_one('Project', [['name', 'is', name]])
    if project is not None:
        return False
    client.create('Project', {'name': name})
    return True


def _load_client_class() -> type['Shotgun']:
    """Load and verify the client class from this checkout."""
    source = str(_SOURCE_ROOT)
    if source not in sys.path:
        sys.path.insert(0, source)
    importlib.invalidate_caches()
    module = importlib.import_module('shotgun_api3')
    module_file = module.__file__
    if module_file is None:
        raise RuntimeError('bootstrap refused a shotgun_api3 module without an origin')
    origin = Path(module_file).resolve()
    if not origin.is_relative_to(_SOURCE_ROOT):
        raise RuntimeError('bootstrap refused a shotgun_api3 import outside this checkout')
    return module.Shotgun


def main() -> int:
    """Create the stable Project fixture and close the client.

    Returns:
        Zero after the Project exists and the client closes cleanly.

    Raises:
        KeyError: If required live-site configuration is absent.
        Exception: If setup or deterministic client closure fails.
    """
    client_class = _load_client_class()
    client = client_class(
        os.environ['SG_TEST_SITE'],
        script_name=os.environ['SG_TEST_SCRIPT'],
        api_key=os.environ['SG_TEST_KEY'],
        connect=False,
    )
    failures: list[BaseException] = []
    created = False
    with _FailureCollector(failures):
        created = _ensure_project(client, os.environ['SG_TEST_PROJECT'])
    with _FailureCollector(failures):
        client.close()
    if len(failures) > 1:
        raise BaseExceptionGroup('bootstrap operation and client close failed', failures)
    if failures:
        raise failures[0]

    state = 'created' if created else 'already present'
    print(f'stable live Project fixture {state}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
