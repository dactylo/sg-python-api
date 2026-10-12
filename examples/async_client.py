#!/usr/bin/env python
"""Query one Project with the asynchronous public client.

Run this example with `--site`, `--project`, and `--credentials-file`.
The credentials file must contain exactly two nonempty UTF-8 lines: the script
name followed by its API key. The installed package must include the `async`
extra.

The example prints the loaded package identity before network access and closes
the client through an async context manager.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, cast

import shotgun_api3
from shotgun_api3 import AsyncShotgun

if TYPE_CHECKING:
    from collections.abc import Sequence

_CREDENTIAL_LINE_COUNT = 2


class _Arguments(NamedTuple):
    """Hold validated command-line arguments."""

    site: str
    project: str
    credentials_file: Path


def _load_credentials(path: Path) -> tuple[str, str]:
    """Load a script name and API key without exposing either value."""
    try:
        lines = path.read_text(encoding='utf-8').splitlines()
    except (OSError, UnicodeError) as error:
        raise ValueError('Unable to read credentials file.') from error
    if len(lines) != _CREDENTIAL_LINE_COUNT or any(not line.strip() for line in lines):
        raise ValueError('Credentials file must contain exactly two nonempty UTF-8 lines.')
    return lines[0], lines[1]


def _parse_args(argv: Sequence[str] | None = None) -> _Arguments:
    """Parse the asynchronous example's command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--credentials-file', required=True, type=Path)
    namespace = parser.parse_args(argv)
    return _Arguments(
        site=cast('str', namespace.site),
        project=cast('str', namespace.project),
        credentials_file=cast('Path', namespace.credentials_file),
    )


def _print_package_identity() -> None:
    """Print the loaded package identity before any network request."""
    print(f'shotgun_api3 version: {shotgun_api3.__version__}')
    print(f'shotgun_api3 module: {Path(shotgun_api3.__file__).resolve()}')


def _print_result(server_info: dict[str, object], project: dict[str, object]) -> None:
    """Print only the example's allowlisted server and Project fields."""
    print(f'server version: {server_info["version"]}')
    print(f'project id: {project["id"]}')
    print(f'project name: {project["name"]}')


async def _run(args: _Arguments, script_name: str, api_key: str) -> int:
    """Query and print one Project while owning the asynchronous client context."""
    _print_package_identity()
    async with AsyncShotgun(
        args.site,
        script_name=script_name,
        api_key=api_key,
    ) as client:
        server_info = await client.info()
        project = await client.find_one(
            'Project',
            [['name', 'is', args.project]],
            ['name'],
        )
        if project is None:
            print(f'Project not found: {args.project}', file=sys.stderr)
            return 1
        _print_result(server_info, project)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the asynchronous public-client example.

    Args:
        argv: Command-line arguments, or `None` to read `sys.argv`.

    Returns:
        Zero on success, or one for an expected example failure.
    """
    args = _parse_args(argv)
    try:
        script_name, api_key = _load_credentials(args.credentials_file)
        return asyncio.run(_run(args, script_name, api_key))
    except Exception:  # noqa: BLE001
        print('Unable to run example.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
