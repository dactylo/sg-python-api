# sg-python-api

sg-python-api is a Python client for Autodesk Flow Production Tracking, written for pipeline tools
and scripts built on Autodesk's `shotgun_api3` client. It keeps the same import name and documented
API, so an existing integration can switch without rewriting its calls and gain shared connections,
bounded network waits, and retries that do not repeat an ambiguous write. It also adds a native
asynchronous client.

This documentation uses SG for Flow Production Tracking, formerly named Shotgun and ShotGrid.

## Why use it?

- **Reuse connections:** share one synchronous client across threads, or one async client across
  tasks on the same event loop.
- **Bound network waits:** default connect and read timeouts help applications recover from an
  unavailable service.
- **Handle writes carefully:** retries account for whether an operation is safe to repeat. An
  ambiguous write is left for the application to reconcile.
- **Test without a site:** synchronous and asynchronous `mockgun` clients provide an in-memory API
  for application tests.

sg-python-api is an independent implementation of the documented API of Autodesk's Python client
`3.10.2`. Some behavior differs on purpose, so validate your application's workflows against a test
site before replacing its production client. The [migration guide](docs/migration-guide.md) walks
through the switch, and the [compatibility reference](COMPATIBILITY.md) lists the differences.

## Installation

You need Python 3.11 or newer and an SG site running server version 7.10.0 or newer. Authenticated
requests to a remote site require HTTPS.

`sg-python-api` and Autodesk's `shotgun-api3` both install the `shotgun_api3` package, so install
exactly one of them in a Python environment. If the official client is already installed, follow
the [migration steps](docs/migration-guide.md#install-the-replacement) to replace it.

From a checkout, install into the Python environment that runs your application:

```sh
python -m pip install .
```

The base installation provides the synchronous `Shotgun` client and both `mockgun` test clients. To
use the native asynchronous client, `AsyncShotgun`, install the `async` extra, which adds the
`httpx2` HTTP library:

```sh
python -m pip install '.[async]'
```

## Quick start

These examples read one project from your site. They take the site URL and script credentials from
environment variables so the API key stays out of source code; the client does not read these
variables itself. Set `SHOTGUN_URL` to your site's HTTPS URL, and set `SHOTGUN_SCRIPT_NAME` and
`SHOTGUN_API_KEY` to a script account that can read projects.

### Synchronous client

```python
import os

from shotgun_api3 import Shotgun

sg = Shotgun(
    os.environ['SHOTGUN_URL'],
    script_name=os.environ['SHOTGUN_SCRIPT_NAME'],
    api_key=os.environ['SHOTGUN_API_KEY'],
)
try:
    project = sg.find_one('Project', [], ['name'])
    print(project)
finally:
    sg.close()
```

The script prints a project dictionary containing `type`, `id`, and `name`, or `None` when no
project is visible to that account. You can share one `Shotgun` client across threads; `close()`
releases its pooled connections when you no longer need it.

### Asynchronous client

With the `async` extra installed and the same environment variables set, run:

```python
import asyncio
import os

from shotgun_api3 import AsyncShotgun


async def main() -> None:
    async with AsyncShotgun(
        os.environ['SHOTGUN_URL'],
        script_name=os.environ['SHOTGUN_SCRIPT_NAME'],
        api_key=os.environ['SHOTGUN_API_KEY'],
    ) as sg:
        project = await sg.find_one('Project', [], ['name'])
        print(project)


asyncio.run(main())
```

The output matches the synchronous example. The `async with` block closes the client even when an
operation fails. Without the `async` extra, importing `AsyncShotgun` raises `ImportError`.

Share an `AsyncShotgun` only among tasks on one event loop, and cancel its work by cancelling the
task. A cancelled write may already have reached the server, so check its outcome before repeating
it. The [async reference](COMPATIBILITY.md#async-extension-surfaces) covers shutdown and
cancellation in detail.

## Documentation

| If you want to...                                     | Read                                                                               |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Replace Autodesk's client in an existing application  | [Migration guide](docs/migration-guide.md)                                         |
| Check what behaves differently from Autodesk's client | [Compatibility](COMPATIBILITY.md)                                                  |
| Tune pooling and page prefetch, or observe requests   | [Configuration extensions](COMPATIBILITY.md#configuration-and-observer-extensions) |
| Confirm which client loads and run a smoke test       | [Testing an installation](docs/testing.md)                                         |
| Compare measured latency and transfer memory          | [Benchmarks](docs/benchmarks.md)                                                   |
| See changes by version                                | [Changelog](CHANGELOG.md)                                                          |

## Development

See the [development guide](docs/development.md) for the required tools, CI, and dependency locks.
With those tools installed, run:

```sh
make setup
make check
```

`make check` also runs the live-site tests, which create and delete records, whenever the
`SG_TEST_*` variables described in the development guide are set.

## License

sg-python-api is available under the [MIT License](LICENSE). It is an independent project and is
not affiliated with or endorsed by Autodesk. Flow Production Tracking is a product of Autodesk,
Inc.
