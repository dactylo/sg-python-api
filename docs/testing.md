# Testing an installation

Use this page to confirm that your application's Python environment loads sg-python-api and that
the client can read from your site. If you have not installed the package yet, start with the
[README](../README.md#installation). An import check confirms which client loads; a read-only smoke
test confirms your credentials, permissions, and connectivity. To run the package's own test suite
from a checkout, see the [development guide](development.md#project-notes).

## Verify the import target

Autodesk's `shotgun-api3` and sg-python-api both provide `shotgun_api3`. In the application's
Python environment, print the loaded version and module path:

```sh
python -c "import shotgun_api3; print(shotgun_api3.__version__); print(shotgun_api3.__file__)"
```

The version is the installed sg-python-api version; Autodesk's client reports its own version
number instead. Confirm that the path points to the installation you intend to test.

To test a checkout without installing it, put its `src` directory first on the import path:

```sh
PYTHONPATH=src python -c "import shotgun_api3; print(shotgun_api3.__version__); print(shotgun_api3.__file__)"
```

The path should then be under the checkout's `src` directory. Async examples also require the
optional `async` dependencies in the same environment.

## Run a read-only smoke test

The examples in the checkout's `examples/` directory read server information and find a Project by
name, then close the client. They print the package identity before making any request, so you can
tell which client produced the result.

From the repository root, create `credentials.txt` with exactly two nonempty UTF-8 lines:

1. Script name
2. API key

A final newline is allowed. Keep the file out of version control. On POSIX systems, restrict its
permissions so other users cannot read the key:

```sh
chmod 600 credentials.txt
```

Set `SG_TEST_SITE` to your test site's full HTTPS URL and `SG_TEST_PROJECT` to the exact name of a
Project the script can read. Run the synchronous example against the installed package:

```sh
python examples/sync_client.py \
    --site "$SG_TEST_SITE" \
    --project "$SG_TEST_PROJECT" \
    --credentials-file credentials.txt
```

For the async client, run:

```sh
python examples/async_client.py \
    --site "$SG_TEST_SITE" \
    --project "$SG_TEST_PROJECT" \
    --credentials-file credentials.txt
```

A successful run prints five lines:

```text
shotgun_api3 version: <installed version>
shotgun_api3 module: <path to shotgun_api3/__init__.py>
server version: <server version>
project id: <Project ID>
project name: <Project name>
```

Prefix either command with `PYTHONPATH=src` to test the checkout instead. Check the printed module
path before interpreting the result: success from a different installed provider does not verify
sg-python-api.

A successful smoke test establishes that the selected client can authenticate and perform these
reads. Validate the other operations your application uses separately. Smoke-test timings are not
benchmark results.

### If the smoke test fails

`Project not found: <name>` means the request succeeded but the script cannot see a Project with
that exact name.

`Unable to run example.` reports any other error during the run. The examples hide exception
details so that diagnostic output never exposes credentials. Check that:

- `credentials.txt` is readable and has exactly two nonempty UTF-8 lines, script name first.
- The site URL uses HTTPS, the API key is valid, and the script can read the named Project.
- TLS trust, proxy settings, and network access to the site are correct.

Do not paste credentials or keys into diagnostic output.

## Explore interactively

Start an interpreter in the application environment, or use the checkout explicitly:

```sh
PYTHONPATH=src python
```

Inspect the package before constructing a client:

```pycon
>>> import shotgun_api3
>>> print(shotgun_api3.__version__)
>>> print(shotgun_api3.__file__)
>>> from shotgun_api3 import Shotgun
```

For method contracts, consult the
[Autodesk Python API reference](https://developers.shotgridsoftware.com/python-api/reference.html)
and this project's [intentional differences](../COMPATIBILITY.md). Native async operations use
`await`; the [README async example](../README.md#asynchronous-client) shows the client lifecycle.
