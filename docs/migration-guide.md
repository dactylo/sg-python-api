# Migrating from Autodesk's client

This guide is for applications that use Autodesk's `shotgun_api3` client and want to switch to
sg-python-api. sg-python-api keeps the `shotgun_api3` import name and targets the documented API of
Autodesk's client `3.10.2`, so migration mainly concerns installation, network policy, and failure
handling. Work in your application's test environment first, so you can validate its actual queries
and writes before changing a production deployment. For a new integration, start with the
[README quick start](../README.md#quick-start) instead.

## Check suitability first

You need Python 3.11 or newer and SG server version 7.10.0 or newer. Review the
[compatibility reference](../COMPATIBILITY.md) and check whether your application depends on any of
these:

- Imports of `shotgun_api3.lib.httplib2`, `shotgun.Http`, `shotgun.ProxyInfo`, or `shotgun.socks`
  must change; those implementation internals are not provided.
- Remote authenticated operations require HTTPS. Check custom CA and proxy configuration before
  switching clients.
- Code that displays or compares `shotgun_api3.__version__` must treat it as the sg-python-api
  version, rather than the Autodesk compatibility target.
- Applications that require legacy form-upload success or older-server thumbnail URL synthesis need
  a different supported route. Hosted direct and multipart uploads are supported.

## Install the replacement

Use your application's package manager to remove `shotgun-api3` and install `sg-python-api`. Both
provide `shotgun_api3`, so they cannot coexist in one environment. In a pip-managed environment,
use these commands when a release is available on your package index:

```sh
python -m pip uninstall shotgun-api3
python -m pip install sg-python-api
```

To install from a checkout instead, run the second command there as:

```sh
python -m pip install .
```

When the application needs native async support, install `sg-python-api[async]` from the index or
`'.[async]'` from a checkout. Existing `import shotgun_api3` statements stay as they are.

Next, [confirm which client loads](testing.md#verify-the-import-target) and run the
[read-only smoke test](testing.md#run-a-read-only-smoke-test) with a test-site credential. A
successful import alone does not check authentication or site compatibility.

## Review changes that affect operation

| Area                       | What to review                                                                                                                               | Why it matters                                                                          |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Timeouts                   | Default connect and read limits are 15 and 300 seconds. An explicit `timeout_secs` must be `None` or a finite positive number.               | Applications receive failures instead of waiting indefinitely.                          |
| Retries                    | Reads may retry; unsafe writes retry only before the request is sent. HTTP status and `Retry-After` retries require replay safety.           | Retrying a sent write can duplicate a side effect.                                      |
| Server-requested delays    | Valid `Retry-After` values above 30 seconds raise the current response error.                                                                | The client neither waits without a bound nor retries earlier than the server requested. |
| TLS                        | System trust is the default. `ca_certs=` or `SHOTGUN_API_CACERTS` replaces it with a custom CA bundle.                                       | Include CAs for the site and any upload or download hosts in that bundle.               |
| Site changes and downloads | Changing the site's scheme, host, or port clears credentials you do not supply again. External download URLs receive no site session cookie. | Credentials stay scoped to their intended site.                                         |
| Concurrency                | Sync clients may be shared across threads; async clients stay on one event loop.                                                             | Reuse connections while respecting the async lifecycle.                                 |
| Diagnostics                | Use structured, redacted observers. Set application `httpx2` logging to WARNING when requests may use signed URLs.                           | INFO request logs can include signed URLs.                                              |

The [complete differences](../COMPATIBILITY.md#what-changes-for-your-application) also cover
paging, datetime conversion, download errors, thumbnail sharing, and user-agent behavior.

## Validate your application's workflows

Start with read-only queries and schema access. Then test writes, uploads, downloads, and error
handling against disposable records where needed. Check the results your application depends on,
including permissions and custom fields.

Keep ordinary serial paging during the initial migration. Add concurrency, prefetch, or observer
callbacks after the replacement behaves correctly, so you can tell which change affects a result.
Performance measurements from another site do not determine suitable tuning for your application.

If SG reports an authentication failure, or a request is interrupted during a write, establish the
server outcome before sending the write again. The same rule applies when you cancel an async task
that was writing.

## Adopt native async calls

Install the `async` extra, use one client per event loop, and await I/O methods. Prefer
`async with` so the client closes on both success and failure. The
[README async example](../README.md#asynchronous-client) shows a complete program.

- Import `AsyncShotgun` explicitly; star imports omit optional dependency names.
- With `connect=False`, entering the async context performs no I/O. An explicit `connect()` or the
  first operation that needs the server performs that work.
- Observer callbacks are synchronous and run on the owning event loop, so keep them short.
- Cancel an operation through its task. The async client has no `abort()` method.
- Outside a context manager, call `await sg.close()`.

For application tests, `shotgun_api3.lib.mockgun.AsyncShotgun` works without the `async` extra
because it uses an in-memory implementation. See the
[async reference](../COMPATIBILITY.md#async-extension-surfaces) for the full lifecycle.
