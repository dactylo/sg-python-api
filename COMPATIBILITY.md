# Compatibility

Use this reference to decide whether sg-python-api fits an existing integration and to configure
the behavior that differs from Autodesk's client. For the installation and validation sequence,
start with the [migration guide](docs/migration-guide.md); for a first request, see the
[README quick start](README.md#quick-start).

| If you want to...                                  | Start here                                                                                          |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Check whether your application is in scope         | [Supported scope](#supported-scope)                                                                 |
| Review the differences that affect applications    | [What changes for your application](#what-changes-for-your-application)                             |
| Tune pooling or page prefetch, or observe requests | [Configuration and observer extensions](#configuration-and-observer-extensions)                     |
| Use the async client or async `mockgun`            | [Async extension surfaces](#async-extension-surfaces)                                               |
| Check which configuration names are supported      | [Supported configuration and capability surfaces](#supported-configuration-and-capability-surfaces) |
| Check when plain HTTP is allowed                   | [Authenticated transport security](#authenticated-transport-security)                               |
| Point an existing client at a different site       | [Server floor and readiness](#server-floor-and-readiness)                                           |

Two terms recur on this page. A site's *origin* is its scheme, normalized hostname, and effective
port, where the effective port is the URL's explicit port or else 443 for HTTPS and 80 for HTTP.
The URL path is not part of the origin. Before the first operation against an origin, the client
reads that server's information and checks its version and page limit; this page calls that step
the *readiness check*.

## Supported scope

The package provides the `shotgun_api3` import name and targets the documented synchronous API of
Autodesk's Python client `3.10.3`, including its method signatures, return shapes, errors, filters,
paging, schema, file, activity, navigation, and scheduling operations. It also provides the
`shotgun_api3.lib.mockgun` and `shotgun_api3.lib.sgtimezone` modules and native async extensions.

The target is the documented API, so test the workflows your application uses before adopting the
package in production. Exception-message text is not guaranteed to match Autodesk's client; handle
documented exception types instead of parsing messages.

You need Python 3.11 or newer and a server version of at least 7.10.0. This package and the
official distribution both install `shotgun_api3`, so they cannot coexist in one Python
environment.

## What changes for your application

The most consequential differences concern timeouts, retries, trust, and shared-client use. Other
differences are grouped below so you can check only the areas your application uses.

### Timeouts and retries

Requests have a 15-second connect timeout and a 300-second read timeout by default. Finite waits
let an application recover when a dependency stops responding. To change both limits, assign
`sg.config.timeout_secs` a finite positive number of seconds no larger than the platform's
`threading.TIMEOUT_MAX`. Assigning a Boolean, zero, a negative or non-finite number, or a value
above that limit raises an error immediately. Set it to `None` only when the application
deliberately accepts an unlimited wait.

Retries use one policy for every operation, with backoff and jitter. Reads can retry. A write that
is unsafe to replay retries only when a failure occurs before the request is sent. Once sent, its
result may be ambiguous, so the client does not repeat it automatically. HTTP status retries,
including those directed by `Retry-After`, require an operation that is safe to replay. There is no
exact legacy retry mode.

Tune the number of attempts with `sg.config.max_rpc_attempts` (default `3`). Tune the base delay
with `sg.config.rpc_attempt_interval`, in milliseconds; its default is `3000`, or the value of the
`SHOTGUN_API_RETRY_INTERVAL` environment variable when the client is created.

When a retryable response carries a valid `Retry-After` header of 0 to 30 seconds, the client waits
that long before the next attempt. A larger valid value stops retrying and raises the current
response's error, so the client neither waits without a bound nor retries earlier than the server
asked. This 30-second bound is not configurable. A malformed `Retry-After` value is ignored, and
the client uses its computed backoff.

### Error handling

- RPC faults, including authentication faults, and terminal transport failures raise exceptions in
  the `ShotgunError` family.
- An HTTP error response raises `shotgun_api3.ProtocolError`. This is the standard library's
  `xmlrpc.client.ProtocolError`, which is outside the `ShotgunError` family.
- An invalid configuration value raises `ValueError`, or `TypeError` when the value has the wrong
  type. An authenticated operation against a disallowed HTTP origin also raises `ValueError`.

Handle these families separately. The transport's concrete exception classes are implementation
details, so do not catch them directly.

### Trust and authentication

The client uses the system trust store by default. Passing `ca_certs=` or setting
`SHOTGUN_API_CACERTS` replaces that trust store with the selected bundle, so a custom bundle must
include every CA needed for the site and for any upload or download hosts. Remote authenticated
operations require HTTPS.

Attachment downloads send the site session cookie only to the client's configured origin.
Retargeting a client to a different origin clears any authentication and session values you do not
supply again. These rules prevent credentials from following an unrelated download or site change.
See [authenticated transport security](#authenticated-transport-security) and
[server floor and readiness](#server-floor-and-readiness) for the exact boundaries.

### Concurrency, pooling, and paging

A synchronous `Shotgun` can be shared across threads. An `AsyncShotgun` can be shared across tasks
on its owning event loop. Existing one-client-per-thread code remains valid.

Page fetching is serial by default, which preserves request order. A positive
`sg.config.page_prefetch` opts into bounded lookahead while still returning results in order.
[Configuration extensions](#configuration-and-observer-extensions) explains pooling and prefetch.
Measure your own workload before tuning them.

The client takes its page size from the server's `api_max_entities_per_page` value and leaves that
raw metadata available. When the value is missing, `None`, `0`, or empty, the page size is 500. Any
other malformed or non-positive value fails the readiness check with `ShotgunError`, and the next
operation repeats the check; the client never pages with an invalid size.

### Files, values, and diagnostics

| Behavior                       | Application impact                                                                                                                                                                                                                                                |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Download setup errors          | Setup RPC faults keep their specific types. Setup or transfer HTTP failures and invalid download URLs raise `ShotgunFileDownloadError` with a generic message and no underlying exception details. Configuration and security failures keep their existing types. |
| Multi-target thumbnail sharing | The client waits for source readiness before sharing and does not upload the source again.                                                                                                                                                                        |
| Legacy form uploads            | Servers requiring legacy form uploads are unsupported. The client raises `ShotgunError` before any upload request; capability reads may occur. Hosted direct and multipart routes are supported.                                                                  |
| Datetime-shaped strings        | Invalid values remain unchanged strings. On platforms without pre-1970 time conversion, valid pre-epoch values use the process standard offset without historical daylight-saving accuracy.                                                                       |
| Request diagnostics            | Structured observers omit credentials and bodies. Application `httpx2` INFO logging can include signed URLs; set that logger to WARNING.                                                                                                                          |
| User agent                     | The default includes an `sg-python-api` marker. `add_user_agent()` preserves it, `reset_user_agent()` restores the default, and assigning `sg.config.user_agents` replaces every entry.                                                                           |
| `shotgun_api3.__version__`     | Reports the installed sg-python-api version, rather than Autodesk's compatibility-target version.                                                                                                                                                                 |

### Unsupported legacy surfaces

The package omits `shotgun_api3.lib.httplib2` and the official client's `shotgun.Http`,
`shotgun.ProxyInfo`, and `shotgun.socks` internals. Applications that import them must use a
supported public API or manage their own transport integration.

Python versions below 3.11 cannot install the package, so setting `SHOTGUN_ALLOW_OLD_PYTHON` has no
effect. Servers below 7.10.0 are unsupported, and the official client's thumbnail URL synthesis for
servers older than 3.3.0 is not provided.

## Configuration and observer extensions

These settings have no counterpart in Autodesk's client. Set them on `sg.config` like any other
configuration attribute.

`page_prefetch` controls bounded lookahead for paged `find()` requests. Its default, `0`, keeps
page fetching serial. A positive value N permits N concurrent page requests, with at most N-1 later
pages fetched ahead while results remain ordered; a value of 1 therefore fetches nothing ahead.

`pool_maxsize` defaults to `10`. For the synchronous client, it sets how many connections the pool
keeps open per origin; concurrent demand can still open additional connections. For the
asynchronous client, it caps both simultaneous and retained keep-alive connections.

### Observers

The five observer fields accept a synchronous callback or `None`. Each callback receives one event
record:

| Configuration field        | Event record                                                                         |
| -------------------------- | ------------------------------------------------------------------------------------ |
| `on_compatibility_warning` | `CompatibilityWarningEvent(code, message)`                                           |
| `on_progress`              | `ProgressEvent(operation, route, attempt, bytes_transferred, total_bytes)`           |
| `on_request`               | `RequestEvent(operation, route, http_method, attempt)`                               |
| `on_response`              | `ResponseEvent(operation, route, attempt, latency, status, outcome, correlation_id)` |
| `on_retry`                 | `RetryEvent(operation, route, attempt, delay, status)`                               |

These frozen records contain only allowlisted metadata. They exclude credentials, cookies, request
and response bodies, signed URLs, arbitrary headers, and backend exception text. The
`correlation_id` field contains only the `x-sg-correlation-id` response header, when the server
sends it.

Application logging of `httpx2` is separate from these observers. Its INFO request logs can include
signed upload or download URLs. Set the logger to WARNING or higher when handling signed URLs:

```python
import logging

logging.getLogger('httpx2').setLevel(logging.WARNING)
```

Compatibility warnings use stable codes and fixed messages. The client emits one code,
`legacy-infinite-timeout`, when an operation runs with `sg.config.timeout_secs` set to `None`.

### Callback threads and failures

Callbacks run synchronously and are never awaited, so keep them short. Serial synchronous
operations invoke callbacks on the caller's thread. With `page_prefetch` above 0 and a multi-page
`find()`, callbacks for every page request, including the first, run on the worker thread executing
that request. `AsyncShotgun` invokes callbacks on its owning event loop.

Each progress record is cumulative and ordered within one attempt: `bytes_transferred` is the total
reported so far, and `total_bytes` is `None` when the complete size is unknown. Chunking determines
when progress is reported, so there is no fixed byte interval between records.

If a callback raises an ordinary `Exception`, the client suppresses it and logs a sanitized warning
that names the event type without formatting the exception value. This boundary does not suppress
`BaseException` subclasses. A callback must not start an operation that would wait on the same
in-progress readiness check; the client raises `RuntimeError` instead of waiting on itself.

## Async extension surfaces

### `shotgun_api3.AsyncShotgun`

`AsyncShotgun` provides native asynchronous counterparts to the synchronous client operations.
Install the `async` extra to use it. Await I/O methods; configuration and properties remain
synchronous.

Import it explicitly with `from shotgun_api3 import AsyncShotgun`. Star imports exclude optional
dependency names, including `AsyncShotgun`.

An instance belongs to one event loop and may be shared by tasks on that loop. Prefer `async with`;
otherwise, call `await sg.close()`. Closing drains in-flight work and releases the current
transport, including page requests already reserved by an in-flight `find()`. Once closing begins,
the closing transport rejects unrelated new work, and a later operation reconnects lazily.

The async client has no `abort()` method; cancel an operation by cancelling its task. Cancellation
is not retried. A cancelled write may already have reached the server, so check its outcome before
repeating it.

`AsyncShotgun` validates constructor arguments synchronously but defers network work. The readiness
check and any one-use `auth_token` exchange run at the first of these points: entering an
`async with` block (only with the default `connect=True`), calling `await sg.connect()`, or
starting an operation that needs readiness. Until then, `server_caps` and `server_info` raise
`ShotgunError` and direct the caller to await `connect()` or an RPC.

### `shotgun_api3.lib.mockgun.AsyncShotgun`

`mockgun.AsyncShotgun` delegates awaitable methods to the in-memory `mockgun.Shotgun`
implementation and works without the `async` extra. Use it to exercise async application code
without making network requests. Configure schema files before construction with
`await mockgun.AsyncShotgun.set_schema_paths(field_path, entity_path)`; retrieving them with
`get_schema_paths()` also requires `await`.

Both async extension surfaces inherit the semantics of their synchronous counterparts.
Transport-specific differences apply to `AsyncShotgun`; `mockgun.AsyncShotgun` works in memory and
has no HTTP transport behavior.

## Supported configuration and capability surfaces

Applications may use these configuration and capability surfaces from Autodesk's client:

- Official `sg.config` attributes, with `records_per_page` read-only.
- `sg.config.set_server_params()`.
- `server_caps` and `ServerCapabilities`: `host`, `server_info`, `version`, `is_dev`, and the
  `ensure_*` methods.
- `client_caps` and `ClientCapabilities`: `platform`, `local_path_field`, `py_version`, and
  `ssl_version`.
- Module-level `LOG`, `SG_TIMEZONE`, `SgTimezone`, and `SHOTGUN_API_DISABLE_ENTITY_OPTIMIZATION`.

The following opt-in extensions have no official counterpart:

- `sg.config.page_prefetch`.
- `sg.config.pool_maxsize`.
- `sg.config.on_compatibility_warning`.
- `sg.config.on_progress`.
- `sg.config.on_request`.
- `sg.config.on_response`.
- `sg.config.on_retry`.
- `sg.config.update_fields()`.
- `shotgun_api3.AsyncShotgun`.
- `shotgun_api3.lib.mockgun.AsyncShotgun`.

## Authenticated transport security

Authenticated RPCs, uploads, thumbnail sharing, and same-origin attachment downloads require HTTPS,
because they carry credentials or the site session cookie. HTTP is allowed only for direct
connections to the exact hostname `localhost` (case-insensitive) or a literal loopback IP address:
IPv4 `127.0.0.0/8` or IPv6 `::1`. A configured proxy disallows this exception even when the proxy
itself is on loopback, because it can forward traffic elsewhere. Hostnames that resolve to
loopback, `localhost.`, and subdomains of `localhost` are not exceptions. There is no opt-out for
insecure remote origins.

The client checks this rule when each authenticated operation starts, not when you construct the
client. A client for a remote or proxied HTTP origin can therefore be created, but its first
authenticated operation raises `ValueError` before sending a request. Cached session tokens do not
bypass this check.

Credential-free downloads from external attachment URLs may still use HTTP. A URL with a different
port from the configured origin counts as external and receives no site session cookie.

## Server floor and readiness

The supported server floor is version 7.10.0. The readiness check belongs to one exact origin, not
to the client as a whole: the client rejects a server below the floor before the first operation
other than `info()` for each origin. A successful `info()` call completes the readiness check for
its origin without a duplicate request. After you point a client at a different origin, the next
operation runs a new readiness check for that origin.

Calling `sg.config.set_server_params()` with a new origin clears every authentication and session
value that belongs to the old origin. A change that affects only the API path keeps them. To switch
origins and credentials together, supply the new origin and its replacement credentials in one
atomic `sg.config.update_fields()` call.

For `AsyncShotgun`, the readiness check runs at the points described under
[async extension surfaces](#async-extension-surfaces).
