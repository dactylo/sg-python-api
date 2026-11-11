# Migrating from Autodesk's client

This guide is for applications that use Autodesk's `shotgun_api3` client and want to switch to
sg-python-api. sg-python-api keeps the `shotgun_api3` import name and targets the documented API of
Autodesk's client `3.10.5`, so migration mainly concerns installation, network policy, and failure
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
- Applications that require legacy form-upload success or older-server thumbnail URL synthesis need
  a different supported route. Hosted direct and multipart uploads are supported.

## Install the replacement

Use your application's package manager to remove `shotgun-api3` and install `sg-python-api`. Both
provide `shotgun_api3`, so they cannot coexist in one environment. In a pip-managed environment,
run these commands from a checkout of `sg-python-api`:

```sh
python -m pip uninstall shotgun-api3
python -m pip install .
```

When the application needs native async support, run `python -m pip install '.[async]'` instead of
the second command. Existing `import shotgun_api3` statements stay as they are.

Next, [confirm which client loads](testing.md#verify-the-import-target) and run the
[read-only smoke test](testing.md#run-a-read-only-smoke-test) with a test-site credential. A
successful import alone does not check authentication or site compatibility.

## Review changes that affect operation

| Area                                                                         | What to check                                                                                                                                                                                   |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Timeouts](../COMPATIBILITY.md#timeouts-and-retries)                         | Default connect and read limits are 15 and 300 seconds. An explicit `timeout_secs` must be `None` or a finite positive number within the platform limit.                                        |
| [Retries](../COMPATIBILITY.md#timeouts-and-retries)                          | Reads may retry; unsafe writes retry only before sending. HTTP status and `Retry-After` retries require replay safety. Valid server-requested delays above 30 seconds raise the response error. |
| [TLS](../COMPATIBILITY.md#trust-and-authentication)                          | System trust is the default. `ca_certs=` or `SHOTGUN_API_CACERTS` replaces it; include CAs for the site and upload/download hosts.                                                              |
| [Site changes and downloads](../COMPATIBILITY.md#server-floor-and-readiness) | Changing scheme, host, or port clears credentials you do not supply again. External download URLs receive no site session cookie.                                                               |
| [Concurrency](../COMPATIBILITY.md#concurrency-pooling-and-paging)            | Sync clients may be shared across threads; async clients stay on one event loop.                                                                                                                |
| [Diagnostics](../COMPATIBILITY.md#observers)                                 | Use structured observers. Set application `httpx2` logging to WARNING to avoid logging signed URLs.                                                                                             |

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
- Cancel an async operation through its task; neither client has an `abort()` method.
- Outside a context manager, call `await sg.close()`.

For application tests, `shotgun_api3.lib.mockgun.AsyncShotgun` works without the `async` extra
because it runs against an in-memory site. See the
[mockgun testing guide](testing.md#test-application-code-with-mockgun) for setup and the
[async reference](../COMPATIBILITY.md#async-extension-surfaces) for the full lifecycle.

## Migrate tests that use mockgun

Use the [testing guide](testing.md#test-application-code-with-mockgun) to prepare a schema and
create a client. The sections below cover expectations to check when adapting existing tests.
Mockgun returns results for supported inputs, raises `Fault` for known rejected inputs, and raises
`MockgunError` when it cannot settle an answer. A passing mockgun test does not guarantee the same
result from a real site; check the [model's limits](../COMPATIBILITY.md#mockgun-test-double).

### Configure clients and schemas

Supply one complete credential form: script name and key, login and password, or a supported token.
Omit credentials to act as the script `mockgun`. Partial and combined forms raise. A script acts as
an `ApiUser`; a login or `sudo_as_login` acts as a `HumanUser`. A site-issued session token keeps
its issuer's actor; another token acts as a placeholder `HumanUser`.

Expect normalized host and URL values. Change page size through `site.update_server_metadata()`
before the first request, or call `info()` to refresh a ready client. Use `new_client()` to share a
site between clients.

Set schema paths before constructing clients. Schema reads return copies; use schema write methods
to change the schema, and expect `Fault` when reading an undeclared field. Use the client API or
`site` controls instead of private record or schema storage.

### Check records and values

Read the dictionary returned by `update()` directly. An empty update returns identity keys without
a server-side entity validation request. `create()` returns submitted fields and identity keys,
plus declared requested fields.

Compare links by `type` and `id`, or include the target's display `name`. Give linked records a
display field when the test reads that name.

Expect multi-entity fields to list their members by display name, as a site does, whatever order
you wrote them in. Names compare character by character without regard to case, so `s10` comes
before `s9`, and equal names fall back to ID order. A `set` that only reorders members changes
nothing. When a member's type has no display field in the schema, mockgun keeps the written order
if it agrees with the named members, and raises `MockgunError` otherwise.

Compare datetime instants after conversion. With default UTC conversion, expect lost microseconds
and local aware values. Date objects travel as ISO strings; valid ISO datetime strings and datetime
objects have the same wire value.

### Query and order records

Assert the requested `limit` and `page` slice. Use list or tuple conditions, or nested
`filter_operator` / `filters` groups. Malformed input fails when the call is made, including on
empty tables or branches that would otherwise short-circuit. Use supported value types and nonempty
membership operands.

Expect `Fault` for rejected relations and `MockgunError` for relations whose answer mockgun cannot
settle. Use `is None` for nulls, and compare multi-entity members by type and ID.

Put the primary order clause first. For supported non-image fields, nulls sort last in either
direction. Status order follows declared `valid_values`; entity order follows display names. Avoid
ordering fixtures whose names differ only in case or ignored punctuation.

Supported `additional_filter_presets` filter results. With `Project.archived` declared, set
`include_archived_projects` explicitly when the test needs archived records.

Use `maximum`, `minimum` and `average` for summaries, rather than short aliases. Narrow tests whose
grouping or search match order is unsettled. A single candidate Task with an ASCII display name
containing no whitespace, a visible Shot link, a visible Project and no image does not match a
one-term ASCII search without explicit `project_ids`. Do not extend that rule to other Task search
forms.

### Check writes and retirement

Declare the fields your test reads. Supported writes maintain audit fields; attempts to update
those fields raise `Fault`. A same-value update leaves audit fields unchanged. Supply uncertain
computed fields explicitly or expect `MockgunError`.

Assert batch atomicity: a failed batch leaves no earlier writes visible. Use declared
`valid_values`, account for paired-field effects, and expect `Fault` for rejected writes.

Assert supported retirement cascades and hidden links until revival: a link to a retired record
reads as empty on active and retired records alike. A write that names a retired record in a link
field raises `Fault`, as it does on a site. Reads or writes involving an unsettled cascade can
raise `MockgunError`; narrow the fixture or revive the parent.

For event rows, assert known types, attributes and links instead of generated descriptions or
literal IDs. With `Attachment.this_file` and the supported event fields declared, a plain script
Note upload of an ASCII lowercase `.txt` file emits four rows when its display name equals its
filename, and five when the names differ.

Event reads raise `MockgunError` after any write whose rows mockgun cannot reproduce. Creating a
Project is one: a site also logs rows for the pages it sets up for the new Project.

### Check files, threads, and activity

Use client upload and download methods; bytes stay in the in-memory site. Reading
`Attachment.this_file` requires its schema declaration and a lowercase `.txt` file containing ASCII
bytes. Other reads raise `MockgunError`.

Clearing `image` also clears `filmstrip_image`. After a failed share from an empty Asset source, a
populated non-filmstrip Asset target's image is cleared in the supported case.

For a script reader, a Reply authored by an `ApiUser` omits requested `entity` in its thread
projection; a direct Reply read retains it. Supported sudo-created Note activity shows the
`HumanUser` creator's declared status and a null image; supported human-session Note activity also
shows its creator. Other creator or event states can raise `MockgunError`. See
[activity streams](testing.md#activity-streams) for the conditions these reads require.

### Use site controls and handle errors

Use client methods and [site controls](testing.md#control-the-site) for follows, preferences, work
schedules, subscriptions, Project access and navigation. Configure exact canned navigation or
export responses; an unconfigured call raises `MockgunError`.

Catch `Fault` for known server rejections and `MockgunError` for answers the site cannot settle.
Match a `Fault` by class unless its precise code or text is part of the contract you need to test.
Keep `MockgunError` handling in tests; application code should not hide unknown answers. See
[handling unsupported answers](testing.md#handle-unsupported-answers) for ways to narrow a fixture.
