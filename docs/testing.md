# Testing an installation and application code

Use this page to confirm that your application's Python environment loads sg-python-api, check that
the client can read from your Flow Production Tracking site, and
[test application code with mockgun](#test-application-code-with-mockgun). If you have not
installed the package yet, start with the [README](../README.md#installation). An import check
confirms which client loads; a read-only smoke test confirms your credentials, permissions, and
connectivity. To run the package's own test suite from a checkout, see the
[development guide](development.md#run-checks-and-tests).

## Verify the import target

Autodesk's `shotgun-api3` and sg-python-api both provide `shotgun_api3`. In the application's
Python environment, print the loaded version and module path:

```sh
python -c "import shotgun_api3; print(shotgun_api3.__version__); print(shotgun_api3.__file__)"
```

Either package reports an Autodesk client version: sg-python-api reports `3.10.5`, the version
whose API it matches. The path shows which package loaded; confirm that it points to the
installation you intend to test. To print the installed sg-python-api version:

```sh
python -c "import importlib.metadata; print(importlib.metadata.version('sg-python-api'))"
```

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
reads. Validate the other operations your application uses separately.

### If the smoke test fails

`Project not found: <name>` means the request succeeded but the script cannot see a Project with
that exact name.

`Unable to run example.` reports errors caught while loading credentials or using the client.
Exception details are hidden to keep credentials out of that error message. If the async example
instead fails to import `AsyncShotgun`, install the `async` extra in the same environment. Check
that:

- `credentials.txt` is readable and has exactly two nonempty UTF-8 lines, script name first.
- The site URL uses HTTPS, the API key is valid, and the script can read the named Project.
- TLS trust, proxy settings, and network access to the site are correct.

Do not paste credentials or keys into diagnostic output.

## Test application code with mockgun

`shotgun_api3.lib.mockgun.Shotgun` and `AsyncShotgun` subclass the real `shotgun_api3.Shotgun` and
`AsyncShotgun` clients. Their requests reach an in-memory site in the same process; no request
leaves the process. Each directly constructed client has its own site. Call `new_client()` to make
another client on the same site. Start by preparing a schema and creating a client below. For
unsupported inputs or uncertain results, see
[Handle unsupported answers](#handle-unsupported-answers).

### Prepare a schema

Use `generate_schema(sg, schema_path, schema_entity_path)` with a client connected to your site to
save its field and entity schemas. Treat those pickle files as trusted input. Configure paths
before constructing a mockgun client. The sync methods are `mockgun.Shotgun.set_schema_paths()` and
`get_schema_paths()`; await `mockgun.AsyncShotgun.set_schema_paths()` and `get_schema_paths()` in
async setup.

```python
from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
assert mockgun.Shotgun.get_schema_paths() == ('schema.pickle', 'schema_entity.pickle')
```

A schema generated from your site provides full field and entity declarations for mockgun's
supported behavior. A partial schema also works: mockgun maintains server fields only where
declared, creates acting user records only for declared user types, writes event rows and
connection records only for declared entity types, and gives a link a display name only when its
target display field is declared. Archive scope uses `Project.archived` only when declared. Schema
reads return the loaded schema as changed by schema writes, without adding missing declarations.
For example, if `HumanUser.sg_status_list` is absent, `authenticate_human_user()` raises `Fault`
for that unknown field.

### Create a client

Use a reserved `.invalid` host such as `https://site.invalid`, so attempts to fetch a mockgun URL
with another library fail immediately. Supply one complete credential form, as with the real
client. The example uses a script name and API key and freezes the clock for repeatable assertions.

```python
from datetime import datetime, timezone

from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
client = mockgun.Shotgun(
    'https://site.invalid', script_name='tests', api_key='test-key', connect=False
)
fixed_time = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
client.site.set_time(fixed_time)
client.site.update_server_metadata({'api_max_entities_per_page': 50})
project = client.create('Project', {'name': 'Demo'})  # First request reads the new metadata.
assert project['name'] == 'Demo'
assert client.site.now() == fixed_time
```

#### Credentials and actors

| Credentials                                        | Actor                                          |
| -------------------------------------------------- | ---------------------------------------------- |
| Script name and API key                            | An `ApiUser` named by the script name.         |
| Login and password                                 | The `HumanUser` with that login.               |
| `sudo_as_login` with credentials                   | The named `HumanUser`.                         |
| Token returned by the site's `get_session_token()` | The issuing client's `ApiUser` or `HumanUser`. |
| Token the site did not issue                       | A placeholder `HumanUser`.                     |
| Base URL alone                                     | The script `mockgun`.                          |

Partial or combined credential forms raise, as they do for the real client. Mockgun creates user
records on first use when the schema declares their type.

### More clients on one site

`new_client(**credentials)` returns the same client class on the same site with exactly the
supplied credentials. Supplying only `sudo_as_login` keeps the original credentials and adds sudo.
Timeout, retry, and user-agent additions are copied. `new_client()` is a plain method on
`AsyncShotgun` too.

```python
from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
client = mockgun.Shotgun('https://site.invalid', script_name='tests', api_key='test-key')
sudo_client = client.new_client(sudo_as_login='artist')
project = client.create('Project', {'name': 'Shared'})
assert sudo_client.find_one('Project', [['id', 'is', project['id']]], ['name'])['name'] == 'Shared'
```

### Async tests and call counts

`mockgun.AsyncShotgun` needs no `async` extra. Await its I/O methods. Each mockgun client's `finds`
attribute counts calls to `find()` and `find_one()` on that client.

### Control the site

`client.site` is a `mockgun.Site`. It offers these test controls:

| Control                                                                                                 | Effect                                                                                                                                                                                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `now()`; `set_time(when)`                                                                               | Read or freeze the clock at an aware datetime. Pass `None` to follow real UTC time again.                                                                                                                                                                                                                           |
| `server_metadata`; `update_server_metadata(values)`                                                     | Read a copy of server metadata or merge values into it. A client sees a change at its next readiness: the first request when constructed with `connect=False`, or an explicit `info()` for an already ready client. Set `api_max_entities_per_page` before that first request when testing a non-default page size. |
| `set_preferences(values)`                                                                               | Merge explicit site preferences.                                                                                                                                                                                                                                                                                    |
| `set_work_week(working)`                                                                                | Set seven booleans, Monday through Sunday.                                                                                                                                                                                                                                                                          |
| `set_subscriptions(values)`                                                                             | Replace subscriptions keyed by `HumanUser` ID.                                                                                                                                                                                                                                                                      |
| `set_canned_response(method, params, response)`; `set_canned_fault(method, params, message, code=None)` | Configure exact parameters for `nav_expand`, `nav_search_string`, `nav_search_entity`, or `export_page`. An unset key raises `MockgunError` and names the parameters.                                                                                                                                               |

```python
from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
client = mockgun.Shotgun('https://site.invalid', script_name='tests', api_key='test-key')
client.site.set_canned_response('nav_expand', {'path': '/Project/1'}, {'children': []})
assert client.nav_expand('/Project/1') == {'children': []}
```

### Files and thumbnails

Uploads keep their bytes in memory, and `download_attachment()` retrieves them through the client.
Storage URLs have the form `https://mockgun-storage.invalid/<path>`;
`get_attachment_download_url()` returns `<base_url>/file_serve/attachment/<id>`. Neither URL is
served to another library. Thumbnails are ready immediately.

When the schema declares `Attachment.this_file`, an uploaded file whose name ends in lowercase
`.txt` and whose bytes are all ASCII reads as a file dictionary with `type` set to `Attachment`.
Reading `this_file` for any other file raises `MockgunError`. If the schema does not declare
`this_file`, reading it raises `MockgunError` even for an ASCII `.txt` file.

```python
from pathlib import Path

from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
client = mockgun.Shotgun('https://site.invalid', script_name='tests', api_key='test-key')
project = client.create('Project', {'name': 'Files'})
note = client.create('Note', {'subject': 'Review', 'project': project})
Path('example.txt').write_bytes(b'Example file\n')
attachment_id = client.upload('Note', note['id'], 'example.txt')
assert client.download_attachment(attachment_id) == b'Example file\n'
file_row = client.find_one('Attachment', [['id', 'is', attachment_id]], ['this_file'])
assert file_row['this_file']['type'] == 'Attachment'
```

### Activity streams

For a supported sudo-created Note linked to a Shot, its Note and Shot activity entries show the
HumanUser creator, the user's `sg_status_list`, a null image, and sudo metadata naming the
underlying script user. A supported human-session Note entry also shows its HumanUser creator.
These reads require a known source event, a declared and known creator status that has not changed
since the write, a present creator, and no creator image. Missing or uncertain inputs raise
`MockgunError`; these rules apply only to the supported creator and event states.

### Handle unsupported answers

Mockgun raises `Fault` for inputs known to be invalid and `shotgun_api3.lib.mockgun.MockgunError`
when the site cannot settle an answer. `MockgunError` is not a `ShotgunError`, so application code
catching `ShotgunError` cannot hide an unknown answer.

A passing mockgun test can differ from a real site, even without `MockgunError`. When a test raises
`MockgunError`, narrow the test to known behavior: give linked records a display field such as
`code`, avoid ordering names that differ only by punctuation or letter case, or supply
server-computed fields that the test reads. A read can also raise when more than 64 combinations of
uncertain record states remain, even if every combination would produce the same answer. Catch
`MockgunError` in a test when the test specifically checks this boundary; do not catch it in
application code.

```python
from shotgun_api3.lib import mockgun

mockgun.Shotgun.set_schema_paths('schema.pickle', 'schema_entity.pickle')
client = mockgun.Shotgun('https://site.invalid', script_name='tests', api_key='test-key')
try:
    client.nav_expand('/Project/1')  # No response was configured for these parameters.
except mockgun.MockgunError:
    pass
else:
    raise AssertionError('expected an unknown mockgun answer')
```

### Deliberate departures

Mockgun accepts any script key, while a real site rejects an invalid one with
`AuthenticationFault`. A `sudo_as_login` naming no existing `HumanUser` creates one. It does not
enforce site permissions, and URLs fetched outside the client are not served. IDs, URLs, and times
differ from a real site; assert relationships among values instead of literal values.

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
