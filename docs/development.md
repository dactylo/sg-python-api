# Development

Use this guide to set up a development checkout, run checks, and work with package artifacts or a
disposable SG site. To check an application installation, use the [testing guide](testing.md)
instead.

## Set up a development checkout

Install these tools before you set up a checkout:

- Python 3.11 or newer, as `python` on `PATH`. To use another interpreter, run
  `make setup PYTHON=/path/to/python`; setup never downloads Python.
- uv 0.13.6. Setup and the lock targets refuse any other version, so dependency locks resolve the
  same way locally and in CI.
- Taplo 0.10.0 and shfmt 3.14.1. The Markdown formatter uses them for TOML and shell code blocks in
  the docs, and shfmt also formats the repository's shell scripts.
- Biome 2.6.6, which formats the repository's JSON files.
- Node.js 26.12.0, which Pyright uses for type checks. Use the same version locally and in CI.

Then, from the repository root:

```sh
make setup
```

`make setup` creates `.venv` if it does not exist, installs the exact hashed pins from
`requirements-ci.txt`, and installs the project in editable mode. Run it again after changing
`pyproject.toml` or the lock files, including when `make format` rewrites `pyproject.toml`. The
other targets compare `.venv` with those files and refuse to run when it is missing or outdated, so
a check never installs or updates packages.

`requirements-ci.txt` pins the development tools, the `async` extra, and the build backend.
`requirements-test.txt` selects the smaller set that test jobs install, constrained by the same
pins. Neither file ships with the package; published dependency ranges stay in `pyproject.toml`.

CI downloads checksum-verified Linux builds of Taplo and shfmt. The lock leaves out the Python
Taplo package that `mdformat-config` would otherwise pull in, because that package lacks some ARM
wheels and CI uses the native binary instead. When Taplo or shfmt is unavailable, mdformat warns
that it could not format a code block but still succeeds, so read its warnings.

## Run checks and tests

For an offline test run:

```sh
.venv/bin/python -m pytest -q -m 'not live'
```

On Windows, use `.venv/Scripts/python`. To run all quality checks and offline tests together:

```sh
make check TEST_FLAGS="-m 'not live'"
```

Without that marker filter, `make test` and `make check` also select live tests when the first four
`SG_TEST_*` variables are set. Those tests create and retire records; read
[live-site tests](#live-site-tests) before enabling them. `make test` runs serially by default.

The quality checks cover formatting and lint, dependency locks, Pyright type checks for Linux,
macOS, and Windows, shell syntax, trailing whitespace, and an offline zizmor audit of GitHub
Actions workflows.

| Command             | Purpose                                                         |
| ------------------- | --------------------------------------------------------------- |
| `make format`       | Apply formatting; inspect its changes before committing.        |
| `make format-check` | Check formatting and lint without rewriting files.              |
| `make quality`      | Run the complete read-only quality checks.                      |
| `make test`         | Run tests, including configured live tests unless filtered out. |
| `make check`        | Run quality checks and tests.                                   |

## Update dependencies

Use the uv version pinned in CI. After changing dependencies in `pyproject.toml`, run
`make lock-update`. To refresh allowed versions, run `make lock-update LOCK_FLAGS=--upgrade`.
Review both requirements file diffs and run `make lock-check`; `make check` includes that check.

## Continuous integration

The [CI workflow](../.github/workflows/ci.yml) runs `quality` first. The `test` jobs then run in
parallel. `package` starts only after those jobs pass; `hosted-validation` reports the overall
outcome, including failures. Jobs run on GitHub-hosted runners; live conformance is a separate
manual command described below.

After installing their locked Python environments, the quality and test jobs check the Node and
Pyright versions before running quality checks or tests.

The `test` matrix covers Python 3.11 to 3.15 on Linux and Python 3.13 on macOS and Windows. All
test jobs have a one-hour hang guard.

CI runs tests in parallel with pytest-xdist. Set `PYTEST_XDIST_AUTO_NUM_WORKERS` when a runner's
visible CPU count exceeds its CPU quota. For the same scheduling locally, run:

```sh
make test TEST_FLAGS='-n auto --dist worksteal --durations=15'
```

## Artifacts, conformance, and benchmarks

These scripts run from a Git checkout, including a public clone. They write results under the
ignored `dist/` directory and change no tracked files.

### Validate packages

`scripts/validate_artifacts.sh` builds the wheel and source distribution from the committed tree,
checks their contents, installs both wheels in disposable prefixes, and runs the offline tests
against each. It requires a clean tracked tree and a `python` on `PATH` with anyio, hatchling,
httpx2, installer, pytest, and urllib3. `make setup` provides these packages in `.venv`. Run:

```sh
PATH="$PWD/.venv/bin:$PATH" scripts/validate_artifacts.sh
```

The source distribution contains the README, package source, and required build and license files.
Documentation, examples, scripts, and tests remain available in the Git checkout.

A passing run keeps the artifacts in `dist/validated-artifacts/` and records their evidence in
`dist/artifact-results.md`. Remove `dist/validated-artifacts/` before validating again. The
conformance and benchmark scripts install the wheel recorded there. They reject evidence if the
validation did not pass, the wheel changed, or a tracked public input changed after the validated
commit. Public inputs are root files, top-level Markdown guides in `docs/`, and files under
`.github/`, `examples/`, `scripts/`, `src/`, and `tests/`. Changes outside that scope do not
invalidate the evidence. Both scripts need network access and a
[disposable test site](#live-site-tests).

### Run conformance

Conformance runs Autodesk's public API suite against the validated wheel. It needs network access
and a dedicated disposable site without concurrent edits to its fixtures. Set these variables:

| Variable                 | Value                           |
| ------------------------ | ------------------------------- |
| `SG_TEST_SITE`           | The test site's full HTTPS URL. |
| `SG_TEST_SCRIPT`         | A script name.                  |
| `SG_TEST_KEY`            | The script's API key.           |
| `SG_TEST_HUMAN_LOGIN`    | An existing test user's login.  |
| `SG_TEST_HUMAN_PASSWORD` | The test user's password.       |

The configured human user must already exist and authenticate with a legacy login passphrase and
bound Personal Access Token. The runner resolves the user by login; it never creates or activates a
replacement. Its name and credentials are written only to a private temporary configuration that is
removed after the run.

The script account must be able to update the test user's project access and restore user fields,
Asset schema and visibility, and WorkDayRule records. The runner temporarily grants the human user
access to the conformance project and restores the original project list after the suite.

With the validated artifacts and credentials ready, run:

```sh
PATH="$PWD/.venv/bin:$PATH" scripts/run_conformance.sh
```

The runner clones Autodesk's suite at `v3.10.5` and appends its result tally to
`dist/conformance-results.md`.

The runner verifies the upstream test-source hashes and adapts disposable copies to collect public
API tests without the official client's legacy transport. Individual tests tied to unsupported
private methods, bundled `httplib2`, or unsupported server versions are explicitly skipped. Their
exact names and reasons are recorded in `scripts/_conformance_setup.py`; unexpected imports still
fail. Four tests use a reduced setup because their assertions do not require the legacy transport
mocks. Two test methods have explicit expectation adaptations: invalid export arguments must raise
`ShotgunError` without requiring Autodesk's exact message text.

The runtime fixtures snapshot and restore the existing user fields, custom schema and visibility,
and schedule exceptions changed by the tests. The two schema tests use unique field names on each
run, preserving their assertions while avoiding names reserved by previously retired fields. Newly
created test fields, schedule rules, and the sudo test's Version are removed from active records.
SG retains deleted fields in Trash and normal audit events; the runner does not permanently delete
them. Cleanup runs after assertion failures as well as passing tests; a failed restoration fails
the gate. The upstream suite retains its shared conformance project and entity fixtures between
runs. Passing this gate does not establish coverage of skipped tests.

Regular CI runs the offline regression suite, quality checks, and packaging checks. Live
conformance is a separate, manually run acceptance gate that needs site credentials. Run it against
fresh validated artifacts before release; a passing ordinary CI run does not replace it.

### Measure performance

The benchmark writes disposable server records and uploads files. Use a disposable site and project
without other workloads, and account for its budget before running it. It needs the `resource`
module, available on Linux and macOS, and the validated wheel described above.

At the default 2 paired rounds, the workload budget is 18 worker processes, 184 server entities,
and 332,032,784 uploaded bytes. A larger even `BENCH_N`, up to 20, increases the budget to as much
as 162 worker processes, 1,840 server entities, and 3,320,327,840 uploaded bytes. Each worker has a
one-hour timeout, with resource recovery and cleanup checks after interruption.

| Setting              | Value                                                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `SG_TEST_SITE`       | The test site's full HTTPS URL.                                                                                           |
| `SG_TEST_SCRIPT`     | The script name.                                                                                                          |
| `SG_TEST_KEY`        | The script's API key.                                                                                                     |
| `SG_TEST_PROJECT`    | The disposable Project's exact name.                                                                                      |
| `SG_OFFICIAL_CLIENT` | A clone of `https://github.com/shotgunsoftware/python-api` at `v3.10.2`, with that `origin` and no local package changes. |
| `BENCH_N`            | An even number of paired rounds from 2 to 20; default `2`.                                                                |
| `BENCH_SEED`         | Ordering seed; default `0`.                                                                                               |

With those settings in place, run:

```sh
.venv/bin/python scripts/benchmark.py
```

The report is written to `dist/benchmarks/benchmark-results.md`. See
[methodology](benchmarks.md#methodology) for how to interpret latency and memory measurements.

### Refresh wire fixtures

`scripts/probe_wire.py` re-records sanitized fixtures in `tests/fixtures/` from a test site. Its
docstring lists its requirements and site effects.

## Live-site tests

The complete live suite requires these environment variables:

| Variable                 | Value                                                    |
| ------------------------ | -------------------------------------------------------- |
| `SG_TEST_SITE`           | The test site's full HTTPS URL                           |
| `SG_TEST_SCRIPT`         | A script name                                            |
| `SG_TEST_KEY`            | The script's API key                                     |
| `SG_TEST_PROJECT`        | The exact name of the disposable test Project            |
| `SG_TEST_HUMAN_LOGIN`    | A non-admin test user's login                            |
| `SG_TEST_HUMAN_PASSWORD` | That user's password                                     |
| `SG_TEST_EXPORT_PAGE`    | A positive integer ID of a Page available for CSV export |

The test human must be non-admin, unable to edit its own permission rule set, and able to create
the suite's records through sudo and a human session. With default permission groups, Manager is
suitable; Artist is not enough.

Keep credentials, tokens, cookies, signed URLs, and site data out of source code and logs. With the
first four variables set, any pytest run without a `-m 'not live'` filter, including `make check`,
selects live tests as well as offline tests; without them, live tests skip.

If the Project does not exist, `.venv/bin/python scripts/bootstrap_test_site.py` creates it once
using this checkout's client. The script leaves the Project on the site for later runs. The live
suite creates and retires disposable records and uploaded Attachments under that Project and
verifies cleanup. It also checks human authentication, sudo, follow restoration, and read access to
site metadata. Use a test site and accounts permitted to perform those operations. After setup,
run:

```sh
.venv/bin/python -m pytest -q tests/live/ --require-live
```

`--require-live` fails when required configuration is missing or a selected test skips.

The extended set runs separately and requires explicit selection:

```sh
.venv/bin/python -m pytest -q tests/live/ --require-live --live-extended
```

This selects only tests whose names start with `test_extended_`. The default run deselects those
tests. The extended checks create and delete two schema fields (their names remain reserved in
Trash), change and restore project work schedules, resend each client's current test-user
subscription once, and upload and retire one file just over 20,000,000 bytes per client. Follow,
schedule, and subscription state must match their snapshots after restoration.

The live guard stops further requests after HTTP 429 or 503, failed cleanup, or exhaustion of its
request, upload, human-authentication, subscription, or schema budget. The limits are 2,500
requests, 42,100,000 uploaded bytes, 12 human authentications, two subscription writes, and two
schema creates. Run no other workload against the test site at the same time. To share those limits
across the default and extended commands and record sanitized counts and outcomes, pass the same
fresh `--live-evidence-dir dist/live-results` directory to both commands. Reusing an old directory
retains its consumed budget.

Before release, run `make check` with live tests deselected, validate the committed artifacts, run
conformance against those artifacts, then run the default live suite with `--require-live`. Review
extended checks separately when their site effects are authorized. A live run against the working
tree does not validate a different committed artifact.

## Test layout

The tests are grouped by behavior:

| Location           | Coverage                                            |
| ------------------ | --------------------------------------------------- |
| `tests/client/`    | Sync and async client operations                    |
| `tests/transport/` | Transports, retries, pagination, and cancellation   |
| `tests/files/`     | Uploads, downloads, multipart handling, and cookies |
| `tests/lib/`       | Mockgun and timezone compatibility                  |
| `tests/tooling/`   | Package verification and development tools          |
| `tests/live/`      | Opt-in checks against a real test site              |

Shared-module and package-surface tests also live directly under `tests/`. Select an offline area
with a command such as `.venv/bin/python -m pytest -q -m 'not live' tests/files/`. To check an
installation against your own site without the live suite, use the
[read-only smoke test](testing.md#run-a-read-only-smoke-test).

## Package version

The package version is written in two places: `version` in `pyproject.toml`, which the release
workflow reads, and `PACKAGE_VERSION` in `src/shotgun_api3/_version.py`, which the package and the
artifact scripts read. Change both together; a test fails when they differ.

## Following Autodesk releases

For each Autodesk release:

1. Read its public changelog entry and the reference entries it changes.
2. Pin the conformance runner to the release's commit and the SHA-256 of each pinned test file.
3. If nothing callers can observe changed, run the conformance gate and update
   `COMPATIBILITY_VERSION` in `src/shotgun_api3/_version.py`.
4. Otherwise, implement the change with tests first, then run the gate and update the target.
5. Name the new target in [README](../README.md), [COMPATIBILITY](../COMPATIBILITY.md), the
   [migration guide](migration-guide.md), the [testing guide](testing.md), and this development
   guide.
