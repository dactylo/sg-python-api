# Development

## Local environment

Install these tools before you set up a checkout:

- Python 3.11 or newer, as `python` on `PATH`. To use another interpreter, run
  `make setup PYTHON=/path/to/python`; setup never downloads Python.
- uv 0.12.23. Setup and the lock targets refuse any other version, so dependency locks resolve the
  same way locally and in CI.
- Taplo 0.10.0 and shfmt 3.14.1. The Markdown formatter uses them for TOML and shell code blocks in
  the docs, and shfmt also formats the repository's shell scripts.
- Biome 2.5.14, which formats the repository's JSON files.
- Node.js 24.21.0, which Pyright uses for type checks. CI installs this exact version in its
  quality and test jobs.

Then, from the repository root:

```sh
make setup
make check
```

`make setup` creates `.venv` if it does not exist, installs the exact hashed pins from
`requirements-ci.txt`, and installs the project in editable mode. Run it again after changing
`pyproject.toml` or the lock files, including when `make format` rewrites `pyproject.toml`. The
other targets compare `.venv` with those files and refuse to run when it is missing or outdated, so
a check never installs or updates packages.

`make quality` runs the read-only checks that CI's quality job runs: formatting and lint,
dependency locks, Pyright type checks for Linux, macOS, and Windows, shell script syntax, and
trailing whitespace. `make check` adds the test suite, and `make test` runs the suite alone.
`make format` applies the formatters; review its changes before committing.

`requirements-ci.txt` pins the development tools, the `async` extra, and the build backend.
`requirements-test.txt` selects the smaller set that test jobs install, constrained by the same
pins. Neither file ships with the package; published dependency ranges stay in `pyproject.toml`.

CI downloads checksum-verified Linux builds of Taplo and shfmt. The lock leaves out the Python
Taplo package that `mdformat-config` would otherwise pull in, because that package lacks some ARM
wheels and CI uses the native binary instead. When Taplo or shfmt is unavailable, mdformat warns
that it could not format a code block but still succeeds, so read its warnings.

## Continuous integration

The [CI workflow](../.github/workflows/ci.yml) runs `quality` first. The `test` jobs then run in
parallel. `package` starts only after those jobs pass; `hosted-validation` reports the outcome of a
hosted run, including failures.

After installing their locked Python environments, the quality and test jobs check the Node and
Pyright versions before running quality checks or tests.

CI uses GitHub-hosted runners unless a run explicitly requests local runners. In a private
repository, the Actions variables `CI_LINUX_RUNNER`, `CI_MACOS_RUNNER`, and `CI_WINDOWS_RUNNER`
select local runners for runs started from Actions > CI > Run workflow with the hosted option
disabled. If a variable is absent, that operating system uses a hosted runner. Pushes, pull
requests, public repositories, and runs with the hosted option enabled use hosted runners for every
job and run every test.

A staging repository sets the Actions variable `CI_RUN_ON_PUSH` to `false`. Its pushes then appear
as `CI / push skipped` and run no jobs; CI runs only on request. A newer requested run on a branch
cancels an older one. Push runs are never cancelled, because the Release workflow needs the tagged
commit's push run.

| Requested run                                                 | Actions > CI > Run workflow | Title               |
| ------------------------------------------------------------- | --------------------------- | ------------------- |
| Quick: local runners, slow tests omitted on macOS and Windows | hosted off, slow tests off  | `CI / local`        |
| Full: local runners, every test                               | hosted off, slow tests on   | `CI / local (full)` |
| Hosted: GitHub-hosted runners, every test                     | hosted on                   | `CI / hosted`       |

The `test` matrix covers Python 3.11 to 3.15 on Linux and Python 3.13 on macOS and Windows. All
test jobs have a one-hour hang guard, on local and hosted runners, in quick and full runs.

CI runs tests in parallel with pytest-xdist. Set `PYTEST_XDIST_AUTO_NUM_WORKERS` when a runner's
visible CPU count exceeds its CPU quota. For the same scheduling locally, run:

```sh
make test TEST_FLAGS='-n auto --dist worksteal --durations=15'
```

The manual [Release workflow](../.github/workflows/release.yml) publishes the package to PyPI after
checking its version tag and a successful push CI run for the tagged commit.

## Dependency updates

Use the uv version pinned in CI. After changing dependencies in `pyproject.toml`, run
`make lock-update`. To refresh allowed versions, run `make lock-update LOCK_FLAGS=--upgrade`.
Review both requirements file diffs and run `make lock-check`; `make check` includes that check.

## Project notes

`make test` runs the suite serially. Quick CI runs (`full_tests=false`) exclude no tests, because
none are marked `slow`. For a guaranteed offline test run, use
`.venv/bin/python -m pytest -q -m 'not live'` (or the Windows equivalent). See
[live-site tests](#live-site-tests) for configuration and site effects.

### Artifacts, conformance, and benchmarks

These scripts run from a Git checkout, including a public clone. They write results under the
ignored `dist/` directory and change no tracked files.

`scripts/validate_artifacts.sh` builds the wheel and source distribution from the committed tree,
checks their contents, installs both wheels in disposable prefixes, and runs the offline tests
against each. It requires a clean tracked tree and a `python` on `PATH` with anyio, hatchling,
httpx2, installer, pytest, and urllib3. `make setup` provides these packages in `.venv`. Run:

```sh
PATH="$PWD/.venv/bin:$PATH" scripts/validate_artifacts.sh
```

A passing run keeps the artifacts in `dist/validated-artifacts/` and records their evidence in
`dist/artifact-results.md`. Remove `dist/validated-artifacts/` before validating again. The
conformance and benchmark scripts install the wheel recorded there. They reject evidence if the
validation did not pass, the wheel changed, or a tracked public input changed after the validated
commit. Public inputs are root files, top-level Markdown guides in `docs/`, and files under
`.github/`, `examples/`, `scripts/`, `src/`, and `tests/`. Changes outside that scope do not
invalidate the evidence. Both scripts need network access and a
[disposable test site](#live-site-tests).

Run `scripts/run_conformance.sh` with the same `PATH` and with `SG_TEST_SITE`, `SG_TEST_SCRIPT`,
`SG_TEST_KEY`, `SG_TEST_HUMAN_LOGIN`, and `SG_TEST_HUMAN_PASSWORD` set. It clones Autodesk's suite
at `v3.10.3`, runs it against the validated wheel, and appends a tally to
`dist/conformance-results.md`.

The configured human user must already exist and authenticate with a legacy login passphrase and
bound Personal Access Token. The runner resolves the user by login; it never creates or activates a
replacement. Its name and credentials are written only to a private temporary configuration that is
removed after the run.

The script account must be able to update the test user's project access and restore user fields,
Asset schema and visibility, and WorkDayRule records. The runner temporarily grants the human user
access to the conformance project and restores the original project list after the suite. Use a
dedicated disposable site without concurrent edits to the test fixtures.

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

Run `.venv/bin/python scripts/benchmark.py` with `SG_TEST_SITE`, `SG_TEST_SCRIPT`, `SG_TEST_KEY`,
`SG_TEST_PROJECT`, and `SG_OFFICIAL_CLIENT` set. `SG_OFFICIAL_CLIENT` points to a clone of
`https://github.com/shotgunsoftware/python-api` checked out at `v3.10.2`, with that `origin` and no
local package changes. The benchmark needs the `resource` module (Linux or macOS). `BENCH_N`
selects an even number of rounds from 2 to 20 (default 2), and `BENCH_SEED` selects the ordering
seed (default 0). It writes `dist/benchmarks/benchmark-results.md`; see the
[methodology](benchmarks.md#methodology) for its workload.

`scripts/probe_wire.py` re-records sanitized fixtures in `tests/fixtures/` from a test site. Its
docstring lists its requirements and site effects.

### Test layout

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

### Live-site tests

Live tests require all four environment variables: `SG_TEST_SITE` is the test site's full HTTPS
URL, `SG_TEST_SCRIPT` is a script name, `SG_TEST_KEY` is its API key, and `SG_TEST_PROJECT` is the
exact name of a Project the script can read. Keep the key out of source code and logs. With those
variables set, any pytest run without a `-m 'not live'` filter, including `make check`, selects
live tests as well as offline tests; without them, live tests skip.

If the Project does not exist, `.venv/bin/python scripts/bootstrap_test_site.py` creates it once
using this checkout's client. The script leaves the Project on the site for later runs. The live
suite creates and deletes disposable Shots, Notes, and uploaded Attachments under that Project and
verifies cleanup. Use a test site and an account permitted to perform those operations. After
setup, run:

```sh
.venv/bin/python -m pytest -q tests/live/ --require-live
```

`--require-live` fails when required configuration is missing or a selected test skips.
