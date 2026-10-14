#!/bin/sh
# Tier-3 conformance gate: run Autodesk's test suite against our shotgun_api3.

set -eu

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
RESULTS_FILE="$REPO_ROOT/dist/artifact-results.md"
LEDGER_FILE="$REPO_ROOT/dist/conformance-results.md"
OFFICIAL_COMMIT='6f56cf219152d0e8a8e92d7f0507b28f02f86905'
OFFICIAL_REF='v3.10.3'
OFFICIAL_REPOSITORY='https://github.com/shotgunsoftware/python-api'
WORK_ROOT=''

fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

cleanup() {
    exit_code=$?
    trap - EXIT HUP INT TERM
    if [ -n "$WORK_ROOT" ] && [ -d "$WORK_ROOT" ]; then
        rm -rf -- "$WORK_ROOT"
    fi
    exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[ -n "${SG_TEST_SITE:-}" ] || fail 'Missing required environment variable: SG_TEST_SITE'
[ -n "${SG_TEST_SCRIPT:-}" ] || fail 'Missing required environment variable: SG_TEST_SCRIPT'
[ -n "${SG_TEST_KEY:-}" ] || fail 'Missing required environment variable: SG_TEST_KEY'
[ -n "${SG_TEST_HUMAN_LOGIN:-}" ] ||
    fail 'Missing required environment variable: SG_TEST_HUMAN_LOGIN'
[ -n "${SG_TEST_HUMAN_PASSWORD:-}" ] ||
    fail 'Missing required environment variable: SG_TEST_HUMAN_PASSWORD'

[ -f "$RESULTS_FILE" ] || fail "Missing artifact validation evidence: $RESULTS_FILE"

EVIDENCE=$(python -I "$REPO_ROOT/scripts/_artifact_evidence.py" "$REPO_ROOT")

saved_ifs=$IFS
IFS='|'
set -- $EVIDENCE
IFS=$saved_ifs
[ "$#" -eq 4 ] || fail 'Artifact validation evidence produced an invalid result'
CANDIDATE_COMMIT=$1
CANDIDATE_VERSION=$2
WHEEL_PATH=$3
WHEEL_SHA256=$4

for command_name in git mktemp python; do
    command -v "$command_name" >/dev/null 2>&1 ||
        fail "Missing required tool: $command_name"
done
if ! python -I -c 'import installer, pytest' >/dev/null 2>&1; then
    fail 'Missing required Python modules: installer and pytest'
fi

umask 077
WORK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sg-conformance.XXXXXX")
if ! RESOLVED_WORK_ROOT=$(
    python -I - "$WORK_ROOT" "$REPO_ROOT" <<'PY'
from pathlib import Path
import sys

try:
    work_root = Path(sys.argv[1]).resolve(strict=True)
    repo_root = Path(sys.argv[2]).resolve(strict=True)
except OSError as error:
    raise SystemExit(f'Cannot resolve disposable work root: {error}') from error
if work_root == repo_root or work_root.is_relative_to(repo_root):
    raise SystemExit('Disposable work root must be outside the repository')
print(work_root)
PY
); then
    exit 1
fi
WORK_ROOT=$RESOLVED_WORK_ROOT
OFFICIAL_ROOT="$WORK_ROOT/official"
INSTALL_ROOT="$WORK_ROOT/installed"
RESULTS_XML="$WORK_ROOT/results.xml"
RAW_RESULTS="$WORK_ROOT/results.txt"
mkdir -p "$INSTALL_ROOT"

git clone --quiet --no-checkout --depth 1 --branch "$OFFICIAL_REF" \
    "$OFFICIAL_REPOSITORY" "$OFFICIAL_ROOT"
resolved_commit=$(git -C "$OFFICIAL_ROOT" rev-parse --verify 'HEAD^{commit}')
[ "$resolved_commit" = "$OFFICIAL_COMMIT" ] ||
    fail "Official suite ref $OFFICIAL_REF did not resolve to $OFFICIAL_COMMIT"
git -C "$OFFICIAL_ROOT" checkout --quiet --detach "$OFFICIAL_COMMIT"
checked_out_commit=$(git -C "$OFFICIAL_ROOT" rev-parse --verify 'HEAD^{commit}')
[ "$checked_out_commit" = "$OFFICIAL_COMMIT" ] ||
    fail "Official suite checkout is not at $OFFICIAL_COMMIT"

python -m installer --destdir "$INSTALL_ROOT" "$WHEEL_PATH" \
    >"$WORK_ROOT/installer.txt" 2>&1
SITE_PACKAGES=$(
    python -I - "$INSTALL_ROOT" <<'PY'
from pathlib import Path
import sys

install_root = Path(sys.argv[1]).resolve()
matches = sorted(
    {
        package.parent.parent.resolve()
        for package in install_root.rglob('shotgun_api3/__init__.py')
        if package.is_file()
    },
)
if len(matches) != 1 or not matches[0].is_relative_to(install_root):
    raise SystemExit('Could not identify one isolated shotgun_api3 installation')
print(matches[0])
PY
)

# The official checkout must not remain an import candidate for its own suite.
rm -rf -- "$OFFICIAL_ROOT/shotgun_api3"

python -I "$REPO_ROOT/scripts/_conformance_setup.py" "$SITE_PACKAGES" "$OFFICIAL_ROOT/tests"

cat >"$WORK_ROOT/conformance_driver.py" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

BOUNDARY_FAILURE = 86


def _fail(message: str) -> None:
    """Stop the driver with a non-pytest boundary status."""
    print(message, file=sys.stderr)
    raise SystemExit(BOUNDARY_FAILURE)


def _module_path(module: ModuleType, install_root: Path) -> Path:
    """Validate and return one candidate module's resolved origin."""
    value = getattr(module, '__file__', None)
    if not isinstance(value, str):
        _fail(f'Loaded module has no filesystem origin: {module.__name__}')
    path = Path(value).resolve()
    if not path.is_relative_to(install_root):
        _fail(f'Loaded module is outside the disposable installation: {module.__name__}')
    return path


def _verify_loaded_modules(install_root: Path) -> None:
    """Prove every loaded candidate module came from the isolated install."""
    found = False
    for name, module in tuple(sys.modules.items()):
        if name == 'shotgun_api3' or name.startswith('shotgun_api3.'):
            if not isinstance(module, ModuleType):
                _fail(f'Loaded shotgun_api3 entry is not a module: {name}')
            _module_path(module, install_root)
            found = True
    if not found:
        _fail('No shotgun_api3 modules were loaded')


site_packages = Path(sys.argv[1]).resolve()
install_root = Path(sys.argv[2]).resolve()
tests_root = Path(sys.argv[3]).resolve()
results_xml = Path(sys.argv[4]).resolve()
expected_version = sys.argv[5]
if not site_packages.is_relative_to(install_root):
    _fail('The candidate site-packages directory is outside the disposable installation')
sys.path.insert(0, str(site_packages))

try:
    import pytest
    import shotgun_api3
except Exception as error:
    _fail(f'Candidate import failed: {error}')

_verify_loaded_modules(install_root)
if shotgun_api3.__version__ != expected_version:
    _fail('Installed candidate version does not match artifact validation evidence')

pytest_status = pytest.main(
    [
        str(tests_root),
        '-p',
        'no:cacheprovider',
        '-q',
        '--tb=no',
        '-rN',
        f'--junitxml={results_xml}',
    ],
)
_verify_loaded_modules(install_root)
raise SystemExit(int(pytest_status))
PY

# The suite receives credentials only through its mode-0600 transient config.
unset SG_TEST_SITE SG_TEST_SCRIPT SG_TEST_KEY
unset SG_TEST_HUMAN_LOGIN SG_TEST_HUMAN_PASSWORD
# Upstream environment overrides must not replace the private resolved config.
unset SG_API_KEY SG_ASSET_CODE SG_HTTP_PROXY SG_HUMAN_LOGIN SG_HUMAN_NAME
unset SG_HUMAN_PASSWORD SG_MOCK SG_PROJECT_NAME SG_SCRIPT_NAME SG_SERVER_URL
unset SG_SESSION_UUID SG_SHOT_CODE SG_TASK_CONTENT SG_VERSION_CODE SG_PLAYLIST_CODE SG_JENKINS
unset PYTHONPATH

set +e
(
    cd "$OFFICIAL_ROOT"
    python -I "$WORK_ROOT/conformance_driver.py" "$SITE_PACKAGES" "$INSTALL_ROOT" \
        "$OFFICIAL_ROOT/tests" "$RESULTS_XML" "$CANDIDATE_VERSION"
) >"$RAW_RESULTS" 2>&1
pytest_status=$?
set -e
if [ "$pytest_status" -gt 1 ]; then
    printf 'Conformance test execution failed (pytest exit %s)\n' "$pytest_status" >&2
    exit "$pytest_status"
fi

COUNTS=$(
    python -I - "$RESULTS_XML" <<'PY'
from __future__ import annotations

import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path


def _fail(message: str) -> None:
    """Stop tally extraction with a sanitized message."""
    raise SystemExit(message)


def _local_name(tag: str) -> str:
    """Remove an optional XML namespace from a tag."""
    return tag.rsplit('}', 1)[-1]


try:
    root = ElementTree.parse(Path(sys.argv[1])).getroot()
except (OSError, ElementTree.ParseError) as error:
    _fail(f'Cannot read sanitized conformance counts: {error}')

if _local_name(root.tag) == 'testsuite':
    suites = [root]
else:
    suites = [element for element in root if _local_name(element.tag) == 'testsuite']
if not suites:
    _fail('Conformance JUnit output contains no test suites')

totals = {'errors': 0, 'failures': 0, 'skipped': 0, 'tests': 0}
for suite in suites:
    for name in totals:
        value = suite.get(name)
        if value is None or not value.isascii() or not value.isdecimal():
            _fail(f'Conformance JUnit output has an invalid {name} count')
        totals[name] += int(value)

failed = totals['failures'] + totals['errors']
passed = totals['tests'] - failed - totals['skipped']
if passed < 0:
    _fail('Conformance JUnit counts are inconsistent')
print(f"{passed}|{failed}|{totals['skipped']}")
PY
)

saved_ifs=$IFS
IFS='|'
set -- $COUNTS
IFS=$saved_ifs
[ "$#" -eq 3 ] || fail 'Conformance JUnit output produced an invalid tally'

result_note='PASS'
if [ "$pytest_status" -eq 1 ]; then
    result_note='FAIL (pytest exit 1)'
fi
python -I - "$LEDGER_FILE" "$CANDIDATE_COMMIT" "$CANDIDATE_VERSION" \
    "$WHEEL_SHA256" "$OFFICIAL_REF" "$OFFICIAL_COMMIT" \
    "$1" "$2" "$3" "$result_note" <<'PY'
from __future__ import annotations

import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

HEADER = (
    '| Date | Candidate commit | Our version | Artifact SHA256 | Official suite ref | '
    'Official suite commit | Passed | Failed | Skipped | Notes |'
)


def _fail(message: str) -> None:
    """Stop ledger recording with a sanitized message."""
    raise SystemExit(message)


ledger_path = Path(sys.argv[1])
(
    candidate_commit,
    candidate_version,
    artifact_hash,
    official_ref,
    official_commit,
    passed,
    failed,
    skipped,
    result_note,
) = sys.argv[2:]

try:
    with ledger_path.open('x', encoding='ascii') as ledger_file:
        ledger_file.write(
            '# Conformance results\n\n'
            f'{HEADER}\n'
            '| ---- | ---------------- | ----------- | --------------- | '
            '------------------ | --------------------- | ------ | ------ | '
            '------- | ----- |\n'
        )
except FileExistsError:
    pass
except OSError as error:
    _fail(f'Cannot create conformance results ledger: {error}')

try:
    ledger = ledger_path.read_text(encoding='ascii')
except (OSError, UnicodeError) as error:
    _fail(f'Cannot read conformance results ledger: {error}')
if ledger.splitlines().count(HEADER) != 1:
    _fail('Conformance results ledger has an invalid table header')

fields = (candidate_version, official_ref, result_note)
if any(not field.isascii() or re.search(r'[|\r\n]', field) for field in fields):
    _fail('Conformance ledger contains an unsafe field')
if re.fullmatch(r'[0-9a-f]{40}', candidate_commit) is None:
    _fail('Conformance ledger candidate commit is invalid')
if re.fullmatch(r'[0-9a-f]{64}', artifact_hash) is None:
    _fail('Conformance ledger artifact SHA-256 is invalid')
if re.fullmatch(r'[0-9a-f]{40}', official_commit) is None:
    _fail('Conformance ledger official commit is invalid')
if any(not count.isascii() or not count.isdecimal() for count in (passed, failed, skipped)):
    _fail('Conformance ledger tally is invalid')
if result_note not in {'PASS', 'FAIL (pytest exit 1)'}:
    _fail('Conformance ledger result note is invalid')

date = datetime.now(UTC).date().isoformat()
row = (
    f'| {date} | {candidate_commit} | {candidate_version} | {artifact_hash} | '
    f'{official_ref} | {official_commit} | {passed} | {failed} | {skipped} | '
    f'{result_note} |\n'
).encode('ascii')
flags = os.O_APPEND | os.O_WRONLY
if hasattr(os, 'O_CLOEXEC'):
    flags |= os.O_CLOEXEC
try:
    descriptor = os.open(ledger_path, flags)
    try:
        written = os.write(descriptor, row)
    finally:
        os.close(descriptor)
except OSError as error:
    _fail(f'Cannot append conformance results ledger: {error}')
if written != len(row):
    _fail('Could not append one complete conformance results row')
PY

printf 'candidate commit: %s\n' "$CANDIDATE_COMMIT"
printf 'candidate version: %s\n' "$CANDIDATE_VERSION"
printf 'artifact sha256: %s\n' "$WHEEL_SHA256"
printf 'official suite ref: %s\n' "$OFFICIAL_REF"
printf 'official suite commit: %s\n' "$OFFICIAL_COMMIT"
printf 'passed: %s\n' "$1"
printf 'failed: %s\n' "$2"
printf 'skipped: %s\n' "$3"
exit "$pytest_status"
