from __future__ import annotations

import base64
import json
import signal
import subprocess
import sys
import time
from typing import TYPE_CHECKING
from urllib.parse import quote

import pytest

from tests.tooling._session_process import ROOT, environment

if TYPE_CHECKING:
    from pathlib import Path

_PLANTED = 'VelvetLanternPlantedValue'
_KEY = 'synthetic/key+value='
_MESSAGES = (
    _PLANTED,
    'Access denied for record Velvet Lantern',
    _KEY,
    base64.b64encode(_KEY.encode()).decode(),
    _KEY.encode().hex(),
    quote(_KEY, safe=''),
    _KEY[::-1],
)


def _run_session(
    args: list[str], *, cwd: Path, env_path: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-m', 'tests.live._session', *args],
        cwd=cwd,
        env=environment(env_path),
        capture_output=True,
        text=True,
        check=False,
    )


def _write_target(path: Path, nodes: list[str]) -> None:
    path.joinpath('target.py').write_text(f"""
from tests.tooling._cleanup_site import case

def make_case(kind, namespace, budget):
    live_case, site = case(kind)
    live_case.namespace = namespace
    live_case.target.budget = budget
    return live_case

def nodes(session):
    return {nodes!r}
""")


def _script(path: Path, *, capture: bool = False, interrupt: bool = False) -> None:
    _write_target(path, ['test_synthetic.py::test_first', 'test_synthetic.py::test_second'])
    action = (
        'Path("ready").write_text("yes"); time.sleep(30)'
        if interrupt
        else f'raise {_PLANTED}({" | ".join(_MESSAGES)!r})'
    )
    path.joinpath('test_synthetic.py').write_text(f"""
import os
import time
import warnings
from pathlib import Path
import pytest
pytestmark = pytest.mark.live
class {_PLANTED}(Exception): pass
@pytest.fixture
def owned():
    yield
    Path('cleaned').write_text('yes')
def test_first(owned{', capfd' if capture else ''}):
    print('{_PLANTED}')
    os.write(1, b'{_PLANTED}')
    warnings.warn('{_PLANTED}', stacklevel=1)
    {action}
def test_second():
    Path('second').write_text('ran')
""")


def test_call_mismatch_is_safe_and_later_cases_run(tmp_path: Path) -> None:
    _script(tmp_path)
    result = _run_session(['C', 'evidence', '--target', 'target'], cwd=tmp_path, env_path=tmp_path)
    assert result.returncode == 1, result.stdout
    digest = json.loads(result.stdout)
    assert (digest['clean'], digest['safe'], digest['mismatches']) == (False, True, 1)
    assert tmp_path.joinpath('second').read_text() == 'ran'
    session = json.loads(tmp_path.joinpath('evidence/C/session.json').read_text())
    assert session['stop_template'] == ''


def test_directed_sweep_reports_unverified_baselines(tmp_path: Path) -> None:
    result = _run_session(
        ['--sweep', str(tmp_path), '--target', 'tests.tooling._live_standin'],
        cwd=ROOT,
        env_path=tmp_path,
    )
    assert result.returncode == 0, result.stdout
    evidence = json.loads((tmp_path / 'Sweep/session.json').read_text())
    assert set(evidence['baselines'].values()) == {'UNVERIFIED'}
    assert evidence['prefix_after'] == 0


def test_l4_deselects_week_overlapping_baseline_rule(tmp_path: Path) -> None:
    tmp_path.joinpath('overlap_target.py').write_text("""
import inspect
from datetime import timedelta
from tests.tooling._live_standin import make_case as original
async def make_case(kind, namespace, budget):
    case = await original(kind, namespace, budget)
    day = case.target.run_date + timedelta(days=45)
    day -= timedelta(days=day.weekday())
    result = case.client.client.work_schedule_update(
        day.isoformat(), False, 'baseline', project=case.project
    )
    if inspect.isawaitable(result): await result
    return case
""")
    result = _run_session(
        ['L4', str(tmp_path / 'evidence'), '--target', 'overlap_target'],
        cwd=ROOT,
        env_path=tmp_path,
    )
    # A baseline schedule event is intentionally unsettled in the stand-in cursor.
    assert result.returncode == 2, result.stdout
    evidence = json.loads((tmp_path / 'evidence/L4/session.json').read_text())
    assert len(evidence['collected']) == 8
    assert len(evidence['selected']) == 7
    locations = [
        location
        for phases in evidence['tests'].values()
        for failure in phases.get('failures', {}).values()
        for location in failure['failure_locations']
    ]
    assert any(location.startswith('tests/_parity.py:') for location in locations)
    assert set(evidence['deselected'].values()) == {'schedule-overlap'}
    assert set(evidence['baselines'].values()) == {'equal'}


def test_refusal_inside_async_test_keeps_teardown_and_stop(tmp_path: Path) -> None:
    _write_target(
        tmp_path, ['test_async.py::test_refused[sync]', 'test_async.py::test_refused[async]']
    )
    tmp_path.joinpath('test_async.py').write_text("""
import pytest
pytestmark = pytest.mark.live
@pytest.fixture
def anyio_backend():
    return 'asyncio'
@pytest.mark.anyio
async def test_refused(api_case):
    await api_case.call('follow', {'type': 'HumanUser', 'id': 2}, {'type': 'Shot', 'id': 9})
""")
    result = _run_session(['C', 'evidence', '--target', 'target'], cwd=tmp_path, env_path=tmp_path)
    assert result.returncode == 2, result.stdout
    session = json.loads(tmp_path.joinpath('evidence/C/session.json').read_text())
    first = session['tests']['test_async.py::test_refused[sync]']
    assert (first['call'], first['teardown']) == ('failed', 'passed')
    assert first['failures']['call']['template'] == 'refused'
    assert session['stop_template'] == 'refused'
    assert session['cleanup_verified']
    assert session['executed'] == ['test_async.py::test_refused[sync]']


@pytest.mark.parametrize('capture', [False, True])
def test_session_suppresses_all_untrusted_output(tmp_path: Path, *, capture: bool) -> None:
    _script(tmp_path, capture=capture)
    source_files = set(tmp_path.iterdir())
    result = _run_session(['C', 'evidence', '--target', 'target'], cwd=tmp_path, env_path=tmp_path)
    assert result.returncode != 0
    assert all(value not in result.stdout + result.stderr for value in _MESSAGES)
    for created in tmp_path.rglob('*'):
        if created.is_file() and created not in source_files:
            assert all(value.encode() not in created.read_bytes() for value in _MESSAGES)
    digest = json.loads(result.stdout)
    assert digest['session'] == 'C'
    assert result.stderr == ''
    for path in tmp_path.joinpath('evidence').rglob('*'):
        if path.is_file():
            assert all(value not in path.read_text() for value in _MESSAGES)
    session = json.loads(tmp_path.joinpath('evidence/C/session.json').read_text())
    if capture:
        assert session['stop_template'] == 'collection'
    else:
        assert (
            session['tests']['test_synthetic.py::test_first']['failures']['call']['error_type']
            == 'OtherException'
        )


@pytest.mark.skipif(
    sys.platform == 'win32', reason='Windows terminates the child instead of delivering SIGTERM'
)
def test_sigterm_runs_current_cleanup_and_no_next_case(tmp_path: Path) -> None:
    _script(tmp_path, interrupt=True)
    with subprocess.Popen(
        [sys.executable, '-m', 'tests.live._session', 'C', 'evidence', '--target', 'target'],
        cwd=tmp_path,
        env=environment(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        deadline = time.monotonic() + 10
        while (
            not tmp_path.joinpath('ready').exists()
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        assert tmp_path.joinpath('ready').exists()
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
    assert tmp_path.joinpath('cleaned').read_text() == 'yes'
    assert not tmp_path.joinpath('second').exists()
    assert json.loads(stdout)['clean'] is False
    assert stderr == b''


@pytest.mark.parametrize('session', ['L4', 'L5'])
def test_standin_sessions_restore_exact_baselines(tmp_path: Path, session: str) -> None:
    result = _run_session(
        [session, str(tmp_path), '--target', 'tests.tooling._live_standin'],
        cwd=ROOT,
        env_path=tmp_path,
    )
    assert result.returncode == 0, result.stdout
    evidence = json.loads((tmp_path / session / 'session.json').read_text())
    assert set(evidence['baselines'].values()) == {'equal'}
    assert evidence['cleanup_verified']
    assert evidence['prefix_after'] == 0
    assert evidence['requests_by_test']['cleanup'] > 0
    if session == 'L5':
        probe = json.loads((tmp_path / session / 'probe.json').read_text())
        assert probe['O1'] == {**probe['O1'], 'status': 'Settled', 'error': ''}
        assert probe['O2'] == {**probe['O2'], 'status': 'Settled', 'error': ''}


@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('residue', ['none', 'record', 'baseline'])
def test_standin_session_verifies_each_case_site(tmp_path: Path, kind: str, residue: str) -> None:
    node = f'test_case.py::test_probe[{kind}]'
    tmp_path.joinpath('target.py').write_text(f"""
from tests.tooling._live_standin import make_case
def nodes(session):
    return [{node!r}]
""")
    tmp_path.joinpath('test_case.py').write_text(f"""
import inspect
import pytest
pytestmark = pytest.mark.live
@pytest.fixture
def anyio_backend():
    return 'asyncio'
@pytest.mark.anyio
async def test_probe(api_case):
    residue = {residue!r}
    if residue == 'baseline':
        result = api_case.client.client.update(
            'Project', api_case.project['id'], {{'archived': True}}
        )
        if inspect.isawaitable(result): await result
    else:
        owner = api_case.client if residue == 'record' else api_case
        await owner.create('Shot', {{
            'code': api_case.namespace + '-probe', 'project': api_case.project
        }})
""")
    result = _run_session(['C', 'evidence', '--target', 'target'], cwd=tmp_path, env_path=tmp_path)
    digest = json.loads(result.stdout)
    evidence = json.loads((tmp_path / 'evidence/C/session.json').read_text())
    clean = residue == 'none'
    assert result.returncode == (0 if clean else 2), result.stdout
    assert digest['clean'] is clean
    assert digest['safe'] is clean
    assert digest['mismatches'] == 0
    assert evidence['cleanup_verified'] is clean
    assert evidence['tests'][node]['call'] == 'passed'
    assert evidence['tests'][node]['teardown'] == ('passed' if clean else 'failed')
    assert evidence['prefix_after'] == 0
    assert set(evidence['baselines'].values()) == {'equal'}
