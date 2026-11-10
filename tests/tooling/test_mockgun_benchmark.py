import json

import pytest

from scripts import mockgun_benchmark
from shotgun_api3.lib import mockgun


def test_benchmark_executes_both_workloads(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    initial_paths = mockgun.Shotgun.get_schema_paths()
    with monkeypatch.context() as patch:
        patch.setattr(mockgun.Shotgun, '_schema_path', initial_paths[0])
        patch.setattr(mockgun.Shotgun, '_schema_entity_path', initial_paths[1])
        assert mockgun_benchmark._main(['--repeat', '1', '--scale', '0.02']) == 0
    report = json.loads(capsys.readouterr().out)
    assert set(report) == {'sync', 'async'}
    assert all(len(client['runs']) == 1 for client in report.values())
    assert mockgun.Shotgun.get_schema_paths() == initial_paths


def test_benchmark_reports_median_per_client(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    initial_paths = mockgun.Shotgun.get_schema_paths()
    samples = {'sync': iter((7.0, 1.0, 4.0)), 'async': iter((10.0, 2.0, 8.0))}
    with monkeypatch.context() as patch:
        patch.setattr(mockgun.Shotgun, '_schema_path', initial_paths[0])
        patch.setattr(mockgun.Shotgun, '_schema_entity_path', initial_paths[1])

        def run_sync(_scale: float) -> float:
            return next(samples['sync'])

        patch.setattr(mockgun_benchmark, '_run_sync', run_sync)

        async def run_async(_scale: float) -> float:
            return next(samples['async'])

        patch.setattr(mockgun_benchmark, '_run_async', run_async)
        assert mockgun_benchmark._main(['--repeat', '3', '--scale', '0.02']) == 0
    report = json.loads(capsys.readouterr().out)
    assert report == {
        'sync': {'runs': [7.0, 1.0, 4.0], 'median': 4.0},
        'async': {'runs': [10.0, 2.0, 8.0], 'median': 8.0},
    }
    assert mockgun.Shotgun.get_schema_paths() == initial_paths
