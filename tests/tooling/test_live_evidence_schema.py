from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.parse import quote

import pytest

from tests.live import _evidence


def test_parity_failure_location_survives_sanitization() -> None:

    parity_path = Path(__file__).resolve().parents[2] / 'tests' / '_parity.py'
    assert _evidence.location(parity_path, 37) == 'tests/_parity.py:37'
    assert _evidence.location(Path('/private/untrusted.py'), 37) is None


@pytest.mark.parametrize(
    'message',
    [
        'Access denied for record Velvet Lantern',
        'synthetic-key-value',
        base64.b64encode(b'synthetic-key-value').decode(),
        b'synthetic-key-value'.hex(),
        quote('synthetic-key-value', safe=''),
        'synthetic-key-value'[::-1],
    ],
)
def test_untrusted_exception_details_are_closed_labels(message: str) -> None:

    class PrivateError(Exception):
        pass

    assert _evidence.exception_label(PrivateError(message)) == 'OtherException'
    assert message not in json.dumps(_evidence.failure(PrivateError(message), []))


@pytest.mark.parametrize(
    'secret', ['LIVE', 'scenario_fragment', 'name_equal', '123-45!', 'exhausted: requests']
)
def test_privacy_scan_ignores_public_vocabulary_and_nonletters(
    tmp_path: Path, secret: str
) -> None:
    node = 'tests/live/test_parity.py::test_shared_scenario[sync-scenario_fragment]'
    (tmp_path / 'session.json').write_text(json.dumps([node, secret]))
    assert _evidence.privacy_hits(tmp_path, [secret], [node]) == 0


@pytest.mark.parametrize('secret', ['private-outside-vocabulary', '@', '://', 'cookie', 'token'])
def test_privacy_scan_keeps_private_needles_and_fixed_patterns(
    tmp_path: Path, secret: str
) -> None:
    (tmp_path / 'session.json').write_text(json.dumps([secret]))
    assert _evidence.privacy_hits(tmp_path, [secret], []) > 0


def test_validation_precedes_write_and_preserves_existing(tmp_path: Path) -> None:

    path = tmp_path / 'budget.json'
    path.write_text('existing')
    with pytest.raises(ValueError, match='evidence'):
        _evidence.write_artifact(path, {'requests': 'Velvet Lantern'}, [])
    assert path.read_text() == 'existing'
    assert list(tmp_path.iterdir()) == [path]
