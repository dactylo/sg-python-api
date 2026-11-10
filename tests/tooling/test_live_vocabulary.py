from __future__ import annotations

import importlib

from tests import _live_safety
from tests._parity import SCENARIOS
from tests.live import _evidence, _session


def test_live_session_tables_share_one_vocabulary() -> None:
    vocabulary = importlib.import_module('tests._live_vocabulary')
    names = vocabulary.SESSION_NAMES
    assert names == ('C', 'L1', 'L2', 'L2b', 'L3', 'L4', 'L5', 'R1', 'R2', 'R3', 'R4', 'Sweep')
    assert set(_session._PROFILES) == set(names)
    assert set(_session._session_scenarios(SCENARIOS)) == set(names)
    assert set(_live_safety._SESSION_RPCS) == set(names)
    assert set(_live_safety._RESERVES) <= set(names)
    assert set(_session._SUDO_CREATE_LIMITS) <= set(names)
    assert set(_session._UNFOLLOW_LIMITS) <= set(names)
    assert _session.session_nodes('L5') == []
    assert _session.session_nodes('Sweep') == []


def test_schema_words_cover_session_names() -> None:
    vocabulary = importlib.import_module('tests._live_vocabulary')
    assert vocabulary.PROBE_KEYS == ('O1', 'O2')
    assert set(vocabulary.SESSION_NAMES) | set(vocabulary.PROBE_KEYS) <= set(
        _evidence._SCHEMA_WORDS.split()
    )
