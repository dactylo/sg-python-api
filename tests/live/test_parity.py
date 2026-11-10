from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._parity import EXTENDED_SCENARIOS, SCENARIOS, Scenario

if TYPE_CHECKING:
    from ._verification import LiveCase

pytestmark = [pytest.mark.live, pytest.mark.async_required, pytest.mark.anyio]


@pytest.mark.parametrize('scenario', SCENARIOS, ids=lambda scenario: scenario.__name__)
async def test_shared_scenario(scenario: Scenario, api_case: LiveCase) -> None:
    await scenario(api_case)


@pytest.mark.parametrize('scenario', EXTENDED_SCENARIOS, ids=lambda scenario: scenario.__name__)
async def test_extended_shared_scenario(scenario: Scenario, api_case: LiveCase) -> None:
    await scenario(api_case)
