from typing import cast

from shotgun_api3 import Shotgun
from tests.conftest import sent_payload


def rpc_params(sg: Shotgun, index: int = 0) -> dict[str, object]:
    payload = sent_payload(sg, index)
    params = cast('list[object]', payload['params'])
    return cast('dict[str, object]', params[1])
