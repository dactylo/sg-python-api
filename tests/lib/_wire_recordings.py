import base64
import json
from pathlib import Path
from typing import cast

FIXTURES = Path(__file__).parents[1] / 'fixtures'


def recording(name: str) -> dict[str, object]:
    return cast('dict[str, object]', json.loads((FIXTURES / f'{name}.json').read_text()))


def response(recording: dict[str, object]) -> object:
    response = cast('dict[str, object]', recording['response'])
    return json.loads(base64.b64decode(cast('str', response['content_b64'])))
