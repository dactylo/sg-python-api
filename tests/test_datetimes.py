import copy
import datetime
import json
import tempfile
import time
from pathlib import Path

import pytest

from shotgun_api3 import _protocol, sg_timezone
from shotgun_api3.lib import sgtimezone as sgtimezone_module
from tests._pyright import run_pyright


def _naive_datetime(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(f'{value}+00:00').replace(tzinfo=None)


@pytest.mark.parametrize(
    'value',
    [
        '2026-07-04t12:00:00Z',
        '2026-07-04T12:00:00z',
        '2026-07-04T12:00:00.000Z',
        '2026-07-04T12:00:00+00:00',
        'prefix 2026-07-04T12:00:00Z',
        '2026-07-04T12:00:00Z suffix',
    ],
)
def test_inbound_datetime_parsing_requires_the_exact_wire_shape(value: str) -> None:
    assert _protocol.transform_inbound(value, convert_utc=False) == value


def test_inbound_datetime_string_parses_and_only_lt_html_entity_restores() -> None:
    data: dict[str, object] = {
        'when': '2026-07-04T12:00:00Z',
        'desc': 'a &lt;b&gt; &amp; c',
    }

    result = _protocol.transform_inbound(data, convert_utc=False)

    when = result['when']
    assert when == _naive_datetime('2026-07-04T12:00:00')
    assert isinstance(when, datetime.datetime)
    assert when.tzinfo is None
    assert result['desc'] == 'a <b&gt; &amp; c'


def test_inbound_preserves_container_topology_and_input() -> None:
    data: dict[str, object] = {
        'items': ('2026-07-04T12:00:00Z', ['a &lt;b', 7]),
    }
    original = copy.deepcopy(data)

    result = _protocol.transform_inbound(data, convert_utc=False)

    assert result == {'items': (_naive_datetime('2026-07-04T12:00:00'), ['a <b', 7])}
    assert isinstance(result, dict)
    assert isinstance(result['items'], tuple)
    assert isinstance(result['items'][1], list)
    assert result is not data
    assert data == original


@pytest.mark.parametrize('error_type', [OSError, OverflowError, ValueError])
def test_inbound_preserves_datetime_when_local_conversion_fails(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    value = '1960-01-01T00:00:00Z'

    def raise_error(_value: datetime.datetime) -> datetime.datetime:
        raise error_type('local conversion failed')

    monkeypatch.setattr(sg_timezone.local, 'fromutc', raise_error)

    assert _protocol.transform_inbound(value, convert_utc=True) == value


@pytest.mark.parametrize(
    'value',
    [
        '0000-01-01T00:00:00Z',
        '2024-02-30T12:00:00Z',
        '2026-13-01T00:00:00Z',
        '2026-12-01T24:00:00Z',
    ],
)
def test_inbound_preserves_invalid_datetime_shaped_strings(value: str) -> None:
    data: dict[str, object] = {
        'invalid': [value],
        'valid': '2026-07-04T12:00:00Z',
    }

    result = _protocol.transform_inbound(data, convert_utc=False)

    assert result['invalid'] == [value]
    assert result['valid'] == _naive_datetime('2026-07-04T12:00:00')


def test_inbound_utc_datetime_uses_public_local_timezone_singleton() -> None:
    utc = datetime.datetime(2026, 7, 4, 12, 0, 0, tzinfo=datetime.UTC)
    expected = utc.astimezone()

    result: object = _protocol.transform_inbound(
        '2026-07-04T12:00:00Z',
        convert_utc=True,
    )

    assert result == expected
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is sg_timezone.local


def test_outbound_adjusted_range_overflow_has_stable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 0)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', -3_600)

    with pytest.raises(
        ValueError,
        match=r'^datetime conversion exceeds the supported range$',
    ) as caught:
        _protocol.transform_outbound(
            _naive_datetime('0001-01-01T00:00:00'),
            convert_utc=True,
        )

    assert caught.value.__cause__ is None


def test_outbound_aware_datetime_converts_to_utc_iso() -> None:
    aware = datetime.datetime(
        2026,
        7,
        4,
        12,
        0,
        0,
        microsecond=123456,
        tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
    )

    out = _protocol.transform_outbound({'when': aware}, convert_utc=True)

    assert out == {'when': '2026-07-04T10:00:00Z'}


def test_outbound_aware_datetime_keeps_wall_time_when_conversion_is_disabled() -> None:
    aware = datetime.datetime(
        2026,
        7,
        4,
        12,
        0,
        0,
        tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
    )

    out = _protocol.transform_outbound({'when': aware}, convert_utc=False)

    assert out == {'when': '2026-07-04T12:00:00Z'}


def test_outbound_date_becomes_iso_date_after_datetime_check() -> None:
    out = _protocol.transform_outbound(
        [datetime.date(2026, 7, 4), _naive_datetime('2026-07-04T01:02:03')],
        convert_utc=False,
    )

    assert out == ['2026-07-04', '2026-07-04T01:02:03Z']


def test_outbound_naive_datetime_keeps_wall_time_when_conversion_is_disabled() -> None:
    value = _naive_datetime('2026-01-15T12:30:00.123456')

    out = _protocol.transform_outbound(value, convert_utc=False)

    assert out == '2026-01-15T12:30:00Z'


def test_outbound_naive_datetime_uses_project_local_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 0)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', -11_700)
    value = _naive_datetime('2026-01-15T12:30:00')
    expected = value.replace(tzinfo=sg_timezone.local).astimezone(datetime.UTC)

    out = _protocol.transform_outbound(value, convert_utc=True)

    assert expected == datetime.datetime(2026, 1, 15, 9, 15, tzinfo=datetime.UTC)
    assert out == expected.strftime('%Y-%m-%dT%H:%M:%SZ')


@pytest.mark.parametrize('platform_function', ['localtime', 'mktime'])
@pytest.mark.parametrize('error_type', [OSError, ValueError])
def test_outbound_pre_epoch_datetime_uses_standard_offset_fallback(
    monkeypatch: pytest.MonkeyPatch,
    platform_function: str,
    error_type: type[Exception],
) -> None:
    def reject_pre_epoch_localtime(stamp: float) -> time.struct_time:
        if stamp < 0 and platform_function == 'localtime':
            raise error_type('pre-epoch timestamp rejected')
        return time.struct_time((1960, 1, 1, 12, 0, 0, 4, 1, 0))

    def reject_pre_epoch_mktime(value: tuple[int, ...]) -> float:
        if value[0] < 1970 and platform_function == 'mktime':
            raise error_type('pre-epoch timestamp rejected')
        return -1.0

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 16_200)
    monkeypatch.setattr(sgtimezone_module.time, 'altzone', 12_600)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_pre_epoch_localtime)
    monkeypatch.setattr(sgtimezone_module.time, 'mktime', reject_pre_epoch_mktime)

    out = _protocol.transform_outbound(
        _naive_datetime('1960-01-01T12:00:00'),
        convert_utc=True,
    )

    assert out == '1960-01-01T16:30:00Z'


def test_outbound_preserves_container_topology_and_input() -> None:
    data: dict[str, object] = {
        'items': (
            datetime.date(2026, 7, 4),
            [_naive_datetime('2026-07-04T01:02:03')],
        )
    }
    original = copy.deepcopy(data)

    out = _protocol.transform_outbound(data, convert_utc=False)

    assert out == {'items': ('2026-07-04', ['2026-07-04T01:02:03Z'])}
    assert isinstance(out, dict)
    assert isinstance(out['items'], tuple)
    assert isinstance(out['items'][1], list)
    assert out is not data
    assert data == original


def test_transform_annotations_preserve_only_container_topology() -> None:
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temporary_directory:
        probe = Path(temporary_directory) / 'transform_types.py'
        probe.write_text(
            """from datetime import date
from typing import assert_type

from shotgun_api3 import _protocol

mapping: dict[str, object] = {'when': '2026-07-04T12:00:00Z'}
items: list[object] = [date(2026, 7, 4)]
values: tuple[object, ...] = ('a &lt;b',)
string_mapping: dict[str, str] = {'when': '2026-07-04T12:00:00Z'}
string_items: list[str] = ['a &lt;b']
string_values: tuple[str, ...] = ('a &lt;b',)
mixed_values: tuple[date, str] = (date(2026, 7, 4), 'a &lt;b')

assert_type(
    _protocol._walk_object(date(2026, 7, 4), lambda _value: 'converted'),
    object,
)

assert_type(_protocol.transform_inbound(mapping, False), dict[str, object])
assert_type(_protocol.transform_inbound(items, False), list[object])
assert_type(_protocol.transform_inbound(values, False), tuple[object, ...])
assert_type(_protocol.transform_inbound('2026-07-04T12:00:00Z', False), object)
assert_type(_protocol.transform_inbound(string_mapping, False), dict[str, object])
assert_type(_protocol.transform_inbound(string_items, False), list[object])
assert_type(_protocol.transform_inbound(string_values, False), tuple[object, ...])
assert_type(_protocol.transform_inbound(mixed_values, False), tuple[object, ...])
assert_type(_protocol.transform_inbound(string_mapping, False)['when'], object)
assert_type(_protocol.transform_inbound(string_items, False)[0], object)
assert_type(_protocol.transform_inbound(string_values, False)[0], object)
assert_type(_protocol.transform_inbound(mixed_values, False)[0], object)

assert_type(_protocol.transform_outbound(mapping, False), dict[str, object])
assert_type(_protocol.transform_outbound(items, False), list[object])
assert_type(_protocol.transform_outbound(values, False), tuple[object, ...])
assert_type(_protocol.transform_outbound(date(2026, 7, 4), False), object)
assert_type(_protocol.transform_outbound(string_mapping, False), dict[str, object])
assert_type(_protocol.transform_outbound(string_items, False), list[object])
assert_type(_protocol.transform_outbound(string_values, False), tuple[object, ...])
assert_type(_protocol.transform_outbound(mixed_values, False), tuple[object, ...])
assert_type(_protocol.transform_outbound(string_mapping, False)['when'], object)
assert_type(_protocol.transform_outbound(string_items, False)[0], object)
assert_type(_protocol.transform_outbound(string_values, False)[0], object)
assert_type(_protocol.transform_outbound(mixed_values, False)[0], object)
""",
            encoding='utf-8',
        )
        repository = Path(__file__).parents[1]

        result = run_pyright(probe, cwd=repository)

    assert result.returncode == 0, result.stdout
    report = json.loads(result.stdout)
    assert report['summary']['errorCount'] == 0
