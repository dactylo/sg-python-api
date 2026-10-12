import datetime
import os
import sys
import time

import pytest

import shotgun_api3.lib.sgtimezone as sgtimezone_module
from shotgun_api3.lib.sgtimezone import SgTimezone


def _struct_time(
    wall_time: tuple[int, int, int, int, int, int],
    is_dst: int,
) -> time.struct_time:
    return time.struct_time((*wall_time, 0, 1, is_dst))


def test_class_aliases_and_instances_are_available() -> None:
    timezone = SgTimezone()

    assert SgTimezone.UTC is sgtimezone_module.UTC
    assert SgTimezone.LocalTimezone is sgtimezone_module.LocalTimezone
    assert isinstance(timezone.utc, sgtimezone_module.UTC)
    assert isinstance(timezone.local, sgtimezone_module.LocalTimezone)
    assert isinstance(timezone.utc, datetime.tzinfo)
    assert isinstance(timezone.local, datetime.tzinfo)


def test_fromutc_detects_fold_and_preserves_microseconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_stamp = 100_000
    second_stamp = first_stamp + 3_600
    repeated_wall_time = _struct_time((2026, 11, 1, 1, 30, 0), 1)
    standard_wall_time = _struct_time((2026, 11, 1, 1, 30, 0), 0)
    other_wall_time = _struct_time((2026, 11, 1, 0, 30, 0), 1)

    def fake_localtime(stamp: float) -> time.struct_time:
        return {
            first_stamp: repeated_wall_time,
            second_stamp: standard_wall_time,
        }.get(int(stamp), other_wall_time)

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 18_000)
    monkeypatch.setattr(sgtimezone_module.time, 'altzone', 14_400)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', fake_localtime)

    local = sgtimezone_module.LocalTimezone()
    epoch = datetime.datetime(1970, 1, 1, tzinfo=local)
    first = local.fromutc(epoch + datetime.timedelta(seconds=first_stamp, microseconds=123_456))
    second = local.fromutc(epoch + datetime.timedelta(seconds=second_stamp, microseconds=654_321))

    assert first.replace(tzinfo=None).isoformat() == '2026-11-01T01:30:00.123456'
    assert second.replace(tzinfo=None).isoformat() == '2026-11-01T01:30:00.654321'
    assert first.fold == 0
    assert second.fold == 1
    assert first.tzinfo is local
    assert second.tzinfo is local


def test_fromutc_rejects_another_tzinfo() -> None:
    local = sgtimezone_module.LocalTimezone()
    value = datetime.datetime(2026, 7, 18, 12, tzinfo=datetime.UTC)

    with pytest.raises(ValueError, match='tzinfo'):
        local.fromutc(value)


@pytest.mark.parametrize('error_type', [OSError, OverflowError])
def test_fromutc_reraises_modern_platform_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    def reject_localtime(_stamp: float) -> time.struct_time:
        raise error_type('modern timestamp rejected')

    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_localtime)

    local = sgtimezone_module.LocalTimezone()
    modern = datetime.datetime(2026, 1, 1, tzinfo=local)

    with pytest.raises(error_type, match='modern timestamp rejected'):
        local.fromutc(modern)


def test_fromutc_retains_direct_arithmetic_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_pre_epoch(stamp: float) -> time.struct_time:
        if stamp < 0:
            raise OSError('pre-epoch timestamp rejected')
        return _struct_time((1970, 1, 1, 0, 0, 0), 0)

    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 28_800)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_pre_epoch)

    local = sgtimezone_module.LocalTimezone()
    earliest = datetime.datetime.min.replace(tzinfo=local)

    with pytest.raises(OverflowError):
        local.fromutc(earliest)


@pytest.mark.parametrize('error_type', [OSError, OverflowError])
def test_fromutc_uses_standard_offset_when_platform_rejects_pre_epoch(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    def reject_pre_epoch(stamp: float) -> time.struct_time:
        if stamp < 0:
            raise error_type('pre-epoch timestamp rejected')
        return _struct_time((1970, 1, 1, 0, 0, 0), 0)

    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 28_800)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_pre_epoch)

    local = sgtimezone_module.LocalTimezone()
    utc = datetime.datetime(1960, 1, 1, tzinfo=local)

    assert local.fromutc(utc) == datetime.datetime(
        1959,
        12,
        31,
        16,
        tzinfo=local,
        fold=0,
    )


def test_local_offset_matches_one_captured_system_instant() -> None:
    captured = datetime.datetime.now().astimezone()
    local_wall_time = captured.replace(tzinfo=None)

    assert sgtimezone_module.LocalTimezone().utcoffset(local_wall_time) == captured.utcoffset()


@pytest.mark.skipif(not hasattr(time, 'tzset'), reason='requires POSIX tzset')
def test_local_timezone_tracks_tzset_and_posix_dst_transition() -> None:
    if sys.platform == 'win32':
        pytest.skip('requires POSIX tzset')
    original_tz = os.environ.get('TZ')
    try:
        os.environ['TZ'] = 'UTC0'
        time.tzset()
        local = sgtimezone_module.LocalTimezone()
        utc_wall_time = datetime.datetime(2026, 7, 15, 12, tzinfo=local)
        assert local.utcoffset(utc_wall_time) == datetime.timedelta(0)

        os.environ['TZ'] = 'PST8PDT,M3.2.0,M11.1.0'
        time.tzset()
        winter = datetime.datetime(2026, 1, 15, 12, tzinfo=local)
        summer = datetime.datetime(2026, 7, 15, 12, tzinfo=local)
        first = datetime.datetime(2026, 11, 1, 8, 30, tzinfo=local)
        second = datetime.datetime(2026, 11, 1, 9, 30, tzinfo=local)

        assert local.utcoffset(winter) == datetime.timedelta(hours=-8)
        assert local.utcoffset(summer) == datetime.timedelta(hours=-7)
        assert local.dst(summer) - local.dst(winter) == datetime.timedelta(hours=1)
        assert local.fromutc(first).fold == 0
        assert local.fromutc(second).fold == 1
        assert local.fromutc(first).replace(tzinfo=None).isoformat() == '2026-11-01T01:30:00'
        assert local.fromutc(second).replace(tzinfo=None).isoformat() == '2026-11-01T01:30:00'
    finally:
        if original_tz is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = original_tz
        time.tzset()


@pytest.mark.parametrize('platform_function', ['localtime', 'mktime'])
@pytest.mark.parametrize('error_type', [OSError, OverflowError])
def test_local_wall_time_reraises_modern_platform_errors(
    monkeypatch: pytest.MonkeyPatch,
    platform_function: str,
    error_type: type[Exception],
) -> None:
    def reject_modern_localtime(_stamp: float) -> time.struct_time:
        if platform_function == 'localtime':
            raise error_type('modern timestamp rejected')
        return _struct_time((2026, 1, 1, 12, 0, 0), 0)

    def reject_modern_mktime(_value: tuple[int, ...]) -> float:
        if platform_function == 'mktime':
            raise error_type('modern timestamp rejected')
        return 1.0

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_modern_localtime)
    monkeypatch.setattr(sgtimezone_module.time, 'mktime', reject_modern_mktime)

    local = sgtimezone_module.LocalTimezone()
    modern = datetime.datetime(2026, 1, 1, 12, tzinfo=local)

    with pytest.raises(error_type, match='modern timestamp rejected'):
        local.utcoffset(modern)


def test_local_wall_time_uses_fold_to_select_gap_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    earlier_stamp = 100_000.0
    later_stamp = earlier_stamp + 3_600
    standard_time = _struct_time((2026, 3, 8, 1, 30, 0), 0)
    daylight_time = _struct_time((2026, 3, 8, 3, 30, 0), 1)

    def fake_localtime(stamp: float) -> time.struct_time:
        if stamp == earlier_stamp:
            return standard_time
        if stamp == later_stamp:
            return daylight_time
        raise AssertionError(f'unexpected timestamp: {stamp}')

    def fake_mktime(value: tuple[int, ...]) -> float:
        return earlier_stamp if value[-1] == 1 else later_stamp

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 28_800)
    monkeypatch.setattr(sgtimezone_module.time, 'altzone', 25_200)
    monkeypatch.setattr(sgtimezone_module.time, 'tzname', ('PST', 'PDT'))
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', fake_localtime)
    monkeypatch.setattr(sgtimezone_module.time, 'mktime', fake_mktime)

    local = sgtimezone_module.LocalTimezone()
    first = datetime.datetime(2026, 3, 8, 2, 30, tzinfo=local, fold=0)
    second = first.replace(fold=1)

    assert local.utcoffset(first) == datetime.timedelta(hours=-8)
    assert local.dst(first) == datetime.timedelta(0)
    assert local.tzname(first) == 'PST'
    assert local.utcoffset(second) == datetime.timedelta(hours=-7)
    assert local.dst(second) == datetime.timedelta(hours=1)
    assert local.tzname(second) == 'PDT'


def test_local_wall_time_uses_fold_to_select_offset_and_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_stamp = 100_000.0
    second_stamp = first_stamp + 3_600
    repeated_daylight_time = _struct_time((2026, 11, 1, 1, 30, 0), 1)
    repeated_standard_time = _struct_time((2026, 11, 1, 1, 30, 0), 0)

    def fake_localtime(stamp: float) -> time.struct_time:
        if stamp == first_stamp:
            return repeated_daylight_time
        if stamp == second_stamp:
            return repeated_standard_time
        raise AssertionError(f'unexpected timestamp: {stamp}')

    def fake_mktime(value: tuple[int, ...]) -> float:
        return second_stamp if value[-1] == 0 else first_stamp

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 18_000)
    monkeypatch.setattr(sgtimezone_module.time, 'altzone', 14_400)
    monkeypatch.setattr(sgtimezone_module.time, 'tzname', ('EST', 'EDT'))
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', fake_localtime)
    monkeypatch.setattr(sgtimezone_module.time, 'mktime', fake_mktime)

    local = sgtimezone_module.LocalTimezone()
    first = datetime.datetime(2026, 11, 1, 1, 30, tzinfo=local, fold=0)
    second = first.replace(fold=1)

    assert local.utcoffset(first) == datetime.timedelta(hours=-4)
    assert local.dst(first) == datetime.timedelta(hours=1)
    assert local.tzname(first) == 'EDT'
    assert local.utcoffset(second) == datetime.timedelta(hours=-5)
    assert local.dst(second) == datetime.timedelta(0)
    assert local.tzname(second) == 'EST'


@pytest.mark.parametrize('platform_function', ['localtime', 'mktime'])
@pytest.mark.parametrize('error_type', [OSError, OverflowError])
def test_local_wall_time_uses_standard_offset_when_platform_rejects_pre_epoch(
    monkeypatch: pytest.MonkeyPatch,
    platform_function: str,
    error_type: type[Exception],
) -> None:
    def reject_pre_epoch_localtime(stamp: float) -> time.struct_time:
        if stamp < 0 and platform_function == 'localtime':
            raise error_type('pre-epoch timestamp rejected')
        return _struct_time((1960, 1, 1, 12, 0, 0), 0)

    def reject_pre_epoch_mktime(value: tuple[int, ...]) -> float:
        if value[0] < 1970 and platform_function == 'mktime':
            raise error_type('pre-epoch timestamp rejected')
        return -1.0 if value[0] < 1970 else 1.0

    monkeypatch.setattr(sgtimezone_module.time, 'daylight', 1)
    monkeypatch.setattr(sgtimezone_module.time, 'timezone', 28_800)
    monkeypatch.setattr(sgtimezone_module.time, 'altzone', 25_200)
    monkeypatch.setattr(sgtimezone_module.time, 'localtime', reject_pre_epoch_localtime)
    monkeypatch.setattr(sgtimezone_module.time, 'mktime', reject_pre_epoch_mktime)

    local = sgtimezone_module.LocalTimezone()
    wall = datetime.datetime(1960, 1, 1, 12, tzinfo=local)

    assert local.utcoffset(wall) == datetime.timedelta(hours=-8)
    assert local.dst(wall) == datetime.timedelta(0)


def test_utc_has_zero_offset_dst_and_name() -> None:
    utc = sgtimezone_module.UTC()
    value = datetime.datetime(2026, 7, 18, 12, 34, 56, tzinfo=utc)

    assert utc.utcoffset(value) == datetime.timedelta(0)
    assert utc.dst(value) == datetime.timedelta(0)
    assert utc.tzname(value) == 'UTC'
