"""Timezone helpers at the official `shotgun_api3.lib.sgtimezone` path."""

import datetime
import time

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
_NAIVE_EPOCH = _EPOCH.replace(tzinfo=None)
_ZERO = datetime.timedelta(0)


class LocalTimezone(datetime.tzinfo):
    """The process's dynamic system-local timezone.

    On platforms without pre-epoch local-time support, pre-1970 values use the
    process's standard offset without historical daylight-saving accuracy.
    """

    def _isdst(self, dt: datetime.datetime | None) -> bool:
        """Return whether a local wall time selects daylight-saving time."""
        if dt is None or not time.daylight:
            return False

        wall_time = (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        time_tuple = (*wall_time, dt.weekday(), 0)
        candidates: list[tuple[float, bool]] = []
        outcomes: list[tuple[float, bool]] = []
        for is_dst in (0, 1):
            try:
                stamp = time.mktime((*time_tuple, is_dst))
                local = time.localtime(stamp)
            except (OSError, OverflowError, ValueError):
                if _is_pre_epoch(dt):
                    return False
                raise
            outcome = (stamp, local.tm_isdst > 0)
            outcomes.append(outcome)
            if local[:6] == wall_time and outcome[1] is bool(is_dst):
                candidates.append(outcome)

        if len(candidates) > 1 and candidates[0][0] != candidates[1][0]:
            candidates.sort(key=lambda candidate: candidate[0])
            return candidates[dt.fold][1]

        if not candidates and outcomes[0][0] != outcomes[1][0]:
            outcomes.sort(key=lambda outcome: outcome[0])
            return outcomes[dt.fold][1]

        try:
            inferred_stamp = time.mktime((*time_tuple, -1))
            return time.localtime(inferred_stamp).tm_isdst > 0
        except (OSError, OverflowError, ValueError):
            if _is_pre_epoch(dt):
                return False
            raise

    @staticmethod
    def _offsets() -> tuple[datetime.timedelta, datetime.timedelta]:
        """Return the process's current standard and daylight offsets."""
        standard = datetime.timedelta(seconds=-time.timezone)
        daylight = datetime.timedelta(seconds=-time.altzone) if time.daylight else standard
        return standard, daylight

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta:
        """Return the daylight-saving adjustment for a local wall time.

        Args:
            dt: Local wall time, with `fold` selecting an ambiguous occurrence.
                `None` selects standard time.

        Returns:
            The daylight-saving adjustment, or zero when DST is not active.
        """
        standard, daylight = self._offsets()
        return daylight - standard if self._isdst(dt) else _ZERO

    def fromutc(self, dt: datetime.datetime) -> datetime.datetime:
        """Convert UTC fields to the corresponding local wall time.

        Args:
            dt: Datetime whose fields express UTC and whose `tzinfo` is this
                instance.

        Returns:
            The equivalent local datetime, with `fold` marking a repeated wall
            time.

        Raises:
            ValueError: If `dt.tzinfo` is not this instance.
        """
        if dt.tzinfo is not self:
            raise ValueError('fromutc() requires dt.tzinfo to be self')

        stamp = (dt.replace(tzinfo=datetime.UTC) - _EPOCH).total_seconds()
        try:
            local = time.localtime(stamp)
        except (OSError, OverflowError, ValueError):
            if stamp >= 0:
                raise
            standard, _daylight = self._offsets()
            return (dt.replace(tzinfo=None) + standard).replace(tzinfo=self, fold=0)

        standard, daylight = self._offsets()
        dst_difference = (daylight - standard).total_seconds()
        try:
            previous = time.localtime(stamp - dst_difference)
        except (OSError, OverflowError, ValueError):
            if stamp >= 0:
                raise
            standard, _daylight = self._offsets()
            return (dt.replace(tzinfo=None) + standard).replace(tzinfo=self, fold=0)
        fold = bool(dst_difference) and local[:6] == previous[:6]
        return datetime.datetime(
            local.tm_year,
            local.tm_mon,
            local.tm_mday,
            local.tm_hour,
            local.tm_min,
            local.tm_sec,
            microsecond=dt.microsecond,
            tzinfo=self,
            fold=int(fold),
        )

    def tzname(self, dt: datetime.datetime | None) -> str:
        """Return the current local timezone name for a wall time.

        Args:
            dt: Local wall time, with `fold` selecting an ambiguous occurrence.
                `None` selects standard time.

        Returns:
            The platform's standard or daylight timezone name.
        """
        return time.tzname[int(self._isdst(dt))]

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        """Return the current local offset from UTC for a wall time.

        Args:
            dt: Local wall time, with `fold` selecting an ambiguous occurrence.
                `None` selects standard time.

        Returns:
            The current standard or daylight offset from UTC.
        """
        standard, daylight = self._offsets()
        return daylight if self._isdst(dt) else standard


class UTC(datetime.tzinfo):
    """UTC with a constant zero offset."""

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta:
        """Return the zero daylight-saving adjustment.

        Args:
            dt: Ignored because UTC does not observe daylight-saving time.

        Returns:
            A zero-length timedelta.
        """
        del dt
        return _ZERO

    def tzname(self, dt: datetime.datetime | None) -> str:
        """Return the UTC timezone name.

        Args:
            dt: Ignored because the UTC name is constant.

        Returns:
            The string `UTC`.
        """
        del dt
        return 'UTC'

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        """Return UTC's zero offset.

        Args:
            dt: Ignored because the UTC offset is constant.

        Returns:
            A zero-length timedelta.
        """
        del dt
        return _ZERO


class SgTimezone:
    """Container exposing UTC and system-local timezone instances.

    Attributes:
        utc: UTC timezone instance.
        local: System-local timezone instance.
    """

    LocalTimezone = LocalTimezone
    UTC = UTC

    def __init__(self) -> None:
        """Initialize UTC and local timezone instances."""
        self.utc = UTC()
        self.local = LocalTimezone()


def _is_pre_epoch(value: datetime.datetime) -> bool:
    """Return whether a datetime precedes the Unix epoch."""
    return value.replace(tzinfo=None) < _NAIVE_EPOCH
