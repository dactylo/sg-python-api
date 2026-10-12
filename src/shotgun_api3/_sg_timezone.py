"""Own the process-wide timezone singleton."""

from .lib.sgtimezone import SgTimezone

SG_TIMEZONE = SgTimezone()
