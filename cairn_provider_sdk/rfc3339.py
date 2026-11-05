"""RFC 3339 timestamps as core (Go `time.Time`) writes and reads them."""

from __future__ import annotations

import re
from datetime import UTC, datetime

_FRACTION = re.compile(r"(\.\d{6})\d+")


def format_time(dt: datetime) -> str:
    """UTC, `Z` suffix; naive datetimes are taken as UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_time(raw: object) -> datetime | None:
    """Go emits up to nine fractional digits; Python reads six. None when unset or malformed."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(_FRACTION.sub(r"\1", raw.replace("Z", "+00:00")))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    # Go's zero time means "unset".
    return None if dt.year == 1 else dt
