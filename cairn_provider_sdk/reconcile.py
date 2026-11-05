"""Reconcile helpers (provider contract §7): the listing window, watermark
parsing and per-account coalescing of scheduler backlogs.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

log = logging.getLogger("cairn.sdk")

# Reconcile never lists further back than this, even without a watermark;
# full-history backfill is the user-driven import queue's job.
LOOKBACK_DAYS = 30
# Drift margin below the watermark: late-arriving activities still get seen.
DRIFT = timedelta(hours=1)
# Cap on fetch_source sub-jobs one reconcile run enqueues.
MAX_ENQUEUE = 500
# Reconcile jobs for an account handled more recently than this are acked
# without touching the provider, so a scheduler backlog drains as no-ops.
COALESCE_S = 600


def parse_watermark(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        log.warning("reconcile: unparseable watermark %r", raw)
        return None


def window_start(
    watermark: datetime | None, *, now: datetime | None = None, lookback_days: int = LOOKBACK_DAYS
) -> datetime:
    """The later of (watermark - drift) and the lookback floor: always
    bounded to recent activity, never a flood.
    """
    start = (now or datetime.now(UTC)) - timedelta(days=lookback_days)
    if watermark is not None:
        start = max(start, watermark - DRIFT)
    return start


def max_enqueue(job: dict[str, object], default: int = MAX_ENQUEUE) -> int:
    return int(str(job.get("max_enqueue") or 0)) or default


class Coalescer:
    """Remembers when each account was last really reconciled."""

    def __init__(self, window_s: float = COALESCE_S) -> None:
        self.window_s = window_s
        self._at: dict[str, float] = {}

    def should_run(self, account_id: str) -> bool:
        """True (and records the run) unless the account ran within the window."""
        now = asyncio.get_running_loop().time()
        last = self._at.get(account_id)
        if last is not None and now - last < self.window_s:
            return False
        self._at[account_id] = now
        return True
