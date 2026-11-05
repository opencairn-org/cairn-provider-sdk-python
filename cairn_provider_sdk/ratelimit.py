"""Shared rate-limit budgets in the cairn_rate_limits KV bucket, arbitrated by
JetStream's compare-and-set. The state and bucket layout mirror the Go
worker's limiter so pools in both languages share one budget per provider
app (provider contract §3 and §9).

Bucket names are `<provider>:<app_id>:<window>` (helper `bucket()`); the
window is read from the suffix after the last ':' and selects the rolling
duration. Stored under the key with ':' mapped to '.' (NATS KV keys).

  - `reserve` BEFORE every provider call; (False, retry_after_s) means the
    bucket is exhausted: raise RetryLaterError(retry_after_s).
  - `force_refill` after a 429 with the provider's authoritative reset time.
  - `sync_usage` with the usage headers of every response.

Unknown buckets (no capacity configured) are permissive: rate-limit policy
is the worker's, and nothing blocks traffic it has no opinion about.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from nats.js.errors import KeyWrongLastSequenceError, NotFoundError
from nats.js.kv import KeyValue

from .rfc3339 import format_time, parse_time

log = logging.getLogger("cairn.sdk")

# Window suffix → seconds. "short"/"daily"/"webhook" are the Go worker's names.
WINDOWS: dict[str, float] = {
    "second": 1,
    "minute": 60,
    "quarter_hour": 900,
    "short": 900,
    "webhook": 900,
    "hour": 3600,
    "day": 86400,
    "daily": 86400,
}
DEFAULT_WINDOW_S = 900.0


def bucket(provider: str, app_id: str, window: str) -> str:
    """`<provider>:<app_id>:<window>`; app_id is the token reply's app_id or,
    without one (older core), the pool's worker name.
    """
    return f"{provider}:{app_id}:{window}"


def kv_key(name: str) -> str:
    return name.replace(":", ".")


@dataclass
class BucketState:
    capacity: int = 0
    used: int = 0
    window_start: datetime | None = None
    window_reset_at: datetime | None = None

    @classmethod
    def from_json(cls, raw: bytes) -> BucketState:
        try:
            d = json.loads(raw or b"{}")
        except ValueError:
            d = {}
        if not isinstance(d, dict):
            d = {}
        return cls(
            capacity=int(d.get("capacity") or 0),
            used=int(d.get("used") or 0),
            window_start=parse_time(d.get("window_start")),
            window_reset_at=parse_time(d.get("window_reset_at")),
        )

    def to_json(self) -> bytes:
        zero = "0001-01-01T00:00:00Z"
        return json.dumps(
            {
                "capacity": self.capacity,
                "used": self.used,
                "window_start": format_time(self.window_start) if self.window_start else zero,
                "window_reset_at": format_time(self.window_reset_at) if self.window_reset_at else zero,
            }
        ).encode()


def _backoff(attempt: int) -> float:
    """Jittered exponential backoff between lost CAS rounds: up to ~0.4 ms, capped at 20 ms."""
    span = min(0.0002 * (2 ** min(attempt, 6)), 0.02)
    return random.uniform(0, span)  # noqa: S311 — jitter, not security


class RateLimiter:
    MAX_ATTEMPTS = 200

    def __init__(
        self,
        kv: KeyValue,
        capacities: Mapping[str, int],
        *,
        windows: Mapping[str, float] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.kv = kv
        self.capacities = dict(capacities)
        self.windows = {**WINDOWS, **(windows or {})}
        self.now = now or (lambda: datetime.now(UTC))
        self.sleep: Callable[[float], Any] = asyncio.sleep

    def window_for(self, name: str) -> float:
        return self.windows.get(name.rsplit(":", 1)[-1], DEFAULT_WINDOW_S)

    async def reserve(self, name: str, tokens: int = 1) -> tuple[bool, float]:
        """Consume tokens; (True, 0) or (False, seconds until the window resets)."""
        capacity = self.capacities.get(name, 0)
        if tokens <= 0 or capacity == 0:
            return True, 0
        window = self.window_for(name)
        for attempt in range(self.MAX_ATTEMPTS):
            if attempt:
                await self.sleep(_backoff(attempt))
            state, rev = await self._read(name)
            now = self.now()
            state = self._roll(state, now, capacity, window)
            if state.used + tokens > state.capacity:
                reset = state.window_reset_at or now
                return False, max((reset - now).total_seconds(), 0)
            state.used += tokens
            if await self._write(name, state, rev):
                return True, 0
        # Persistent contention is not an error: retry after a short jittered delay.
        return False, 0.05 + _backoff(8)

    @staticmethod
    def _roll(state: BucketState, now: datetime, capacity: int, window: float) -> BucketState:
        """A fresh window once the previous one expired; a learned capacity carries over."""
        if state.window_start is not None and state.window_reset_at is not None and now < state.window_reset_at:
            return state
        carried = state.capacity if state.capacity > 0 else capacity
        return BucketState(capacity=carried, used=0, window_start=now, window_reset_at=now + timedelta(seconds=window))

    async def force_refill(self, name: str, available: int, resets_at: datetime) -> None:
        """After a 429: the provider's own remaining budget and reset time, unconditionally."""
        capacity = self.capacities.get(name, 0) or available
        state = BucketState(
            capacity=capacity, used=max(capacity - available, 0), window_start=self.now(), window_reset_at=resets_at
        )
        await self.kv.put(kv_key(name), state.to_json())

    async def sync_usage(self, name: str, used: int, limit: int, resets_at: datetime) -> None:
        """Overwrite with the (used, limit) pair the provider reported; no-op without a limit."""
        if limit <= 0:
            return
        state = BucketState(capacity=limit, used=max(used, 0), window_start=self.now(), window_reset_at=resets_at)
        await self.kv.put(kv_key(name), state.to_json())

    async def _read(self, name: str) -> tuple[BucketState, int]:
        try:
            entry = await self.kv.get(kv_key(name))
        except NotFoundError:
            return BucketState(), 0
        # A malformed value reads as empty so a corrupt entry cannot pin the bucket.
        return BucketState.from_json(entry.value or b""), int(entry.revision or 0)

    async def _write(self, name: str, state: BucketState, rev: int) -> bool:
        """CAS on the revision read (create when none); False when another writer won."""
        try:
            if rev == 0:
                await self.kv.create(kv_key(name), state.to_json())
            else:
                await self.kv.update(kv_key(name), state.to_json(), last=rev)
        except KeyWrongLastSequenceError:
            return False
        return True
