"""KV-backed rate limiter: capacities, windows, rollover, CAS contention, Go-compatible state."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from nats.js.errors import KeyWrongLastSequenceError

from cairn_provider_sdk import ratelimit
from cairn_provider_sdk.ratelimit import BucketState, RateLimiter, bucket, kv_key
from cairn_provider_sdk.testing import FakeKV


class Clock:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def now(self) -> datetime:
        return self.at


def limiter(capacities: dict[str, int], **kw: Any) -> tuple[RateLimiter, FakeKV]:
    kv = FakeKV()
    rl = RateLimiter(kv.as_kv(), capacities, **kw)

    async def no_sleep(_: float) -> None:
        pass

    rl.sleep = no_sleep
    return rl, kv


def test_bucket_naming() -> None:
    assert bucket("strava", "app1", "short") == "strava:app1:short"
    assert kv_key("strava:app1:short") == "strava.app1.short"


def test_reserve_allows_until_capacity() -> None:
    rl, _ = limiter({"strava:a:short": 3})
    for _ in range(3):
        assert asyncio.run(rl.reserve("strava:a:short")) == (True, 0)
    ok, retry = asyncio.run(rl.reserve("strava:a:short"))
    assert ok is False
    assert 0 < retry <= 15 * 60


def test_unknown_bucket_and_zero_tokens_are_permissive() -> None:
    rl, kv = limiter({})
    for _ in range(50):
        assert asyncio.run(rl.reserve("x:y:minute")) == (True, 0)
    rl2, _ = limiter({"a:b:minute": 1})
    assert asyncio.run(rl2.reserve("a:b:minute", 0)) == (True, 0)
    assert kv.store == {}


@pytest.mark.parametrize(
    ("name", "seconds"),
    [
        ("p:a:second", 1),
        ("p:a:minute", 60),
        ("p:a:quarter_hour", 900),
        ("p:a:short", 900),
        ("p:a:hour", 3600),
        ("p:a:day", 86400),
        ("p:a:daily", 86400),
        ("p:a:custom", 900),
    ],
)
def test_window_by_suffix(name: str, seconds: float) -> None:
    rl, _ = limiter({})
    assert rl.window_for(name) == seconds


def test_custom_windows_override() -> None:
    rl, _ = limiter({}, windows={"custom": 5, "minute": 61})
    assert rl.window_for("p:a:custom") == 5
    assert rl.window_for("p:a:minute") == 61


def test_window_rollover_with_a_controlled_clock() -> None:
    clock = Clock(datetime(2026, 5, 18, 12, 0, tzinfo=UTC))
    rl, kv = limiter({"p:a:minute": 2}, now=clock.now)
    assert asyncio.run(rl.reserve("p:a:minute", 2)) == (True, 0)
    ok, retry = asyncio.run(rl.reserve("p:a:minute"))
    assert (ok, retry) == (False, 60)
    clock.at += timedelta(seconds=30)
    ok, retry = asyncio.run(rl.reserve("p:a:minute"))
    assert (ok, retry) == (False, 30)
    clock.at += timedelta(seconds=31)
    assert asyncio.run(rl.reserve("p:a:minute")) == (True, 0)
    state = BucketState.from_json(kv.store["p.a.minute"].value or b"")
    assert (state.capacity, state.used) == (2, 1)
    assert state.window_start == clock.at
    assert state.window_reset_at == clock.at + timedelta(seconds=60)


def test_state_json_matches_the_go_worker() -> None:
    raw = (
        b'{"capacity":100,"used":3,"window_start":"2026-05-18T12:00:00.123456789Z",'
        b'"window_reset_at":"2026-05-18T12:15:00Z"}'
    )
    state = BucketState.from_json(raw)
    assert (state.capacity, state.used) == (100, 3)
    assert state.window_start == datetime(2026, 5, 18, 12, 0, 0, 123456, tzinfo=UTC)
    out = json.loads(state.to_json())
    assert out["window_reset_at"] == "2026-05-18T12:15:00Z"
    assert set(out) == {"capacity", "used", "window_start", "window_reset_at"}
    assert json.loads(BucketState().to_json())["window_start"] == "0001-01-01T00:00:00Z"
    assert BucketState.from_json(b"garbage") == BucketState()
    assert BucketState.from_json(b"[1]") == BucketState()


def test_sync_usage_and_force_refill_overwrite() -> None:
    clock = Clock(datetime(2026, 5, 18, 12, 0, tzinfo=UTC))
    rl, kv = limiter({"p:a:short": 10}, now=clock.now)
    reset = clock.at + timedelta(minutes=10)
    asyncio.run(rl.sync_usage("p:a:short", used=99, limit=100, resets_at=reset))
    state = BucketState.from_json(kv.store["p.a.short"].value or b"")
    assert (state.capacity, state.used, state.window_reset_at) == (100, 99, reset)
    assert asyncio.run(rl.reserve("p:a:short")) == (True, 0)  # learned capacity 100 wins over the configured 10
    assert asyncio.run(rl.reserve("p:a:short")) == (False, 600)

    asyncio.run(rl.force_refill("p:a:short", available=5, resets_at=reset))
    state = BucketState.from_json(kv.store["p.a.short"].value or b"")
    assert (state.capacity, state.used) == (10, 5)
    asyncio.run(rl.force_refill("p:x:short", available=7, resets_at=reset))  # unknown bucket learns its capacity
    assert BucketState.from_json(kv.store["p.x.short"].value or b"").capacity == 7
    asyncio.run(rl.sync_usage("p:a:short", used=1, limit=0, resets_at=reset))  # no limit: no-op
    assert BucketState.from_json(kv.store["p.a.short"].value or b"").used == 5


def test_learned_capacity_carries_across_windows() -> None:
    clock = Clock(datetime(2026, 5, 18, 12, 0, tzinfo=UTC))
    rl, _ = limiter({"p:a:minute": 1}, now=clock.now)
    asyncio.run(rl.sync_usage("p:a:minute", used=0, limit=3, resets_at=clock.at + timedelta(seconds=1)))
    clock.at += timedelta(seconds=2)
    for _ in range(3):
        assert asyncio.run(rl.reserve("p:a:minute")) == (True, 0)
    assert asyncio.run(rl.reserve("p:a:minute"))[0] is False


def test_lost_cas_retries_then_yields() -> None:
    rl, kv = limiter({"p:a:minute": 5})
    attempts = {"n": 0}
    original = kv.update

    async def flaky_update(key: str, value: bytes, last: int | None = None) -> int:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise KeyWrongLastSequenceError
        return await original(key, value, last)

    asyncio.run(rl.reserve("p:a:minute"))  # creates the entry
    kv.update = flaky_update  # type: ignore[method-assign]
    assert asyncio.run(rl.reserve("p:a:minute")) == (True, 0)
    assert attempts["n"] == 3

    async def always_lost(key: str, value: bytes, last: int | None = None) -> int:
        raise KeyWrongLastSequenceError

    kv.update = always_lost  # type: ignore[method-assign]
    rl.MAX_ATTEMPTS = 3
    ok, retry = asyncio.run(rl.reserve("p:a:minute"))
    assert ok is False
    assert 0.05 <= retry < 0.1


def test_create_race_on_first_reserve() -> None:
    rl, kv = limiter({"p:a:minute": 5})
    calls = {"n": 0}
    original = kv.create

    async def racing_create(key: str, value: bytes) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            await kv.put(key, BucketState(capacity=5, used=4).to_json())
            raise KeyWrongLastSequenceError
        return await original(key, value)

    kv.create = racing_create  # type: ignore[method-assign]
    assert asyncio.run(rl.reserve("p:a:minute")) == (True, 0)
    assert BucketState.from_json(kv.store["p.a.minute"].value or b"").used == 1  # the raced entry had no window: rolled


def test_backoff_is_bounded() -> None:
    for attempt in range(12):
        assert 0 <= ratelimit._backoff(attempt) <= 0.02
