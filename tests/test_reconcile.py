"""Reconcile window, watermark parsing, coalescing, RFC 3339 helpers."""

import asyncio
from datetime import UTC, datetime, timedelta

from cairn_provider_sdk import reconcile as R
from cairn_provider_sdk.rfc3339 import format_time, parse_time


def test_parse_watermark() -> None:
    assert R.parse_watermark(None) is None
    assert R.parse_watermark("") is None
    assert R.parse_watermark("nope") is None
    assert R.parse_watermark("2026-05-01T10:00:00Z") == datetime(2026, 5, 1, 10, tzinfo=UTC)


def test_window_start_is_bounded() -> None:
    now = datetime(2026, 6, 1, tzinfo=UTC)
    assert R.window_start(None, now=now) == now - timedelta(days=30)
    recent = now - timedelta(days=2)
    assert R.window_start(recent, now=now) == recent - timedelta(hours=1)
    old = now - timedelta(days=90)
    assert R.window_start(old, now=now) == now - timedelta(days=30)
    assert R.window_start(None, now=now, lookback_days=7) == now - timedelta(days=7)
    assert R.window_start(None) <= datetime.now(UTC)


def test_max_enqueue() -> None:
    assert R.max_enqueue({}) == R.MAX_ENQUEUE
    assert R.max_enqueue({"max_enqueue": 0}) == R.MAX_ENQUEUE
    assert R.max_enqueue({"max_enqueue": "12"}) == 12
    assert R.max_enqueue({}, default=3) == 3


def test_coalescer() -> None:
    async def run() -> list[bool]:
        c = R.Coalescer(window_s=0.05)
        first = c.should_run("a")
        second = c.should_run("a")
        other = c.should_run("b")
        await asyncio.sleep(0.06)
        third = c.should_run("a")
        return [first, second, other, third]

    assert asyncio.run(run()) == [True, False, True, True]


def test_rfc3339_round_trip() -> None:
    dt = datetime(2026, 5, 18, 12, 0, 0, 123456, tzinfo=UTC)
    assert format_time(dt) == "2026-05-18T12:00:00.123456Z"
    assert parse_time("2026-05-18T12:00:00.123456789Z") == dt
    assert parse_time("2026-05-18T14:00:00+02:00") == datetime(2026, 5, 18, 12, tzinfo=UTC)
    assert parse_time("2026-05-18T12:00:00") == datetime(2026, 5, 18, 12, tzinfo=UTC)
    assert parse_time("0001-01-01T00:00:00Z") is None
    assert parse_time("") is None
    assert parse_time(None) is None
    assert parse_time("not a time") is None
    assert format_time(datetime(2026, 1, 1)) == "2026-01-01T00:00:00Z"
