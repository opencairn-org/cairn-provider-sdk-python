"""The test kit's copy of core's import rules."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from cairn.v1 import activity_pb2 as a
from cairn.worker.v1 import worker_service_pb2 as w
from cairn_provider_sdk.testing import assert_importable, result_problems, validate_activity, validate_metric

T0 = datetime(2026, 5, 1, 7, tzinfo=UTC)


def activity(mutate: Callable[[a.ActivitySourcePayload], None] = lambda _: None) -> w.ImportedActivity:
    p = a.ActivitySourcePayload(type=a.ACTIVITY_TYPE_RUN, timezone="Europe/Berlin")
    p.start_time.FromDatetime(T0)
    p.end_time.FromDatetime(T0 + timedelta(hours=1))
    p.elapsed_duration.FromTimedelta(timedelta(hours=1))
    p.moving_duration.FromTimedelta(timedelta(minutes=50))
    mutate(p)
    return w.ImportedActivity(ref=w.ExternalRef(external_id="a1"), payload=p)


def _set(field: str, value: object) -> Callable[[a.ActivitySourcePayload], None]:
    return lambda p: setattr(p, field, value)


@pytest.mark.parametrize(
    ("mutate", "want"),
    [
        (_set("timezone", "Etc/GMT-2"), ""),
        (_set("timezone", ""), "timezone is required"),
        (_set("timezone", "UTC+2"), "not an IANA zone"),
        (_set("type", a.ACTIVITY_TYPE_UNSPECIFIED), "type"),
        (_set("type", 9999), "type"),
        (lambda p: p.end_time.FromDatetime(T0 - timedelta(minutes=1)), "precedes"),
        (lambda p: p.elapsed_duration.FromTimedelta(timedelta(seconds=-1)), "negative"),
        (lambda p: p.moving_duration.FromTimedelta(timedelta(hours=2)), "exceeds"),
    ],
)
def test_validate_activity(mutate: Callable[[a.ActivitySourcePayload], None], want: str) -> None:
    ia = activity(mutate)
    if not want:
        validate_activity(ia)
        return
    with pytest.raises(ValueError, match=want):
        validate_activity(ia)


def test_validate_activity_needs_a_payload() -> None:
    validate_activity(activity())
    with pytest.raises(ValueError, match="payload"):
        validate_activity(w.ImportedActivity())


def metric(key: str, **over: object) -> w.ImportedMetric:
    m = w.ImportedMetric(
        ref=w.ExternalRef(user_id="u", provider="acme", external_account_id="acc", external_id=f"{key}:2026-05-01"),
        key=key,
        value_numeric=1.0,
    )
    m.ts.FromDatetime(T0)
    for k, v in over.items():
        if k in ("user_id", "external_account_id", "external_id"):
            setattr(m.ref, k, v)
        elif k == "ts":
            m.ClearField("ts")
        elif k == "ts_seconds":
            m.ts.seconds, m.ts.nanos = int(v), 0  # type: ignore[call-overload]
        elif k == "value_numeric" and v is None:
            m.ClearField("value_numeric")
        else:
            setattr(m, k, v)
    return m


@pytest.mark.parametrize(
    ("m", "want"),
    [
        (metric("HRV"), ""),
        (metric("spo2_pct"), ""),
        (metric("acme.recovery_score"), ""),
        (metric("sleep_stages", value_numeric=None, value_json='{"deep": 60}'), ""),
        (metric("HRV", user_id=""), "user_id"),
        (metric("HRV", external_account_id=""), "external_account_id"),
        (metric("HRV", external_id=""), "external_id"),
        (metric("HRV", ts=None), "ts is required"),
        (metric("HRV", ts_seconds=-62135596800), "ts is required"),
        (metric("HRV", ts_seconds=0), ""),
        (metric("HRV", value_numeric=None), "exactly one"),
        (metric("acme.x", value_json="{}"), "exactly one"),
        (metric("acme.x", value_numeric=None, value_json="{"), "not valid JSON"),
        (metric("Spo2"), "valid store key"),
        (metric("acme." + "x" * 60), "valid store key"),
        (metric("sleep_stages"), "shape"),
        (metric("sleep_score", value_numeric=None, value_json="80"), "shape"),
    ],
)
def test_validate_metric(m: w.ImportedMetric, want: str) -> None:
    if not want:
        validate_metric(m)
        return
    with pytest.raises(ValueError, match=want):
        validate_metric(m)


def test_result_problems() -> None:
    def ev(**kw: object) -> w.WorkerEvent:
        return w.WorkerEvent(**kw)  # type: ignore[arg-type]

    good = w.JobResult(events=[ev(activity=activity()), ev(metric=metric("HRV")), ev(metric=metric("spo2_pct")), ev()])
    assert result_problems(good) == []
    assert assert_importable(good) is good

    bad = w.JobResult(
        events=[
            ev(activity=activity(_set("timezone", ""))),
            ev(metric=metric("Weight")),
            ev(metric=metric("weight_kg")),
            ev(metric=metric("x", external_id="")),
        ]
    )
    problems = result_problems(bad)
    assert len(problems) == 3, problems
    assert "timezone is required" in problems[0]
    assert "external_id" in problems[1]
    assert "both as Weight and as weight_kg" in problems[2]
    with pytest.raises(AssertionError, match="core would not import"):
        assert_importable(bad)
