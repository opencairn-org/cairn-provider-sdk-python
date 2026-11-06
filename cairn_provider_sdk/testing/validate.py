"""Core's import rules for a worker's results, so a test fails where core
would reject an activity or refuse a metric (the Go SDK's sdktest does the
same). Mirrors cairn-core's Activity.Validate and metrics.Store.Import.
"""

from __future__ import annotations

import json
import re
import zoneinfo

from cairn.v1 import activity_pb2 as a
from cairn.worker.v1 import worker_service_pb2 as w

# Data types core files in its daily health series, with the reserved store
# key a worker must not send alongside each.
HEALTH_TYPES = {
    "Steps": "steps",
    "HRV": "hrv_rmssd_ms",
    "RestingHR": "resting_hr_bpm",
    "Sleep": "sleep_total_min",
    "Weight": "weight_kg",
    "WaterIntake": "",
}

# cairn-core internal/domain/metric_query.go: key syntax and the value shape of reserved keys.
_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_.]*$")
_MAX_KEY_LEN = 64
_GO_ZERO_TIME = -62135596800  # 0001-01-01T00:00:00Z in Unix seconds
STRUCT_METRIC_KEYS = frozenset({"sleep_stages"})
RESERVED_NUMERIC_METRIC_KEYS = frozenset(
    {
        "weight_kg", "body_fat_pct", "lean_mass_kg", "bone_mass_kg", "hydration_pct", "muscle_mass_kg",
        "resting_hr_bpm", "max_hr_bpm", "hrv_rmssd_ms", "hrv_sdnn_ms",
        "blood_pressure_systolic_mmhg", "blood_pressure_diastolic_mmhg", "spo2_pct",
        "steps", "floors_climbed", "active_minutes", "calories_burned_kcal",
        "sleep_total_min", "sleep_deep_min", "sleep_rem_min", "sleep_light_min", "sleep_awake_min", "sleep_score",
        "vo2max_ml_kg_min", "ftp_cycling_w", "lthr_running_bpm", "lthr_cycling_bpm",
        "threshold_pace_running_s_per_km", "css_swimming_s_per_100m", "critical_power_w", "w_prime_kj",
        "tss", "ctl", "atl", "tsb",
        "mood", "energy", "soreness", "stress", "motivation",
    }
)  # fmt: skip


def validate_activity(ia: w.ImportedActivity) -> None:
    """Raise ValueError where core would reject the activity: a known type,
    end not before start, durations not negative, moving within elapsed, and
    a loadable IANA zone. Discipline/type compatibility is core's merge logic."""
    if not ia.HasField("payload"):
        raise ValueError("payload is missing")
    p = ia.payload
    if p.type == a.ACTIVITY_TYPE_UNSPECIFIED or p.type not in a.ActivityType.values():
        raise ValueError(f"type {p.type} is invalid")
    if p.HasField("start_time") and p.HasField("end_time") and p.end_time.ToDatetime() < p.start_time.ToDatetime():
        raise ValueError("end_time precedes start_time")
    elapsed, moving = p.elapsed_duration.ToTimedelta(), p.moving_duration.ToTimedelta()
    if elapsed.total_seconds() < 0 or moving.total_seconds() < 0:
        raise ValueError("duration is negative")
    if moving > elapsed:
        raise ValueError(f"moving_duration {moving} exceeds elapsed_duration {elapsed}")
    if not p.timezone:
        raise ValueError("timezone is required")
    try:
        zoneinfo.ZoneInfo(p.timezone)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"timezone {p.timezone!r} is not an IANA zone") from e


def validate_metric(im: w.ImportedMetric) -> None:
    """Raise ValueError where core would refuse the metric (user and account,
    a timestamp, exactly one value, a health data type or valid store key, the
    value shape a reserved key dictates, valid JSON), or derive its dedup id
    because external_id is empty."""
    _check_ref(im.ref)
    # Core refuses a missing ts or Go's zero time (0001-01-01); the Unix epoch is a value.
    if not im.HasField("ts") or (im.ts.seconds == _GO_ZERO_TIME and im.ts.nanos == 0):
        raise ValueError("ts is required")
    has_num, has_json = _check_value(im)
    key = im.key
    if key in HEALTH_TYPES:
        return
    if len(key) > _MAX_KEY_LEN or not _KEY_RE.match(key):
        raise ValueError(f"key {key!r} is neither a health data type nor a valid store key")
    if (key in STRUCT_METRIC_KEYS and has_num) or (key in RESERVED_NUMERIC_METRIC_KEYS and has_json):
        raise ValueError(f"key {key!r} carries the wrong value shape")


def _check_ref(ref: w.ExternalRef) -> None:
    if not ref.user_id:
        raise ValueError("ref.user_id is required")
    if not ref.external_account_id:
        raise ValueError("ref.external_account_id is required")
    if not ref.external_id:
        raise ValueError("ref.external_id is required for a stable upsert")


def _check_value(im: w.ImportedMetric) -> tuple[bool, bool]:
    """Exactly one value, JSON valid; returns (numeric, json)."""
    has_num, has_json = im.HasField("value_numeric"), bool(im.value_json.strip())
    if has_num == has_json:
        raise ValueError("exactly one of value_numeric and value_json is required")
    if has_json:
        try:
            json.loads(im.value_json)
        except ValueError as e:
            raise ValueError("value_json is not valid JSON") from e
    return has_num, has_json


def result_problems(jr: w.JobResult) -> list[str]:
    """Every activity core would reject and metric it would refuse or
    duplicate in one result, plus a value sent both as a data type and as its
    reserved store twin."""
    out: list[str] = []
    keys: set[str] = set()
    for ev in jr.events:
        if ev.HasField("activity"):
            try:
                validate_activity(ev.activity)
            except ValueError as e:
                out.append(f"activity {ev.activity.ref.external_id!r}: {e}")
        elif ev.HasField("metric"):
            keys.add(ev.metric.key)
            try:
                validate_metric(ev.metric)
            except ValueError as e:
                out.append(f"metric {ev.metric.key!r} at {ev.metric.ts.ToJsonString()}: {e}")
    for health, twin in HEALTH_TYPES.items():
        if twin and health in keys and twin in keys:
            out.append(f"metric {twin} is sent both as {health} and as {twin}; send the data type only")
    return out


def assert_importable(jr: w.JobResult) -> w.JobResult:
    """Fail with every problem core would have with the result; returns it."""
    problems = result_problems(jr)
    assert not problems, "core would not import this result:\n  " + "\n  ".join(problems)
    return jr
