"""Worker identity, NATS settings from the environment, and the subject layout
of the Cairn worker contract (cairn-core ADR 0006: everything is per pool).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

JOB_STREAM = "CAIRN_JOBS"
WEBHOOK_STREAM = "CAIRN_WEBHOOKS"
CORE_INFO_KV = "cairn_core"
PRESENCE_KV = "cairn_worker_presence"
MANIFEST_KV = "cairn_worker_manifests"
RATE_LIMITS_KV = "cairn_rate_limits"

# Job kinds a pool may consume (provider contract §3).
JOB_TYPES = ("fetch_source", "parse_blob", "backfill", "reconcile", "import_metrics", "import_gear")

# Durable consumer name suffix per job kind: `<worker_name>-<suffix>`. Stable
# across releases so an upgraded worker resumes its pool's durables.
DURABLE_SUFFIX = {
    "fetch_source": "fetch",
    "import_metrics": "metrics",
    "parse_blob": "parseblob",
    "reconcile": "reconcile",
    "import_gear": "gear",
    "backfill": "backfill",
}


def worker_name(default: str) -> str:
    """The admin-defined routing name, shared by every instance of a pool."""
    return os.getenv("CAIRN_WORKER_NAME", default)


def read_release(path: Path) -> str:
    """Release tag written next to the worker package at build time; "" in dev."""
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def worker_key(name: str, provider: str) -> str:
    return f"{name}-{provider}" if name else provider


def job_subject(job: str, provider: str, name: str) -> str:
    return f"cairn.jobs.{job}.{provider}.{name}"


def result_subject(job: str, provider: str, name: str) -> str:
    return f"cairn.results.{job}.{provider}.{name}"


def result_subject_for(job_subject: str) -> str:
    """The result subject of a job subject; the pool token stays
    (cairn.jobs.fetch_source.garmin.w1 → cairn.results.fetch_source.garmin.w1).
    """
    if job_subject.startswith("cairn.jobs."):
        return "cairn.results." + job_subject.removeprefix("cairn.jobs.")
    return job_subject + ".result"


def discover_subject(provider: str, name: str) -> str:
    return f"cairn.discover.{provider}.{name}"


def oauth_exchange_subject(provider: str, name: str) -> str:
    return f"cairn.oauth.{provider}.{name}.exchange"


def lookup_subject(provider: str, name: str) -> str:
    return f"cairn.accounts.lookup.{provider}.{name}"


def creds_subject(provider: str) -> str:
    return f"cairn.creds.{provider}.get"


def tokens_subject(provider: str, op: str) -> str:
    return f"cairn.tokens.{provider}.{op}"


def presign_upload_subject(provider: str) -> str:
    return f"cairn.blobs.presign_upload.{provider}"


def presign_download_subject(provider: str) -> str:
    return f"cairn.blobs.presign_download.{provider}"


def webhook_event_subject(key: str) -> str:
    return f"cairn.webhooks.{key}.event"


def webhook_verify_subject(key: str) -> str:
    return f"cairn.webhooks.{key}.verify"


@dataclass(frozen=True)
class Identity:
    """Compile-time identity of a worker (cairn-core ADR 0005): provider,
    package, parser version `N.M` for dataset `N`, and the release tag.
    """

    provider: str
    package: str
    version: str
    dataset_version: int
    release: str = ""
    worker_name: str = ""

    def __post_init__(self) -> None:
        if not self.provider or not self.package or not self.version:
            raise ValueError("provider, package and version are required")
        if not self.worker_name:
            object.__setattr__(self, "worker_name", f"{self.provider}-fetcher")

    @property
    def worker_key(self) -> str:
        return worker_key(self.worker_name, self.provider)

    def job_subject(self, job: str) -> str:
        return job_subject(job, self.provider, self.worker_name)

    def result_subject(self, job: str) -> str:
        return result_subject(job, self.provider, self.worker_name)

    @property
    def discover_subject(self) -> str:
        return discover_subject(self.provider, self.worker_name)

    @property
    def oauth_exchange_subject(self) -> str:
        return oauth_exchange_subject(self.provider, self.worker_name)

    @property
    def lookup_subject(self) -> str:
        return lookup_subject(self.provider, self.worker_name)

    @property
    def creds_subject(self) -> str:
        return creds_subject(self.provider)

    def tokens_subject(self, op: str) -> str:
        return tokens_subject(self.provider, op)

    @property
    def presign_upload_subject(self) -> str:
        return presign_upload_subject(self.provider)

    @property
    def presign_download_subject(self) -> str:
        return presign_download_subject(self.provider)

    @property
    def webhook_event_subject(self) -> str:
        return webhook_event_subject(self.worker_key)

    @property
    def webhook_verify_subject(self) -> str:
        return webhook_verify_subject(self.worker_key)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass
class Settings:
    """NATS connection and runtime settings, read from the environment.
    Workers extend it with their own fields (dataclass inheritance).
    """

    nats_url: str = field(default_factory=lambda: os.getenv("CAIRN_NATS_URL", "nats://localhost:4222"))
    # Per-instance id, only needs to be UNIQUE per running instance (it
    # disambiguates replicas in presence). Unset → random; in k8s set it to
    # the pod name so a restart reuses its presence key.
    instance_id: str = field(default_factory=lambda: os.getenv("CAIRN_WORKER_INSTANCE_ID", "") or uuid.uuid4().hex[:12])
    # Enrollment token from core; its NATS auth-callout admits it scoped to the provider.
    enrollment_token: str = field(default_factory=lambda: os.getenv("CAIRN_WORKER_ENROLLMENT_TOKEN", ""))
    nats_user: str = field(default_factory=lambda: os.getenv("CAIRN_NATS_USER", ""))
    nats_password: str = field(default_factory=lambda: os.getenv("CAIRN_NATS_PASSWORD", ""))
    heartbeat_interval_s: int = field(default_factory=lambda: _env_int("CAIRN_HEARTBEAT_INTERVAL_S", 20))
    pull_batch: int = field(default_factory=lambda: _env_int("CAIRN_PULL_BATCH", 4))
