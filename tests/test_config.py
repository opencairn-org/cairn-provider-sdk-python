"""Identity, settings and the subject layout."""

from pathlib import Path

import pytest

from cairn_provider_sdk import config as C
from cairn_provider_sdk.versions import parser_allows, release_compatible


def test_identity_defaults_and_subjects() -> None:
    ident = C.Identity(provider="acme", package="cairn.workers.acme", version="1.2", dataset_version=1)
    assert ident.worker_name == "acme-fetcher"
    assert ident.worker_key == "acme-fetcher-acme"
    assert ident.job_subject("fetch_source") == "cairn.jobs.fetch_source.acme.acme-fetcher"
    assert ident.result_subject("reconcile") == "cairn.results.reconcile.acme.acme-fetcher"
    assert ident.discover_subject == "cairn.discover.acme.acme-fetcher"
    assert ident.oauth_exchange_subject == "cairn.oauth.acme.acme-fetcher.exchange"
    assert ident.lookup_subject == "cairn.accounts.lookup.acme.acme-fetcher"
    assert ident.creds_subject == "cairn.creds.acme.get"
    assert ident.tokens_subject("store") == "cairn.tokens.acme.store"
    assert ident.presign_upload_subject == "cairn.blobs.presign_upload.acme"
    assert ident.presign_download_subject == "cairn.blobs.presign_download.acme"
    assert ident.webhook_event_subject == "cairn.webhooks.acme-fetcher-acme.event"
    assert ident.webhook_verify_subject == "cairn.webhooks.acme-fetcher-acme.verify"


def test_identity_requires_the_core_fields() -> None:
    with pytest.raises(ValueError, match="required"):
        C.Identity(provider="", package="p", version="1.0", dataset_version=1)


def test_worker_key_without_name_is_the_provider() -> None:
    assert C.worker_key("", "acme") == "acme"


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CAIRN_NATS_URL", "nats://x:1")
    monkeypatch.setenv("CAIRN_WORKER_INSTANCE_ID", "pod-7")
    monkeypatch.setenv("CAIRN_WORKER_ENROLLMENT_TOKEN", "tok")
    monkeypatch.setenv("CAIRN_HEARTBEAT_INTERVAL_S", "5")
    monkeypatch.setenv("CAIRN_PULL_BATCH", "2")
    monkeypatch.setenv("CAIRN_WORKER_NAME", "pool-a")
    s = C.Settings()
    assert (s.nats_url, s.instance_id, s.enrollment_token, s.heartbeat_interval_s, s.pull_batch) == (
        "nats://x:1",
        "pod-7",
        "tok",
        5,
        2,
    )
    assert C.worker_name("acme-fetcher") == "pool-a"


def test_settings_generate_an_instance_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAIRN_WORKER_INSTANCE_ID", raising=False)
    assert len(C.Settings().instance_id) == 12
    assert C.Settings().instance_id != C.Settings().instance_id


def test_read_release(tmp_path: Path) -> None:
    assert C.read_release(tmp_path / "RELEASE") == ""
    (tmp_path / "RELEASE").write_text("v0.7.0\n")
    assert C.read_release(tmp_path / "RELEASE") == "v0.7.0"


def test_result_subject_keeps_the_pool() -> None:
    assert C.result_subject_for("cairn.jobs.fetch_source.garmin.w1") == "cairn.results.fetch_source.garmin.w1"
    assert C.result_subject_for("cairn.jobs.fetch_source.garmin") == "cairn.results.fetch_source.garmin"
    assert C.result_subject_for("other") == "other.result"


@pytest.mark.parametrize(
    ("own", "min_version", "ok"),
    [
        ("1.2", "", True),
        ("1.2", "1", True),
        ("1.2", "1.2", True),
        ("1.2", "1.3", False),
        ("2.0", "1.4", False),
        ("1.2", "dev", False),
        ("dev", "dev", True),
    ],
)
def test_parser_allows(own: str, min_version: str, ok: bool) -> None:
    assert parser_allows(own, min_version) is ok


@pytest.mark.parametrize(
    ("core", "worker", "ok"),
    [
        ("v0.5.0", "v0.5.3", True),
        ("v0.5.2-3-gabc-dirty", "v0.5.0", True),
        ("v0.5.0", "v0.4.9", False),
        ("v0.5.0", "v0.6.0", False),
        ("v1.5.0", "v0.5.0", False),
        ("v1.3.0", "v1.1.4", True),
        ("v1.3.0", "v1.4.0", False),
        ("dev", "v0.4.0", True),
        ("v0.5.0", "", True),
    ],
)
def test_release_compatible(core: str, worker: str, ok: bool) -> None:
    assert release_compatible(core, worker) is ok
