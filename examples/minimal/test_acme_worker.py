"""The example worker end to end: a real nats-server, the fake core, the
contract's mechanical checks (presence, consumers, discover), an import
through reconcile, a re-parse from the archive, and the unit-level handler
behaviour against the fakes."""

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from google.protobuf import json_format

from cairn.worker.v1 import worker_service_pb2 as w
from cairn_provider_sdk import Job, ParserTooOldError, Settings, TerminalJobError, archive
from cairn_provider_sdk.archive import ResultRef
from cairn_provider_sdk.testing import FakeCore, FakeJS, FakeNC, NatsServer, make_worker, nats_server_binary
from examples.minimal.acme_worker import IDENTITY, AcmeAPI, AcmeWorker, demo_activities

ACC = {"account_id": "acc-1", "user_id": "u1", "provider": "acme"}


@pytest.fixture
def server() -> Iterator[NatsServer]:
    if not nats_server_binary():
        if os.environ.get("CI"):
            pytest.fail("nats-server not installed; run `make tools`")
        pytest.skip("nats-server not installed; run `make tools`")
    with NatsServer() as srv:
        yield srv


def test_acme_worker_end_to_end(server: NatsServer) -> None:
    async def scenario() -> None:
        core = await FakeCore.start(server.url, provider="acme", release="v0.7.0")
        core.account("acc-1", client_id="me", client_secret="t0k")
        wk = AcmeWorker(AcmeAPI(demo_activities(3)), Settings(nats_url=server.url, instance_id="e1"))
        run = asyncio.create_task(wk.run())
        await core.wait_for_consumers(IDENTITY, ["fetch_source", "reconcile", "parse_blob"])
        hb = await core.wait_for_presence(IDENTITY, "e1")
        assert hb["capabilities"]["Activity"]["read"] is True

        page1 = await core.discover(IDENTITY, "acc-1")
        assert [i["external_id"] for i in page1["items"]] == ["100", "101"]
        assert page1["next_page"] == 2
        assert (await core.discover(IDENTITY, "acc-1", 3)) == {"items": [], "complete": True}
        assert "error" in await core.discover(IDENTITY, "unknown")

        await core.publish_job(IDENTITY, "reconcile", {**ACC, "job_id": "j1", "known_ext_ids": ["101"]})
        rec = await core.await_result("reconcile")
        assert rec.job_result.new_watermark.ToDatetime(tzinfo=UTC) > datetime.now(UTC) - timedelta(hours=2)
        imported = {
            (await core.await_result("fetch_source", skip=i)).job_result.events[0].activity.ref.external_id
            for i in range(2)
        }
        assert imported == {"100", "102"}  # 101 was known
        archived = [k for k in core.blobs.blobs if k.startswith("users/")]
        assert sorted(archived) == ["users/u1/acme/100.json", "users/u1/acme/102.json"]

        blob = {"url": "", "fallback_handle": "users/u1/acme/100.json"}
        await core.publish_job(IDENTITY, "parse_blob", {**ACC, "job_id": "j2", "ext_id": "100", "blob": blob})
        reparsed = await core.await_result("fetch_source", skip=2)
        assert reparsed.job_result.events[0].activity.payload.title == "Run 0"
        assert reparsed.job_result.events[0].activity.raw_blob_id == "users/u1/acme/100.json"

        await core.wait_drained()
        wk.stop()
        await asyncio.wait_for(run, 15)
        await core.close()

    asyncio.run(scenario())


# --- handlers against the fakes ---


def fake_worker(acts: list[dict[str, Any]] | None = None) -> tuple[AcmeWorker, FakeJS, list[bytes]]:
    nc = FakeNC({IDENTITY.creds_subject: {"client_id": "me", "client_secret": "t0k"}})
    wk, js = make_worker(AcmeWorker(AcmeAPI(acts or demo_activities(2))), nc)
    return wk, js, []


@pytest.fixture
def uploads(monkeypatch: pytest.MonkeyPatch) -> list[bytes]:
    bodies: list[bytes] = []

    async def fake_upload(nc: object, provider: str, body: bytes) -> ResultRef:
        bodies.append(body)
        return {"blob_id": f"transfer/{len(bodies)}", "size": len(body), "sha256": "x"}

    async def fake_archive(nc: object, provider: str, *args: Any) -> tuple[str | None, int]:
        return "raw/100.json", 99

    monkeypatch.setattr(archive, "upload_result", fake_upload)
    monkeypatch.setattr(archive, "archive", fake_archive)
    return bodies


def test_fetch_source_maps_and_archives(uploads: list[bytes]) -> None:
    wk, js, _ = fake_worker()
    asyncio.run(wk.fetch_source(Job.of({**ACC, "ext_id": "100"})))
    full = json_format.Parse(uploads[0], w.JobResult())
    act = full.events[0].activity
    assert (act.ref.external_id, act.raw_blob_id, act.raw_size_bytes) == ("100", "raw/100.json", 99)
    assert act.payload.summary.distance_m == 5000
    assert js.published[0][0] == IDENTITY.result_subject("fetch_source")


def test_fetch_source_terminal_errors() -> None:
    wk, _, _ = fake_worker()
    with pytest.raises(TerminalJobError, match="ext_id"):
        asyncio.run(wk.fetch_source(Job.of(ACC)))
    with pytest.raises(TerminalJobError, match="no activity"):
        asyncio.run(wk.fetch_source(Job.of({**ACC, "ext_id": "999"})))


def test_reconcile_window_and_coalescing() -> None:
    now = datetime.now(UTC)
    acts = [
        {"id": 1, "name": "new", "start": (now - timedelta(hours=1)).isoformat()},
        {"id": 2, "name": "old", "start": (now - timedelta(days=40)).isoformat()},
    ]
    wk, js, _ = fake_worker(acts)
    asyncio.run(wk.reconcile(Job.of({**ACC, "job_id": "j1"})))
    fetches = [s for s, _, _ in js.published if s == IDENTITY.job_subject("fetch_source")]
    assert len(fetches) == 1
    assert js.published[-1][0] == f"{IDENTITY.result_subject('reconcile')}.acc-1"
    asyncio.run(wk.reconcile(Job.of({**ACC, "job_id": "j2"})))  # coalesced: nothing new published
    assert len(js.published) == 2


def test_parse_blob_guards(monkeypatch: pytest.MonkeyPatch, uploads: list[bytes]) -> None:
    wk, _, _ = fake_worker()
    with pytest.raises(ParserTooOldError):
        asyncio.run(wk.parse_blob(Job.of({**ACC, "min_parser_version": "1.9"})))

    async def future(nc: object, provider: str, url: str, fallback: str) -> dict[str, Any]:
        return {"version": 2}

    monkeypatch.setattr(archive, "fetch", future)
    with pytest.raises(TerminalJobError, match="dataset"):
        asyncio.run(wk.parse_blob(Job.of({**ACC, "blob": {}})))
    assert uploads == []
