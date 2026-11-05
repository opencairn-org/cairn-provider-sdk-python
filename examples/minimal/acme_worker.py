"""A complete Cairn worker for a fake provider ("acme") with an in-memory API.

It shows everything a credential-based activity provider needs: identity and
manifest, discover, fetch_source with archive + claim-checked result,
reconcile with window/coalescing/sub-jobs, parse_blob re-parse from the
archive, and the credentials RPC. Run it against a dev core:

    CAIRN_NATS_URL=nats://localhost:4222 uv run python examples/minimal/acme_worker.py
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from google.protobuf import timestamp_pb2

from cairn.v1 import activity_pb2 as a
from cairn.worker.v1 import worker_service_pb2 as w
from cairn_provider_sdk import (
    Identity,
    Job,
    ParserTooOldError,
    Settings,
    TerminalJobError,
    Worker,
    parser_allows,
    serve,
    worker_name,
)
from cairn_provider_sdk import reconcile as R

log = logging.getLogger("cairn.acme")

PER_PAGE = 2

IDENTITY = Identity(
    provider="acme",
    package="cairn.workers.acme",
    version="1.0",  # parser N.M: dataset 1, output revision 0
    dataset_version=1,
    worker_name=worker_name("acme-fetcher"),
)

MANIFEST = {"Activity": {"read": True, "backfill": True, "granularity": "per-activity"}}

PROVIDER_META = {
    "display_name": "Acme",
    "activity_url_template": "https://acme.example/activities/{id}",
    "auth_method": "credentials",
    "config_fields": [
        {"key": "client_id", "label": "Acme user"},
        {"key": "client_secret", "label": "API token", "secret": True},
    ],
}


class AcmeAPI:
    """The provider: activities newest first, behind a per-user token."""

    def __init__(self, activities: list[dict[str, Any]] | None = None, token: str = "t0k") -> None:
        self.activities = activities or []
        self.token = token

    def _auth(self, token: str) -> None:
        if token != self.token:
            raise PermissionError("acme: bad token")

    def list(self, token: str, offset: int, limit: int) -> list[dict[str, Any]]:
        self._auth(token)
        newest_first = sorted(self.activities, key=lambda x: x["start"], reverse=True)
        return newest_first[offset : offset + limit]

    def get(self, token: str, ext_id: str) -> dict[str, Any]:
        self._auth(token)
        for act in self.activities:
            if str(act["id"]) == str(ext_id):
                return act
        raise LookupError(f"acme: no activity {ext_id}")


def map_activity(env: dict[str, Any], *, user_id: str, account_id: str, ext_id: str, blob_id: str) -> w.WorkerEvent:
    """Archive envelope → ACTIVITY event. Pure: a re-parse reproduces the import."""
    act = env["activity"]
    start = datetime.fromisoformat(act["start"])
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(start)
    payload = a.ActivitySourcePayload(
        type=a.ACTIVITY_TYPE_RUN,
        title=str(act.get("name") or ""),
        start_time=ts,
        timezone="UTC",
        summary=a.ActivitySummary(distance_m=float(act.get("distance_m") or 0)),
    )
    ref = w.ExternalRef(user_id=user_id, provider="acme", external_account_id=account_id, external_id=ext_id)
    return w.WorkerEvent(
        type=w.WORKER_EVENT_TYPE_ACTIVITY, activity=w.ImportedActivity(ref=ref, raw_blob_id=blob_id, payload=payload)
    )


class AcmeWorker(Worker):
    def __init__(self, api: AcmeAPI, settings: Settings | None = None) -> None:
        super().__init__(IDENTITY, settings, manifest=MANIFEST, provider_meta=PROVIDER_META)
        self.api = api
        self._tokens: dict[str, str] = {}
        self._coalescer = R.Coalescer()
        self.handle("fetch_source", self.fetch_source)
        self.handle("reconcile", self.reconcile)
        self.handle("parse_blob", self.parse_blob)
        self.on_discover(self.discover)

    async def _token(self, account_id: str) -> str:
        if account_id not in self._tokens:
            creds = await self.fetch_creds(account_id)
            self._tokens[account_id] = creds.client_secret
        return self._tokens[account_id]

    async def discover(self, req: dict[str, Any]) -> dict[str, Any]:
        page = max(int(req.get("start_page", 1) or 1), 1)
        token = await self._token(req.get("account_id", ""))
        acts = await asyncio.to_thread(self.api.list, token, (page - 1) * PER_PAGE, PER_PAGE)
        if not acts:
            return {"items": [], "complete": True}
        items = [{"item_type": "activity", "external_id": str(x["id"]), "item_time": x["start"]} for x in acts]
        return {"items": items, "next_page": page + 1}

    async def fetch_source(self, job: Job) -> None:
        d = job.data
        ext_id = str(d.get("ext_id") or "")
        if not ext_id:
            raise TerminalJobError("missing ext_id")
        account_id, user_id = d.get("account_id", ""), d.get("user_id", "")
        token = await self._token(account_id)
        try:
            act = await asyncio.to_thread(self.api.get, token, ext_id)
        except LookupError as e:
            raise TerminalJobError(str(e)) from e
        env = self.envelope(activity=act)
        blob_id, size = await self.archive(user_id, ext_id, env)
        event = map_activity(env, user_id=user_id, account_id=account_id, ext_id=ext_id, blob_id=blob_id or "")
        if blob_id:
            event.activity.raw_content_type = "application/json"
            event.activity.raw_size_bytes = size
        await self.publish_result(self.identity.result_subject("fetch_source"), self.job_result([event]))
        log.info("published activity %s for user %s", ext_id, user_id)

    async def reconcile(self, job: Job) -> None:
        d = job.data
        account_id = d.get("account_id", "")
        if not self._coalescer.should_run(account_id):
            return  # coalesced: ack without API calls
        watermark = R.parse_watermark(d.get("watermark"))
        start = R.window_start(watermark)
        known = set(d.get("known_ext_ids") or [])
        token = await self._token(account_id)
        enqueued, high_water, offset = 0, watermark, 0
        while enqueued < R.max_enqueue(d):
            acts = await asyncio.to_thread(self.api.list, token, offset, PER_PAGE)
            if not acts:
                break
            for act in acts:
                started = datetime.fromisoformat(act["start"])
                if started < start:
                    acts = []  # newest first: the rest is older
                    break
                if str(act["id"]) in known:
                    continue
                await self.enqueue_fetch(d, str(act["id"]))
                enqueued += 1
                high_water = max(high_water or started, started)
            if len(acts) < PER_PAGE:
                break
            offset += PER_PAGE
        await self.publish_reconcile_result(d, high_water)
        log.info("reconcile for %s: %d new activity job(s) enqueued", account_id, enqueued)

    async def parse_blob(self, job: Job) -> None:
        d = job.data
        min_parser = d.get("min_parser_version", "")
        if not parser_allows(self.identity.version, min_parser):
            raise ParserTooOldError(f"source parsed by {min_parser}, this parser is {self.identity.version}")
        blob = d.get("blob") or {}
        env = await self.fetch_blob(blob.get("url", ""), blob.get("fallback_handle", ""))
        if env.get("version") != self.identity.dataset_version:
            raise TerminalJobError(
                f"archive dataset {env.get('version')}, parser reads {self.identity.dataset_version}"
            )
        event = map_activity(
            env,
            user_id=d.get("user_id", ""),
            account_id=d.get("account_id", ""),
            ext_id=str(d.get("ext_id", "")),
            blob_id=blob.get("fallback_handle", ""),
        )
        await self.publish_result(self.identity.result_subject("fetch_source"), self.job_result([event]))


def demo_activities(n: int = 3) -> list[dict[str, Any]]:
    now = datetime.now(UTC).replace(microsecond=0)
    return [
        {"id": 100 + i, "name": f"Run {i}", "start": (now - timedelta(hours=i + 1)).isoformat(), "distance_m": 5000 + i}
        for i in range(n)
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    serve(AcmeWorker(AcmeAPI(demo_activities())))
