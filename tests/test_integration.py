"""The runtime against a real nats-server with core's streams and buckets and
a fake core: presence, manifest, consumers, discover, claim-checked results,
reconcile sub-jobs, parse_blob re-presign, OAuth exchange, webhooks, the
rate limiter and the token refresh lock across two instances."""

import asyncio
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cairn.worker.v1 import worker_service_pb2 as w
from cairn_provider_sdk import (
    ClientCreds,
    Identity,
    Job,
    OAuthExchangeReply,
    OAuthExchangeRequest,
    RetryLaterError,
    Settings,
    TokenState,
    WebhookEvent,
    WebhookVerifyRequest,
    WebhookVerifyResponse,
    Worker,
)
from cairn_provider_sdk.testing import FakeCore, NatsServer, nats_server_binary, token_fields, until

IDENT = Identity(provider="acme", package="cairn.workers.acme", version="1.0", dataset_version=1, release="v0.7.0")


# One server per test: a test must not depend on streams or buckets another test created.
@pytest.fixture
def server() -> Iterator[NatsServer]:
    if not nats_server_binary():
        if os.environ.get("CI"):
            pytest.fail("nats-server not installed; run `make tools`")
        pytest.skip("nats-server not installed; run `make tools`")
    with NatsServer() as srv:
        yield srv


def settings(url: str, instance: str = "i1") -> Settings:
    return Settings(nats_url=url, instance_id=instance, heartbeat_interval_s=1)


def event(ext_id: str, account_id: str = "acc-1") -> w.WorkerEvent:
    ref = w.ExternalRef(user_id="u1", provider="acme", external_account_id=account_id, external_id=ext_id)
    return w.WorkerEvent(type=w.WORKER_EVENT_TYPE_ACTIVITY, activity=w.ImportedActivity(ref=ref))


class EchoWorker(Worker):
    """Enough handlers to exercise every contract path from outside."""

    def __init__(self, url: str, instance: str = "i1") -> None:
        super().__init__(IDENT, settings(url, instance), manifest={"Activity": {"read": True}}, provider_meta={"a": 1})
        self.archived: list[str] = []
        self.webhooks_seen: list[WebhookEvent] = []
        self.handle("fetch_source", self.fetch_source)
        self.handle("reconcile", self.reconcile)
        self.handle("parse_blob", self.parse_blob)
        self.on_discover(self.discover)
        self.on_oauth_exchange(self.exchange)
        self.on_webhook_event(self.webhook)
        self.on_webhook_verify(self.verify)

    async def discover(self, req: dict[str, Any]) -> dict[str, Any]:
        creds = await self.fetch_creds(req.get("account_id", ""))
        return {"items": [{"item_type": "activity", "external_id": creds.client_id, "item_time": ""}], "next_page": 2}

    async def fetch_source(self, job: Job) -> None:
        d = job.data
        if d.get("ext_id") == "later" and job.delivered == 1:
            raise RetryLaterError(0.1, "rate limited")
        env = self.envelope(activity={"id": d["ext_id"]})
        blob_id, _ = await self.archive(d.get("user_id", ""), d["ext_id"], env)
        self.archived.append(blob_id or "")
        await self.publish_result(IDENT.result_subject("fetch_source"), self.job_result([event(d["ext_id"])]))

    async def reconcile(self, job: Job) -> None:
        d = job.data
        await self.enqueue_fetch(d, "7")
        await self.publish_reconcile_result(d, datetime(2026, 5, 1, tzinfo=UTC))

    async def parse_blob(self, job: Job) -> None:
        d = job.data
        env = await self.fetch_blob(d["blob"].get("url", ""), d["blob"]["fallback_handle"])
        await self.publish_result(IDENT.result_subject("parse_blob"), self.job_result([event(env["activity"]["id"])]))

    async def exchange(self, req: OAuthExchangeRequest) -> OAuthExchangeReply:
        state = TokenState(access_token=f"at-{req.code}", expires_at=datetime(2030, 1, 1, tzinfo=UTC))
        return OAuthExchangeReply.of(state, provider_account_id=req.client_id)

    async def webhook(self, ev: WebhookEvent) -> None:
        self.webhooks_seen.append(ev)

    async def verify(self, req: WebhookVerifyRequest) -> WebhookVerifyResponse:
        return WebhookVerifyResponse(body=req.query.get("hub.challenge", "").encode())


def test_worker_against_a_fake_core(server: NatsServer) -> None:
    async def scenario() -> None:
        core = await FakeCore.start(server.url, provider="acme", release="v0.7.3")
        core.account("acc-1", client_id="me", client_secret="pw", provider_account_id="athlete-1")
        wk = EchoWorker(server.url)
        run = asyncio.create_task(wk.run())
        await core.wait_for_consumers(IDENT, ["fetch_source", "reconcile", "parse_blob"])
        hb = await core.wait_for_presence(IDENT, "i1")
        assert hb["release"] == "v0.7.0"
        assert hb["webhooks"] is True

        async def manifest_up() -> bool:
            return (await core.manifest(IDENT) or {}).get("provider_meta") == {"a": 1}

        await until(manifest_up, "manifest")

        assert (await core.discover(IDENT, "acc-1"))["items"][0]["external_id"] == "me"
        assert "error" in await core.discover(IDENT, "nobody")
        assert await wk.lookup_account("athlete-1") == {"account_id": "acc-1", "user_id": "u1"}

        job = {"job_id": "j1", "account_id": "acc-1", "user_id": "u1", "provider": "acme"}
        await core.publish_job(IDENT, "reconcile", job)
        rec = await core.await_result("reconcile")
        assert rec.subject.endswith(".acc-1")
        assert rec.job_result.new_watermark.ToDatetime(tzinfo=UTC) == datetime(2026, 5, 1, tzinfo=UTC)
        fetched = await core.await_result("fetch_source")
        assert fetched.envelope.HasField("payload_ref")
        assert fetched.job_result.events[0].activity.ref.external_id == "7"
        assert wk.archived == ["users/u1/acme/7.json"]

        await core.publish_job(IDENT, "fetch_source", {**job, "ext_id": "later"})
        later = await core.await_result("fetch_source", skip=1, timeout_s=20)
        assert later.job_result.events[0].activity.ref.external_id == "later"

        blob = {"url": "http://127.0.0.1:1/expired", "fallback_handle": "users/u1/acme/7.json"}
        await core.publish_job(IDENT, "parse_blob", {**job, "ext_id": "7", "blob": blob})
        reparsed = await core.await_result("parse_blob")
        assert reparsed.job_result.events[0].activity.ref.external_id == "7"

        reply = await core.exchange_code(IDENT, connection_id="c1", client_id="cid", client_secret="s", code="xyz")
        assert (reply["access_token"], reply["provider_account_id"]) == ("at-xyz", "cid")
        assert await core.verify_webhook(IDENT, {"hub.challenge": "ch"}) == {
            "status": 200,
            "content_type": "application/json",
            "body": "Y2g=",
        }
        await core.forward_webhook(IDENT, b'{"hook": 1}', headers={"x-sig": "abc"})

        async def hooked() -> bool:
            return bool(wk.webhooks_seen)

        await until(hooked, "webhook event")
        assert wk.webhooks_seen[0].headers["x-sig"] == "abc"
        assert wk.webhooks_seen[0].body == b'{"hook": 1}'

        await core.wait_drained()
        await core.wait_drained("CAIRN_WEBHOOKS")
        wk.stop()
        await asyncio.wait_for(run, 15)
        await core.close()

    asyncio.run(scenario())


def test_rate_limiter_on_the_real_bucket(server: NatsServer) -> None:
    async def scenario() -> None:
        core = await FakeCore.start(server.url, provider="acme")
        a, b = Worker(IDENT, settings(server.url, "a")), Worker(IDENT, settings(server.url, "b"))
        await a.connect()
        await b.connect()
        la = await a.rate_limiter({"acme:app:minute": 3})
        lb = await b.rate_limiter({"acme:app:minute": 3})
        results = await asyncio.gather(
            *(la.reserve("acme:app:minute") for _ in range(2)), *(lb.reserve("acme:app:minute") for _ in range(2))
        )
        assert sorted(ok for ok, _ in results) == [False, True, True, True]
        await la.sync_usage("acme:app:minute", used=0, limit=3, resets_at=datetime.now(UTC) + timedelta(minutes=1))
        assert (await lb.reserve("acme:app:minute"))[0] is True
        await a._nats.close()
        await b._nats.close()
        await core.close()

    asyncio.run(scenario())


def test_token_refresh_is_locked_across_instances(server: NatsServer) -> None:
    refreshes: list[str] = []

    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        refreshes.append(state.access_token)
        await asyncio.sleep(0.3)
        return TokenState(access_token="fresh", refresh_token="rt2", expires_at=datetime.now(UTC) + timedelta(hours=1))

    async def scenario() -> None:
        core = await FakeCore.start(server.url, provider="acme")
        acc = core.account("acc-1", client_id="cid", client_secret="cs", app_id="app")
        acc.token = token_fields("stale", expires_in_s=-5, refresh="rt")
        a = Worker(IDENT, settings(server.url, "a"), refresh=refresh)
        b = Worker(IDENT, settings(server.url, "b"), refresh=refresh)
        await a.connect()
        await b.connect()
        ta, tb = await asyncio.gather(a.tokens.get("acc-1"), b.tokens.get("acc-1"))
        assert (ta.access_token, tb.access_token) == ("fresh", "fresh")
        assert refreshes == ["stale"]  # one refresh for two instances
        assert len(core.token_stores) == 1
        assert acc.token["refresh_token"] == "rt2"
        kv = await a._js.key_value("cairn_rate_limits")
        assert "lock.acme.acc-1" not in (await kv.keys() if await _has_keys(kv) else [])
        await a._nats.close()
        await b._nats.close()
        await core.close()

    asyncio.run(scenario())


async def _has_keys(kv: Any) -> bool:
    try:
        await kv.keys()
    except Exception:  # noqa: BLE001 — an empty bucket raises NoKeysError
        return False
    return True


def test_incompatible_core_stops_the_worker(server: NatsServer) -> None:
    async def scenario() -> None:
        core = await FakeCore.start(server.url, provider="acme")
        info = await core.js.key_value("cairn_core")
        await info.put("info", json.dumps({"release": "v0.8.0"}).encode())
        wk = EchoWorker(server.url, "z")
        with pytest.raises(Exception, match=r"major\.minor"):
            await wk.run()
        await core.close()

    asyncio.run(scenario())
