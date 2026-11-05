"""The runtime against the in-memory fakes: settlement, presence, manifest,
responders, results, failure envelopes, release check and run() wiring."""

import asyncio
import base64
import json
from datetime import UTC, datetime
from typing import Any, cast

import nats
import pytest
from google.protobuf import json_format
from nats.js.errors import KeyNotFoundError

from cairn.worker.v1 import worker_service_pb2 as w
from cairn_provider_sdk import (
    IncompatibleCoreError,
    Job,
    OAuthExchangeReply,
    OAuthExchangeRequest,
    ParserTooOldError,
    RetryLaterError,
    TerminalJobError,
    WebhookEvent,
    WebhookVerifyRequest,
    WebhookVerifyResponse,
    Worker,
    archive,
)
from cairn_provider_sdk.archive import ResultRef
from cairn_provider_sdk.testing import (
    FakeJS,
    FakeKV,
    FakeMsg,
    FakeNC,
    FakePullSub,
    Responder,
    example_identity,
    example_settings,
    make_worker,
)

IDENT = example_identity(release="v0.7.0")
FETCH = IDENT.job_subject("fetch_source")
RESULT = IDENT.result_subject("fetch_source")


def worker(nc: FakeNC | None = None, **kw: Any) -> tuple[Worker, FakeJS]:
    return make_worker(Worker(IDENT, example_settings(), manifest={"Activity": {"read": True}}, **kw), nc)


def results(js: FakeJS, subject: str) -> list[w.JobResult]:
    return [json_format.Parse(d, w.JobResult()) for d in js.bodies(subject)]


# --- registration ---


def test_handle_validates_job_types() -> None:
    wk, _ = worker()

    async def h(job: Job) -> None:
        pass

    wk.handle("fetch_source", h)
    with pytest.raises(ValueError, match="already"):
        wk.handle("fetch_source", h)
    with pytest.raises(ValueError, match="unknown job type"):
        wk.handle("make_coffee", h)
    assert wk.jobs == ("fetch_source",)


def test_job_parses_its_body_lazily() -> None:
    job = Job.of({"ext_id": "7"}, subject="s")
    assert job.data == {"ext_id": "7"}
    with pytest.raises(ValueError, match="JSON object"):
        _ = Job(subject="s", body=b"[1]").data


def test_unconnected_worker_refuses_io() -> None:
    wk = Worker(IDENT)
    with pytest.raises(RuntimeError, match="NATS"):
        _ = wk._nats
    with pytest.raises(RuntimeError, match="JetStream"):
        _ = wk._js


# --- consume + settle ---


def test_consume_settles_each_message() -> None:
    wk, js = worker()
    ok = FakeMsg(b'{"ok": true}')
    retry = FakeMsg(b"{}")
    dead = FakeMsg(b'{"ext_id": "9"}', wk.MAX_DELIVERIES)
    poison = FakeMsg(b'{"ext_id": "10", "poison": true}')
    later = FakeMsg(b'{"later": true}')
    js.psub = FakePullSub([RuntimeError("flaky"), [ok, retry, dead, poison, later]], wk.stop)

    async def handler(job: Job) -> None:
        if job.data.get("poison"):
            raise TerminalJobError("unmappable")
        if job.data.get("later"):
            raise RetryLaterError(42, "rate limited")
        if not job.data.get("ok"):
            raise RuntimeError("boom")

    async def run() -> None:
        wk.ACK_EXTEND_S = 0
        await wk._consume(FETCH, "fetch", handler)

    asyncio.run(run())
    assert ok.settled[-1] == "ack"
    assert retry.settled[-1] == f"nak:{wk.NAK_DELAY_S}"
    assert dead.settled[-1] == "term"
    assert poison.settled[-1] == "term"
    assert later.settled[-1] == "nak:42"
    assert [r.failed_ref.external_id for r in results(js, RESULT)] == ["9", "10"]
    assert js.subscribed == [(FETCH, {"durable": "fetch", "stream": "CAIRN_JOBS"})]


def test_parser_too_old_backs_off_then_gives_up() -> None:
    wk, js = worker()
    first = FakeMsg(b'{"ext_id": "1"}', 1)
    third = FakeMsg(b'{"ext_id": "1"}', 3)
    last = FakeMsg(b'{"ext_id": "1"}', wk.PARSER_TOO_OLD_ATTEMPTS)

    async def handler(job: Job) -> None:
        raise ParserTooOldError("newer parser needed")

    async def run() -> None:
        for m in (first, third, last):
            await wk._settle(FETCH, cast(Any, m), handler)

    asyncio.run(run())
    assert first.settled == ["nak:30"]
    assert third.settled == ["nak:120"]
    assert last.settled == ["term"]
    (failure,) = results(js, RESULT)
    assert failure.error.code == "parser_too_old"


def test_ack_extension_heartbeats_until_cancelled() -> None:
    wk, _ = worker()
    m = FakeMsg(b"{}")

    async def run() -> None:
        wk.ACK_EXTEND_S = 0
        task = asyncio.create_task(wk._extend_ack(cast(Any, m)))
        await asyncio.sleep(0.01)
        task.cancel()
        await task

    asyncio.run(run())
    assert "in_progress" in m.settled


# --- results ---


def test_publish_result_claim_checks_events(monkeypatch: pytest.MonkeyPatch) -> None:
    wk, js = worker()
    uploaded: dict[str, bytes] = {}

    async def fake_upload(nc: object, provider: str, body: bytes) -> ResultRef:
        uploaded["body"] = body
        return {"blob_id": f"transfer/{provider}/x.json", "size": len(body), "sha256": "abc"}

    monkeypatch.setattr(archive, "upload_result", fake_upload)
    jr = wk.job_result([w.WorkerEvent(type=w.WORKER_EVENT_TYPE_ACTIVITY)])
    jr.new_watermark.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    asyncio.run(wk.publish_result(RESULT, jr, headers={"Nats-Msg-Id": "result:j1:d1"}))

    ((subject, data, headers),) = js.published
    assert (subject, headers) == (RESULT, {"Nats-Msg-Id": "result:j1:d1"})
    env = json_format.Parse(data, w.JobResult())
    assert env.payload_ref.blob_id == "transfer/acme/x.json"
    assert env.payload_ref.size_bytes == len(uploaded["body"])
    assert env.payload_ref.content_sha256 == "abc"
    assert env.HasField("new_watermark")
    assert not env.events
    full = json_format.Parse(uploaded["body"], w.JobResult())
    assert len(full.events) == 1
    assert (full.worker_name, full.worker_version, full.worker_package, full.worker_release) == (
        "acme-fetcher",
        "1.0",
        "cairn.workers.acme",
        "v0.7.0",
    )


def test_publish_result_inline_when_no_events() -> None:
    wk, js = worker()
    asyncio.run(wk.publish_result(RESULT, wk.job_result()))
    ((_, data, _),) = js.published
    assert not json_format.Parse(data, w.JobResult()).HasField("payload_ref")


def test_publish_failure_envelope() -> None:
    wk, js = worker()
    body = json.dumps({"ext_id": "99", "account_id": "a1", "user_id": "u1"}).encode()
    asyncio.run(wk.publish_failure(FETCH, body, RuntimeError("boom")))
    ((subject, data, _),) = js.published
    assert subject == RESULT
    env = json_format.Parse(data, w.JobResult())
    assert (env.failed_ref.external_id, env.failed_ref.external_account_id, env.failed_ref.user_id) == (
        "99",
        "a1",
        "u1",
    )
    assert env.failed_ref.provider == "acme"
    assert (env.error.message, env.error.code) == ("boom", "RuntimeError")


@pytest.mark.parametrize("body", [b"not json", b"[1]", json.dumps({"user_id": "u1"}).encode()])
def test_failure_envelope_needs_a_job_with_ext_id(body: bytes) -> None:
    wk, js = worker()
    asyncio.run(wk.publish_failure(FETCH, body, RuntimeError("x")))
    assert js.published == []


def test_failure_publish_errors_are_swallowed() -> None:
    wk, js = worker()

    async def broken(*_: Any, **__: Any) -> None:
        raise RuntimeError("nats down")

    js.publish = broken  # type: ignore[method-assign]
    asyncio.run(wk.publish_failure(FETCH, b'{"ext_id": "1"}', RuntimeError("x")))


def test_reconcile_result_and_sub_jobs() -> None:
    wk, js = worker()
    job = {"job_id": "j1", "account_id": "acc-1", "user_id": "u-1"}
    now = datetime(2026, 5, 1, tzinfo=UTC)

    async def run() -> None:
        await wk.enqueue_fetch(job, "101")
        await wk.publish_reconcile_result(job, now)
        await wk.publish_reconcile_result(job, None)

    asyncio.run(run())
    fetch, with_wm, without = js.published
    assert fetch[0] == FETCH
    assert json.loads(fetch[1]) == {
        "job_id": "j1:101",
        "account_id": "acc-1",
        "user_id": "u-1",
        "provider": "acme",
        "ext_id": "101",
        "fetch_streams": True,
        "reason": "reconcile",
    }
    assert fetch[2] == {"Nats-Msg-Id": "reconcile:acme:acc-1:101"}
    assert with_wm[0] == f"{IDENT.result_subject('reconcile')}.acc-1"
    assert with_wm[2] == {"Nats-Msg-Id": "result:j1"}
    assert json_format.Parse(with_wm[1], w.JobResult()).new_watermark.ToDatetime(tzinfo=UTC) == now
    assert not json_format.Parse(without[1], w.JobResult()).HasField("new_watermark")


# --- presence, manifest, release ---


def test_heartbeat_publishes_presence() -> None:
    wk, js = worker()
    js.kv = FakeKV(wk.stop)
    asyncio.run(wk._heartbeat_loop())
    ((key, payload),) = js.kv.puts
    assert key == "acme-fetcher.i1"
    assert payload["capabilities"] == {"Activity": {"read": True}}
    assert payload["worker_key"] == "acme-fetcher-acme"
    assert payload["release"] == "v0.7.0"
    assert payload["webhooks"] is False
    assert payload["last_seen"]


def test_heartbeat_survives_a_failing_put() -> None:
    wk, js = worker()
    kv = FakeKV(wk.stop)

    async def broken(key: str, value: bytes) -> int:
        wk.stop()
        raise RuntimeError("no")

    kv.put = broken  # type: ignore[method-assign]
    js.kv = kv
    asyncio.run(wk._heartbeat_loop())


def test_heartbeat_stops_on_incompatible_core() -> None:
    wk, js = worker()
    js.kv = FakeKV()
    js.kv.store["info"] = cast(Any, type("E", (), {"value": b'{"release": "v0.8.0"}', "revision": 1})())
    asyncio.run(wk._heartbeat_loop())
    assert isinstance(wk._fatal, IncompatibleCoreError)
    assert wk._stop.is_set()


def test_core_release_check() -> None:
    wk, js = worker()
    js.kv = FakeKV()
    asyncio.run(wk._check_core_release())  # no info yet: allowed
    asyncio.run(js.kv.put("info", b'{"release": "v0.7.9"}'))
    asyncio.run(wk._check_core_release())
    asyncio.run(js.kv.put("info", b'{"release": "v1.0.0"}'))
    with pytest.raises(IncompatibleCoreError):
        asyncio.run(wk._check_core_release())


def test_manifest_with_and_without_provider_meta() -> None:
    meta = {"display_name": "Acme", "auth_method": "credentials"}
    wk, js = make_worker(Worker(IDENT, example_settings(), manifest=lambda: {"A": {}}, provider_meta=lambda: meta))
    js.kv = FakeKV(wk.stop)
    asyncio.run(wk._manifest_loop())
    ((key, payload),) = js.kv.puts
    assert key == "acme-fetcher"
    assert (payload["provider"], payload["package"], payload["version"]) == ("acme", "cairn.workers.acme", "1.0")
    assert payload["provider_meta"] == meta
    assert wk.manifest() == {"A": {}}

    plain, js2 = worker()
    js2.kv = FakeKV(plain.stop)
    asyncio.run(plain._manifest_loop())
    assert "provider_meta" not in js2.kv.puts[0][1]


def test_manifest_loop_logs_failures() -> None:
    wk, js = worker()

    async def broken(bucket: str) -> FakeKV:
        wk.stop()
        raise RuntimeError("kv gone")

    js.key_value = broken  # type: ignore[method-assign]
    asyncio.run(wk._manifest_loop())


# --- request/reply responders ---


def test_discover_wrapper_reports_errors() -> None:
    wk, _ = worker()

    async def discover(req: dict[str, Any]) -> dict[str, Any]:
        if req.get("account_id") == "bad":
            raise RuntimeError("no creds")
        return {"items": [], "complete": True}

    wk.on_discover(discover)
    ok, bad, junk = Responder(b'{"account_id": "a"}'), Responder(b'{"account_id": "bad"}'), Responder(b"[]")
    for r in (ok, bad, junk):
        asyncio.run(wk._on_discover(cast(Any, r)))
    assert ok.response == {"items": [], "complete": True}
    assert bad.response == {"error": "no creds"}
    assert junk.response == {"items": [], "complete": True}


def test_oauth_exchange_wrapper() -> None:
    wk, _ = worker()
    seen: list[OAuthExchangeRequest] = []

    async def exchange(req: OAuthExchangeRequest) -> OAuthExchangeReply:
        seen.append(req)
        if req.code == "boom":
            raise RuntimeError("provider down")
        if req.code == "bad":
            return OAuthExchangeReply.failure("exchange_failed", "invalid code")
        state = OAuthExchangeReply(access_token="at", refresh_token="rt", expires_at=datetime(2026, 1, 1, tzinfo=UTC))
        state.provider_account_id = "athlete-1"
        return state

    wk.on_oauth_exchange(exchange)
    good = Responder(
        json.dumps({"connection_id": "c1", "client_id": "id", "client_secret": "s", "code": "ok"}).encode()
    )
    bad = Responder(json.dumps({"code": "bad"}).encode())
    boom = Responder(json.dumps({"code": "boom"}).encode())
    junk = Responder(b"nope")
    for r in (good, bad, boom, junk):
        asyncio.run(wk._on_oauth_exchange(cast(Any, r)))
    assert seen[0].connection_id == "c1"
    assert good.response == {
        "access_token": "at",
        "refresh_token": "rt",
        "scope": "",
        "token_type": "",
        "provider_account_id": "athlete-1",
        "expires_at": "2026-01-01T00:00:00Z",
    }
    assert bad.response == {"error": "exchange_failed", "message": "invalid code"}
    assert boom.response == {"error": "transient", "message": "provider down"}
    assert junk.response["error"] == "bad_request"


def test_webhook_verify_and_event_wrappers() -> None:
    wk, _ = worker()
    events: list[WebhookEvent] = []

    async def verify(req: WebhookVerifyRequest) -> WebhookVerifyResponse:
        if req.query.get("hub.verify_token") != "secret":
            return WebhookVerifyResponse(status=403, content_type="text/plain", body=b"no")
        return WebhookVerifyResponse(body=json.dumps({"hub.challenge": req.query["hub.challenge"]}).encode())

    async def on_event(ev: WebhookEvent) -> None:
        events.append(ev)

    wk.on_webhook_verify(verify)
    wk.on_webhook_event(on_event)
    assert wk.webhooks is True
    ok = Responder(
        json.dumps({"method": "GET", "query": {"hub.verify_token": "secret", "hub.challenge": "c"}}).encode()
    )
    denied = Responder(json.dumps({"method": "GET", "query": {}}).encode())
    broken = Responder(b"{")
    for r in (ok, denied, broken):
        asyncio.run(wk._on_webhook_verify(cast(Any, r)))
    assert ok.response["status"] == 200
    assert json.loads(base64.b64decode(ok.response["body"])) == {"hub.challenge": "c"}
    assert denied.response["status"] == 403
    assert broken.response["status"] == 500

    job = Job(
        subject=IDENT.webhook_event_subject, body=b'{"x": 1}', delivered=2, headers={"x-webhook-source-addr": "1.2.3.4"}
    )
    asyncio.run(wk._dispatch_webhook(job))
    assert events[0].headers == {"x-webhook-source-addr": "1.2.3.4"}
    assert events[0].body == b'{"x": 1}'
    assert events[0].delivered == 2


def test_responders_without_handlers_fail_loudly() -> None:
    wk, _ = worker()
    with pytest.raises(RuntimeError, match="no handler"):
        asyncio.run(wk._dispatch_webhook(Job(subject="s", body=b"")))


# --- core RPC helpers ---


def test_fetch_creds_and_lookup() -> None:
    nc = FakeNC(
        {
            IDENT.creds_subject: {"client_id": "me", "client_secret": "pw"},
            IDENT.lookup_subject: {"account_id": "acc-1", "user_id": "u1"},
        }
    )
    wk, _ = worker(nc)
    creds = asyncio.run(wk.fetch_creds("acc-1"))
    assert (creds.client_id, creds.client_secret) == ("me", "pw")
    assert asyncio.run(wk.lookup_account("athlete-9", "sub-1")) == {"account_id": "acc-1", "user_id": "u1"}
    assert nc.requests[1] == (IDENT.lookup_subject, {"external_id": "athlete-9", "subscription_id": "sub-1"})


def test_blob_helpers_delegate_to_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    wk, _ = worker()
    calls: list[tuple[str, ...]] = []

    async def fake_archive(
        nc: object, provider: str, user_id: str, source_id: str, env: dict[str, Any]
    ) -> tuple[str, int]:
        calls.append(("archive", provider, user_id, source_id))
        return "raw/1", 3

    async def fake_fetch(nc: object, provider: str, url: str, fallback: str) -> dict[str, Any]:
        calls.append(("fetch", provider, url, fallback))
        return {"version": 1}

    monkeypatch.setattr(archive, "archive", fake_archive)
    monkeypatch.setattr(archive, "fetch", fake_fetch)
    env = wk.envelope(activity={"id": 1})
    assert env["version"] == 1
    assert env["provenance"]["package"] == "cairn.workers.acme"
    assert env["activity"] == {"id": 1}
    assert asyncio.run(wk.archive("u1", "1", env)) == ("raw/1", 3)
    assert asyncio.run(wk.fetch_blob("https://x", "raw/1")) == {"version": 1}
    assert calls == [("archive", "acme", "u1", "1"), ("fetch", "acme", "https://x", "raw/1")]


def test_rate_limiter_binds_the_shared_bucket() -> None:
    wk, js = worker()
    js.buckets["cairn_rate_limits"] = FakeKV()
    rl = asyncio.run(wk.rate_limiter({"acme:app:minute": 1}))
    assert asyncio.run(rl.reserve("acme:app:minute")) == (True, 0)
    ok, _ = asyncio.run(rl.reserve("acme:app:minute"))
    assert ok is False


# --- run() wiring ---


def test_run_wires_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    nc = FakeNC()
    js = FakeJS()
    js.kv = FakeKV()
    nc.js = js  # type: ignore[attr-defined]
    nc.jetstream = lambda: js  # type: ignore[attr-defined]

    async def connect(url: str, **opts: Any) -> FakeNC:
        nc.replies["connect"] = opts
        return nc

    monkeypatch.setattr(nats, "connect", connect)
    wk = Worker(IDENT, example_settings(enrollment_token="tok", nats_user="ignored"), manifest={})

    async def noop(job: Job) -> None:
        pass

    async def discover(req: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def exchange(req: OAuthExchangeRequest) -> OAuthExchangeReply:
        return OAuthExchangeReply()

    async def verify(req: WebhookVerifyRequest) -> WebhookVerifyResponse:
        return WebhookVerifyResponse()

    wk.handle("fetch_source", noop)
    wk.handle("reconcile", noop)
    wk.on_discover(discover)
    wk.on_oauth_exchange(exchange)
    wk.on_webhook_verify(verify)
    wk.on_webhook_event(cast(Any, noop))
    js.psub = FakePullSub([], wk.stop)
    asyncio.run(wk.run())

    assert nc.replies["connect"] == {"name": "acme-fetcher", "max_reconnect_attempts": -1, "token": "tok"}
    assert set(nc.subscriptions) == {
        IDENT.discover_subject,
        IDENT.oauth_exchange_subject,
        IDENT.webhook_verify_subject,
    }
    assert sorted(js.subscribed) == sorted(
        [
            (FETCH, {"durable": "acme-fetcher-fetch", "stream": "CAIRN_JOBS"}),
            (IDENT.job_subject("reconcile"), {"durable": "acme-fetcher-reconcile", "stream": "CAIRN_JOBS"}),
            (IDENT.webhook_event_subject, {"durable": "acme-fetcher-webhook", "stream": "CAIRN_WEBHOOKS"}),
        ]
    )
    assert js.kv.puts[0][0] == "acme-fetcher.i1"  # presence before the manifest


def test_run_connects_with_user_password_and_raises_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    nc = FakeNC()
    js = FakeJS()
    js.kv = FakeKV()
    js.kv.store["info"] = cast(Any, type("E", (), {"value": b'{"release": "v9.0.0"}', "revision": 1})())
    nc.jetstream = lambda: js  # type: ignore[attr-defined]
    opts: dict[str, Any] = {}

    async def connect(url: str, **kw: Any) -> FakeNC:
        opts.update(kw)
        return nc

    monkeypatch.setattr(nats, "connect", connect)
    wk = Worker(IDENT, example_settings(nats_user="u", nats_password="p"))
    with pytest.raises(IncompatibleCoreError):
        asyncio.run(wk.run())
    assert opts == {"name": "acme-fetcher", "max_reconnect_attempts": -1, "user": "u", "password": "p"}


def test_fake_kv_get_hides_deleted_keys() -> None:
    kv = FakeKV()
    asyncio.run(kv.put("k", b"{}"))
    asyncio.run(kv.delete("k"))
    with pytest.raises(KeyNotFoundError):
        asyncio.run(kv.get("k"))
