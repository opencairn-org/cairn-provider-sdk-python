"""The NATS runtime a provider worker builds on: connect, release check,
presence heartbeat, manifest publish, discover and OAuth-exchange
responders, one durable pull consumer per registered job kind with ack
extension and bounded retries, claim-checked results and failure envelopes.

Job wire (core → worker) is JSON; results are protojson JobResult.

    worker = Worker(identity, Settings(), manifest=MANIFEST, provider_meta=META)
    worker.handle("fetch_source", fetch_source)
    worker.handle("reconcile", reconcile)
    worker.on_discover(discover)
    serve(worker)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cached_property
from typing import Any

import nats
from google.protobuf import json_format
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeout
from nats.js import JetStreamContext

from cairn.worker.v1 import worker_service_pb2 as w

from . import archive as archive_mod
from . import config as C
from . import creds as creds_mod
from .config import Identity, Settings
from .errors import IncompatibleCoreError, ParserTooOldError, RetryLaterError, TerminalJobError
from .oauth import OAuthExchangeHandler, OAuthExchangeReply, OAuthExchangeRequest
from .ratelimit import RateLimiter
from .tokens import RefreshFn, TokenSource
from .versions import release_compatible
from .webhooks import WebhookEvent, WebhookVerifyRequest, WebhookVerifyResponse

log = logging.getLogger("cairn.sdk")

Manifest = Mapping[str, Any] | Callable[[], Mapping[str, Any]]


@dataclass
class Job:
    """One delivered job: the raw body, parsed on demand."""

    subject: str
    body: bytes
    delivered: int = 1
    headers: dict[str, str] = field(default_factory=dict)

    @cached_property
    def data(self) -> dict[str, Any]:
        d = json.loads(self.body)
        if not isinstance(d, dict):
            raise ValueError("job body is not a JSON object")
        return d

    @classmethod
    def of(cls, data: Mapping[str, Any], subject: str = "", delivered: int = 1) -> Job:
        return cls(subject=subject, body=json.dumps(data).encode(), delivered=delivered)


Handler = Callable[[Job], Awaitable[None]]
DiscoverHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
WebhookEventHandler = Callable[[WebhookEvent], Awaitable[None]]
WebhookVerifyHandler = Callable[[WebhookVerifyRequest], Awaitable[WebhookVerifyResponse]]


class Worker:
    # A job that keeps failing (activity gone upstream, unmappable payload)
    # is terminated after this many deliveries instead of NAK-looping forever.
    MAX_DELIVERIES = 5
    # Re-parse jobs handed back as parser_too_old: give a newer instance about
    # 35 minutes to join the pool before telling core.
    PARSER_TOO_OLD_ATTEMPTS = 10
    # How long a failed job waits before redelivery: keeps a failing
    # activity from hot-looping against the provider.
    NAK_DELAY_S = 60
    # Ack-deadline extension cadence; must stay below the consumer AckWait
    # (default 30s). Covers slow handlers AND messages waiting in the batch.
    ACK_EXTEND_S = 10
    # The manifest KV has a 15 min TTL; core persists provider_meta on first sight.
    MANIFEST_INTERVAL_S = 600

    def __init__(
        self,
        identity: Identity,
        settings: Settings | None = None,
        *,
        manifest: Manifest | None = None,
        provider_meta: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
        refresh: RefreshFn | None = None,
    ) -> None:
        self.identity = identity
        self.cfg = settings or Settings()
        self._manifest: Manifest = manifest or {}
        self._provider_meta = provider_meta
        self.nc: NATS | None = None
        self.js: JetStreamContext | None = None
        self._stop = asyncio.Event()
        self._fatal: Exception | None = None  # set when core moves to an incompatible release
        self._handlers: dict[str, Handler] = {}
        self._discover: DiscoverHandler | None = None
        self._oauth_exchange: OAuthExchangeHandler | None = None
        self._webhook_event: WebhookEventHandler | None = None
        self._webhook_verify: WebhookVerifyHandler | None = None
        self.tokens = TokenSource(self, refresh)

    # --- registration (before run) ---
    def handle(self, job: str, handler: Handler) -> None:
        """Consume `cairn.jobs.<job>.<provider>.<worker_name>` with handler."""
        if job not in C.JOB_TYPES:
            raise ValueError(f"unknown job type {job!r}; one of {', '.join(C.JOB_TYPES)}")
        if job in self._handlers:
            raise ValueError(f"handler for {job} already registered")
        self._handlers[job] = handler

    def on_discover(self, fn: DiscoverHandler) -> None:
        self._discover = fn

    def on_oauth_exchange(self, fn: OAuthExchangeHandler) -> None:
        self._oauth_exchange = fn

    def on_webhook_event(self, fn: WebhookEventHandler) -> None:
        self._webhook_event = fn

    def on_webhook_verify(self, fn: WebhookVerifyHandler) -> None:
        self._webhook_verify = fn

    @property
    def jobs(self) -> tuple[str, ...]:
        return tuple(self._handlers)

    @property
    def webhooks(self) -> bool:
        return self._webhook_event is not None

    # --- connection ---
    @property
    def _nats(self) -> NATS:
        if self.nc is None:
            raise RuntimeError("not connected to NATS")
        return self.nc

    @property
    def _js(self) -> JetStreamContext:
        if self.js is None:
            raise RuntimeError("not connected to JetStream")
        return self.js

    @property
    def worker_key(self) -> str:
        return self.identity.worker_key

    async def connect(self) -> None:
        # The auth-callout matches the connection name against the enrollment name.
        opts: dict[str, Any] = {"name": self.identity.worker_name, "max_reconnect_attempts": -1}
        if self.cfg.enrollment_token:
            if self.cfg.nats_user:
                log.warning("CAIRN_WORKER_ENROLLMENT_TOKEN is set; ignoring CAIRN_NATS_USER/CAIRN_NATS_PASSWORD")
            opts["token"] = self.cfg.enrollment_token
        elif self.cfg.nats_user:
            opts["user"] = self.cfg.nats_user
            opts["password"] = self.cfg.nats_password
        nc = await nats.connect(self.cfg.nats_url, **opts)
        self.nc, self.js = nc, nc.jetstream()
        log.info("connected to %s", self.cfg.nats_url)

    async def run(self) -> None:
        await self.connect()
        await self._check_core_release()
        ident = self.identity
        if self._discover is not None:
            await self._nats.subscribe(ident.discover_subject, cb=self._on_discover)
        if self._oauth_exchange is not None:
            await self._nats.subscribe(ident.oauth_exchange_subject, cb=self._on_oauth_exchange)
        if self._webhook_verify is not None:
            await self._nats.subscribe(ident.webhook_verify_subject, cb=self._on_webhook_verify)
        tasks = [asyncio.create_task(self._heartbeat_loop()), asyncio.create_task(self._manifest_loop())]
        for job, handler in self._handlers.items():
            durable = f"{ident.worker_name}-{C.DURABLE_SUFFIX[job]}"
            tasks.append(asyncio.create_task(self._consume(ident.job_subject(job), durable, handler)))
        if self._webhook_event is not None:
            durable = f"{ident.worker_name}-webhook"
            tasks.append(
                asyncio.create_task(
                    self._consume(ident.webhook_event_subject, durable, self._dispatch_webhook, stream=C.WEBHOOK_STREAM)
                )
            )
        log.info(
            "serving %s; pulling %s",
            ident.discover_subject if self._discover else "no discover",
            ", ".join(ident.job_subject(j) for j in self._handlers) or "no jobs",
        )
        await self._stop.wait()
        for t in tasks:
            t.cancel()
        if self.nc:
            await self.nc.drain()
        if self._fatal is not None:
            raise self._fatal

    def stop(self) -> None:
        self._stop.set()

    async def _check_core_release(self) -> None:
        """Refuse to run against a core of another major.minor (core ADR 0005)."""
        try:
            kv = await self._js.key_value(C.CORE_INFO_KV)
            entry = await kv.get("info")
            core = json.loads(entry.value or b"{}").get("release", "")
        except Exception as e:  # noqa: BLE001 — older core or not yet published
            log.debug("core info unavailable: %s", e)
            return
        if not release_compatible(core, self.identity.release):
            raise IncompatibleCoreError(f"core {core}, worker {self.identity.release}: major.minor must match")

    # --- presence + manifest ---
    def manifest(self) -> dict[str, Any]:
        m = self._manifest() if callable(self._manifest) else self._manifest
        return dict(m)

    def provider_meta(self) -> dict[str, Any] | None:
        if self._provider_meta is None:
            return None
        m = self._provider_meta() if callable(self._provider_meta) else self._provider_meta
        return dict(m)

    def presence_payload(self) -> dict[str, Any]:
        ident = self.identity
        return {
            "worker_name": ident.worker_name,
            "worker_key": ident.worker_key,
            "instance_id": self.cfg.instance_id,
            "version": ident.version,
            "release": ident.release,
            "provider": ident.provider,
            "package": ident.package,
            "webhooks": self.webhooks,
            "capabilities": self.manifest(),
            "last_seen": datetime.now(UTC).isoformat(),
        }

    async def _heartbeat_loop(self) -> None:
        kv = await self._js.key_value(C.PRESENCE_KV)  # created by core's stream bootstrap
        key = f"{self.identity.worker_name}.{self.cfg.instance_id}"
        while not self._stop.is_set():
            try:
                await kv.put(key, json.dumps(self.presence_payload()).encode())
            except Exception as e:  # noqa: BLE001
                log.debug("heartbeat failed: %s", e)
            # Core upgraded underneath us: stop instead of mis-parsing.
            try:
                await self._check_core_release()
            except IncompatibleCoreError as e:
                self._fatal = e
                self._stop.set()
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.heartbeat_interval_s)

    def manifest_payload(self) -> dict[str, Any]:
        ident = self.identity
        payload: dict[str, Any] = {
            "worker_name": ident.worker_name,
            "version": ident.version,
            "provider": ident.provider,
            "package": ident.package,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        meta = self.provider_meta()
        if meta is not None:
            payload["provider_meta"] = meta
        return payload

    async def publish_manifest(self) -> None:
        kv = await self._js.key_value(C.MANIFEST_KV)
        await kv.put(self.identity.worker_name, json.dumps(self.manifest_payload()).encode())

    async def _manifest_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.publish_manifest()
            except Exception as e:  # noqa: BLE001
                log.warning("manifest publish failed: %s", e)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.MANIFEST_INTERVAL_S)

    # --- request/reply responders ---
    async def _on_discover(self, msg: Msg) -> None:
        try:
            req = json.loads(msg.data)
            resp = await _required(self._discover)(req if isinstance(req, dict) else {})
        except Exception as e:  # noqa: BLE001
            log.error("discover error: %s", e)
            resp = {"error": str(e)}
        await msg.respond(json.dumps(resp).encode())

    async def _on_oauth_exchange(self, msg: Msg) -> None:
        try:
            req = OAuthExchangeRequest.from_json(json.loads(msg.data))
        except Exception:  # noqa: BLE001
            reply = OAuthExchangeReply.failure("bad_request", "undecodable request")
        else:
            try:
                reply = await _required(self._oauth_exchange)(req)
            except Exception as e:  # noqa: BLE001
                reply = OAuthExchangeReply.failure("transient", str(e))
            if reply.error:
                log.warning("oauth exchange refused for %s: %s: %s", req.connection_id, reply.error, reply.message)
        await msg.respond(json.dumps(reply.to_json()).encode())

    async def _on_webhook_verify(self, msg: Msg) -> None:
        try:
            req = WebhookVerifyRequest.from_json(json.loads(msg.data))
            resp = await _required(self._webhook_verify)(req)
        except Exception as e:  # noqa: BLE001
            log.error("webhook verify error: %s", e)
            resp = WebhookVerifyResponse(status=500, content_type="text/plain", body=str(e).encode())
        await msg.respond(json.dumps(resp.to_json()).encode())

    async def _dispatch_webhook(self, job: Job) -> None:
        await _required(self._webhook_event)(WebhookEvent(job.subject, job.headers, job.body, job.delivered))

    # --- pull consumption ---
    async def _extend_ack(self, m: Msg) -> None:
        # Ends on cancel, or once the message is acked/naked or the delivery is gone.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            while True:
                await asyncio.sleep(self.ACK_EXTEND_S)
                await m.in_progress()

    async def _consume(self, subject: str, durable: str, handler: Handler, *, stream: str = C.JOB_STREAM) -> None:
        psub = await self._js.pull_subscribe(subject, durable=durable, stream=stream)
        while not self._stop.is_set():
            try:
                msgs = await psub.fetch(self.cfg.pull_batch, timeout=5)
            except NatsTimeout:
                continue
            except Exception as e:  # noqa: BLE001
                log.warning("fetch error on %s: %s", subject, e)
                await asyncio.sleep(1)
                continue
            # Extend every fetched message's ack deadline from arrival: the
            # batch is processed sequentially, and without this a slow handler
            # pushes the waiting messages past AckWait into redelivery loops.
            heartbeats = [asyncio.create_task(self._extend_ack(m)) for m in msgs]
            for m, hb in zip(msgs, heartbeats, strict=True):
                try:
                    await self._settle(subject, m, handler)
                finally:
                    hb.cancel()

    async def _settle(self, subject: str, m: Msg, handler: Handler) -> None:
        deliveries = m.metadata.num_delivered or 1
        job = Job(subject=m.subject or subject, body=m.data, delivered=deliveries, headers=dict(m.headers or {}))
        try:
            await handler(job)
            await m.ack()
        except TerminalJobError as e:
            log.error("handler error on %s (terminal): %s", subject, e)
            await self.publish_failure(subject, m.data, e)
            await m.term()
        except ParserTooOldError as e:
            # Rolling upgrade: a newer instance of the pool takes it. Does
            # not count as a failure until the back-off window is over.
            if deliveries >= self.PARSER_TOO_OLD_ATTEMPTS:
                log.error("handler error on %s (parser too old, giving up): %s", subject, e)
                await self.publish_failure(subject, m.data, e)
                await m.term()
            else:
                await m.nak(delay=min(30 * 2 ** (deliveries - 1), 600))
        except RetryLaterError as e:
            log.info("handler on %s asks for retry in %.0fs: %s", subject, e.delay_s, e.reason)
            await m.nak(delay=e.delay_s)
        except Exception as e:  # noqa: BLE001
            if deliveries >= self.MAX_DELIVERIES:
                log.error("handler error on %s (delivery %d, terminating): %s", subject, deliveries, e)
                await self.publish_failure(subject, m.data, e)
                await m.term()
            else:
                log.error("handler error on %s (delivery %d/%d): %s", subject, deliveries, self.MAX_DELIVERIES, e)
                await m.nak(delay=self.NAK_DELAY_S)

    # --- results ---
    def job_result(self, events: Iterable[w.WorkerEvent] = ()) -> w.JobResult:
        """A JobResult stamped with this worker's identity."""
        ident = self.identity
        return w.JobResult(
            events=list(events),
            worker_name=ident.worker_name,
            worker_version=ident.version,
            worker_package=ident.package,
            worker_release=ident.release,
        )

    async def publish_result(self, subject: str, jr: w.JobResult, *, headers: dict[str, str] | None = None) -> None:
        """Publish a JobResult. Event-carrying results are claim-checked:
        the full protojson body goes to the blob store and only a small
        envelope with payload_ref travels over NATS, so stream-heavy
        activities can't exceed the broker's max_payload. Event-free
        results (nothing to spill) publish inline.
        """
        body = json_format.MessageToJson(jr).encode()
        if not jr.events:
            await self._js.publish(subject, body, headers=headers)
            return
        ref = await archive_mod.upload_result(self._nats, self.identity.provider, body)
        env = w.JobResult(
            worker_name=jr.worker_name,
            worker_version=jr.worker_version,
            worker_package=jr.worker_package,
            worker_release=jr.worker_release,
            more_available=jr.more_available,
        )
        if jr.HasField("new_watermark"):
            env.new_watermark.CopyFrom(jr.new_watermark)
        env.payload_ref.blob_id = ref["blob_id"]
        env.payload_ref.size_bytes = ref["size"]
        env.payload_ref.content_type = "application/json"
        env.payload_ref.content_sha256 = ref["sha256"]
        await self._js.publish(subject, json_format.MessageToJson(env).encode(), headers=headers)

    async def publish_failure(self, job_subject: str, body: bytes, err: Exception) -> None:
        """Best-effort failure envelope published alongside a Term, so core
        fails the import-queue item with the true reason instead of the
        stale reaper's masked "no result after N dispatch attempts".
        """
        try:
            job = json.loads(body)
        except Exception:  # noqa: BLE001
            return
        ext_id = str(job.get("ext_id") or "") if isinstance(job, dict) else ""
        if not ext_id:
            return
        jr = self.job_result()
        setattr(jr.error, "class", w.ERROR_CLASS_WORKER_BUG)
        jr.error.code = getattr(err, "code", type(err).__name__)
        jr.error.message = str(err)[:500]
        jr.failed_ref.user_id = str(job.get("user_id") or "")
        jr.failed_ref.provider = self.identity.provider
        jr.failed_ref.external_account_id = str(job.get("account_id") or "")
        jr.failed_ref.external_id = ext_id
        try:
            await self._js.publish(C.result_subject_for(job_subject), json_format.MessageToJson(jr).encode())
        except Exception as e:  # noqa: BLE001
            log.warning("failure result publish failed for %s: %s", ext_id, e)

    async def publish_reconcile_result(self, job: Mapping[str, Any], high_water: datetime | None) -> None:
        """The reconcile result MUST be published even when nothing new was
        found: advancing last_sync_at is what stops the scheduler from
        re-polling the account every tick. Account-suffixed for attribution.
        """
        jr = self.job_result()
        if high_water is not None:
            jr.new_watermark.FromDatetime(high_water)
        subject = f"{self.identity.result_subject('reconcile')}.{job.get('account_id', '')}"
        headers = {"Nats-Msg-Id": f"result:{job.get('job_id', '')}"}
        await self._js.publish(subject, json_format.MessageToJson(jr).encode(), headers=headers)

    async def enqueue_fetch(
        self, job: Mapping[str, Any], ext_id: str, *, fetch_streams: bool = True, reason: str = "reconcile"
    ) -> None:
        """A fetch_source sub-job to this pool, deduplicated by a deterministic
        msg-id within JetStream's window; ingest dedups idempotently beyond it.
        """
        account_id = job.get("account_id", "")
        body = {
            "job_id": f"{job.get('job_id', '')}:{ext_id}",
            "account_id": account_id,
            "user_id": job.get("user_id", ""),
            "provider": self.identity.provider,
            "ext_id": ext_id,
            "fetch_streams": fetch_streams,
            "reason": reason,
        }
        msg_id = f"{reason}:{self.identity.provider}:{account_id}:{ext_id}"
        await self._js.publish(
            self.identity.job_subject("fetch_source"), json.dumps(body).encode(), headers={"Nats-Msg-Id": msg_id}
        )

    # --- core RPCs and blobs ---
    async def fetch_creds(self, account_id: str) -> creds_mod.Credentials:
        return await creds_mod.fetch_creds(self._nats, self.identity.provider, account_id)

    async def lookup_account(self, external_id: str, subscription_id: str = "") -> dict[str, Any]:
        """Resolve a provider owner id to {account_id, user_id} or {error}."""
        req: dict[str, Any] = {"external_id": external_id}
        if subscription_id:
            req["subscription_id"] = subscription_id
        resp = await self._nats.request(self.identity.lookup_subject, json.dumps(req).encode(), timeout=5)
        out = json.loads(resp.data)
        return out if isinstance(out, dict) else {"error": "malformed reply"}

    def envelope(self, **inputs: Any) -> dict[str, Any]:
        return archive_mod.envelope(self.identity, **inputs)

    async def archive(self, user_id: str, source_id: str, env: dict[str, Any]) -> tuple[str | None, int]:
        return await archive_mod.archive(self._nats, self.identity.provider, user_id, source_id, env)

    async def fetch_blob(self, url: str, fallback_handle: str) -> dict[str, Any]:
        return await archive_mod.fetch(self._nats, self.identity.provider, url, fallback_handle)

    async def rate_limiter(self, capacities: Mapping[str, int], **kw: Any) -> RateLimiter:
        """A limiter over core's cairn_rate_limits bucket."""
        return RateLimiter(await self._js.key_value(C.RATE_LIMITS_KV), capacities, **kw)


def _required[T](fn: T | None) -> T:
    if fn is None:
        raise RuntimeError("no handler registered")
    return fn


def serve(worker: Worker) -> None:
    """Run the worker until SIGINT/SIGTERM."""

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()

    asyncio.run(_main())
