"""Integration kit: a real `nats-server -js`, core's streams and buckets, a
fake core answering the request/reply subjects a worker uses, an in-memory
blob store behind the presigned URLs, and helpers to publish a job and
await its result.

    with NatsServer() as srv:
        async def scenario():
            core = await FakeCore.start(srv.url, provider="acme", release="v0.7.0")
            worker = ...; run = asyncio.create_task(worker.run())
            await core.wait_for_consumers(worker.identity, ["fetch_source"])
            await core.publish_job(worker.identity, "fetch_source", {...})
            result = await core.await_result("fetch_source")
            worker.stop(); await run; await core.close()
        asyncio.run(scenario())
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import nats
from google.protobuf import json_format
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import DiscardPolicy, KeyValueConfig, ObjectStoreConfig, RetentionPolicy, StorageType, StreamConfig

from cairn.worker.v1 import worker_service_pb2 as w

from .. import config as C
from ..config import Identity
from ..rfc3339 import format_time, parse_time

# --- nats-server ---


def nats_server_binary() -> str | None:
    """nats-server from PATH or $GOPATH/bin (go install github.com/nats-io/nats-server/v2)."""
    found = shutil.which("nats-server")
    if found:
        return found
    gobin = Path(os.environ.get("GOPATH", Path.home() / "go")) / "bin" / "nats-server"
    return str(gobin) if gobin.exists() else None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class NatsServer:
    """`nats-server -js` on a free port with its own store dir; a context manager."""

    def __init__(self, *, config: dict[str, Any] | None = None, binary: str | None = None) -> None:
        self.binary = binary or nats_server_binary()
        self.config = config or {}
        self.port = free_port()
        self.url = f"nats://127.0.0.1:{self.port}"
        self._tmp: tempfile.TemporaryDirectory[str] | None = None
        self._proc: subprocess.Popen[bytes] | None = None

    def start(self) -> NatsServer:
        if not self.binary:
            raise RuntimeError("nats-server not installed; go install github.com/nats-io/nats-server/v2@latest")
        self._tmp = tempfile.TemporaryDirectory()
        conf = {
            "host": "127.0.0.1",
            "port": self.port,
            "jetstream": {"store_dir": f"{self._tmp.name}/js"},
            **self.config,
        }
        path = Path(self._tmp.name) / "nats.conf"
        path.write_text(json.dumps(conf))
        self._proc = subprocess.Popen(
            [self.binary, "-c", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        deadline = time.monotonic() + 10
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                if time.monotonic() > deadline or self._proc.poll() is not None:
                    self.stop()
                    raise RuntimeError("nats-server did not start") from None
                time.sleep(0.05)

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait(timeout=10)
            self._proc = None
        if self._tmp is not None:
            self._tmp.cleanup()
            self._tmp = None

    def __enter__(self) -> NatsServer:
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()


# --- core's bootstrap (internal/adapter/secondary/nats/bootstrap_streams.go) ---


async def bootstrap_core(js: JetStreamContext, *, release: str = "") -> None:
    """Create the streams, KV and object-store buckets exactly as core does,
    and publish its release under `info` in cairn_core (ADR 0005).
    """
    file = StorageType.FILE
    streams = [
        StreamConfig(
            name=C.JOB_STREAM,
            subjects=["cairn.jobs.>"],
            retention=RetentionPolicy.WORK_QUEUE,
            storage=file,
            discard=DiscardPolicy.NEW,
            duplicate_window=300,
        ),
        StreamConfig(
            name="CAIRN_RESULTS",
            subjects=["cairn.results.>"],
            retention=RetentionPolicy.INTEREST,
            storage=file,
            discard=DiscardPolicy.OLD,
            duplicate_window=60,
        ),
        StreamConfig(
            name="CAIRN_EVENTS",
            subjects=["cairn.events.>"],
            retention=RetentionPolicy.LIMITS,
            storage=file,
            discard=DiscardPolicy.OLD,
            max_age=7 * 86400,
            duplicate_window=60,
        ),
        StreamConfig(
            name="CAIRN_DLQ",
            subjects=["cairn.dlq.>"],
            retention=RetentionPolicy.LIMITS,
            storage=file,
            max_age=30 * 86400,
        ),
        StreamConfig(
            name=C.WEBHOOK_STREAM,
            subjects=["cairn.webhooks.>"],
            retention=RetentionPolicy.WORK_QUEUE,
            storage=file,
            discard=DiscardPolicy.OLD,
            max_age=86400,
            duplicate_window=300,
        ),
    ]
    for sc in streams:
        await js.add_stream(sc)
    buckets = [
        KeyValueConfig(bucket=C.PRESENCE_KV, ttl=60, history=1),
        KeyValueConfig(bucket=C.MANIFEST_KV, ttl=900, history=5),
        KeyValueConfig(bucket=C.RATE_LIMITS_KV, ttl=900, history=1),
        KeyValueConfig(bucket=C.CORE_INFO_KV, history=1),
    ]
    for bc in buckets:
        await js.create_key_value(config=bc)
    await js.create_object_store("cairn_transient", config=ObjectStoreConfig(max_bytes=1 << 30, ttl=3600))
    info = await js.key_value(C.CORE_INFO_KV)
    await info.put("info", json.dumps({"release": release}).encode())


# --- blob store behind the presigned URLs ---


class _BlobHandler(BaseHTTPRequestHandler):
    store: BlobStore

    def do_PUT(self) -> None:
        self.store.blobs[self.path.lstrip("/")] = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(200)
        self.end_headers()

    def do_GET(self) -> None:
        body = self.store.blobs.get(self.path.lstrip("/"))
        self.send_response(200 if body is not None else 404)
        self.end_headers()
        self.wfile.write(body or b"")

    def do_DELETE(self) -> None:
        self.store.blobs.pop(self.path.lstrip("/"), None)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        pass


class BlobStore:
    """Stands in for S3: an HTTP server keeping PUT bodies in memory by key."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        handler = type("Handler", (_BlobHandler,), {"store": self})
        self._srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.url = f"http://127.0.0.1:{self._srv.server_address[1]}"
        threading.Thread(target=self._srv.serve_forever, daemon=True).start()

    def url_for(self, key: str) -> str:
        return f"{self.url}/{key}"

    def close(self) -> None:
        self._srv.shutdown()
        self._srv.server_close()


# --- fake core ---


@dataclass
class Result:
    subject: str
    envelope: w.JobResult
    job_result: w.JobResult  # the full body: payload_ref resolved from the blob store

    @property
    def job(self) -> str:
        return self.subject.split(".")[2]


@dataclass
class FakeAccount:
    """What core knows about one external account the worker may ask for."""

    account_id: str
    user_id: str = "u1"
    provider_account_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    token: dict[str, Any] = field(default_factory=dict)
    app_id: str = ""
    # Set to "needs_reauth", "account_gone" or "transient" to answer an error.
    error: str = ""


async def until(cond: Callable[[], Awaitable[bool]], what: str, timeout_s: float = 15) -> None:
    try:
        async with asyncio.timeout(timeout_s):
            while not await cond():  # noqa: ASYNC110 — polling a remote condition
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise AssertionError(f"timed out waiting for {what}") from None


class FakeCore:
    """Core's side of the worker contract on an admin connection: responders
    for creds, tokens, presign, account lookup; a results collector; and
    the requests core makes (discover, OAuth exchange, webhook verify).
    """

    def __init__(self, nc: NATS, provider: str, blobs: BlobStore) -> None:
        self.nc, self.js = nc, nc.jetstream()
        self.provider = provider
        self.blobs = blobs
        self.accounts: dict[str, FakeAccount] = {}
        self.results: list[Result] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.token_stores: list[dict[str, Any]] = []
        self.reauth: list[dict[str, Any]] = []
        self._n = 0

    @classmethod
    async def start(
        cls,
        url: str,
        *,
        provider: str,
        release: str = "",
        bootstrap: bool = True,
        blobs: BlobStore | None = None,
        **connect: Any,
    ) -> FakeCore:
        nc = await nats.connect(url, **connect)
        core = cls(nc, provider, blobs or BlobStore())
        if bootstrap:
            await bootstrap_core(core.js, release=release)
        await core._serve()
        return core

    async def close(self) -> None:
        await self.nc.close()
        self.blobs.close()

    def account(self, account_id: str, **fields: Any) -> FakeAccount:
        acc = FakeAccount(account_id, **fields)
        self.accounts[account_id] = acc
        return acc

    async def _serve(self) -> None:
        p = self.provider
        await self.nc.subscribe("cairn.results.>", cb=self._on_result)
        await self.nc.subscribe(C.presign_upload_subject(p), cb=self._presign_upload)
        await self.nc.subscribe(C.presign_download_subject(p), cb=self._presign_download)
        await self.nc.subscribe(C.creds_subject(p), cb=self._creds)
        await self.nc.subscribe(C.tokens_subject(p, "get"), cb=self._token_get)
        await self.nc.subscribe(C.tokens_subject(p, "store"), cb=self._token_store)
        await self.nc.subscribe(C.tokens_subject(p, "needs_reauth"), cb=self._needs_reauth)
        await self.nc.subscribe(f"cairn.accounts.lookup.{p}.*", cb=self._lookup)
        await self.nc.flush()

    async def _reply(self, m: Msg, body: dict[str, Any]) -> None:
        await m.respond(json.dumps(body).encode())

    def _req(self, m: Msg) -> dict[str, Any]:
        req = json.loads(m.data)
        out = req if isinstance(req, dict) else {}
        self.requests.append((m.subject, out))
        return out

    async def _on_result(self, m: Msg) -> None:
        env = json_format.Parse(m.data, w.JobResult())
        full = env
        if env.HasField("payload_ref"):
            body = self.blobs.blobs.get(env.payload_ref.blob_id)
            full = json_format.Parse(body, w.JobResult()) if body is not None else env
        self.results.append(Result(m.subject, env, full))

    async def _presign_upload(self, m: Msg) -> None:
        req = self._req(m)
        self._n += 1
        if req.get("kind") == "result":
            key = f"transfer/{self.provider}/{self._n}.json"
        else:
            # Archives are named by source, so a test can address one whatever the upload order.
            key = f"users/{req.get('user_id') or 'u'}/{self.provider}/{req.get('source_id') or self._n}.json"
        await self._reply(m, {"url": self.blobs.url_for(key), "method": "PUT", "blob_id": key, "required_headers": {}})

    async def _presign_download(self, m: Msg) -> None:
        req = self._req(m)
        key = str(req.get("blob_id") or "")
        if key not in self.blobs.blobs:
            await self._reply(m, {"error": "not_found"})
            return
        await self._reply(m, {"url": self.blobs.url_for(key)})

    def _account_or_error(self, req: dict[str, Any]) -> tuple[FakeAccount | None, dict[str, Any]]:
        acc = self.accounts.get(str(req.get("account_id") or ""))
        if acc is None:
            return None, {"error": "account_gone"}
        if acc.error:
            return None, {"error": acc.error}
        return acc, {}

    async def _creds(self, m: Msg) -> None:
        acc, err = self._account_or_error(self._req(m))
        if acc is None:
            await self._reply(m, err)
            return
        await self._reply(m, {"client_id": acc.client_id, "client_secret": acc.client_secret})

    async def _token_get(self, m: Msg) -> None:
        acc, err = self._account_or_error(self._req(m))
        if acc is None:
            await self._reply(m, err)
            return
        reply = {**acc.token, "client_id": acc.client_id, "client_secret": acc.client_secret}
        if acc.app_id:
            reply["app_id"] = acc.app_id
        await self._reply(m, reply)

    async def _token_store(self, m: Msg) -> None:
        req = self._req(m)
        self.token_stores.append(req)
        acc, err = self._account_or_error(req)
        if acc is None:
            await self._reply(m, err)
            return
        if not req.get("access_token") or not req.get("expires_at"):
            await self._reply(m, {"error": "bad_request"})
            return
        # Optimistic on the previous expiry, like core.
        current = parse_time(acc.token.get("expires_at"))
        if current is not None and parse_time(req.get("previous_expires_at")) != current:
            await self._reply(m, {"error": "stale_store"})
            return
        acc.token = {
            k: req[k] for k in ("access_token", "refresh_token", "expires_at", "scope", "token_type") if k in req
        }
        await self._reply(m, {"ok": True})

    async def _needs_reauth(self, m: Msg) -> None:
        req = self._req(m)
        self.reauth.append(req)
        acc = self.accounts.get(str(req.get("account_id") or ""))
        if acc is not None:
            acc.error = "needs_reauth"
        await self._reply(m, {"ok": True})

    async def _lookup(self, m: Msg) -> None:
        req = self._req(m)
        ext = str(req.get("external_id") or "")
        for acc in self.accounts.values():
            if acc.provider_account_id == ext:
                await self._reply(m, {"account_id": acc.account_id, "user_id": acc.user_id})
                return
        await self._reply(m, {"error": "not_found"})

    # --- what core does to a worker ---
    async def publish_job(self, identity: Identity, job: str, body: dict[str, Any], *, msg_id: str = "") -> str:
        subject = identity.job_subject(job)
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        await self.js.publish(subject, json.dumps(body).encode(), headers=headers)
        return subject

    async def await_result(
        self,
        job: str | None = None,
        *,
        subject: str | None = None,
        where: Callable[[Result], bool] | None = None,
        skip: int = 0,
        timeout_s: float = 15,
    ) -> Result:
        """The (skip+1)-th result matching job kind / exact subject / predicate."""

        def matches(r: Result) -> bool:
            return (
                (job is None or r.job == job)
                and (subject is None or r.subject == subject)
                and (where is None or where(r))
            )

        async def ready() -> bool:
            return len([r for r in self.results if matches(r)]) > skip

        await until(ready, f"result for {job or subject}", timeout_s)
        return [r for r in self.results if matches(r)][skip]

    async def discover(
        self, identity: Identity, account_id: str, start_page: int = 1, timeout_s: float = 5
    ) -> dict[str, Any]:
        req = json.dumps({"account_id": account_id, "start_page": start_page}).encode()
        reply = await self.nc.request(identity.discover_subject, req, timeout=timeout_s)
        out = json.loads(reply.data)
        assert isinstance(out, dict)
        return out

    async def exchange_code(self, identity: Identity, timeout_s: float = 20, **req: str) -> dict[str, Any]:
        reply = await self.nc.request(identity.oauth_exchange_subject, json.dumps(req).encode(), timeout=timeout_s)
        out = json.loads(reply.data)
        assert isinstance(out, dict)
        return out

    async def verify_webhook(self, identity: Identity, query: dict[str, str], method: str = "GET") -> dict[str, Any]:
        """Like GET /webhooks/<worker_key>. The verify subject is also captured
        by the CAIRN_WEBHOOKS stream, whose pub-ack arrives as a first reply:
        it is skipped here, and the captured request is purged afterwards.
        """
        subject = identity.webhook_verify_subject
        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox)
        await self.nc.publish(subject, json.dumps({"method": method, "query": query}).encode(), reply=inbox)
        try:
            async with asyncio.timeout(5):
                while True:
                    out = json.loads((await sub.next_msg(timeout=5)).data)
                    if isinstance(out, dict) and "stream" not in out:
                        return out
        finally:
            await sub.unsubscribe()
            await self.js.purge_stream(C.WEBHOOK_STREAM, subject=subject)

    async def forward_webhook(self, identity: Identity, body: bytes, headers: dict[str, str] | None = None) -> None:
        """Like POST /webhooks/<worker_key>: the raw body onto the webhook stream."""
        await self.js.publish(
            identity.webhook_event_subject, body, headers={**(headers or {}), "x-webhook-source-addr": "127.0.0.1"}
        )

    async def presence(self, identity: Identity, instance_id: str) -> dict[str, Any] | None:
        kv = await self.js.key_value(C.PRESENCE_KV)
        try:
            entry = await kv.get(f"{identity.worker_name}.{instance_id}")
        except Exception:  # noqa: BLE001
            return None
        out = json.loads(entry.value or b"{}")
        return out if isinstance(out, dict) else None

    async def manifest(self, identity: Identity) -> dict[str, Any] | None:
        kv = await self.js.key_value(C.MANIFEST_KV)
        try:
            entry = await kv.get(identity.worker_name)
        except Exception:  # noqa: BLE001
            return None
        out = json.loads(entry.value or b"{}")
        return out if isinstance(out, dict) else None

    async def wait_for_presence(self, identity: Identity, instance_id: str, timeout_s: float = 15) -> dict[str, Any]:
        found: dict[str, Any] = {}

        async def ready() -> bool:
            hb = await self.presence(identity, instance_id)
            if hb:
                found.update(hb)
            return bool(hb)

        await until(ready, "presence heartbeat", timeout_s)
        return found

    async def wait_for_consumers(self, identity: Identity, jobs: list[str], timeout_s: float = 15) -> None:
        """Until a durable consumer filters every listed job subject (contract §3)."""
        wanted = {identity.job_subject(j) for j in jobs}

        async def ready() -> bool:
            infos = await self.js.consumers_info(C.JOB_STREAM)
            filters = {ci.config.filter_subject for ci in infos}
            return wanted <= filters

        await until(ready, f"durable consumers for {sorted(wanted)}", timeout_s)

    async def wait_drained(self, stream: str = C.JOB_STREAM, timeout_s: float = 15) -> None:
        async def drained() -> bool:
            return (await self.js.stream_info(stream)).state.messages == 0

        await until(drained, f"{stream} drained", timeout_s)


def token_fields(access: str, *, expires_in_s: float = 3600, refresh: str = "", scope: str = "") -> dict[str, Any]:
    """A token reply as core sends it; negative expires_in_s makes it expired."""
    return {
        "access_token": access,
        "refresh_token": refresh,
        "expires_at": format_time(datetime.now(UTC) + timedelta(seconds=expires_in_s)),
        "scope": scope,
        "token_type": "Bearer",
    }
