"""In-memory stand-ins for NATS, JetStream and KV, and a Worker wired to them
without a connection. Unit-test the handlers; the integration kit covers the
real broker.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeout
from nats.js import JetStreamContext
from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError
from nats.js.kv import KeyValue

from ..config import Identity, Settings
from ..worker import Worker

Published = tuple[str, bytes, dict[str, str]]


@dataclass
class FakeEntry:
    key: str
    value: bytes | None
    revision: int


class FakeKV:
    """A KV bucket with put/get/create/update/delete and JetStream's CAS rules.
    `puts` records every put as (key, decoded JSON); `on_put` runs after each.
    """

    def __init__(self, on_put: Callable[[], None] | None = None) -> None:
        self.puts: list[tuple[str, dict[str, Any]]] = []
        self.on_put = on_put
        self.store: dict[str, FakeEntry] = {}
        self._rev = 0

    def _set(self, key: str, value: bytes | None) -> int:
        self._rev += 1
        self.store[key] = FakeEntry(key, value, self._rev)
        return self._rev

    async def put(self, key: str, value: bytes) -> int:
        rev = self._set(key, value)
        self.puts.append((key, json.loads(value)))
        if self.on_put:
            self.on_put()
        return rev

    async def get(self, key: str) -> FakeEntry:
        entry = self.store.get(key)
        if entry is None or entry.value is None:
            raise KeyNotFoundError
        return entry

    async def create(self, key: str, value: bytes) -> int:
        entry = self.store.get(key)
        if entry is not None and entry.value is not None:
            raise KeyWrongLastSequenceError
        return self._set(key, value)

    async def update(self, key: str, value: bytes, last: int | None = None) -> int:
        entry = self.store.get(key)
        if last is not None and (entry is None or entry.revision != last):
            raise KeyWrongLastSequenceError
        return self._set(key, value)

    async def delete(self, key: str, last: int | None = None) -> bool:
        entry = self.store.get(key)
        if last is not None and (entry is None or entry.revision != last):
            raise KeyWrongLastSequenceError
        self._set(key, None)
        return True

    def as_kv(self) -> KeyValue:
        return cast(KeyValue, self)


class FakeMsg:
    """A JetStream message that records how it was settled."""

    def __init__(
        self, data: bytes, num_delivered: int = 1, subject: str = "", headers: dict[str, str] | None = None
    ) -> None:
        self.data = data
        self.subject = subject
        self.headers = headers
        self.metadata = SimpleNamespace(num_delivered=num_delivered)
        self.settled: list[str] = []

    async def ack(self) -> None:
        self.settled.append("ack")

    async def nak(self, delay: float | None = None) -> None:
        self.settled.append(f"nak:{delay}")

    async def term(self) -> None:
        self.settled.append("term")

    async def in_progress(self) -> None:
        self.settled.append("in_progress")


class FakePullSub:
    """Hands out queued batches, then calls `on_drained` and times out."""

    def __init__(self, batches: list[list[FakeMsg] | Exception], on_drained: Callable[[], None]) -> None:
        self.batches = batches
        self.on_drained = on_drained

    async def fetch(self, batch: int, **_: Any) -> list[FakeMsg]:
        if not self.batches:
            self.on_drained()
            raise NatsTimeout
        b = self.batches.pop(0)
        if isinstance(b, Exception):
            raise b
        return b


class FakeJS:
    """Records (subject, data, headers) for every publish; serves KV buckets and pull subscriptions."""

    def __init__(self) -> None:
        self.published: list[Published] = []
        self.kv: FakeKV | None = None
        self.buckets: dict[str, FakeKV] = {}
        self.psub: FakePullSub | None = None
        self.subscribed: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, subject: str, data: bytes, headers: dict[str, str] | None = None) -> None:
        self.published.append((subject, data, headers or {}))

    async def key_value(self, bucket: str) -> FakeKV:
        """The bucket registered under its name, else the default `kv`."""
        if bucket in self.buckets:
            return self.buckets[bucket]
        assert self.kv is not None, f"no fake KV for bucket {bucket}"
        return self.kv

    async def pull_subscribe(self, subject: str, **kw: Any) -> FakePullSub:
        self.subscribed.append((subject, kw))
        assert self.psub is not None
        return self.psub

    def bodies(self, subject: str) -> list[bytes]:
        return [d for s, d, _ in self.published if s == subject]


@dataclass
class Reply:
    data: bytes


class FakeNC:
    """Request/reply stub: answers each subject with a canned JSON object (or a
    list of them, consumed in order), or raises it. Records every request.
    """

    def __init__(self, replies: dict[str, Any] | None = None) -> None:
        self.replies: dict[str, Any] = replies or {}
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.subscriptions: dict[str, Any] = {}
        self.published: list[Published] = []

    async def request(self, subject: str, payload: bytes, **_: Any) -> Reply:
        self.requests.append((subject, json.loads(payload)))
        reply = self.replies.get(subject)
        if isinstance(reply, list):
            reply = reply.pop(0) if reply else None
        if reply is None:
            raise TimeoutError(subject)
        if isinstance(reply, Exception):
            raise reply
        return Reply(json.dumps(reply).encode())

    async def subscribe(self, subject: str, cb: Any = None, **_: Any) -> None:
        self.subscriptions[subject] = cb

    async def publish(self, subject: str, payload: bytes, headers: dict[str, str] | None = None) -> None:
        self.published.append((subject, payload, headers or {}))

    async def drain(self) -> None:
        pass


class Responder:
    """A core-NATS request message: `respond` captures the decoded reply."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.response: dict[str, Any] = {}

    async def respond(self, payload: bytes) -> None:
        self.response = json.loads(payload)


def as_nats(nc: FakeNC) -> NATS:
    return cast(NATS, nc)


def make_worker[W: Worker](
    worker: W,
    nc: FakeNC | None = None,
) -> tuple[W, FakeJS]:
    """Wire a constructed worker to a FakeJS and a FakeNC."""
    js = FakeJS()
    worker.js = cast(JetStreamContext, js)
    worker.nc = as_nats(nc or FakeNC())
    return worker, js


def example_identity(**over: Any) -> Identity:
    base: dict[str, Any] = {"provider": "acme", "package": "cairn.workers.acme", "version": "1.0", "dataset_version": 1}
    return Identity(**{**base, **over})


def example_settings(**over: Any) -> Settings:
    return Settings(**{"nats_url": "nats://127.0.0.1:1", "instance_id": "i1", "heartbeat_interval_s": 0, **over})
