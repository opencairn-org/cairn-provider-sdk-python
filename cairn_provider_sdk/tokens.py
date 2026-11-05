"""OAuth token state from core (cairn.tokens.<provider>.get/store/needs_reauth)
with an in-process cache, worker-driven refresh ahead of expiry, and a
cross-instance refresh lock so pooled instances never refresh the same
account at once (single-use and rotating refresh tokens would otherwise
invalidate each other).

The lock is a key `lock.<provider>.<account_id>` in the cairn_rate_limits KV
bucket (the only worker-writable bucket with a TTL): created with
compare-and-set, carrying its owner and expiry, deleted after the refresh.
A crashed holder's lock is taken over once its expiry passed, and the
bucket's 15 min TTL reaps whatever is left.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from nats.js.errors import KeyWrongLastSequenceError, NotFoundError
from nats.js.kv import KeyValue

from . import config as C
from .errors import TokenError
from .rfc3339 import format_time, parse_time

if TYPE_CHECKING:
    from .worker import Worker

log = logging.getLogger("cairn.sdk")


@dataclass
class TokenState:
    access_token: str = ""
    refresh_token: str = ""
    expires_at: datetime | None = None
    scope: str = ""
    token_type: str = ""
    # Hash of the connection's client id; keys the rate-limit buckets.
    app_id: str = ""

    def fresh(self, skew_s: float = 60) -> bool:
        if not self.access_token:
            return False
        return self.expires_at is None or self.expires_at - datetime.now(UTC) > timedelta(seconds=skew_s)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TokenState:
        return cls(
            access_token=str(d.get("access_token") or ""),
            refresh_token=str(d.get("refresh_token") or ""),
            expires_at=parse_time(d.get("expires_at")),
            scope=str(d.get("scope") or ""),
            token_type=str(d.get("token_type") or ""),
            app_id=str(d.get("app_id") or ""),
        )

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"access_token": self.access_token}
        if self.refresh_token:
            out["refresh_token"] = self.refresh_token
        if self.expires_at is not None:
            out["expires_at"] = format_time(self.expires_at)
        if self.scope:
            out["scope"] = self.scope
        if self.token_type:
            out["token_type"] = self.token_type
        return out


@dataclass(frozen=True)
class ClientCreds:
    """The connection's OAuth app identity; per account, delivered with the token."""

    client_id: str = ""
    client_secret: str = ""


RefreshFn = Callable[[TokenState, ClientCreds], Awaitable[TokenState]]


class KVLock:
    """A lease in a KV bucket: create-if-absent, take over when expired, delete to release."""

    def __init__(self, kv: KeyValue, key: str, *, owner: str, ttl_s: float = 30, poll_s: float = 0.2) -> None:
        self.kv, self.key, self.owner, self.ttl_s, self.poll_s = kv, key, owner, ttl_s, poll_s
        self._rev: int | None = None

    def _lease(self) -> bytes:
        return json.dumps(
            {"owner": self.owner, "expires_at": format_time(datetime.now(UTC) + timedelta(seconds=self.ttl_s))}
        ).encode()

    async def try_acquire(self) -> bool:
        try:
            self._rev = await self.kv.create(self.key, self._lease())
            return True
        except KeyWrongLastSequenceError:
            pass
        try:
            entry = await self.kv.get(self.key)
        except NotFoundError:
            return False  # released between the two calls; the next poll creates it
        expires = parse_time(json.loads(entry.value or b"{}").get("expires_at")) if entry.value else None
        if expires is not None and expires > datetime.now(UTC):
            return False
        try:
            self._rev = await self.kv.update(self.key, self._lease(), last=entry.revision)
            return True
        except KeyWrongLastSequenceError:
            return False

    async def acquire(self, timeout_s: float) -> bool:
        """Poll until acquired, or False after timeout_s."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            if await self.try_acquire():
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(self.poll_s)

    async def release(self) -> None:
        if self._rev is None:
            return
        try:
            await self.kv.delete(self.key, last=self._rev)
        except Exception as e:  # noqa: BLE001 — the TTL reaps it
            log.debug("lock release %s: %s", self.key, e)
        self._rev = None


@dataclass
class _Entry:
    state: TokenState = field(default_factory=TokenState)
    creds: ClientCreds = field(default_factory=ClientCreds)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class TokenSource:
    """`get(account_id)` returns a token that is valid for at least REFRESH_SKEW_S."""

    REFRESH_SKEW_S: float = 60
    LOCK_TTL_S: float = 30

    def __init__(self, worker: Worker, refresh: RefreshFn | None = None, *, timeout: float = 5) -> None:
        self._worker = worker
        self._refresh = refresh
        self.timeout = timeout
        self._cache: dict[str, _Entry] = {}

    @property
    def _provider(self) -> str:
        return self._worker.identity.provider

    def invalidate(self, account_id: str) -> None:
        self._cache.pop(account_id, None)

    async def get(self, account_id: str) -> TokenState:
        entry = self._cache.setdefault(account_id, _Entry())
        async with entry.lock:
            if entry.state.fresh(self.REFRESH_SKEW_S):
                return entry.state
            entry.state, entry.creds = await self.server_get(account_id)
            if entry.state.fresh(self.REFRESH_SKEW_S):
                return entry.state
            if self._refresh is None:
                raise TokenError("no_refresh", f"token for {account_id} expired and no refresh configured")
            entry.state = await self._refresh_locked(account_id, entry.creds)
            return entry.state

    async def _rpc(self, op: str, body: dict[str, Any]) -> dict[str, Any]:
        subject = C.tokens_subject(self._provider, op)
        try:
            resp = await self._worker._nats.request(subject, json.dumps(body).encode(), timeout=self.timeout)
        except Exception as e:
            raise TokenError("transient", f"{subject}: {e}", retry_after_s=30) from e
        reply = json.loads(resp.data)
        return reply if isinstance(reply, dict) else {"error": "malformed reply"}

    async def server_get(self, account_id: str) -> tuple[TokenState, ClientCreds]:
        reply = await self._rpc("get", {"account_id": account_id})
        if reply.get("error"):
            raise TokenError(str(reply["error"]), retry_after_s=float(reply.get("retry_after") or 30))
        return TokenState.from_json(reply), ClientCreds(
            str(reply.get("client_id") or ""), str(reply.get("client_secret") or "")
        )

    async def store(
        self, account_id: str, state: TokenState, previous_expires_at: datetime | None
    ) -> TokenState | None:
        """Persist a refreshed state; the winning state when another instance stored first."""
        body = {"account_id": account_id, **state.to_json()}
        body["previous_expires_at"] = (
            format_time(previous_expires_at) if previous_expires_at else "0001-01-01T00:00:00Z"
        )
        reply = await self._rpc("store", body)
        if reply.get("ok"):
            return None
        error = str(reply.get("error") or "unknown")
        if error == "stale_store":
            log.debug("token store stale for %s; using the stored state", account_id)
            winner, _ = await self.server_get(account_id)
            return winner
        raise TokenError(error, f"token store for {account_id}: {error}")

    async def needs_reauth(self, account_id: str, reason: str) -> None:
        try:
            await self._rpc("needs_reauth", {"account_id": account_id, "reason": reason})
        except TokenError as e:
            log.warning("needs_reauth for %s failed: %s", account_id, e)

    async def _lock(self, account_id: str) -> KVLock | None:
        try:
            kv = await self._worker._js.key_value(C.RATE_LIMITS_KV)
        except Exception as e:  # noqa: BLE001 — refresh still runs, serialised per process only
            log.warning("refresh lock unavailable (%s); refreshing without it", e)
            return None
        key = f"lock.{self._provider}.{account_id}"
        return KVLock(kv, key, owner=self._worker.cfg.instance_id, ttl_s=self.LOCK_TTL_S)

    async def _refresh_locked(self, account_id: str, creds: ClientCreds) -> TokenState:
        refresh = self._refresh
        if refresh is None:
            raise TokenError("no_refresh", "no refresh configured")
        lock = await self._lock(account_id)
        if lock is not None and not await lock.acquire(self.LOCK_TTL_S * 2):
            raise TokenError("transient", f"refresh lock for {account_id} busy", retry_after_s=5)
        try:
            # Whoever held the lock may have stored a fresh token meanwhile.
            current, server_creds = await self.server_get(account_id)
            if current.fresh(self.REFRESH_SKEW_S):
                return current
            log.info("refreshing OAuth token for %s (expires %s)", account_id, current.expires_at)
            try:
                fresh = await refresh(current, server_creds if server_creds.client_id else creds)
            except TokenError as e:
                if e.terminal:
                    await self.needs_reauth(account_id, e.reason)
                raise
            if not fresh.app_id:
                fresh.app_id = current.app_id
            return await self.store(account_id, fresh, current.expires_at) or fresh
        finally:
            if lock is not None:
                await lock.release()
