"""Token source: cache, server round-trips, refresh under the KV lock, and the lock itself."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from nats.js.errors import KeyWrongLastSequenceError

from cairn_provider_sdk import ClientCreds, TokenError, TokenSource, TokenState, Worker
from cairn_provider_sdk.rfc3339 import format_time
from cairn_provider_sdk.testing import FakeKV, FakeNC, example_identity, example_settings, make_worker
from cairn_provider_sdk.tokens import KVLock

GET, STORE, REAUTH = "cairn.tokens.acme.get", "cairn.tokens.acme.store", "cairn.tokens.acme.needs_reauth"


def token(access: str, expires_in: float, **extra: Any) -> dict[str, Any]:
    return {
        "access_token": access,
        "refresh_token": "rt",
        "expires_at": format_time(datetime.now(UTC) + timedelta(seconds=expires_in)),
        "client_id": "cid",
        "client_secret": "cs",
        "app_id": "app",
        **extra,
    }


def source(replies: dict[str, Any], refresh: Any = None, *, with_kv: bool = True) -> tuple[TokenSource, FakeNC, FakeKV]:
    nc = FakeNC(replies)
    wk, js = make_worker(Worker(example_identity(), example_settings(), refresh=refresh), nc)
    kv = FakeKV()
    if with_kv:
        js.buckets["cairn_rate_limits"] = kv
    return wk.tokens, nc, kv


def test_token_state_round_trip() -> None:
    s = TokenState.from_json(token("at", 3600, scope="read"))
    assert s.fresh()
    assert s.to_json()["scope"] == "read"
    assert "app_id" not in s.to_json()
    assert not TokenState().fresh()
    assert TokenState(access_token="x").fresh()  # no expiry known: trusted


def test_get_caches_fresh_tokens() -> None:
    ts, nc, _ = source({GET: token("at", 3600)})
    first = asyncio.run(ts.get("acc"))
    second = asyncio.run(ts.get("acc"))
    assert first.access_token == "at"
    assert second is first
    assert len(nc.requests) == 1
    ts.invalidate("acc")
    asyncio.run(ts.get("acc"))
    assert len(nc.requests) == 2


def test_get_without_refresh_fails_on_expired() -> None:
    ts, _, _ = source({GET: token("old", -10)})
    with pytest.raises(TokenError, match="no refresh"):
        asyncio.run(ts.get("acc"))


def test_server_errors_map_to_token_errors() -> None:
    ts, _, _ = source({GET: {"error": "needs_reauth"}})
    with pytest.raises(TokenError) as e:
        asyncio.run(ts.get("acc"))
    assert e.value.terminal
    ts2, _, _ = source({GET: {"error": "transient", "retry_after": 7}})
    with pytest.raises(TokenError) as e2:
        asyncio.run(ts2.get("acc"))
    assert (e2.value.terminal, e2.value.retry_after_s) == (False, 7)
    ts3, _, _ = source({})
    with pytest.raises(TokenError) as e3:
        asyncio.run(ts3.get("acc"))
    assert e3.value.reason == "transient"


def test_refresh_stores_under_the_lock() -> None:
    refreshed: list[tuple[str, str]] = []

    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        refreshed.append((state.refresh_token, creds.client_id))
        return TokenState(access_token="new", refresh_token="rt2", expires_at=datetime.now(UTC) + timedelta(hours=1))

    ts, nc, kv = source({GET: [token("old", -10), token("old", -10)], STORE: {"ok": True}}, refresh)
    got = asyncio.run(ts.get("acc"))
    assert got.access_token == "new"
    assert got.app_id == "app"  # carried over from the server state
    assert refreshed == [("rt", "cid")]
    store = next(body for subj, body in nc.requests if subj == STORE)
    assert (store["account_id"], store["access_token"], store["refresh_token"]) == ("acc", "new", "rt2")
    assert store["previous_expires_at"]
    assert all(e.value is None for e in kv.store.values())  # lock released


def test_refresh_uses_the_winner_on_stale_store() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        return TokenState(access_token="mine", expires_at=datetime.now(UTC) + timedelta(hours=1))

    replies = {GET: [token("old", -10), token("old", -10), token("theirs", 3600)], STORE: {"error": "stale_store"}}
    ts, _, _ = source(replies, refresh)
    assert asyncio.run(ts.get("acc")).access_token == "theirs"


def test_refresh_skipped_when_another_instance_already_did() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        raise AssertionError("must not refresh")

    ts, _, _ = source({GET: [token("old", -10), token("fresh", 3600)]}, refresh)
    assert asyncio.run(ts.get("acc")).access_token == "fresh"


def test_terminal_refresh_marks_needs_reauth() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        raise TokenError("invalid_grant", "revoked")

    ts, nc, _ = source({GET: [token("old", -10), token("old", -10)], REAUTH: {"ok": True}}, refresh)
    with pytest.raises(TokenError, match="revoked"):
        asyncio.run(ts.get("acc"))
    assert (REAUTH, {"account_id": "acc", "reason": "invalid_grant"}) in nc.requests


def test_store_errors() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        return TokenState(access_token="n", expires_at=datetime.now(UTC) + timedelta(hours=1))

    ts, _, _ = source({GET: [token("old", -10), token("old", -10)], STORE: {"error": "account_gone"}}, refresh)
    with pytest.raises(TokenError, match="account_gone"):
        asyncio.run(ts.get("acc"))


def test_refresh_without_kv_still_works() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        return TokenState(access_token="n", expires_at=datetime.now(UTC) + timedelta(hours=1))

    ts, _, _ = source({GET: [token("old", -10), token("old", -10)], STORE: {"ok": True}}, refresh, with_kv=False)
    assert asyncio.run(ts.get("acc")).access_token == "n"


def test_busy_lock_is_transient() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        raise AssertionError("must not refresh")

    ts, _, kv = source({GET: [token("old", -10)]}, refresh)
    ts.LOCK_TTL_S = 0.01
    lease = {"owner": "other", "expires_at": format_time(datetime.now(UTC) + timedelta(hours=1))}
    asyncio.run(kv.put("lock.acme.acc", json.dumps(lease).encode()))
    with pytest.raises(TokenError, match="lock"):
        asyncio.run(ts.get("acc"))


def test_needs_reauth_rpc_failure_is_logged_only() -> None:
    ts, _, _ = source({})
    asyncio.run(ts.needs_reauth("acc", "x"))


# --- the lock ---


def test_lock_create_release_and_takeover() -> None:
    kv = FakeKV()
    a = KVLock(kv.as_kv(), "lock.k", owner="a", ttl_s=60, poll_s=0)
    b = KVLock(kv.as_kv(), "lock.k", owner="b", ttl_s=60, poll_s=0)

    async def run() -> None:
        assert await a.try_acquire()
        assert not await b.try_acquire()
        assert not await b.acquire(0)  # times out at once
        await a.release()
        assert await b.acquire(1)
        await b.release()
        await b.release()  # idempotent

    asyncio.run(run())


def test_lock_takes_over_an_expired_lease() -> None:
    kv = FakeKV()
    stale = {"owner": "dead", "expires_at": format_time(datetime.now(UTC) - timedelta(seconds=1))}
    asyncio.run(kv.put("lock.k", json.dumps(stale).encode()))
    lock = KVLock(kv.as_kv(), "lock.k", owner="me", ttl_s=60)
    assert asyncio.run(lock.try_acquire())
    assert json.loads(kv.store["lock.k"].value or b"")["owner"] == "me"


def test_lock_loses_the_takeover_race() -> None:
    kv = FakeKV()
    stale = {"owner": "dead", "expires_at": format_time(datetime.now(UTC) - timedelta(seconds=1))}
    asyncio.run(kv.put("lock.k", json.dumps(stale).encode()))

    async def racing_update(key: str, value: bytes, last: int | None = None) -> int:
        raise KeyWrongLastSequenceError

    kv.update = racing_update  # type: ignore[method-assign]
    assert not asyncio.run(KVLock(kv.as_kv(), "lock.k", owner="me").try_acquire())


def test_lock_handles_release_between_create_and_get() -> None:
    kv = FakeKV()
    asyncio.run(kv.put("lock.k", b"{}"))
    original_get = kv.get

    async def get_then_gone(key: str) -> Any:
        asyncio.get_running_loop().call_soon(lambda: None)
        await kv.delete(key)
        return await original_get(key)

    kv.get = get_then_gone  # type: ignore[method-assign]
    assert not asyncio.run(KVLock(kv.as_kv(), "lock.k", owner="me").try_acquire())


def test_invalidate_an_unknown_account() -> None:
    ts, _, _ = source({})
    ts.invalidate("never-seen")


def test_failure_reasons_and_retry_hints() -> None:
    # The reason decides terminal (re-authorize) vs retry; the hint is when.
    cases: list[tuple[dict[str, Any], str, float]] = [
        ({GET: token("old", -10)}, "no_refresh", 0),  # nothing can refresh it
        ({}, "transient", 30),  # core did not answer
        ({GET: {"error": "transient"}}, "transient", 30),  # no retry_after: the default
        ({GET: [["not", "an", "object"]]}, "malformed reply", 30),
    ]
    for replies, reason, retry in cases:
        ts, _, _ = source(replies)
        with pytest.raises(TokenError) as e:
            asyncio.run(ts.get("acc"))
        assert (e.value.reason, e.value.terminal, e.value.retry_after_s) == (reason, False, retry), replies


def test_busy_lock_reason_and_lease() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        raise AssertionError("must not refresh")

    ts, _, kv = source({GET: [token("old", -10)]}, refresh)
    ts.LOCK_TTL_S = 0.01
    lease = {"owner": "other", "expires_at": format_time(datetime.now(UTC) + timedelta(hours=1))}
    asyncio.run(kv.put("lock.acme.acc", json.dumps(lease).encode()))
    with pytest.raises(TokenError) as e:
        asyncio.run(ts.get("acc"))
    assert (e.value.reason, e.value.terminal, e.value.retry_after_s) == ("transient", False, 5)
    assert json.loads(kv.store["lock.acme.acc"].value or b"{}")["owner"] == "other"  # never taken


def test_refresh_uses_the_connection_creds_and_stores_the_previous_expiry() -> None:
    seen: list[ClientCreds] = []

    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        seen.append(creds)
        return TokenState(access_token="new", app_id="own", expires_at=datetime.now(UTC) + timedelta(hours=1))

    old = token("old", -10, client_id="conn-id", client_secret="conn-secret")
    ts, nc, _ = source({GET: [old, old], STORE: {"ok": True}}, refresh)
    got = asyncio.run(ts.get("acc"))
    assert seen == [ClientCreds("conn-id", "conn-secret")]
    assert got.app_id == "own"  # the refresher's app id wins over the stored one
    store = next(body for subj, body in nc.requests if subj == STORE)
    assert store["previous_expires_at"] == old["expires_at"]

    # Only a refresh token, no expiry: the store says so with the zero time.
    bare = {"refresh_token": "rt", "client_id": "cid"}
    ts2, nc2, _ = source({GET: [bare, bare], STORE: {"ok": True}}, refresh)
    asyncio.run(ts2.get("acc"))
    store2 = next(body for subj, body in nc2.requests if subj == STORE)
    assert store2["previous_expires_at"] == "0001-01-01T00:00:00Z"


def test_store_failure_reasons() -> None:
    async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
        return TokenState(access_token="n", expires_at=datetime.now(UTC) + timedelta(hours=1))

    for reply, reason in [({"error": "account_gone"}, "account_gone"), ({}, "unknown")]:
        ts, _, _ = source({GET: [token("old", -10), token("old", -10)], STORE: reply}, refresh)
        with pytest.raises(TokenError) as e:
            asyncio.run(ts.get("acc"))
        assert e.value.reason == reason
        assert e.value.terminal == (reason == "account_gone")


def test_token_state_json_fields() -> None:
    at = datetime(2026, 11, 6, 9, 0, tzinfo=UTC)
    d = {
        "access_token": "a",
        "refresh_token": "r",
        "expires_at": format_time(at),
        "scope": "s",
        "token_type": "Bearer",
        "app_id": "x",
    }
    s = TokenState.from_json(d)
    assert (s.access_token, s.refresh_token, s.expires_at, s.scope, s.token_type, s.app_id) == (
        "a",
        "r",
        at,
        "s",
        "Bearer",
        "x",
    )
    assert s.to_json() == {k: v for k, v in d.items() if k != "app_id"}
    assert TokenState.from_json({}) == TokenState()
    assert TokenState(access_token="a").to_json() == {"access_token": "a"}
    soon = TokenState(access_token="a", expires_at=datetime.now(UTC) + timedelta(seconds=30))
    assert not soon.fresh()
    assert soon.fresh(skew_s=10)
