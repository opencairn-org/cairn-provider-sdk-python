"""cairn.creds.<provider>.get: slots, legacy aliases, errors."""

import asyncio
from typing import Any

import pytest

from cairn_provider_sdk import CredentialsError
from cairn_provider_sdk.creds import fetch_creds
from cairn_provider_sdk.testing import FakeNC, as_nats

SUBJECT = "cairn.creds.acme.get"


def test_slots_win_over_aliases() -> None:
    nc = FakeNC({SUBJECT: {"client_id": "me", "client_secret": "pw", "email": "old", "password": "x"}})
    c = asyncio.run(fetch_creds(as_nats(nc), "acme", "acc-1"))
    assert (c.client_id, c.client_secret) == ("me", "pw")
    assert nc.requests == [(SUBJECT, {"account_id": "acc-1"})]


def test_legacy_aliases_still_read() -> None:
    nc = FakeNC({SUBJECT: {"email": "e@x", "password": "pw"}})
    c = asyncio.run(fetch_creds(as_nats(nc), "acme", "acc-1"))
    assert (c.client_id, c.client_secret) == ("e@x", "pw")


@pytest.mark.parametrize(
    ("reply", "reason"),
    [({"error": "needs_reauth"}, "needs_reauth"), ({}, "empty reply"), (RuntimeError("no responders"), "rpc failed")],
)
def test_errors_raise(reply: Any, reason: str) -> None:
    with pytest.raises(CredentialsError, match=reason) as e:
        asyncio.run(fetch_creds(as_nats(FakeNC({SUBJECT: reply})), "acme", "acc-2"))
    assert e.value.account_id == "acc-2"
