"""Credential providers: the connection's login pair from cairn.creds.<provider>.get."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from nats.aio.client import Client as NATS

from . import config as C
from .errors import CredentialsError

log = logging.getLogger("cairn.sdk")


@dataclass(frozen=True)
class Credentials:
    client_id: str
    client_secret: str


async def fetch_creds(nc: NATS, provider: str, account_id: str, *, timeout_s: float = 5) -> Credentials:
    """Ask core for the account's credentials; raises CredentialsError when
    core answers an error (account_gone, needs_reauth, transient, ...) or
    does not answer.
    """
    try:
        resp = await nc.request(
            C.creds_subject(provider), json.dumps({"account_id": account_id}).encode(), timeout=timeout_s
        )
        data = json.loads(resp.data)
    except Exception as e:
        log.warning("creds rpc failed for %s: %s", account_id, e)
        raise CredentialsError(account_id, f"rpc failed: {e}") from e
    # client_id/client_secret are the connection's credential slots;
    # email/password are the aliases core <= 0.6 replied with.
    login = data.get("client_id") or data.get("email")
    if data.get("error") or not login:
        log.warning("creds rpc for %s: %s", account_id, data.get("error"))
        raise CredentialsError(account_id, str(data.get("error") or "empty reply"))
    return Credentials(str(login), str(data.get("client_secret") or data.get("password", "")))
