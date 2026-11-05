"""OAuth 2 on the worker side (provider contract §9b): the authorization-code
exchange core asks the pool for on cairn.oauth.<provider>.<pool>.exchange,
and the refresh grant the TokenSource runs. Both speak to the provider's
token endpoint directly, so client authentication (body or HTTP Basic),
PKCE, extra parameters and non-standard endpoints stay in the worker.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import requests

from .errors import TokenError
from .rfc3339 import format_time
from .tokens import ClientCreds, TokenState

ClientAuth = Literal["body", "basic"]


@dataclass
class OAuthExchangeRequest:
    """Core's request: the callback's code plus the connection's app credentials."""

    connection_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    code: str = ""
    redirect_uri: str = ""
    code_verifier: str = ""  # PKCE, when the provider meta declares it

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> OAuthExchangeRequest:
        return cls(**{k: str(d.get(k) or "") for k in cls.__dataclass_fields__})


@dataclass
class OAuthExchangeReply:
    """Tokens plus the provider's stable account id, or an error
    ("exchange_failed" | "bad_request" | "transient") with a message.
    """

    access_token: str = ""
    refresh_token: str = ""
    expires_at: datetime | None = None
    scope: str = ""
    token_type: str = ""
    provider_account_id: str = ""
    error: str = ""
    message: str = ""

    @classmethod
    def failure(cls, error: str, message: str) -> OAuthExchangeReply:
        return cls(error=error, message=message)

    @classmethod
    def of(cls, state: TokenState, provider_account_id: str = "") -> OAuthExchangeReply:
        return cls(
            access_token=state.access_token,
            refresh_token=state.refresh_token,
            expires_at=state.expires_at,
            scope=state.scope,
            token_type=state.token_type,
            provider_account_id=provider_account_id,
        )

    def to_json(self) -> dict[str, Any]:
        if self.error:
            return {"error": self.error, "message": self.message}
        out: dict[str, Any] = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
            "token_type": self.token_type,
            "provider_account_id": self.provider_account_id,
        }
        if self.expires_at is not None:
            out["expires_at"] = format_time(self.expires_at)
        return out


OAuthExchangeHandler = Callable[[OAuthExchangeRequest], Awaitable[OAuthExchangeReply]]


def parse_token_response(data: Mapping[str, Any], *, previous: TokenState | None = None) -> TokenState:
    """A standard token response; `expires_in` becomes an absolute expiry,
    and a missing refresh token or scope carries over from `previous`.
    """
    access = str(data.get("access_token") or "")
    if not access:
        raise TokenError("exchange_failed", "token response without access_token")
    expires_at = previous.expires_at if previous else None
    if data.get("expires_in") is not None:
        expires_at = datetime.now(UTC) + timedelta(seconds=float(data["expires_in"]))
    elif data.get("expires_at") is not None:
        expires_at = datetime.fromtimestamp(float(data["expires_at"]), tz=UTC)
    return TokenState(
        access_token=access,
        refresh_token=str(data.get("refresh_token") or (previous.refresh_token if previous else "")),
        expires_at=expires_at,
        scope=str(data.get("scope") or (previous.scope if previous else "")),
        token_type=str(data.get("token_type") or "Bearer"),
        app_id=previous.app_id if previous else "",
    )


async def token_request(
    token_url: str,
    form: Mapping[str, str],
    *,
    client: ClientCreds | None = None,
    client_auth: ClientAuth = "body",
    headers: Mapping[str, str] | None = None,
    timeout_s: float = 15,
) -> dict[str, Any]:
    """POST a form to the token endpoint. 5xx/network errors raise
    TokenError("transient"); 4xx raise TokenError(<error code>) with the
    provider's description, so `invalid_grant` is terminal.
    """
    data = dict(form)
    auth = None
    if client is not None:
        if client_auth == "basic":
            auth = (client.client_id, client.client_secret)
        else:
            data.setdefault("client_id", client.client_id)
            data.setdefault("client_secret", client.client_secret)
    hdrs = {"Accept": "application/json", **(headers or {})}
    try:
        resp = await asyncio.to_thread(requests.post, token_url, data=data, auth=auth, headers=hdrs, timeout=timeout_s)
    except requests.RequestException as e:
        raise TokenError("transient", f"token endpoint: {e}", retry_after_s=30) from e
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if resp.status_code >= 500 or resp.status_code == 429:
        retry = float(resp.headers.get("Retry-After") or 30)
        raise TokenError("transient", f"token endpoint status {resp.status_code}", retry_after_s=retry)
    if resp.status_code >= 400:
        reason = str(body.get("error") or f"http_{resp.status_code}")
        raise TokenError(reason, str(body.get("error_description") or body.get("message") or resp.text[:200]))
    return body


async def exchange_code(
    req: OAuthExchangeRequest,
    token_url: str,
    *,
    client_auth: ClientAuth = "body",
    extra_params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    account_id_from: Callable[[Mapping[str, Any]], str] | None = None,
    account_id_lookup: Callable[[TokenState], Awaitable[str]] | None = None,
    timeout_s: float = 15,
) -> OAuthExchangeReply:
    """The standard authorization-code grant as an exchange reply. The
    provider account id comes from the token response (`account_id_from`,
    e.g. Strava's athlete.id) or a follow-up call (`account_id_lookup`).
    """
    if not req.client_id or not req.client_secret or not req.code:
        return OAuthExchangeReply.failure("bad_request", "client_id, client_secret and code are required")
    form = {"grant_type": "authorization_code", "code": req.code, **(extra_params or {})}
    if req.redirect_uri:
        form["redirect_uri"] = req.redirect_uri
    if req.code_verifier:
        form["code_verifier"] = req.code_verifier
    creds = ClientCreds(req.client_id, req.client_secret)
    try:
        body = await token_request(
            token_url, form, client=creds, client_auth=client_auth, headers=headers, timeout_s=timeout_s
        )
        state = parse_token_response(body)
        account_id = account_id_from(body) if account_id_from else ""
        if not account_id and account_id_lookup is not None:
            account_id = await account_id_lookup(state)
    except TokenError as e:
        return OAuthExchangeReply.failure("transient" if e.reason == "transient" else "exchange_failed", str(e))
    return OAuthExchangeReply.of(state, account_id)


async def refresh_grant(
    token_url: str,
    current: TokenState,
    creds: ClientCreds,
    *,
    client_auth: ClientAuth = "body",
    extra_params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_s: float = 15,
) -> TokenState:
    """The standard refresh grant, shaped as a TokenSource refresh function:
    `TokenSource(worker, refresh=lambda s, c: refresh_grant(URL, s, c))`.
    """
    if not current.refresh_token:
        raise TokenError("needs_reauth", "no refresh token")
    form = {"grant_type": "refresh_token", "refresh_token": current.refresh_token, **(extra_params or {})}
    body = await token_request(
        token_url, form, client=creds, client_auth=client_auth, headers=headers, timeout_s=timeout_s
    )
    return parse_token_response(body, previous=current)
