"""Code exchange and refresh grant against a fake token endpoint."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import requests

from cairn_provider_sdk import ClientCreds, OAuthExchangeReply, OAuthExchangeRequest, TokenError, TokenState, oauth

URL = "https://provider.example/oauth/token"


class Resp:
    def __init__(self, status: int, body: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = "" if body is None else str(body)

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no json")
        return self._body


class Endpoint:
    def __init__(self, *responses: Resp | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kw: Any) -> Resp:
        self.calls.append({"url": url, **kw})
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


@pytest.fixture
def endpoint(monkeypatch: pytest.MonkeyPatch) -> Endpoint:
    ep = Endpoint()
    monkeypatch.setattr(requests, "post", ep.post)
    return ep


REQ = OAuthExchangeRequest(
    connection_id="c1",
    client_id="id",
    client_secret="secret",
    code="code",
    redirect_uri="https://me/cb",
    code_verifier="v",
)


def test_exchange_with_body_auth_and_pkce(endpoint: Endpoint) -> None:
    endpoint.responses = [
        Resp(200, {"access_token": "at", "refresh_token": "rt", "expires_in": 3600, "athlete": {"id": 7}})
    ]
    reply = asyncio.run(
        oauth.exchange_code(REQ, URL, extra_params={"x": "1"}, account_id_from=lambda d: str(d["athlete"]["id"]))
    )
    assert reply.error == ""
    assert (reply.access_token, reply.refresh_token, reply.provider_account_id, reply.token_type) == (
        "at",
        "rt",
        "7",
        "Bearer",
    )
    assert reply.expires_at is not None
    assert reply.expires_at > datetime.now(UTC)
    call = endpoint.calls[0]
    assert call["auth"] is None
    assert call["data"] == {
        "grant_type": "authorization_code",
        "code": "code",
        "x": "1",
        "redirect_uri": "https://me/cb",
        "code_verifier": "v",
        "client_id": "id",
        "client_secret": "secret",
    }
    assert reply.to_json()["provider_account_id"] == "7"


def test_exchange_with_basic_auth_and_lookup(endpoint: Endpoint) -> None:
    endpoint.responses = [Resp(200, {"access_token": "at", "expires_at": 1_900_000_000})]

    async def lookup(state: TokenState) -> str:
        return f"user-of-{state.access_token}"

    reply = asyncio.run(oauth.exchange_code(REQ, URL, client_auth="basic", account_id_lookup=lookup))
    assert reply.provider_account_id == "user-of-at"
    assert endpoint.calls[0]["auth"] == ("id", "secret")
    assert "client_secret" not in endpoint.calls[0]["data"]
    assert reply.expires_at == datetime.fromtimestamp(1_900_000_000, tz=UTC)


def test_exchange_rejects_incomplete_requests(endpoint: Endpoint) -> None:
    reply = asyncio.run(oauth.exchange_code(OAuthExchangeRequest(code="c"), URL))
    assert reply.error == "bad_request"
    assert endpoint.calls == []


@pytest.mark.parametrize(
    ("response", "error", "fragment"),
    [
        (Resp(400, {"error": "invalid_grant", "error_description": "bad code"}), "exchange_failed", "bad code"),
        (Resp(401, None), "exchange_failed", "http_401"),
        (Resp(200, {"token_type": "Bearer"}), "exchange_failed", "without access_token"),
        (Resp(503, None), "transient", "503"),
        (Resp(429, None, {"Retry-After": "9"}), "transient", "429"),
        (requests.ConnectionError("down"), "transient", "down"),
    ],
)
def test_exchange_error_mapping(endpoint: Endpoint, response: Resp | Exception, error: str, fragment: str) -> None:
    endpoint.responses = [response]
    reply = asyncio.run(oauth.exchange_code(REQ, URL))
    assert reply.error == error
    assert fragment in reply.message
    assert reply.to_json() == {"error": error, "message": reply.message}


def test_refresh_grant_keeps_previous_fields(endpoint: Endpoint) -> None:
    endpoint.responses = [Resp(200, {"access_token": "new", "expires_in": 60})]
    current = TokenState(access_token="old", refresh_token="rt", scope="read", app_id="app")
    state = asyncio.run(oauth.refresh_grant(URL, current, ClientCreds("id", "s")))
    assert (state.access_token, state.refresh_token, state.scope, state.app_id) == ("new", "rt", "read", "app")
    assert endpoint.calls[0]["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "rt",
        "client_id": "id",
        "client_secret": "s",
    }


def test_refresh_grant_errors(endpoint: Endpoint) -> None:
    with pytest.raises(TokenError) as e:
        asyncio.run(oauth.refresh_grant(URL, TokenState(access_token="x"), ClientCreds()))
    assert e.value.reason == "needs_reauth"
    endpoint.responses = [Resp(400, {"error": "invalid_grant"})]
    with pytest.raises(TokenError) as e2:
        asyncio.run(oauth.refresh_grant(URL, TokenState(refresh_token="rt"), ClientCreds()))
    assert e2.value.terminal
    endpoint.responses = [Resp(500, [1, 2])]
    with pytest.raises(TokenError) as e3:
        asyncio.run(oauth.refresh_grant(URL, TokenState(refresh_token="rt"), ClientCreds()))
    assert e3.value.reason == "transient"


def test_request_and_reply_json() -> None:
    req = OAuthExchangeRequest.from_json({"connection_id": "c", "code": "x", "extra": 1})
    assert (req.connection_id, req.code, req.client_id) == ("c", "x", "")
    exp = datetime(2026, 1, 1, tzinfo=UTC)
    reply = OAuthExchangeReply.of(TokenState(access_token="a", expires_at=exp, scope="s"), "pid")
    assert reply.to_json() == {
        "access_token": "a",
        "refresh_token": "",
        "scope": "s",
        "token_type": "",
        "provider_account_id": "pid",
        "expires_at": "2026-01-01T00:00:00Z",
    }
    assert "expires_at" not in OAuthExchangeReply(access_token="a").to_json()


def test_parse_token_response_defaults() -> None:
    s = oauth.parse_token_response({"access_token": "a"})
    assert (s.token_type, s.expires_at, s.refresh_token) == ("Bearer", None, "")
    prev = TokenState(expires_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=1))
    assert oauth.parse_token_response({"access_token": "a"}, previous=prev).expires_at == prev.expires_at
