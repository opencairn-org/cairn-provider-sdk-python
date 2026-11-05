"""Webhook signature helpers (constant-time HMAC) and the wire types of the
webhook relay: core forwards POST /webhooks/<worker_key> bodies to
cairn.webhooks.<worker_key>.event and GET handshakes to
cairn.webhooks.<worker_key>.verify.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from dataclasses import dataclass, field
from typing import Any, Literal

Encoding = Literal["hex", "base64", "base64url"]


def _encode(mac: bytes, encoding: Encoding) -> str:
    if encoding == "hex":
        return mac.hex()
    if encoding == "base64":
        return base64.b64encode(mac).decode()
    return base64.urlsafe_b64encode(mac).rstrip(b"=").decode()


def _decode(sig: str, encoding: Encoding) -> bytes | None:
    try:
        if encoding == "hex":
            return bytes.fromhex(sig.strip())
        if encoding == "base64":
            return base64.b64decode(sig.strip(), validate=True)
        padded = sig.strip() + "=" * (-len(sig.strip()) % 4)
        return base64.urlsafe_b64decode(padded)
    except ValueError:
        return None


def signed_payload(body: bytes, timestamp: str | None = None, separator: bytes = b".") -> bytes:
    """`body`, or `<timestamp><separator><body>` when the scheme signs a timestamp too."""
    return body if timestamp is None else timestamp.encode() + separator + body


def sign(
    secret: str | bytes,
    body: bytes,
    *,
    timestamp: str | None = None,
    separator: bytes = b".",
    digest: str = "sha256",
    encoding: Encoding = "hex",
) -> str:
    """HMAC of the body (or timestamp + body), encoded as the provider sends it."""
    key = secret.encode() if isinstance(secret, str) else secret
    mac = hmac.new(key, signed_payload(body, timestamp, separator), getattr(hashlib, digest)).digest()
    return _encode(mac, encoding)


def verify(
    secret: str | bytes,
    body: bytes,
    signature: str,
    *,
    timestamp: str | None = None,
    separator: bytes = b".",
    digest: str = "sha256",
    encoding: Encoding = "hex",
    prefix: str = "",
) -> bool:
    """Constant-time check of a provider's signature (strip `prefix` such as
    "sha256=" first). Any malformed signature is simply false.
    """
    if prefix:
        if not signature.startswith(prefix):
            return False
        signature = signature[len(prefix) :]
    given = _decode(signature, encoding)
    if given is None:
        return False
    key = secret.encode() if isinstance(secret, str) else secret
    expected = hmac.new(key, signed_payload(body, timestamp, separator), getattr(hashlib, digest)).digest()
    return hmac.compare_digest(expected, given)


def parse_signature_header(header: str) -> dict[str, str]:
    """`t=1700000000,v1=abcd` → {"t": "1700000000", "v1": "abcd"}."""
    out: dict[str, str] = {}
    for part in header.split(","):
        k, sep, v = part.strip().partition("=")
        if sep:
            out[k.strip()] = v.strip()
    return out


@dataclass
class WebhookEvent:
    """One POST core forwarded: lowercased headers plus `x-webhook-source-addr`."""

    subject: str
    headers: dict[str, str]
    body: bytes
    delivered: int = 1


@dataclass
class WebhookVerifyRequest:
    method: str
    query: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> WebhookVerifyRequest:
        return cls(method=str(data.get("method") or "GET"), query=dict(data.get("query") or {}))


@dataclass
class WebhookVerifyResponse:
    """Core writes `body` verbatim with the status and content type."""

    status: int = 200
    content_type: str = "application/json"
    body: bytes = b""

    def to_json(self) -> dict[str, Any]:
        # Core decodes `body` as Go []byte: base64.
        return {"status": self.status, "content_type": self.content_type, "body": base64.b64encode(self.body).decode()}
