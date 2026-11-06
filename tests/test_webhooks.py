"""HMAC helpers and the verify wire types."""

import base64
import hashlib
import hmac

import pytest

from cairn_provider_sdk import WebhookVerifyRequest, WebhookVerifyResponse
from cairn_provider_sdk import webhooks as wh

SECRET, BODY = "s3cret", b'{"event": "update"}'


def test_sign_and_verify_hex() -> None:
    sig = wh.sign(SECRET, BODY)
    assert sig == hmac.new(SECRET.encode(), BODY, hashlib.sha256).hexdigest()
    assert wh.verify(SECRET, BODY, sig)
    assert wh.verify(SECRET, BODY, "sha256=" + sig, prefix="sha256=")
    assert not wh.verify(SECRET, BODY, sig, prefix="sha256=")
    assert not wh.verify(SECRET, BODY + b" ", sig)
    assert not wh.verify("other", BODY, sig)
    assert not wh.verify(SECRET, BODY, "zz")  # not hex


@pytest.mark.parametrize("encoding", ["base64", "base64url"])
def test_base64_encodings(encoding: str) -> None:
    sig = wh.sign(b"k", BODY, encoding=encoding, digest="sha1")  # type: ignore[arg-type]
    assert wh.verify(b"k", BODY, sig, encoding=encoding, digest="sha1")  # type: ignore[arg-type]
    assert not wh.verify(b"k", BODY, "***", encoding=encoding, digest="sha1")  # type: ignore[arg-type]
    if encoding == "base64":
        assert base64.b64decode(sig) == hmac.new(b"k", BODY, hashlib.sha1).digest()


def test_timestamp_schemes() -> None:
    sig = wh.sign(SECRET, BODY, timestamp="1700000000")
    assert wh.verify(SECRET, BODY, sig, timestamp="1700000000")
    assert not wh.verify(SECRET, BODY, sig, timestamp="1700000001")
    assert not wh.verify(SECRET, BODY, sig)
    v = wh.sign(SECRET, BODY, timestamp="1700000000", separator=b"")
    assert wh.signed_payload(BODY, "1700000000", b"") == b"1700000000" + BODY
    assert wh.verify(SECRET, BODY, v, timestamp="1700000000", separator=b"")


def test_parse_signature_header() -> None:
    assert wh.parse_signature_header("t=1700000000, v1=abc,v0=def") == {"t": "1700000000", "v1": "abc", "v0": "def"}
    assert wh.parse_signature_header("") == {}


def test_verify_wire_types() -> None:
    req = WebhookVerifyRequest.from_json({"method": "GET", "query": {"hub.challenge": "x"}})
    assert (req.method, req.query) == ("GET", {"hub.challenge": "x"})
    assert WebhookVerifyRequest.from_json({}).method == "GET"
    resp = WebhookVerifyResponse(body=b"hi")
    assert resp.to_json() == {
        "status": 200,
        "content_type": "application/json",
        "body": base64.b64encode(b"hi").decode(),
    }


def test_signature_formats_are_strict() -> None:
    # base64url signatures go out unpadded, as the providers send them.
    assert "=" not in wh.sign(b"k", BODY, encoding="base64url", digest="sha1")
    sig = wh.sign(b"k", BODY, encoding="base64")
    assert not wh.verify(b"k", BODY, sig[:10] + "!" + sig[10:], encoding="base64")
    assert wh.signed_payload(BODY, "1700000000") == b"1700000000." + BODY
    # A value may itself contain "=" (base64 padding).
    assert wh.parse_signature_header("t=1,v1=YWJj=") == {"t": "1", "v1": "YWJj="}
    assert WebhookVerifyRequest.from_json({"method": "POST"}).method == "POST"
