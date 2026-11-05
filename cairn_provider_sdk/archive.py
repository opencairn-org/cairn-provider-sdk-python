"""Blob I/O through URLs core presigns: raw-response archival, claim-check
uploads of results, and downloads of archived envelopes.

Core mints a presigned PUT on cairn.blobs.presign_upload.<provider>; the
worker uploads a self-describing JSON blob and hands the returned key back
verbatim as raw_blob_id. Archiving is best-effort (an import is durable
without it); result uploads are not (a lost body is a lost import).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any, TypedDict

import requests
from nats.aio.client import Client as NATS

from . import config as C
from .config import Identity

log = logging.getLogger("cairn.sdk")


class ResultRef(TypedDict):
    blob_id: str
    size: int
    sha256: str


def envelope(identity: Identity, **inputs: Any) -> dict[str, Any]:
    """The archived envelope: dataset version, provenance and every input the
    parser reads (cairn-core ADR 0005), in the given order.
    """
    return {
        "version": identity.dataset_version,
        "provenance": {
            "provider": identity.provider,
            "worker_name": identity.worker_name,
            "worker_version": identity.version,
            "package": identity.package,
            "archived_at": datetime.now(UTC).isoformat(),
        },
        **inputs,
    }


async def _presign_upload(nc: NATS, provider: str, req: dict[str, Any]) -> dict[str, Any]:
    reply = await nc.request(C.presign_upload_subject(provider), json.dumps(req).encode(), timeout=10)
    up = json.loads(reply.data)
    return up if isinstance(up, dict) else {"error": "malformed presign reply"}


async def _put(up: dict[str, Any], body: bytes) -> requests.Response:
    method = up.get("method") or "PUT"
    headers = dict(up.get("required_headers") or {})
    headers.setdefault("Content-Type", "application/json")
    return await asyncio.to_thread(requests.request, method, up["url"], data=body, headers=headers, timeout=30)


async def archive(nc: NATS, provider: str, user_id: str, source_id: str, env: dict[str, Any]) -> tuple[str | None, int]:
    """Upload an archive envelope; (blob_id, size), or (None, 0) on any failure."""
    body = json.dumps(env).encode()
    req = {
        "user_id": user_id,
        "source_id": source_id,
        "provider": provider,
        "content_type": "application/json",
        "content_length": len(body),
    }
    try:
        up = await _presign_upload(nc, provider, req)
    except Exception as e:  # noqa: BLE001
        log.warning("presign failed: %s", e)
        return None, 0
    if up.get("error") or not up.get("url"):
        log.warning("presign rejected: %s", up.get("error"))
        return None, 0
    try:
        resp = await _put(up, body)
    except Exception as e:  # noqa: BLE001
        log.warning("blob upload failed: %s", e)
        return None, 0
    if not (200 <= resp.status_code < 300):
        log.warning("blob upload status %s", resp.status_code)
        return None, 0
    return (up.get("blob_id") or None), len(body)


async def upload_result(nc: NATS, provider: str, body: bytes) -> ResultRef:
    """Claim-check upload of an event-carrying JobResult (kind "result": a
    short-lived transfer/ key core deletes after ingest). Raises on failure
    so the job NAKs and retries.
    """
    sha = hashlib.sha256(body).hexdigest()
    req = {
        "provider": provider,
        "kind": "result",
        "content_type": "application/json",
        "content_length": len(body),
        "content_sha256": sha,
    }
    up = await _presign_upload(nc, provider, req)
    if up.get("error") or not up.get("url"):
        raise RuntimeError(f"result presign rejected: {up.get('error')}")
    resp = await _put(up, body)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"result upload status {resp.status_code}")
    return {"blob_id": up.get("blob_id") or "", "size": len(body), "sha256": sha}


async def presign_download(nc: NATS, provider: str, blob_id: str) -> str | None:
    """A fresh GET URL for an archived blob key, or None."""
    try:
        reply = await nc.request(
            C.presign_download_subject(provider), json.dumps({"blob_id": blob_id}).encode(), timeout=10
        )
        up = json.loads(reply.data)
    except Exception as e:  # noqa: BLE001
        log.warning("presign download failed: %s", e)
        return None
    if up.get("error") or not up.get("url"):
        return None
    return str(up["url"])


async def fetch(nc: NATS, provider: str, url: str, fallback_handle: str) -> dict[str, Any]:
    """Download + decode an archived envelope, re-presigning from the stored
    blob key if the supplied URL is missing or expired.
    """

    async def _get(u: str) -> requests.Response:
        return await asyncio.to_thread(requests.get, u, timeout=30)

    resp = None
    if url:
        try:
            resp = await _get(url)
        except Exception as e:  # noqa: BLE001
            log.warning("blob GET failed, will re-presign: %s", e)
            resp = None
    if (resp is None or resp.status_code >= 400) and fallback_handle:
        fresh = await presign_download(nc, provider, fallback_handle)
        if not fresh:
            raise RuntimeError("could not presign archived blob")
        resp = await _get(fresh)
    if resp is None:
        raise RuntimeError("no usable blob URL")
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"blob GET status {resp.status_code}")
    env = json.loads(resp.content)
    if not isinstance(env, dict):
        raise TypeError("archived blob is not a JSON object")
    return env
