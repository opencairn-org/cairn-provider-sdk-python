"""Cairn provider worker SDK: the NATS runtime, blob I/O, credentials, OAuth,
rate limits and webhook helpers shared by Python workers. The generated
contract stubs ship alongside as `cairn.v1` and `cairn.worker.v1`.
"""

from __future__ import annotations

from .config import Identity, Settings, read_release, worker_name
from .creds import Credentials
from .errors import (
    CredentialsError,
    IncompatibleCoreError,
    ParserTooOldError,
    RetryLaterError,
    TerminalJobError,
    TokenError,
)
from .oauth import OAuthExchangeReply, OAuthExchangeRequest
from .ratelimit import RateLimiter
from .tokens import ClientCreds, TokenSource, TokenState
from .versions import parser_allows, release_compatible
from .webhooks import WebhookEvent, WebhookVerifyRequest, WebhookVerifyResponse
from .worker import Job, Worker, serve

__all__ = [
    "ClientCreds",
    "Credentials",
    "CredentialsError",
    "Identity",
    "IncompatibleCoreError",
    "Job",
    "OAuthExchangeReply",
    "OAuthExchangeRequest",
    "ParserTooOldError",
    "RateLimiter",
    "RetryLaterError",
    "Settings",
    "TerminalJobError",
    "TokenError",
    "TokenSource",
    "TokenState",
    "WebhookEvent",
    "WebhookVerifyRequest",
    "WebhookVerifyResponse",
    "Worker",
    "parser_allows",
    "read_release",
    "release_compatible",
    "serve",
    "worker_name",
]
