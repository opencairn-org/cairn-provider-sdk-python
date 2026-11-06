"""Test kit: in-memory fakes for unit tests and a real nats-server with a
fake core for integration tests. Not imported by the runtime.
"""

from __future__ import annotations

from .fakes import (
    FakeJS,
    FakeKV,
    FakeMsg,
    FakeNC,
    FakePullSub,
    Responder,
    as_nats,
    example_identity,
    example_settings,
    make_worker,
)
from .integration import (
    BlobStore,
    FakeAccount,
    FakeCore,
    NatsServer,
    Result,
    bootstrap_core,
    nats_server_binary,
    token_fields,
    until,
)
from .validate import assert_importable, result_problems, validate_activity, validate_metric

__all__ = [
    "BlobStore",
    "FakeAccount",
    "FakeCore",
    "FakeJS",
    "FakeKV",
    "FakeMsg",
    "FakeNC",
    "FakePullSub",
    "NatsServer",
    "Responder",
    "Result",
    "as_nats",
    "assert_importable",
    "bootstrap_core",
    "example_identity",
    "example_settings",
    "make_worker",
    "nats_server_binary",
    "result_problems",
    "token_fields",
    "until",
    "validate_activity",
    "validate_metric",
]
