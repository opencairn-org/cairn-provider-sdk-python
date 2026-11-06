# Cairn provider SDK for Python

The runtime shared by Python [Cairn](https://github.com/opencairn-org/cairn-core)
provider workers. A worker imports activities, gear and health metrics from
one provider (Garmin, a file drop, ...) into a Cairn server; the SDK speaks the
[provider contract](https://docs.opencairn.org/architecture/provider-contract)
so the worker only writes the provider-specific parts: listing and fetching,
mapping to the `cairn.worker.v1` messages, and credentials.

What the SDK does for a worker:

- connects to NATS with the enrollment token (core's auth-callout), refuses
  to run against a core of another major.minor, heartbeats presence and
  publishes the manifest with `provider_meta`
- one durable pull consumer per job kind with ack extension, bounded retries,
  failure envelopes, `parser_too_old` hand-back and rate-limit retries
- claim-checked results, raw archives and re-parse downloads through core's
  presigned URLs
- `cairn.creds.*` for credential providers; `cairn.tokens.*` with a cached,
  cross-instance-locked refresh and an OAuth code-exchange responder for
  OAuth providers
- a KV-backed rate limiter sharing one budget per provider app across
  instances and languages, and webhook HMAC helpers
- a test kit: in-memory fakes for handler tests and a real `nats-server`
  with a fake core for integration tests

The generated contract stubs (`cairn.v1`, `cairn.worker.v1`) ship in the
distribution, so a worker no longer vendors them.

## Install

The package is not on PyPI. Every [GitHub release](https://github.com/opencairn-org/cairn-provider-sdk-python/releases)
carries the wheel the release workflow built, signed and attested; depend on it:

```toml
[project]
dependencies = ["cairn-provider-sdk>=0.7.1,<0.8"]

[tool.uv.sources]
cairn-provider-sdk = { url = "https://github.com/opencairn-org/cairn-provider-sdk-python/releases/download/v0.7.1/cairn_provider_sdk-0.7.1-py3-none-any.whl" }
```

`uv lock` records the wheel's hash. A git source (`{ git = "…", tag = "v0.7.1" }`)
works too but needs git wherever the worker is built.

Python 3.14; runtime dependencies are `nats-py`, `protobuf` and `requests`.

## A worker in short

```python
from cairn_provider_sdk import Identity, Job, Settings, Worker, serve

IDENTITY = Identity(provider="acme", package="cairn.workers.acme", version="1.0", dataset_version=1)


class AcmeWorker(Worker):
    def __init__(self) -> None:
        super().__init__(IDENTITY, Settings(), manifest=MANIFEST, provider_meta=PROVIDER_META)
        self.handle("fetch_source", self.fetch_source)
        self.handle("reconcile", self.reconcile)
        self.handle("parse_blob", self.parse_blob)
        self.on_discover(self.discover)

    async def fetch_source(self, job: Job) -> None:
        creds = await self.fetch_creds(job.data["account_id"])
        raw = await api.get(creds, job.data["ext_id"])
        env = self.envelope(activity=raw)
        blob_id, size = await self.archive(job.data["user_id"], job.data["ext_id"], env)
        event = map_activity(env, blob_id=blob_id or "")
        await self.publish_result(self.identity.result_subject("fetch_source"), self.job_result([event]))


serve(AcmeWorker())
```

[examples/minimal](examples/minimal/acme_worker.py) is the complete version
of this worker, with its tests; [docs/writing-a-worker.md](docs/writing-a-worker.md)
walks through every part of the contract the SDK covers and what a worker
still owns.

## Versioning

The SDK's **major.minor tracks cairn-core** ([ADR 0002](docs/adr/0002-release-lockstep-with-core.md),
core's ADR 0005): SDK 0.7.x implements the 0.7 worker contract, and a worker
built on it reports a release of the same major.minor. Patch releases are
free. The public API is checked against the previous tag with `griffe check`
on every release; a breaking change in the Python API bumps the major.

## Development

```bash
make tools        # uv sync --locked, fleetlint, nats-server
make check-fast   # ruff + unit tests, before every commit
make check        # lint, tests (incl. integration), coverage, audit, API check, build, fleetlint
make proto        # regenerate cairn/ from ../cairn-core/api/proto
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Security reports: [SECURITY.md](SECURITY.md).

## License

Apache-2.0.
