# Changelog

All notable changes to the Cairn provider SDK for Python are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/), and the
project follows [Semantic Versioning](https://semver.org/) with the release
rule of [docs/adr/0002](docs/adr/0002-release-lockstep-with-core.md): the
major.minor tracks cairn-core. Dates are ISO-8601.

## [Unreleased]

### Fixed
- Integration tests: each test runs against its own `nats-server` with core's
  streams and buckets bootstrapped; three tests passed only after the first
  one had created them (failed alone or in random order).
- `make mutate`: the testing kit is excluded again (`testing/*`) and
  `examples/` and the generated `cairn/` package are copied next to the
  mutants, so the run gets past its baseline test run.

## [0.7.0] — 2026-11-05

First release, extracted from cairn-provider-garmin 0.7.0; implements the
cairn-core 0.7 worker contract.

### Added
- `Worker`: NATS connection (enrollment token or user/password), the core
  release check (ADR 0005), presence heartbeat, manifest publish with
  `provider_meta`, one durable pull consumer per registered job kind
  (`fetch_source`, `parse_blob`, `backfill`, `reconcile`, `import_metrics`,
  `import_gear`) with ack extension, bounded retries, `parser_too_old`
  back-off and `RetryLaterError`, the `discover`, OAuth-exchange and webhook
  verify responders, and the webhook event consumer.
- Results: claim-checked `publish_result`, `publish_failure` envelopes,
  `publish_reconcile_result`, `enqueue_fetch` with deterministic msg-ids.
- `archive`: envelope, best-effort archive, result upload, download with
  re-presign. `creds`: `cairn.creds.<provider>.get`. `reconcile`: window,
  watermark and coalescing helpers. `versions`: release and parser rules.
- `TokenSource` over `cairn.tokens.*` with an in-process cache, worker-driven
  refresh and a cross-instance refresh lock in the `cairn_rate_limits` KV
  bucket (`lock.<provider>.<account_id>`, compare-and-set lease).
- `oauth`: `exchange_code` (body or Basic client auth, PKCE verifier, extra
  parameters, account id from the response or a lookup) and `refresh_grant`.
- `RateLimiter`: the Go worker's KV bucket semantics (`<provider>:<app_id>:<window>`,
  CAS reservations, `sync_usage`, `force_refill`) with per-second, minute,
  quarter-hour, hour and day windows plus custom ones.
- `webhooks`: constant-time HMAC sign/verify (hex/base64, body or
  timestamp+body) and the verify wire types.
- `cairn_provider_sdk.testing`: in-memory fakes (`FakeNC`, `FakeJS`, `FakeKV`,
  `FakeMsg`, `FakePullSub`, `make_worker`) and an integration kit
  (`NatsServer`, `bootstrap_core`, `FakeCore`, `BlobStore`).
- The generated contract stubs `cairn.v1` and `cairn.worker.v1` ship in the
  distribution; `scripts/sync-proto.sh` regenerates them from cairn-core.
- `examples/minimal`: a runnable worker for a fake provider, tested end to end.
