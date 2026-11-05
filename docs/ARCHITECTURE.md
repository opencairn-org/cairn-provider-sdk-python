# Architecture

cairn-provider-sdk-python is a library: the runtime a Python Cairn provider
worker builds on. A worker is a long-running process that imports one
provider's activities, gear and health metrics into a Cairn server; it talks
to the server only over NATS and to object storage only through URLs the
server presigns. The SDK implements the mechanical parts of the
[provider contract](https://docs.opencairn.org/architecture/provider-contract);
the worker supplies the provider-specific parts. [docs/writing-a-worker.md](writing-a-worker.md)
is the author's guide.

## Components

- **`cairn_provider_sdk/worker.py`** — the `Worker`: NATS connection, core
  release check, presence heartbeat, manifest publish, the `discover`,
  OAuth-exchange and webhook-verify responders, one JetStream pull consumer
  per registered job kind with ack extension and bounded retries, result and
  failure publishing, reconcile sub-jobs. `serve()` wires signals.
- **`config.py`** — `Identity` (provider, package, parser and dataset version,
  release, pool name) and `Settings` (NATS connection from the environment);
  the subject layout of the contract (per pool, core ADR 0006).
- **`archive.py`** — raw-response archival, claim-check uploads of results and
  downloads of archived envelopes through presigned URLs.
- **`creds.py`**, **`tokens.py`**, **`oauth.py`** — credentials from core,
  OAuth token state with cached refresh under a cross-instance KV lease, and
  the token-endpoint grants (code exchange, refresh).
- **`ratelimit.py`** — the shared KV budget, byte-compatible with the Go
  worker's limiter.
- **`reconcile.py`**, **`versions.py`**, **`webhooks.py`**, **`rfc3339.py`** —
  pure helpers: listing window and coalescing, release and parser rules,
  HMAC signatures, Go-compatible timestamps.
- **`cairn/`** — generated protobuf modules and type stubs of the contract
  (`cairn.v1`, `cairn.worker.v1`), shipped in the wheel so workers do not
  vendor them; `scripts/sync-proto.sh` regenerates them from cairn-core.
- **`testing/`** — in-memory fakes for handler tests and the integration kit:
  a real `nats-server`, core's streams and buckets, a fake core answering the
  server-side subjects, an in-memory blob store.

## Boundaries

- The only contract with the server is the proto messages plus the NATS
  subjects in `config.py`. There is no code dependency on cairn-core; the
  stubs are regenerated from its `api/proto` on a contract change.
- Provider knowledge stays in the worker: listing, fetching, mapping, the
  token endpoint, signature schemes, rate-limit capacities. The SDK carries
  no provider names.
- Credentials and tokens are fetched from core per account and cached only in
  memory; blob storage is reached only via presigned URLs.
- The worker-writable NATS surface is core's standard permission template:
  the pool's job, result and lookup subjects, `cairn.tokens/creds/blobs.*`,
  and the presence, manifest and rate-limit KV buckets. The refresh lock
  lives in the rate-limit bucket because it is the one writable bucket with
  a TTL.

## Data flow

1. Core publishes a job on `cairn.jobs.<kind>.<provider>.<pool>` in
   `CAIRN_JOBS`; the pool's durable consumer delivers it to the registered
   handler as a `Job`, with its ack deadline extended until settlement.
2. The handler fetches credentials or a token through the SDK, calls the
   provider, archives the raw inputs, maps them to `WorkerEvent`s.
3. `publish_result` uploads event-carrying results to the blob store and
   publishes a `payload_ref` envelope on `cairn.results.<kind>.<provider>.<pool>`;
   event-free results publish inline. Reconcile results are account-suffixed.
4. A failing job is NAKed with a delay and terminated after five deliveries
   with a failure envelope carrying the real error; `parser_too_old` hands a
   re-parse back to a newer pool instance first.

## Decisions

- **One runtime for every Python worker** (extracted from the Garmin worker):
  the contract's mechanics are identical across providers and change only
  with core, so they are versioned with core (ADR 0002) and tested once.
- **Committed generated stubs in the distribution.** A worker pins the
  contract through the SDK version; regenerating is an explicit minor
  release, and workers build without buf or cairn-core.
- **Claim-check for results.** Activity streams can exceed the broker's
  `max_payload`; uploading the body and publishing a reference keeps every
  message small regardless of activity size.
- **Cross-instance refresh lock by KV compare-and-set.** Pooled instances
  share single-use and rotating refresh tokens; a lease in a TTL bucket
  serialises the refresh without a coordinator and heals itself.
