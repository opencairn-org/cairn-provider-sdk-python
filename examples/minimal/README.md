# Minimal worker: the "acme" provider

A complete worker for a fake provider with an in-memory API, showing what a
credential-based activity provider needs: identity and manifest, discover,
`fetch_source` with archive and claim-checked result, `reconcile` with
window, coalescing and sub-jobs, `parse_blob` from the archive, and the
credentials RPC.

```bash
# against a dev core (its NATS, no enrollment token)
CAIRN_NATS_URL=nats://localhost:4222 uv run python examples/minimal/acme_worker.py

# the tests: handlers against the fakes, and end to end on a real nats-server with a fake core
uv run pytest examples/minimal
```

`test_acme_worker.py` is also the template for a worker's own test suite.
