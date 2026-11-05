# Writing a worker

A Cairn provider worker is a long-running Python process that talks to the
server only over NATS and to object storage only through URLs the server
presigns. The SDK implements the mechanical parts of the
[provider contract](https://docs.opencairn.org/architecture/provider-contract);
this page walks through what the worker author writes, in the order the
contract lists it. [examples/minimal/acme_worker.py](../examples/minimal/acme_worker.py)
is the complete runnable version.

## 1. Identity, settings, manifest

```python
from pathlib import Path
from cairn_provider_sdk import Identity, Settings, Worker, read_release, worker_name

IDENTITY = Identity(
    provider="acme",  # the enrollment's provider
    package="cairn.workers.acme",  # stable build identity
    version="1.2",  # parser N.M: dataset N, output revision M
    dataset_version=1,  # the archive envelope format this parser reads
    release=read_release(Path(__file__).with_name("RELEASE")),  # vX.Y.Z, written at build time
    worker_name=worker_name("acme-fetcher"),  # CAIRN_WORKER_NAME, the pool's routing name
)
```

`Settings()` reads the NATS connection from the environment (`CAIRN_NATS_URL`,
`CAIRN_WORKER_ENROLLMENT_TOKEN` or `CAIRN_NATS_USER`/`CAIRN_NATS_PASSWORD`,
`CAIRN_WORKER_INSTANCE_ID`, `CAIRN_HEARTBEAT_INTERVAL_S`, `CAIRN_PULL_BATCH`).
Extend it with the worker's own fields by dataclass inheritance.

The **manifest** declares the data types the worker supplies (canonical
`DataType` keys with `read`/`backfill`/`granularity`); the **provider meta**
declares how the provider is connected: `auth_method` `credentials` (core
delivers the connection's login pair on `cairn.creds.<provider>.get`) or
`oauth2` (core runs the authorize round-trip from the `oauth` block and asks
the pool to exchange the code), plus display name, activity link, docs link
and the labels of the two credential slots. The SDK publishes both: the
manifest in every presence heartbeat, the meta in the manifest KV every
10 minutes.

```python
worker = Worker(IDENTITY, Settings(), manifest=MANIFEST, provider_meta=PROVIDER_META)
```

Subclassing `Worker` (as the example does) keeps handlers and helpers on one
object; composition works just as well.

## 2. Handlers

Register one handler per job kind the pool consumes. The contract requires
`fetch_source` and `reconcile`; `parse_blob` when raw blobs are archived;
`import_metrics` when the manifest declares a metric type; `import_gear` when
it declares `Gear`. The SDK creates one durable pull consumer per registered
kind, named `<worker_name>-<suffix>` (stable across releases).

```python
worker.handle("fetch_source", fetch_source)
worker.handle("reconcile", reconcile)
worker.handle("parse_blob", parse_blob)
worker.on_discover(discover)
```

A handler receives a `Job` (`job.data` is the parsed JSON body, `job.delivered`
the delivery count) and settles it by how it returns:

| Handler outcome | Settlement |
|---|---|
| returns | ack |
| raises `TerminalJobError` | failure envelope published, term |
| raises `ParserTooOldError` | nak with growing delay; failure + term after 10 deliveries |
| raises `RetryLaterError(delay_s)` | nak with that delay (rate limits) |
| raises anything else | nak after 60 s; failure + term after 5 deliveries |

Every fetched message's ack deadline is extended from arrival until its
settlement, so slow handlers and queued batch messages never fall into
redelivery loops.

### fetch_source

Fetch one activity, archive every input the parser reads, map, publish:

```python
async def fetch_source(job: Job) -> None:
    d = job.data
    token = await creds_for(d["account_id"])
    raw = await asyncio.to_thread(api.get, token, d["ext_id"])
    env = worker.envelope(activity=raw)  # version + provenance + inputs
    blob_id, size = await worker.archive(d["user_id"], d["ext_id"], env)  # best-effort
    event = map_activity(env, ..., blob_id=blob_id or "")
    await worker.publish_result(worker.identity.result_subject("fetch_source"), worker.job_result([event]))
```

`publish_result` claim-checks event-carrying results (the body goes to the
blob store, a `payload_ref` envelope travels over NATS) and publishes
event-free results inline. `job_result` stamps name, parser version, package
and release.

### reconcile

List what is new since the watermark inside the bounded window, enqueue
`fetch_source` sub-jobs, and always publish the account-suffixed result:

```python
coalescer = reconcile.Coalescer()


async def reconcile_job(job: Job) -> None:
    d = job.data
    if not coalescer.should_run(d["account_id"]):
        return  # backlog: ack without API calls
    start = reconcile.window_start(reconcile.parse_watermark(d.get("watermark")))
    high_water = None
    for act in newer_than(start, limit=reconcile.max_enqueue(d)):
        if act.id not in set(d.get("known_ext_ids") or []):
            await worker.enqueue_fetch(d, act.id)  # msg-id reconcile:<provider>:<account>:<ext_id>
            high_water = max(high_water or act.start, act.start)
    await worker.publish_reconcile_result(d, high_water)
```

### parse_blob

Re-parse from the archive, no provider call. Hand back jobs a newer parser
owns, refuse other datasets:

```python
async def parse_blob(job: Job) -> None:
    d = job.data
    if not parser_allows(IDENTITY.version, d.get("min_parser_version", "")):
        raise ParserTooOldError("a newer instance takes it")
    env = await worker.fetch_blob(d["blob"].get("url", ""), d["blob"]["fallback_handle"])
    if env.get("version") != IDENTITY.dataset_version:
        raise TerminalJobError("archive of another dataset")
    ...
```

`fetch_blob` re-presigns from the fallback handle when the URL expired.

### discover

```python
async def discover(req: dict[str, Any]) -> dict[str, Any]:
    ...
    return {"items": [{"item_type": "activity", "external_id": "7", "item_time": "<RFC3339>"}], "next_page": 2}
```

Return `{"items": [], "complete": True}` at the end, `"rate_limited": True`
mid-walk when the budget is exhausted; exceptions become `{"error": "..."}`.
Never return an empty success for a failure.

## 3. Credentials

Credential providers ask core per account: `await worker.fetch_creds(account_id)`
returns `Credentials(client_id, client_secret)` or raises `CredentialsError`.
Cache the resulting client, never the credentials.

OAuth providers use `worker.tokens` (a `TokenSource`): `await worker.tokens.get(account_id)`
returns a `TokenState` valid for at least 60 s, fetching from
`cairn.tokens.<provider>.get` and refreshing through the worker's refresh
function when needed:

```python
from cairn_provider_sdk.oauth import refresh_grant


async def refresh(state: TokenState, creds: ClientCreds) -> TokenState:
    return await refresh_grant("https://provider/oauth/token", state, creds, client_auth="basic")


worker = Worker(IDENTITY, Settings(), manifest=..., provider_meta=..., refresh=refresh)
```

The refresh is serialised per account inside the process and across
instances: before refreshing, the source takes a lease
`lock.<provider>.<account_id>` in the `cairn_rate_limits` KV bucket by
compare-and-set (30 s, re-taken when expired), re-reads the server state
(another instance may just have stored a fresh token), refreshes, stores
with `previous_expires_at` so core detects a lost race, and releases. A
terminal refresh failure (`invalid_grant`, `needs_reauth`) flags the account
through `cairn.tokens.<provider>.needs_reauth` and raises `TokenError`.

The code exchange for new connections is answered by the pool:

```python
from cairn_provider_sdk.oauth import exchange_code


async def exchange(req: OAuthExchangeRequest) -> OAuthExchangeReply:
    return await exchange_code(
        req,
        "https://provider/oauth/token",
        client_auth="basic",
        account_id_from=lambda body: str(body["athlete"]["id"]),
    )


worker.on_oauth_exchange(exchange)
```

`exchange_code` sends the PKCE verifier core hands over when the meta
declares `pkce`, adds `extra_params`, and can fetch the provider account id
with a follow-up call (`account_id_lookup`) for providers whose token
response lacks it.

## 4. Rate limits

Budgets are shared per provider app across every instance (and the Go
workers) in the `cairn_rate_limits` bucket:

```python
limiter = await worker.rate_limiter({bucket("acme", app_id, "minute"): 300, bucket("acme", app_id, "day"): 10_000})

ok, retry_after = await limiter.reserve(bucket("acme", app_id, "minute"))
if not ok:
    raise RetryLaterError(retry_after, "acme minute budget")
resp = call_provider()
await limiter.sync_usage(name, used=int(resp.headers["X-Used"]), limit=int(resp.headers["X-Limit"]), resets_at=...)
```

`app_id` is the token reply's `app_id` (a hash of the connection's client
id) or, without one, the pool's worker name. The window is the bucket's
suffix: `second`, `minute`, `quarter_hour`/`short`, `hour`, `day`/`daily`,
or any custom suffix given in `windows=`. Unknown buckets are permissive.
On a 429, `force_refill(name, available, resets_at)` anchors every instance
to the provider's reset time.

## 5. Webhooks

Core forwards `POST /webhooks/<worker_key>` bodies to the pool and relays
`GET` handshakes:

```python
from cairn_provider_sdk import webhooks


async def on_event(ev: WebhookEvent) -> None:
    if not webhooks.verify(SECRET, ev.body, ev.headers.get("x-signature", ""), prefix="sha256="):
        raise TerminalJobError("bad signature")
    who = await worker.lookup_account(json.loads(ev.body)["owner_id"])
    await worker.enqueue_fetch({**who, "job_id": f"webhook:{ev.delivered}"}, ext_id, reason="webhook")


async def on_verify(req: WebhookVerifyRequest) -> WebhookVerifyResponse:
    return WebhookVerifyResponse(body=json.dumps({"hub.challenge": req.query["hub.challenge"]}).encode())


worker.on_webhook_event(on_event)
worker.on_webhook_verify(on_verify)
```

`webhooks.sign`/`verify` are constant-time HMACs over the body or
`timestamp + separator + body`, hex or base64; `parse_signature_header`
splits `t=...,v1=...` headers.

## 6. Running and testing

`serve(worker)` runs until SIGINT/SIGTERM. The worker refuses to start, and
stops, against a core of another major.minor (`IncompatibleCoreError`).

Handler tests use the in-memory fakes:

```python
from cairn_provider_sdk.testing import FakeNC, make_worker

wk, js = make_worker(AcmeWorker(api), FakeNC({IDENTITY.creds_subject: {"client_id": "me", "client_secret": "t"}}))
await wk.fetch_source(Job.of({"account_id": "a", "user_id": "u", "ext_id": "7"}))
subject, body, headers = js.published[0]
```

Integration tests run the real thing:

```python
from cairn_provider_sdk.testing import FakeCore, NatsServer

with NatsServer() as srv:  # nats-server -js on a free port
    core = await FakeCore.start(srv.url, provider="acme", release="v0.7.0")  # core's streams, buckets, responders
    core.account("acc-1", client_id="me", client_secret="t")
    run = asyncio.create_task(worker.run())
    await core.wait_for_consumers(IDENTITY, ["fetch_source", "reconcile", "parse_blob"])
    await core.publish_job(IDENTITY, "reconcile", {"job_id": "j1", "account_id": "acc-1", "user_id": "u1"})
    result = await core.await_result("fetch_source")  # payload_ref resolved from the blob store
```

`FakeCore` answers creds, tokens (get/store/needs_reauth with the stale-store
rule), presign upload/download over an in-memory `BlobStore`, and account
lookup; it collects results and can `discover`, `exchange_code`,
`verify_webhook` and `forward_webhook` like core does. cairn-core's
`cmd/worker-conformance` checks a running worker from the outside.

## 7. Versions and releases

The worker carries three versions (core ADR 0005): its **release** tag
(major.minor equal to core's; the SDK's major.minor follows the same rule,
[ADR 0002](adr/0002-release-lockstep-with-core.md)), the **dataset**
version of its archive envelope, and the **parser** version `N.M` reported
as `worker_version`. Bump `M` on every change to the parsed output and pin
the output per parser version with a golden-file test; bump `N` only when the
parser can no longer read the previous envelope.
