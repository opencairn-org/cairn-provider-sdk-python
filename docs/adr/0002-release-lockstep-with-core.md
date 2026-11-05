# 2. The SDK release follows cairn-core's major.minor

Status: accepted (2026-11-05). Applies cairn-core ADR 0005 ("Providers carry three versions:
release, dataset, parser") to a library.

## Context

cairn-core ADR 0005 gives a provider worker three versions. The **release** (`v0.7.0`, the git
tag) states the core contract the worker implements: proto messages, NATS subjects, job and
result shapes. Before 1.0 a worker is compatible with core when both share major and minor;
patch levels are free. From 1.0 on, minor and patch releases are backwards compatible: same
major, and the worker's minor not above core's. Core publishes its release in the `cairn_core`
KV bucket; a worker refuses to start, and stops, against an incompatible core. The **dataset**
version (`N`) and **parser** version (`N.M`) describe a worker's archive format and parsed
output; they belong to the worker, not to the SDK.

The SDK implements the contract part of that release: the subjects, the payloads it encodes
and decodes, the generated stubs, the ack discipline. A worker's release must therefore be
readable off the SDK it was built with.

## Decision

- The SDK's **major.minor equals the cairn-core major.minor whose contract it implements**:
  SDK 0.7.x speaks the 0.7 contract and ships the stubs generated from core 0.7's `api/proto`.
  A core minor release with a contract change gets an SDK minor release; before 1.0 every core
  minor release gets one, even without a contract change (core ADR 0005 requires the same
  minor on both sides before 1.0).
- The SDK's **patch** is free: fixes and additions that keep the contract and the public
  Python API compatible.
- A **breaking change in the SDK's own Python API** (removed or re-typed public names in
  `cairn_provider_sdk` or `cairn_provider_sdk.testing`) is only made together with a core
  major bump, or, before 1.0, with a minor bump that the CHANGELOG marks as breaking. The
  release target runs `griffe check` against the previous tag so a break cannot slip into a
  patch.
- A worker pins `cairn-provider-sdk` to the major.minor of the core release it targets
  (`~=0.7.0` style) and reports its own release tag, whose major.minor must match; the
  SDK's `release_compatible` check enforces that against the running core.
- Dataset and parser versions stay the worker's (`Identity.version`, `Identity.dataset_version`);
  the SDK never bumps them.

## Consequences

- The SDK version is a statement about the contract, not about its feature set: 0.7.3 may add
  helpers that 0.7.0 lacked, but both speak to core 0.7.
- Regenerating `cairn/` from a newer core is a minor release, never a patch.
- `make release` refuses a tag whose `pyproject.toml` version and CHANGELOG section disagree;
  the release workflow re-runs the full check, including the API comparison, before publishing.
