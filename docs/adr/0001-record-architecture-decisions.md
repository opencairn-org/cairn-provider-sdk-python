# 1. Record architecture decisions

Status: accepted

## Context

The SDK's structural choices (one runtime for every Python worker, the committed generated
stubs, NATS-only transport, presigned object storage, the release rule shared with core) are
otherwise described as prose in the README and docs/writing-a-worker.md, which drifts.

## Decision

Significant architecture decisions are recorded here as numbered ADRs (context, decision,
consequences). An ADR is never rewritten after acceptance; a later ADR supersedes it.

## Consequences

Structural changes come with an ADR in the same pull request. Decisions taken in cairn-core
that bind the SDK (the worker contract, core's ADR 0005 and 0006) are referenced, not copied,
except where the SDK adds its own rule on top (ADR 0002).
