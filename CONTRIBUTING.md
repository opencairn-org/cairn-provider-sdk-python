# Contributing to cairn-provider-sdk-python

Contributions are welcome. For anything beyond a small fix, open an issue first
so the direction is agreed before the work starts.

## License of contributions

The project is Apache-2.0. By submitting a contribution you agree it is licensed
under the same terms (Section 5 of the Apache License). There is no CLA.

Sign off every commit (`git commit -s`) to assert the
[Developer Certificate of Origin](https://developercertificate.org/).

## Setup

Prerequisites: [uv](https://docs.astral.sh/uv/), `make`, Go (to install
fleetlint and nats-server) and [gitleaks](https://github.com/gitleaks/gitleaks/releases).

```bash
make tools        # uv sync --locked (Python and all dev tools from uv.lock), fleetlint, nats-server
prek install      # git hooks from .pre-commit-config.yaml
```

## Working rules

- `make check` must pass: ruff format and lint, mypy strict, the tests with the
  coverage floor from `pyproject.toml` (the integration tests start a real
  `nats-server`), pip-audit on the locked dependencies, the runtime license
  allowlist, deptry, `uv lock --check`, gitleaks, `griffe check` against the
  previous tag, the wheel build with an import smoke test, and fleetlint. CI
  runs the same target. `make check-fast` is the quick loop before a commit.
- Dependencies change through `uv add` / `uv lock`; commit `pyproject.toml` and
  `uv.lock` together.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/);
  the commit-msg hook enforces it. `main` is changed through pull requests only.
- `cairn/` is generated from the Cairn proto contract by `make proto`
  (`scripts/sync-proto.sh`); never edit it by hand, and regenerate the
  `_pb2.py` modules and their `.pyi` stubs together.
- The public API is `cairn_provider_sdk` and `cairn_provider_sdk.testing`.
  A breaking change needs a new major version (`make api-check` compares
  with the previous tag) and a CHANGELOG entry.
- Behaviour that is part of the worker contract (subjects, payloads, ack
  discipline, claim-check) changes only with the contract in cairn-core;
  cite the contract section in the commit.

## Releases

The major.minor follows cairn-core (docs/adr/0002). Update the version in
`pyproject.toml` and add a `## [X.Y.Z]` section to `CHANGELOG.md`, then on
`main` run `make release V=vX.Y.Z` and push the tag. The release workflow
re-runs `make check`, builds the wheel and sdist and creates the GitHub
release with the CHANGELOG section as notes and the build as assets.
