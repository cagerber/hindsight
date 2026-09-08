# bank-migrate

A reference implementation of a downstream consumer tool for the
Hindsight memory daemon. Ships with:

- **A library** (`hindsight_bank_migrate/`) — typed Python client for
  the daemon's REST API, with operations for bank lifecycle, lobby
  resolution, registry, reflect, and relate.
- **A CLI** (`hindsight-bank-migrate`) — every operation is a subcommand.
- **An MCP server** (`hindsight-bank-migrate-mcp`) — exposes lobby-aware
  tools to coding-agent harnesses.
- **A conformance test suite** (`tests/conformance/`) — live-daemon
  tripwires that catch upstream breaking changes.
- **A hermetic test fixture** (`tests/fixtures/docker/`) — dockerised
  daemon + deterministic seed data for reproducible testing.

## Why this exists

Upstream Hindsight exposes a rich REST API but ships no client library,
no test pack, and no fixture bundle. This package fills that gap for
downstream consumers who want a typed, testable interface to the
daemon's bank lifecycle, lobby, and reflect primitives.

The package lives under `hindsight-integrations/bank-migrate/` because
it's a downstream tool — it lives alongside the other integrations
(`hindsight-integrations/coding-agents/`, etc.) and is released on the
same cadence as upstream Hindsight itself.

## Quick start

```bash
# Install (uses uv)
cd hindsight-integrations/bank-migrate
uv sync

# Run unit tests
make test

# Run conformance tests against your live daemon (defaults to 127.0.0.1:9177)
make test-conformance

# Or against the docker fixture (hermetic, reproducible)
make test-fixture-up
HINDSIGHT_DAEMON_URL=http://127.0.0.1:18888 make test-conformance
make test-fixture-down
```

## Layout

```
hindsight-integrations/bank-migrate/
├── hindsight_bank_migrate/     # library (cli, daemon, registry, reflect, relate, mcp_server)
├── tests/
│   ├── test_*.py              # hermetic unit tests
│   ├── conformance/           # live-daemon conformance tests (openapi tripwire etc.)
│   └── fixtures/docker/       # Docker fixture (daemon + seed loader)
├── pyproject.toml
├── Makefile
├── AGENTS.md
└── CHANGELOG.md
```

## Subcommands (CLI)

| Subcommand | What it does |
|---|---|
| `merge` | Export + import: merge one bank into another |
| `rename` | Rename a bank across all references |
| `split` | Split a bank by tag/scope into N target banks |
| `diff` | Compare two banks (document set, fact counts) |
| `register` | Upsert a registry entity (lobby) |
| `lobby` | Resolve cwd → entity → bank mapping |
| `relate` | Rank entities by query relevance |
| `reflect` | Run daemon reflect, optionally via lobby fan-out |
| `list-banks`, `diagnose`, `scope-list`, `lifecycle`, `tag`, `sync-status` | Read-only introspection |

## MCP tools

The `hindsight-bank-migrate-mcp` server exposes lobby-aware tools so
coding-agent harnesses can call our operations without rebuilding the
identity hierarchy (bank_id > entity > cwd > PWD) on their side.

See `tests/conformance/test_conformance.py::test_mcp_tool_invoke_each_tool`
for the canonical list.

## Test pack

This package ships a two-layer test pack:

1. **Unit tests** (`tests/test_*.py`) — hermetic, no daemon required.
2. **Conformance tests** (`tests/conformance/`) — live-daemon tripwires
   that assert every endpoint we call is still present with the expected
   request/response shape. Catches upstream breaking changes the day
   they ship.

Conformance tests are tagged with `@pytest.mark.conformance` and excluded
from default `make test`. Run them explicitly:

```bash
make test-conformance                          # against your live daemon
HINDSIGHT_DAEMON_URL=http://... make test-conformance  # against any daemon
```

## Test fixture (Docker)

`tests/fixtures/docker/` is a hermetic test environment:

- Builds on `ghcr.io/vectorize-io/hindsight:latest`
- Adds a Python seed loader that applies declarative fixtures at startup
- Maps the daemon to host port `18888` (NOT `9177` — that's your live daemon)
- Idempotent: skips banks that already have facts

```bash
make test-fixture-up        # start the fixture (host port 18888)
make test-fixture-down      # tear down + wipe pg0 volume
make test-fixture-reseed    # force a fresh seed
```

The fixture's `seed_banks.yaml` ships empty (`banks: []`) in the
baseline branch. Specific fixtures (ODS cubes, cli_builder conventions,
lobby registry) are added on top in the change-specific branches.

## Versioning

This package tracks upstream Hindsight. Versions are tagged in lockstep
with Hindsight releases — when upstream ships `v0.9.3`, this package's
`CHANGELOG.md` notes any breaking changes caught by the conformance
suite.
