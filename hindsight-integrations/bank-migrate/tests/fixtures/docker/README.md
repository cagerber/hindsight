# Conformance test fixture (Docker)

Reproducible test environment for the Hindsight conformance suite.

**Why this exists**: upstream Hindsight doesn't ship a test pack. The
fixture gives us a self-contained daemon on port `18888` with
deterministic seed data, so conformance tests are reproducible and
isolated from your live environment.

## Layout

```
tests/fixtures/docker/
├── Dockerfile.fixture           # builds on hindsight:latest, adds seed+loader
├── docker-compose.test.yml      # host port 18888 → container 8888
├── seed_banks.yaml              # declarative fixtures (empty in baseline)
├── seed/loader.py               # applies fixtures to the live daemon
├── scripts/entrypoint.py        # daemon + seed + idle wait
└── README.md                    # this file
```

## Baseline vs change-specific

The baseline `seed_banks.yaml` ships **empty** (`banks: []`). Specific
fixtures (ODS cube pages, cli_builder conventions, lobby registry
entities) are added on top in change-specific branches.

To populate: append a block under `banks:` in `seed_banks.yaml` and
`make test-fixture-reseed`.

## Usage

### Build

```bash
make test-fixture-up    # also builds the image
```

### Test against it

```bash
HINDSIGHT_DAEMON_URL=http://127.0.0.1:18888 make test-conformance
```

### Tear down

```bash
make test-fixture-down
```

## Wiring into the conformance suite

The conformance tests are daemon-agnostic — they connect to whatever
`HINDSIGHT_DAEMON_URL` points at.

```bash
# Production daemon (default)
make test-conformance
# → http://127.0.0.1:9177 (your live daemon)

# Fixture daemon (hermetic, reproducible)
HINDSIGHT_DAEMON_URL=http://127.0.0.1:18888 make test-conformance
# → seeded fixture banks, isolated from production
```

## Hermetic testing (no LLM required)

The fixture as-built needs an LLM API key (the daemon's extraction
pipeline requires it). For truly hermetic testing, upstream would need
a "fake" LLM provider (it doesn't ship one). Two practical options:

1. **Mock the daemon at the test layer**: replace the HTTP client with
   a stub that returns canned responses. Faster, but doesn't test the
   real API surface.
2. **Use a small/fast LLM**: any openai-compatible endpoint with a
   cheap model (gpt-4o-mini, claude-haiku) works. The fixture's seed
   pipeline takes ~30s with a fast model.

## Adding fixtures

1. Add a block to `seed_banks.yaml` with a unique bank_id and document_ids.
2. Add a test in `tests/conformance/test_conformance.py` that asserts
   the new facts recall correctly.
3. `make test-fixture-reseed` to rebuild with the new fixtures.
4. Re-run the conformance suite.

## Cleanup

The fixture's `hindsight-fixture-data` volume persists between runs.
To wipe it: `make test-fixture-down` (which removes the volume) or
`docker volume rm hindsight-bank-migrate_hindsight-fixture-data`.
