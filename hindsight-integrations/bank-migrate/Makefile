.PHONY: test test-unit test-conformance test-conformance-fixture \
        test-fixture-up test-fixture-down test-fixture-reseed upstream-check install

# Default: unit tests only (fast, hermetic).
test:
	uv run pytest -q

test-unit:
	uv run pytest -q

# Conformance tests — requires a live Hindsight daemon at HINDSIGHT_DAEMON_URL
# (defaults to http://127.0.0.1:9177). Use before every upstream upgrade.
test-conformance:
	uv run pytest -m conformance -v

# Conformance tests against the docker fixture (hermetic, reproducible).
test-conformance-fixture:
	HINDSIGHT_DAEMON_URL=http://127.0.0.1:18888 uv run pytest -m conformance -v

# Start the docker test fixture (host port 18888).
test-fixture-up:
	docker compose -f tests/fixtures/docker/docker-compose.test.yml up --build -d

# Tear down + wipe pg0 volume.
test-fixture-down:
	docker compose -f tests/fixtures/docker/docker-compose.test.yml down
	docker volume rm hindsight-bank-migrate_hindsight-fixture-data 2>/dev/null || true

# Force a fresh seed on the fixture.
test-fixture-reseed:
	docker compose -f tests/fixtures/docker/docker-compose.test.yml down
	docker volume rm hindsight-bank-migrate_hindsight-fixture-data 2>/dev/null || true
	SEED_FORCE=true docker compose -f tests/fixtures/docker/docker-compose.test.yml up --build -d

install:
	uv sync
