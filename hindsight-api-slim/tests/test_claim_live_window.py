"""Live work before backfill, and a per-bank hourly backfill budget (A3, ingest mitigations).

With ``live_window_hours`` set, the claim takes operations queued within the window ahead of an
older backlog that is already due, so a session's write does not wait behind a September import.
``backfill_excluded_banks`` (worked out by the poller from ``backfill_budget_spent_banks``) skips a
bank's retains queued before the window once its hourly budget is spent. Both default off.

The fixtures mirror ``test_claim_bank_fairness.py`` on a private schema of their own.
"""

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

# Use loadgroup to ensure these tests run in the same worker
# since they share database state
pytestmark = pytest.mark.xdist_group("worker_tests")

_TABLE = "async_operations"


@pytest.fixture(scope="session")
def isolated_ops_schema(pg0_db_url):
    """A private, migrated Postgres schema for this file's claim tests.

    ``ops.claim_tasks`` scans the whole schema on the connection's search_path,
    and every test here asserts on an *exact* claim, so another pytest-xdist
    worker's pending rows would make them meaningless. One schema per worker,
    created + migrated once and dropped at session end — the same isolation
    ``test_claim_bank_serialization.py`` uses, under its own name so the two
    files never share rows.
    """
    from hindsight_api.engine.db import create_database_backend
    from hindsight_api.pg0 import resolve_database_url

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    schema = f"livewin_iso_{worker}"

    async def _provision() -> str:
        url = await resolve_database_url(pg0_db_url)
        b = create_database_backend("postgresql")
        await b.initialize(url, min_size=1, max_size=2)
        try:
            async with b.get_pool().acquire() as conn:
                # Rebuild from scratch so a schema left by a crashed prior run
                # can't carry stale state into this session.
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                await conn.execute(f'CREATE SCHEMA "{schema}"')
            b.run_migrations(url, schema=schema)
        finally:
            await b.shutdown()
        return url

    async def _drop(url: str) -> None:
        b = create_database_backend("postgresql")
        await b.initialize(url, min_size=1, max_size=2)
        try:
            async with b.get_pool().acquire() as conn:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await b.shutdown()

    loop = asyncio.new_event_loop()
    try:
        url = loop.run_until_complete(_provision())
    finally:
        loop.close()

    yield schema

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_drop(url))
    finally:
        loop.close()


@pytest_asyncio.fixture
async def backend(pg0_db_url, isolated_ops_schema):
    """Create a DatabaseBackend whose pool is pinned to this file's private schema."""
    from hindsight_api.engine.db import create_database_backend
    from hindsight_api.pg0 import resolve_database_url

    resolved_url = await resolve_database_url(pg0_db_url)

    async def _use_isolated_schema(conn):
        await conn.execute(f'SET search_path TO "{isolated_ops_schema}", public')

    b = create_database_backend("postgresql")
    await b.initialize(resolved_url, min_size=2, max_size=10, command_timeout=30, init_callback=_use_isolated_schema)
    yield b
    await b.shutdown()


@pytest_asyncio.fixture
async def pool(backend):
    """Expose the raw asyncpg pool from the backend for direct DB access in tests."""
    yield backend.get_pool()


@pytest_asyncio.fixture
async def clean_operations(pool):
    """Clear leftovers from a prior test in this group.

    Safe to be broad: the pool is pinned to this file's private schema, so this
    only ever touches its own rows.
    """
    await pool.execute(f"DELETE FROM {_TABLE}")
    yield
    await pool.execute(f"DELETE FROM {_TABLE} WHERE bank_id LIKE 'test-livewin-%'")


async def _make_bank(pool) -> str:
    """Create a bank with a unique id so concurrent runs can't collide."""
    bank_id = f"test-livewin-{uuid.uuid4().hex[:8]}"
    await pool.execute(
        "INSERT INTO banks (bank_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        bank_id,
        bank_id,
    )
    return bank_id


async def _insert_op(
    pool,
    bank_id: str,
    op_type: str = "retain",
    status: str = "pending",
    *,
    age_seconds: float = 0.0,
    worker_id: str | None = None,
    retry_in_seconds: float | None = None,
    serialization_key: str | None = None,
    content_chars: int = 0,
    claimed_seconds_ago: float | None = None,
) -> uuid.UUID:
    """Insert one claimable operation row, ``age_seconds`` old."""
    op_id = uuid.uuid4()
    created_at = datetime.now(UTC) - timedelta(seconds=age_seconds)
    payload = json.dumps(
        {
            "type": op_type,
            "bank_id": bank_id,
            "operation_id": str(op_id),
            "contents": [{"content": "x" * content_chars}] if content_chars else [],
        }
    )
    claimed_ago = (
        datetime.now(UTC) - timedelta(seconds=claimed_seconds_ago) if claimed_seconds_ago is not None else None
    )
    claimed_at = claimed_ago if claimed_ago is not None else (created_at if status == "processing" else None)
    next_retry_at = datetime.now(UTC) + timedelta(seconds=retry_in_seconds) if retry_in_seconds else None
    await pool.execute(
        f"""
        INSERT INTO {_TABLE}
            (operation_id, bank_id, operation_type, status, task_payload, worker_id,
             created_at, claimed_at, next_retry_at, serialization_key, updated_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, now())
        """,
        op_id,
        bank_id,
        op_type,
        status,
        payload,
        worker_id,
        created_at,
        claimed_at,
        next_retry_at,
        serialization_key,
    )
    return op_id


async def _claim(
    backend,
    *,
    shared: int = 10,
    reserved: dict[str, int] | None = None,
    cursor: str = "zzzz",
    window: int = 0,
    excluded: tuple[str, ...] = (),
):
    """Run one claim cycle. The default cursor sorts after every bank, so the rotation tier is empty."""
    async with backend.acquire() as conn:
        async with conn.transaction():
            return await backend.ops.claim_tasks(
                conn,
                _TABLE,
                "test-livewin-worker",
                reserved or {},
                shared,
                bank_cursor=cursor,
                live_window_hours=window,
                backfill_excluded_banks=excluded,
            )


def _ids(claimed) -> list[str]:
    """Claimed operation ids, in claim order (rotation row first)."""
    return [str(row["operation_id"]) for row in claimed.rows]


H = 3600


@pytest.mark.asyncio
async def test_without_a_window_claiming_stays_oldest_first(pool, backend, clean_operations):
    bank = await _make_bank(pool)
    old = await _insert_op(pool, bank, age_seconds=48 * H)
    await _insert_op(pool, bank, age_seconds=60)
    assert _ids(await _claim(backend, shared=1)) == [str(old)]


@pytest.mark.asyncio
async def test_live_work_is_claimed_ahead_of_a_due_backlog(pool, backend, clean_operations):
    bank = await _make_bank(pool)
    backlog = [await _insert_op(pool, bank, age_seconds=48 * H + i) for i in range(5)]
    live = [await _insert_op(pool, bank, age_seconds=120), await _insert_op(pool, bank, age_seconds=60)]
    claimed = await _claim(backend, shared=3, window=6)
    # live rows first, oldest of them first; the pool's spare slot still goes to the backlog
    assert _ids(claimed)[:2] == [str(live[0]), str(live[1])]
    assert _ids(claimed)[2] == str(backlog[-1])


@pytest.mark.asyncio
async def test_the_rotation_still_comes_first(pool, backend, clean_operations):
    a, b = sorted([await _make_bank(pool), await _make_bank(pool)])
    await _insert_op(pool, a, age_seconds=60)
    starved = await _insert_op(pool, b, age_seconds=48 * H)
    claimed = await _claim(backend, shared=1, window=6, cursor=a)
    assert _ids(claimed) == [str(starved)]


@pytest.mark.asyncio
async def test_window_is_work_conserving(pool, backend, clean_operations):
    bank = await _make_bank(pool)
    backlog = [await _insert_op(pool, bank, age_seconds=48 * H + i) for i in range(3)]
    claimed = await _claim(backend, shared=5, window=6)
    assert sorted(_ids(claimed)) == sorted(str(op) for op in backlog)


@pytest.mark.asyncio
async def test_excluded_bank_backfill_waits_but_its_live_work_does_not(pool, backend, clean_operations):
    spent, other = await _make_bank(pool), await _make_bank(pool)
    spent_old = await _insert_op(pool, spent, age_seconds=48 * H)
    spent_live = await _insert_op(pool, spent, age_seconds=60)
    other_old = await _insert_op(pool, other, age_seconds=48 * H)
    claimed = set(_ids(await _claim(backend, shared=10, window=6, excluded=(spent,))))
    assert claimed == {str(spent_live), str(other_old)}
    assert str(spent_old) not in claimed


@pytest.mark.asyncio
async def test_exclusion_only_holds_back_retains(pool, backend, clean_operations):
    spent = await _make_bank(pool)
    refresh = await _insert_op(pool, spent, op_type="refresh_mental_model", age_seconds=48 * H)
    await _insert_op(pool, spent, age_seconds=48 * H)
    assert _ids(await _claim(backend, shared=10, window=6, excluded=(spent,))) == [str(refresh)]


@pytest.mark.asyncio
async def test_reserved_slots_take_live_work_first_and_honour_the_budget(pool, backend, clean_operations):
    spent, other = await _make_bank(pool), await _make_bank(pool)
    await _insert_op(pool, spent, age_seconds=48 * H)
    other_old = await _insert_op(pool, other, age_seconds=49 * H)
    live = await _insert_op(pool, spent, age_seconds=60)
    claimed = await _claim(backend, shared=0, reserved={"retain": 3}, window=6, excluded=(spent,))
    assert _ids(claimed) == [str(live), str(other_old)]


@pytest.mark.asyncio
async def test_budget_counts_backfill_claimed_in_the_last_hour(pool, backend, clean_operations):
    heavy, light, live_only = await _make_bank(pool), await _make_bank(pool), await _make_bank(pool)
    for _ in range(3):
        await _insert_op(
            pool, heavy, "retain", "completed", age_seconds=48 * H, content_chars=4000, claimed_seconds_ago=600
        )
    await _insert_op(
        pool, light, "retain", "completed", age_seconds=48 * H, content_chars=1000, claimed_seconds_ago=600
    )
    # claimed long ago, or claimed while live: neither spends the backfill budget
    await _insert_op(
        pool, light, "retain", "completed", age_seconds=48 * H, content_chars=9000, claimed_seconds_ago=2 * H
    )
    await _insert_op(
        pool, live_only, "retain", "completed", age_seconds=700, content_chars=11000, claimed_seconds_ago=600
    )
    async with backend.acquire() as conn:
        spent = await backend.ops.backfill_budget_spent_banks(conn, _TABLE, 10_000, 6)
        spent_without_window = await backend.ops.backfill_budget_spent_banks(conn, _TABLE, 10_000, 0)
    assert sorted(spent) == [heavy]
    assert sorted(spent_without_window) == sorted([heavy, live_only])
