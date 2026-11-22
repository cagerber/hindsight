"""With HINDSIGHT_API_RETAIN_QUEUE_ENABLED=false an async retain writes no queue rows."""

import asyncio

import pytest
import pytest_asyncio

from hindsight_api.config import clear_config_cache, get_config

pytestmark = pytest.mark.xdist_group("worker_tests")


@pytest_asyncio.fixture
async def pool(pg0_db_url):
    import asyncpg

    from hindsight_api.pg0 import resolve_database_url

    p = await asyncpg.create_pool(await resolve_database_url(pg0_db_url), min_size=1, max_size=3, command_timeout=30)
    yield p
    await p.close()


@pytest.mark.asyncio
async def test_queue_disabled_runs_in_background_without_rows(memory, request_context, pool, monkeypatch):
    bank_id = "test_retain_queue_disabled"
    monkeypatch.setenv("HINDSIGHT_API_RETAIN_QUEUE_ENABLED", "false")
    clear_config_cache()
    calls: list[dict] = []

    async def fake_handle(payload):
        await asyncio.sleep(0.05)
        calls.append(payload)

    monkeypatch.setattr(memory, "_handle_batch_retain", fake_handle)

    result = await memory.submit_async_retain(
        bank_id,
        [{"content": "hello", "document_id": "doc-1"}],
        request_context=request_context,
    )
    assert result["operation_id"] is None
    assert result["items_count"] == 1

    # A second append to the same document must run after the first, never concurrently.
    await memory.submit_async_retain(
        bank_id, [{"content": "again", "document_id": "doc-1"}], request_context=request_context
    )
    await asyncio.gather(*list(memory._bg_retain_tasks))
    assert [c["contents"][0]["content"] for c in calls] == ["hello", "again"]
    assert all(c["bank_id"] == bank_id for c in calls)

    rows = await pool.fetchval("SELECT count(*) FROM async_operations WHERE bank_id = $1", bank_id)
    assert rows == 0
    clear_config_cache()


@pytest.mark.asyncio
async def test_queue_enabled_still_queues(memory, request_context, pool):
    bank_id = "test_retain_queue_enabled"
    assert get_config().retain_queue_enabled is True
    result = await memory.submit_async_retain(
        bank_id, [{"content": "queued", "document_id": "doc-q"}], request_context=request_context
    )
    assert result["operation_id"] is not None
