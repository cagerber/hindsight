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


@pytest.mark.asyncio
async def test_queue_disabled_client_retry_is_not_run_twice(memory, request_context, monkeypatch):
    import uuid

    monkeypatch.setenv("HINDSIGHT_API_RETAIN_QUEUE_ENABLED", "false")
    clear_config_cache()
    calls: list[dict] = []

    async def fake_handle(payload):
        calls.append(payload)

    monkeypatch.setattr(memory, "_handle_batch_retain", fake_handle)
    op = str(uuid.uuid4())
    item = [{"content": "turn 1", "document_id": "doc-r", "update_mode": "append"}]
    first = await memory.submit_async_retain(
        "test_retain_idem_bg", item, request_context=request_context, operation_id=op
    )
    again = await memory.submit_async_retain(
        "test_retain_idem_bg", item, request_context=request_context, operation_id=op
    )
    await asyncio.gather(*list(memory._bg_retain_tasks))
    assert first["operation_id"] is None and again["operation_id"] is None
    assert len(calls) == 1
    clear_config_cache()


@pytest.mark.asyncio
async def test_queue_disabled_transient_failure_is_retried(memory, request_context, monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_RETAIN_QUEUE_ENABLED", "false")
    clear_config_cache()
    attempts = 0

    async def flaky(payload):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("transient")

    async def no_sleep(_):
        return None

    monkeypatch.setattr(memory, "_handle_batch_retain", flaky)
    monkeypatch.setattr("hindsight_api.engine.memory_engine.asyncio.sleep", no_sleep)
    await memory.submit_async_retain(
        "test_retain_retry_bg", [{"content": "x", "document_id": "doc-t"}], request_context=request_context
    )
    await asyncio.gather(*list(memory._bg_retain_tasks))
    assert attempts == 2
    clear_config_cache()
