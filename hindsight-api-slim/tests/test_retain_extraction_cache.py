"""Extraction checkpoint cache (fork mitigation E4).

A chunk's extraction is saved the moment the LLM returns, keyed by a hash of the exact request.
A retry after a failure later in the retain, or an unchanged re-send, reuses it instead of paying
for the same call again. Off by default; a forced re-extract always calls the LLM.
"""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from hindsight_api.config import clear_config_cache
from hindsight_api.engine.db_utils import acquire_with_retry
from hindsight_api.engine.maintenance import MaintenanceLoop
from hindsight_api.engine.retain import chunk_storage, fact_extraction
from hindsight_api.engine.retain.extraction_cache import request_hash
from hindsight_api.engine.schema import fq_store_table as fq_table

_EVENT_DATE = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
_TEXT = (
    "Alice owns the maintenance window on Tuesdays and keeps the rotation schedule in the shared "
    "calendar so that the on-call engineer can see which bank is being drained at any time.\n\n"
    "Bob found that the backfill budget per bank and hour is what keeps an old backlog from "
    "starving the live queue, and he wants the watchdog to alert when it is exceeded twice."
)


class _CallSpy:
    """Counts the extraction calls that actually reach the LLM."""

    def __init__(self, monkeypatch) -> None:
        self.calls = 0
        original = fact_extraction._extract_facts_from_chunk_uncached

        async def _spy(*args, **kwargs):
            self.calls += 1
            return await original(*args, **kwargs)

        monkeypatch.setattr(fact_extraction, "_extract_facts_from_chunk_uncached", _spy)


@pytest.fixture
def _env(monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION", "false")
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_OBSERVATIONS", "false")
    clear_config_cache()
    yield
    clear_config_cache()


@pytest.fixture
def _cache_on(_env, monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_RETAIN_EXTRACTION_CACHE", "true")
    clear_config_cache()
    yield


async def _retain(memory, request_context, bank_id: str, document_id: str, text: str = _TEXT) -> None:
    await memory.retain_async(
        bank_id=bank_id,
        content=text,
        context="notes",
        event_date=_EVENT_DATE,
        document_id=document_id,
        request_context=request_context,
    )


async def _cache_rows(memory, bank_id: str) -> int:
    async with acquire_with_retry(memory._backend) as conn:
        return await conn.fetchval(f"SELECT count(*) FROM {fq_table('extraction_cache')} WHERE bank_id = $1", bank_id)


async def _unit_count(memory, request_context, bank_id: str, document_id: str) -> int:
    units = await memory.list_memory_units(bank_id, document_id=document_id, limit=100, request_context=request_context)
    return len(units["items"])


async def _cleanup(memory, request_context, bank_id: str) -> None:
    await memory.delete_bank(bank_id, request_context=request_context)


class _Schema(BaseModel):
    facts: list[str]


def _key(**overrides) -> bytes:
    args = dict(
        provider="openai",
        model="m1",
        system_prompt="sys",
        user_message="chunk text",
        response_schema=_Schema,
        extraction_mode="concise",
        temperature=0.1,
    )
    args.update(overrides)
    return request_hash(**args)


def test_request_hash_covers_everything_the_model_is_asked():
    base = _key()
    assert base == _key()
    assert len(base) == 16
    for field, value in [
        ("provider", "anthropic"),
        ("model", "m2"),
        ("system_prompt", "sys2"),
        ("user_message", "other chunk"),
        ("extraction_mode", "verbose"),
        ("temperature", 0.2),
    ]:
        assert _key(**{field: value}) != base, field


@pytest.mark.asyncio
async def test_off_by_default_calls_llm_every_time(memory, request_context, _env, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    spy = _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        first = spy.calls
        assert first > 0
        await memory.delete_document("doc-a", bank_id, request_context=request_context)
        await _retain(memory, request_context, bank_id, "doc-a")
        assert spy.calls == 2 * first
        assert await _cache_rows(memory, bank_id) == 0
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_resend_reuses_saved_extraction(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    spy = _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        first = spy.calls
        assert first > 0
        assert await _cache_rows(memory, bank_id) == first
        units_before = await _unit_count(memory, request_context, bank_id, "doc-a")

        await memory.delete_document("doc-a", bank_id, request_context=request_context)
        await _retain(memory, request_context, bank_id, "doc-a")
        assert spy.calls == first, "an unchanged re-send must not call the LLM again"
        assert await _unit_count(memory, request_context, bank_id, "doc-a") == units_before
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_retry_after_failed_write_reuses_paid_extraction(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    spy = _CallSpy(monkeypatch)
    original = chunk_storage.store_chunks_batch
    failures = {"left": 1}

    async def _flaky(*args, **kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise RuntimeError("simulated crash after extraction")
        return await original(*args, **kwargs)

    monkeypatch.setattr(chunk_storage, "store_chunks_batch", _flaky)
    try:
        with pytest.raises(Exception):
            await _retain(memory, request_context, bank_id, "doc-a")
        paid = spy.calls
        assert paid > 0
        assert await _cache_rows(memory, bank_id) == paid

        await _retain(memory, request_context, bank_id, "doc-a")
        assert spy.calls == paid, "the retry must reuse the extraction the failed run paid for"
        assert await _unit_count(memory, request_context, bank_id, "doc-a") > 0
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_changed_text_misses(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    spy = _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        first = spy.calls
        await _retain(memory, request_context, bank_id, "doc-b", _TEXT + " Carol agreed.")
        assert spy.calls > first
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_forced_reextract_bypasses_cache(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    spy = _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        first = spy.calls
        await memory.reprocess_document(bank_id, "doc-a", request_context=request_context)
        assert spy.calls > first, "a forced re-extract must call the LLM"
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_maintenance_prunes_expired_entries(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        rows = await _cache_rows(memory, bank_id)
        assert rows > 0
        loop = MaintenanceLoop(memory)
        await loop._run_extraction_cache_prune(3600)
        assert await _cache_rows(memory, bank_id) == rows, "fresh entries are kept"

        async with acquire_with_retry(memory._backend) as conn:
            await conn.execute(
                f"UPDATE {fq_table('extraction_cache')} SET created_at = now() - interval '2 hours' WHERE bank_id = $1",
                bank_id,
            )
        await loop._run_extraction_cache_prune(3600)
        assert await _cache_rows(memory, bank_id) == 0
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_bank_delete_drops_its_entries(memory, request_context, _cache_on, monkeypatch):
    bank_id = f"xc-{uuid.uuid4().hex[:8]}"
    _CallSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a")
        assert await _cache_rows(memory, bank_id) > 0
    finally:
        await _cleanup(memory, request_context, bank_id)
    assert await _cache_rows(memory, bank_id) == 0
