"""``HINDSIGHT_API_CONSOLIDATION_SKIP_SESSION_INGEST``: session_ingest memories bypass the LLM.

Raw terminal commands and file listings from coding-agent session transcripts are noise for
consolidation. With the (default-off) flag they are stamped consolidated without an LLM call;
everything else consolidates as usual.
"""

from __future__ import annotations

import json
import uuid

import pytest

from hindsight_api.config import DEFAULT_CONSOLIDATION_SKIP_SESSION_INGEST, DEFAULT_RECALL_SKIP_SESSION_INGEST
from hindsight_api.engine.consolidation import consolidator as C
from hindsight_api.engine.memory_engine import MemoryEngine
from tests.test_consolidation_scope_parallelism import (  # noqa: F401  (fixture import)
    _mock_llm_one_obs_per_fact,
    _override_config,
    enable_observations,
)


def test_session_ingest_switches_are_off_by_default():
    assert DEFAULT_CONSOLIDATION_SKIP_SESSION_INGEST is False
    assert DEFAULT_RECALL_SKIP_SESSION_INGEST is False


async def _insert(conn, bank_id: str, text: str, metadata: dict | None) -> uuid.UUID:
    unit_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO memory_units (id, bank_id, text, fact_type, tags, metadata) "
        "VALUES ($1, $2, $3, 'experience', '{}', $4::jsonb)",
        unit_id,
        bank_id,
        text,
        json.dumps(metadata or {}),
    )
    return unit_id


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
@pytest.mark.parametrize("skip", [True, False])
async def test_session_ingest_memories_skip_the_llm_only_when_enabled(memory: MemoryEngine, request_context, skip):
    bank_id = f"test-skip-ingest-{uuid.uuid4().hex[:8]}"
    await memory.ensure_bank_profile(bank_id=bank_id, request_context=request_context)
    try:
        async with memory._pool.acquire() as conn:
            noise = await _insert(conn, bank_id, "The assistant ran cd /x && ls", {"kind": "session_ingest"})
            real = await _insert(conn, bank_id, "Alice prefers tea", {})

        wrapper, mock_llm = _mock_llm_one_obs_per_fact()
        original_llm = memory._consolidation_llm_config
        memory._consolidation_llm_config = wrapper
        try:
            from unittest.mock import patch

            with (
                _override_config(memory, consolidation_skip_session_ingest=skip, consolidation_llm_batch_size=1),
                patch.object(memory, "submit_async_consolidation"),
            ):
                await C.run_consolidation_job(memory_engine=memory, bank_id=bank_id, request_context=request_context)
        finally:
            memory._consolidation_llm_config = original_llm

        async with memory._pool.acquire() as conn:
            rows = {
                r["id"]: r
                for r in await conn.fetch(
                    "SELECT id, consolidated_at, consolidation_failed_at FROM memory_units WHERE id = ANY($1::uuid[])",
                    [noise, real],
                )
            }
        assert rows[real]["consolidated_at"] is not None
        assert rows[noise]["consolidated_at"] is not None  # stamped either way (skipped, or consolidated)
        assert rows[noise]["consolidation_failed_at"] is None

        observations = (
            await memory.list_memory_units(bank_id, fact_type="observation", limit=100, request_context=request_context)
        )["items"]
        texts = " ".join(o["text"] for o in observations)
        assert ("ran cd" in texts) is (not skip), f"skip={skip}: observations were {texts!r}"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
