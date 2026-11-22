"""Bank-wide source-span filter (fork mitigation E3).

A paragraph that a surviving chunk of the bank already extracted is shown to the LLM as a short
``[already retained: ...]`` marker, and a reference (``span_refs``) records which chunk holds it.
Stored chunks keep their original text. When the source chunk goes away, maintenance repoints
the reference at another copy of the paragraph, or, after a grace period, drops the referring
chunk and replays its document so that chunk is re-extracted with the paragraph in full.
"""

import json
import uuid

import pytest

from hindsight_api.config import clear_config_cache, get_config
from hindsight_api.engine.db_utils import acquire_with_retry
from hindsight_api.engine.maintenance import MaintenanceLoop
from hindsight_api.engine.retain import fact_extraction
from hindsight_api.engine.retain.span_filter import only_markers
from hindsight_api.engine.retain.span_paragraphs import (
    MARKER_PREFIX,
    iter_paragraphs,
    marker_for,
    para_hash,
    rewrite_paragraphs,
)
from hindsight_api.engine.schema import fq_store_table as fq_table

# Paragraphs comfortably above the default 160-char floor.
_SHARED = (
    "The deployment runbook says the embedded Postgres instance must be stopped before the "
    "ZFS pool is exported, otherwise pg0 leaves an empty instance.json behind and refuses to "
    "start on the next boot without printing any error at all."
)
_OWN_A = (
    "Alice owns the maintenance window on Tuesdays and keeps the rotation schedule in the shared "
    "calendar so that the on-call engineer can see which bank is being drained at any time."
)
_OWN_B = (
    "Bob found that the backfill budget per bank and hour is what keeps an old backlog from "
    "starving the live queue, and he wants the watchdog to alert when it is exceeded twice."
)


# ── pure segmentation ────────────────────────────────────────────────────────


def _replace_shared(p: str) -> str | None:
    return marker_for(p) if para_hash(p) == para_hash(_SHARED) else None


def test_plain_text_paragraph_is_replaced_and_separators_kept():
    text = f"{_OWN_A}\n\n{_SHARED}\n  \n{_OWN_B}"
    out = rewrite_paragraphs(text, _replace_shared)
    assert _SHARED not in out
    assert marker_for(_SHARED) in out
    assert out.startswith(_OWN_A + "\n\n")
    assert out.endswith("\n  \n" + _OWN_B)


def test_json_conversation_rewrites_only_turn_content():
    turns = [
        {"role": "user", "content": f"{_OWN_A}\n\n{_SHARED}"},
        {"role": "assistant", "content": _OWN_B},
    ]
    out = rewrite_paragraphs(json.dumps(turns), _replace_shared)
    parsed = json.loads(out)
    assert [t["role"] for t in parsed] == ["user", "assistant"]
    assert parsed[0]["content"] == f"{_OWN_A}\n\n{marker_for(_SHARED)}"
    assert parsed[1]["content"] == _OWN_B


def test_jsonl_conversation_is_rewritten_line_by_line():
    lines = [json.dumps({"role": "user", "content": _SHARED}), json.dumps({"role": "assistant", "content": _OWN_B})]
    out = rewrite_paragraphs("\n".join(lines), _replace_shared)
    first, second = (json.loads(ln) for ln in out.split("\n"))
    assert first["content"] == marker_for(_SHARED)
    assert second["content"] == _OWN_B


def test_unchanged_text_is_returned_identical():
    text = json.dumps([{"role": "user", "content": _OWN_A}], indent=2)
    assert rewrite_paragraphs(text, _replace_shared) == text


def test_iter_paragraphs_skips_short_and_marker_paragraphs():
    text = f"short one\n\n{_SHARED}\n\n{marker_for(_OWN_A)}\n\n{_OWN_B}"
    assert list(iter_paragraphs(text, 160)) == [_SHARED, _OWN_B]


def test_para_hash_ignores_surrounding_whitespace():
    assert para_hash(f"  {_SHARED}\n") == para_hash(_SHARED)
    assert para_hash(_SHARED) != para_hash(_OWN_A)


def test_marker_is_short_and_recognisable():
    m = marker_for(_SHARED)
    assert m.startswith(MARKER_PREFIX)
    assert len(m) < 120
    assert only_markers(f"{m}\n\n{marker_for(_OWN_A)}")
    assert not only_markers(f"{m}\n\n{_OWN_B}")


# ── engine: retain, filter, reconcile ────────────────────────────────────────


@pytest.fixture
def _span_env(monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION", "false")
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_OBSERVATIONS", "false")
    monkeypatch.setenv("HINDSIGHT_API_RETAIN_SPAN_FILTER", "true")
    monkeypatch.setenv("HINDSIGHT_API_RETAIN_SPAN_ORPHAN_GRACE_SECONDS", "0")
    clear_config_cache()
    yield
    clear_config_cache()


class _PromptSpy:
    """Records the chunk text each extraction LLM call actually sees."""

    def __init__(self, monkeypatch) -> None:
        self.prompts: list[str] = []
        original = fact_extraction._extract_facts_with_auto_split

        async def _spy(*args, **kwargs):
            self.prompts.append(kwargs["chunk"])
            return await original(*args, **kwargs)

        monkeypatch.setattr(fact_extraction, "_extract_facts_with_auto_split", _spy)


async def _retain(memory, request_context, bank_id: str, document_id: str, text: str) -> None:
    await memory.retain_async(
        bank_id=bank_id,
        content=text,
        context="notes",
        document_id=document_id,
        request_context=request_context,
    )


async def _refs(memory, bank_id: str) -> list[dict]:
    async with acquire_with_retry(memory._backend) as conn:
        rows = await conn.fetch(
            f"""SELECT r.referring_chunk_id, r.source_chunk_id, r.orphaned_at, c.document_id
                FROM {fq_table("span_refs")} r JOIN {fq_table("chunks")} c ON c.chunk_id = r.referring_chunk_id
                WHERE r.bank_id = $1""",
            bank_id,
        )
    return [dict(r) for r in rows]


async def _chunk_ids(memory, bank_id: str, document_id: str) -> list[str]:
    async with acquire_with_retry(memory._backend) as conn:
        rows = await conn.fetch(
            f"SELECT chunk_id FROM {fq_table('chunks')} WHERE bank_id = $1 AND document_id = $2 ORDER BY chunk_index",
            bank_id,
            document_id,
        )
    return [r["chunk_id"] for r in rows]


async def _cleanup(memory, request_context, bank_id: str) -> None:
    await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
async def test_repeated_paragraph_is_sent_as_marker_and_referenced(memory, request_context, _span_env, monkeypatch):
    bank_id = f"span-{uuid.uuid4().hex[:8]}"
    spy = _PromptSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a", f"{_OWN_A}\n\n{_SHARED}")
        await _retain(memory, request_context, bank_id, "doc-b", f"{_SHARED}\n\n{_OWN_B}")

        assert any(_SHARED in p for p in spy.prompts[:1]), "first copy must be extracted in full"
        second = spy.prompts[-1]
        assert _SHARED not in second
        assert marker_for(_SHARED) in second
        assert _OWN_B in second

        # Stored text is untouched: delta hashes and re-extraction work on the original.
        async with acquire_with_retry(memory._backend) as conn:
            stored = await conn.fetchval(
                f"SELECT chunk_text FROM {fq_table('chunks')} WHERE bank_id = $1 AND document_id = 'doc-b'",
                bank_id,
            )
        assert _SHARED in stored and MARKER_PREFIX not in stored

        refs = await _refs(memory, bank_id)
        assert len(refs) == 1
        (a_chunk,) = await _chunk_ids(memory, bank_id, "doc-a")
        assert refs[0]["document_id"] == "doc-b"
        assert refs[0]["source_chunk_id"] == a_chunk
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_off_by_default_sends_full_text(memory, request_context, monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION", "false")
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_OBSERVATIONS", "false")
    clear_config_cache()
    bank_id = f"span-{uuid.uuid4().hex[:8]}"
    spy = _PromptSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a", f"{_OWN_A}\n\n{_SHARED}")
        await _retain(memory, request_context, bank_id, "doc-b", f"{_SHARED}\n\n{_OWN_B}")
        assert all(MARKER_PREFIX not in p for p in spy.prompts)
        assert await _refs(memory, bank_id) == []
    finally:
        await _cleanup(memory, request_context, bank_id)
        clear_config_cache()


@pytest.mark.asyncio
async def test_lost_source_is_repointed_to_another_copy(memory, request_context, monkeypatch):
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION", "false")
    monkeypatch.setenv("HINDSIGHT_API_ENABLE_OBSERVATIONS", "false")
    clear_config_cache()
    bank_id = f"span-{uuid.uuid4().hex[:8]}"
    _PromptSpy(monkeypatch)
    try:
        # Stored before the filter was on: extracted in full, indexed only by the backfill.
        await _retain(memory, request_context, bank_id, "doc-x", f"{_SHARED}\n\n{_OWN_B} (copy x)")
        monkeypatch.setenv("HINDSIGHT_API_RETAIN_SPAN_FILTER", "true")
        clear_config_cache()
        await _retain(memory, request_context, bank_id, "doc-a", f"{_OWN_A}\n\n{_SHARED}")
        await _retain(memory, request_context, bank_id, "doc-b", f"{_SHARED}\n\n{_OWN_B}")
        (a_chunk,) = await _chunk_ids(memory, bank_id, "doc-a")
        assert [r["source_chunk_id"] for r in await _refs(memory, bank_id)] == [a_chunk]

        await memory.delete_document("doc-a", bank_id, request_context=request_context)
        await MaintenanceLoop(memory)._run_span_reconcile(get_config())

        (x_chunk,) = await _chunk_ids(memory, bank_id, "doc-x")
        (ref,) = await _refs(memory, bank_id)
        assert ref["source_chunk_id"] == x_chunk
        assert ref["orphaned_at"] is None
    finally:
        await _cleanup(memory, request_context, bank_id)
        clear_config_cache()


@pytest.mark.asyncio
async def test_orphaned_reference_restores_the_chunk(memory, request_context, _span_env, monkeypatch):
    bank_id = f"span-{uuid.uuid4().hex[:8]}"
    spy = _PromptSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a", f"{_OWN_A}\n\n{_SHARED}")
        await _retain(memory, request_context, bank_id, "doc-b", f"{_SHARED}\n\n{_OWN_B}")
        await memory.delete_document("doc-a", bank_id, request_context=request_context)

        loop = MaintenanceLoop(memory)
        spy.prompts.clear()
        # Pass 1 stamps the reference orphaned; with a zero grace period pass 2 restores it.
        await loop._run_span_reconcile(get_config())
        await loop._run_span_reconcile(get_config())

        assert any(_SHARED in p for p in spy.prompts), "the paragraph must be re-extracted in full"
        assert await _refs(memory, bank_id) == []
        chunks = await _chunk_ids(memory, bank_id, "doc-b")
        assert len(chunks) == 1
        units = await memory.list_memory_units(bank_id, document_id="doc-b", limit=100, request_context=request_context)
        assert units["items"], "doc-b keeps its facts after the restore"
    finally:
        await _cleanup(memory, request_context, bank_id)


@pytest.mark.asyncio
async def test_restore_re_extracts_only_the_dropped_chunk(memory, request_context, _span_env, monkeypatch):
    bank_id = f"span-{uuid.uuid4().hex[:8]}"
    spy = _PromptSpy(monkeypatch)
    try:
        await _retain(memory, request_context, bank_id, "doc-a", f"{_OWN_A}\n\n{_SHARED}")
        await memory._config_resolver.update_bank_config(bank_id, {"retain_chunk_size": 280}, request_context)
        # Three chunks; only the middle one references doc-a.
        await _retain(memory, request_context, bank_id, "doc-b", f"{_OWN_B}\n\n{_SHARED}\n\n{_OWN_A} (tail b)")
        b_chunks = await _chunk_ids(memory, bank_id, "doc-b")
        assert len(b_chunks) >= 2
        refs = await _refs(memory, bank_id)
        assert len(refs) == 1
        await memory.delete_document("doc-a", bank_id, request_context=request_context)

        loop = MaintenanceLoop(memory)
        spy.prompts.clear()
        await loop._run_span_reconcile(get_config())
        await loop._run_span_reconcile(get_config())

        assert len(spy.prompts) == 1, spy.prompts
        assert _SHARED in spy.prompts[0]
        after = await _chunk_ids(memory, bank_id, "doc-b")
        assert len(after) == len(b_chunks)
        assert set(after) - {refs[0]["referring_chunk_id"]} <= set(b_chunks)
        assert await _refs(memory, bank_id) == []
    finally:
        await _cleanup(memory, request_context, bank_id)
