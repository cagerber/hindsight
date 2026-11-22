"""A newly queued replace cancels older queued replaces of the same document that it contains.

A conversation harness re-sends the whole session on every turn. When the worker is behind, the
queue holds several copies of one document, each a prefix of the next; extracting the older ones
pays for text the newest copy writes again. Submission cancels them while they are still pending.
"""

import json
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.retain.orchestrator import _queued_body_superseded_by
from hindsight_api.engine.schema import fq_table


def _turns(n: int, *, last_ts: str = "2026-01-01T00:00:00Z") -> str:
    turns = [
        {"role": "user" if i % 2 == 0 else "assistant", "content": f"turn {i}", "timestamp": f"2026-01-01T00:0{i}:00Z"}
        for i in range(n)
    ]
    turns[-1]["timestamp"] = last_ts
    return json.dumps(turns)


class TestQueuedBodySupersededBy:
    def test_text_prefix(self):
        assert _queued_body_superseded_by("alpha beta", "alpha beta gamma")

    def test_equal_text(self):
        assert _queued_body_superseded_by("alpha beta", "alpha beta")

    def test_diverging_text(self):
        assert not _queued_body_superseded_by("alpha delta", "alpha beta gamma")

    def test_newer_shorter(self):
        assert not _queued_body_superseded_by("alpha beta", "alpha")

    def test_empty(self):
        assert not _queued_body_superseded_by("", "x")
        assert not _queued_body_superseded_by("x", None)

    def test_conversation_prefix_with_restamped_last_turn(self):
        assert _queued_body_superseded_by(_turns(2, last_ts="2026-01-01T09:00:00Z"), _turns(4))

    def test_same_conversation_restamped(self):
        assert _queued_body_superseded_by(_turns(3, last_ts="2026-01-01T09:00:00Z"), _turns(3))

    def test_conversation_with_edited_turn(self):
        older = json.loads(_turns(2))
        older[0]["content"] = "edited"
        assert not _queued_body_superseded_by(json.dumps(older), _turns(4))


@pytest.fixture
def queue_only(memory, monkeypatch):
    """Submissions stay queued: the test backend would otherwise run each one on submit."""

    async def _noop(payload):
        return None

    monkeypatch.setattr(memory._task_backend, "submit_task", _noop)
    return memory


def _bank():
    return f"test_supersede_{datetime.now(timezone.utc).timestamp()}"


async def _submit(memory, request_context, bank_id, document_id, content, update_mode=None):
    item = {"content": content, "context": "chat", "document_id": document_id}
    if update_mode:
        item["update_mode"] = update_mode
    res = await memory.submit_async_retain(bank_id, [item], request_context=request_context)
    return res["operation_id"]


async def _children(memory, parent_id: str) -> list[dict]:
    pool = await memory._get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""SELECT operation_id, status, error_message FROM {fq_table("async_operations")}
                WHERE result_metadata->>'parent_operation_id' = $1""",
            parent_id,
        )
        parent = await conn.fetchval(
            f"SELECT status FROM {fq_table('async_operations')} WHERE operation_id = $1::uuid", parent_id
        )
    return [dict(r) for r in rows], parent


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_newer_replace_cancels_contained_older_ones(queue_only, request_context):
    memory, bank_id = queue_only, _bank()
    try:
        first = await _submit(memory, request_context, bank_id, "s1", _turns(2, last_ts="2026-01-01T09:00:00Z"))
        second = await _submit(memory, request_context, bank_id, "s1", _turns(3, last_ts="2026-01-01T09:01:00Z"))
        third = await _submit(memory, request_context, bank_id, "s1", _turns(4))

        for parent in (first, second):
            children, parent_status = await _children(memory, parent)
            assert [c["status"] for c in children] == ["cancelled"]
            assert children[0]["error_message"].startswith("superseded by queued replace")
            assert parent_status == "cancelled"
        children, parent_status = await _children(memory, third)
        assert [c["status"] for c in children] == ["pending"]
        assert parent_status == "pending"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_diverging_or_other_documents_are_kept(queue_only, request_context):
    memory, bank_id = queue_only, _bank()
    try:
        edited = json.loads(_turns(2))
        edited[0]["content"] = "edited"
        diverging = await _submit(memory, request_context, bank_id, "s1", json.dumps(edited))
        other_doc = await _submit(memory, request_context, bank_id, "s2", _turns(2))
        await _submit(memory, request_context, bank_id, "s1", _turns(4))

        for parent in (diverging, other_doc):
            children, _ = await _children(memory, parent)
            assert [c["status"] for c in children] == ["pending"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_appends_neither_cancel_nor_get_cancelled(queue_only, request_context):
    memory, bank_id = queue_only, _bank()
    try:
        older_append = await _submit(memory, request_context, bank_id, "s1", "alpha", update_mode="append")
        older_replace = await _submit(memory, request_context, bank_id, "s1", "alpha")
        await _submit(memory, request_context, bank_id, "s1", "alpha beta", update_mode="append")
        children, _ = await _children(memory, older_replace)
        assert [c["status"] for c in children] == ["pending"]  # a newer append does not supersede

        await _submit(memory, request_context, bank_id, "s1", "alpha beta gamma")
        children, _ = await _children(memory, older_append)
        assert [c["status"] for c in children] == ["pending"]  # an append is never cancelled
        children, _ = await _children(memory, older_replace)
        assert [c["status"] for c in children] == ["cancelled"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_claimed_older_replace_is_left_to_its_worker(queue_only, request_context):
    memory, bank_id = queue_only, _bank()
    try:
        older = await _submit(memory, request_context, bank_id, "s1", "alpha")
        children, _ = await _children(memory, older)
        pool = await memory._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {fq_table('async_operations')} SET status = 'processing' WHERE operation_id = $1",
                children[0]["operation_id"],
            )
        await _submit(memory, request_context, bank_id, "s1", "alpha beta")
        children, _ = await _children(memory, older)
        assert [c["status"] for c in children] == ["processing"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_multi_document_batches_are_untouched(queue_only, request_context):
    memory, bank_id = queue_only, _bank()
    try:
        res = await memory.submit_async_retain(
            bank_id,
            [
                {"content": "alpha", "context": "chat", "document_id": "s1"},
                {"content": "other", "context": "chat", "document_id": "s2"},
            ],
            request_context=request_context,
        )
        await _submit(memory, request_context, bank_id, "s1", "alpha beta")
        children, _ = await _children(memory, res["operation_id"])
        assert {c["status"] for c in children} == {"pending"}
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
