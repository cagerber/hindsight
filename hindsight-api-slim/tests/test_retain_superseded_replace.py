"""A queued replace that a newer copy of the same document overtook must not roll it back.

A conversation harness re-sends the whole session as a replace on every turn. When those requests
queue up, an older copy can be processed after a newer one has been retained. Without a guard it
replaces the document with the shorter body, deletes the newer turns' chunks and facts, and the next
submission pays to extract them again.
"""

import json
import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.retain.orchestrator import _stored_body_extends
from hindsight_api.engine.schema import fq_table


def _turns(n: int, *, last_ts: str = "2026-01-01T00:00:00Z") -> str:
    turns = [
        {"role": "user" if i % 2 == 0 else "assistant", "content": f"turn {i}", "timestamp": f"2026-01-01T00:0{i}:00Z"}
        for i in range(n)
    ]
    turns[-1]["timestamp"] = last_ts
    return json.dumps(turns)


class TestStoredBodyExtends:
    def test_text_prefix(self):
        assert _stored_body_extends("alpha beta gamma", "alpha beta")

    def test_equal_body_is_not_a_prefix(self):
        assert not _stored_body_extends("alpha beta", "alpha beta")

    def test_diverging_text(self):
        assert not _stored_body_extends("alpha beta gamma", "alpha delta")

    def test_longer_incoming(self):
        assert not _stored_body_extends("alpha", "alpha beta")

    def test_empty_sides(self):
        assert not _stored_body_extends(None, "x")
        assert not _stored_body_extends("x", "")

    def test_conversation_prefix_ignores_restamped_last_turn(self):
        stored = json.loads(_turns(4))
        older = json.loads(_turns(2, last_ts="2026-01-01T09:99:00Z"))
        # The older copy's last turn carries the send time, not the turn time.
        assert older[-1]["timestamp"] != stored[1]["timestamp"]
        assert _stored_body_extends(json.dumps(stored), json.dumps(older))

    def test_conversation_with_edited_turn(self):
        stored = json.loads(_turns(4))
        older = json.loads(_turns(2))
        older[0]["content"] = "edited"
        assert not _stored_body_extends(json.dumps(stored), json.dumps(older))

    def test_conversation_same_length(self):
        assert not _stored_body_extends(_turns(3), _turns(3, last_ts="2026-01-01T09:00:00Z"))


async def _queue_op(memory, bank_id: str, created_at: datetime) -> str:
    op_id = uuid.uuid4()
    pool = await memory._get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO {fq_table("async_operations")} (operation_id, bank_id, operation_type, status, created_at)
                VALUES ($1, $2, 'retain', 'processing', $3)""",
            op_id,
            bank_id,
            created_at,
        )
    return str(op_id)


async def _replace(memory, request_context, bank_id, document_id, content, operation_id=None):
    await memory.retain_batch_async(
        bank_id=bank_id,
        contents=[{"content": content, "context": "chat", "document_id": document_id}],
        request_context=request_context,
        operation_id=operation_id,
    )


def _ts():
    return datetime.now(timezone.utc).timestamp()


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_older_queued_replace_does_not_roll_the_document_back(memory, request_context):
    bank_id = f"test_superseded_{_ts()}"
    document_id = "session-1"
    try:
        queued_at = datetime.now(timezone.utc)
        older = "Alice works at Google.\nBob works at Microsoft."
        newer = older + "\nCarol works at Apple."
        await _replace(memory, request_context, bank_id, document_id, newer)
        op_id = await _queue_op(memory, bank_id, queued_at)

        await _replace(memory, request_context, bank_id, document_id, older, operation_id=op_id)

        doc = await memory.get_document(document_id, bank_id, request_context=request_context)
        assert "Carol works at Apple" in doc["original_text"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_replace_queued_after_the_last_write_can_truncate(memory, request_context):
    bank_id = f"test_superseded_trunc_{_ts()}"
    document_id = "doc-1"
    try:
        shorter = "Alice works at Google."
        await _replace(memory, request_context, bank_id, document_id, shorter + "\nBob works at Microsoft.")
        op_id = await _queue_op(memory, bank_id, datetime.now(timezone.utc))

        await _replace(memory, request_context, bank_id, document_id, shorter, operation_id=op_id)

        doc = await memory.get_document(document_id, bank_id, request_context=request_context)
        assert "Bob works at Microsoft" not in doc["original_text"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.memory_backend_incompatible
async def test_synchronous_replace_can_truncate(memory, request_context):
    bank_id = f"test_superseded_sync_{_ts()}"
    document_id = "doc-1"
    try:
        shorter = "Alice works at Google."
        await _replace(memory, request_context, bank_id, document_id, shorter + "\nBob works at Microsoft.")

        await _replace(memory, request_context, bank_id, document_id, shorter)

        doc = await memory.get_document(document_id, bank_id, request_context=request_context)
        assert "Bob works at Microsoft" not in doc["original_text"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
