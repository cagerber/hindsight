"""Clearing a bank's memories says how many it erased (#4307).

The delete is the only participant that knows the exact figure: a client that
differences two listings around it races any retain landing in between. So the
count must come back on the response, scoped to the `type` filter when one is
given, and be `0` (not absent) when there was nothing to clear.
"""

from __future__ import annotations

import httpx
import pytest

from hindsight_system_tests.payloads import consolidation, extracted, fact

pytestmark = pytest.mark.asyncio


async def _fact_types(client, bank: str) -> list[str]:
    memories = await client.memory.list_memories(bank, limit=100)
    return sorted(item.fact_type for item in memories.items)


async def test_a_clear_reports_what_it_erased_per_type(client, llm, bank_id, settled):
    llm.on_step("extract_facts").returns(
        extracted(
            fact("Alice moved to Berlin", who="Alice", entities=["Alice"]),
            fact("Alice plays cello", who="Alice", entities=["Alice"]),
            fact("I recommended a cello teacher to Alice", fact_type="assistant", entities=["Alice"]),
        )
    )
    llm.on_step("consolidate").returns(consolidation())
    await client.aretain(bank_id=bank_id, content="Alice moved to Berlin and plays cello; I found her a teacher.")
    await settled(bank_id)
    assert await _fact_types(client, bank_id) == ["experience", "world", "world"]

    typed = await client.memory.clear_bank_memories(bank_id, type="world")
    assert typed.deleted_count == 2
    assert await _fact_types(client, bank_id) == ["experience"]

    rest = await client.memory.clear_bank_memories(bank_id)
    assert rest.deleted_count == 1
    assert await _fact_types(client, bank_id) == []

    empty = await client.memory.clear_bank_memories(bank_id)
    assert empty.deleted_count == 0


async def test_b_clear_rejects_request_body(hindsight_server, client, bank_id):
    """The DELETE endpoint accepts only `?type=`. A JSON body is not part of the
    contract — it is a destructive endpoint with no documented body filter, so
    any non-empty body must be refused (422) rather than silently cleared.

    History: on 2026-09-29 a caller passed ``{"metadata": {...}}`` intending a
    scoped cleanup. The endpoint accepted the body, ignored it, and cleared
    7,447 memory units across banks. The 2026-09-29 bank export zip
    ``c1f44083-...`` was the only artifact that survived without restoring the
    whole DB. See /home/anton/dev-projects/trifour/.estate-tmp/ for the
    contemporaneous write-up and the discussion that produced this guard.
    """
    url = f"{hindsight_server.url}/v1/default/banks/{bank_id}/memories"

    # 1. Documented call (no body) still works.
    empty = await client.memory.clear_bank_memories(bank_id)
    assert empty.deleted_count == 0  # nothing in this fresh bank

    # 2. Documented `?type=` query still works.
    typed = await client.memory.clear_bank_memories(bank_id, type="observation")
    assert typed.deleted_count == 0

    # 3. ANY non-empty JSON body is refused with 422 — even an empty object,
    # and even one that *names* a field the endpoint does not document.
    async with httpx.AsyncClient(base_url=hindsight_server.url, timeout=5.0) as raw:
        for body in (
            b'{"metadata": {"probe": "true"}}',  # the exact 2026-09-29 shape
            b'{}',                               # empty object — still a body
            b'{"type": "world"}',                # even the documented field
            b'{"document_id": "x"}',
        ):
            resp = await raw.request("DELETE", url, content=body, headers={"Content-Type": "application/json"})
            assert resp.status_code == 422, (
                f"DELETE with body {body!r} should be 422, got {resp.status_code}: {resp.text}"
            )

        # 4. A whitespace-only body is tolerated (some clients emit "" / " ").
        #    This is the carve-out so existing curl-based smoke tests don't break.
        for body in (b"", b"   ", b"\n"):
            resp = await raw.request("DELETE", url, content=body)
            assert resp.status_code == 200, (
                f"DELETE with whitespace-only body {body!r} should still succeed, "
                f"got {resp.status_code}: {resp.text}"
            )
