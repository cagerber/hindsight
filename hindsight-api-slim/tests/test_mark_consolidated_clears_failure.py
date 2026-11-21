"""mark_consolidated: a success stamp clears an earlier failure stamp."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.memories.pg.reads import mark_consolidated


class _Conn:
    def __init__(self) -> None:
        self.sql: list[str] = []

    async def execute(self, sql: str, *args) -> None:
        self.sql.append(" ".join(sql.split()))


async def _run(**kw) -> str:
    conn = _Conn()
    await mark_consolidated(conn=conn, fq_table=lambda t: t, bank_id="b", unit_ids=[str(uuid.uuid4())], **kw)
    return conn.sql[0]


@pytest.mark.asyncio
async def test_success_clears_failed_at():
    sql = await _run(when=datetime.now(timezone.utc))
    assert "SET consolidated_at = $1, consolidation_failed_at = NULL" in sql


@pytest.mark.asyncio
async def test_failure_leaves_consolidated_at_alone():
    sql = await _run(when=datetime.now(timezone.utc), failed=True)
    assert "SET consolidation_failed_at = $1" in sql
    assert "consolidated_at =" not in sql.replace("consolidation_failed_at", "")


@pytest.mark.asyncio
async def test_requeue_clear_keeps_fact_type_guard():
    sql = await _run(when=None)
    assert "consolidation_failed_at = NULL" in sql
    assert "fact_type IN ('experience', 'world')" in sql
