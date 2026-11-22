"""``HINDSIGHT_API_RECALL_SKIP_SESSION_INGEST``: the recall SQL excludes session_ingest memories.

The clause must keep memories that carry no ``kind`` at all (``!=`` against a NULL would drop
them), and must be absent when the switch is off.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hindsight_api.config import get_config
from hindsight_api.engine.memories.pg import recall

CLAUSE = "metadata->>'kind' IS DISTINCT FROM 'session_ingest'"


class _RecordingConn:
    def __init__(self):
        self.queries: list[str] = []

    async def fetch(self, query, *_params):
        self.queries.append(query)
        return []


def _patch_config(monkeypatch, skip: bool):
    real = get_config()

    class _Cfg:
        recall_skip_session_ingest = skip

        def __getattr__(self, name):
            return getattr(real, name)

    cfg = _Cfg()
    monkeypatch.setattr(recall, "get_config", lambda: cfg)


@pytest.mark.parametrize("skip", [True, False])
async def test_semantic_arms_carry_the_filter_only_when_enabled(monkeypatch, skip):
    _patch_config(monkeypatch, skip)
    conn = _RecordingConn()

    await recall.retrieve_semantic_bm25_combined_sql(
        conn, "[0.1,0.2]", "q", "bank", ["experience", "world"], 10, enable_text_search=False
    )

    assert (CLAUSE in conn.queries[0]) is skip


@pytest.mark.parametrize("skip", [True, False])
async def test_temporal_arms_carry_the_filter_only_when_enabled(monkeypatch, skip):
    _patch_config(monkeypatch, skip)
    conn = _RecordingConn()
    start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)

    await recall.retrieve_temporal_combined_sql(conn, "[0.1,0.2]", "bank", ["experience"], start, end, 10)

    assert (CLAUSE in conn.queries[0]) is skip
