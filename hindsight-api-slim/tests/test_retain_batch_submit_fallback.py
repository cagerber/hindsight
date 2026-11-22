"""A batch retain whose job cannot be *submitted* falls back to real-time extraction.

Only a pre-submission failure may fall back: once a batch_id exists the batch may still
produce facts, so re-extracting in real time could double-write them.
"""

from types import SimpleNamespace

import pytest

from hindsight_api.engine.retain import fact_extraction
from hindsight_api.engine.retain.fact_extraction import BatchSubmitError, extract_facts_from_contents
from hindsight_api.engine.retain.types import RetainContent


class _Config(SimpleNamespace):
    """Only the routing settings matter here; any other setting reads as unset."""

    def __getattr__(self, _name):
        return None


def _config() -> _Config:
    return _Config(retain_extraction_mode="concise", retain_batch_enabled=True)


async def test_submit_failure_falls_back_to_realtime(monkeypatch):
    realtime_calls = []

    async def batch_fails(*_a, **_k):
        raise BatchSubmitError("HTTPException: 500")

    async def realtime(**kwargs):
        realtime_calls.append(kwargs["text"])
        return [], [], fact_extraction.TokenUsage()

    monkeypatch.setattr(fact_extraction, "extract_facts_from_contents_batch_api", batch_fails)
    monkeypatch.setattr(fact_extraction, "_build_extraction_prompt_and_schema", lambda _c: object())
    monkeypatch.setattr(fact_extraction, "_add_temporal_offsets", lambda *_a: None)
    monkeypatch.setattr(fact_extraction, "extract_facts_from_text", realtime)

    await extract_facts_from_contents([RetainContent(content="hello")], object(), _config())

    assert realtime_calls == ["hello"]


async def test_failure_after_submission_is_not_swallowed(monkeypatch):
    async def batch_fails(*_a, **_k):
        raise RuntimeError("poll failed after the batch was queued")

    async def realtime(**_k):  # pragma: no cover - must not run
        raise AssertionError("real-time fallback must not run after submission")

    monkeypatch.setattr(fact_extraction, "extract_facts_from_contents_batch_api", batch_fails)
    monkeypatch.setattr(fact_extraction, "extract_facts_from_text", realtime)

    with pytest.raises(RuntimeError, match="poll failed"):
        await extract_facts_from_contents([RetainContent(content="hello")], object(), _config())
