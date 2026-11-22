"""Bank-wide source-span filter (fork mitigation E3).

Agent transcripts re-send the same paragraphs again and again across documents: a plan pasted
into three sessions, a tool output replayed in a resumed conversation, a skill text quoted in
every turn. Delta retain only skips chunks it already holds for the SAME document, so each new
document pays full extraction for text whose facts the bank already has.

While a retain runs, this filter shows the LLM a short ``[already retained: <first 80 chars>]``
marker in place of any paragraph that a SURVIVING chunk has already extracted: a chunk of another
document, or an unchanged chunk this write keeps. The stored chunk keeps its original text, so
delta hashes and later re-extraction work exactly as before; only the prompt changes.

Every marker is recorded as a reference (``span_refs``: referring chunk -> source chunk). When a
source is deleted or rewritten, maintenance repoints the reference at another live copy of the
paragraph, or - after a grace period - has the referring chunk re-extracted with the paragraph in
full (see ``memories/pg/span_index.py``). The facts are never silently lost with the source.

Off by default (``HINDSIGHT_API_RETAIN_SPAN_FILTER``). Active only for banks whose store keeps the
index (the Postgres store), with document text stored, and never for a forced re-extract or the
verbatim/chunks extraction modes (whose facts ARE the chunk text). In those cases chunks are still
indexed when the switch is on, so they can serve as sources.

The filter rides a context variable set around one document's retain in ``retain_batch``: the
extraction tasks and the chunk-storing consumer it spawns inherit it.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from .span_paragraphs import MARKER_PREFIX, SpanChunk, iter_paragraphs, marker_for, para_hash, rewrite_paragraphs

logger = logging.getLogger(__name__)

_ACTIVE: ContextVar[SpanFilter | None] = ContextVar("retain_span_filter", default=None)

_NO_FILTER_MODES = frozenset({"verbatim", "chunks"})


def current() -> SpanFilter | None:
    """The filter of the retain running in this context, if any."""
    return _ACTIVE.get()


def only_markers(text: str) -> bool:
    """True when every paragraph of a (filtered) chunk is a marker: nothing new to extract."""
    seen = {"marker": 0, "other": 0}

    def _count(p: str) -> None:
        seen["marker" if p.startswith(MARKER_PREFIX) else "other"] += 1
        return None

    rewrite_paragraphs(text, _count)
    return seen["marker"] > 0 and seen["other"] == 0


def _text_key(text: str) -> bytes:
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).digest()


class SpanFilter:
    """Per-document filter state for one ``retain_batch`` call."""

    def __init__(self, *, pool: Any, store: Any, bank_id: str, document_id: str, min_chars: int, filtering: bool):
        self.pool = pool
        self.store = store
        self.bank_id = bank_id
        self.document_id = document_id
        self.min_chars = min_chars
        self.filtering = filtering
        # This document's chunks that survive the write (delta retain's unchanged set); the only
        # same-document chunks allowed as sources.
        self.kept_chunk_ids: set[str] = set()
        # chunk text -> {paragraph hash: (source chunk id, paragraph length)} for what was marked.
        self._marks: dict[bytes, dict[bytes, tuple[str, int]]] = {}
        self.marked_paragraphs = 0
        self.marked_chars = 0

    def set_kept(self, chunk_ids: set[str]) -> None:
        self.kept_chunk_ids = set(chunk_ids)

    async def prompt_texts(self, chunks: list[str]) -> list[str]:
        """The texts to show the LLM for ``chunks``: each paragraph a live surviving chunk already
        extracted is replaced by a marker. Any lookup failure shows the chunks unchanged."""
        if not self.filtering or not chunks:
            return chunks
        hashes = list({para_hash(p) for c in chunks for p in iter_paragraphs(c, self.min_chars)})
        if not hashes:
            return chunks
        try:
            from ..db_utils import acquire_with_retry

            async with acquire_with_retry(self.pool, max_retries=1) as conn:
                sources = await self.store.span_sources(
                    conn=conn,
                    bank_id=self.bank_id,
                    document_id=self.document_id,
                    kept_chunk_ids=sorted(self.kept_chunk_ids),
                    para_hashes=hashes,
                )
        except Exception as e:  # noqa: BLE001 - an optimisation; never fail the retain over it
            logger.warning(f"[span] source lookup failed for {self.document_id}, extracting in full: {e}")
            return chunks
        if not sources:
            return chunks

        out: list[str] = []
        for chunk in chunks:
            marks: dict[bytes, tuple[str, int]] = {}

            def _replace(p: str, marks: dict[bytes, tuple[str, int]] = marks) -> str | None:
                if len(p) < self.min_chars:
                    return None
                h = para_hash(p)
                src = sources.get(h)
                if src is None:
                    return None
                marks[h] = (src, len(p))
                return marker_for(p)

            shown = rewrite_paragraphs(chunk, _replace)
            if marks:
                self._marks.setdefault(_text_key(chunk), {}).update(marks)
                self.marked_paragraphs += len(marks)
                self.marked_chars += sum(n for _, n in marks.values())
            out.append(shown)
        if self.marked_paragraphs:
            logger.info(
                f"[span] {self.bank_id}/{self.document_id}: {self.marked_paragraphs} paragraph(s), "
                f"{self.marked_chars} chars shown as markers so far"
            )
        return out

    async def record_stored(self, conn: Any, chunks: list[Any], chunk_id_map: dict[int, str]) -> None:
        """Index chunks just written in this transaction (``ChunkMetadata`` items, original text):
        what each extraction saw in full makes it a source; what it saw as a marker is a reference."""
        from .chunk_storage import compute_chunk_hash

        entries: list[SpanChunk] = []
        for cm in chunks:
            chunk_id = chunk_id_map.get(cm.chunk_index)
            if not chunk_id:
                continue
            marks = self._marks.get(_text_key(cm.chunk_text), {})
            entry = SpanChunk(chunk_id, compute_chunk_hash(cm.chunk_text))
            for p in iter_paragraphs(cm.chunk_text, self.min_chars):
                h = para_hash(p)
                if h in marks:
                    src, n = marks[h]
                    entry.refs.append((h, src, n))
                else:
                    entry.shown.append(h)
            entries.append(entry)
        if entries:
            await self.store.span_record(conn=conn, bank_id=self.bank_id, chunks=entries)


def for_retain(pool: Any, bank_id: str, document_id: str, config: Any, *, force_reextract: bool) -> SpanFilter | None:
    """The filter for one document's retain, or None when the index is off for this bank."""
    from ...config import get_config
    from ..memories import get_memories

    server = get_config()  # the switch and threshold are static, server-level settings
    if not server.retain_span_filter or not document_id:
        return None
    if not config.store_document_text:  # per bank: no stored text, nothing to re-extract from
        return None

    store = get_memories()
    if store.store_owned_for(bank_id) or not store.span_index_for(bank_id):
        return None
    filtering = not force_reextract and config.retain_extraction_mode not in _NO_FILTER_MODES
    return SpanFilter(
        pool=pool,
        store=store,
        bank_id=bank_id,
        document_id=document_id,
        min_chars=server.retain_span_filter_min_chars,
        filtering=filtering,
    )


@contextmanager
def activated(span_filter: SpanFilter | None) -> Iterator[SpanFilter | None]:
    """Make ``span_filter`` the current filter for the enclosed retain (and the tasks it spawns)."""
    token = _ACTIVE.set(span_filter)
    try:
        yield span_filter
    finally:
        _ACTIVE.reset(token)
