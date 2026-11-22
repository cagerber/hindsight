"""SQL for the bank-wide source-span index (fork mitigation E3); tables from migration e3spn0a1b2c3.

* ``chunk_paragraphs`` - paragraphs a chunk's extraction saw in full, with the chunk's content
  hash at the time. A row is LIVE only while that hash still equals ``chunks.content_hash``: chunk
  ids are reused when a document is rewritten, so an overwritten chunk's old rows never match.
* ``chunk_span_index`` - which version of each chunk has been indexed (drives the backfill).
* ``span_refs`` - paragraphs a chunk saw as an ``[already retained: ...]`` marker, and the chunk
  whose facts cover them. The source side is not a foreign key on purpose: when a source goes,
  the row stays as an orphan for :func:`maintain` to repoint or to have the referring chunk
  re-extracted, instead of the facts silently disappearing with the source.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from ...retain.span_paragraphs import SpanChunk, SpanMaintenance, iter_paragraphs, para_hash


def _live_para(fq_table: Callable[[str], str], chunk_col: str, hash_col: str) -> str:
    """EXISTS clause: paragraph ``hash_col`` is live in chunk ``chunk_col``."""
    return f"""EXISTS (
        SELECT 1 FROM {fq_table("chunk_paragraphs")} lp
        JOIN {fq_table("chunks")} lc ON lc.chunk_id = lp.chunk_id
        WHERE lp.chunk_id = {chunk_col} AND lp.para_hash = {hash_col}
          AND lp.chunk_hash IS NOT DISTINCT FROM lc.content_hash)"""


async def sources(
    *,
    conn,
    fq_table: Callable[[str], str],
    bank_id: str,
    document_id: str,
    kept_chunk_ids: list[str],
    para_hashes: list[bytes],
) -> dict[bytes, str]:
    """For each paragraph hash, one live chunk of the bank that already extracted it, from another
    document or from this document's kept (unchanged, surviving) chunks. Oldest chunk wins, so
    repeated writes keep pointing at the same source."""
    if not para_hashes:
        return {}
    rows = await conn.fetch(
        f"""
        SELECT DISTINCT ON (p.para_hash) p.para_hash, p.chunk_id
        FROM {fq_table("chunk_paragraphs")} p
        JOIN {fq_table("chunks")} c ON c.chunk_id = p.chunk_id
        WHERE p.bank_id = $1 AND p.para_hash = ANY($2::bytea[])
          AND c.bank_id = $1
          AND p.chunk_hash IS NOT DISTINCT FROM c.content_hash
          AND (c.document_id <> $3 OR c.chunk_id = ANY($4::text[]))
        ORDER BY p.para_hash, c.created_at, p.chunk_id
        """,
        bank_id,
        para_hashes,
        document_id,
        kept_chunk_ids,
    )
    return {bytes(r["para_hash"]): r["chunk_id"] for r in rows}


async def record(*, conn, fq_table: Callable[[str], str], bank_id: str, chunks: list[SpanChunk]) -> None:
    """Replace the index rows of the given (just-stored) chunks."""
    if not chunks:
        return
    ids = [c.chunk_id for c in chunks]
    await conn.execute(f"DELETE FROM {fq_table('chunk_paragraphs')} WHERE chunk_id = ANY($1::text[])", ids)
    await conn.execute(f"DELETE FROM {fq_table('span_refs')} WHERE referring_chunk_id = ANY($1::text[])", ids)
    p_ids, p_hashes, p_chunk_hashes = [], [], []
    r_ids, r_hashes, r_sources, r_chars = [], [], [], []
    for c in chunks:
        for h in dict.fromkeys(c.shown):
            p_ids.append(c.chunk_id)
            p_hashes.append(h)
            p_chunk_hashes.append(c.chunk_hash)
        for h, src, n in c.refs:
            r_ids.append(c.chunk_id)
            r_hashes.append(h)
            r_sources.append(src)
            r_chars.append(n)
    if p_ids:
        await conn.execute(
            f"""
            INSERT INTO {fq_table("chunk_paragraphs")} (chunk_id, para_hash, bank_id, chunk_hash)
            SELECT u.chunk_id, u.para_hash, $1, u.chunk_hash
            FROM unnest($2::text[], $3::bytea[], $4::text[]) AS u(chunk_id, para_hash, chunk_hash)
            ON CONFLICT DO NOTHING
            """,
            bank_id,
            p_ids,
            p_hashes,
            p_chunk_hashes,
        )
    if r_ids:
        await conn.execute(
            f"""
            INSERT INTO {fq_table("span_refs")} (referring_chunk_id, para_hash, bank_id, source_chunk_id, chars)
            SELECT u.rid, u.para_hash, $1, u.src, u.chars
            FROM unnest($2::text[], $3::bytea[], $4::text[], $5::int[]) AS u(rid, para_hash, src, chars)
            ON CONFLICT DO NOTHING
            """,
            bank_id,
            r_ids,
            r_hashes,
            r_sources,
            r_chars,
        )
    await conn.execute(
        f"""
        INSERT INTO {fq_table("chunk_span_index")} (chunk_id, bank_id, chunk_hash, indexed_at)
        SELECT u.chunk_id, $1, u.chunk_hash, now()
        FROM unnest($2::text[], $3::text[]) AS u(chunk_id, chunk_hash)
        ON CONFLICT (chunk_id) DO UPDATE SET chunk_hash = EXCLUDED.chunk_hash, indexed_at = now()
        """,
        bank_id,
        ids,
        [c.chunk_hash for c in chunks],
    )


async def _backfill(*, conn, fq_table: Callable[[str], str], min_chars: int, limit: int) -> int:
    """Index chunks never indexed (stored before the filter, by a path that does not index, or
    rewritten since). A chunk that holds markers is never backfilled: its stored text is the
    original, so indexing it would claim paragraphs its extraction never saw."""
    rows = await conn.fetch(
        f"""
        SELECT c.chunk_id, c.bank_id, c.chunk_text, c.content_hash
        FROM {fq_table("chunks")} c
        LEFT JOIN {fq_table("chunk_span_index")} i ON i.chunk_id = c.chunk_id
        WHERE (i.chunk_id IS NULL OR i.chunk_hash IS DISTINCT FROM c.content_hash)
          AND c.chunk_text <> ''
          AND NOT EXISTS (SELECT 1 FROM {fq_table("span_refs")} r WHERE r.referring_chunk_id = c.chunk_id)
        LIMIT $1
        """,
        limit,
    )
    by_bank: dict[str, list[SpanChunk]] = {}
    for r in rows:
        shown = [para_hash(p) for p in iter_paragraphs(r["chunk_text"], min_chars)]
        by_bank.setdefault(r["bank_id"], []).append(SpanChunk(r["chunk_id"], r["content_hash"], shown))
    for bank_id, chunks in by_bank.items():
        async with conn.transaction():
            await record(conn=conn, fq_table=fq_table, bank_id=bank_id, chunks=chunks)
    return len(rows)


async def maintain(
    *,
    conn,
    fq_table: Callable[[str], str],
    min_chars: int,
    backfill_limit: int,
    reconcile_limit: int,
    grace_seconds: int,
) -> SpanMaintenance:
    """One maintenance pass: backfill the index, then repair references whose source is gone.

    A dangling reference is first REPOINTED at any other live chunk of the bank holding the same
    paragraph. Failing that it is stamped orphaned; once it has stayed unresolvable for
    ``grace_seconds`` (a replace that deletes a source re-writes the paragraph in a later
    sub-batch), the referring chunk is returned in ``restore``: the caller deletes it with its
    facts and replays its document, whose resumed retain re-extracts exactly that missing chunk,
    this time with the paragraph in full. Nothing here changes chunks or facts.
    """
    out = SpanMaintenance()
    out.indexed = await _backfill(conn=conn, fq_table=fq_table, min_chars=min_chars, limit=backfill_limit)

    live = _live_para(fq_table, "r.source_chunk_id", "r.para_hash")
    out.cleared = int(
        (
            await conn.execute(
                f"""UPDATE {fq_table("span_refs")} r SET orphaned_at = NULL, restore_requested_at = NULL
                    WHERE r.orphaned_at IS NOT NULL AND {live}"""
            )
        ).split()[-1]
    )
    dangling = await conn.fetch(
        f"""
        SELECT r.referring_chunk_id, r.para_hash, r.bank_id, r.orphaned_at, r.restore_requested_at
        FROM {fq_table("span_refs")} r
        WHERE NOT {live}
          AND (r.restore_requested_at IS NULL OR r.restore_requested_at < now() - make_interval(secs => $2))
        ORDER BY r.orphaned_at NULLS FIRST
        LIMIT $1
        FOR UPDATE OF r SKIP LOCKED
        """,
        reconcile_limit,
        float(grace_seconds),
    )
    now = datetime.now(UTC)
    restore_chunks: set[str] = set()
    for d in dangling:
        async with conn.transaction():
            repl = await conn.fetchval(
                f"""
                SELECT p.chunk_id FROM {fq_table("chunk_paragraphs")} p
                JOIN {fq_table("chunks")} c ON c.chunk_id = p.chunk_id
                WHERE p.bank_id = $1 AND p.para_hash = $2 AND p.chunk_id <> $3
                  AND p.chunk_hash IS NOT DISTINCT FROM c.content_hash
                ORDER BY c.created_at, p.chunk_id
                LIMIT 1
                """,
                d["bank_id"],
                d["para_hash"],
                d["referring_chunk_id"],
            )
            if repl is not None:
                await conn.execute(
                    f"""UPDATE {fq_table("span_refs")} SET source_chunk_id = $3, orphaned_at = NULL,
                        restore_requested_at = NULL WHERE referring_chunk_id = $1 AND para_hash = $2""",
                    d["referring_chunk_id"],
                    d["para_hash"],
                    repl,
                )
                out.repointed += 1
                continue
            if d["orphaned_at"] is None:
                await conn.execute(
                    f"""UPDATE {fq_table("span_refs")} SET orphaned_at = now()
                        WHERE referring_chunk_id = $1 AND para_hash = $2""",
                    d["referring_chunk_id"],
                    d["para_hash"],
                )
                out.orphaned += 1
                continue
            if now - d["orphaned_at"] < timedelta(seconds=grace_seconds):
                continue
            # Stamped so that neither another process nor the next pass restores it again while
            # this replay is in flight; a replay that never lands is retried after another grace.
            await conn.execute(
                f"""UPDATE {fq_table("span_refs")} SET restore_requested_at = now()
                    WHERE referring_chunk_id = $1 AND para_hash = $2""",
                d["referring_chunk_id"],
                d["para_hash"],
            )
            restore_chunks.add(d["referring_chunk_id"])

    if restore_chunks:
        rows = await conn.fetch(
            f"SELECT chunk_id, bank_id, document_id FROM {fq_table('chunks')} WHERE chunk_id = ANY($1::text[])",
            sorted(restore_chunks),
        )
        out.restore = sorted((r["bank_id"], r["document_id"], r["chunk_id"]) for r in rows)
    return out
