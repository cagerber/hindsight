"""SQL for the extraction checkpoint cache (fork mitigation E4); table from migration e4xcache1a2b.

``extraction_cache`` holds one chunk extraction per (bank, request hash): the parsed facts as
JSON plus the tokens the call cost, so a hit can report what it saved.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any


async def get(*, conn, fq_table: Callable[[str], str], bank_id: str, request_hash: bytes) -> dict[str, Any] | None:
    """The cached entry for ``request_hash`` (stamping the hit), or None."""
    row = await conn.fetchrow(
        f"""UPDATE {fq_table("extraction_cache")}
               SET hits = hits + 1, last_hit_at = now()
             WHERE bank_id = $1 AND request_hash = $2
         RETURNING facts, model, input_tokens, output_tokens""",
        bank_id,
        request_hash,
    )
    if row is None:
        return None
    facts = row["facts"]
    if isinstance(facts, str):  # no JSON codec registered on this connection
        facts = json.loads(facts)
    return {
        "facts": facts,
        "model": row["model"],
        "input_tokens": row["input_tokens"],
        "output_tokens": row["output_tokens"],
    }


async def put(
    *,
    conn,
    fq_table: Callable[[str], str],
    bank_id: str,
    request_hash: bytes,
    facts: list[dict[str, Any]],
    model: str | None,
    input_tokens: int,
    output_tokens: int,
) -> None:
    """Save one extraction. A concurrent writer of the same request keeps the first entry."""
    await conn.execute(
        f"""INSERT INTO {fq_table("extraction_cache")}
                (bank_id, request_hash, facts, model, input_tokens, output_tokens)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6)
            ON CONFLICT (bank_id, request_hash) DO NOTHING""",
        bank_id,
        request_hash,
        json.dumps(facts, ensure_ascii=False),
        model,
        int(input_tokens),
        int(output_tokens),
    )


async def prune(*, conn, fq_table: Callable[[str], str], older_than_seconds: int, limit: int) -> int:
    """Delete up to ``limit`` entries created more than ``older_than_seconds`` ago; returns the count."""
    rows = await conn.fetch(
        f"""DELETE FROM {fq_table("extraction_cache")}
             WHERE ctid IN (SELECT ctid FROM {fq_table("extraction_cache")}
                             WHERE created_at < now() - make_interval(secs => $1)
                             LIMIT $2)
         RETURNING 1""",
        float(older_than_seconds),
        limit,
    )
    return len(rows)
