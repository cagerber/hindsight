"""Extraction checkpoint cache (fork mitigation E4).

A chunk's LLM extraction is the expensive step of a retain, and it is paid before anything is
committed: a worker timeout, a cancellation, a crash or a ConcurrentAppendConflict after the call
returns throws the result away, and the retry pays for the same call again. Sub-batch commit
recovery only covers chunks already stored.

While a retain runs, this cache saves each chunk's parsed facts the moment the LLM returns, keyed
by a 16-byte blake2b of the exact request: provider and model, system prompt, user message
(which carries the chunk text, its position, event date, context and metadata), response schema,
extraction mode and temperature. The next call with the same request reads the facts back and
makes no LLM call. Because the key is the whole request, identical text in another document only
hits when its context, date and position match too, so the cache never changes what the model
was asked, only whether it is asked twice.

Off by default (``HINDSIGHT_API_RETAIN_EXTRACTION_CACHE``). Active only for banks whose store
keeps the cache (the Postgres store), never for a forced re-extract (which exists to ask again),
and never for a chunk with inline attachments (the images are not part of the key). Entries are
pruned after ``HINDSIGHT_API_RETAIN_EXTRACTION_CACHE_TTL_SECONDS`` by the maintenance loop.

A cache failure never fails a retain: a lookup or save error is logged and the call proceeds.

The cache rides a context variable set around one document's retain in ``retain_batch``: the
extraction tasks it spawns inherit it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_KEY_VERSION = "e4v1"  # bump when the cached payload or what the key covers changes

_ACTIVE: ContextVar[ExtractionCache | None] = ContextVar("retain_extraction_cache", default=None)


def current() -> ExtractionCache | None:
    """The cache of the retain running in this context, if any."""
    return _ACTIVE.get()


def request_hash(
    *,
    provider: str | None,
    model: str | None,
    system_prompt: str,
    user_message: str,
    response_schema: Any,
    extraction_mode: str | None,
    temperature: Any,
) -> bytes:
    """Identity of one extraction request: everything that decides what the model is asked."""
    try:
        schema = json.dumps(response_schema.model_json_schema(), sort_keys=True)
    except Exception:  # noqa: BLE001 - a schema we cannot render still has a stable name
        schema = getattr(response_schema, "__qualname__", repr(response_schema))
    payload = json.dumps(
        [_KEY_VERSION, provider, model, extraction_mode, str(temperature), system_prompt, user_message, schema],
        ensure_ascii=False,
    )
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).digest()


@dataclass
class ExtractionCache:
    """Per-document cache state for one ``retain_batch`` call."""

    pool: Any
    store: Any
    bank_id: str
    hits: int = 0
    saves: int = 0
    saved_input_tokens: int = 0
    saved_output_tokens: int = 0

    async def get(self, key: bytes) -> dict[str, Any] | None:
        """The cached entry for ``key``, or None (including on any error)."""
        from ..db_utils import acquire_with_retry

        try:
            async with acquire_with_retry(self.pool, max_retries=1) as conn:
                entry = await self.store.extraction_cache_get(conn=conn, bank_id=self.bank_id, request_hash=key)
        except Exception as e:  # noqa: BLE001 - an optimisation; never fail the retain over it
            logger.warning(f"[extraction-cache] lookup failed for bank {self.bank_id}: {e}")
            return None
        if entry is not None:
            self.hits += 1
            self.saved_input_tokens += int(entry.get("input_tokens") or 0)
            self.saved_output_tokens += int(entry.get("output_tokens") or 0)
        return entry

    async def put(
        self, key: bytes, *, facts: list[dict[str, Any]], model: str | None, input_tokens: int, output_tokens: int
    ) -> None:
        """Save one extraction; errors are logged, never raised."""
        from ..db_utils import acquire_with_retry

        try:
            async with acquire_with_retry(self.pool, max_retries=1) as conn:
                await self.store.extraction_cache_put(
                    conn=conn,
                    bank_id=self.bank_id,
                    request_hash=key,
                    facts=facts,
                    model=model,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                )
            self.saves += 1
        except Exception as e:  # noqa: BLE001 - an optimisation; never fail the retain over it
            logger.warning(f"[extraction-cache] save failed for bank {self.bank_id}: {e}")


def for_retain(pool: Any, bank_id: str, *, force_reextract: bool) -> ExtractionCache | None:
    """The cache for one document's retain, or None when it is off for this bank."""
    from ...config import get_config
    from ..memories import get_memories

    if force_reextract or pool is None:
        return None
    if not get_config().retain_extraction_cache:  # static, server-level switch
        return None
    store = get_memories()
    if store.store_owned_for(bank_id) or not store.extraction_cache_for(bank_id):
        return None
    return ExtractionCache(pool=pool, store=store, bank_id=bank_id)


@contextmanager
def activated(cache: ExtractionCache | None) -> Iterator[ExtractionCache | None]:
    """Make ``cache`` the current cache for the enclosed retain (and the tasks it spawns)."""
    token = _ACTIVE.set(cache)
    try:
        yield cache
    finally:
        _ACTIVE.reset(token)
        if cache is not None and (cache.hits or cache.saves):
            logger.info(
                f"[extraction-cache] bank {cache.bank_id}: {cache.hits} hit(s) saved "
                f"{cache.saved_input_tokens}+{cache.saved_output_tokens} tokens, {cache.saves} saved"
            )
