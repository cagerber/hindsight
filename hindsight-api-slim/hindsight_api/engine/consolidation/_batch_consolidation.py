"""
Batch consolidation: submit all scope groups as one xiaomimimo Batch API job.

Each scope group (memories sharing the same observation tags) becomes one batch request
with custom_id = f"consolidation_{bank_id}_{scope_idx}" so results can be routed
back after polling. Memories stay alive (no consolidation_failed_at) while the batch
is in-flight; they are only marked failed if the batch job itself fails permanently.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..db_utils import acquire_with_retry
from ..llm_wrapper import sanitize_llm_output
from ..memories import get_memories
from ..memory_engine import fq_table
from .consolidator import (
    _ConsolidationBatchResponse,
    _build_response_model,
    _consolidation_batch_key,
    _count_unconsolidated_rows,
    _fetch_unconsolidated_rows,
    build_consolidation_input,
    build_consolidation_system_prompt,
)

if TYPE_CHECKING:
    from ..memory_engine import MemoryEngine

logger = logging.getLogger(__name__)


async def run_consolidation_job_batch(
    memory_engine: "MemoryEngine",
    bank_id: str,
    request_context: Any,
    config: Any,
    llm_config: Any,
    operation_id: str | None,
    observation_scopes: list[list[str]] | None,
) -> dict[str, Any]:
    """
    Submit all consolidation scope groups as one xiaomimimo Batch API job.

    1. Fetch and group all unconsolidated memories by observation scope
    2. Submit one batch job covering all scope groups
    3. Poll until the batch completes (up to 24h window)
    4. Retrieve results, route by custom_id, apply each scope's result
    5. Mark memories consolidated (or failed if the batch permanently failed)

    Falls back to sync mode if batch submission fails.
    """
    pool = memory_engine._backend
    max_memories_per_batch = getattr(config, "consolidation_batch_size", 50)
    llm_batch_size = max(1, getattr(config, "consolidation_llm_batch_size", 1))

    # Count total unconsolidated memories
    async with acquire_with_retry(pool) as conn:
        total_count = await _count_unconsolidated_rows(
            conn, bank_id, ["experience", "world"], observation_scopes
        )

    if total_count == 0:
        return {"status": "no_new_memories", "bank_id": bank_id, "memories_processed": 0}

    logger.info(f"[CONSOLIDATION BATCH] bank={bank_id} total_unconsolidated={total_count}")

    # Fetch all unconsolidated memories in one shot
    async with acquire_with_retry(pool) as conn:
        memories = await _fetch_unconsolidated_rows(
            conn,
            bank_id,
            ["experience", "world"],
            max_memories_per_batch,
            observation_scopes,
            1,
        )

    if not memories:
        return {"status": "no_new_memories", "bank_id": bank_id, "memories_processed": 0}

    # Group by observation scope
    tag_groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for m in memories:
        key = _consolidation_batch_key(dict(m))
        tag_groups.setdefault(key, []).append(dict(m))

    # Split each tag group into LLM batches respecting llm_batch_size
    scope_groups: list[tuple[tuple[str, ...], list[dict[str, Any]]]] = []
    for tag_key, group_memories in tag_groups.items():
        for i in range(0, len(group_memories), llm_batch_size):
            chunk = group_memories[i : i + llm_batch_size]
            scope_groups.append((tag_key, chunk))

    logger.info(
        f"[CONSOLIDATION BATCH] bank={bank_id} {len(memories)} memories -> "
        f"{len(scope_groups)} scope groups"
    )

    # Build batch requests: one per scope group
    batch_requests: list[dict[str, Any]] = []
    for idx, (tag_key, group_memories) in enumerate(scope_groups):
        custom_id = f"consolidation_{bank_id}_{idx}"
        request_body = _build_consolidation_batch_request_body(
            memories=group_memories,
            tag_key=tag_key,
            bank_id=bank_id,
            config=config,
            llm_config=llm_config,
        )
        batch_requests.append({
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": request_body,
        })

    if not batch_requests:
        return {"status": "no_new_memories", "bank_id": bank_id, "memories_processed": 0}

    # Get the batch-capable provider implementation
    try:
        batch_impl = await llm_config.batch_provider_impl()
    except Exception as exc:
        logger.warning(
            f"[CONSOLIDATION BATCH] bank={bank_id} could not get batch provider impl: {exc}. "
            "Falling back to sync."
        )
        return await _run_sync_fallback(
            memory_engine, bank_id, request_context, config, llm_config, operation_id, observation_scopes
        )

    if batch_impl is None:
        logger.warning(
            f"[CONSOLIDATION BATCH] bank={bank_id} batch provider is None. "
            "Falling back to sync."
        )
        return await _run_sync_fallback(
            memory_engine, bank_id, request_context, config, llm_config, operation_id, observation_scopes
        )

    # Submit the batch
    try:
        batch_metadata = await batch_impl.submit_batch(batch_requests)
    except Exception as exc:
        logger.warning(
            f"[CONSOLIDATION BATCH] bank={bank_id} batch submission failed: {exc}. "
            "Falling back to sync."
        )
        return await _run_sync_fallback(
            memory_engine, bank_id, request_context, config, llm_config, operation_id, observation_scopes
        )

    batch_id = batch_metadata["batch_id"]
    poll_interval = getattr(config, "consolidation_batch_poll_interval_seconds", 60) or 60

    # Store batch state in operation metadata for crash recovery
    if operation_id and pool:
        batch_state = {
            "batch_id": batch_id,
            "batch_provider": getattr(batch_impl, "provider", "unknown"),
            "batch_account": getattr(batch_impl, "batch_account_key", "unknown"),
            "scope_groups_count": len(scope_groups),
            "memories_count": len(memories),
        }
        table = fq_table("async_operations", getattr(config, "schema", None))
        async with acquire_with_retry(pool) as conn:
            await conn.execute(
                f"UPDATE {table} SET result_metadata = result_metadata || $1::jsonb "
                f"WHERE operation_id = $2",
                json.dumps(batch_state),
                operation_id,
            )
        logger.info(f"[CONSOLIDATION BATCH] bank={bank_id} stored batch state for operation {operation_id}")

    # Poll until batch completes
    logger.info(
        f"[CONSOLIDATION BATCH] bank={bank_id} batch={batch_id} submitted, "
        f"polling every {poll_interval}s"
    )

    start_time = time.perf_counter()
    while True:
        status_info = await batch_impl.get_batch_status(batch_id)
        status = status_info["status"]
        elapsed = time.perf_counter() - start_time

        counts = status_info.get("request_counts", {})
        completed = counts.get("completed", 0)
        total = counts.get("total", 0)

        logger.info(
            f"[CONSOLIDATION BATCH] bank={bank_id} batch={batch_id} "
            f"status={status} completed={completed}/{total} elapsed={elapsed:.0f}s"
        )

        if status == "completed":
            break
        elif status in ("failed", "expired", "cancelled"):
            error_msg = status_info.get("errors", "Unknown error")
            logger.error(
                f"[CONSOLIDATION BATCH] bank={bank_id} batch={batch_id} "
                f"failed with status {status}: {error_msg}"
            )
            await _mark_memories_failed(pool, bank_id, memories)
            return {
                "status": "batch_failed",
                "bank_id": bank_id,
                "batch_id": batch_id,
                "error": error_msg,
                "memories_processed": 0,
            }

        await asyncio.sleep(poll_interval)

    logger.info(
        f"[CONSOLIDATION BATCH] bank={bank_id} batch={batch_id} "
        f"completed in {time.perf_counter() - start_time:.0f}s, retrieving results"
    )

    # Retrieve and sanitize results
    raw_results = await batch_impl.retrieve_batch_results(batch_id)
    batch_results = [sanitize_llm_output(r) for r in raw_results]
    results_by_id = {result["custom_id"]: result for result in batch_results}

    # Apply results per scope group
    stats: dict[str, int] = {
        "memories_processed": 0,
        "observations_created": 0,
        "observations_updated": 0,
        "observations_merged": 0,
        "observations_deleted": 0,
        "actions_executed": 0,
        "skipped": 0,
        "memories_failed": 0,
    }

    for idx, (tag_key, group_memories) in enumerate(scope_groups):
        custom_id = f"consolidation_{bank_id}_{idx}"
        result = results_by_id.get(custom_id)

        if not result:
            logger.warning(
                f"[CONSOLIDATION BATCH] bank={bank_id} missing result for {custom_id}"
            )
            await _mark_memories_failed(pool, bank_id, group_memories)
            stats["memories_failed"] += len(group_memories)
            continue

        if result.get("error"):
            logger.warning(
                f"[CONSOLIDATION BATCH] bank={bank_id} error in {custom_id}: {result['error']}"
            )
            await _mark_memories_failed(pool, bank_id, group_memories)
            stats["memories_failed"] += len(group_memories)
            continue

        try:
            scope_stats = await _apply_consolidation_result(
                memory_engine=memory_engine,
                bank_id=bank_id,
                request_context=request_context,
                memories=group_memories,
                tag_key=tag_key,
                batch_response=result,
                config=config,
                llm_config=llm_config,
            )
            for k, v in scope_stats.items():
                stats[k] = stats.get(k, 0) + v
        except Exception as exc:
            logger.error(
                f"[CONSOLIDATION BATCH] bank={bank_id} failed to apply result for "
                f"{custom_id}: {exc}",
                exc_info=True,
            )
            await _mark_memories_failed(pool, bank_id, group_memories)
            stats["memories_failed"] += len(group_memories)

    logger.info(
        f"[CONSOLIDATION BATCH] bank={bank_id} completed: "
        f"{stats['memories_processed']} processed, "
        f"{stats['observations_created']} created, "
        f"{stats['observations_updated']} updated, "
        f"{stats['memories_failed']} failed"
    )

    return {"status": "completed", "bank_id": bank_id, **stats}


def _build_consolidation_batch_request_body(
    memories: list[dict[str, Any]],
    tag_key: tuple[str, ...],
    bank_id: str,
    config: Any,
    llm_config: Any,
) -> dict[str, Any]:
    """Build a single batch API request body for a consolidation scope group."""
    observations_text = "[]"

    def _fact_line(m: dict[str, Any]) -> str:
        text = f"[{m['id']}] {m['text']}"
        temporal_parts = []
        if m.get("occurred_start"):
            temporal_parts.append(f"occurred_start={m['occurred_start']}")
        if m.get("occurred_end"):
            temporal_parts.append(f"occurred_end={m['occurred_end']}")
        if m.get("mentioned_at"):
            temporal_parts.append(f"mentioned_at={m['mentioned_at']}")
        if temporal_parts:
            text += f" ({', '.join(temporal_parts)})"
        return text

    facts_lines = "\n".join(_fact_line(m) for m in memories)
    observation_capacity_note = None

    system_prompt = build_consolidation_system_prompt(
        llm_output_language=getattr(config, "llm_output_language", None),
    )
    user_content = build_consolidation_input(
        facts_text=facts_lines,
        observations_text=observations_text,
        observations_mission=getattr(config, "observations_mission", None),
        observation_capacity_note=observation_capacity_note,
    )

    response_model = _build_response_model(
        max_creates=None,
        fact_ids=[str(m["id"]) for m in memories],
        supports_max_items=getattr(config, "llm_supports_max_items", False),
    )

    temperature = getattr(config, "llm_temperature_consolidation", 0.0)

    body = {
        "model": llm_config.model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
    }

    # Add response format — extract schema from the Pydantic model so it's JSON-serializable.
    if hasattr(response_model, "model_json_schema"):
        from ..structured_output import provider_json_schema, strict_json_schema
        retain_strict_schema = getattr(config, "llm_strict_schema_consolidation", False)
        schema = strict_json_schema(response_model) if retain_strict_schema else provider_json_schema(response_model)
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "response", "schema": schema, "strict": retain_strict_schema},
        }

    return body


async def _apply_consolidation_result(
    memory_engine: "MemoryEngine",
    bank_id: str,
    request_context: Any,
    memories: list[dict[str, Any]],
    tag_key: tuple[str, ...],
    batch_response: dict[str, Any],
    config: Any,
    llm_config: Any,
) -> dict[str, int]:
    """
    Apply a single scope group's batch consolidation result.

    Parses the batch LLM response, builds a _BatchLLMResult, and calls
    _process_memory_batch with precomputed_llm_result so it skips the LLM call
    but still applies all writes (creates, updates, deletes) through the full
    write pipeline including transactions, embedding, and dedup.
    """
    from .consolidator import _BatchLLMResult, _process_memory_batch

    response_body = batch_response.get("response", {}).get("body", {})
    choices = response_body.get("choices", [])
    if not choices:
        return _empty_stats(memories)

    message = choices[0].get("message", {})
    content = message.get("content", "")
    if not content:
        return _empty_stats(memories)

    # Parse the JSON response from the LLM output
    try:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if not match:
            logger.warning(f"[CONSOLIDATION BATCH] no JSON found in content: {content[:200]}")
            return _empty_stats(memories)
        json_str = match.group(0)
        parsed = json.loads(json_str)
    except Exception as exc:
        logger.warning(f"[CONSOLIDATION BATCH] failed to parse response: {exc}")
        return _empty_stats(memories)

    # Validate using _ConsolidationBatchResponse
    try:
        llm_response = _ConsolidationBatchResponse.model_validate(parsed)
    except Exception as exc:
        logger.warning(f"[CONSOLIDATION BATCH] response validation failed: {exc}")
        return _empty_stats(memories)

    # Build _BatchLLMResult from the parsed response
    precomputed = _BatchLLMResult(
        creates=llm_response.creates,
        updates=llm_response.updates,
        deletes=llm_response.deletes,
        obs_count=len(llm_response.creates) + len(llm_response.updates),
        prompt_chars=len(content),
        failed=False,
        failed_attempts=0,
    )

    memory_ids = [str(m["id"]) for m in memories]
    pool = memory_engine._backend

    if pool is None:
        logger.error(f"[CONSOLIDATION BATCH] bank={bank_id} pool is None, cannot apply result")
        return _empty_stats(memories)

    # Call _process_memory_batch with the precomputed result.
    # This skips the LLM call but runs the full write pipeline (embedding, transactions, dedup).
    _, _, _ = await _process_memory_batch(
        pool=pool,
        memory_engine=memory_engine,
        llm_config=llm_config,
        bank_id=bank_id,
        memories=memories,
        request_context=request_context,
        perf=None,
        config=config,
        obs_tags_override=list(tag_key),
        mark_consolidated_ids=memory_ids,
        precomputed_llm_result=precomputed,
    )

    return {
        "memories_processed": len(memories),
        "observations_created": len(llm_response.creates),
        "observations_updated": len(llm_response.updates),
        "observations_merged": 0,
        "observations_deleted": len(llm_response.deletes),
        "actions_executed": len(llm_response.creates) + len(llm_response.updates) + len(llm_response.deletes),
        "skipped": 0,
        "memories_failed": 0,
    }


def _empty_stats(memories: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "memories_processed": 0,
        "observations_created": 0,
        "observations_updated": 0,
        "observations_merged": 0,
        "observations_deleted": 0,
        "actions_executed": 0,
        "skipped": 0,
        "memories_failed": len(memories),
    }


async def _mark_memories_failed(pool: Any, bank_id: str, memories: list[dict[str, Any]]) -> None:
    """Mark memories as consolidation_failed_at=now."""
    if not memories:
        return
    memory_ids = [str(m["id"]) for m in memories]
    store = get_memories()
    now = datetime.now(timezone.utc)
    async with acquire_with_retry(pool) as conn:
        await store.mark_consolidated(
            conn=conn,
            fq_table=lambda t: fq_table(t, None),
            bank_id=bank_id,
            unit_ids=memory_ids,
            when=now,
            failed=True,
        )


async def _run_sync_fallback(
    memory_engine: "MemoryEngine",
    bank_id: str,
    request_context: Any,
    config: Any,
    llm_config: Any,
    operation_id: str | None,
    observation_scopes: list[list[str]] | None,
) -> dict[str, Any]:
    """Fall back to sync consolidation when batch submission fails."""
    logger.info(f"[CONSOLIDATION BATCH] bank={bank_id} falling back to sync consolidation")
    from .consolidator import _run_consolidation_job

    # Call _run_consolidation_job directly to bypass the batch check that
    # run_consolidation_job would apply (which would re-route to batch again).
    pending_refresh_tags = None
    return await _run_consolidation_job(
        memory_engine=memory_engine,
        bank_id=bank_id,
        request_context=request_context,
        config=config,
        llm_config=llm_config,
        operation_id=operation_id,
        observation_scopes=observation_scopes,
        pending_refresh_tags=pending_refresh_tags,
        use_batch=False,
    )
