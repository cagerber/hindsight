"""Add the extraction checkpoint cache (fork mitigation E4)

One PostgreSQL-only table backs ``engine/retain/extraction_cache.py``: each chunk's parsed
extraction, saved the moment the LLM returns and keyed by a 16-byte blake2b of the exact request
(model, system prompt, user message, response schema). A retry, a timed-out run or an unchanged
re-send of the same chunk reads it back instead of paying for the call again.

Deliberately not tied to ``chunks``: an entry is written before its chunk is stored (that is the
point - a run that dies after the call still leaves it behind), so the maintenance loop prunes
entries by age instead of a cascade. Oracle is a no-op: the cache is gated off there.

Revision ID: e4xcache1a2b
Revises: e3spn0a1b2c3
Create Date: 2026-11-22
"""

from collections.abc import Sequence

from alembic import context, op

from hindsight_api.alembic._dialect import run_for_dialect

revision: str = "e4xcache1a2b"
down_revision: str | Sequence[str] | None = "e3spn0a1b2c3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _pg_schema_prefix() -> str:
    """Schema-qualifier for raw SQL on PG (multi-tenant search_path)."""
    schema = context.config.get_main_option("target_schema")
    return f'"{schema}".' if schema else ""


def _pg_upgrade() -> None:
    s = _pg_schema_prefix()
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {s}extraction_cache (
            bank_id       TEXT        NOT NULL,
            request_hash  BYTEA       NOT NULL,
            facts         JSONB       NOT NULL,
            model         TEXT,
            input_tokens  INTEGER     NOT NULL DEFAULT 0,
            output_tokens INTEGER     NOT NULL DEFAULT 0,
            hits          INTEGER     NOT NULL DEFAULT 0,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_hit_at   TIMESTAMPTZ,
            PRIMARY KEY (bank_id, request_hash)
        )
        """
    )
    op.execute(f"CREATE INDEX IF NOT EXISTS idx_extraction_cache_created ON {s}extraction_cache (created_at)")


def _pg_downgrade() -> None:
    s = _pg_schema_prefix()
    op.execute(f"DROP TABLE IF EXISTS {s}extraction_cache")


def upgrade() -> None:
    run_for_dialect(pg=_pg_upgrade)  # oracle slot intentionally absent: the cache is PostgreSQL-only


def downgrade() -> None:
    run_for_dialect(pg=_pg_downgrade)
