"""Add the source-span index for the bank-wide paragraph filter (fork mitigation E3)

Three PostgreSQL-only tables back ``engine/retain/span_filter.py``:

* ``chunk_paragraphs`` - one row per paragraph a chunk's extraction actually saw, keyed by a
  12-byte blake2b of the paragraph text, plus the chunk's content hash at indexing time so a row
  left behind by an overwrite (chunk ids are reused by a replace) never matches.
* ``chunk_span_index`` - which version of each chunk has been indexed, so the maintenance
  backfill can tell an unindexed or overwritten chunk from one with no qualifying paragraphs.
* ``span_refs`` - one row per paragraph a chunk showed the LLM as an ``[already retained: ...]``
  marker, naming the chunk whose facts cover it. Rows cascade with the referring chunk; the
  source side is deliberately not a foreign key, so deleting a source leaves an orphan row for
  the reconcile job to repoint or restore instead of silently losing the facts.

All three cascade from ``chunks``. Oracle is a no-op: the filter is gated off there.

Revision ID: e3spn0a1b2c3
Revises: kbtok1a2b3c4
Create Date: 2026-11-22
"""

from collections.abc import Sequence

from alembic import context, op

from hindsight_api.alembic._dialect import run_for_dialect

revision: str = "e3spn0a1b2c3"
down_revision: str | Sequence[str] | None = "kbtok1a2b3c4"
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
        CREATE TABLE IF NOT EXISTS {s}chunk_paragraphs (
            chunk_id   TEXT  NOT NULL REFERENCES {s}chunks(chunk_id) ON DELETE CASCADE,
            para_hash  BYTEA NOT NULL,
            bank_id    TEXT  NOT NULL,
            chunk_hash TEXT,
            PRIMARY KEY (chunk_id, para_hash)
        )
        """
    )
    op.execute(f"CREATE INDEX IF NOT EXISTS idx_chunk_paragraphs_bank_hash ON {s}chunk_paragraphs (bank_id, para_hash)")
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {s}chunk_span_index (
            chunk_id   TEXT PRIMARY KEY REFERENCES {s}chunks(chunk_id) ON DELETE CASCADE,
            bank_id    TEXT NOT NULL,
            chunk_hash TEXT,
            indexed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {s}span_refs (
            referring_chunk_id   TEXT  NOT NULL REFERENCES {s}chunks(chunk_id) ON DELETE CASCADE,
            para_hash            BYTEA NOT NULL,
            bank_id              TEXT  NOT NULL,
            source_chunk_id      TEXT  NOT NULL,
            chars                INTEGER NOT NULL DEFAULT 0,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
            orphaned_at          TIMESTAMPTZ,
            restore_requested_at TIMESTAMPTZ,
            PRIMARY KEY (referring_chunk_id, para_hash)
        )
        """
    )
    op.execute(f"CREATE INDEX IF NOT EXISTS idx_span_refs_source ON {s}span_refs (source_chunk_id)")
    op.execute(
        f"CREATE INDEX IF NOT EXISTS idx_span_refs_orphaned ON {s}span_refs (orphaned_at) WHERE orphaned_at IS NOT NULL"
    )


def _pg_downgrade() -> None:
    s = _pg_schema_prefix()
    op.execute(f"DROP TABLE IF EXISTS {s}span_refs")
    op.execute(f"DROP TABLE IF EXISTS {s}chunk_span_index")
    op.execute(f"DROP TABLE IF EXISTS {s}chunk_paragraphs")


def upgrade() -> None:
    run_for_dialect(pg=_pg_upgrade)  # oracle slot intentionally absent: the span filter is PostgreSQL-only


def downgrade() -> None:
    run_for_dialect(pg=_pg_downgrade)
