"""Seed loader for the Hindsight conformance test fixture.

Reads `seed_banks.yaml` and applies each document to the live daemon via
the standard retain endpoint. Idempotent: skips banks that already have
non-empty fact_count.

Run from a container entrypoint:

    python loader.py --daemon http://127.0.0.1:8888 --config /seed/seed_banks.yaml

Or locally against any daemon:

    python loader.py --daemon http://127.0.0.1:9177 --config ../seed_banks.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [seed] %(message)s")
log = logging.getLogger("seed")


async def wait_for_daemon(daemon: str, timeout: float = 60.0) -> None:
    """Block until /health returns 200 with database=connected."""
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                r = await client.get(f"{daemon}/health")
                if r.status_code == 200:
                    body = r.json()
                    if body.get("status") == "healthy" and body.get("database") == "connected":
                        log.info("daemon healthy: %s", body)
                        return
            except Exception as e:  # noqa: BLE001
                log.debug("daemon not ready: %s", e)
            await asyncio.sleep(1.0)
    raise TimeoutError(f"daemon at {daemon} did not become healthy within {timeout}s")


async def bank_has_facts(daemon: str, bank_id: str, client: httpx.AsyncClient) -> bool:
    """Return True if bank already has any facts."""
    try:
        r = await client.get(f"{daemon}/v1/default/banks/{bank_id}")
        if r.status_code == 200:
            return (r.json().get("fact_count") or 0) > 0
    except Exception:  # noqa: BLE001
        pass
    return False


async def seed_document(
    daemon: str, bank_id: str, doc: dict, client: httpx.AsyncClient
) -> None:
    """Retain a single document against the daemon."""
    payload = {
        "items": [
            {
                "content": doc["content"],
                "context": doc.get("metadata", {}).get("doc_type", "fixture"),
                "fact_type": "world",
                "document_id": doc["document_id"],
                "tags": doc.get("tags", []),
                "metadata": doc.get("metadata", {}),
            }
        ],
        "document_id": doc["document_id"],
    }
    r = await client.post(
        f"{daemon}/v1/default/banks/{bank_id}/memories",
        json=payload,
        timeout=30.0,
    )
    if r.status_code not in (200, 201, 202):
        log.error(
            "FAILED to retain %s in %s: %s %s",
            doc["document_id"], bank_id, r.status_code, r.text[:200],
        )
        raise RuntimeError(f"retain failed for {doc['document_id']}")
    log.info("seeded %s in %s", doc["document_id"], bank_id)


async def seed_bank(daemon: str, bank: dict, client: httpx.AsyncClient, *, force: bool = False) -> None:
    bank_id = bank["bank_id"]
    if not force and await bank_has_facts(daemon, bank_id, client):
        log.info("skipping %s (already has facts — set --force to reseed)", bank_id)
        return
    if force:
        try:
            await client.delete(f"{daemon}/v1/default/banks/{bank_id}", timeout=10.0)
            log.info("deleted %s for force-reseed", bank_id)
        except Exception:  # noqa: BLE001
            pass
    for doc in bank["documents"]:
        await seed_document(daemon, bank_id, doc, client)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the Hindsight conformance fixture.")
    parser.add_argument("--daemon", default=os.environ.get("HINDSIGHT_API_URL", "http://127.0.0.1:8888"))
    parser.add_argument("--config", default="/seed/seed_banks.yaml")
    parser.add_argument("--force", action="store_true", help="Delete existing banks before reseeding.")
    parser.add_argument("--wait", type=float, default=120.0, help="Max seconds to wait for daemon.")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        log.error("config not found: %s", config_path)
        sys.exit(1)
    cfg = yaml.safe_load(config_path.read_text())
    log.info("loaded %d banks from %s", len(cfg.get("banks") or []), config_path)

    await wait_for_daemon(args.daemon, timeout=args.wait)
    async with httpx.AsyncClient() as client:
        for bank in cfg.get("banks") or []:
            await seed_bank(args.daemon, bank, client, force=args.force)
    log.info("seed complete: %d banks", len(cfg.get("banks") or []))


if __name__ == "__main__":
    asyncio.run(main())
