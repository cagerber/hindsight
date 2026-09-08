"""Entrypoint script: start the daemon, run seed, wait for shutdown signal.

Used as the ENTRYPOINT in Dockerfile.fixture. Spawns the daemon in the
background, runs the seed loader, then waits indefinitely.

For a *test fixture* we control the lifecycle explicitly so the seed
runs deterministically before any test client connects.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [entrypoint] %(message)s")
log = logging.getLogger("entrypoint")

DAEMON_URL = os.environ.get("HINDSIGHT_API_URL", "http://127.0.0.1:8888")
DAEMON_PORT = os.environ.get("HINDSIGHT_API_PORT", "8888")
SEED_CONFIG = os.environ.get("SEED_CONFIG", "/seed/seed_banks.yaml")
SEED_FORCE = os.environ.get("SEED_FORCE", "false").lower() == "true"


def start_daemon() -> subprocess.Popen:
    """Start the standalone Hindsight daemon in the background."""
    log.info("starting daemon (port=%s, force-seed=%s)", DAEMON_PORT, SEED_FORCE)
    return subprocess.Popen(
        ["/app/start-all.sh"],
        env={**os.environ, "HINDSIGHT_API_PORT": DAEMON_PORT},
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )


async def run_seed() -> None:
    """Run the seed loader."""
    args = [
        sys.executable, "/seed/loader.py",
        "--daemon", DAEMON_URL,
        "--config", SEED_CONFIG,
    ]
    if SEED_FORCE:
        args.append("--force")
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"seed loader failed with exit code {proc.returncode}")


async def main() -> None:
    daemon_proc = start_daemon()
    try:
        await run_seed()
    except Exception as e:
        log.error("seed failed: %s — killing daemon", e)
        daemon_proc.terminate()
        daemon_proc.wait()
        sys.exit(1)
    log.info("seed complete; daemon running on %s", DAEMON_URL)
    stop = asyncio.Event()

    def _sigterm(*_):
        log.info("received signal — shutting down")
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _sigterm)

    await stop.wait()
    log.info("terminating daemon")
    daemon_proc.terminate()
    daemon_proc.wait()


if __name__ == "__main__":
    asyncio.run(main())
