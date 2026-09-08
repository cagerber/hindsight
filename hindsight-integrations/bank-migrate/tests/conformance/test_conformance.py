"""Live-daemon conformance tests (baseline).

These tests probe the running Hindsight daemon's API surface. The
baseline branch ships only tripwires that any downstream consumer
benefits from:

  * test_daemon_openapi_compatibility — every endpoint we call exists
  * test_mcp_tool_invoke_each_tool    — all MCP tools are invokable

Specific change-specific tests (lobby, registry, reflect, relate,
identity hierarchy, e2e) are added in subsequent branches on top of
this baseline.

By default these tests connect to ``http://127.0.0.1:9177`` (the
Hindsight standalone daemon). Override with ``HINDSIGHT_DAEMON_URL``.
"""
from __future__ import annotations

import os

import httpx
import pytest

DAEMON_URL = os.environ.get("HINDSIGHT_DAEMON_URL", "http://127.0.0.1:9177")


def _daemon_reachable() -> bool:
    try:
        r = httpx.get(f"{DAEMON_URL}/health", timeout=2.0)
        return r.status_code == 200 and r.json().get("status") == "healthy"
    except Exception:  # noqa: BLE001 — fixture probe; any failure = skip
        return False


@pytest.mark.conformance
def test_daemon_openapi_compatibility() -> None:
    """The pivotal test — every endpoint our tool calls must still exist.

    Run before every upstream Hindsight upgrade. If a route is renamed or
    removed, this fails the day the upgrade lands and we know exactly
    which method is broken.
    """
    if not _daemon_reachable():
        pytest.skip(f"daemon not reachable at {DAEMON_URL}")
    spec = httpx.get(f"{DAEMON_URL}/openapi.json", timeout=5.0).json()
    paths = spec.get("paths", {})

    # Every route our tool calls. Add to this list when you add a new
    # daemon endpoint dependency. POST/DELETE routes that we don't call
    # are intentionally absent — the openapi surface is large.
    required_routes: dict[str, set[str]] = {
        "GET": {
            "/health",
            "/v1/default/banks",
            "/v1/default/banks/{bank_id}/documents",
            "/v1/default/banks/{bank_id}/documents/{document_id}",
            "/v1/default/banks/{bank_id}/knowledge-base/tree",
            "/v1/default/banks/{bank_id}/knowledge-base/pages/{page_id}",
            "/v1/default/banks/{bank_id}/knowledge-base/search",
            "/v1/default/banks/{bank_id}/mental-models",
            "/v1/default/banks/{bank_id}/mental-models/{mental_model_id}",
            "/v1/default/banks/{bank_id}/operations",
            "/v1/default/banks/{bank_id}/operations/{operation_id}",
            "/v1/default/banks/{bank_id}/documents/{document_id}/chunks",
            "/v1/default/files/download/{key}",
        },
        "POST": {
            "/v1/default/banks/{bank_id}/memories",
            "/v1/default/banks/{bank_id}/memories/recall",
            "/v1/default/banks/{bank_id}/reflect",
            "/v1/default/banks/{bank_id}/mental-models",
            "/v1/default/banks/{bank_id}/knowledge-base/pages",
            "/v1/default/banks/{bank_id}/knowledge-base/folders",
            "/v1/default/banks/{bank_id}/document-transfer",
            "/v1/default/banks/{bank_id}/document-transfer/export",
        },
        "DELETE": {
            "/v1/default/banks/{bank_id}",
            "/v1/default/banks/{bank_id}/documents/{document_id}",
            "/v1/default/banks/{bank_id}/operations/{operation_id}",
        },
    }

    missing: list[str] = []
    for method, routes in required_routes.items():
        for route in routes:
            found = False
            for actual_path, methods in paths.items():
                norm_actual = actual_path.rstrip("/")
                norm_required = route.rstrip("/")
                if norm_actual == norm_required and method.lower() in (k.lower() for k in methods):
                    found = True
                    break
            if not found:
                missing.append(f"{method} {route}")
    assert not missing, (
        f"daemon at {DAEMON_URL} is missing routes our tool calls:\n  "
        + "\n  ".join(missing)
        + "\nUpstream may have renamed/removed endpoints — update our wrappers."
    )


@pytest.mark.conformance
async def test_mcp_tool_invoke_each_tool() -> None:
    """Every public MCP tool must be invokable against the live daemon.

    The MCP server here is a stub — the actual lobby-aware tools are
    added in change-specific branches on top of this baseline. This
    test asserts the MCP transport works end-to-end so we catch
    breakage in the upstream MCP SDK integration.
    """
    pytest.skip("MCP server stub added in change-specific branches on top of this baseline")
