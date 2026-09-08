# Conformance tests (baseline)

Live-daemon tests that probe the running Hindsight daemon's API surface.
**Excluded from default `make test` runs** because they require a
reachable daemon.

## Why a separate suite

| Concern | Unit tests | Conformance tests |
|---|---|---|
| Run on every PR | yes | optional |
| Need a daemon | no | yes |
| Catch upstream breaking changes | no | yes |
| Catch daemon schema evolution | no | yes |

The **pivotal test** is `test_daemon_openapi_compatibility`: it pulls
the live daemon's `openapi.json` and asserts every endpoint our tool
calls is still present with the expected method. If upstream renames or
removes a route, this fails the day the upgrade lands.

## Running

```bash
# Default daemon (http://127.0.0.1:9177)
make test-conformance

# Different daemon
HINDSIGHT_DAEMON_URL=http://other-host:9177 make test-conformance
```

If the daemon isn't reachable, every test skips — so it's safe to run
in CI without a daemon.

## Adding tests

When you add a new tool or dependency on a daemon endpoint:

1. Add the route to `required_routes` in
   `test_daemon_openapi_compatibility` (if it's a new daemon endpoint).
2. Add a dedicated conformance test if the new dependency has
   non-trivial behaviour.
3. Run `make test-conformance` — should still pass.

## Workflow before every upstream upgrade

```bash
make test-conformance                                  # baseline
pip install -U hindsight-api
systemctl --user restart hindsight-hermes               # or restart your daemon
make test-conformance                                  # any failure names the broken route
```

If a test fails after upgrade:

1. Read the failure — it names the exact route or method that broke.
2. Check upstream's changelog for the route rename/move.
3. Update our wrappers to use the new path.
4. Update `required_routes` in `test_daemon_openapi_compatibility`
   to match the new reality.
5. Re-run — should pass.
