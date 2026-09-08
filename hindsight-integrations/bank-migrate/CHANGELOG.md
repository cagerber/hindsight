# Changelog

All notable changes to bank-migrate are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/).

Versions track upstream Hindsight. The first release corresponds to the
upstream version it was tested against.

## [0.9.2] — baseline test pack

Foundation release. Ships:

- Library: typed Python client for the daemon's REST API
- CLI: every operation as a subcommand
- MCP server: lobby-aware tools for coding-agent harnesses
- Conformance test suite: live-daemon tripwires (openapi compat, MCP smoke)
- Docker fixture: hermetic test environment with deterministic seed loader

Specific fixture data (e.g., ODS cube pages, cli_builder conventions,
lobby registry entities) is shipped in subsequent change-specific
branches on top of this baseline.
