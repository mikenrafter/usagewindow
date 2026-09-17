# usagewindow

Harness-agnostic usage-window tracking, resume scheduling, and compaction assistance for
coding-agent CLIs (Claude Code, Codex, and others via a pluggable adapter).

Status: early scaffolding — see `docs/architecture.md` for the design and build-order plan.

## Crates

- `uw-core` — public library: domain types + the `HarnessAdapter` trait.
- `uw-adapters` — harness adapters (Claude Code, Codex, generic fallback).
- `uw-policy` — pure threshold/trigger logic (no I/O).
- `uw-store` — SQLite persistence.
- `uw-daemon` (`uw-daemon` binary) — the long-running service.
- `uw-cli` (`uw` binary) — CLI, thin client over the daemon's local API.
- `uw-web` — web UI server.
- `uw-mcp` (`uw-mcp` binary) — MCP server for harnesses that support it.

## Development

```
cargo test
cargo clippy --all-targets -- -D warnings
```
