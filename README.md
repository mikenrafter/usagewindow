# usagewindow

Harness-agnostic usage-window tracking, resume scheduling, and compaction assistance for
coding-agent CLIs (Claude Code, Codex, and others via a pluggable adapter).

Status: the daemon, CLI/API, adapters, MCP server, and Nix package build as one workspace.
See `docs/architecture.md` for the design and the research notes for capability gaps.

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
nix develop --command cargo test --workspace
nix develop --command cargo clippy --all-targets -- -D warnings
nix build .#default
```

Run `result/bin/uw-daemon` with `UW_DB_PATH` set to the SQLite path. The daemon listens
on `UW_LISTEN_ADDR`, defaulting to `127.0.0.1:7878`. Configure `uw-hook` with
`UW_HOOK_PROVIDER=claude-code` or `UW_HOOK_PROVIDER=codex`; `UW_DAEMON_URL` defaults to
the matching local address. Generic usage polling is available through
`UW_GENERIC_USAGE_COMMAND`.

Automatic reseed remains off unless `UW_RESEED_AUTO=true` and all of
`UW_SUMMARIZER_BASE_URL`, `UW_SUMMARIZER_MODEL`, `UW_RESEED_MODEL`,
`UW_RESEED_ESTIMATED_COST_USD`, and `UW_WAIT_FOR_RESET_ESTIMATED_COST_USD` are set.
This makes the cost comparison explicit instead of inventing prices.
