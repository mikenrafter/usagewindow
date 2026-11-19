# usagewindow

> This is currently a W.I.P. If you choose to try it out, there may be bugs, security or performance issues, or unintuitive UX. You have been warned.

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
- `uw-mcp` (`uw-mcp` binary) — stateless HTTP MCP server, with legacy stdio available.

## Development

```
nix develop --command cargo test --workspace
nix develop --command cargo clippy --all-targets -- -D warnings
nix build .#default
nix flake check --no-build
```

The normal package output remains an optimized release build. The optional
`dynamicPackages.x86_64-linux` output uses
[cargo-dyndrv](https://github.com/obsidiansystems/cargo-dyndrv) to split the
Cargo graph into content-addressed dynamic derivations, so unchanged crates can
be reused across source changes. It requires a Nix daemon with
`ca-derivations` and `dynamic-derivations` enabled; the current workstation Nix
version is too old to build it. After upgrading Nix, build it with:

```
nix --extra-experimental-features 'ca-derivations dynamic-derivations recursive-nix' \
  build .#dynamicPackages.x86_64-linux
```

Run `result/bin/uw-daemon` with `UW_DB_PATH` set to the SQLite path. The daemon listens
on `UW_LISTEN_ADDR`, defaulting to `127.0.0.1:7878`; port `0` chooses a free port.
The web login still protects TCP, including traffic proxied by Tailscale Serve.
Local tools use a private Unix socket without a token or login. The socket has
mode `0600` and its directory must have mode `0700`.

`uw`, `uw-mcp`, and `uw-hook` discover the daemon automatically. They check their
URL override first (`UW_API_URL` for `uw`, `UW_DAEMON_URL` for MCP and hooks, with
the other variable accepted as a fallback), then live sockets under
`$XDG_RUNTIME_DIR/usagewindow`, the directory containing `UW_DB_PATH`, and
`$HOME/.usagewindow`, then those directories' `endpoint` discovery files, then
`http://127.0.0.1:7878`. Overrides accept `unix:/absolute/path` and HTTP URLs.
Explicit HTTP connections still need the web login when it is configured.

The daemon defaults to `$XDG_RUNTIME_DIR/usagewindow/api.sock`, or
`<data directory>/api.sock` when no runtime directory is set. Set `UW_SOCKET_PATH`
to choose a socket and `UW_ENDPOINT_FILE` to choose the discovery file, which
otherwise lives beside the database. The discovery file records the socket and
actual web address. A system service can use `<data directory>/api.sock` so
clients with a different runtime directory still find it. `uw endpoint` prints
the selected endpoint; `uw endpoint --check` also checks API access.

Configure `uw-hook` with `UW_HOOK_PROVIDER=claude-code`, `codex`, or `cursor`.
Hooks return an empty response promptly if no daemon is available. Generic usage
polling is available through `UW_GENERIC_USAGE_COMMAND`.

The hook is harness-neutral at the executable boundary: Claude Code and Codex use
their command-hook documents, while Cursor uses native `.cursor/hooks.json` event
names. All three pass their events to the same `uw-hook` binary with the provider
environment variable above.

Run `result/bin/uw-mcp` beside the daemon; it discovers the same local socket.
MCP never opens the SQLite database. Its MCP endpoint
is `http://127.0.0.1:7880/mcp`; change the listener with `UW_MCP_LISTEN_ADDR`. The
endpoint implements MCP `2026-07-28`: each JSON-RPC request is an independent HTTP
POST with modern request metadata and the required `MCP-Protocol-Version`,
`Mcp-Method`, and, for tool calls, `Mcp-Name` headers. It supports
`server/discover`, `tools/list`, and `tools/call`. It does not create protocol sessions
or expose the 2025 GET/SSE and DELETE lifecycle endpoints.

The implementation intentionally covers only the documented envelope, discovery,
tool, and stateless HTTP behavior used by these three tools. It does not attempt the
rest of the MCP feature set, such as resources, prompts, subscriptions, MRTR, or SSE
progress responses. This avoids inventing a server framework while Rust MCP crates
catch up with the revision. The wire behavior follows the official
[MCP 2026-07-28 specification](https://modelcontextprotocol.io/specification/2026-07-28).
For old local clients, `uw-mcp --stdio` or `UW_MCP_TRANSPORT=stdio` starts the retained
newline-delimited `2024-11-05` mode.

The MCP protocol does not expose the host harness's agent-session ID: `2026-07-28`
deliberately removed protocol sessions and `Mcp-Session-Id`. For a tool call, omit
`session_id` or pass `null` to use the caller context. Stdio clients inherit
`CLAUDE_CODE_SESSION_ID`; HTTP clients may provide the same value in the
`com.usagewindow/sessionId` request metadata extension. An explicit string always
takes precedence.

HTTP mode rejects non-local browser origins by default. Set
`UW_MCP_ALLOWED_ORIGINS` to a comma-separated list of exact additional origins when a
trusted reverse proxy needs them. Keep the default loopback binding unless the
endpoint has authentication and TLS in front of it.

Automatic reseed remains off unless `UW_RESEED_AUTO=true` and all of
`UW_SUMMARIZER_BASE_URL`, `UW_SUMMARIZER_MODEL`, `UW_RESEED_MODEL`,
`UW_RESEED_ESTIMATED_COST_USD`, and `UW_WAIT_FOR_RESET_ESTIMATED_COST_USD` are set.
This makes the cost comparison explicit instead of inventing prices.
