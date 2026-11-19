# usagewindow MCP server

Kasetto registers the long-running `uw-mcp` HTTP endpoint. The executable is in
this repository's `packages.${system}.default` Nix output and listens on
`127.0.0.1:7880` by default.

```json
{
  "mcpServers": {
    "usagewindow": {
      "type": "http",
      "url": "http://127.0.0.1:7880/mcp"
    }
  }
}
```

It exposes `get_usage`, `get_resume_state`, `request_compaction`, and
`schedule_resume`. MCP discovers the daemon's private Unix socket automatically
and needs no token or web login. Set `UW_DAEMON_URL` to `unix:/path` or an HTTP
URL for an explicit override. MCP does not open the SQLite database. The daemon's
TCP API requires its configured web login, including the internal route aliases.
Set `UW_MCP_LISTEN_ADDR` to change its listener. If a browser or reverse proxy sends an
`Origin` other than localhost, add the exact origin to the comma-separated
`UW_MCP_ALLOWED_ORIGINS` value.

The endpoint implements the stateless MCP `2026-07-28` HTTP transport. It does not
implement the session ID, standalone GET/SSE, or DELETE behavior from the 2025
Streamable HTTP revisions. `uw-mcp --stdio` retains the old newline-delimited
`2024-11-05` server for local clients that still need it.
