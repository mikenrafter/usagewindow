# usagewindow MCP server

Kasetto should register this as a stdio MCP server. The `uw-mcp` executable is the
workspace binary produced by this repository's `packages.${system}.default` Nix
output.

```yaml
name: usagewindow
transport: stdio
command: uw-mcp
source: .
```

It exposes `get_usage`, `get_resume_state`, and the capability-gated
`request_compaction` tool.
