# Cursor integration research

## Compaction TODO

Status: usage polling is implemented. Compaction delivery remains disabled until a
supported, owner-preserving send path is verified.

- [x] Keep the Cursor adapter's `can_trigger_compaction` and `headless_resume`
  capabilities false until delivery and identity are proven.
- [x] Move shared compaction text and command-prefix normalization into
  `uw-core::compaction`; adapters provide their command syntax.
- [x] Verify Cursor's command names. `/summarize` is the documented command and
  `/compress` is an alias.
- [x] Verify that `preCompact` is observational. It reports `trigger`, context usage,
  token counts, window size, and message counts, but cannot trigger or modify the
  compaction.
- [x] Inspect the installed CLI. `cursor-agent` version `2026.09.08-6caf4ff` exposes
  `--resume [chatId]`, `persist`, `persist attach`, and `persist stop`. Its help does
  not document sending a new prompt to an existing chat by chat ID.
- [ ] Run a disposable Cursor CLI experiment with a real chat: record the chat ID,
  start it through `persist`, attach and detach it, then test whether a second
  `cursor-agent --resume <chat-id>` process can deliver `/compress` without creating
  a fork. Record the exact command, session IDs, and transcript evidence here.
- [ ] Test the Cursor SDK local-agent path. Confirm that `Agent.resume(agentId)` plus
  `agent.send(message)` targets the same local agent and can be safely wrapped by a
  small long-lived helper process. The Rust adapter must communicate with that helper
  through a narrow authenticated local protocol, not depend on undocumented Cursor
  internals.
- [ ] Test Cursor's Agent Client Protocol (ACP) as a possible owner-preserving route.
  Establish whether the protocol can attach to an existing Cursor chat or only create
  a new agent process.
- [ ] Add a provider-specific `CursorCompactionTransport` only after one route passes
  the experiment. It must prove session ownership, require an idle session, and use
  the claim-before-act queue.
- [ ] Add red/green tests for exact `/compress` delivery, duplicate-prefix removal,
  session identity preservation, active-session rejection, and terminal failure with
  no blind retry.
- [ ] Add Cursor `preCompact` hook ingress as observation only. Store context usage
  and compaction trigger data when the session identity can be matched; do not use the
  hook to request or rewrite compaction.
- [ ] Enable `can_trigger_compaction`, `can_observe_compaction`, or token reporting
  only after each capability has a verified transport and matching tests.

Decision gate: if CLI, SDK, and ACP experiments cannot deliver to an existing Cursor
session without forking it, leave Cursor compaction unsupported and expose the reason
through the API rather than adding a best-effort send path.

Cursor's current dashboard usage endpoint is the Connect-RPC method
`POST https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage`.
It accepts `{}` and returns the current billing cycle in `billingCycleStart` and
`billingCycleEnd` (Unix milliseconds, commonly encoded as strings). The two
provider usage bars are `planUsage.autoPercentUsed` and
`planUsage.apiPercentUsed`.

The adapter uses either a bearer access token or an already-issued
`WorkosCursorSessionToken` cookie. It does not attempt login, token exchange, or
browser automation. Configure one of `UW_CURSOR_ACCESS_TOKEN` or
`UW_CURSOR_SESSION_COOKIE`; setting both disables the daemon's Cursor adapter.

Cursor's bars are monthly billing-pool percentages, not duration-keyed rolling
windows. They are stored as `WindowKind::Custom("auto")` and
`WindowKind::Custom("api")`, both using `billingCycleEnd` as their reset. Cursor
does not expose a supported external message-delivery or resume channel through
this integration, so the adapter cannot currently receive a compaction request.
Cursor's CLI does expose `/compress`, with `/summarize` as the canonical command and
`/compress` as an alias. That is useful syntax for a future interactive delivery adapter,
but it is not an API that usagewindow can send to a running session today.

Cursor's current hooks also expose an observational `preCompact` event. It reports whether
the compaction was automatic or manual and can return a `user_message` shown to the user,
but the hook cannot block or modify the compaction. We therefore do not treat it as a
trigger or a way to inject the preserved-state instructions. A future Cursor integration
should investigate a supported CLI/session transport and hook installation before claiming
`can_trigger_compaction` or `can_inject_at_session_start`.

The usage pace estimate uses the last 24 hours of samples. This is deliberately
different from the default short lookback used by rolling-window providers.

Sources verified 2026-09-20:

- Cursor dashboard usage documentation: `GetCurrentPeriodUsage` and the
  `planUsage` fields are documented in the public Cursor integration research
  used for this adapter.
- Cursor's usage-limits documentation confirms the two current monthly pools and
  billing-cycle reset behavior.
- Cursor CLI slash-command documentation confirms `/summarize` and its `/compress` alias:
  https://prod.cursor.com/docs/cli/reference/slash-commands
- Cursor hook documentation confirms `preCompact` is observational and cannot modify
  compaction:
  https://prod.cursor.com/docs/hooks
- Cursor's TypeScript SDK documents resumable local agents through `Agent.resume()` and
  follow-up messages through `agent.send()`:
  https://cursor.com/docs/sdk/typescript
- Cursor's Agent Client Protocol (ACP) documentation is the reference for testing a
  structured client connection:
  https://prod.cursor.com/docs/cli/acp
