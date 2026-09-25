# T3Code integration research

This records the local T3Code integration surface verified on 2026-09-18 before
implementing the usagewindow adapter.

## Verified message delivery

The running T3Code server exposes an authenticated HTTP API on its configured
environment endpoint. A Claude-backed thread can receive a user turn through:

```text
POST /api/orchestration/dispatch
Content-Type: application/json

{
  "type": "thread.turn.start",
  "commandId": "<uuid>",
  "threadId": "<t3code-thread-uuid>",
  "message": {
    "messageId": "<uuid>",
    "role": "user",
    "text": "<message>",
    "attachments": []
  },
  "runtimeMode": "full-access",
  "interactionMode": "default",
  "createdAt": "<iso timestamp>"
}
```

The endpoint returns a dispatch sequence. The thread can be read with:

```text
GET /api/orchestration/threads/:threadId
```

The request requires an authenticated T3Code session. T3Code supports bearer
access tokens and browser-session cookies. The adapter therefore accepts an
already-issued bearer token or cookie value; it does not mint pairing/session
credentials or automate a browser.

## Verified behavior

On the local T3Code instance, dispatching the command above to thread
`3cfef86e-7ab8-4bb1-8410-f54e9c135ea4` caused the existing Claude session to
complete a turn and reply `received through T3Code.`. This proves the native
T3Code path preserves the thread/session-manager ownership boundary.

## Adapter boundaries

The adapter owns only T3Code orchestration delivery. It does not parse Claude
transcripts or start `claude --resume`, because doing either would bypass the
T3Code owner process and can create a detached branch. Discovery, usage polling,
and provider-specific compaction remain unsupported until T3Code exposes a
stable corresponding endpoint in the adapter configuration.

## Auth: pairing token vs. session credential (verified 2026-09-21)

`GET /api/orchestration/threads/:threadId` and `POST /api/orchestration/dispatch`
both require a real T3Code session, not the one-time pairing token printed by
`journalctl -u t3` (`Token: ...` / `.../pair#token=...`). That pairing token is
consumed client-side by the `/pair` route's React auth gate; the exchange for a
real session lives in a bundled chunk this pass didn't need to trace, because
completing an unsupervised auth flow against a live, in-use T3Code instance is
exactly the kind of pairing/session-minting the adapter is documented to avoid
(see "Verified message delivery" above). The right source of a credential
remains what's already stated: an already-issued bearer token or cookie,
obtained by a human completing `/pair` (or already signed in) and copying it
out of the browser.

Once given a real session cookie (`Cookie: <cookie-name>=<jwt>` — note this is
`name=value`, not `name:value`; a browser devtools "cookie value" copy can come
out either way depending on the panel), `GET /api/orchestration/threads/:id`
returns:

```json
{
  "snapshotSequence": ...,
  "thread": {
    "id": "...", "projectId": "...", "title": "Connectivity Test Ping",
    "modelSelection": ..., "runtimeMode": "full-access", "interactionMode": ...,
    "messages": [...], "activities": [
      {"id": "...", "tone": "info", "kind": "context-window.updated", "summary": "...",
       "payload": {"usedTokens": 20042, "maxTokens": 200000, ...}, "turnId": "...", "createdAt": "..."},
      {"id": "...", "tone": "info", "kind": "checkpoint.captured", ...}
    ],
    "checkpoints": [...],
    "session": {
      "threadId": "...", "status": "stopped", "providerName": "claudeAgent",
      "providerInstanceId": "claudeAgent", "runtimeMode": "full-access",
      "activeTurnId": null, "lastError": null, "updatedAt": "..."
    }
  }
}
```

One thing worth noting for whoever picks this up next: **`thread.title` exists
and is populated** ("Connectivity Test Ping" on the thread checked here) — T3Code
has its own title, independently of Claude's `ai-title` transcript records. If
usagewindow ever surfaces T3Code sessions with titles, this is the field, not
something synthesized.

## Stop detection: verified quota-error shape (verified 2026-09-21)

Thread `35a334bc-07c7-432b-9942-6da8b512fb6a`, a Codex-backed thread that had
actually hit its provider's quota limit, returned:

```json
"session": {
  "threadId": "35a334bc-07c7-432b-9942-6da8b512fb6a",
  "status": "error",
  "providerName": "codex",
  "providerInstanceId": "codex",
  "runtimeMode": "full-access",
  "activeTurnId": null,
  "lastError": "You've hit your usage limit. Upgrade to Pro (https://chatgpt.com/explore/pro), visit https://chatgpt.com/codex/settings/usage to purchase more credits or try again at 3:48 AM.",
  "updatedAt": "2026-09-21T06:25:25.218Z"
}
```

Confirms: `status` does take a distinct value (`"error"`, not `"stopped"`) for
a quota-limit stop, and `lastError` is a plain string — provider-passthrough
text, not a structured code. Since T3Code can host more than one underlying
provider (`providerName` here is `codex`, not `claudeAgent`), the adapter's
`detect_stop` gates on `status == "error"` and then matches the `lastError`
text against the quota phrasing already trusted elsewhere in this codebase
("usage limit" from Codex's own message, "rate limit" as the general Claude
`rate_limit_error` shape) rather than inventing new evidence. Implemented in
`crates/uw-adapters/src/t3code.rs`.

Still unverified: a Claude-backed T3Code thread's `lastError` text for a
quota stop (this example is Codex-backed) — if it turns out to phrase
differently than either "usage limit" or "rate limit", `detect_stop` will
silently under-report it as "not stopped" rather than false-positive, which
is the safe failure direction but worth tightening if a Claude-backed example
ever turns up.

## Native compaction and interruption (verified 2026-09-22)

The Context Window meter's **Compact context** action is delivered through the
same orchestration dispatch endpoint as a user turn:

```json
{
  "type": "thread.turn.start",
  "threadId": "<thread-id>",
  "message": {"role": "user", "text": "/compact", "attachments": []},
  "runtimeMode": "full-access",
  "interactionMode": "default"
}
```

For usagewindow's queued compaction, T3Code first receives a separate short
instruction turn, then usagewindow sends `thread.turn.interrupt` for that
thread, and only then dispatches the standalone `/compact` turn. This prevents
the agent from continuing the instruction turn before T3Code's native
compaction operation runs. The interrupt command uses the same dispatch
endpoint and includes `type`, `commandId`, `threadId`, and `createdAt`; no
`turnId` is required because T3Code resolves the active turn for the thread.
The adapter also exposes that interrupt as a standalone control operation so
the daemon can stop an already-running thread as soon as it crosses the hard
95% quota boundary, before the queued compaction delivery begins.

## Native-provider ownership mapping (verified 2026-09-21)

The HTTP thread snapshots above do not include the provider resume cursor. The
local T3Code state database does: `/home/v0id/.t3/userdata/state.sqlite`, table
`provider_session_runtime`. The verified row for the affected Claude thread
maps:

```text
thread_id: d02b9d75-f564-4cbd-8dcd-62bb445b28c6
provider_name: claudeAgent
resume_cursor_json.resume: de50f3dc-713e-44ff-baaa-ea46fd0e4e1a
```

The same inspection found that Claude rows use
`resume_cursor_json.$.resume`, Cursor rows use `$.sessionId`, and Codex rows
use `$.threadId`. Therefore a usagewindow resume for a provider-native
session must consult this local database to discover its owning T3Code thread;
looking up the native ID directly through the HTTP snapshot endpoint cannot
establish ownership. The adapter will treat the state-database path as
configuration, defaulting to `$HOME/.t3/userdata/state.sqlite`.

## Descendant ownership routing (verified 2026-09-24)

T3Code persists the provider session that it owns, but a provider can create child
sessions without adding one row per child to `provider_session_runtime`. In the Cursor
incident, T3Code owned root session `a293a959-d37d-4e73-8848-f5b7f3d560a0` through
thread `4a9941d1-eebf-4201-8fad-ecdf4a1871f0`. Cursor then created child session
`e5ded152-88c0-4222-be0d-d629dc344555`. The child chat metadata named the T3-owned
session as both its parent and root, while the T3 database correctly retained only the
root binding.

The T3Code adapter now receives lineage on `SessionSummary` and checks ownership
candidates in this order: requested session, parent, root, then related IDs. The first
matching provider-native ID selects the T3 thread. Direct ownership therefore wins when
both a child and an ancestor have bindings. If none match, the operation remains
`Unsupported` and the adapter does not contact T3Code.

T3Code does not parse provider storage. Each native adapter owns that work and supplies
verified lineage during discovery. Cursor currently supplies parent and root IDs from
its chat database. No Claude Code or Codex child format is claimed here; those adapters
can use the same model after their formats are verified.
