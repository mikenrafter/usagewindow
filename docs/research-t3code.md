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

Two things worth noting for whoever picks this up next:

- **`thread.title` exists and is populated** ("Connectivity Test Ping" on the
  thread checked here) — T3Code has its own title, independently of Claude's
  `ai-title` transcript records. If usagewindow ever surfaces T3Code sessions
  with titles, this is the field, not something synthesized.
- **`session.lastError` is the presumed stop-detection signal, but this pass
  only observed it as `null`** on a thread that ended normally (`status:
  "stopped"`, no error). Its shape when a thread actually stops from a
  provider rate limit is unverified — I do not know whether it is a string, a
  structured object, what field would carry a Claude-shaped `rate_limit_error`
  the same way the Claude Code transcript adapter checks for one, or whether
  `status` takes on a distinct value (e.g. `"error"`) versus staying
  `"stopped"` with only `lastError` populated. Implementing `detect_stop`
  against a guessed shape here would repeat the exact mistake the Claude/Codex
  stop-detection fixes were written to stop doing (inventing a stop instead of
  evidencing one). This needs one real example: a T3Code thread that actually
  hit a provider quota limit, inspected the same way.
