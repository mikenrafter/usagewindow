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
