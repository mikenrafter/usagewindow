---
name: usagewindow-status
description: Interpret usagewindow status messages and acknowledge keepalive turns.
---

# usagewindow status

Usagewindow may report these literal status strings:

- `will auto resume at <time>` means the stopped session has a scheduled resume.
- `compacted from X to Y tokens` means context compaction completed and reduced the
  observed token count from X to Y.
- `auto resume canceled` means the pending automatic resume will not be attempted.

Briefly acknowledge these statuses when they appear. Do not invent a new task from a
status message or claim that a resume/compaction happened unless the status says so.

A turn containing `[[uw-keepalive]]` is a usagewindow cache keepalive turn. It exists
only to keep a provider cache warm. Acknowledge it briefly if needed, then treat it as
no-op housekeeping: do not interpret it as user work, add it to a task summary, or
repeat it as a planned action.
