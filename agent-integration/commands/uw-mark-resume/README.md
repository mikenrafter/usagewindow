# /uw-mark-resume

Mark a session for resume by running:

```sh
uw resume <session-id>
```

## Claude Code

Claude Code custom slash commands receive the text after the command in
`$ARGUMENTS`; they do not expose the hook JSON payload (and therefore do not inject
`session_id`) into the command body. Invoke `/uw-mark-resume <session-id>` with the
stable harness-assigned id. The same id is the `session_id` field delivered to Claude
Code hooks and the id accepted by `claude --resume`; usagewindow sanitizes it before
using it as a key.

The command implementation is intentionally a shell command so Kasetto can substitute
`$ARGUMENTS` in the command context:

```sh
set -- $ARGUMENTS
test -n "${1:-}" || { echo 'usage: /uw-mark-resume <session-id>' >&2; exit 2; }
exec uw resume "$1"
```
