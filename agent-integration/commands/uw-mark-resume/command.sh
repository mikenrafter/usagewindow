#!/bin/sh
set -- $ARGUMENTS
test -n "${1:-}" || { echo 'usage: /uw-mark-resume <session-id>' >&2; exit 2; }
exec uw resume "$1"
