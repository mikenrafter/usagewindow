#!/usr/bin/env bash
set -euo pipefail

if [[ ! -t 0 || ! -t 1 ]]; then
  echo "run this script from an interactive terminal" >&2
  exit 2
fi

read -r -s -p "Usagewindow password: " password
printf '\n' >&2
read -r -s -p "Confirm password: " confirmation
printf '\n' >&2

if [[ -z "$password" ]]; then
  echo "password must not be empty" >&2
  exit 1
fi
if [[ "$password" != "$confirmation" ]]; then
  echo "passwords do not match" >&2
  exit 1
fi

unset confirmation
salt="$(openssl rand -hex 16)"
hash="$(printf '%s' "$password" | nix shell nixpkgs#libargon2 --command \
  argon2 "$salt" -id -t 3 -m 16 -p 1 -e)"
unset password salt

printf '%s\n' "$hash"
