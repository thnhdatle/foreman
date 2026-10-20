#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every example against the compose Postgres and checks its expected output.
set -euo pipefail

cd "$(dirname "$0")/.."
compose="docker compose -f deploy/docker-compose.yml"
started=0
if ! $compose ps --status running postgres 2>/dev/null | grep -q postgres; then
  $compose up -d --wait postgres
  started=1
fi
# shellcheck disable=SC2317,SC2329 # invoked by the EXIT trap
cleanup() { if [ "$started" -eq 1 ]; then $compose down -v --remove-orphans >/dev/null 2>&1; fi; }
trap cleanup EXIT

export FOREMAN_DATABASE_URL="${FOREMAN_DATABASE_URL:-postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable}"
status=0
for dir in examples/*/; do
  name=$(basename "$dir")
  [ "$name" = internal ] && continue # shared helpers, not an example
  [ -f "${dir}expected_output.txt" ] || { echo "example ${name}: missing expected_output.txt" >&2; status=1; continue; }
  out=$(mktemp)
  if [ -x "${dir}run.sh" ]; then
    "${dir}run.sh" >"$out" 2>&1 || { echo "example ${name}: failed" >&2; cat "$out" >&2; status=1; continue; }
  else
    go run "./${dir}" >"$out" 2>&1 || { echo "example ${name}: failed" >&2; cat "$out" >&2; status=1; continue; }
  fi
  while IFS= read -r expected; do
    [ -z "$expected" ] && continue
    grep -qF -- "$expected" "$out" || { echo "example ${name}: missing line: ${expected}" >&2; cat "$out" >&2; status=1; }
  done <"${dir}expected_output.txt"
  echo "example ${name}: ok"
  rm -f "$out"
done
exit "$status"
