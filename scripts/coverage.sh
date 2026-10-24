#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Merged unit + integration coverage: >= 80% overall, >= 90% for domain and app.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p coverage

CGO_ENABLED=1 go test -race -count=1 -tags integration -timeout 40m \
  -coverpkg=./... -coverprofile=coverage/all.out ./... >coverage/test.log 2>&1 || {
  tail -n 80 coverage/test.log
  exit 1
}

module=github.com/thnhdatle/foreman
{
  head -n 1 coverage/all.out
  tail -n +2 coverage/all.out \
    | grep -vE "^${module}/(cmd|examples)/" \
    | grep -vE "^${module}/internal/testutil/" \
    | grep -vE '_gen\.go:|\.pb\.go:'
} >coverage/filtered.out
{
  head -n 1 coverage/all.out
  tail -n +2 coverage/filtered.out | grep -E "^${module}/internal/(domain|app)/"
} >coverage/core.out

total=$(go tool cover -func=coverage/filtered.out | awk '/^total:/ {sub("%", "", $3); print $3}')
core=$(go tool cover -func=coverage/core.out | awk '/^total:/ {sub("%", "", $3); print $3}')
echo "coverage: total ${total}% (min 80), domain+app ${core}% (min 90)"
awk -v t="$total" -v c="$core" 'BEGIN { exit !(t >= 80 && c >= 90) }'
