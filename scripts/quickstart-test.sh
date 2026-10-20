#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Clones the repository into a temp dir and runs the README quickstart verbatim:
# every command inside the block between <!-- quickstart:start --> and
# <!-- quickstart:end --> is executed in order and must succeed in < 5 minutes.
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
tmp=$(mktemp -d)
# QUICKSTART_KEEP_STACK=1 leaves the compose stack up (the acceptance runner owns it).
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
  cd /
  if [ -z "${QUICKSTART_KEEP_STACK:-}" ]; then
    (cd "$tmp/foreman" 2>/dev/null && docker compose -f deploy/docker-compose.yml down -v >/dev/null 2>&1)
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

git clone -q "$repo" "$tmp/foreman"
cd "$tmp/foreman"
cmds=$(awk '/<!-- quickstart:start -->/{f=1;next} /<!-- quickstart:end -->/{f=0} f && /^```/{next} f' README.md)
if [ -z "$cmds" ]; then
  echo "no quickstart block in README.md" >&2
  exit 1
fi
start=$(date +%s)
out="$tmp/quickstart.log"
bash -euo pipefail -c "$cmds" 2>&1 | tee "$out"
elapsed=$(($(date +%s) - start))
grep -q "job completed" "$out" || { echo "quickstart did not complete a job" >&2; exit 1; }
echo "quickstart completed in ${elapsed}s"
[ "$elapsed" -lt 300 ]
