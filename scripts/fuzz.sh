#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every fuzz target for the given duration (default 30s).
set -euo pipefail

cd "$(dirname "$0")/.."
fuzztime="${1:-30s}"
found=0
while IFS= read -r line; do
  file="${line%%:*}"
  target=$(sed -E 's/.*func (Fuzz[A-Za-z0-9_]+)\(.*/\1/' <<<"${line#*:}")
  dir="./$(dirname "$file")"
  found=1
  echo "== ${dir} ${target}"
  go test -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzztime" "$dir"
done < <(git grep -nE '^func Fuzz[A-Za-z0-9_]+\(' -- '*_test.go' | cut -d: -f1,3-)
if [ "$found" -eq 0 ]; then
  echo "no fuzz targets"
fi
