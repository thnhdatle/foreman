#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Prints the CHANGELOG.md section of a release: release-notes.sh v0.1.0
set -euo pipefail

version="${1:?usage: release-notes.sh <tag>}"
version="${version#v}"
cd "$(dirname "$0")/.."
notes=$(awk -v v="$version" '
  $0 ~ "^## \\[" v "\\]" { found = 1; next }
  found && /^## \[/ { exit }
  found && /^\[[^]]+\]: / { exit }
  found { print }
' CHANGELOG.md)
if [ -z "$(printf '%s' "$notes" | tr -d '[:space:]')" ]; then
  echo "release-notes: no CHANGELOG.md section for ${version}" >&2
  exit 1
fi
printf '%s\n' "$notes"
