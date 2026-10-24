#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Static analysis: Go linters, Dockerfile, shell scripts, secrets, SPDX headers.
set -euo pipefail

cd "$(dirname "$0")/.."

golangci-lint run ./...

hadolint Dockerfile
shellcheck scripts/*.sh .githooks/*

gitleaks git --no-banner --redact --log-level warn .

if [ -d deploy/kubernetes ]; then
  kubeconform -strict -summary deploy/kubernetes
fi

missing=$(git ls-files --cached --others --exclude-standard '*.go' '*.sh' '*.py' '*.sql' \
  | grep -v '^testdata/' | while read -r f; do
    [ -f "$f" ] || continue
    head -n 3 "$f" | grep -q 'SPDX-License-Identifier: Apache-2.0' || echo "$f"
  done)
if [ -n "$missing" ]; then
  echo "missing SPDX header:" >&2
  echo "$missing" >&2
  exit 1
fi

if git grep -nE 'TODO([^(]|$)' -- '*.go' ':!testdata' >/dev/null; then
  echo "bare TODO found; use TODO(roadmap): and list it in ROADMAP.md" >&2
  git grep -nE 'TODO([^(]|$)' -- '*.go' ':!testdata' >&2
  exit 1
fi
