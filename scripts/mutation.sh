#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Mutation testing (Gremlins) on the core packages listed below; efficacy >= 60%.
set -euo pipefail

cd "$(dirname "$0")/.."
packages=(
  internal/domain/job
  internal/domain/backoff
  internal/domain/fairness
  internal/domain/unique
  internal/app/elector
  internal/app/completer
)
status=0
for pkg in "${packages[@]}"; do
  echo "== ${pkg}"
  if ! go tool gremlins unleash --threshold-efficacy 60 --timeout-coefficient 10 "./${pkg}"; then
    echo "mutation threshold not met: ${pkg}" >&2
    status=1
  fi
done
exit "$status"
