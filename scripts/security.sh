#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Vulnerabilities (govulncheck), licenses (go-licenses), image scan (trivy).
set -euo pipefail

cd "$(dirname "$0")/.."
govulncheck ./...
go tool go-licenses check ./... \
  --allowed_licenses=Apache-2.0,MIT,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0
image="${IMAGE:-thnhdatle/foreman:dev}"
docker build -q -t "$image" . >/dev/null
trivy image --quiet --exit-code 1 --severity CRITICAL,HIGH --ignore-unfixed "$image"
