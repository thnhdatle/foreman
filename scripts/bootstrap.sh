#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Checks tool versions, downloads modules, pulls compose images and sets the hooks path.
# Installs nothing globally.
set -euo pipefail

cd "$(dirname "$0")/.."

missing=0
check() {
  local tool="$1" hint="$2"
  if command -v "$tool" >/dev/null 2>&1; then
    printf '  %-14s %s\n' "$tool" "$("$tool" "${3:---version}" 2>&1 | head -n 1)"
  else
    printf '  %-14s MISSING (%s)\n' "$tool" "$hint"
    missing=1
  fi
}

echo "tools:"
check go "brew install go" version
check golangci-lint "brew install golangci-lint"
check docker "OrbStack, Colima or Docker Desktop"
check hadolint "brew install hadolint"
check shellcheck "brew install shellcheck"
check gitleaks "brew install gitleaks" version
check govulncheck "go install golang.org/x/vuln/cmd/govulncheck@latest" -version
check trivy "brew install trivy"
check goreleaser "brew install goreleaser"
check uv "brew install uv"
check syft "fetched below into ~/.cache/rayld/foreman/tools when missing"
check jq "brew install jq"

# syft (SBOMs for release-dry) goes to the project cache, pinned and checksum-verified.
syft_version=1.54.1
tools="${HOME}/.cache/rayld/foreman/tools"
if ! command -v syft >/dev/null 2>&1 && [ ! -x "${tools}/syft" ]; then
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  [ "$arch" = x86_64 ] && arch=amd64
  [ "$arch" = aarch64 ] && arch=arm64
  mkdir -p "$tools"
  base="https://github.com/anchore/syft/releases/download/v${syft_version}"
  curl -sSfL -o "${tools}/syft.tgz" "${base}/syft_${syft_version}_${os}_${arch}.tar.gz"
  curl -sSfL -o "${tools}/syft_checksums.txt" "${base}/syft_${syft_version}_checksums.txt"
  (cd "$tools" && grep "_${os}_${arch}.tar.gz" syft_checksums.txt | awk '{print $1"  syft.tgz"}' | shasum -a 256 -c -)
  tar -xzf "${tools}/syft.tgz" -C "$tools" syft
  missing=0
fi
go mod download
docker compose -f deploy/docker-compose.yml pull --quiet
git config core.hooksPath .githooks

if [ "$missing" -ne 0 ]; then
  echo "some tools are missing (see above)" >&2
  exit 1
fi
