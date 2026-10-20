#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Installs the pinned CLI tools on a Linux CI runner (toolchain setup only).
set -euo pipefail

GOLANGCI_LINT_VERSION=2.14.0
HADOLINT_VERSION=2.15.1
GITLEAKS_VERSION=8.30.1
TRIVY_VERSION=0.76.0
SYFT_VERSION=1.54.1
BIN="${HOME}/.local/bin"
mkdir -p "$BIN"
echo "$BIN" >>"${GITHUB_PATH:-/dev/null}"
export PATH="$BIN:$PATH"

arch=$(uname -m)
case "$arch" in
  x86_64) goarch=amd64; harch=x86_64 ;;
  aarch64 | arm64) goarch=arm64; harch=arm64 ;;
  *) echo "unsupported arch $arch" >&2; exit 1 ;;
esac

curl -sSfL "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/golangci-lint-${GOLANGCI_LINT_VERSION}-linux-${goarch}.tar.gz" \
  | tar -xz -C /tmp
install "/tmp/golangci-lint-${GOLANGCI_LINT_VERSION}-linux-${goarch}/golangci-lint" "$BIN/"

curl -sSfL -o "$BIN/hadolint" \
  "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-${harch}"
chmod +x "$BIN/hadolint"

curl -sSfL "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/gitleaks_${GITLEAKS_VERSION}_linux_${harch/x86_64/x64}.tar.gz" \
  | tar -xz -C "$BIN" gitleaks

curl -sSfL "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-${goarch/amd64/64bit}.tar.gz" \
  | tar -xz -C "$BIN" trivy

curl -sSfL "https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_${goarch}.tar.gz" \
  | tar -xz -C "$BIN" syft

sudo apt-get update -qq && sudo apt-get install -y -qq shellcheck >/dev/null
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
