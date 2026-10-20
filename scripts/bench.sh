#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs the micro benchmarks and the performance acceptance criteria 3 times each
# and writes bench/RESULTS.md.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p out/bench
go test -run '^$' -bench . -benchmem -count 6 ./internal/... | tee out/bench/micro.txt
go run ./bench/cmd/results -runs "${BENCH_RUNS:-3}" -version "${RELEASE:-dev}" -out bench/RESULTS.md
