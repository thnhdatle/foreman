#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs a command and prints a one-line PASS/FAIL summary for a make target.
set -uo pipefail

name="$1"
shift
start=$(date +%s)
"$@"
status=$?
elapsed=$(($(date +%s) - start))
if [ "$status" -eq 0 ]; then
  echo "PASS ${name} (${elapsed}s)"
else
  echo "FAIL ${name} (${elapsed}s, exit ${status})"
fi
exit "$status"
