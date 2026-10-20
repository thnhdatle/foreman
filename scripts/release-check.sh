#!/usr/bin/env bash
# shellcheck disable=SC2317,SC2329 # check functions are invoked indirectly through check()
# SPDX-License-Identifier: Apache-2.0
# Pre-release checks, run by `make release-check` before tagging.
set -euo pipefail

cd "$(dirname "$0")/.."
fail=0
check() {
  local name="$1"
  shift
  if "$@"; then echo "ok   ${name}"; else echo "FAIL ${name}"; fail=1; fi
}

# Only these hidden directories and top-level Markdown files belong in the repository.
only_project_files() {
  ! git ls-files | grep -E '^\.[^/]+/' | grep -vEq '^\.(github|githooks)/' &&
    ! git ls-files '*.md' | grep -v / | grep -vEq '^(README|CHANGELOG|CONTRIBUTING|CODE_OF_CONDUCT|SECURITY|ROADMAP)\.md$'
}
commit_subjects_valid() {
  local re='^(feat|fix|perf|refactor|test|docs|build|ci|chore)(\([a-z0-9-]+\))?!?: [a-z0-9].{0,70}[^.]$'
  ! git log --format=%s | grep -vEq "$re" && ! git log --format=%s | awk 'length > 72' | grep -q .
}
no_commit_bodies() { [ "$(git log --format=%b | grep -c .)" -eq 0 ]; }
spdx_headers() {
  ! git ls-files '*.go' '*.sh' '*.py' '*.sql' | grep -v '^testdata/' \
    | xargs grep -L 'SPDX-License-Identifier: Apache-2.0' | grep -q .
}
no_bare_todo() { ! git grep -nE 'TODO([^(]|$)' -- '*.go' ':!testdata' >/dev/null; }
files_present() {
  local f
  for f in "$@"; do [ -f "$f" ] || { echo "  missing $f"; return 1; }; done
}
changelog_release() { grep -Eq '^## \[0\.1\.0\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$' CHANGELOG.md; }
report_clean() { [ -f acceptance/report.md ] && ! grep -Eq '^\| FMN-[0-9]+ \| FAIL' acceptance/report.md; }
config_validates() { go run ./cmd/foreman config validate --config deploy/config.example.yaml >/dev/null; }

check "only project files tracked" only_project_files
check "commit subjects are one-line Conventional Commits" commit_subjects_valid
check "no multi-line commit messages" no_commit_bodies
check "SPDX header in every source file" spdx_headers
check "no TODO without (roadmap)" no_bare_todo
check "community files present" files_present README.md CHANGELOG.md ROADMAP.md CONTRIBUTING.md \
  CODE_OF_CONDUCT.md SECURITY.md LICENSE NOTICE .github/CODEOWNERS .github/dependabot.yml \
  .github/PULL_REQUEST_TEMPLATE.md .github/ISSUE_TEMPLATE/bug_report.yml .github/ISSUE_TEMPLATE/feature_request.yml
check "required guides present" files_present docs/guides/{quickstart,configuration,operations,troubleshooting,errors,getting-started,workers,transactional-enqueue,retries-and-dead-letters,periodic-jobs,unique-jobs,concurrency-and-fairness,insert-from-other-languages}.md
check "bench/RESULTS.md present" files_present bench/RESULTS.md
check "CHANGELOG 0.1.0 section is dated" changelog_release
check "acceptance report has no FAIL rows" report_clean
check "config.example.yaml validates" config_validates
exit "$fail"
