# Contributing to Foreman

Set up the repo, run the tests and get a change merged.

Follow the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities through
[SECURITY.md](SECURITY.md), not in an issue.

## Prerequisites

- Go 1.27 or newer
- Docker: OrbStack, Colima or Docker Desktop (for Testcontainers)
- golangci-lint 2.14
- hadolint
- shellcheck
- gitleaks
- uv (only for `examples/insert-from-python`)

## Getting started

```sh
git clone https://github.com/thnhdatle/foreman.git
cd foreman
make bootstrap    # pinned tools, modules, Git hooks in .githooks/
```

## Makefile targets

Every target prints a one-line PASS/FAIL summary.

| Target | Does |
|---|---|
| `bootstrap` | installs tools, enables hooks |
| `generate` | code generation |
| `fmt` | formats code |
| `lint` | golangci-lint, hadolint, shellcheck, gitleaks |
| `test` | unit tests, `-race`, goleak |
| `test-integration` | integration tests (Testcontainers) |
| `coverage` | merged coverage, thresholds |
| `mutation` | mutation testing, threshold |
| `acceptance` | acceptance criteria, report |
| `bench` | benchmarks, writes `bench/RESULTS.md` |
| `fuzz` | each fuzz target for `FUZZTIME` (default 30s) |
| `examples` | runs examples, checks output |
| `quickstart-test` | README quickstart in a temp clone |
| `security` | govulncheck, trivy, go-licenses |
| `build` | binaries into `./bin` |
| `docker` | local image, host architecture |
| `up` / `down` | start / stop compose stack (`down` removes volumes) |
| `ci` | `lint test test-integration coverage examples security` |
| `release-check` | release readiness checks |
| `release-dry` | GoReleaser snapshot build |

## Test levels

```sh
make test                                                # unit, no dependencies
make test-integration                                    # needs Docker running
FOREMAN_TEST_PG_IMAGE=postgres:14 make test-integration  # another Postgres version
make acceptance                                          # full acceptance suite
make acceptance ONLY=FMN-03                              # a subset
make acceptance CI=1                                     # CI thresholds
```

Details: [acceptance suite](acceptance/README.md).

## Commit messages

One line only: no body, no footer, no trailers. Use
[Conventional Commits](https://www.conventionalcommits.org/). The `commit-msg` hook enforces:

```regex
^(feat|fix|perf|refactor|test|docs|build|ci|chore)(\([a-z0-9-]+\))?!?: [a-z0-9].{0,70}[^.]$
```

- Type: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci` or `chore`.
- Scope: one from the list below. Omit it only for repo-wide changes.
- Breaking change: `!` before the colon, e.g. `feat(client)!: ...`.
- Summary: imperative, lowercase first letter, no trailing period.
- Whole line: at most 72 characters.

Scopes: `client`, `insert`, `worker`, `job`, `state`, `backoff`, `schedule`, `cron`,
`periodic`, `unique`, `fairness`, `concurrency`, `producer`, `executor`, `completer`,
`elector`, `leader`, `rescue`, `cleaner`, `notifier`, `pgstore`, `migrate`, `admin`, `cli`,
`foremantest`, `config`, `bench`, `acceptance`, `deps`, `docs`, `ci`, `build`.

Good: `fix(rescue): requeue jobs whose lease expired during shutdown`. Bad: `fix bug`, `feat: Implemented the worker.`

## Adding a dependency

Answer these in the commit or PR:

- [ ] Maintained: a release or commit in the last 12 months.
- [ ] License is one of Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, or MPL-2.0.
- [ ] No transitive GPL or AGPL dependencies (`make security` runs go-licenses).

Prefer the standard library when it is enough.

## Design changes

Public API, storage schema or runtime behaviour: open an issue or discussion first.

## Pull request checklist

- [ ] Tests added or updated.
- [ ] Docs updated (godoc, README, guides) where behaviour changed.
- [ ] Entry under `[Unreleased]` in `CHANGELOG.md`.
- [ ] `make lint test test-integration` passes locally.

## Where to ask questions

- Usage and ideas: [GitHub Discussions](https://github.com/thnhdatle/foreman/discussions).
- Bugs and features: [GitHub issues](https://github.com/thnhdatle/foreman/issues) (use the templates).
- Security: [SECURITY.md](SECURITY.md). Do not open a public issue.
