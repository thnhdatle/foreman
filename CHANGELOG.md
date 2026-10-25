# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog 1.1](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning 2.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-10-25

### Added

- Interactive demo page in `docs/demo`.

### Removed

- Acceptance, benchmark and mutation-testing code and their Makefile targets; benchmark
  results stay in the README.

## [0.1.0] - 2026-10-21

First public release.

### Added

- Typed jobs and workers: `JobArgs`, `Worker[T]`, `Job[T]`, `NewWorkers`, `AddWorker` with
  per-kind timeout, retry policy and max attempts; `Snooze`, `Discard`, `Cancel`; panic recovery
  with stack traces; timeouts (job, kind, client); middleware around inserts and work;
  OpenTelemetry spans `foreman.work <kind>` and `foreman.insert <kind>`.
- Client lifecycle: `NewClient`, `Start`, graceful `Stop`, `StopAndCancel`; producer-only
  clients; `PollOnly` mode for PgBouncer transaction pooling.
- Inserts: `Insert`, `InsertTx`, `InsertMany`, `InsertManyTx` (COPY for batches of 10 or more),
  priorities, scheduling, tags, metadata, concurrency keys, args size limit.
- Unique jobs by args, selected fields, queue, period and states, with the canonical form computed
  once in SQL (`foreman_canonical_json`) and golden vectors.
- Cross-language insert contract: SQL function `foreman_insert_job(kind, args, opts)`.
- Fetching with `FOR UPDATE SKIP LOCKED`, per-key concurrency limits across the cluster
  (advisory locks) and fairness between keys under backlog.
- Retries with exponential backoff and full jitter (`ExponentialBackoff`, `ConstantBackoff`,
  `RetryPolicyFunc`), dead letters (`discarded`), `ErrorHandler`, rescue of stuck jobs by
  heartbeat, and a completion guard against stale results; `CompleteTx`.
- Leader election by lease row and leader services: scheduler, rescuer, cleaner with retention,
  concurrent reindexer, periodic enqueuer (cron with optional seconds and time zones, `Every`,
  exactly once per tick, catch-up window) and queue-depth sampler.
- Cancellation of running jobs and pause/resume of queues propagated by NOTIFY and polling.
- Admin API on the client (`JobGet`, `JobList`, `JobCancel`, `JobRetry`, `JobDiscard`,
  `JobRetryMany`, `JobDiscardMany`, `JobDeleteMany`, `JobWork`, `QueuePause`, `QueueResume`,
  `QueueList`, `LeaderGet`, `PeriodicList`), the embeddable JSON handler `foremanhttp.Handler`
  with an OpenAPI document, and the `foreman` CLI (migrate, jobs, queues, leader, periodic,
  stats, config, version).
- Embedded migrations (`migrate.Up`, `Down`, `Version`, `SQL`) for any schema, Postgres 14-17.
- Prometheus metrics with label capping, and a Grafana dashboard.
- Test helpers `foremantest`: `RequireInserted`, `RequireNotInserted`, `WorkJob`, `WorkInline`,
  fake `Clock`.
- Guides and runnable examples.

[Unreleased]: https://github.com/thnhdatle/foreman/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/thnhdatle/foreman/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/thnhdatle/foreman/releases/tag/v0.1.0
