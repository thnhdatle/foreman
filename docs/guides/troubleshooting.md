# Troubleshooting

Find out why jobs are not running, run twice or run late.

```sh
foreman stats            # pending counts, queues, leader
foreman queues list      # paused queues, LAST_SEEN per queue
foreman jobs get <id>    # a job's error history
```

## Jobs are not running

Check in this order:

- **Schema not migrated.** `Start` returns `ErrSchemaNotMigrated`. Run `foreman migrate up`.
  Check that `Config.Schema` matches.
- **No client serves the queue.** Check `foreman_jobs_unserved_queue_total{queue}` and
  `LAST_SEEN`. Often a typo in `InsertOpts.Queue`.
- **Client not started, or stopped.** Clients without `Queues` never run jobs.
- **Queue paused.** `foreman queues list` shows `PAUSED true`. Run
  `foreman queues resume <name>`.
- **No worker for the kind.** The job fails with
  `executor: no worker registered for kind "<kind>"` and is eventually discarded. Register every
  kind on every client that serves its queue.
- **Job waiting.** `scheduled` and `retryable` jobs need a leader to become available.
- **Workers busy.** `foreman_running{queue}` at `MaxWorkers` everywhere. Add clients or raise
  `MaxWorkers`.

## Jobs stuck in running

A job of a dead client is rescued after `RescueStuckJobsAfter` (1h). To recover faster, lower it
(at least 2 x `HeartbeatInterval`), or by hand:

```sh
foreman jobs discard 1234
foreman jobs retry 1234
```

Rescues of live workers mean missed heartbeats. Call `foreman.Heartbeat(ctx)` in long jobs.

## A job ran twice

Execution is at least once. Make workers idempotent and use `CompleteTx`; see
[workers](workers.md#at-least-once-execution). To prevent duplicate inserts, use
[unique jobs](unique-jobs.md).

## Clock skew warning

`client clock differs from the database clock by more than TTL/3` means over 5 seconds of skew.
Fix time sync (NTP, chrony).

## Behind PgBouncer

Transaction pooling breaks LISTEN/NOTIFY. Set `Config.PollOnly: true`. Pickup then takes up to
`FetchInterval`. See [operations](operations.md#pgbouncer).

## ErrUniqueArgsNotCanonical

The unique identity has a fraction (`9.99`, `1e-3`). Use strings, or exclude the field with
`ByArgFields`. See [unique jobs](unique-jobs.md#identity-and-canonical-form).

## No leader is elected

`foreman leader` prints `no leader`. Only started clients with at least one queue can lead.
Without a leader, scheduled and retryable jobs, rescue, cleanup and periodic jobs all stop. After
a leader crash, its 15-second lease must expire first.

## High fetch or pickup latency

- High `foreman_job_queue_latency_seconds`, low `foreman_fetch_duration_seconds`: queue saturated
  or polling. Check `foreman_running`, `MaxWorkers` and `PollOnly`.
- High `foreman_fetch_duration_seconds`: table bloat. Check autovacuum, the reindexer
  (`foreman_maintenance_last_run_timestamp_seconds{service="reindexer"}`) and retention.
- Fairness or per-key limits with very many keys slow each fetch.

## Other errors

See [errors](errors.md).
