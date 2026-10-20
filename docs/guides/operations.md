# Operations

Run Foreman in production: sizing, Postgres, PgBouncer, migrations, upgrades, metrics and admin
tools.

## Sizing the connection pool

```text
pool size >= jobs running at once that use the database + about 5 per client
```

The pgx default is too small. Set `?pool_max_conns=60` in the URL, or `MaxConns`. Postgres
`max_connections` must cover all pools.

## Postgres settings

Defaults work. For busy queues:

```text
autovacuum = on
autovacuum_naptime = 10s            # keeps up with queue churn
max_locks_per_transaction = 256     # PerKeyLimit with many keys
random_page_cost = 1.1              # on SSDs
statement_timeout                   # unset for the Foreman pool, or high enough for reindexing
```

The migration sets these on `foreman_jobs`:

| Parameter | Value |
| --- | --- |
| `fillfactor` | 90 |
| `autovacuum_vacuum_scale_factor` | 0.01 |
| `autovacuum_vacuum_threshold` | 1000 |
| `autovacuum_analyze_scale_factor` | 0.02 |
| `autovacuum_vacuum_cost_limit` | 2000 |

## Maintenance services

One client is elected leader (15-second lease). It runs:

| Service | Interval | Purpose |
| --- | --- | --- |
| scheduler | 1s | makes due jobs `available` |
| rescuer | `HeartbeatInterval` (30s) | rescues jobs silent for `RescueStuckJobsAfter` (1h) |
| cleaner | `CleanerInterval` (30s) | deletes old finalized jobs |
| reindexer | `ReindexInterval` (24h) | rebuilds the queue indexes |
| periodic | 1s | enqueues [periodic jobs](periodic-jobs.md) |
| depth sampler | 15s | samples `foreman_queue_depth` |

### Retention

| Setting | Default |
| --- | --- |
| `CompletedJobRetention` | 24h |
| `CancelledJobRetention` | 7 days |
| `DiscardedJobRetention` | 7 days |

Negative keeps jobs forever.

### Reindexing

Set a negative `ReindexInterval` to disable it, for example if you run your own reindex job.

## PgBouncer

- Session pooling works as is.
- Transaction pooling cannot deliver LISTEN/NOTIFY. Set `Config.PollOnly: true`.

A new job then waits up to `FetchInterval` (default 1s). Set pgx `default_query_exec_mode` for
your PgBouncer version. Run migrations and the CLI without transaction pooling.

## Migrations

```sh
foreman migrate up        # apply pending migrations
foreman migrate version   # current and latest version
foreman migrate sql up    # print the SQL for your own migration tool
foreman migrate down      # remove every Foreman object (destroys all jobs)
```

From Go, `migrate.Up(ctx, pool)` is safe from every instance at startup. For a non-public schema,
pass `migrate.WithSchema` or `--schema`, and set the same `Config.Schema`.

## Backup and restore

Your normal database backups include Foreman's tables. After a restore, jobs that were `running`
are rescued after `RescueStuckJobsAfter`.

## Upgrades

1. Read the changelog for schema changes.
2. Run `foreman migrate up` with the new version's CLI.
3. Deploy the new version.

After migrating, old binaries cannot start (`migrate.ErrUnknownVersion`). Plan rollbacks for
that.

## Metrics

Set `Config.Registerer`, for example `prometheus.DefaultRegisterer`. The `queue`, `kind` and
`name` labels keep at most 50 values each; the rest are `other`.

| Metric | Type | Labels |
| --- | --- | --- |
| `foreman_jobs_inserted_total` | counter | `queue`, `kind` |
| `foreman_jobs_completed_total` | counter | `queue`, `kind`, `outcome` |
| `foreman_job_duration_seconds` | histogram | `queue`, `kind` |
| `foreman_job_queue_latency_seconds` | histogram | `queue` (scheduled time to start) |
| `foreman_queue_depth` | gauge | `queue`, `state` (leader only) |
| `foreman_running` | gauge | `queue` (this client) |
| `foreman_leader` | gauge | 1 on the leader |
| `foreman_rescued_total` | counter | |
| `foreman_stale_completions_total` | counter | |
| `foreman_periodic_skipped_total` | counter | `name` |
| `foreman_jobs_unserved_queue_total` | counter | `queue` |
| `foreman_fetch_duration_seconds` | histogram | `queue` |
| `foreman_maintenance_last_run_timestamp_seconds` | gauge | `service` |
| `foreman_maintenance_rows_total` | counter | `service` |
| `foreman_build_info` | gauge | `version`, `go_version` |

- `outcome`: `success`, `error`, `panic`, `snooze`, `discard`, `cancel`, or `cancelled` (by an
  operator). Running out of attempts counts as `error`.
- `service`: `scheduler`, `rescuer`, `cleaner`, `reindexer`, `periodic`, `depth_sampler`.

Suggested alerts:

```text
sum(foreman_leader) == 0                                       # no leader, for 1 minute
time() - max(foreman_maintenance_last_run_timestamp_seconds{service="scheduler"}) > 60
foreman_queue_depth{state="available"}                         # growing for 15 minutes
foreman_jobs_completed_total{outcome=~"error|panic"}      # rate above normal
increase(foreman_rescued_total[1h]) > 0                        # lost workers
increase(foreman_jobs_unserved_queue_total[10m]) > 0           # misrouted jobs
increase(foreman_periodic_skipped_total[1h]) > 0               # missed schedules
```

Also watch a high p95 of `foreman_job_queue_latency_seconds`.

## Admin HTTP API

`foremanhttp.Handler(client)` serves a JSON API (`api/openapi.yaml`) for jobs, queues and the
leader. **It has no authentication.** Mount it behind your own middleware:

```go
mux.Handle("/admin/foreman/",
	requireAdmin(http.StripPrefix("/admin/foreman", foremanhttp.Handler(client))))
```

Error codes are in [errors](errors.md).

## Admin CLI

```sh
go install github.com/thnhdatle/foreman/cmd/foreman@latest
```

| Command | Purpose |
| --- | --- |
| `migrate up\|down\|version\|sql [up\|down]` | manage the schema |
| `jobs list [--state --kind --queue --limit --cursor]` | list jobs, newest first |
| `jobs get\|retry\|cancel\|discard <id>` | inspect or change one job |
| `queues list`, `queues pause\|resume <name>` | queue state and counts |
| `leader` | current leader lease |
| `periodic list` | periodic jobs and last tick |
| `stats` | pending jobs, queues, leader, periodic jobs |
| `config validate`, `config print --redact` | check the CLI configuration |
| `version` | print the version |

Add `-o json` for JSON output. Connection settings are in
[configuration](configuration.md#cli-configuration).
