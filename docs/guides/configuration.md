# Configuration

Set up a Foreman client with `foreman.Config`, and the admin CLI with a file or env vars.

## The 5 most-used options

```go
client, err := foreman.NewClient(pool, foreman.Config{
	Queues:     map[string]foreman.QueueConfig{"default": {MaxWorkers: 50}}, // queues and jobs at once
	Workers:    workers,                                                     // from NewWorkers and AddWorker
	JobTimeout: 10 * time.Minute,                                            // default Work deadline
	Registerer: prometheus.DefaultRegisterer,                                // metrics
})
```

The fifth is `PeriodicJobs` ([periodic jobs](periodic-jobs.md)). Metrics are in
[operations](operations.md#metrics).

## Config

A zero field uses its default. A negative duration means off, where allowed. `NewClient` returns
every problem at once, wrapped in `ErrInvalidConfig`.

| Field | Default | Notes |
| --- | --- | --- |
| `Queues` | none | None: producer-only client. |
| `Workers` | empty | Workers by kind. |
| `PeriodicJobs` | none | Enqueued by the leader. |
| `Logger` | `slog.Default()` | |
| `Clock` | wall clock | `foremantest` has a fake. |
| `ID` | `<hostname>-<pid>-<random>` | At most 200 characters. |
| `Schema` | `"public"` | Must match the migrated schema. |
| `JobTimeout` | 0 (none) | Not negative. |
| `HeartbeatInterval` | 30s | Also the rescuer period. |
| `RescueStuckJobsAfter` | 1h | At least 2 x `HeartbeatInterval`. |
| `RetryPolicy` | `ExponentialBackoff{}` | 1s base, 24h cap, full jitter. |
| `CompletedJobRetention` | 24h | Negative keeps forever. |
| `CancelledJobRetention` | 7 days | Negative keeps forever. |
| `DiscardedJobRetention` | 7 days | Negative keeps forever. |
| `ReindexInterval` | 24h | Negative disables. |
| `CleanerInterval` | 30s | Not negative. |
| `ErrorHandler` | nil | Can override failed attempts. |
| `Middleware` | none | First is outermost. |
| `PollOnly` | false | No LISTEN/NOTIFY, for PgBouncer. |
| `MaxArgsBytes` | 1 MiB | Larger: `ErrArgsTooLarge`. |
| `TracerProvider` | nil (no spans) | OpenTelemetry. |
| `Registerer` | nil (private) | Prometheus. |

`Default*` constants hold these defaults, for example `DefaultMaxWorkers`.

## QueueConfig

| Field | Default | Notes |
| --- | --- | --- |
| `MaxWorkers` | 10 | Jobs at once in this client. |
| `FetchInterval` | 1s | Polling fallback. |
| `PerKeyLimit` | 0 (none) | Running jobs per key, cluster-wide. |
| `Fairness` | false | Share capacity between keys. |

Queue names and job kinds must match `^[A-Za-z0-9][A-Za-z0-9_.:\-]{0,127}$`. See
[concurrency and fairness](concurrency-and-fairness.md).

## Per-kind and per-job options

```go
foreman.AddWorker(workers, &EmailWorker{},
	foreman.WithTimeout(time.Minute),            // default timeout of the kind
	foreman.WithRetryPolicy(policy),             // retry policy of the kind
	foreman.WithMaxAttempts(5))                  // max attempts of jobs this client inserts
```

`InsertOpts` apply per insert. A zero field falls back to the args type's `InsertOpts()`, then
worker options, then defaults.

| Field | Default | Notes |
| --- | --- | --- |
| `Queue` | `"default"` (`DefaultQueue`) | |
| `Priority` | 2 (`DefaultPriority`) | 1 (highest) to 4. |
| `ScheduledAt` | now | Future: inserted as `scheduled`. |
| `MaxAttempts` | 25 (`DefaultMaxAttempts`) | At least 1. |
| `Tags` | none | Each 1 to 255 bytes. |
| `Metadata` | `{}` | JSON object. |
| `ConcurrencyKey` | none | At most 255 bytes. |
| `UniqueOpts` | nil | See [unique jobs](unique-jobs.md). |
| `Timeout` | 0 | At most 24 days. |

Invalid options return `ErrInvalidInsertOpts` with every problem.

## Fixed internal settings

Not configurable: leader lease 15s, scheduler and periodic intervals 1s, queue depth sampling
15s, periodic tick retention 7 days, grace after a `Stop` deadline 5s.

## CLI configuration

The CLI reads defaults, then a YAML file, then `FOREMAN_*` env vars, then flags. Later wins.

```sh
foreman --config foreman.yaml config validate         # "config ok" or every problem
foreman --config foreman.yaml config print --redact   # effective config, password redacted
```

Name the file with `--config` or `FOREMAN_CONFIG`.
[`deploy/config.example.yaml`](../../deploy/config.example.yaml) lists every key.

| Key | Env var | Flag | Default |
| --- | --- | --- | --- |
| (none) | `FOREMAN_DATABASE_URL` | `--database-url` | none |
| `database_url_file` | `FOREMAN_DATABASE_URL_FILE` | | none |
| `schema` | `FOREMAN_SCHEMA` | `--schema` | `public` |
| `output` | `FOREMAN_OUTPUT` | `-o`, `--output` | `text` (or `json`) |
| `timeout` | `FOREMAN_TIMEOUT` | | `30s` |
| `log.level` | `FOREMAN_LOG_LEVEL` | | `warn` (`debug`, `info`, `warn`, `error`) |
| `log.format` | `FOREMAN_LOG_FORMAT` | | `text` (or `json`) |

- The database URL is a secret. A `database_url` key in the YAML file is rejected.
- If both a URL and a URL file are set, the URL wins.
- Unknown keys are errors. Empty env vars are ignored.
- Durations accept a `d` suffix for days: `30s`, `2m`, `1d12h`.
- `timeout` bounds each command.
- `config print` requires `--redact`.

Invalid configuration exits with code 2. See [errors](errors.md#cli-exit-codes).
