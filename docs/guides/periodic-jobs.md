# Periodic jobs

Enqueue jobs on a schedule, once per tick across the cluster.

## Declaring periodic jobs

```go
cfg := foreman.Config{
	Queues:  map[string]foreman.QueueConfig{"default": {}},
	Workers: workers,
	PeriodicJobs: []*foreman.PeriodicJob{
		{
			Name:     "nightly-report",
			Schedule: foreman.MustCron("0 3 * * *"), // 03:00 UTC every day
			Args: func() (foreman.JobArgs, *foreman.InsertOpts) {
				return ReportArgs{Period: "daily"}, &foreman.InsertOpts{Queue: "reports"}
			},
			CatchUp: 6 * time.Hour, // enqueue ticks missed in the last 6h
		},
		{
			Name:       "refresh-cache",
			Schedule:   foreman.Every(5 * time.Minute),
			Args:       func() (foreman.JobArgs, *foreman.InsertOpts) { return RefreshArgs{}, nil },
			RunOnStart: true, // also run when a client becomes leader
		},
	},
}
```

| Field | Meaning |
| --- | --- |
| `Name` | Unique; never change it. |
| `Schedule` | `Cron(...)`, `MustCron(...)` or `Every(...)`. |
| `Args` | Args and optional `InsertOpts` per tick. |
| `RunOnStart` | Also enqueue when a client becomes leader. |
| `CatchUp` | How far back missed ticks are enqueued (default 0). |
| `TimeZone` | Cron location (default UTC). |

Only the leader enqueues. Configure the same `PeriodicJobs` on every client that serves queues,
or ticks are missed.

## Schedules

```go
foreman.MustCron("*/15 * * * *")   // 5 fields: minute hour day-of-month month day-of-week
foreman.MustCron("30 0 * * * *")   // 6 fields, leading seconds
foreman.MustCron("@daily")         // @yearly @annually @monthly @weekly @daily @midnight @hourly
foreman.Every(time.Hour)           // multiples of d since the Unix epoch
```

`Cron(expr)` returns an error for an invalid expression; `MustCron` panics. `@every` is rejected; use `Every(d)`. `Every(7 * time.Minute)` fires at multiples of 7 minutes
since 1970, not 7 minutes after start. `d` must be positive.

## Time zones and DST

```go
paris, err := time.LoadLocation("Europe/Paris")
// ...
job := &foreman.PeriodicJob{Name: "morning-digest", Schedule: foreman.MustCron("0 8 * * *"),
	TimeZone: paris, Args: digestArgs}
```

A wall time skipped by a DST change does not fire. A repeated wall time fires twice. Prefer UTC,
and avoid local times between 01:00 and 03:00.

## Exactly once per tick

Each tick is enqueued at most once, even when leadership changes. Each job gets metadata
`foreman_periodic` (the name) and `foreman_tick_at` (the tick, RFC 3339 in UTC).

Execution is still at least once. To skip a tick while the previous one is pending, add
[unique options](unique-jobs.md):

```go
foreman.UniqueOpts{ByState: []foreman.State{foreman.StateAvailable, foreman.StateScheduled,
	foreman.StateRunning, foreman.StateRetryable}}
```

## Leader gaps and catch-up

After a leader gap, ticks within `CatchUp` plus 15 seconds are enqueued. Older ones are skipped
and counted in `foreman_periodic_skipped_total{name}`. A new periodic job starts from now,
without back-filling. `RunOnStart` runs once more on every leader change.

## Inspecting

```sh
foreman periodic list    # each periodic job with its latest tick
```

`Client.PeriodicList` returns the same data, and `foreman stats` includes it.
