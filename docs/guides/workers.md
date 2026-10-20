# Workers

Write workers that run jobs of one kind, and control their results, timeouts and shutdown.

## Defining a worker

```go
foreman.AddWorker(workers, &EmailWorker{}, foreman.WithTimeout(time.Minute)) // a type with Work
foreman.AddWorker(workers, foreman.WorkFunc[ResizeArgs](                     // or a function
	func(ctx context.Context, job *foreman.Job[ResizeArgs]) error {
		return resize(ctx, job.Args.ImageID)
	}))
```

`Job[T]` holds the decoded `Args`, the 1-based `Attempt` and the job row fields.

## Results

| `Work` returns | Job becomes | Attempt consumed |
| --- | --- | --- |
| `nil` | `completed` | yes |
| any other error | `retryable`, or `discarded` when `Attempt >= MaxAttempts` | yes |
| `foreman.Snooze(d)` | `scheduled` to run after `d` | no |
| `foreman.Discard(err)` | `discarded` at once (dead letter) | yes |
| `foreman.Cancel(err)` | `cancelled` at once | yes |

A panic counts as an error. See [retries and dead letters](retries-and-dead-letters.md).

## Timeouts

The deadline comes from the first one set:

```go
foreman.InsertOpts{Timeout: d}   // 1. on the job, at most 24 days
foreman.WithTimeout(d)           // 2. on the kind
foreman.Config{JobTimeout: d}    // 3. on the client (default 0, none)
```

On timeout the context is cancelled. `Work` must watch `ctx`. A job that ignores it keeps running
and keeps its slot.

## Cancellation

The `Work` context is cancelled on timeout, on operator cancel or discard
(`foreman jobs cancel <id>`), and on `StopAndCancel` or a `Stop` deadline. After an operator
cancel the job ends `cancelled`, whatever `Work` returns.

## Heartbeats and long jobs

Running jobs are heartbeated every `HeartbeatInterval` (30s). A job silent for
`RescueStuckJobsAfter` (1h) is rescued. Refresh it between slow steps:

```go
for _, chunk := range chunks {
	if err := process(ctx, chunk); err != nil {
		return err
	}
	foreman.Heartbeat(ctx) // refresh the heartbeat now
}
```

## Using the client inside Work

```go
client := foreman.ClientFromContext(ctx) // nil outside Work
_, err := client.Insert(ctx, ThumbnailArgs{ImageID: job.Args.ImageID}, nil)
```

To complete the job in the transaction of its writes, see
[transactional enqueue](transactional-enqueue.md#completing-a-job-in-your-transaction).

## Middleware

`Config.Middleware` wraps every insert and attempt. The first is outermost.

```go
type timing struct {
	foreman.MiddlewareDefaults // implement only the hooks you need
	logger *slog.Logger
}

func (m timing) Work(
	ctx context.Context, job *foreman.JobRow, next func(context.Context) error,
) error {
	start := time.Now()
	err := next(ctx)
	m.logger.Info("job attempt", "job_id", job.ID, "queue", job.Queue,
		"duration_ms", time.Since(start).Milliseconds())
	return err
}
```

## OpenTelemetry

Set `Config.TracerProvider`, for example `otel.GetTracerProvider()`. You get a
`foreman.work <kind>` span per attempt and a `foreman.insert <kind>` span per insert
(`foreman.insert batch` for several jobs).

## At-least-once execution

A job may run more than once. Make `Work` idempotent:

- use the job id or a business key as an idempotency key;
- write with upserts or guarded updates;
- commit same-database effects with `CompleteTx`.

## Graceful shutdown

```go
client.Stop(ctx)          // stop fetching, wait for jobs until ctx is done, then cancel them
client.StopAndCancel(ctx) // cancel running jobs at once
```

Cancelling the `Start` context acts like `StopAndCancel`. Interrupted jobs that return an error
are retried.

Give `Stop` a deadline shorter than your orchestrator's kill timeout, for example 25s for a 30s
Kubernetes `terminationGracePeriodSeconds`.

## PollOnly

`Config.PollOnly` disables LISTEN. Jobs, cancels and pauses are picked up within
`FetchInterval` (1s). Use it behind PgBouncer transaction pooling; see
[operations](operations.md#pgbouncer).
