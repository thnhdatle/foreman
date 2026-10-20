# Retries and dead letters

Control how failed jobs are retried, and inspect or retry jobs that ran out of attempts.

## Default backoff

`ExponentialBackoff{}`: after attempt `n` fails, wait a random delay up to
`min(24h, 1s * 2^(n-1))`. The job is `retryable` until then.

## Choosing a policy

```go
// Client-wide default.
cfg.RetryPolicy = foreman.ExponentialBackoff{Base: 5 * time.Second, Cap: time.Hour}

// Per kind.
foreman.AddWorker(workers, &WebhookWorker{},
	foreman.WithRetryPolicy(foreman.ConstantBackoff{Delay: 30 * time.Second}))

// Anything else.
foreman.AddWorker(workers, &SyncWorker{}, foreman.WithRetryPolicy(foreman.RetryPolicyFunc(
	func(job foreman.JobRow, now time.Time) time.Time {
		return now.Add(time.Duration(job.Attempt) * time.Minute) // linear
	})))
```

Zero `ExponentialBackoff` fields use base 1s and cap 24h.

## Max attempts

A job is discarded when attempt `MaxAttempts` fails. The first one set wins:

1. `InsertOpts.MaxAttempts` on the insert;
2. `InsertOpts()` of the args type;
3. `WithMaxAttempts(n)` on the kind, if the inserting client registered it;
4. `DefaultMaxAttempts` (25).

With the default policy, the 24 retries wait at most about 8.5 days in total. Use `MaxAttempts: 1` to never
retry.

## Results that skip the retry path

- `foreman.Discard(err)`: discard at once.
- `foreman.Cancel(err)`: cancel at once.
- `foreman.Snooze(d)`: run again after `d`, no attempt consumed.

See [workers](workers.md#results).

## Error handlers

`Config.ErrorHandler` sees every error and panic, and may override the outcome:

```go
type handler struct{}

func (handler) HandleError(
	ctx context.Context, job *foreman.JobRow, err error,
) *foreman.ErrorHandlerResult {
	if errors.Is(err, ErrInvalidPayload) {
		return &foreman.ErrorHandlerResult{SetDiscarded: true} // retrying will not help
	}
	return nil // default: retry with backoff, or discard when attempts are exhausted
}

func (handler) HandlePanic(
	ctx context.Context, job *foreman.JobRow, v any, trace string,
) *foreman.ErrorHandlerResult {
	reportPanic(job.Kind, v, trace)
	return nil
}
```

`ErrorHandlerResult` fields are `SetCancelled`, `SetDiscarded` and `SnoozeFor`. The first set
wins, in that order.

## Dead letters

Discarded jobs keep their args and error history for `DiscardedJobRetention` (7 days).

```sh
foreman jobs list --state discarded --kind webhook    # find them
foreman jobs get 1234                                 # args and errors
```

### Retrying

```sh
foreman jobs retry 1234    # or JobRetry, or POST /v1/jobs/{id}/retry
```

Works on `discarded`, `cancelled` and `completed` jobs. The job gets its full `MaxAttempts`
again. Its metadata gains `foreman_retried_at`, `foreman_retried_from` and
`foreman_retry_count`.

Retry in bulk after fixing a bug:

```go
n, err := client.JobRetryMany(ctx, &foreman.JobFilter{
	Kinds:        []string{"webhook"},
	States:       []foreman.State{foreman.StateDiscarded},
	CreatedAfter: incidentStart,
})
```

`JobDiscard` and `JobDiscardMany` discard jobs by hand.

## Rescue of stuck jobs

If a client dies, its running jobs are retried (or discarded, when out of attempts) once their
heartbeat is older than `RescueStuckJobsAfter` (1h). The error is
`rescued: no heartbeat since <time>`.

`RescueStuckJobsAfter` must be at least 2 x `HeartbeatInterval`. Lower it (for example 5m) to
recover faster, if healthy workers never miss heartbeats that long. See
[troubleshooting](troubleshooting.md#jobs-stuck-in-running).
