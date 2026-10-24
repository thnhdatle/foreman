// SPDX-License-Identifier: Apache-2.0

package foremantest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/internal/app/executor"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// RequireInserted fails the test unless a matching job exists and returns it.
func RequireInserted[T foreman.JobArgs](
	ctx context.Context, tb testing.TB, client *foreman.Client, expected T,
) *foreman.JobRow {
	tb.Helper()
	row, err := findInserted(ctx, client, expected)
	switch {
	case errors.Is(err, errNotInserted):
		tb.Fatalf("foremantest: no %s job with args %s was inserted", expected.Kind(), mustJSON(expected))
	case err != nil:
		tb.Fatalf("foremantest: %v", err)
	}
	return row
}

// RequireNotInserted fails the test if a matching job exists.
func RequireNotInserted[T foreman.JobArgs](ctx context.Context, tb testing.TB, client *foreman.Client, args T) {
	tb.Helper()
	row, err := findInserted(ctx, client, args)
	switch {
	case errors.Is(err, errNotInserted):
	case err != nil:
		tb.Fatalf("foremantest: %v", err)
	default:
		tb.Fatalf("foremantest: unexpected %s job %d with args %s", args.Kind(), row.ID, row.Args)
	}
}

var errNotInserted = errors.New("not inserted")

func findInserted[T foreman.JobArgs](ctx context.Context, client *foreman.Client, expected T) (*foreman.JobRow, error) {
	want, err := canonical(mustJSON(expected))
	if err != nil {
		return nil, err
	}
	params := &foreman.JobListParams{Kinds: []string{expected.Kind()}, Limit: foreman.MaxListLimit}
	for {
		page, err := client.JobList(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("list jobs: %w", err)
		}
		for _, j := range page.Jobs {
			got, err := canonical(j.Args)
			if err == nil && bytes.Equal(got, want) {
				return j, nil
			}
		}
		if page.NextCursor == "" {
			return nil, errNotInserted
		}
		params.Cursor = page.NextCursor
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("foremantest: encode args: %v", err))
	}
	return b
}

func canonical(b []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode args: %w", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode args: %w", err)
	}
	return out, nil
}

// WorkResult is the outcome of running a job with WorkJob.
type WorkResult struct {
	State       foreman.State
	ScheduledAt time.Time
	Err         string
}

// WorkOpts configures WorkJob.
type WorkOpts struct {
	// Attempt is the attempt number. Default 1. MaxAttempts defaults to 25.
	Attempt     int
	MaxAttempts int
	Timeout     time.Duration
	RetryPolicy foreman.RetryPolicy
	Clock       foreman.Clock
}

// WorkJob runs worker on args in memory, without a database.
func WorkJob[T foreman.JobArgs](
	ctx context.Context, tb testing.TB, worker foreman.Worker[T], args T, opts *WorkOpts,
) *WorkResult {
	tb.Helper()
	if opts == nil {
		opts = &WorkOpts{}
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.New()
	}
	policy := opts.RetryPolicy
	if policy == nil {
		policy = foreman.ExponentialBackoff{}
	}
	now := clk.Now()
	row := &job.Row{
		ID: 1, Kind: args.Kind(), Queue: foreman.DefaultQueue, Args: mustJSON(args), State: job.StateRunning,
		Priority: foreman.DefaultPriority, Attempt: max(opts.Attempt, 1), MaxAttempts: opts.MaxAttempts,
		ScheduledAt: now, CreatedAt: now, AttemptedAt: &now,
	}
	if row.MaxAttempts <= 0 {
		row.MaxAttempts = foreman.DefaultMaxAttempts
	}
	ex, err := executor.New(executor.Config{
		Lookup: func(string) (executor.Handler, bool) {
			return executor.Handler{Work: func(ctx context.Context, r *job.Row) error {
				var a T
				if err := json.Unmarshal(r.Args, &a); err != nil {
					return fmt.Errorf("foremantest: decode args: %w", err)
				}
				return worker.Work(ctx, &foreman.Job[T]{
					ID: r.ID, Args: a, Attempt: r.Attempt, MaxAttempts: r.MaxAttempts, Queue: r.Queue,
					Priority: r.Priority, CreatedAt: r.CreatedAt, ScheduledAt: r.ScheduledAt,
				})
			}}, true
		},
		DefaultTimeout: opts.Timeout,
		DefaultRetry: func(r *job.Row, now time.Time) time.Time {
			return policy.NextRetry(foreman.JobRow{ID: r.ID, Kind: r.Kind, Attempt: r.Attempt, MaxAttempts: r.MaxAttempts}, now)
		},
		Clock: clk,
	})
	if err != nil {
		tb.Fatalf("foremantest: %v", err)
		return nil
	}
	c, _ := ex.Execute(ctx, row)
	res := &WorkResult{State: foreman.State(c.State), ScheduledAt: c.ScheduledAt}
	if c.Error != nil {
		res.Err = c.Error.Error
	}
	return res
}

// WorkInline inserts args and runs the job synchronously on client.
func WorkInline[T foreman.JobArgs](
	ctx context.Context, tb testing.TB, client *foreman.Client, args T, opts *foreman.InsertOpts,
) *foreman.JobRow {
	tb.Helper()
	res, err := client.Insert(ctx, args, opts)
	if err != nil {
		tb.Fatalf("foremantest: insert: %v", err)
		return nil
	}
	row, err := client.JobWork(ctx, res.Job.ID)
	if err != nil {
		tb.Fatalf("foremantest: work: %v", err)
		return nil
	}
	return row
}

// Clock is a manually advanced clock for tests.
type Clock struct {
	f *clock.Fake
}

// NewClock returns a fake clock set to start.
func NewClock(start time.Time) *Clock { return &Clock{f: clock.NewFake(start)} }

// Now returns the fake time.
func (c *Clock) Now() time.Time { return c.f.Now() }

// After fires once the fake time has advanced by d.
func (c *Clock) After(d time.Duration) <-chan time.Time { return c.f.After(d) }

// NewTicker returns a ticker driven by the fake time.
func (c *Clock) NewTicker(d time.Duration) foreman.Ticker { return c.f.NewTicker(d) }

// Advance moves the fake time forward and fires due timers and tickers.
func (c *Clock) Advance(d time.Duration) { c.f.Advance(d) }

// Set moves the fake time to t (never backwards).
func (c *Clock) Set(t time.Time) { c.f.Set(t) }

// Waiters returns the number of pending timers and tickers.
func (c *Clock) Waiters() int { return c.f.Waiters() }
