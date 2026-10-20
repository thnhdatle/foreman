// SPDX-License-Identifier: Apache-2.0

package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/app/executor"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

var (
	t0           = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	errBoom      = errors.New("boom")
	defaultRetry = func(_ *job.Row, now time.Time) time.Time { return now.Add(time.Minute) }
	kindRetry    = func(_ *job.Row, now time.Time) time.Time { return now.Add(time.Hour) }
)

type observed struct {
	outcome string
	calls   int
}

func newExec(t *testing.T, handlers map[string]executor.Handler, mutate func(*executor.Config)) (*executor.Executor, *observed) {
	t.Helper()
	obs := &observed{}
	cfg := executor.Config{
		Lookup: func(kind string) (executor.Handler, bool) {
			h, ok := handlers[kind]
			return h, ok
		},
		DefaultRetry: defaultRetry,
		Clock:        clock.NewFake(t0),
		Observe: func(_ *job.Row, outcome string, _ time.Duration) {
			obs.outcome = outcome
			obs.calls++
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	ex, err := executor.New(cfg)
	require.NoError(t, err)
	return ex, obs
}

func work(fn func(ctx context.Context) error) executor.Handler {
	return executor.Handler{Work: func(ctx context.Context, _ *job.Row) error { return fn(ctx) }}
}

func newRow(kind string, attempt int) *job.Row {
	return &job.Row{ID: 11, Kind: kind, Queue: "q", Attempt: attempt, MaxAttempts: 3}
}

func TestNew_RequiresDependencies(t *testing.T) {
	t.Parallel()
	_, err := executor.New(executor.Config{})
	require.Error(t, err)
	_, err = executor.New(executor.Config{Lookup: func(string) (executor.Handler, bool) { return executor.Handler{}, false }})
	require.Error(t, err)
}

func TestExecute_ResultTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		handler   executor.Handler
		kind      string
		attempt   int
		state     job.State
		sched     time.Time
		errSubstr string
		outcome   string
	}{
		{name: "success", handler: work(func(context.Context) error { return nil }), state: job.StateCompleted, outcome: "success"},
		{
			name: "error retries with default policy", handler: work(func(context.Context) error { return errBoom }),
			state: job.StateRetryable, sched: t0.Add(time.Minute), errSubstr: "boom", outcome: "error",
		},
		{
			name: "error retries with kind policy",
			handler: executor.Handler{
				Work:  func(context.Context, *job.Row) error { return errBoom },
				Retry: kindRetry,
			},
			state: job.StateRetryable, sched: t0.Add(time.Hour), errSubstr: "boom", outcome: "error",
		},
		{
			name: "error discards on last attempt", attempt: 3, handler: work(func(context.Context) error { return errBoom }),
			state: job.StateDiscarded, errSubstr: "boom", outcome: "error",
		},
		{
			name: "panic is recovered with stack", handler: work(func(context.Context) error { panic("kaboom") }),
			state: job.StateRetryable, sched: t0.Add(time.Minute), errSubstr: "panic: kaboom", outcome: "panic",
		},
		{
			name: "snooze", handler: work(func(context.Context) error { return &job.SnoozeError{Duration: time.Second} }),
			state: job.StateScheduled, sched: t0.Add(time.Second), outcome: "snooze",
		},
		{
			name: "discard", handler: work(func(context.Context) error { return &job.DiscardError{Err: errBoom} }),
			state: job.StateDiscarded, errSubstr: "discarded", outcome: "discard",
		},
		{
			name: "cancel", handler: work(func(context.Context) error { return &job.CancelError{Err: errBoom} }),
			state: job.StateCancelled, errSubstr: "cancelled", outcome: "cancel",
		},
		{
			name: "unknown kind retries", kind: "other", handler: work(func(context.Context) error { return nil }),
			state: job.StateRetryable, sched: t0.Add(time.Minute), errSubstr: `no worker registered for kind "other"`,
			outcome: "error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ex, obs := newExec(t, map[string]executor.Handler{"k": tc.handler}, nil)
			kind := tc.kind
			if kind == "" {
				kind = "k"
			}
			attempt := tc.attempt
			if attempt == 0 {
				attempt = 1
			}
			c, skip := ex.Execute(context.Background(), newRow(kind, attempt))
			assert.False(t, skip)
			assert.Equal(t, tc.state, c.State)
			assert.Equal(t, tc.sched, c.ScheduledAt)
			assert.Equal(t, tc.outcome, obs.outcome)
			assert.Equal(t, 1, obs.calls)
			if tc.errSubstr == "" {
				assert.Nil(t, c.Error)
				return
			}
			require.NotNil(t, c.Error)
			assert.Contains(t, c.Error.Error, tc.errSubstr)
			if tc.outcome == "panic" {
				assert.Contains(t, c.Error.Trace, "executor_test.go")
			}
		})
	}
}

func TestExecute_TimeoutPrecedence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                       string
		clientTO, kindTO, rowTO    time.Duration
		wantDeadline, wantTimeout2 bool
	}{
		{name: "none"},
		{name: "client default", clientTO: 30 * time.Millisecond, wantDeadline: true},
		{name: "kind beats client", clientTO: time.Hour, kindTO: 30 * time.Millisecond, wantDeadline: true},
		{name: "job beats kind", clientTO: time.Hour, kindTO: time.Hour, rowTO: 30 * time.Millisecond, wantDeadline: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var deadline time.Time
			var hasDeadline bool
			h := executor.Handler{
				Timeout: tc.kindTO,
				Work: func(ctx context.Context, _ *job.Row) error {
					deadline, hasDeadline = ctx.Deadline()
					if !hasDeadline {
						return nil
					}
					<-ctx.Done()
					return ctx.Err()
				},
			}
			ex, _ := newExec(t, map[string]executor.Handler{"k": h}, func(c *executor.Config) { c.DefaultTimeout = tc.clientTO })
			r := newRow("k", 1)
			r.Timeout = tc.rowTO
			start := time.Now()
			c, _ := ex.Execute(context.Background(), r)
			assert.Equal(t, tc.wantDeadline, hasDeadline)
			if !tc.wantDeadline {
				assert.Equal(t, job.StateCompleted, c.State)
				return
			}
			assert.WithinDuration(t, start.Add(30*time.Millisecond), deadline, 25*time.Millisecond)
			assert.Equal(t, job.StateRetryable, c.State)
			require.NotNil(t, c.Error)
			assert.Contains(t, c.Error.Error, "deadline exceeded")
		})
	}
}

func TestExecute_CancelRequestedWinsWhateverWorkReturns(t *testing.T) {
	t.Parallel()
	for _, ret := range []error{nil, errBoom, &job.SnoozeError{Duration: time.Hour}} {
		ex, obs := newExec(t, map[string]executor.Handler{"k": work(func(ctx context.Context) error {
			<-ctx.Done()
			return ret
		})}, nil)
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(job.ErrCancelRequested)
		c, skip := ex.Execute(ctx, newRow("k", 1))
		assert.False(t, skip)
		assert.Equal(t, job.StateCancelled, c.State)
		assert.Equal(t, "cancelled", obs.outcome)
		assert.False(t, c.FinalizedAt.IsZero())
	}
}

func TestExecute_PlainContextCancelIsAnError(t *testing.T) {
	t.Parallel()
	ex, _ := newExec(t, map[string]executor.Handler{"k": work(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // client shutdown (StopAndCancel), not an operator cancel
	c, _ := ex.Execute(ctx, newRow("k", 1))
	assert.Equal(t, job.StateRetryable, c.State)
}

func TestExecute_ErrorHandlers(t *testing.T) {
	t.Parallel()
	var gotErr error
	var gotPanic any
	var gotTrace string
	ex, _ := newExec(t, map[string]executor.Handler{
		"err":   work(func(context.Context) error { return errBoom }),
		"panic": work(func(context.Context) error { panic("p") }),
		"snz":   work(func(context.Context) error { return &job.SnoozeError{Duration: time.Second} }),
	}, func(c *executor.Config) {
		c.HandleError = func(_ context.Context, _ *job.Row, err error) *job.Override {
			gotErr = err
			return &job.Override{SetDiscarded: true}
		}
		c.HandlePanic = func(_ context.Context, _ *job.Row, v any, trace string) *job.Override {
			gotPanic, gotTrace = v, trace
			return &job.Override{SnoozeFor: time.Minute}
		}
	})
	c, _ := ex.Execute(context.Background(), newRow("err", 1))
	assert.Equal(t, job.StateDiscarded, c.State)
	require.ErrorIs(t, gotErr, errBoom)

	c, _ = ex.Execute(context.Background(), newRow("panic", 1))
	assert.Equal(t, job.StateScheduled, c.State)
	assert.Equal(t, t0.Add(time.Minute), c.ScheduledAt)
	assert.Equal(t, "p", gotPanic)
	assert.NotEmpty(t, gotTrace)

	gotErr = nil
	c, _ = ex.Execute(context.Background(), newRow("snz", 1))
	assert.Equal(t, job.StateScheduled, c.State)
	assert.NoError(t, gotErr, "handler not called for explicit snooze")
}

func TestExecute_PanickingHandlerIsIgnored(t *testing.T) {
	t.Parallel()
	ex, _ := newExec(t, map[string]executor.Handler{"k": work(func(context.Context) error { return errBoom })},
		func(c *executor.Config) {
			c.HandleError = func(context.Context, *job.Row, error) *job.Override { panic("handler bug") }
		})
	c, _ := ex.Execute(context.Background(), newRow("k", 1))
	assert.Equal(t, job.StateRetryable, c.State)
}

func TestExecute_PanicWithoutPanicHandlerSkipsErrorHandler(t *testing.T) {
	t.Parallel()
	called := false
	ex, _ := newExec(t, map[string]executor.Handler{"k": work(func(context.Context) error { panic("x") })},
		func(c *executor.Config) {
			c.HandleError = func(context.Context, *job.Row, error) *job.Override {
				called = true
				return nil
			}
		})
	c, _ := ex.Execute(context.Background(), newRow("k", 1))
	assert.Equal(t, job.StateRetryable, c.State)
	assert.False(t, called)
}

func TestExecute_CompleteTx(t *testing.T) {
	t.Parallel()
	var seen *job.Row
	mark := func(ret error) executor.Handler {
		return work(func(ctx context.Context) error {
			a := executor.AttemptFromContext(ctx)
			seen = a.Row
			a.MarkCompletedInTx()
			assert.True(t, a.CompletedInTx())
			return ret
		})
	}
	ex, _ := newExec(t, map[string]executor.Handler{"ok": mark(nil), "fail": mark(errBoom)}, nil)
	r := newRow("ok", 1)
	_, skip := ex.Execute(context.Background(), r)
	assert.True(t, skip, "completion already written by CompleteTx")
	assert.Same(t, r, seen)

	c, skip := ex.Execute(context.Background(), newRow("fail", 1))
	assert.False(t, skip, "an error after CompleteTx goes through the guard")
	assert.Equal(t, job.StateRetryable, c.State)
	assert.Nil(t, executor.AttemptFromContext(context.Background()))
}
