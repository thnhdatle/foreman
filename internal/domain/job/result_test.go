// SPDX-License-Identifier: Apache-2.0

package job_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

var (
	now     = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	retryAt = now.Add(42 * time.Second)
	errBoom = errors.New("boom")
)

func row(attempt, maxAttempts int) *job.Row {
	return &job.Row{ID: 7, Kind: "email", Queue: "default", Attempt: attempt, MaxAttempts: maxAttempts}
}

func TestMapResult_EveryResultType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		attempt   int
		outcome   job.Outcome
		state     job.State
		sched     time.Time
		final     bool
		delta     int
		errSubstr string
		trace     string
		kind      string
	}{
		{name: "success", attempt: 1, outcome: job.Outcome{}, state: job.StateCompleted, final: true, kind: "success"},
		{
			name: "error with attempts left", attempt: 1, outcome: job.Outcome{Err: errBoom},
			state: job.StateRetryable, sched: retryAt, errSubstr: "boom", kind: "error",
		},
		{
			name: "error on last attempt", attempt: 3, outcome: job.Outcome{Err: errBoom},
			state: job.StateDiscarded, final: true, errSubstr: "boom", kind: "error",
		},
		{
			name: "error beyond max attempts", attempt: 4, outcome: job.Outcome{Err: errBoom},
			state: job.StateDiscarded, final: true, errSubstr: "boom", kind: "error",
		},
		{
			name: "panic with attempts left", attempt: 2,
			outcome: job.Outcome{Panicked: true, PanicValue: "nil map", Trace: "goroutine 1"},
			state:   job.StateRetryable, sched: retryAt, errSubstr: "panic: nil map", trace: "goroutine 1", kind: "panic",
		},
		{
			name: "panic on last attempt", attempt: 3, outcome: job.Outcome{Panicked: true, PanicValue: 1},
			state: job.StateDiscarded, final: true, errSubstr: "panic: 1", kind: "panic",
		},
		{
			name: "snooze", attempt: 2, outcome: job.Outcome{Err: &job.SnoozeError{Duration: time.Minute}},
			state: job.StateScheduled, sched: now.Add(time.Minute), delta: -1, kind: "snooze",
		},
		{
			name: "wrapped snooze", attempt: 2,
			outcome: job.Outcome{Err: fmt.Errorf("outer: %w", &job.SnoozeError{Duration: time.Second})},
			state:   job.StateScheduled, sched: now.Add(time.Second), delta: -1, kind: "snooze",
		},
		{
			name: "negative snooze runs now", attempt: 1, outcome: job.Outcome{Err: &job.SnoozeError{Duration: -time.Hour}},
			state: job.StateScheduled, sched: now, delta: -1, kind: "snooze",
		},
		{
			name: "discard", attempt: 1, outcome: job.Outcome{Err: &job.DiscardError{Err: errBoom}},
			state: job.StateDiscarded, final: true, errSubstr: "job discarded: boom", kind: "discard",
		},
		{
			name: "cancel", attempt: 1, outcome: job.Outcome{Err: &job.CancelError{Err: errBoom}},
			state: job.StateCancelled, final: true, errSubstr: "job cancelled: boom", kind: "cancel",
		},
		{
			name: "cancel requested wins over success", attempt: 1, outcome: job.Outcome{CancelRequested: true},
			state: job.StateCancelled, final: true, errSubstr: "cancel requested", kind: "cancelled",
		},
		{
			name: "cancel requested wins over error", attempt: 1, outcome: job.Outcome{CancelRequested: true, Err: errBoom},
			state: job.StateCancelled, final: true, errSubstr: "boom", kind: "cancelled",
		},
		{
			name: "cancel requested wins over snooze", attempt: 1,
			outcome: job.Outcome{CancelRequested: true, Err: &job.SnoozeError{Duration: time.Hour}},
			state:   job.StateCancelled, final: true, errSubstr: "snoozed", kind: "cancelled",
		},
		{
			name: "timeout is an ordinary error", attempt: 1, outcome: job.Outcome{Err: fmt.Errorf("work: %w", errDeadline)},
			state: job.StateRetryable, sched: retryAt, errSubstr: "deadline", kind: "error",
		},
		{
			name: "handler sets cancelled", attempt: 1,
			outcome: job.Outcome{Err: errBoom, Override: &job.Override{SetCancelled: true}},
			state:   job.StateCancelled, final: true, errSubstr: "boom", kind: "error",
		},
		{
			name: "handler sets discarded on panic", attempt: 1,
			outcome: job.Outcome{Panicked: true, PanicValue: "x", Override: &job.Override{SetDiscarded: true}},
			state:   job.StateDiscarded, final: true, errSubstr: "panic: x", kind: "panic",
		},
		{
			name: "handler snoozes", attempt: 3,
			outcome: job.Outcome{Err: errBoom, Override: &job.Override{SnoozeFor: 5 * time.Second}},
			state:   job.StateScheduled, sched: now.Add(5 * time.Second), delta: -1, kind: "error",
		},
		{
			name: "empty override keeps default", attempt: 1,
			outcome: job.Outcome{Err: errBoom, Override: &job.Override{}},
			state:   job.StateRetryable, sched: retryAt, errSubstr: "boom", kind: "error",
		},
		{
			name: "override ignored for explicit discard", attempt: 1,
			outcome: job.Outcome{Err: &job.DiscardError{}, Override: &job.Override{SnoozeFor: time.Second}},
			state:   job.StateDiscarded, final: true, errSubstr: "no reason given", kind: "discard",
		},
		{
			name: "override ignored on success", attempt: 1,
			outcome: job.Outcome{Override: &job.Override{SetDiscarded: true}},
			state:   job.StateCompleted, final: true, kind: "success",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			called := false
			c := job.MapResult(row(tc.attempt, 3), tc.outcome, now, func() time.Time {
				called = true
				return retryAt
			})
			assert.Equal(t, tc.kind, tc.outcome.Kind())
			assert.Equal(t, int64(7), c.ID)
			assert.Equal(t, tc.attempt, c.Attempt)
			assert.Equal(t, now, c.At)
			assert.Equal(t, "email", c.Kind)
			assert.Equal(t, "default", c.Queue)
			assert.Equal(t, tc.state, c.State)
			assert.Equal(t, tc.sched, c.ScheduledAt)
			assert.Equal(t, tc.state == job.StateRetryable, called, "retryAt called only for retries")
			if tc.final {
				assert.Equal(t, now, c.FinalizedAt)
			} else {
				assert.True(t, c.FinalizedAt.IsZero())
			}
			assert.Equal(t, tc.delta, c.AttemptDelta)
			if tc.errSubstr == "" {
				assert.Nil(t, c.Error)
				return
			}
			require.NotNil(t, c.Error)
			assert.Contains(t, c.Error.Error, tc.errSubstr)
			assert.Equal(t, tc.attempt, c.Error.Attempt)
			assert.Equal(t, now, c.Error.At)
			assert.Equal(t, tc.trace, c.Error.Trace)
		})
	}
}

var errDeadline = errors.New("context deadline exceeded")

func TestOutcome_IsHandlerCandidate(t *testing.T) {
	t.Parallel()
	assert.True(t, job.Outcome{Err: errBoom}.IsHandlerCandidate())
	assert.True(t, job.Outcome{Panicked: true}.IsHandlerCandidate())
	assert.False(t, job.Outcome{}.IsHandlerCandidate())
	assert.False(t, job.Outcome{Err: &job.SnoozeError{}}.IsHandlerCandidate())
	assert.False(t, job.Outcome{Err: &job.DiscardError{}}.IsHandlerCandidate())
	assert.False(t, job.Outcome{Err: &job.CancelError{}}.IsHandlerCandidate())
	assert.False(t, job.Outcome{Err: errBoom, CancelRequested: true}.IsHandlerCandidate())
}

func TestResultErrors_MessagesAndUnwrap(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "job snoozed for 1m0s", (&job.SnoozeError{Duration: time.Minute}).Error())
	d := &job.DiscardError{Err: errBoom}
	assert.Equal(t, "job discarded: boom", d.Error())
	require.ErrorIs(t, d, errBoom)
	c := &job.CancelError{Err: errBoom}
	assert.Equal(t, "job cancelled: boom", c.Error())
	require.ErrorIs(t, c, errBoom)
	assert.Equal(t, "job cancelled: no reason given", (&job.CancelError{}).Error())
}

var resultEvents = map[job.State][]job.Event{
	job.StateCompleted: {job.EventSucceeded},
	job.StateRetryable: {job.EventFailedRetry},
	job.StateDiscarded: {job.EventFailedDiscard, job.EventWorkerDiscard},
	job.StateCancelled: {job.EventWorkerCancel},
	job.StateScheduled: {job.EventSnoozed},
}

// Property: every mapped completion is a legal transition out of running,
// retries happen only with attempts left, and only snoozes refund an attempt.
func TestMapResult_Property_LegalTransitions(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		maxAttempts := rapid.IntRange(1, 30).Draw(rt, "max")
		attempt := rapid.IntRange(1, maxAttempts+2).Draw(rt, "attempt")
		var err error
		switch rapid.IntRange(0, 4).Draw(rt, "result") {
		case 1:
			err = errBoom
		case 2:
			err = &job.SnoozeError{Duration: time.Duration(rapid.Int64Range(-10, 1e12).Draw(rt, "snooze"))}
		case 3:
			err = &job.DiscardError{Err: errBoom}
		case 4:
			err = &job.CancelError{}
		}
		o := job.Outcome{
			Err:             err,
			Panicked:        err == nil && rapid.Bool().Draw(rt, "panic"),
			CancelRequested: rapid.Bool().Draw(rt, "cancelRequested"),
		}
		c := job.MapResult(row(attempt, maxAttempts), o, now, func() time.Time { return retryAt })
		allowed := false
		for _, ev := range resultEvents[c.State] {
			if to, e := job.Next(job.StateRunning, ev); e == nil && to == c.State {
				allowed = true
			}
		}
		if !allowed {
			rt.Fatalf("running -> %s is not a legal result transition", c.State)
		}
		if c.State == job.StateRetryable && attempt >= maxAttempts {
			rt.Fatalf("retry scheduled with no attempts left (%d/%d)", attempt, maxAttempts)
		}
		if (c.AttemptDelta != 0) != (c.State == job.StateScheduled) {
			rt.Fatalf("attempt delta %d for state %s", c.AttemptDelta, c.State)
		}
		if o.CancelRequested && c.State != job.StateCancelled {
			rt.Fatalf("cancel requested but state %s", c.State)
		}
		if c.State.IsFinal() != !c.FinalizedAt.IsZero() {
			rt.Fatalf("finalized_at mismatch for %s", c.State)
		}
		if c.State == job.StateScheduled && c.ScheduledAt.Before(now) {
			rt.Fatalf("snooze scheduled in the past")
		}
	})
}
