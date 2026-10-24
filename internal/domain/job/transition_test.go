// SPDX-License-Identifier: Apache-2.0

package job_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

var allEvents = []job.Event{
	job.EventScheduleReached, job.EventFetched, job.EventSucceeded, job.EventFailedRetry,
	job.EventFailedDiscard, job.EventWorkerDiscard, job.EventWorkerCancel, job.EventSnoozed,
	job.EventRescued, job.EventRescuedExhausted, job.EventBackoffElapsed, job.EventAdminCancel,
	job.EventAdminDiscard, job.EventAdminRetry,
}

var expected = map[job.State]map[job.Event]job.State{
	job.StateAvailable: {
		job.EventFetched:      job.StateRunning,
		job.EventAdminCancel:  job.StateCancelled,
		job.EventAdminDiscard: job.StateDiscarded,
	},
	job.StateScheduled: {
		job.EventScheduleReached: job.StateAvailable,
		job.EventAdminCancel:     job.StateCancelled,
		job.EventAdminDiscard:    job.StateDiscarded,
	},
	job.StateRunning: {
		job.EventSucceeded:        job.StateCompleted,
		job.EventFailedRetry:      job.StateRetryable,
		job.EventRescued:          job.StateRetryable,
		job.EventFailedDiscard:    job.StateDiscarded,
		job.EventWorkerDiscard:    job.StateDiscarded,
		job.EventRescuedExhausted: job.StateDiscarded,
		job.EventAdminDiscard:     job.StateDiscarded,
		job.EventWorkerCancel:     job.StateCancelled,
		job.EventSnoozed:          job.StateScheduled,
	},
	job.StateRetryable: {
		job.EventBackoffElapsed: job.StateAvailable,
		job.EventAdminCancel:    job.StateCancelled,
		job.EventAdminDiscard:   job.StateDiscarded,
	},
	job.StateCompleted: {job.EventAdminRetry: job.StateAvailable},
	job.StateCancelled: {job.EventAdminRetry: job.StateAvailable},
	job.StateDiscarded: {job.EventAdminRetry: job.StateAvailable},
}

func TestNext_MatchesSpecDiagramExhaustively(t *testing.T) {
	t.Parallel()
	for _, from := range job.AllStates {
		for _, ev := range allEvents {
			to, err := job.Next(from, ev)
			want, ok := expected[from][ev]
			if ok {
				require.NoErrorf(t, err, "%s --%s-->", from, ev)
				assert.Equalf(t, want, to, "%s --%s-->", from, ev)
				assert.True(t, job.CanApply(from, ev))
				continue
			}
			require.Errorf(t, err, "%s --%s--> must be invalid", from, ev)
			require.ErrorIs(t, err, job.ErrInvalidTransition)
			var ite *job.InvalidTransitionError
			require.ErrorAs(t, err, &ite)
			assert.Equal(t, from, ite.From)
			assert.Equal(t, ev, ite.Event)
			assert.Contains(t, err.Error(), string(ev))
			assert.False(t, job.CanApply(from, ev))
		}
	}
}

func TestInvalidTransitionError_IsOnlyMatchesSentinel(t *testing.T) {
	t.Parallel()
	err := &job.InvalidTransitionError{From: job.StateCompleted, Event: job.EventFetched}
	require.ErrorIs(t, err, job.ErrInvalidTransition)
	require.NotErrorIs(t, err, job.ErrNotFound)
	assert.Equal(t, "job: fetched not allowed in state completed", err.Error())
}

func TestState_Properties(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state job.State
		final bool
		bit   int
	}{
		{job.StateAvailable, false, 0},
		{job.StateScheduled, false, 1},
		{job.StateRunning, false, 2},
		{job.StateRetryable, false, 3},
		{job.StateCompleted, true, 4},
		{job.StateCancelled, true, 5},
		{job.StateDiscarded, true, 6},
		{job.State("bogus"), false, -1},
	}
	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.final, tc.state.IsFinal())
			assert.Equal(t, tc.bit, tc.state.Bit())
			assert.Equal(t, tc.bit >= 0, tc.state.Valid())
			parsed, err := job.ParseState(string(tc.state))
			if tc.bit < 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "bogus")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.state, parsed)
		})
	}
	assert.Equal(t, []job.State{job.StateAvailable, job.StateScheduled, job.StateRunning, job.StateRetryable},
		job.PendingStates)
}

func TestInitialState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, job.StateAvailable, job.InitialState(now, now))
	assert.Equal(t, job.StateAvailable, job.InitialState(now.Add(-time.Second), now))
	assert.Equal(t, job.StateScheduled, job.InitialState(now.Add(time.Nanosecond), now))
	assert.Equal(t, job.StateAvailable, job.InitialState(time.Time{}, now))
}

func TestNext_Property_RandomWalksStayValid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		state := rapid.SampledFrom([]job.State{job.StateAvailable, job.StateScheduled}).Draw(rt, "initial")
		steps := rapid.IntRange(1, 60).Draw(rt, "steps")
		for i := 0; i < steps; i++ {
			ev := rapid.SampledFrom(allEvents).Draw(rt, "event")
			next, err := job.Next(state, ev)
			if err != nil {
				if !errors.Is(err, job.ErrInvalidTransition) {
					rt.Fatalf("unexpected error type %v", err)
				}
				continue
			}
			if !next.Valid() {
				rt.Fatalf("invalid state %q", next)
			}
			if state.IsFinal() && ev != job.EventAdminRetry {
				rt.Fatalf("left final state %s via %s", state, ev)
			}
			if next == job.StateRunning && (state != job.StateAvailable || ev != job.EventFetched) {
				rt.Fatalf("entered running from %s via %s", state, ev)
			}
			state = next
		}
	})
}
