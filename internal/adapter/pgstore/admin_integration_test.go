// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgstore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

func filterIDs(ids ...int64) pgstore.Filter { return pgstore.Filter{IDs: ids} }

func jobInState(t *testing.T, e *env, st job.State) int64 {
	t.Helper()
	switch st {
	case job.StateAvailable:
		return e.insert(t, params("k", "q"))[0].Row.ID
	case job.StateScheduled:
		p := params("k", "q")
		p.State, p.ScheduledAt = job.StateScheduled, t0.Add(time.Hour)
		return e.insert(t, p)[0].Row.ID
	case job.StateRunning:
		return running(t, e, "c1", 1)[0].ID
	default:
		id := running(t, e, "c1", 1)[0].ID
		c := job.Completion{ID: id, Attempt: 1, State: st, At: t0}
		if st.IsFinal() {
			c.FinalizedAt = t0
		} else {
			c.ScheduledAt = t0.Add(time.Hour)
		}
		_, err := e.store.Complete(context.Background(), "c1", []job.Completion{c})
		require.NoError(t, err)
		return id
	}
}

func TestJobUpdate_CancelInEachState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		state     job.State
		changed   bool
		final     job.State
		requested bool
	}{
		{job.StateAvailable, true, job.StateCancelled, false},
		{job.StateScheduled, true, job.StateCancelled, false},
		{job.StateRetryable, true, job.StateCancelled, false},
		{job.StateRunning, true, job.StateRunning, true},
		{job.StateCompleted, false, job.StateCompleted, false},
		{job.StateCancelled, false, job.StateCancelled, false},
		{job.StateDiscarded, false, job.StateDiscarded, false},
	}
	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			id := jobInState(t, e, tc.state)
			rows, err := e.store.JobUpdate(ctx, pgstore.OpCancel, filterIDs(id), t0, 10)
			require.NoError(t, err)
			assert.Equal(t, tc.changed, len(rows) == 1)
			r, err := e.store.JobGet(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, tc.final, r.State)
			assert.Equal(t, tc.requested, r.CancelRequestedAt != nil)
		})
	}
}

func TestJobUpdate_DiscardAndRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	runningID := jobInState(t, e, job.StateRunning)
	availID := jobInState(t, e, job.StateAvailable)
	doneID := jobInState(t, e, job.StateCompleted)

	rows, err := e.store.JobUpdate(ctx, pgstore.OpDiscard, filterIDs(runningID, availID, doneID), t0, 10)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "completed jobs cannot be discarded")
	for _, r := range rows {
		assert.Equal(t, job.StateDiscarded, r.State)
	}
	applied, err := e.store.Complete(ctx, "c1", []job.Completion{{ID: runningID, Attempt: 1, State: job.StateCompleted, FinalizedAt: t0, At: t0}})
	require.NoError(t, err)
	assert.Empty(t, applied)

	later := t0.Add(time.Minute)
	rows, err = e.store.JobUpdate(ctx, pgstore.OpRetry, filterIDs(runningID, availID, doneID), later, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for _, r := range rows {
		assert.Equal(t, job.StateAvailable, r.State)
		assert.Equal(t, 0, r.Attempt)
		assert.Nil(t, r.FinalizedAt)
		assert.True(t, later.Equal(r.ScheduledAt))
		var meta map[string]any
		require.NoError(t, json.Unmarshal(r.Metadata, &meta))
		assert.EqualValues(t, 1, meta["foreman_retry_count"])
		assert.Contains(t, []any{"discarded", "completed"}, meta["foreman_retried_from"])
	}
	rows, err = e.store.JobUpdate(ctx, pgstore.OpRetry, filterIDs(availID), later, 10)
	require.NoError(t, err)
	assert.Empty(t, rows, "available jobs cannot be retried")
}

func TestJobUpdate_RetryUniqueConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	opts := unique.Options{ByArgs: true}
	first := e.insert(t, uniqueParams("u", `{}`, opts))[0].Row.ID
	_, err := e.store.JobUpdate(ctx, pgstore.OpDiscard, filterIDs(first), t0, 10)
	require.NoError(t, err)
	e.insert(t, uniqueParams("u", `{}`, opts))
	_, err = e.store.JobUpdate(ctx, pgstore.OpRetry, filterIDs(first), t0, 10)
	require.ErrorIs(t, err, job.ErrUniqueConflict)
}

func TestJobUpdateAllAndDeleteAll(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	var ps []job.InsertParams
	for i := 0; i < 2100; i++ {
		p := params("bulk", "q")
		if i%2 == 0 {
			p.Kind = "other"
		}
		ps = append(ps, p)
	}
	e.insert(t, ps...)
	n, err := e.store.JobUpdateAll(ctx, pgstore.OpDiscard, pgstore.Filter{Kinds: []string{"bulk"}}, t0)
	require.NoError(t, err)
	assert.Equal(t, 1050, n)
	n, err = e.store.JobUpdateAll(ctx, pgstore.OpRetry, pgstore.Filter{States: []job.State{job.StateDiscarded}}, t0)
	require.NoError(t, err)
	assert.Equal(t, 1050, n)
	runningID := running(t, e, "c1", 1)[0].ID
	n, err = e.store.JobDeleteAll(ctx, pgstore.Filter{Queues: []string{"q"}, CreatedBefore: t0.Add(time.Second)})
	require.NoError(t, err)
	assert.Equal(t, 2100, n, "running jobs are never deleted")
	_, err = e.store.JobGet(ctx, runningID)
	require.NoError(t, err)
	n, err = e.store.JobDeleteAll(ctx, pgstore.Filter{CreatedAfter: t0.Add(time.Hour)})
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestJobList_FiltersAndCursor(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	var ps []job.InsertParams
	for i := 0; i < 7; i++ {
		p := params("a", "q")
		if i >= 5 {
			p.Kind = "b"
		}
		ps = append(ps, p)
	}
	res := e.insert(t, ps...)
	page, err := e.store.JobList(ctx, pgstore.Filter{Kinds: []string{"a"}}, 3)
	require.NoError(t, err)
	require.Len(t, page, 3)
	assert.Equal(t, res[4].Row.ID, page[0].ID, "newest first")
	next, err := e.store.JobList(ctx, pgstore.Filter{Kinds: []string{"a"}, BeforeID: page[2].ID}, 3)
	require.NoError(t, err)
	assert.Len(t, next, 2)
	all, err := e.store.JobList(ctx, pgstore.Filter{States: []job.State{job.StateAvailable}, Queues: []string{"q"}}, 50)
	require.NoError(t, err)
	assert.Len(t, all, 7)
	_, err = e.store.JobGet(ctx, 999999)
	require.ErrorIs(t, err, job.ErrNotFound)
}

func TestQueueListAndPeriodicList(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.insert(t, params("k", "alpha"), params("k", "alpha"), params("k", "beta"))
	require.NoError(t, e.store.QueueSetPaused(ctx, "gamma", true, t0))
	qs, err := e.store.QueueList(ctx)
	require.NoError(t, err)
	require.Len(t, qs, 3)
	assert.Equal(t, "alpha", qs[0].Name)
	assert.EqualValues(t, 2, qs[0].Counts[job.StateAvailable])
	assert.Nil(t, qs[0].PausedAt)
	assert.Equal(t, "gamma", qs[2].Name)
	assert.NotNil(t, qs[2].PausedAt)
	assert.Empty(t, qs[2].Counts)

	for _, tick := range []time.Time{t0, t0.Add(time.Minute)} {
		ok, err := e.store.EnqueueTick(ctx, "report", tick, params("report", "q"))
		require.NoError(t, err)
		assert.True(t, ok)
	}
	ok, err := e.store.EnqueueTick(ctx, "report", t0, params("report", "q"))
	require.NoError(t, err)
	assert.False(t, ok, "a tick is enqueued once")
	assert.Equal(t, 2, e.count(t, "kind = 'report'"))
	ps, err := e.store.PeriodicList(ctx)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.EqualValues(t, 2, ps[0].Ticks)
	assert.True(t, t0.Add(time.Minute).Equal(ps[0].LastTick))
	last, err := e.store.LastTicks(ctx)
	require.NoError(t, err)
	assert.True(t, t0.Add(time.Minute).Equal(last["report"]))
	n, err := e.store.PruneTicks(ctx, t0.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the latest tick is kept")
}
