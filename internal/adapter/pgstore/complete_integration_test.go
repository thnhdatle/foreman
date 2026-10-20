// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

func running(t *testing.T, e *env, client string, n int) []*job.Row {
	t.Helper()
	ps := make([]job.InsertParams, n)
	for i := range ps {
		ps[i] = params("k", "q")
		// Sort ahead of other fixtures so exactly these jobs are fetched.
		ps[i].Priority = 1
		ps[i].ScheduledAt = t0.Add(-time.Hour)
	}
	e.insert(t, ps...)
	return fetch(t, e, job.FetchParams{Queue: "q", ClientID: client, Limit: n})
}

func TestComplete_AppliesEveryOutcomeUnderGuard(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rows := running(t, e, "c1", 5)
	later := t0.Add(time.Hour)
	errEntry := &job.AttemptError{At: t0, Attempt: 1, Error: "boom", Trace: "stack"}
	cs := []job.Completion{
		{ID: rows[0].ID, Attempt: 1, State: job.StateCompleted, FinalizedAt: t0, At: t0},
		{ID: rows[1].ID, Attempt: 1, State: job.StateRetryable, ScheduledAt: later, Error: errEntry, At: t0},
		{ID: rows[2].ID, Attempt: 1, State: job.StateScheduled, ScheduledAt: later, AttemptDelta: -1, At: t0},
		{ID: rows[3].ID, Attempt: 1, State: job.StateDiscarded, FinalizedAt: t0, Error: errEntry, At: t0},
		{ID: rows[4].ID, Attempt: 2, State: job.StateCompleted, FinalizedAt: t0, At: t0}, // wrong attempt: stale
	}
	applied, err := e.store.Complete(ctx, "c1", cs)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{rows[0].ID, rows[1].ID, rows[2].ID, rows[3].ID}, applied)

	get := func(id int64) *job.Row {
		r, err := e.store.JobGet(ctx, id)
		require.NoError(t, err)
		return r
	}
	r := get(rows[0].ID)
	assert.Equal(t, job.StateCompleted, r.State)
	require.NotNil(t, r.FinalizedAt)
	r = get(rows[1].ID)
	assert.Equal(t, job.StateRetryable, r.State)
	assert.True(t, later.Equal(r.ScheduledAt))
	assert.Nil(t, r.FinalizedAt)
	require.Len(t, r.Errors, 1)
	assert.Equal(t, "boom", r.Errors[0].Error)
	assert.Equal(t, "stack", r.Errors[0].Trace)
	r = get(rows[2].ID)
	assert.Equal(t, job.StateScheduled, r.State)
	assert.Equal(t, 0, r.Attempt, "snooze does not consume an attempt")
	r = get(rows[3].ID)
	assert.Equal(t, job.StateDiscarded, r.State)
	assert.Len(t, r.Errors, 1)
	assert.Equal(t, job.StateRunning, get(rows[4].ID).State)

	// A second completion of the same attempt is stale.
	applied, err = e.store.Complete(ctx, "c1", cs[:1])
	require.NoError(t, err)
	assert.Empty(t, applied)
	empty, err := e.store.Complete(ctx, "c1", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestComplete_GuardRejectsOtherClient(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rows := running(t, e, "owner", 1)
	applied, err := e.store.Complete(context.Background(), "intruder", []job.Completion{
		{ID: rows[0].ID, Attempt: 1, State: job.StateCompleted, FinalizedAt: t0, At: t0},
	})
	require.NoError(t, err)
	assert.Empty(t, applied)
}

func TestComplete_CancelRequestedForcesCancelled(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rows := running(t, e, "c1", 2)
	_, err := e.store.JobUpdate(ctx, "cancel", filterIDs(rows[0].ID, rows[1].ID), t0, 10)
	require.NoError(t, err)
	at := t0.Add(5 * time.Second)
	applied, err := e.store.Complete(ctx, "c1", []job.Completion{
		{ID: rows[0].ID, Attempt: 1, State: job.StateCompleted, FinalizedAt: at, At: at},
		{ID: rows[1].ID, Attempt: 1, State: job.StateRetryable, ScheduledAt: t0.Add(time.Hour), At: at},
	})
	require.NoError(t, err)
	assert.Len(t, applied, 2)
	for _, r := range rows {
		got, err := e.store.JobGet(ctx, r.ID)
		require.NoError(t, err)
		assert.Equal(t, job.StateCancelled, got.State)
		require.NotNil(t, got.FinalizedAt)
		assert.True(t, at.Equal(*got.FinalizedAt))
	}
}

func TestCompleteTx(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rows := running(t, e, "c1", 1)
	ref := job.Ref{ID: rows[0].ID, Attempt: 1}

	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, e.store.CompleteTx(ctx, tx, "c1", ref, t0))
	require.NoError(t, tx.Rollback(ctx))
	r, _ := e.store.JobGet(ctx, ref.ID)
	assert.Equal(t, job.StateRunning, r.State, "rolled back with the caller's transaction")

	tx, err = e.pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, e.store.CompleteTx(ctx, tx, "c1", ref, t0))
	require.NoError(t, tx.Commit(ctx))
	r, _ = e.store.JobGet(ctx, ref.ID)
	assert.Equal(t, job.StateCompleted, r.State)

	tx, err = e.pool.Begin(ctx)
	require.NoError(t, err)
	err = e.store.CompleteTx(ctx, tx, "c1", ref, t0)
	require.ErrorIs(t, err, job.ErrStale)
	require.NoError(t, tx.Rollback(ctx))
}

func TestHeartbeatAndCancelPoll(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rows := running(t, e, "c1", 3)
	refs := []job.Ref{{ID: rows[0].ID, Attempt: 1}, {ID: rows[1].ID, Attempt: 9}, {ID: rows[2].ID, Attempt: 1}}
	n, err := e.store.Heartbeat(ctx, "c1", t0.Add(time.Minute), refs)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "wrong attempt is rejected by the guard")
	n, err = e.store.Heartbeat(ctx, "c2", t0.Add(time.Minute), refs)
	require.NoError(t, err)
	assert.Zero(t, n)
	n, err = e.store.Heartbeat(ctx, "c1", t0, nil)
	require.NoError(t, err)
	assert.Zero(t, n)
	r, _ := e.store.JobGet(ctx, rows[0].ID)
	assert.True(t, t0.Add(time.Minute).Equal(*r.HeartbeatAt))

	_, err = e.store.JobUpdate(ctx, "cancel", filterIDs(rows[2].ID), t0, 10)
	require.NoError(t, err)
	ids, err := e.store.CancelRequested(ctx, []int64{rows[0].ID, rows[2].ID})
	require.NoError(t, err)
	assert.Equal(t, []int64{rows[2].ID}, ids)
	none, err := e.store.CancelRequested(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ttl := 15 * time.Second
	ok, err := e.store.AcquireLease(ctx, "default", "a", t0, ttl)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = e.store.AcquireLease(ctx, "default", "b", t0.Add(10*time.Second), ttl)
	require.NoError(t, err)
	assert.False(t, ok, "held by a until expiry")
	ok, err = e.store.AcquireLease(ctx, "default", "a", t0.Add(5*time.Second), ttl)
	require.NoError(t, err)
	assert.True(t, ok, "the holder may re-acquire")
	l, err := e.store.LeaderGet(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, "a", l.LeaderID)
	assert.True(t, t0.Equal(l.ElectedAt), "re-acquiring keeps elected_at")

	ok, err = e.store.RenewLease(ctx, "default", "a", t0.Add(10*time.Second), ttl)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = e.store.RenewLease(ctx, "default", "b", t0.Add(10*time.Second), ttl)
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = e.store.AcquireLease(ctx, "default", "b", t0.Add(26*time.Second), ttl)
	require.NoError(t, err)
	assert.True(t, ok, "expired lease is taken over")
	ok, err = e.store.RenewLease(ctx, "default", "a", t0.Add(27*time.Second), ttl)
	require.NoError(t, err)
	assert.False(t, ok, "the old leader learns it lost")

	require.NoError(t, e.store.ReleaseLease(ctx, "default", "a"), "releasing a lease one does not hold is a no-op")
	require.NoError(t, e.store.ReleaseLease(ctx, "default", "b"))
	_, err = e.store.LeaderGet(ctx, "default")
	require.ErrorIs(t, err, job.ErrNotFound)
}
