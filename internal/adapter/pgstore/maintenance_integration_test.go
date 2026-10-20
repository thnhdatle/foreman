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
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestPromoteDue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	due := jobInState(t, e, job.StateScheduled)
	retry := jobInState(t, e, job.StateRetryable)
	n, err := e.store.PromoteDue(ctx, t0.Add(30*time.Minute), 10)
	require.NoError(t, err)
	assert.Zero(t, n)
	n, err = e.store.PromoteDue(ctx, t0.Add(time.Hour), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "limited batch")
	n, err = e.store.PromoteDue(ctx, t0.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	for _, id := range []int64{due, retry} {
		r, _ := e.store.JobGet(ctx, id)
		assert.Equal(t, job.StateAvailable, r.State)
	}
}

func TestStuckJobsAndRescue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rows := running(t, e, "c1", 3) // heartbeat_at = t0
	_, err := e.store.Heartbeat(ctx, "c1", t0.Add(time.Hour), []job.Ref{{ID: rows[2].ID, Attempt: 1}})
	require.NoError(t, err)
	cutoff := t0.Add(time.Minute)
	stuck, err := e.store.StuckJobs(ctx, cutoff, 10)
	require.NoError(t, err)
	require.Len(t, stuck, 2)
	now := t0.Add(2 * time.Minute)
	cs := []job.Completion{
		{
			ID: stuck[0].ID, Attempt: 1, State: job.StateRetryable, ScheduledAt: now, At: now,
			Error: &job.AttemptError{At: now, Attempt: 1, Error: "rescued"},
		},
		{ID: stuck[1].ID, Attempt: 1, State: job.StateDiscarded, FinalizedAt: now, At: now},
		{ID: rows[2].ID, Attempt: 1, State: job.StateRetryable, ScheduledAt: now, At: now}, // heartbeat is fresh
	}
	n, err := e.store.Rescue(ctx, cutoff, cs)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	r, _ := e.store.JobGet(ctx, stuck[0].ID)
	assert.Equal(t, job.StateRetryable, r.State)
	assert.Equal(t, "rescued", r.Errors[0].Error)
	r, _ = e.store.JobGet(ctx, rows[2].ID)
	assert.Equal(t, job.StateRunning, r.State)
	n, err = e.store.Rescue(ctx, cutoff, nil)
	require.NoError(t, err)
	assert.Zero(t, n)
	// The rescued attempt's late completion is stale.
	applied, err := e.store.Complete(ctx, "c1", []job.Completion{{ID: stuck[0].ID, Attempt: 1, State: job.StateCompleted, FinalizedAt: now, At: now}})
	require.NoError(t, err)
	assert.Empty(t, applied)
}

func TestDeleteFinalized(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		jobInState(t, e, job.StateCompleted)
	}
	jobInState(t, e, job.StateDiscarded)
	n, err := e.store.DeleteFinalized(ctx, job.StateCompleted, t0, 10)
	require.NoError(t, err)
	assert.Zero(t, n, "finalized exactly at cutoff is kept")
	n, err = e.store.DeleteFinalized(ctx, job.StateCompleted, t0.Add(time.Second), 2)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = e.store.DeleteFinalized(ctx, job.StateCompleted, t0.Add(time.Second), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, e.count(t, "state = 'discarded'"))
}

func TestReindexAndQueueDepths(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	jobInState(t, e, job.StateAvailable)
	jobInState(t, e, job.StateAvailable)
	jobInState(t, e, job.StateRunning)
	jobInState(t, e, job.StateCompleted)
	// Skipped while another index build runs in the database (other tests migrate in parallel).
	testutil.Eventually(t, func() bool {
		ran, err := e.store.Reindex(ctx)
		require.NoError(t, err)
		return ran
	}, 30*time.Second)
	depths, err := e.store.QueueDepths(ctx)
	require.NoError(t, err)
	assert.Equal(t, []job.QueueDepth{
		{Queue: "q", State: job.StateAvailable, Count: 2},
		{Queue: "q", State: job.StateRunning, Count: 1},
	}, depths)
}

func TestListener_ReceivesInsertAndControlNotifications(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	l := e.store.NewListener()
	_, err := l.WaitForNotification(ctx)
	require.Error(t, err, "not connected")
	require.Error(t, l.Listen(ctx, "x"))
	require.Error(t, l.Ping(ctx))
	require.NoError(t, l.Connect(ctx))
	defer func() { require.NoError(t, l.Close(context.Background())) }()
	names := e.store.Names()
	require.NoError(t, l.Listen(ctx, names.InsertChannel))
	require.NoError(t, l.Listen(ctx, names.ControlChannel))

	// An idle timeout leaves the connection usable (the notifier pings it).
	idleCtx, idleCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = l.WaitForNotification(idleCtx)
	idleCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, l.Ping(ctx))

	// One notification per queue per statement, only for available jobs.
	sched := params("k", "q2")
	sched.State, sched.ScheduledAt = job.StateScheduled, t0.Add(time.Hour)
	e.insert(t, params("k", "q1"), params("k", "q1"), sched)
	n, err := l.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, job.Notification{Channel: names.InsertChannel, Payload: "q1"}, n)

	require.NoError(t, e.store.QueueSetPaused(ctx, "q1", true, t0))
	n, err = l.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, names.ControlChannel, n.Channel)
	assert.JSONEq(t, `{"action":"pause","queue":"q1"}`, n.Payload)

	id := running(t, e, "c1", 1)[0].ID
	n, err = l.WaitForNotification(ctx) // insert of the running helper's job
	require.NoError(t, err)
	assert.Equal(t, "q", n.Payload)
	_, err = e.store.JobUpdate(ctx, "cancel", filterIDs(id), t0, 10)
	require.NoError(t, err)
	n, err = l.WaitForNotification(ctx)
	require.NoError(t, err)
	var msg map[string]any
	require.NoError(t, jsonUnmarshal(n.Payload, &msg))
	assert.Equal(t, "cancel", msg["action"])
	assert.EqualValues(t, id, msg["job_id"])
	require.NoError(t, l.Close(ctx))
	require.NoError(t, l.Close(ctx), "closing twice is fine")
}

func TestNames(t *testing.T) {
	t.Parallel()
	n, err := pgstoreNames("")
	require.NoError(t, err)
	assert.Equal(t, "foreman_insert", n.InsertChannel)
	assert.Equal(t, `"public"`, n.Quoted)
	n, err = pgstoreNames("tenant_a")
	require.NoError(t, err)
	assert.Equal(t, "tenant_a_foreman_control", n.ControlChannel)
	_, err = pgstoreNames(`bad"name`)
	require.Error(t, err)
}
