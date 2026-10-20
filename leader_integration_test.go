// SPDX-License-Identifier: Apache-2.0

//go:build integration

package foreman

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

// step advances the shared fake clock by d in 1s increments, letting the
// clients' goroutines run between increments.
func step(t *testing.T, fc *clock.Fake, d time.Duration, until func() bool) time.Duration {
	t.Helper()
	elapsed := time.Duration(0)
	for elapsed < d {
		if until != nil && until() {
			return elapsed
		}
		fc.Advance(time.Second)
		elapsed += time.Second
		// Give goroutines woken by the advance a moment of real time.
		deadline := time.Now().Add(20 * time.Millisecond)
		testutil.Eventually(t, func() bool { return time.Now().After(deadline) }, time.Second)
	}
	return elapsed
}

func leaderOf(clients []*Client) *Client {
	for _, c := range clients {
		if c.IsLeader() {
			return c
		}
	}
	return nil
}

func TestLeader_FailoverWithinTTLPlusRenewInterval(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	fc := clock.NewFake(time.Now().Truncate(time.Second))
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { return nil }))
	cfg := Config{Queues: map[string]QueueConfig{"default": fastQueue(1)}, Workers: workers, Clock: fc}
	clients := []*Client{e.started(t, cfg), e.started(t, cfg), e.started(t, cfg)}
	testutil.Eventually(t, func() bool { return leaderOf(clients) != nil }, 10*time.Second)
	old := leaderOf(clients)
	lease, err := old.LeaderGet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, old.ID(), lease.LeaderID)
	n := 0
	for _, c := range clients {
		if c.IsLeader() {
			n++
		}
	}
	assert.Equal(t, 1, n, "exactly one leader")

	old.mu.Lock()
	old.run.crash() // leader dies without resigning
	old.mu.Unlock()
	assert.False(t, old.IsLeader())
	elapsed := step(t, fc, time.Minute, func() bool {
		l := leaderOf(clients)
		return l != nil && l != old
	})
	assert.LessOrEqual(t, elapsed, leaderTTL+leaderTTL/3, "new leader within TTL + renew interval")
	assert.GreaterOrEqual(t, elapsed, leaderTTL-leaderTTL/3, "not before the abandoned lease could expire")
}

type tickArgs struct{}

func (tickArgs) Kind() string { return "tick" }

func TestLeader_PeriodicRescueAndMetrics(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	start := time.Date(2026, 6, 1, 12, 0, 30, 0, time.UTC)
	fc := clock.NewFake(start)
	reg := prometheus.NewRegistry()
	var ticks atomic.Int32
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[tickArgs](func(context.Context, *Job[tickArgs]) error { ticks.Add(1); return nil }))
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { return nil }))
	c := e.started(t, Config{
		Queues: map[string]QueueConfig{"default": fastQueue(4)}, Workers: workers, Clock: fc, Registerer: reg,
		HeartbeatInterval: 2 * time.Second, RescueStuckJobsAfter: 10 * time.Second,
		PeriodicJobs: []*PeriodicJob{{
			Name: "every-minute", Schedule: MustCron("* * * * *"), CatchUp: 5 * time.Minute,
			Args: func() (JobArgs, *InsertOpts) { return tickArgs{}, nil },
		}},
	})
	testutil.Eventually(t, c.IsLeader, 10*time.Second)

	// A job left running by a client that died: no heartbeat for > 10s.
	stale := start.Add(-time.Hour)
	res, err := c.Insert(ctx, countArgs{N: 9}, &InsertOpts{Queue: "orphan"})
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, `UPDATE "`+e.schema+`".foreman_jobs SET state = 'running', attempt = 1,
		attempted_by = '{ghost}', attempted_at = $1, heartbeat_at = $1, queue = 'default' WHERE id = $2`, stale, res.Job.ID)
	require.NoError(t, err)

	step(t, fc, 3*time.Minute, func() bool { return ticks.Load() >= 3 })
	assert.GreaterOrEqual(t, ticks.Load(), int32(3))
	r := waitState(t, c, res.Job.ID, StateCompleted)
	assert.Equal(t, 2, r.Attempt, "rescued and run again by a live client")
	assert.Contains(t, r.Errors[0].Error, "rescued: no heartbeat")
	assert.Equal(t, []string{"ghost", c.ID()}, r.AttemptedBy)

	ps, err := c.PeriodicList(ctx)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "every-minute", ps[0].Name)
	assert.InDelta(t, 1, promtest.ToFloat64(c.metrics.Leader), 0)
	assert.GreaterOrEqual(t, promtest.ToFloat64(c.metrics.Rescued), 1.0)
	assert.Positive(t, promtest.CollectAndCount(reg, "foreman_jobs_completed_total"))
	assert.Positive(t, promtest.CollectAndCount(reg, "foreman_maintenance_last_run_timestamp_seconds"))
}

func TestClient_UnservedQueueMetric(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { return nil }))
	e.started(t, Config{Queues: map[string]QueueConfig{"served": fastQueue(1)}, Workers: workers})
	producer := e.client(t, Config{})
	testutil.Eventually(t, func() bool {
		qs, err := producer.QueueList(ctx)
		require.NoError(t, err)
		return len(qs) == 1 && qs[0].LastSeenAt != nil
	}, 10*time.Second)
	_, err := producer.Insert(ctx, countArgs{}, &InsertOpts{Queue: "served"})
	require.NoError(t, err)
	_, err = producer.Insert(ctx, countArgs{}, &InsertOpts{Queue: "nobody"})
	require.NoError(t, err)
	assert.InDelta(t, 0, promtest.ToFloat64(producer.metrics.Unserved.WithLabelValues("served")), 0)
	assert.InDelta(t, 1, promtest.ToFloat64(producer.metrics.Unserved.WithLabelValues("nobody")), 0)
}
