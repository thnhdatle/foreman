// SPDX-License-Identifier: Apache-2.0

package maintenance_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/foreman/internal/app/maintenance"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

type observations struct {
	mu   sync.Mutex
	rows map[string]int
	runs map[string]int
}

func (o *observations) observe(service string, rows int, _ time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rows[service] += rows
	o.runs[service]++
}

func (o *observations) get(service string) (rows, runs int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rows[service], o.runs[service]
}

func newBase(s *testutil.FakeStore, fc *clock.Fake) (maintenance.Base, *observations) {
	o := &observations{rows: map[string]int{}, runs: map[string]int{}}
	return maintenance.Base{Store: s, Clock: fc, Observe: o.observe}, o
}

func runService(t *testing.T, svc maintenance.Service) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		maintenance.RunAll(ctx, svc)
		close(done)
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

func add(s *testutil.FakeStore, st job.State, at time.Time, attempt int) int64 {
	id := s.Insert(job.InsertParams{Kind: "k", Queue: "q", State: st, Priority: 2, MaxAttempts: 3, ScheduledAt: at, CreatedAt: t0})
	s.Update(id, func(r *job.Row) { r.Attempt = attempt })
	return id
}

func state(t *testing.T, s *testutil.FakeStore, id int64) job.State {
	t.Helper()
	r, ok := s.Job(id)
	require.True(t, ok)
	return r.State
}

func TestScheduler_PromotesDueJobsInBatches(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	var due []int64
	for i := 0; i < 5; i++ {
		due = append(due, add(s, job.StateScheduled, t0, 0))
	}
	retry := add(s, job.StateRetryable, t0.Add(-time.Second), 1)
	future := add(s, job.StateScheduled, t0.Add(10*time.Second), 0)
	runService(t, &maintenance.Scheduler{Base: base, Batch: 2})
	testutil.Eventually(t, func() bool { rows, runs := obs.get("scheduler"); return runs == 1 && rows == 6 }, time.Second)
	for _, id := range append(due, retry) {
		assert.Equal(t, job.StateAvailable, state(t, s, id))
	}
	assert.Equal(t, job.StateScheduled, state(t, s, future))
	fc.Advance(10 * time.Second)
	testutil.Eventually(t, func() bool { return state(t, s, future) == job.StateAvailable }, time.Second)
}

func TestScheduler_ErrorIsRetriedNextRun(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	id := add(s, job.StateScheduled, t0, 0)
	s.FailNext(1)
	runService(t, &maintenance.Scheduler{Base: base})
	testutil.Eventually(t, func() bool { _, runs := obs.get("scheduler"); return runs == 1 }, time.Second)
	assert.Equal(t, job.StateScheduled, state(t, s, id))
	fc.Advance(time.Second)
	testutil.Eventually(t, func() bool { return state(t, s, id) == job.StateAvailable }, time.Second)
}

func TestRescuer_RescuesStuckJobs(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	stale := t0.Add(-2 * time.Hour)
	fresh := t0.Add(-time.Minute)
	stuck := add(s, job.StateRunning, t0, 1)
	exhausted := add(s, job.StateRunning, t0, 3)
	alive := add(s, job.StateRunning, t0, 1)
	s.Update(stuck, func(r *job.Row) { r.HeartbeatAt = &stale })
	s.Update(exhausted, func(r *job.Row) { r.HeartbeatAt = &stale })
	s.Update(alive, func(r *job.Row) { r.HeartbeatAt = &fresh })
	var rescued int
	var mu sync.Mutex
	runService(t, &maintenance.Rescuer{
		Base: base, Interval: 30 * time.Second, StuckAfter: time.Hour, Batch: 1,
		Retry:     func(_ *job.Row, now time.Time) time.Time { return now.Add(time.Second) },
		OnRescued: func(n int) { mu.Lock(); rescued += n; mu.Unlock() },
	})
	testutil.Eventually(t, func() bool { rows, _ := obs.get("rescuer"); return rows == 2 }, time.Second)
	r, _ := s.Job(stuck)
	assert.Equal(t, job.StateRetryable, r.State)
	assert.Equal(t, t0.Add(time.Second), r.ScheduledAt)
	require.Len(t, r.Errors, 1)
	assert.Contains(t, r.Errors[0].Error, "rescued: no heartbeat since 2026-05-01T07:00:00Z")
	e, _ := s.Job(exhausted)
	assert.Equal(t, job.StateDiscarded, e.State)
	require.NotNil(t, e.FinalizedAt)
	assert.Equal(t, t0, *e.FinalizedAt)
	assert.Equal(t, job.StateRunning, state(t, s, alive))
	mu.Lock()
	assert.Equal(t, 2, rescued)
	mu.Unlock()

	fc.Advance(time.Hour)
	testutil.Eventually(t, func() bool { return state(t, s, alive) == job.StateRetryable }, time.Second)
}

func TestRescueCompletion_NeverHeartbeat(t *testing.T) {
	t.Parallel()
	c := maintenance.RescueCompletion(&job.Row{ID: 1, Attempt: 1, MaxAttempts: 2}, t0, func(*job.Row, time.Time) time.Time { return t0 })
	assert.Equal(t, job.StateRetryable, c.State)
	assert.Contains(t, c.Error.Error, "since never")
	assert.Equal(t, t0, c.At)
}

func TestRescuer_StoreErrors(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	s.FailNext(1)
	runService(t, &maintenance.Rescuer{
		Base: base, Interval: time.Second, StuckAfter: time.Hour,
		Retry: func(*job.Row, time.Time) time.Time { return t0 },
	})
	testutil.Eventually(t, func() bool { _, runs := obs.get("rescuer"); return runs == 1 }, time.Second)
}

func TestCleaner_DeletesPerStateRetention(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	final := func(st job.State, age time.Duration) int64 {
		id := add(s, st, t0, 1)
		f := t0.Add(-age)
		s.Update(id, func(r *job.Row) { r.FinalizedAt = &f })
		return id
	}
	oldDone := []int64{final(job.StateCompleted, 2*time.Hour), final(job.StateCompleted, 3*time.Hour), final(job.StateCompleted, 25*time.Hour)}
	youngDone := final(job.StateCompleted, 30*time.Minute)
	oldCancelled := final(job.StateCancelled, 8*24*time.Hour)
	keptDiscarded := final(job.StateDiscarded, 300*24*time.Hour)
	running := add(s, job.StateRunning, t0, 1)
	_, err := s.EnqueueTick(context.Background(), "old", t0.Add(-30*24*time.Hour), job.InsertParams{Kind: "k", Queue: "q"})
	require.NoError(t, err)
	_, err = s.EnqueueTick(context.Background(), "old", t0.Add(-29*24*time.Hour), job.InsertParams{Kind: "k", Queue: "q"})
	require.NoError(t, err)
	runService(t, &maintenance.Cleaner{Base: base, Batch: 2, Retention: map[job.State]time.Duration{
		job.StateCompleted: time.Hour, job.StateCancelled: 7 * 24 * time.Hour, job.StateDiscarded: 0,
	}})
	testutil.Eventually(t, func() bool { rows, _ := obs.get("cleaner"); return rows == 5 }, time.Second)
	assert.Equal(t, []time.Time{t0.Add(-29 * 24 * time.Hour)}, s.Ticks("old"), "latest tick kept")
	for _, id := range append(oldDone, oldCancelled) {
		_, ok := s.Job(id)
		assert.False(t, ok, "job %d deleted", id)
	}
	for _, id := range []int64{youngDone, keptDiscarded, running} {
		_, ok := s.Job(id)
		assert.True(t, ok, "job %d kept", id)
	}
	parked := func(runs int) func() bool {
		return func() bool { _, n := obs.get("cleaner"); return n == runs && fc.Waiters() >= 1 }
	}
	testutil.Eventually(t, parked(1), time.Second)
	fc.Advance(31 * time.Minute)
	testutil.Eventually(t, func() bool { _, ok := s.Job(youngDone); return !ok }, time.Second)
	testutil.Eventually(t, parked(2), time.Second)
	s.FailNext(1)
	fc.Advance(maintenance.DefaultCleanerInterval)
	testutil.Eventually(t, func() bool { _, runs := obs.get("cleaner"); return runs == 3 }, time.Second)
}

func TestReindexer(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	off := &maintenance.Reindexer{Base: base}
	require.NoError(t, off.Run(context.Background()), "zero interval disables the reindexer")

	runService(t, &maintenance.Reindexer{Base: base, Interval: 24 * time.Hour})
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	_, runs := obs.get("reindexer")
	assert.Zero(t, runs, "not at election time")
	fc.Advance(24 * time.Hour)
	testutil.Eventually(t, func() bool { rows, n := obs.get("reindexer"); return n == 1 && rows == 1 }, time.Second)
	s.FailNext(1)
	testutil.Eventually(t, func() bool { return fc.Waiters() >= 1 }, time.Second)
	fc.Advance(24 * time.Hour)
	testutil.Eventually(t, func() bool { return fc.Waiters() >= 1 }, time.Second)
	_, runs = obs.get("reindexer")
	assert.Equal(t, 1, runs, "failed run not observed")
}

func TestDepthSampler(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	base, obs := newBase(s, fc)
	add(s, job.StateAvailable, t0, 0)
	add(s, job.StateAvailable, t0, 0)
	add(s, job.StateRunning, t0, 1)
	var got []job.QueueDepth
	var mu sync.Mutex
	s.FailNext(1)
	runService(t, &maintenance.DepthSampler{Base: base, OnDepths: func(d []job.QueueDepth) { mu.Lock(); got = d; mu.Unlock() }})
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	fc.Advance(maintenance.DefaultDepthInterval)
	testutil.Eventually(t, func() bool { _, runs := obs.get("depth_sampler"); return runs == 1 }, time.Second)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []job.QueueDepth{
		{Queue: "q", State: job.StateAvailable, Count: 2}, {Queue: "q", State: job.StateRunning, Count: 1},
	}, got)
}

func TestCancelPoller(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := add(s, job.StateRunning, t0, 1)
	b := add(s, job.StateRunning, t0, 1)
	now := t0
	s.Update(b, func(r *job.Row) { r.CancelRequestedAt = &now })
	var mu sync.Mutex
	var cancelled []int64
	var running []int64
	p := &maintenance.CancelPoller{
		Store: s, Clock: fc, Interval: time.Second,
		Running:  func() []int64 { mu.Lock(); defer mu.Unlock(); return running },
		OnCancel: func(id int64) { mu.Lock(); cancelled = append(cancelled, id); mu.Unlock() },
	}
	p.Poll(context.Background())
	mu.Lock()
	running = []int64{a, b}
	mu.Unlock()
	s.FailNext(1)
	p.Poll(context.Background())
	runService(t, p)
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	fc.Advance(time.Second)
	testutil.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(cancelled) == 1 }, time.Second)
	mu.Lock()
	assert.Equal(t, []int64{b}, cancelled)
	mu.Unlock()
}

var errBuild = errors.New("build failed")

func TestRunAll_WaitsForEveryService(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	stopped := 0
	svc := serviceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		mu.Lock()
		stopped++
		mu.Unlock()
		return nil
	})
	done := make(chan struct{})
	go func() { maintenance.RunAll(ctx, svc, svc, svc); close(done) }()
	cancel()
	<-done
	assert.Equal(t, 3, stopped)
}

type serviceFunc func(ctx context.Context) error

func (f serviceFunc) Run(ctx context.Context) error { return f(ctx) }
