// SPDX-License-Identifier: Apache-2.0

package maintenance_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/app/maintenance"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/schedule"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func minuteJob(t *testing.T, name string, catchUp time.Duration) maintenance.PeriodicJob {
	t.Helper()
	s, err := schedule.Every(time.Minute)
	require.NoError(t, err)
	return maintenance.PeriodicJob{
		Name: name, Schedule: s, CatchUp: catchUp,
		Build: func(tick time.Time) (job.InsertParams, error) {
			return job.InsertParams{Kind: "tick", Queue: "q", State: job.StateAvailable, ScheduledAt: tick, CreatedAt: tick}, nil
		},
	}
}

type periodicRun struct {
	stop    func()
	mu      sync.Mutex
	skipped map[string]int
	ticks   int
}

func startPeriodic(t *testing.T, s *testutil.FakeStore, fc *clock.Fake, jobs ...maintenance.PeriodicJob) *periodicRun {
	t.Helper()
	base, _ := newBase(s, fc)
	pr := &periodicRun{skipped: map[string]int{}}
	p := &maintenance.Periodic{
		Base: base, Jobs: jobs,
		OnSkipped:  func(name string, n int) { pr.mu.Lock(); pr.skipped[name] += n; pr.mu.Unlock() },
		OnEnqueued: func(string, time.Time) { pr.mu.Lock(); pr.ticks++; pr.mu.Unlock() },
	}
	pr.stop = runService(t, p)
	return pr
}

func advanceSeconds(t *testing.T, fc *clock.Fake, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		testutil.Eventually(t, func() bool { return fc.Waiters() >= 1 }, time.Second)
		fc.Advance(time.Second)
	}
}

func TestPeriodic_NoJobsReturnsImmediately(t *testing.T) {
	t.Parallel()
	p := &maintenance.Periodic{}
	require.NoError(t, p.Run(context.Background()))
}

func TestPeriodic_EnqueuesEachTickOnce(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0.Add(30 * time.Second))
	startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	advanceSeconds(t, fc, 150)
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 3 }, time.Second)
	assert.Equal(t, []time.Time{t0.Add(time.Minute), t0.Add(2 * time.Minute), t0.Add(3 * time.Minute)}, s.Ticks("m"))
	assert.Len(t, s.Jobs(), 3)
}

func TestPeriodic_LeaderGapCatchesUpAndSkipsOlderTicks(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	first := startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	advanceSeconds(t, fc, 60)
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 1 }, time.Second)
	first.stop()

	fc.Advance(10 * time.Minute)
	second := startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 7 }, time.Second)
	want := []time.Time{t0.Add(time.Minute)}
	for m := 6; m <= 11; m++ {
		want = append(want, t0.Add(time.Duration(m)*time.Minute))
	}
	assert.Equal(t, want, s.Ticks("m"))
	second.mu.Lock()
	assert.Equal(t, 4, second.skipped["m"])
	assert.Equal(t, 6, second.ticks)
	second.mu.Unlock()
}

func TestPeriodic_OverlappingLeadersNeverDuplicate(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	b := startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	for i := 0; i < 180; i++ {
		testutil.Eventually(t, func() bool { return fc.Waiters() >= 2 }, time.Second)
		fc.Advance(time.Second)
	}
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 3 }, time.Second)
	assert.Len(t, s.Jobs(), 3, "one job per tick despite two leaders")
	a.mu.Lock()
	b.mu.Lock()
	assert.Equal(t, 3, a.ticks+b.ticks)
	b.mu.Unlock()
	a.mu.Unlock()
}

func TestPeriodic_RunOnStartAndFailures(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0.Add(10 * time.Second))
	j := minuteJob(t, "m", time.Minute)
	j.RunOnStart = true
	broken := minuteJob(t, "broken", time.Minute)
	broken.Build = func(time.Time) (job.InsertParams, error) { return job.InsertParams{}, errBuild }
	s.FailNext(1)
	startPeriodic(t, s, fc, j, broken)
	advanceSeconds(t, fc, 1)
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 1 }, time.Second)
	assert.Equal(t, []time.Time{t0.Add(11 * time.Second)}, s.Ticks("m"), "run on start enqueues at start time")
	advanceSeconds(t, fc, 50)
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 2 }, time.Second)
	assert.Empty(t, s.Ticks("broken"))
}

func TestPeriodic_EnqueueErrorRetriesSameTick(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0.Add(59 * time.Second))
	startPeriodic(t, s, fc, minuteJob(t, "m", time.Minute))
	testutil.Eventually(t, func() bool { return fc.Waiters() >= 1 }, time.Second)
	s.FailNext(1)
	advanceSeconds(t, fc, 1)
	advanceSeconds(t, fc, 1)
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 1 }, time.Second)
	assert.Equal(t, []time.Time{t0.Add(time.Minute)}, s.Ticks("m"))
}

func TestPeriodic_AllMissedTicksSkipped(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	j := minuteJob(t, "m", 0)
	hourly, err := schedule.ParseCron("@hourly", nil)
	require.NoError(t, err)
	j.Schedule = hourly
	_, err = s.EnqueueTick(context.Background(), "m", t0.Add(-5*time.Hour), job.InsertParams{Kind: "tick", Queue: "q"})
	require.NoError(t, err)
	fc.Set(t0.Add(30 * time.Minute))
	pr := startPeriodic(t, s, fc, j)
	testutil.Eventually(t, func() bool { pr.mu.Lock(); defer pr.mu.Unlock(); return pr.skipped["m"] == 5 }, time.Second)
	advanceSeconds(t, fc, 3)
	pr.mu.Lock()
	assert.Equal(t, 5, pr.skipped["m"], "skipped ticks are counted once")
	pr.mu.Unlock()
	assert.Len(t, s.Ticks("m"), 1)
}

func TestPeriodic_StartMarkerLetsNextLeaderCatchUp(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0.Add(50 * time.Second))
	first := startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	testutil.Eventually(t, func() bool { last, _ := s.LastTicks(context.Background()); return !last["m"].IsZero() },
		time.Second, "start marker recorded")
	first.stop()
	fc.Advance(30 * time.Second)
	startPeriodic(t, s, fc, minuteJob(t, "m", 5*time.Minute))
	testutil.Eventually(t, func() bool { return len(s.Ticks("m")) == 1 }, time.Second)
	assert.Equal(t, []time.Time{t0.Add(time.Minute)}, s.Ticks("m"), "the 09:01 tick is caught up")
	require.Error(t, func() error { s.FailNext(1); return s.MarkPeriodicStart(context.Background(), "x", t0) }())
}
