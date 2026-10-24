// SPDX-License-Identifier: Apache-2.0

package producer_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/app/producer"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

func insert(s *testutil.FakeStore, queue, key string, prio int) int64 {
	return s.Insert(job.InsertParams{
		Kind: "k", Queue: queue, State: job.StateAvailable, Priority: prio, MaxAttempts: 3,
		ScheduledAt: t0, CreatedAt: t0, ConcurrencyKey: key,
	})
}

func completeIn(s *testutil.FakeStore, clientID string, before func(ctx context.Context, r *job.Row)) func(context.Context, *job.Row) {
	return func(ctx context.Context, r *job.Row) {
		if before != nil {
			before(ctx, r)
		}
		_, _ = s.Complete(context.WithoutCancel(ctx), clientID, []job.Completion{{
			ID: r.ID, Attempt: r.Attempt, State: job.StateCompleted, FinalizedAt: t0, At: t0,
		}})
	}
}

type running struct {
	p      *producer.Producer
	cancel context.CancelFunc
	done   chan error
}

func run(t *testing.T, cfg producer.Config) *running {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "c1"
	}
	if cfg.Queue == "" {
		cfg.Queue = "q"
	}
	p, err := producer.New(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{p: p, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- p.Run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	require.NoError(t, <-r.done)
	r.p.CancelAll()
	require.NoError(t, r.p.Wait(context.Background()))
}

func countState(s *testutil.FakeStore, st job.State) int {
	n := 0
	for _, r := range s.Jobs() {
		if r.State == st {
			n++
		}
	}
	return n
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	_, err := producer.New(producer.Config{})
	require.Error(t, err)
	s := testutil.NewFakeStore()
	exec := func(context.Context, *job.Row) {}
	_, err = producer.New(producer.Config{Queue: "q", ClientID: "c", Store: s, Clock: clock.New(), Execute: exec, PerKeyLimit: -1})
	require.Error(t, err)
	p, err := producer.New(producer.Config{Queue: "q", ClientID: "c", Store: s, Clock: clock.New(), Execute: exec})
	require.NoError(t, err)
	assert.Equal(t, "q", p.Queue())
	assert.Equal(t, 0, p.RunningCount())
}

func TestProducer_RunsJobsWithinMaxWorkers(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	for i := 0; i < 40; i++ {
		insert(s, "q", "", 2)
	}
	insert(s, "other", "", 2)
	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	r := run(t, producer.Config{
		Store: s, MaxWorkers: 4, FetchInterval: 10 * time.Millisecond, FetchCooldown: time.Millisecond,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			inFlight.Add(-1)
		}),
	})
	testutil.Eventually(t, func() bool { return r.p.RunningCount() == 4 }, 2*time.Second)
	assert.Len(t, r.p.Running(), 4)
	close(release)
	testutil.Eventually(t, func() bool { return countState(s, job.StateCompleted) == 40 }, 5*time.Second)
	assert.LessOrEqual(t, peak.Load(), int32(4))
	assert.Equal(t, 1, countState(s, job.StateAvailable), "other queue untouched")
}

func TestProducer_WakeFetchesWithoutWaitingForPoll(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	var ran atomic.Int32
	r := run(t, producer.Config{
		Store: s, FetchInterval: time.Hour,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	insert(s, "q", "", 2)
	r.p.Wake()
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second)
}

func TestProducer_PollFallback(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	var ran atomic.Int32
	run(t, producer.Config{
		Store: s, FetchInterval: 20 * time.Millisecond,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	insert(s, "q", "", 2)
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second)
}

func TestProducer_PausedFetchesNothing(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	s.SetPaused("q", true)
	insert(s, "q", "", 2)
	var ran atomic.Int32
	var fetches atomic.Int32
	r := run(t, producer.Config{
		Store: s, FetchInterval: 10 * time.Millisecond,
		Execute:   completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
		OnFetched: func([]*job.Row, time.Duration) { fetches.Add(1) },
	})
	testutil.Eventually(t, r.p.Paused, time.Second)
	r.p.Wake()
	deadline := time.Now().Add(80 * time.Millisecond)
	testutil.Eventually(t, func() bool { return time.Now().After(deadline) }, time.Second)
	assert.Zero(t, ran.Load())
	assert.Zero(t, fetches.Load())

	r.p.SetPaused(true)
	s.SetPaused("q", false)
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second, "resume picked up by polling")
}

func TestProducer_SetPausedFalseWakes(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	var ran atomic.Int32
	r := run(t, producer.Config{
		Store: s, FetchInterval: time.Hour,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	r.p.SetPaused(true)
	insert(s, "q", "", 2)
	r.p.Wake()
	r.p.SetPaused(false)
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second)
}

func TestProducer_CanFetchGate(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	insert(s, "q", "", 2)
	var open atomic.Bool
	var ran atomic.Int32
	r := run(t, producer.Config{
		Store: s, FetchInterval: 10 * time.Millisecond, CanFetch: open.Load,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	r.p.Wake()
	deadline := time.Now().Add(50 * time.Millisecond)
	testutil.Eventually(t, func() bool { return time.Now().After(deadline) }, time.Second)
	assert.Zero(t, ran.Load())
	open.Store(true)
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second)
}

func TestProducer_FetchErrorsRetryOnPoll(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	insert(s, "q", "", 2)
	s.FailNext(3)
	var ran atomic.Int32
	run(t, producer.Config{
		Store: s, FetchInterval: 10 * time.Millisecond,
		Execute: completeIn(s, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second)
}

func TestProducer_CancelAndCancelAll(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	a := insert(s, "q", "", 2)
	insert(s, "q", "", 2)
	causes := sync.Map{}
	r := run(t, producer.Config{
		Store: s, FetchInterval: 10 * time.Millisecond,
		Execute: func(ctx context.Context, row *job.Row) {
			<-ctx.Done()
			causes.Store(row.ID, context.Cause(ctx))
		},
	})
	testutil.Eventually(t, func() bool { return r.p.RunningCount() == 2 }, 2*time.Second)
	assert.False(t, r.p.Cancel(999))
	assert.True(t, r.p.Cancel(a))
	testutil.Eventually(t, func() bool { return r.p.RunningCount() == 1 }, 2*time.Second)
	c, _ := causes.Load(a)
	require.ErrorIs(t, c.(error), job.ErrCancelRequested)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.p.Wait(ctx), context.DeadlineExceeded)
	r.p.CancelAll()
	require.NoError(t, r.p.Wait(context.Background()))
	causes.Range(func(k, v any) bool {
		if k.(int64) != a {
			assert.ErrorIs(t, v.(error), producer.ErrStopping)
		}
		return true
	})
}

func TestProducer_JobsOutliveFetchLoop(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	insert(s, "q", "", 2)
	release := make(chan struct{})
	var ctxErr atomic.Value
	r := run(t, producer.Config{
		Store: s, FetchInterval: 10 * time.Millisecond,
		Execute: completeIn(s, "c1", func(ctx context.Context, _ *job.Row) {
			<-release
			ctxErr.Store(fmt.Sprint(ctx.Err()))
		}),
	})
	testutil.Eventually(t, func() bool { return r.p.RunningCount() == 1 }, 2*time.Second)
	r.cancel()
	require.NoError(t, <-r.done)
	r.cancel = nil
	close(release)
	require.NoError(t, r.p.Wait(context.Background()))
	assert.Equal(t, "<nil>", ctxErr.Load(), "graceful stop does not cancel running jobs")
	assert.Equal(t, 1, countState(s, job.StateCompleted))
}

func TestProducer_FairnessServesSmallKeyAheadOfBacklog(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	for i := 0; i < 200; i++ {
		insert(s, "q", "A", 2)
	}
	b := insert(s, "q", "B", 2)
	var order []int64
	var mu sync.Mutex
	run(t, producer.Config{
		Store: s, MaxWorkers: 1, Fairness: true, FetchInterval: 10 * time.Millisecond, FetchCooldown: time.Millisecond,
		Execute: completeIn(s, "c1", func(_ context.Context, r *job.Row) {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, r.ID)
		}),
	})
	testutil.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 3
	}, 2*time.Second)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, order[:3], b, "B is served within the first fetches despite A's backlog")
}

func TestProducer_UnfairKeyedFetchFollowsOrder(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	high := insert(s, "q", "B", 1)
	for i := 0; i < 5; i++ {
		insert(s, "q", "A", 2)
	}
	var first atomic.Int64
	run(t, producer.Config{
		Store: s, MaxWorkers: 1, PerKeyLimit: 3, FetchInterval: 10 * time.Millisecond,
		Execute: completeIn(s, "c1", func(_ context.Context, r *job.Row) { first.CompareAndSwap(0, r.ID) }),
	})
	testutil.Eventually(t, func() bool { return countState(s, job.StateCompleted) == 6 }, 2*time.Second)
	assert.Equal(t, high, first.Load(), "priority 1 job first")
}

func TestProducer_Property_PerKeyLimitAcrossProducers(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		producers := rapid.IntRange(2, 4).Draw(rt, "producers")
		keys := rapid.IntRange(1, 4).Draw(rt, "keys")
		limit := rapid.IntRange(1, 3).Draw(rt, "limit")
		jobs := rapid.IntRange(10, 40).Draw(rt, "jobs")
		fair := rapid.Bool().Draw(rt, "fair")
		s := testutil.NewFakeStore()
		var violation atomic.Value
		for i := 0; i < jobs; i++ {
			insert(s, "q", fmt.Sprintf("k%d", i%keys), 2)
		}
		var mu sync.Mutex
		active := map[string]int{}
		var runs []*running
		for i := 0; i < producers; i++ {
			id := fmt.Sprintf("p%d", i)
			cfg := producer.Config{
				Queue: "q", ClientID: id, Store: s, Clock: clock.New(), MaxWorkers: 3, PerKeyLimit: limit,
				Fairness: fair, FetchInterval: 5 * time.Millisecond, FetchCooldown: time.Millisecond,
				Execute: completeIn(s, id, func(_ context.Context, r *job.Row) {
					mu.Lock()
					active[r.ConcurrencyKey]++
					if active[r.ConcurrencyKey] > limit {
						violation.Store(fmt.Sprintf("worker side: key %s has %d", r.ConcurrencyKey, active[r.ConcurrencyKey]))
					}
					mu.Unlock()
					for k, n := range s.RunningByKey("q") {
						if n > limit {
							violation.Store(fmt.Sprintf("store side: key %s has %d running", k, n))
						}
					}
					mu.Lock()
					active[r.ConcurrencyKey]--
					mu.Unlock()
				}),
			}
			p, err := producer.New(cfg)
			if err != nil {
				rt.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			rr := &running{p: p, cancel: cancel, done: make(chan error, 1)}
			go func() { rr.done <- p.Run(ctx) }()
			runs = append(runs, rr)
		}
		deadline := time.Now().Add(10 * time.Second)
		for countState(s, job.StateCompleted) < jobs && time.Now().Before(deadline) {
			for _, rr := range runs {
				rr.p.Wake()
			}
			<-time.After(time.Millisecond)
		}
		for _, rr := range runs {
			rr.cancel()
			<-rr.done
			rr.p.CancelAll()
			_ = rr.p.Wait(context.Background())
		}
		if v := violation.Load(); v != nil {
			rt.Fatalf("per-key limit %d violated: %v", limit, v)
		}
		if got := countState(s, job.StateCompleted); got != jobs {
			rt.Fatalf("completed %d of %d jobs", got, jobs)
		}
	})
}

type hangingStore struct {
	*testutil.FakeStore
	hung atomic.Bool
}

func (h *hangingStore) Fetch(ctx context.Context, p job.FetchParams) ([]*job.Row, error) {
	if h.hung.CompareAndSwap(false, true) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return h.FakeStore.Fetch(ctx, p)
}

func TestProducer_HungStoreCallTimesOut(t *testing.T) {
	t.Parallel()
	s := &hangingStore{FakeStore: testutil.NewFakeStore()}
	insert(s.FakeStore, "q", "", 2)
	var ran atomic.Int32
	run(t, producer.Config{
		Store: s, FetchInterval: 20 * time.Millisecond, CallTimeout: 50 * time.Millisecond,
		Execute: completeIn(s.FakeStore, "c1", func(context.Context, *job.Row) { ran.Add(1) }),
	})
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 2*time.Second, "fetch retried after the timeout")
	assert.True(t, s.hung.Load())
}
