// SPDX-License-Identifier: Apache-2.0

package elector_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/foreman/internal/app/elector"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

type node struct {
	e        *elector.Elector
	cancel   context.CancelFunc
	done     chan error
	services atomic.Int32
	starts   atomic.Int32
	mu       sync.Mutex
	changes  []bool
}

func (n *node) changeLog() []bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]bool(nil), n.changes...)
}

func startNode(t *testing.T, id string, s *testutil.FakeStore, fc *clock.Fake) *node {
	t.Helper()
	n := &node{done: make(chan error, 1)}
	e, err := elector.New(elector.Config{
		ClientID: id, Store: s, Clock: fc, TTL: 15 * time.Second,
		Services: func(ctx context.Context) {
			n.services.Add(1)
			n.starts.Add(1)
			<-ctx.Done()
			n.services.Add(-1)
		},
		OnChange: func(l bool) {
			n.mu.Lock()
			defer n.mu.Unlock()
			n.changes = append(n.changes, l)
		},
	})
	require.NoError(t, err)
	n.e = e
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go func() { n.done <- e.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-n.done
		n.done <- nil
	})
	return n
}

func (n *node) stop(t *testing.T) {
	t.Helper()
	n.cancel()
	require.NoError(t, <-n.done)
	n.done <- nil
}

func settle(t *testing.T, fc *clock.Fake, waiters int) {
	t.Helper()
	testutil.Eventually(t, func() bool { return fc.Waiters() >= waiters }, 2*time.Second)
}

func TestNew_ValidatesAndDefaults(t *testing.T) {
	t.Parallel()
	_, err := elector.New(elector.Config{})
	require.Error(t, err)
	e, err := elector.New(elector.Config{
		ClientID: "a", Store: testutil.NewFakeStore(), Clock: clock.NewFake(t0), Services: func(context.Context) {},
	})
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, e.Interval(), "default TTL 15s, renew every TTL/3")
	assert.False(t, e.IsLeader())
	e.Crash()
}

func TestElector_SingleLeaderAndServices(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	b := startNode(t, "b", s, fc)
	settle(t, fc, 2)
	assert.False(t, b.e.IsLeader())
	assert.Equal(t, int32(1), a.services.Load())
	assert.Equal(t, int32(0), b.services.Load())
	holder, expires := s.LeaseHolder()
	assert.Equal(t, "a", holder)
	assert.Equal(t, t0.Add(15*time.Second), expires)

	for i := 1; i <= 6; i++ {
		fc.Advance(5 * time.Second)
		settle(t, fc, 2)
		testutil.Eventually(t, func() bool {
			_, exp := s.LeaseHolder()
			return exp.Equal(t0.Add(time.Duration(5*i+15) * time.Second))
		}, time.Second)
		assert.True(t, a.e.IsLeader())
		assert.False(t, b.e.IsLeader())
	}
	assert.Equal(t, int32(1), a.starts.Load(), "services started once per term")
	assert.Equal(t, []bool{true}, a.changeLog())
}

func TestElector_ResignOnShutdownHandsOver(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	b := startNode(t, "b", s, fc)
	settle(t, fc, 2)
	a.stop(t)
	holder, _ := s.LeaseHolder()
	assert.Empty(t, holder, "lease released on shutdown")
	assert.Equal(t, int32(0), a.services.Load())
	assert.Equal(t, []bool{true, false}, a.changeLog())
	b.e.Wake()
	testutil.Eventually(t, b.e.IsLeader, time.Second)
	assert.Equal(t, int32(1), b.services.Load())
}

func TestElector_CrashAbandonsLeaseUntilExpiry(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	b := startNode(t, "b", s, fc)
	settle(t, fc, 2)

	a.e.Crash()
	assert.Equal(t, int32(0), a.services.Load(), "services stopped immediately")
	assert.False(t, a.e.IsLeader())
	holder, expires := s.LeaseHolder()
	assert.Equal(t, "a", holder, "lease abandoned, not released")
	assert.Equal(t, t0.Add(15*time.Second), expires)

	elapsed := time.Duration(0)
	for !b.e.IsLeader() && elapsed <= 20*time.Second {
		settle(t, fc, 1)
		fc.Advance(time.Second)
		elapsed += time.Second
		testutil.Eventually(t, func() bool { return fc.Waiters() >= 1 }, time.Second)
	}
	testutil.Eventually(t, b.e.IsLeader, time.Second)
	assert.Greater(t, elapsed, 15*time.Second, "not before the lease expired")
	assert.LessOrEqual(t, elapsed, 20*time.Second, "within TTL + renew interval")
}

func TestElector_RenewFailureStopsServicesImmediately(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	settle(t, fc, 1)
	s.FailNext(1)
	fc.Advance(5 * time.Second)
	testutil.Eventually(t, func() bool { return !a.e.IsLeader() }, time.Second)
	assert.Equal(t, int32(0), a.services.Load())
	settle(t, fc, 1)
	fc.Advance(5 * time.Second)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	assert.Equal(t, int32(2), a.starts.Load())
	assert.Equal(t, []bool{true, false, true}, a.changeLog())
}

func TestElector_LostLeaseToAnotherClient(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	settle(t, fc, 1)
	require.NoError(t, s.ReleaseLease(context.Background(), "default", "a"))
	ok, err := s.AcquireLease(context.Background(), "default", "intruder", fc.Now(), time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	fc.Advance(5 * time.Second)
	testutil.Eventually(t, func() bool { return !a.e.IsLeader() }, time.Second)
	assert.Equal(t, int32(0), a.services.Load())
}

func TestElector_AcquireErrorRetries(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	s.FailNext(1)
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	settle(t, fc, 1)
	assert.False(t, a.e.IsLeader())
	fc.Advance(5 * time.Second)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
}

func TestElector_ReleaseErrorIsTolerated(t *testing.T) {
	t.Parallel()
	s := testutil.NewFakeStore()
	fc := clock.NewFake(t0)
	a := startNode(t, "a", s, fc)
	testutil.Eventually(t, a.e.IsLeader, time.Second)
	settle(t, fc, 1)
	s.FailNext(1)
	a.stop(t)
	holder, _ := s.LeaseHolder()
	assert.Equal(t, "a", holder, "release failed; lease left to expire")
}
