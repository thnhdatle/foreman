// SPDX-License-Identifier: Apache-2.0

package completer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/app/completer"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

type fakeHeartbeats struct {
	mu    sync.Mutex
	calls [][]job.Ref
	at    []time.Time
	fail  bool
}

func (f *fakeHeartbeats) Heartbeat(_ context.Context, clientID string, now time.Time, refs []job.Ref) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if clientID != "c1" {
		panic("wrong client id " + clientID)
	}
	if f.fail {
		return 0, errDown
	}
	f.calls = append(f.calls, append([]job.Ref(nil), refs...))
	f.at = append(f.at, now)
	return len(refs), nil
}

func (f *fakeHeartbeats) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestNewHeartbeater_Validates(t *testing.T) {
	t.Parallel()
	_, err := completer.NewHeartbeater(completer.HeartbeatConfig{})
	require.Error(t, err)
	_, err = completer.NewHeartbeater(completer.HeartbeatConfig{
		ClientID: "c1", Store: &fakeHeartbeats{}, Clock: clock.NewFake(t0), Running: func() []job.Ref { return nil },
	})
	require.Error(t, err, "interval must be positive")
}

func TestHeartbeater_BeatsRunningJobsEveryInterval(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(t0)
	store := &fakeHeartbeats{}
	var mu sync.Mutex
	running := make([]job.Ref, 2500)
	for i := range running {
		running[i] = job.Ref{ID: int64(i), Attempt: 1}
	}
	h, err := completer.NewHeartbeater(completer.HeartbeatConfig{
		ClientID: "c1", Store: store, Clock: fc, Interval: 30 * time.Second,
		Running: func() []job.Ref {
			mu.Lock()
			defer mu.Unlock()
			return running
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	fc.Advance(29 * time.Second)
	assert.Equal(t, 0, store.count())
	fc.Advance(time.Second)
	testutil.Eventually(t, func() bool { return store.count() == 3 }, time.Second)
	store.mu.Lock()
	assert.Len(t, store.calls[0], 1000)
	assert.Len(t, store.calls[1], 1000)
	assert.Len(t, store.calls[2], 500)
	assert.Equal(t, t0.Add(30*time.Second), store.at[0])
	store.mu.Unlock()

	mu.Lock()
	running = nil
	mu.Unlock()
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	fc.Advance(30 * time.Second)
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	assert.Equal(t, 3, store.count(), "nothing running, nothing written")

	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()
	mu.Lock()
	running = []job.Ref{{ID: 1, Attempt: 1}}
	mu.Unlock()
	fc.Advance(30 * time.Second) // failure is logged and the loop continues
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	cancel()
	require.NoError(t, <-done)
}

func TestHeartbeater_Beat(t *testing.T) {
	t.Parallel()
	store := &fakeHeartbeats{}
	h, err := completer.NewHeartbeater(completer.HeartbeatConfig{
		ClientID: "c1", Store: store, Clock: clock.NewFake(t0), Interval: time.Second,
		Running: func() []job.Ref { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, h.Beat(context.Background(), job.Ref{ID: 9, Attempt: 2}))
	assert.Equal(t, [][]job.Ref{{{ID: 9, Attempt: 2}}}, store.calls)
	store.fail = true
	require.ErrorIs(t, h.Beat(context.Background(), job.Ref{ID: 9, Attempt: 2}), errDown)
}
