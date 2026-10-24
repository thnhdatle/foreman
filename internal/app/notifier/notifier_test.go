// SPDX-License-Identifier: Apache-2.0

package notifier_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/foreman/internal/app/notifier"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var errConn = errors.New("connection refused")

type fakeListener struct {
	mu          sync.Mutex
	connectFail int
	listenFail  int
	listening   []string
	connects    int
	closes      int
	pings       int
	pingFail    bool
	msgs        chan job.Notification
	drop        chan struct{}
}

func newListener() *fakeListener {
	return &fakeListener{msgs: make(chan job.Notification, 16), drop: make(chan struct{}, 1)}
}

func (f *fakeListener) Connect(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	if f.connectFail > 0 {
		f.connectFail--
		return errConn
	}
	f.listening = nil
	return nil
}

func (f *fakeListener) Listen(_ context.Context, ch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listenFail > 0 {
		f.listenFail--
		return errConn
	}
	f.listening = append(f.listening, ch)
	return nil
}

func (f *fakeListener) WaitForNotification(ctx context.Context) (job.Notification, error) {
	select {
	case <-ctx.Done():
		return job.Notification{}, ctx.Err()
	case <-f.drop:
		return job.Notification{}, errConn
	case m := <-f.msgs:
		return m, nil
	}
}

func (f *fakeListener) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	if f.pingFail {
		return errConn
	}
	return nil
}

func (f *fakeListener) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeListener) stats() (connects, closes int, listening []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := slices.Clone(f.listening)
	slices.Sort(l)
	return f.connects, f.closes, l
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	_, err := notifier.New(notifier.Config{})
	require.Error(t, err)
	n, err := notifier.New(notifier.Config{Listener: newListener(), Clock: clock.New(), MinBackoff: time.Minute})
	require.NoError(t, err)
	assert.False(t, n.Connected())
}

func TestNotifier_DispatchReconnectAndBackoff(t *testing.T) {
	t.Parallel()
	l := newListener()
	l.connectFail = 2
	l.listenFail = 1
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var connects atomic.Int32
	n, err := notifier.New(notifier.Config{
		Listener: l, Clock: fc, MinBackoff: 100 * time.Millisecond, MaxBackoff: 300 * time.Millisecond,
		Rand: func() float64 { return 0 }, OnConnect: func() { connects.Add(1) },
	})
	require.NoError(t, err)
	var mu sync.Mutex
	got := map[string][]string{}
	for _, ch := range []string{"insert", "control"} {
		n.Subscribe(ch, func(p string) { mu.Lock(); got[ch] = append(got[ch], p); mu.Unlock() })
	}
	n.Subscribe("insert", func(string) {})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	for _, d := range []time.Duration{50, 100, 150} {
		testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
		fc.Advance(d*time.Millisecond - time.Millisecond)
		assert.False(t, n.Connected())
		fc.Advance(time.Millisecond)
	}
	testutil.Eventually(t, n.Connected, time.Second)
	c, _, listening := l.stats()
	assert.Equal(t, 4, c)
	assert.Equal(t, []string{"control", "insert"}, listening)
	assert.Equal(t, int32(1), connects.Load())

	l.msgs <- job.Notification{Channel: "insert", Payload: "default"}
	l.msgs <- job.Notification{Channel: "control", Payload: `{"action":"cancel"}`}
	l.msgs <- job.Notification{Channel: "unknown", Payload: "x"}
	testutil.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got["control"]) == 1 }, time.Second)
	mu.Lock()
	assert.Equal(t, []string{"default"}, got["insert"])
	mu.Unlock()

	l.drop <- struct{}{}
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	assert.False(t, n.Connected())
	fc.Advance(50 * time.Millisecond)
	testutil.Eventually(t, func() bool { return connects.Load() == 2 }, time.Second)
	cancel()
	require.NoError(t, <-done)
	_, closes, _ := l.stats()
	assert.Equal(t, 3, closes, "every connection that got past Connect is closed")
}

func TestNotifier_CancelDuringBackoff(t *testing.T) {
	t.Parallel()
	l := newListener()
	l.connectFail = 100
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	n, err := notifier.New(notifier.Config{Listener: l, Clock: fc})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	testutil.Eventually(t, func() bool { return fc.Waiters() == 1 }, time.Second)
	cancel()
	require.NoError(t, <-done)
}

func TestNotifier_PingsWhenIdleAndReconnectsOnFailure(t *testing.T) {
	t.Parallel()
	l := newListener()
	n, err := notifier.New(notifier.Config{
		Listener: l, Clock: clock.New(), IdleTimeout: 10 * time.Millisecond, MinBackoff: time.Millisecond,
	})
	require.NoError(t, err)
	n.Subscribe("insert", func(string) {})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	pings := func() int { l.mu.Lock(); defer l.mu.Unlock(); return l.pings }
	testutil.Eventually(t, func() bool { return pings() >= 2 }, 2*time.Second, "pinged while idle")
	c, _, _ := l.stats()
	assert.Equal(t, 1, c, "a healthy ping keeps the connection")

	l.mu.Lock()
	l.pingFail = true
	l.mu.Unlock()
	testutil.Eventually(t, func() bool { c, _, _ := l.stats(); return c >= 2 }, 2*time.Second, "reconnected after a failed ping")
	cancel()
	require.NoError(t, <-done)
}
