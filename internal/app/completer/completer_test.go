// SPDX-License-Identifier: Apache-2.0

package completer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/foreman/internal/app/completer"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
	"github.com/thnhdatle/foreman/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var (
	t0      = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	errDown = errors.New("db down")
)

type fakeWriter struct {
	mu      sync.Mutex
	calls   [][]job.Completion
	clients []string
	fail    int            // number of upcoming calls that fail
	stale   map[int64]bool // ids rejected by the guard
	dead    bool           // fail forever
	block   chan struct{}  // when non-nil, calls wait for it
}

func (f *fakeWriter) Complete(ctx context.Context, clientID string, cs []job.Completion) ([]int64, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead || f.fail > 0 {
		f.fail--
		return nil, errDown
	}
	f.calls = append(f.calls, append([]job.Completion(nil), cs...))
	f.clients = append(f.clients, clientID)
	var applied []int64
	for _, c := range cs {
		if !f.stale[c.ID] {
			applied = append(applied, c.ID)
		}
	}
	return applied, nil
}

func (f *fakeWriter) written() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []int64
	for _, call := range f.calls {
		for _, c := range call {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

func (f *fakeWriter) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, 0, len(f.calls))
	for _, call := range f.calls {
		out = append(out, len(call))
	}
	return out
}

type harness struct {
	c      *completer.Completer
	fc     *clock.Fake
	store  *fakeWriter
	cancel context.CancelFunc
	done   chan error

	mu      sync.Mutex
	applied []job.Completion
	stale   int
	drained int
}

func start(t *testing.T, store *fakeWriter, mutate func(*completer.Config)) *harness {
	t.Helper()
	h := &harness{fc: clock.NewFake(t0), store: store, done: make(chan error, 1)}
	cfg := completer.Config{
		ClientID: "client-1", Store: store, Clock: h.fc,
		BatchSize: 3, FlushInterval: 50 * time.Millisecond, BufferSize: 5,
		MaxRetryDelay: time.Second, FlushTimeout: time.Second,
		Rand: func() float64 { return 0 },
		OnApplied: func(a []job.Completion, stale int) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.applied = append(h.applied, a...)
			h.stale += stale
		},
		OnDrained: func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.drained++
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := completer.New(cfg)
	require.NoError(t, err)
	h.c = c
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-h.done
	})
	return h
}

// waitBlocked waits until Run is parked on the fake clock.
func (h *harness) waitBlocked(t *testing.T) {
	t.Helper()
	testutil.Eventually(t, func() bool { return h.fc.Waiters() > 0 }, 2*time.Second)
}

func comp(id int64) job.Completion {
	return job.Completion{ID: id, Attempt: 1, State: job.StateCompleted, Kind: "k", Queue: "q"}
}

func TestNew_ValidatesAndDefaults(t *testing.T) {
	t.Parallel()
	_, err := completer.New(completer.Config{})
	require.Error(t, err)
	_, err = completer.New(completer.Config{ClientID: "c", Store: &fakeWriter{}})
	require.Error(t, err)
	c, err := completer.New(completer.Config{ClientID: "c", Store: &fakeWriter{}, Clock: clock.NewFake(t0)})
	require.NoError(t, err)
	for i := 0; i < completer.DefaultBufferSize-1; i++ {
		c.Add(comp(int64(i)))
	}
	assert.False(t, c.Full())
	c.Add(comp(1 << 40))
	assert.True(t, c.Full(), "default buffer size is 10,000")
	assert.Equal(t, completer.DefaultBufferSize, c.Len())
}

func TestCompleter_FlushesOnIntervalInOrder(t *testing.T) {
	t.Parallel()
	h := start(t, &fakeWriter{}, nil)
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.c.Add(comp(2))
	assert.Empty(t, h.store.written(), "below batch size nothing is written before the interval")
	h.fc.Advance(49 * time.Millisecond)
	h.waitBlocked(t)
	assert.Empty(t, h.store.written())
	h.fc.Advance(time.Millisecond)
	testutil.Eventually(t, func() bool { return len(h.store.written()) == 2 }, 2*time.Second)
	assert.Equal(t, []int64{1, 2}, h.store.written())
	assert.Equal(t, []string{"client-1"}, h.store.clients)
	assert.Equal(t, 0, h.c.Len())
}

func TestCompleter_FlushesBySizeInBatches(t *testing.T) {
	t.Parallel()
	h := start(t, &fakeWriter{}, nil)
	h.waitBlocked(t)
	for i := int64(1); i <= 7; i++ {
		h.c.Add(comp(i))
	}
	testutil.Eventually(t, func() bool { return len(h.store.written()) == 7 }, 2*time.Second)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7}, h.store.written())
	assert.Equal(t, []int{3, 3, 1}, h.store.sizes(), "batches never exceed BatchSize")
}

func TestCompleter_RetriesWithoutLosingResults(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{fail: 3}
	h := start(t, store, nil)
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.c.Add(comp(2))
	for i := 0; i < 20 && len(store.written()) < 2; i++ {
		h.waitBlocked(t)
		h.fc.Advance(time.Second)
		if i == 1 {
			h.c.Add(comp(3)) // arrives while the database is down
		}
	}
	testutil.Eventually(t, func() bool { return len(store.written()) == 3 }, 2*time.Second)
	assert.Equal(t, []int64{1, 2, 3}, store.written(), "every result written exactly once, in order")
}

func TestCompleter_RetryBackoffGrowsAndIsCapped(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{dead: true}
	h := start(t, store, nil)
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.fc.Advance(50 * time.Millisecond) // first attempt fails
	// With Rand = 0 the delay is half of min(50ms << failures, 1s): 50ms, 100ms, 200ms, 400ms, 500ms, 500ms.
	for _, want := range []time.Duration{50, 100, 200, 400, 500, 500} {
		h.waitBlocked(t)
		h.fc.Advance(want*time.Millisecond - time.Millisecond)
		h.waitBlocked(t)
		assert.Equal(t, 1, h.c.Len())
		h.fc.Advance(time.Millisecond) // attempt fires exactly at the delay
	}
	store.mu.Lock()
	store.dead = false
	store.mu.Unlock()
	h.waitBlocked(t)
	h.fc.Advance(time.Second)
	testutil.Eventually(t, func() bool { return len(store.written()) == 1 }, 2*time.Second)
}

func TestCompleter_FullAndDrained(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{dead: true}
	h := start(t, store, func(c *completer.Config) { c.BatchSize = 10 })
	h.waitBlocked(t)
	for i := int64(1); i <= 4; i++ {
		h.c.Add(comp(i))
	}
	assert.False(t, h.c.Full())
	h.c.Add(comp(5))
	assert.True(t, h.c.Full(), "full at BufferSize")
	h.c.Add(comp(6)) // still accepted: never dropped
	assert.Equal(t, 6, h.c.Len())
	store.mu.Lock()
	store.dead = false
	store.mu.Unlock()
	h.fc.Advance(time.Second)
	testutil.Eventually(t, func() bool { return !h.c.Full() }, 2*time.Second)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, store.written())
	h.mu.Lock()
	assert.Equal(t, 1, h.drained)
	h.mu.Unlock()
}

func TestCompleter_ReportsAppliedAndStale(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{stale: map[int64]bool{2: true}}
	h := start(t, store, nil)
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.c.Add(comp(2))
	h.c.Add(comp(3))
	testutil.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.applied) == 2
	}, 2*time.Second)
	h.mu.Lock()
	defer h.mu.Unlock()
	assert.Equal(t, []int64{1, 3}, []int64{h.applied[0].ID, h.applied[1].ID})
	assert.Equal(t, 1, h.stale)
	assert.Equal(t, 0, h.drained)
}

func TestCompleter_FinalFlushOnShutdown(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{fail: 1}
	h := start(t, store, nil)
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.cancel()
	// The final flush retries after a failure, still driven by the clock.
	var err error
	testutil.Eventually(t, func() bool {
		h.fc.Advance(time.Second)
		select {
		case err = <-h.done:
			return true
		default:
			return false
		}
	}, 2*time.Second)
	require.NoError(t, err)
	h.done <- nil // for cleanup
	assert.Equal(t, []int64{1}, store.written())
}

func TestCompleter_FinalFlushGivesUpAfterTimeout(t *testing.T) {
	t.Parallel()
	store := &fakeWriter{block: make(chan struct{})}
	h := start(t, store, func(c *completer.Config) { c.FlushTimeout = 20 * time.Millisecond })
	h.waitBlocked(t)
	h.c.Add(comp(1))
	h.cancel()
	err := <-h.done
	h.done <- nil
	require.Error(t, err)
	assert.Contains(t, err.Error(), "left unwritten")
	assert.Equal(t, 1, h.c.Len(), "unwritten results are kept, the job will be rescued")
}

func TestCompleter_EmptyShutdownIsClean(t *testing.T) {
	t.Parallel()
	h := start(t, &fakeWriter{}, nil)
	h.waitBlocked(t)
	h.cancel()
	require.NoError(t, <-h.done)
	h.done <- nil
}
