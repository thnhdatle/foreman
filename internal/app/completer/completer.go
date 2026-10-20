// SPDX-License-Identifier: Apache-2.0

package completer

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// Defaults.
const (
	DefaultBatchSize     = 100
	DefaultFlushInterval = 50 * time.Millisecond
	DefaultBufferSize    = 10_000
	DefaultMaxRetryDelay = 5 * time.Second
	DefaultFlushTimeout  = 10 * time.Second
	DefaultCallTimeout   = 30 * time.Second
)

// Config configures a Completer.
type Config struct {
	ClientID string
	Store    job.CompletionWriter
	Clock    clock.Clock
	Logger   *slog.Logger
	// BatchSize is the maximum number of results per statement.
	BatchSize int
	// FlushInterval is the longest a result waits before being written.
	FlushInterval time.Duration
	// BufferSize is the number of pending results at which Full reports true.
	BufferSize int
	// MaxRetryDelay caps the backoff between failed writes.
	MaxRetryDelay time.Duration
	// FlushTimeout bounds the final flush on shutdown.
	FlushTimeout time.Duration
	// CallTimeout bounds one write.
	CallTimeout time.Duration
	// OnApplied receives the completions that passed the guard and the number rejected as stale.
	OnApplied func(applied []job.Completion, stale int)
	// OnDrained is called when the buffer stops being full.
	OnDrained func()
	// Rand returns a float in [0, 1) for retry jitter; nil uses math/rand/v2.
	Rand func() float64
}

// Completer batches job state updates (by size or interval) into single
// statements. Results are never dropped: on a database error they stay in
// memory and the write is retried with jittered exponential backoff.
// The buffer is unbounded in memory but producers stop fetching
// while Full reports true, which bounds it to BufferSize plus running jobs.
type Completer struct {
	cfg  Config
	wake chan struct{}

	mu      sync.Mutex // protects pending and full
	pending []job.Completion
	full    bool
}

// New returns a Completer; ClientID, Store and Clock are required.
func New(cfg Config) (*Completer, error) {
	if cfg.ClientID == "" || cfg.Store == nil || cfg.Clock == nil {
		return nil, errors.New("completer: ClientID, Store and Clock are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	setDefault(&cfg.BatchSize, DefaultBatchSize)
	setDefault(&cfg.FlushInterval, DefaultFlushInterval)
	setDefault(&cfg.BufferSize, DefaultBufferSize)
	setDefault(&cfg.MaxRetryDelay, DefaultMaxRetryDelay)
	setDefault(&cfg.FlushTimeout, DefaultFlushTimeout)
	setDefault(&cfg.CallTimeout, DefaultCallTimeout)
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	return &Completer{cfg: cfg, wake: make(chan struct{}, 1)}, nil
}

func setDefault[T int | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Add queues one result. It never blocks and never drops.
func (c *Completer) Add(comp job.Completion) {
	c.mu.Lock()
	c.pending = append(c.pending, comp)
	n := len(c.pending)
	if n >= c.cfg.BufferSize {
		c.full = true
	}
	c.mu.Unlock()
	if n >= c.cfg.BatchSize {
		c.signal()
	}
}

// Full reports whether producers must stop fetching until the buffer drains.
func (c *Completer) Full() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.full
}

// Len returns the number of results waiting to be written.
func (c *Completer) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

func (c *Completer) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Run writes results until ctx is cancelled, then performs a final flush
// bounded by FlushTimeout. It returns an error only if results remain
// unwritten after the final flush; those jobs stay running and are rescued.
func (c *Completer) Run(ctx context.Context) error {
	failures := 0
	for {
		wait := c.cfg.FlushInterval
		if failures > 0 {
			wait = c.retryDelay(failures)
		}
		select {
		case <-ctx.Done():
			return c.finalFlush(ctx)
		case <-c.wake:
			if failures > 0 {
				continue // keep backing off; new results join the next attempt
			}
		case <-c.cfg.Clock.After(wait):
		}
		if err := c.flush(ctx); err != nil {
			failures++
			c.cfg.Logger.Warn("completer: write failed, retrying", "error", err.Error(), "attempt", failures,
				"count", c.Len())
			continue
		}
		failures = 0
	}
}

func (c *Completer) retryDelay(failures int) time.Duration {
	d := c.cfg.FlushInterval << min(failures, 20)
	if d <= 0 || d > c.cfg.MaxRetryDelay {
		d = c.cfg.MaxRetryDelay
	}
	// Equal jitter keeps a floor so a dead database is not hammered.
	return d/2 + time.Duration(c.cfg.Rand()*float64(d/2))
}

// flush writes batches until the buffer is empty or a write fails.
func (c *Completer) flush(ctx context.Context) error {
	for {
		c.mu.Lock()
		n := min(len(c.pending), c.cfg.BatchSize)
		batch := slices.Clone(c.pending[:n])
		c.mu.Unlock()
		if n == 0 {
			return nil
		}
		callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
		applied, err := c.cfg.Store.Complete(callCtx, c.cfg.ClientID, batch)
		cancel()
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.pending = c.pending[n:]
		drained := c.full && len(c.pending) < c.cfg.BufferSize
		if drained {
			c.full = false
		}
		c.mu.Unlock()
		c.report(batch, applied)
		if drained && c.cfg.OnDrained != nil {
			c.cfg.OnDrained()
		}
	}
}

func (c *Completer) report(batch []job.Completion, applied []int64) {
	if c.cfg.OnApplied == nil {
		return
	}
	ok := make(map[int64]struct{}, len(applied))
	for _, id := range applied {
		ok[id] = struct{}{}
	}
	done := make([]job.Completion, 0, len(applied))
	for _, comp := range batch {
		if _, found := ok[comp.ID]; found {
			done = append(done, comp)
		}
	}
	c.cfg.OnApplied(done, len(batch)-len(done))
}

func (c *Completer) finalFlush(ctx context.Context) error {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.FlushTimeout)
	defer cancel()
	failures := 0
	for {
		err := c.flush(flushCtx)
		if err == nil {
			return nil
		}
		failures++
		select {
		case <-flushCtx.Done():
			return errors.Join(errors.New("completer: results left unwritten at shutdown"), err)
		case <-c.cfg.Clock.After(c.retryDelay(failures)):
		}
	}
}
