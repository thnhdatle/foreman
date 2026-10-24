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

const (
	DefaultBatchSize     = 100
	DefaultFlushInterval = 50 * time.Millisecond
	DefaultBufferSize    = 10_000
	DefaultMaxRetryDelay = 5 * time.Second
	DefaultFlushTimeout  = 10 * time.Second
	DefaultCallTimeout   = 30 * time.Second
)

type Config struct {
	ClientID      string
	Store         job.CompletionWriter
	Clock         clock.Clock
	Logger        *slog.Logger
	BatchSize     int
	FlushInterval time.Duration
	BufferSize    int
	MaxRetryDelay time.Duration
	FlushTimeout  time.Duration
	CallTimeout   time.Duration
	OnApplied     func(applied []job.Completion, stale int)
	OnDrained     func()
	Rand          func() float64
}

// Completer batches job completions into single statements and retries them on database errors.
type Completer struct {
	cfg  Config
	wake chan struct{}

	mu      sync.Mutex
	pending []job.Completion
	full    bool
}

// New validates cfg and applies defaults.
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

// Add queues one completion for the next batch.
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

func (c *Completer) Full() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.full
}

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

// Run writes batches until ctx is cancelled, then flushes what is left.
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
				continue
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
	return d/2 + time.Duration(c.cfg.Rand()*float64(d/2))
}

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
