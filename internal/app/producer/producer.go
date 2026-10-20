// SPDX-License-Identifier: Apache-2.0

package producer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/fairness"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// Defaults.
const (
	DefaultMaxWorkers    = 10
	DefaultFetchInterval = time.Second
	DefaultFetchCooldown = 5 * time.Millisecond
	DefaultMaxKeys       = 1000
	// DefaultCallTimeout bounds each store call, so a connection that died
	// silently stalls the queue for at most this long.
	DefaultCallTimeout = 15 * time.Second
)

// ErrStopping is the cancellation cause of job contexts on StopAndCancel.
var ErrStopping = job.ErrClientStopping

// Config configures a Producer.
type Config struct {
	Queue    string
	ClientID string
	// MaxWorkers is the number of jobs this producer runs at once.
	MaxWorkers int
	// FetchInterval is the polling fallback and the paused-state refresh period.
	FetchInterval time.Duration
	// FetchCooldown is the minimum time between two fetches; it coalesces wake-ups.
	FetchCooldown time.Duration
	// PerKeyLimit caps running jobs per concurrency key across the cluster (0 = none).
	PerKeyLimit int
	// Fairness alternates between keys instead of strict fetch order.
	Fairness bool
	// MaxKeys bounds the keys considered per keyed fetch.
	MaxKeys int
	// CallTimeout bounds each store call (default 15s).
	CallTimeout time.Duration
	Store       job.Fetcher
	Clock       clock.Clock
	Logger      *slog.Logger
	// Execute runs one fetched job in its own goroutine; ctx is the job context.
	Execute func(ctx context.Context, row *job.Row)
	// CanFetch returns false while results cannot be accepted (completer full).
	CanFetch func() bool
	// OnFetched observes each successful fetch.
	OnFetched func(rows []*job.Row, d time.Duration)
}

type runningJob struct {
	attempt int
	cancel  context.CancelCauseFunc
}

// Producer fetches jobs of one queue and runs them on up to MaxWorkers
// goroutines. Wake-ups come from NOTIFY (Wake), from finished jobs
// and from the FetchInterval poll.
type Producer struct {
	cfg     Config
	wake    chan struct{}
	paused  atomic.Bool
	tracker *fairness.Tracker // used only by the Run goroutine
	jobs    sync.WaitGroup

	mu      sync.Mutex // protects running
	running map[int64]*runningJob
}

// New validates cfg and applies defaults.
func New(cfg Config) (*Producer, error) {
	if cfg.Queue == "" || cfg.ClientID == "" || cfg.Store == nil || cfg.Clock == nil || cfg.Execute == nil {
		return nil, errors.New("producer: Queue, ClientID, Store, Clock and Execute are required")
	}
	if cfg.PerKeyLimit < 0 {
		return nil, errors.New("producer: PerKeyLimit must not be negative")
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = DefaultMaxWorkers
	}
	if cfg.FetchInterval <= 0 {
		cfg.FetchInterval = DefaultFetchInterval
	}
	if cfg.FetchCooldown <= 0 {
		cfg.FetchCooldown = DefaultFetchCooldown
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.CanFetch == nil {
		cfg.CanFetch = func() bool { return true }
	}
	return &Producer{
		cfg: cfg, wake: make(chan struct{}, 1), tracker: fairness.NewTracker(), running: map[int64]*runningJob{},
	}, nil
}

// Queue returns the queue name.
func (p *Producer) Queue() string { return p.cfg.Queue }

// Wake asks for a fetch soon; it never blocks.
func (p *Producer) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// SetPaused records the paused state received by NOTIFY.
func (p *Producer) SetPaused(paused bool) {
	p.paused.Store(paused)
	if !paused {
		p.Wake()
	}
}

// Paused reports the last known paused state.
func (p *Producer) Paused() bool { return p.paused.Load() }

// Run fetches until ctx is cancelled. Jobs already started keep running;
// use Wait and CancelAll to drain them.
func (p *Producer) Run(ctx context.Context) error {
	ticker := p.cfg.Clock.NewTicker(p.cfg.FetchInterval)
	defer ticker.Stop()
	p.refreshPaused(ctx)
	var last time.Time
	failing := false
	for {
		if !p.waitSignal(ctx, ticker, failing) {
			return nil
		}
		if p.paused.Load() || !p.cfg.CanFetch() || p.free() == 0 {
			continue
		}
		if !p.cooldown(ctx, last) {
			return nil
		}
		last = p.cfg.Clock.Now()
		n, err := p.fetch(ctx)
		failing = err != nil
		if err != nil {
			if ctx.Err() == nil {
				p.cfg.Logger.Warn("fetch failed", "queue", p.cfg.Queue, "error", err.Error())
			}
			continue
		}
		if n > 0 && p.free() > 0 {
			p.Wake() // more jobs may be waiting
		}
	}
}

// waitSignal waits for a wake-up or the poll tick; it returns false when ctx
// is done. After a fetch error only the poll retries, so wake-ups cannot
// hammer a failing database.
func (p *Producer) waitSignal(ctx context.Context, ticker clock.Ticker, failing bool) bool {
	wake := p.wake
	if failing {
		wake = nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-wake:
	case <-ticker.C():
		p.refreshPaused(ctx)
	}
	return true
}

// cooldown enforces FetchCooldown since the previous fetch.
func (p *Producer) cooldown(ctx context.Context, last time.Time) bool {
	wait := p.cfg.FetchCooldown - p.cfg.Clock.Now().Sub(last)
	if wait <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-p.cfg.Clock.After(wait):
		return true
	}
}

func (p *Producer) refreshPaused(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, p.cfg.CallTimeout)
	defer cancel()
	paused, err := p.cfg.Store.QueueHeartbeat(callCtx, p.cfg.Queue, p.cfg.Clock.Now())
	if err != nil {
		if ctx.Err() == nil {
			p.cfg.Logger.Warn("queue state refresh failed", "queue", p.cfg.Queue, "error", err.Error())
		}
		return
	}
	p.paused.Store(paused)
}

func (p *Producer) free() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.MaxWorkers - len(p.running)
}

func (p *Producer) fetch(ctx context.Context) (int, error) {
	// Jobs started below keep ctx (without this timeout) as their parent.
	callCtx, cancel := context.WithTimeout(ctx, p.cfg.CallTimeout)
	defer cancel()
	free := p.free()
	start := p.cfg.Clock.Now()
	params := job.FetchParams{Queue: p.cfg.Queue, ClientID: p.cfg.ClientID, Now: start, Limit: free}
	if p.cfg.Fairness || p.cfg.PerKeyLimit > 0 {
		quotas, err := p.allocate(callCtx, free)
		if err != nil || len(quotas) == 0 {
			return 0, err
		}
		params.Keyed, params.Quotas, params.PerKeyLimit = true, quotas, p.cfg.PerKeyLimit
	}
	rows, err := p.cfg.Store.Fetch(callCtx, params)
	if err != nil {
		return 0, err //nolint:wrapcheck // the store already adds context
	}
	if params.Keyed {
		for _, r := range rows {
			p.tracker.Served(r.ConcurrencyKey)
		}
	}
	if p.cfg.OnFetched != nil {
		p.cfg.OnFetched(rows, p.cfg.Clock.Now().Sub(start))
	}
	for _, r := range rows {
		p.start(ctx, r)
	}
	return len(rows), nil
}

// allocate splits free capacity between keys with available jobs.
func (p *Producer) allocate(ctx context.Context, free int) ([]job.KeyQuota, error) {
	stats, err := p.cfg.Store.KeyStats(ctx, p.cfg.Queue, p.cfg.MaxKeys, p.cfg.PerKeyLimit > 0)
	if err != nil {
		return nil, err //nolint:wrapcheck // the store already adds context
	}
	cands := make([]fairness.Candidate, len(stats))
	keep := make(map[string]struct{}, len(stats))
	for i, s := range stats {
		headroom := fairness.Unlimited
		if p.cfg.PerKeyLimit > 0 && s.Key != "" {
			headroom = max(0, p.cfg.PerKeyLimit-s.Running)
		}
		cands[i] = fairness.Candidate{
			Key: s.Key, Priority: s.Priority, ScheduledAt: s.ScheduledAt, ID: s.ID, Headroom: headroom,
		}
		keep[s.Key] = struct{}{}
	}
	if p.tracker.Len() > 4*len(stats)+p.cfg.MaxKeys {
		p.tracker.Retain(keep)
	}
	allocs := fairness.Allocate(free, cands, p.cfg.Fairness, p.tracker)
	quotas := make([]job.KeyQuota, len(allocs))
	for i, a := range allocs {
		quotas[i] = job.KeyQuota{Key: a.Key, Quota: a.Quota}
	}
	return quotas, nil
}

// start runs a fetched job. Job contexts do not derive their cancellation
// from the fetch loop: a graceful stop lets running jobs finish.
func (p *Producer) start(ctx context.Context, row *job.Row) {
	jobCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	p.mu.Lock()
	p.running[row.ID] = &runningJob{attempt: row.Attempt, cancel: cancel}
	p.mu.Unlock()
	p.jobs.Add(1)
	go func() {
		defer p.jobs.Done()
		defer cancel(nil)
		p.cfg.Execute(jobCtx, row)
		p.mu.Lock()
		delete(p.running, row.ID)
		p.mu.Unlock()
		p.Wake()
	}()
}

// Cancel cancels the context of job id with cause job.ErrCancelRequested if
// this producer runs it.
func (p *Producer) Cancel(id int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.running[id]
	if ok {
		r.cancel(job.ErrCancelRequested)
	}
	return ok
}

// CancelAll cancels every running job context with cause ErrStopping.
func (p *Producer) CancelAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.running {
		r.cancel(ErrStopping)
	}
}

// Wait blocks until every started job has returned or ctx is done.
func (p *Producer) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.jobs.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Running lists the attempts in progress.
func (p *Producer) Running() []job.Ref {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]job.Ref, 0, len(p.running))
	for id, r := range p.running {
		out = append(out, job.Ref{ID: id, Attempt: r.attempt})
	}
	return out
}

// RunningCount returns the number of jobs in progress.
func (p *Producer) RunningCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.running)
}
