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

const (
	DefaultMaxWorkers    = 10
	DefaultFetchInterval = time.Second
	DefaultFetchCooldown = 5 * time.Millisecond
	DefaultMaxKeys       = 1000
	DefaultCallTimeout   = 15 * time.Second
)

var ErrStopping = job.ErrClientStopping

type Config struct {
	Queue         string
	ClientID      string
	MaxWorkers    int
	FetchInterval time.Duration
	FetchCooldown time.Duration
	PerKeyLimit   int
	Fairness      bool
	MaxKeys       int
	CallTimeout   time.Duration
	Store         job.Fetcher
	Clock         clock.Clock
	Logger        *slog.Logger
	Execute       func(ctx context.Context, row *job.Row)
	CanFetch      func() bool
	OnFetched     func(rows []*job.Row, d time.Duration)
}

type runningJob struct {
	attempt int
	cancel  context.CancelCauseFunc
}

// Producer fetches jobs of one queue and runs them on up to MaxWorkers goroutines.
type Producer struct {
	cfg     Config
	wake    chan struct{}
	paused  atomic.Bool
	tracker *fairness.Tracker
	jobs    sync.WaitGroup

	mu      sync.Mutex
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

func (p *Producer) Queue() string { return p.cfg.Queue }

func (p *Producer) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Producer) SetPaused(paused bool) {
	p.paused.Store(paused)
	if !paused {
		p.Wake()
	}
}

func (p *Producer) Paused() bool { return p.paused.Load() }

// Run fetches and starts jobs until ctx is cancelled.
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
			p.Wake()
		}
	}
}

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

func (p *Producer) Cancel(id int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.running[id]
	if ok {
		r.cancel(job.ErrCancelRequested)
	}
	return ok
}

// CancelAll cancels the context of every running job.
func (p *Producer) CancelAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.running {
		r.cancel(ErrStopping)
	}
}

// Wait blocks until all running jobs have finished or ctx is done.
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

func (p *Producer) Running() []job.Ref {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]job.Ref, 0, len(p.running))
	for id, r := range p.running {
		out = append(out, job.Ref{ID: id, Attempt: r.attempt})
	}
	return out
}

func (p *Producer) RunningCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.running)
}
