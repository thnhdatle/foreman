// SPDX-License-Identifier: Apache-2.0

package maintenance

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

const (
	DefaultSchedulerInterval = time.Second
	DefaultCleanerInterval   = 30 * time.Second
	DefaultDepthInterval     = 15 * time.Second
	DefaultPeriodicInterval  = time.Second
	DefaultBatch             = 1000
	maxBatchesPerRun         = 100
	RunTimeout               = 2 * time.Minute
	ReindexTimeout           = time.Hour
)

// Service is a long-running maintenance loop.
type Service interface {
	Run(ctx context.Context) error
}

type Base struct {
	Store   job.MaintenanceStore
	Clock   clock.Clock
	Logger  *slog.Logger
	Observe func(service string, rows int, at time.Time)
}

func (b Base) observe(service string, rows int) {
	if b.Observe != nil {
		b.Observe(service, rows, b.Clock.Now())
	}
}

func (b Base) warn(ctx context.Context, service string, err error) {
	if ctx.Err() != nil || b.Logger == nil {
		return
	}
	b.Logger.Warn("maintenance run failed", "component", service, "error", err.Error())
}

func every(ctx context.Context, clk clock.Clock, interval time.Duration, fn func(context.Context)) error {
	for {
		runCtx, cancel := context.WithTimeout(ctx, RunTimeout)
		fn(runCtx)
		cancel()
		select {
		case <-ctx.Done():
			return nil
		case <-clk.After(interval):
		}
	}
}

// RunAll runs services concurrently and returns when all have stopped.
func RunAll(ctx context.Context, services ...Service) {
	var wg sync.WaitGroup
	for _, s := range services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Run(ctx)
		}()
	}
	wg.Wait()
}

// Scheduler moves scheduled and retryable jobs to available when they are due.
type Scheduler struct {
	Base
	Interval time.Duration
	Batch    int
}

func (s *Scheduler) Run(ctx context.Context) error {
	interval, batch := orDefault(s.Interval, DefaultSchedulerInterval), orDefault(s.Batch, DefaultBatch)
	return every(ctx, s.Clock, interval, func(ctx context.Context) {
		total := 0
		for i := 0; i < maxBatchesPerRun; i++ {
			n, err := s.Store.PromoteDue(ctx, s.Clock.Now(), batch)
			total += n
			if err != nil {
				s.warn(ctx, "scheduler", err)
				break
			}
			if n < batch {
				break
			}
		}
		s.observe("scheduler", total)
	})
}

// Rescuer returns running jobs without a heartbeat to retryable, or discards them when attempts run out.
type Rescuer struct {
	Base
	Interval   time.Duration
	StuckAfter time.Duration
	Retry      func(row *job.Row, now time.Time) time.Time
	Batch      int
	OnRescued  func(n int)
}

func (r *Rescuer) Run(ctx context.Context) error {
	batch := orDefault(r.Batch, DefaultBatch)
	return every(ctx, r.Clock, r.Interval, func(ctx context.Context) {
		total := 0
		for i := 0; i < maxBatchesPerRun; i++ {
			n, more, err := r.rescueBatch(ctx, batch)
			total += n
			if err != nil {
				r.warn(ctx, "rescuer", err)
				break
			}
			if !more {
				break
			}
		}
		if total > 0 && r.OnRescued != nil {
			r.OnRescued(total)
		}
		r.observe("rescuer", total)
	})
}

func (r *Rescuer) rescueBatch(ctx context.Context, batch int) (n int, more bool, err error) {
	now := r.Clock.Now()
	cutoff := now.Add(-r.StuckAfter)
	rows, err := r.Store.StuckJobs(ctx, cutoff, batch)
	if err != nil || len(rows) == 0 {
		return 0, false, err
	}
	cs := make([]job.Completion, 0, len(rows))
	for _, row := range rows {
		cs = append(cs, RescueCompletion(row, now, r.Retry))
	}
	n, err = r.Store.Rescue(ctx, cutoff, cs)
	return n, len(rows) == batch, err
}

func RescueCompletion(row *job.Row, now time.Time, retry func(*job.Row, time.Time) time.Time) job.Completion {
	since := "never"
	if row.HeartbeatAt != nil {
		since = row.HeartbeatAt.UTC().Format(time.RFC3339Nano)
	}
	c := job.Completion{
		ID: row.ID, Attempt: row.Attempt, At: now, Kind: row.Kind, Queue: row.Queue,
		Error: &job.AttemptError{At: now, Attempt: row.Attempt, Error: "rescued: no heartbeat since " + since},
	}
	if row.Attempt >= row.MaxAttempts {
		c.State = job.StateDiscarded
		c.FinalizedAt = now
		return c
	}
	c.State = job.StateRetryable
	c.ScheduledAt = retry(row, now)
	return c
}

// Cleaner deletes finalized jobs older than their retention.
type Cleaner struct {
	Base
	Interval      time.Duration
	Retention     map[job.State]time.Duration
	TickRetention time.Duration
	Batch         int
}

const DefaultTickRetention = 7 * 24 * time.Hour

func (c *Cleaner) Run(ctx context.Context) error {
	interval, batch := orDefault(c.Interval, DefaultCleanerInterval), orDefault(c.Batch, DefaultBatch)
	return every(ctx, c.Clock, interval, func(ctx context.Context) {
		total := 0
		for _, st := range []job.State{job.StateCompleted, job.StateCancelled, job.StateDiscarded} {
			retention := c.Retention[st]
			if retention <= 0 {
				continue
			}
			for i := 0; i < maxBatchesPerRun; i++ {
				n, err := c.Store.DeleteFinalized(ctx, st, c.Clock.Now().Add(-retention), batch)
				total += n
				if err != nil {
					c.warn(ctx, "cleaner", err)
					break
				}
				if n < batch {
					break
				}
			}
		}
		n, err := c.Store.PruneTicks(ctx, c.Clock.Now().Add(-orDefault(c.TickRetention, DefaultTickRetention)))
		if err != nil {
			c.warn(ctx, "cleaner", err)
		}
		c.observe("cleaner", total+n)
	})
}

// Reindexer rebuilds the queue indexes every Interval.
type Reindexer struct {
	Base
	Interval time.Duration
}

func (r *Reindexer) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.Clock.After(r.Interval):
		}
		runCtx, cancel := context.WithTimeout(ctx, ReindexTimeout)
		ran, err := r.Store.Reindex(runCtx)
		cancel()
		if err != nil {
			r.warn(ctx, "reindexer", err)
			continue
		}
		rows := 0
		if ran {
			rows = 1
		}
		r.observe("reindexer", rows)
	}
}

// DepthSampler samples the queue depth per state for metrics.
type DepthSampler struct {
	Base
	Interval time.Duration
	OnDepths func([]job.QueueDepth)
}

func (d *DepthSampler) Run(ctx context.Context) error {
	return every(ctx, d.Clock, orDefault(d.Interval, DefaultDepthInterval), func(ctx context.Context) {
		depths, err := d.Store.QueueDepths(ctx)
		if err != nil {
			d.warn(ctx, "depth_sampler", err)
			return
		}
		if d.OnDepths != nil {
			d.OnDepths(depths)
		}
		d.observe("depth_sampler", len(depths))
	})
}

// CancelPoller finds cancel requests for this client's running jobs by polling.
type CancelPoller struct {
	Store    job.CancelPoller
	Clock    clock.Clock
	Logger   *slog.Logger
	Interval time.Duration
	Running  func() []int64
	OnCancel func(id int64)
}

func (p *CancelPoller) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.Clock.After(p.Interval):
		}
		pollCtx, cancel := context.WithTimeout(ctx, max(p.Interval, time.Second)*5)
		p.Poll(pollCtx)
		cancel()
	}
}

func (p *CancelPoller) Poll(ctx context.Context) {
	ids := p.Running()
	if len(ids) == 0 {
		return
	}
	cancelled, err := p.Store.CancelRequested(ctx, ids)
	if err != nil {
		if ctx.Err() == nil && p.Logger != nil {
			p.Logger.Warn("cancel poll failed", "error", err.Error())
		}
		return
	}
	for _, id := range cancelled {
		p.OnCancel(id)
	}
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}
