// SPDX-License-Identifier: Apache-2.0

package maintenance

import (
	"context"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

const PeriodicGrace = 15 * time.Second

const maxTicksPerRun = 100

type PeriodicJob struct {
	Name       string
	Schedule   schedule.Schedule
	CatchUp    time.Duration
	RunOnStart bool
	Build      func(tick time.Time) (job.InsertParams, error)
}

// Periodic enqueues each periodic job once per tick.
type Periodic struct {
	Base
	Jobs       []PeriodicJob
	Interval   time.Duration
	OnSkipped  func(name string, n int)
	OnEnqueued func(name string, tick time.Time)
}

func (p *Periodic) Run(ctx context.Context) error {
	if len(p.Jobs) == 0 {
		return nil
	}
	var last map[string]time.Time
	return every(ctx, p.Clock, orDefault(p.Interval, DefaultPeriodicInterval), func(ctx context.Context) {
		if last == nil {
			l, err := p.start(ctx)
			if err != nil {
				p.warn(ctx, "periodic", err)
				return
			}
			last = l
		}
		total := 0
		for _, j := range p.Jobs {
			total += p.enqueueDue(ctx, j, last)
		}
		p.observe("periodic", total)
	})
}

func (p *Periodic) start(ctx context.Context) (map[string]time.Time, error) {
	stored, err := p.Store.LastTicks(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // the store already adds context
	}
	now := p.Clock.Now()
	last := make(map[string]time.Time, len(p.Jobs))
	for _, j := range p.Jobs {
		t, ok := stored[j.Name]
		ranOnStart := j.RunOnStart && p.enqueue(ctx, j, now)
		if !ok {
			if !ranOnStart {
				if err := p.Store.MarkPeriodicStart(ctx, j.Name, now); err != nil {
					return nil, err //nolint:wrapcheck // the store already adds context
				}
			}
			t = now
		}
		last[j.Name] = t
	}
	return last, nil
}

func (p *Periodic) enqueueDue(ctx context.Context, j PeriodicJob, last map[string]time.Time) int {
	due, skipped := schedule.DueTicks(j.Schedule, last[j.Name], p.Clock.Now(), j.CatchUp+PeriodicGrace, maxTicksPerRun)
	if skipped > 0 {
		if p.OnSkipped != nil {
			p.OnSkipped(j.Name, skipped)
		}
		if p.Logger != nil {
			p.Logger.Warn("periodic ticks skipped beyond catch-up window", "operation", j.Name, "count", skipped)
		}
	}
	n := 0
	for _, tick := range due {
		if !p.enqueue(ctx, j, tick) {
			return n
		}
		last[j.Name] = tick
		n++
	}
	if len(due) == 0 && skipped > 0 {
		last[j.Name] = p.Clock.Now().Add(-j.CatchUp - PeriodicGrace)
	}
	return n
}

func (p *Periodic) enqueue(ctx context.Context, j PeriodicJob, tick time.Time) bool {
	params, err := j.Build(tick)
	if err != nil {
		p.warn(ctx, "periodic", err)
		return false
	}
	inserted, err := p.Store.EnqueueTick(ctx, j.Name, tick, params)
	if err != nil {
		p.warn(ctx, "periodic", err)
		return false
	}
	if inserted && p.OnEnqueued != nil {
		p.OnEnqueued(j.Name, tick)
	}
	return true
}
