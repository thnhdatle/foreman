// SPDX-License-Identifier: Apache-2.0

package maintenance

import (
	"context"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

// PeriodicGrace is added to every catch-up window: a tick enqueued this late
// during normal operation is not considered missed.
const PeriodicGrace = 15 * time.Second

// maxTicksPerRun bounds the ticks one periodic job enqueues per run.
const maxTicksPerRun = 100

// PeriodicJob is one registered periodic job.
type PeriodicJob struct {
	Name       string
	Schedule   schedule.Schedule
	CatchUp    time.Duration
	RunOnStart bool
	// Build returns the job to insert for tick.
	Build func(tick time.Time) (job.InsertParams, error)
}

// Periodic enqueues periodic jobs exactly once per tick: each tick
// is recorded in foreman_periodic_ticks under a unique constraint in the same
// transaction as the job, so overlapping leaders cannot enqueue it twice.
// After a leader gap, missed ticks within CatchUp are enqueued oldest first
// and older ones are skipped and counted.
type Periodic struct {
	Base
	Jobs     []PeriodicJob
	Interval time.Duration
	// OnSkipped receives the number of ticks skipped beyond the catch-up window.
	OnSkipped func(name string, n int)
	// OnEnqueued is called for each tick this leader inserted.
	OnEnqueued func(name string, tick time.Time)
}

// Run enqueues due ticks every Interval (default 1s) while leader.
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

// start loads the last enqueued tick of every job. A job with no history
// starts from now (no catch-up of ticks before it was ever scheduled); a
// start marker is recorded so that a later leader catches up from here even
// if this one dies before the first tick.
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
			// The run-on-start tick is history enough; otherwise leave a marker.
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
			return n // retried next run; last is not advanced past a failed tick
		}
		last[j.Name] = tick
		n++
	}
	if len(due) == 0 && skipped > 0 {
		// Every missed tick was skipped: move past them.
		last[j.Name] = p.Clock.Now().Add(-j.CatchUp - PeriodicGrace)
	}
	return n
}

// enqueue inserts one tick; it returns false on error.
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
