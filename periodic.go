// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"errors"
	"fmt"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

// Schedule computes the ticks of a periodic job. Ticks must be deterministic:
// every client computes the same instants, which is what lets the tick table
// enforce exactly-once enqueueing across leaders.
type Schedule interface {
	// Next returns the first tick strictly after t.
	Next(t time.Time) time.Time
}

type cronSchedule struct {
	expr string
	s    schedule.Schedule
}

func (c *cronSchedule) Next(t time.Time) time.Time { return c.s.Next(t) }

// Cron parses a cron expression: 5 fields, 6 fields with leading seconds,
// or a descriptor such as @hourly or @daily. It is evaluated in the
// PeriodicJob's TimeZone (UTC by default). Wall times skipped by a DST
// spring-forward do not fire that day; wall times repeated by a fall-back
// fire once per occurrence. Use UTC to avoid both.
func Cron(expr string) (Schedule, error) {
	s, err := schedule.ParseCron(expr, time.UTC)
	if err != nil {
		return nil, fmt.Errorf("foreman: %w", err)
	}
	return &cronSchedule{expr: expr, s: s}, nil
}

// MustCron is Cron that panics on an invalid expression, for package-level
// declarations of periodic jobs.
func MustCron(expr string) Schedule {
	s, err := Cron(expr)
	if err != nil {
		panic(err)
	}
	return s
}

type everySchedule struct{ d time.Duration }

func (e everySchedule) Next(t time.Time) time.Time {
	s, err := schedule.Every(e.d)
	if err != nil {
		return time.Time{}
	}
	return s.Next(t)
}

// Every returns a schedule ticking every d, aligned to multiples of d since
// the Unix epoch so that all clients agree on the ticks. d must be positive
// (checked by NewClient).
func Every(d time.Duration) Schedule { return everySchedule{d: d} }

// PeriodicJob is a job enqueued on a schedule by the leader, exactly once
// per tick across the cluster.
type PeriodicJob struct {
	// Name identifies the job in the tick table; it must be unique and stable.
	Name string
	// Schedule is Cron(...) or Every(...).
	Schedule Schedule
	// Args returns the job to insert for each tick.
	Args func() (JobArgs, *InsertOpts)
	// RunOnStart also enqueues the job when a client becomes leader.
	RunOnStart bool
	// CatchUp is how far back missed ticks are enqueued after a leader gap
	// (oldest first); older ticks are skipped and counted.
	CatchUp time.Duration
	// TimeZone evaluates cron expressions; default UTC.
	TimeZone *time.Location
}

// resolvedSchedule applies the job's time zone to cron schedules.
func (p *PeriodicJob) resolvedSchedule() (schedule.Schedule, error) {
	switch s := p.Schedule.(type) {
	case *cronSchedule:
		loc := p.TimeZone
		if loc == nil {
			loc = time.UTC
		}
		return schedule.ParseCron(s.expr, loc) //nolint:wrapcheck // already prefixed by the schedule package
	case everySchedule:
		return schedule.Every(s.d) //nolint:wrapcheck // already prefixed by the schedule package
	default:
		return p.Schedule, nil
	}
}

func validatePeriodic(jobs []*PeriodicJob) []error {
	var errs []error
	names := map[string]bool{}
	for i, p := range jobs {
		switch {
		case p == nil:
			errs = append(errs, fmt.Errorf("PeriodicJobs[%d] is nil", i))
			continue
		case p.Name == "":
			errs = append(errs, fmt.Errorf("PeriodicJobs[%d] has no Name", i))
		case names[p.Name]:
			errs = append(errs, fmt.Errorf("periodic job name %q is used twice", p.Name))
		}
		names[p.Name] = true
		if p.Args == nil {
			errs = append(errs, fmt.Errorf("periodic job %q has no Args func", p.Name))
		}
		if p.CatchUp < 0 {
			errs = append(errs, fmt.Errorf("periodic job %q: CatchUp must not be negative", p.Name))
		}
		if p.Schedule == nil {
			errs = append(errs, fmt.Errorf("periodic job %q has no Schedule", p.Name))
			continue
		}
		if _, err := p.resolvedSchedule(); err != nil {
			errs = append(errs, fmt.Errorf("periodic job %q: %w", p.Name, err))
		}
	}
	return errs
}

var errNilPeriodicArgs = errors.New("foreman: periodic Args returned nil args")
