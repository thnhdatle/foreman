// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"errors"
	"fmt"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

// Schedule computes the ticks of a periodic job.
type Schedule interface {
	Next(t time.Time) time.Time
}

type cronSchedule struct {
	expr string
	s    schedule.Schedule
}

func (c *cronSchedule) Next(t time.Time) time.Time { return c.s.Next(t) }

// Cron parses a cron expression.
func Cron(expr string) (Schedule, error) {
	s, err := schedule.ParseCron(expr, time.UTC)
	if err != nil {
		return nil, fmt.Errorf("foreman: %w", err)
	}
	return &cronSchedule{expr: expr, s: s}, nil
}

// MustCron is Cron that panics on an invalid expression.
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

// Every returns a schedule ticking every d.
func Every(d time.Duration) Schedule { return everySchedule{d: d} }

// PeriodicJob is a job the leader enqueues on a schedule.
type PeriodicJob struct {
	Name       string
	Schedule   Schedule
	Args       func() (JobArgs, *InsertOpts)
	RunOnStart bool
	// CatchUp is how far back missed ticks are enqueued.
	CatchUp time.Duration
	// TimeZone evaluates cron expressions. Default UTC.
	TimeZone *time.Location
}

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
