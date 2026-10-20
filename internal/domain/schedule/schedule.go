// SPDX-License-Identifier: Apache-2.0

package schedule

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule computes tick times. Ticks must be deterministic: every leader
// computes the same instants, which is what makes the (name, tick) unique
// constraint enforce exactly-once enqueueing.
type Schedule interface {
	// Next returns the first tick strictly after t.
	Next(t time.Time) time.Time
}

// ErrInvalid is wrapped by schedule parsing errors.
var ErrInvalid = errors.New("schedule: invalid")

type cronSchedule struct {
	s   cron.Schedule
	loc *time.Location
}

// ParseCron parses a standard 5-field cron expression, a 6-field expression
// with leading seconds, or a descriptor such as @hourly, evaluated in loc
// (UTC when nil). @every is rejected because its ticks are relative to the
// evaluating process; use Every instead.
//
// DST follows robfig/cron: a wall time skipped by a spring-forward transition
// does not fire that day, and a wall time repeated by a fall-back transition
// fires once per occurrence.
func ParseCron(expr string, loc *time.Location) (Schedule, error) {
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s, err := p.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("%w: cron %q: %w", ErrInvalid, expr, err)
	}
	if _, ok := s.(cron.ConstantDelaySchedule); ok {
		return nil, fmt.Errorf("%w: cron %q: @every is not deterministic across clients, use Every", ErrInvalid, expr)
	}
	if loc == nil {
		loc = time.UTC
	}
	return cronSchedule{s: s, loc: loc}, nil
}

// Next returns the next tick after t, in UTC. A zero result means the
// expression never fires again.
func (c cronSchedule) Next(t time.Time) time.Time {
	n := c.s.Next(t.In(c.loc))
	if n.IsZero() {
		return n
	}
	return n.UTC()
}

type every struct{ d time.Duration }

// Every returns a schedule ticking every d, aligned to multiples of d since
// the Unix epoch so that every client computes the same ticks. d must be
// positive.
func Every(d time.Duration) (Schedule, error) {
	if d <= 0 {
		return nil, fmt.Errorf("%w: every %s: interval must be positive", ErrInvalid, d)
	}
	return every{d: d}, nil
}

func (e every) Next(t time.Time) time.Time {
	n := t.UnixNano()
	d := int64(e.d)
	q := n / d
	if n%d != 0 && n < 0 {
		q--
	}
	return time.Unix(0, (q+1)*d).UTC()
}

// maxSkipCount bounds how many skipped ticks DueTicks counts one by one.
const maxSkipCount = 100_000

// DueTicks returns the ticks t with last < t <= now that are inside the
// catch-up window (t >= now - window), oldest first, and the number of
// earlier ticks that were skipped. At most limit ticks are returned; the
// skipped count saturates at 100,000.
func DueTicks(s Schedule, last, now time.Time, window time.Duration, limit int) (due []time.Time, skipped int) {
	cutoff := now.Add(-window)
	t := s.Next(last)
	for !t.IsZero() && t.Before(cutoff) {
		skipped++
		if skipped >= maxSkipCount {
			// Jump to the window; anything before it is skipped anyway.
			t = s.Next(cutoff.Add(-time.Nanosecond))
			break
		}
		t = advance(s, t)
	}
	for !t.IsZero() && !t.After(now) && len(due) < limit {
		due = append(due, t)
		t = advance(s, t)
	}
	return due, skipped
}

// advance returns the tick after t, or zero if the schedule does not move forward.
func advance(s Schedule, t time.Time) time.Time {
	n := s.Next(t)
	if !n.After(t) {
		return time.Time{}
	}
	return n
}
