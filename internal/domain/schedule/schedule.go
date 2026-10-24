// SPDX-License-Identifier: Apache-2.0

package schedule

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule computes tick times.
type Schedule interface {
	Next(t time.Time) time.Time
}

var ErrInvalid = errors.New("schedule: invalid")

type cronSchedule struct {
	s   cron.Schedule
	loc *time.Location
}

// ParseCron parses a cron expression evaluated in loc.
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

func (c cronSchedule) Next(t time.Time) time.Time {
	n := c.s.Next(t.In(c.loc))
	if n.IsZero() {
		return n
	}
	return n.UTC()
}

type every struct{ d time.Duration }

// Every returns a schedule that ticks every d.
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

const maxSkipCount = 100_000

// DueTicks returns the ticks due since last, oldest first, and how many were skipped.
func DueTicks(s Schedule, last, now time.Time, window time.Duration, limit int) (due []time.Time, skipped int) {
	cutoff := now.Add(-window)
	t := s.Next(last)
	for !t.IsZero() && t.Before(cutoff) {
		skipped++
		if skipped >= maxSkipCount {
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

func advance(s Schedule, t time.Time) time.Time {
	n := s.Next(t)
	if !n.After(t) {
		return time.Time{}
	}
	return n
}
