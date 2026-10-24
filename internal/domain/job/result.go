// SPDX-License-Identifier: Apache-2.0

package job

import (
	"errors"
	"fmt"
	"time"
)

// Outcome describes how one execution of a job ended.
type Outcome struct {
	Err             error
	Panicked        bool
	PanicValue      any
	Trace           string
	CancelRequested bool
	Override        *Override
}

type Override struct {
	SetCancelled bool
	SetDiscarded bool
	SnoozeFor    time.Duration
}

func (o Outcome) Kind() string {
	switch {
	case o.CancelRequested:
		return "cancelled"
	case o.Panicked:
		return "panic"
	case o.Err == nil:
		return "success"
	}
	var se *SnoozeError
	var de *DiscardError
	var ce *CancelError
	switch {
	case errors.As(o.Err, &se):
		return "snooze"
	case errors.As(o.Err, &de):
		return "discard"
	case errors.As(o.Err, &ce):
		return "cancel"
	default:
		return "error"
	}
}

func (o Outcome) IsHandlerCandidate() bool {
	k := o.Kind()
	return k == "error" || k == "panic"
}

// MapResult converts the outcome of an attempt into a completion.
func MapResult(row *Row, o Outcome, now time.Time, retryAt func() time.Time) Completion {
	c := Completion{ID: row.ID, Attempt: row.Attempt, At: now, Kind: row.Kind, Queue: row.Queue}
	failure := attemptError(row.Attempt, o, now)

	if o.CancelRequested {
		return final(c, StateCancelled, now, failure)
	}
	if o.IsHandlerCandidate() && o.Override != nil {
		switch {
		case o.Override.SetCancelled:
			return final(c, StateCancelled, now, failure)
		case o.Override.SetDiscarded:
			return final(c, StateDiscarded, now, failure)
		case o.Override.SnoozeFor > 0:
			return snooze(c, now, o.Override.SnoozeFor)
		}
	}
	if !o.Panicked && o.Err == nil {
		return final(c, StateCompleted, now, nil)
	}
	var se *SnoozeError
	var de *DiscardError
	var ce *CancelError
	switch {
	case errors.As(o.Err, &se):
		return snooze(c, now, se.Duration)
	case errors.As(o.Err, &de):
		return final(c, StateDiscarded, now, failure)
	case errors.As(o.Err, &ce):
		return final(c, StateCancelled, now, failure)
	case row.Attempt >= row.MaxAttempts:
		return final(c, StateDiscarded, now, failure)
	default:
		c.State = StateRetryable
		c.ScheduledAt = retryAt()
		c.Error = failure
		return c
	}
}

func final(c Completion, s State, now time.Time, failure *AttemptError) Completion {
	c.State = s
	c.FinalizedAt = now
	c.Error = failure
	return c
}

func snooze(c Completion, now time.Time, d time.Duration) Completion {
	if d < 0 {
		d = 0
	}
	c.State = StateScheduled
	c.ScheduledAt = now.Add(d)
	c.AttemptDelta = -1
	return c
}

func attemptError(attempt int, o Outcome, now time.Time) *AttemptError {
	switch {
	case o.Panicked:
		return &AttemptError{At: now, Attempt: attempt, Error: fmt.Sprintf("panic: %v", o.PanicValue), Trace: o.Trace}
	case o.Err != nil:
		return &AttemptError{At: now, Attempt: attempt, Error: o.Err.Error()}
	case o.CancelRequested:
		return &AttemptError{At: now, Attempt: attempt, Error: ErrCancelRequested.Error()}
	default:
		return nil
	}
}
