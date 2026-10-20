// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/backoff"
)

// RetryPolicy decides when a failed job runs again. job.Attempt is the
// attempt that just failed (1-based).
type RetryPolicy interface {
	NextRetry(job JobRow, now time.Time) time.Time
}

// RetryPolicyFunc adapts a function to RetryPolicy.
type RetryPolicyFunc func(job JobRow, now time.Time) time.Time

// NextRetry calls f.
func (f RetryPolicyFunc) NextRetry(job JobRow, now time.Time) time.Time { return f(job, now) }

// ExponentialBackoff waits min(Cap, Base*2^(attempt-1)) with full jitter.
// The zero value uses Base 1s and Cap 24h (the default policy).
type ExponentialBackoff struct {
	Base time.Duration
	Cap  time.Duration
}

// NextRetry implements RetryPolicy.
func (e ExponentialBackoff) NextRetry(job JobRow, now time.Time) time.Time {
	p := backoff.NewExponential()
	if e.Base > 0 {
		p.Base = e.Base
	}
	if e.Cap > 0 {
		p.Cap = e.Cap
	}
	return now.Add(p.Delay(job.Attempt))
}

// ConstantBackoff always waits Delay.
type ConstantBackoff struct {
	Delay time.Duration
}

// NextRetry implements RetryPolicy.
func (c ConstantBackoff) NextRetry(_ JobRow, now time.Time) time.Time {
	return now.Add(backoff.Constant{D: c.Delay}.Delay(0))
}

// ErrorHandler is notified of failed attempts and may override the outcome.
// Returning nil keeps the default (retry with backoff, or discard when
// attempts are exhausted).
type ErrorHandler interface {
	HandleError(ctx context.Context, job *JobRow, err error) *ErrorHandlerResult
	HandlePanic(ctx context.Context, job *JobRow, panicVal any, trace string) *ErrorHandlerResult
}

// ErrorHandlerResult overrides the outcome of a failed attempt. The first
// field set wins, in field order.
type ErrorHandlerResult struct {
	SetCancelled bool
	SetDiscarded bool
	SnoozeFor    time.Duration
}
