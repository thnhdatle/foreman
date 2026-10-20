// SPDX-License-Identifier: Apache-2.0

package clock

import "time"

// Clock is the source of time for every client-side time value and every
// time comparison Foreman sends to Postgres.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After returns a channel that receives the time once d has elapsed.
	After(d time.Duration) <-chan time.Time
	// NewTicker returns a ticker that fires every d.
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers ticks at a fixed interval until stopped.
type Ticker interface {
	// C returns the channel on which ticks are delivered.
	C() <-chan time.Time
	// Stop turns the ticker off.
	Stop()
	// Reset changes the ticker period and restarts it.
	Reset(d time.Duration)
}

// Real is the wall clock.
type Real struct{}

// New returns the wall clock.
func New() Real { return Real{} }

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// After wraps time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTicker wraps time.NewTicker.
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time   { return r.t.C }
func (r realTicker) Stop()                 { r.t.Stop() }
func (r realTicker) Reset(d time.Duration) { r.t.Reset(d) }
