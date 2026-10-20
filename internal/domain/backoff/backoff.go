// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"math/rand/v2"
	"time"
)

// Default parameters of the exponential policy.
const (
	DefaultBase = time.Second
	DefaultCap  = 24 * time.Hour
)

// Policy computes the delay before the next attempt of a job whose attempt
// number attempt (1-based) just failed.
type Policy interface {
	Delay(attempt int) time.Duration
}

// Exponential is min(Cap, Base*2^(attempt-1)) with full jitter: the delay is
// drawn uniformly from [0, ceiling).
type Exponential struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a float in [0, 1); nil uses math/rand/v2 (goroutine-safe).
	Rand func() float64
}

// NewExponential returns the default policy: base 1s, cap 24h.
func NewExponential() Exponential {
	return Exponential{Base: DefaultBase, Cap: DefaultCap}
}

// Ceiling returns the jitter upper bound for attempt: min(Cap, Base*2^(attempt-1)).
// Attempts below 1 are treated as 1; non-positive Base or Cap yield 0.
func (e Exponential) Ceiling(attempt int) time.Duration {
	if e.Base <= 0 || e.Cap <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift >= 62 {
		return e.Cap
	}
	d := e.Base << shift
	if d <= 0 || d>>shift != e.Base || d > e.Cap {
		return e.Cap
	}
	return d
}

// Delay returns a jittered delay in [0, Ceiling(attempt)).
func (e Exponential) Delay(attempt int) time.Duration {
	ceiling := e.Ceiling(attempt)
	r := e.Rand
	if r == nil {
		r = rand.Float64
	}
	f := r()
	if f < 0 || f >= 1 {
		f = 0
	}
	return time.Duration(f * float64(ceiling))
}

// Constant always waits D (negative values are treated as 0).
type Constant struct{ D time.Duration }

// Delay returns D.
func (c Constant) Delay(int) time.Duration { return max(c.D, 0) }

// Func adapts a function to a Policy; negative results are treated as 0.
type Func func(attempt int) time.Duration

// Delay calls f.
func (f Func) Delay(attempt int) time.Duration { return max(f(attempt), 0) }
