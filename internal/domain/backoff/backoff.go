// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"math/rand/v2"
	"time"
)

const (
	DefaultBase = time.Second
	DefaultCap  = 24 * time.Hour
)

// Policy returns the delay before the next attempt of a failed job.
type Policy interface {
	Delay(attempt int) time.Duration
}

// Exponential is an exponential delay with full jitter.
type Exponential struct {
	Base time.Duration
	Cap  time.Duration
	Rand func() float64
}

// NewExponential returns the default policy.
func NewExponential() Exponential {
	return Exponential{Base: DefaultBase, Cap: DefaultCap}
}

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

type Constant struct{ D time.Duration }

func (c Constant) Delay(int) time.Duration { return max(c.D, 0) }

type Func func(attempt int) time.Duration

func (f Func) Delay(attempt int) time.Duration { return max(f(attempt), 0) }
