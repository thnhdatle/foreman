// SPDX-License-Identifier: Apache-2.0

package backoff_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/backoff"
)

func TestExponential_Ceiling(t *testing.T) {
	t.Parallel()
	e := backoff.NewExponential()
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{-3, time.Second},
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{10, 512 * time.Second},
		{17, 65536 * time.Second},
		{18, 24 * time.Hour},
		{40, 24 * time.Hour},
		{62, 24 * time.Hour},
		{63, 24 * time.Hour},
		{1 << 20, 24 * time.Hour},
	}
	for _, tc := range tests {
		assert.Equalf(t, tc.want, e.Ceiling(tc.attempt), "attempt %d", tc.attempt)
	}
}

func TestExponential_CeilingEdgeCases(t *testing.T) {
	t.Parallel()
	assert.Equal(t, time.Duration(0), backoff.Exponential{Base: 0, Cap: time.Hour}.Ceiling(3))
	assert.Equal(t, time.Duration(0), backoff.Exponential{Base: time.Second, Cap: 0}.Ceiling(3))
	assert.Equal(t, time.Duration(0), backoff.Exponential{Base: -time.Second, Cap: time.Hour}.Ceiling(3))
	big := backoff.Exponential{Base: time.Duration(1) << 40, Cap: time.Duration(1<<63 - 1)}
	assert.Equal(t, big.Cap, big.Ceiling(30), "overflow saturates at cap")
	exact := backoff.Exponential{Base: time.Second, Cap: 4 * time.Second}
	assert.Equal(t, 4*time.Second, exact.Ceiling(3), "ceiling equal to cap")
	assert.Equal(t, 4*time.Second, exact.Ceiling(4))
}

func TestExponential_DelayUsesFullJitter(t *testing.T) {
	t.Parallel()
	e := backoff.Exponential{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 0.5 }}
	assert.Equal(t, 2*time.Second, e.Delay(3))
	e.Rand = func() float64 { return 0 }
	assert.Equal(t, time.Duration(0), e.Delay(3))
	e.Rand = func() float64 { return 0.999 }
	assert.Equal(t, time.Duration(0.999*float64(4*time.Second)), e.Delay(3))
	e.Rand = func() float64 { return 1.5 }
	assert.Equal(t, time.Duration(0), e.Delay(3))
	e.Rand = func() float64 { return -0.1 }
	assert.Equal(t, time.Duration(0), e.Delay(3))
	d := backoff.NewExponential().Delay(1)
	assert.GreaterOrEqual(t, d, time.Duration(0))
	assert.Less(t, d, time.Second)
}

func TestConstantAndFunc(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 3*time.Second, backoff.Constant{D: 3 * time.Second}.Delay(9))
	assert.Equal(t, time.Duration(0), backoff.Constant{D: -time.Second}.Delay(1))
	f := backoff.Func(func(a int) time.Duration { return time.Duration(a) * time.Minute })
	assert.Equal(t, 5*time.Minute, f.Delay(5))
	assert.Equal(t, time.Duration(0), f.Delay(-1))
}

func TestExponential_Property_BoundsAndMonotoneExpectation(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		base := time.Duration(rapid.Int64Range(1, int64(time.Hour)).Draw(rt, "base"))
		limit := time.Duration(rapid.Int64Range(1, int64(30*24*time.Hour)).Draw(rt, "cap"))
		seed := rapid.Uint64().Draw(rt, "seed")
		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		e := backoff.Exponential{Base: base, Cap: limit, Rand: rng.Float64}
		prev := time.Duration(0)
		for attempt := 1; attempt <= 80; attempt++ {
			d := e.Delay(attempt)
			if d < 0 || d > limit {
				rt.Fatalf("delay %s out of [0, %s] at attempt %d", d, limit, attempt)
			}
			c := e.Ceiling(attempt)
			if c < prev {
				rt.Fatalf("expectation decreased at attempt %d: %s < %s", attempt, c, prev)
			}
			if d > c {
				rt.Fatalf("delay %s above ceiling %s", d, c)
			}
			prev = c
		}
	})
}
