// SPDX-License-Identifier: Apache-2.0

package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/platform/clock"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fired(ch <-chan time.Time) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestFake_After_FiresOnlyWhenDue(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(epoch)
	ch := fc.After(10 * time.Second)
	assert.Equal(t, 1, fc.Waiters())
	fc.Advance(9 * time.Second)
	assert.False(t, fired(ch))
	fc.Advance(time.Second)
	assert.True(t, fired(ch))
	assert.Equal(t, 0, fc.Waiters())
	assert.Equal(t, epoch.Add(10*time.Second), fc.Now())
}

func TestFake_After_NonPositiveFiresImmediately(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(epoch)
	assert.True(t, fired(fc.After(0)))
	assert.True(t, fired(fc.After(-time.Second)))
}

func TestFake_Set_NeverMovesBackwards(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(epoch)
	fc.Set(epoch.Add(-time.Hour))
	assert.Equal(t, epoch, fc.Now())
}

func TestFake_Ticker_FiresOncePerAdvanceAndRealigns(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(epoch)
	tk := fc.NewTicker(time.Second)
	fc.Advance(500 * time.Millisecond)
	assert.False(t, fired(tk.C()))
	fc.Advance(3 * time.Second) // several periods at once deliver a single tick
	assert.True(t, fired(tk.C()))
	assert.False(t, fired(tk.C()))
	fc.Advance(time.Second)
	assert.True(t, fired(tk.C()))

	tk.Reset(10 * time.Second)
	fc.Advance(5 * time.Second)
	assert.False(t, fired(tk.C()))
	fc.Advance(5 * time.Second)
	assert.True(t, fired(tk.C()))

	tk.Stop()
	assert.Equal(t, 0, fc.Waiters())
	fc.Advance(time.Minute)
	assert.False(t, fired(tk.C()))
}

func TestFake_NewTicker_PanicsOnNonPositive(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(epoch)
	assert.Panics(t, func() { fc.NewTicker(0) })
	tk := fc.NewTicker(time.Second)
	assert.Panics(t, func() { tk.Reset(0) })
}

func TestReal_Basics(t *testing.T) {
	t.Parallel()
	c := clock.New()
	before := time.Now()
	require.False(t, c.Now().Before(before))
	<-c.After(time.Millisecond)
	tk := c.NewTicker(time.Millisecond)
	<-tk.C()
	tk.Reset(2 * time.Millisecond)
	<-tk.C()
	tk.Stop()
}
