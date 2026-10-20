// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"sync"
	"time"
)

// Fake is a manually advanced clock for tests and simulations. Timers and
// tickers created from it fire only when Advance or Set moves time past
// their deadline. It is safe for concurrent use.
type Fake struct {
	mu      sync.Mutex // protects all fields below
	now     time.Time
	waiters []*fakeWaiter
	tickers map[*fakeTicker]struct{}
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

// NewFake returns a fake clock set to start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, tickers: map[*fakeTicker]struct{}{}}
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After returns a channel that fires once the fake time reaches now+d.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, &fakeWaiter{at: f.now.Add(d), ch: ch})
	return ch
}

// NewTicker returns a ticker driven by the fake clock.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker interval")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTicker{clock: f, ch: make(chan time.Time, 1), period: d, next: f.now.Add(d)}
	f.tickers[t] = struct{}{}
	return t
}

// Advance moves the fake time forward by d and fires due timers and tickers.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()
	f.Set(target)
}

// Set moves the fake time to t (never backwards) and fires due timers and tickers.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.Before(f.now) {
		return
	}
	f.now = t
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.at.After(t) {
			w.ch <- t
			continue
		}
		kept = append(kept, w)
	}
	for i := len(kept); i < len(f.waiters); i++ {
		f.waiters[i] = nil
	}
	f.waiters = kept
	for tk := range f.tickers {
		if tk.next.After(t) {
			continue
		}
		select {
		case tk.ch <- t:
		default: // like time.Ticker, drop ticks for slow receivers
		}
		for !tk.next.After(t) {
			tk.next = tk.next.Add(tk.period)
		}
	}
}

// Waiters returns the number of pending After timers plus active tickers.
// Tests use it to wait until goroutines are blocked on the clock.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters) + len(f.tickers)
}

type fakeTicker struct {
	clock  *Fake
	ch     chan time.Time
	period time.Duration
	next   time.Time // protected by clock.mu
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	delete(t.clock.tickers, t)
}

func (t *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: non-positive ticker interval")
	}
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.period = d
	t.next = t.clock.now.Add(d)
	t.clock.tickers[t] = struct{}{}
}
