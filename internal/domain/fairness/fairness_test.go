// SPDX-License-Identifier: Apache-2.0

package fairness_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/fairness"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func cand(key string, prio, offsetSec int, id int64, headroom int) fairness.Candidate {
	return fairness.Candidate{
		Key: key, Priority: prio, ScheduledAt: t0.Add(time.Duration(offsetSec) * time.Second),
		ID: id, Headroom: headroom,
	}
}

func TestAllocate(t *testing.T) {
	t.Parallel()
	u := fairness.Unlimited
	tests := []struct {
		name   string
		k      int
		cands  []fairness.Candidate
		fair   bool
		served []string
		want   []fairness.Allocation
	}{
		{name: "no capacity", k: 0, cands: []fairness.Candidate{cand("a", 2, 0, 1, u)}},
		{name: "no candidates", k: 5},
		{
			name:  "unfair fills head key first",
			k:     10,
			cands: []fairness.Candidate{cand("b", 2, 5, 2, u), cand("a", 2, 0, 1, u)},
			want:  []fairness.Allocation{{Key: "a", Quota: 10}},
		},
		{
			name:  "unfair respects headroom and spills over",
			k:     10,
			cands: []fairness.Candidate{cand("a", 2, 0, 1, 3), cand("b", 2, 5, 2, 4), cand("c", 3, 0, 3, u)},
			want:  []fairness.Allocation{{Key: "a", Quota: 3}, {Key: "b", Quota: 4}, {Key: "c", Quota: 3}},
		},
		{
			name:  "zero and negative headroom get nothing",
			k:     4,
			cands: []fairness.Candidate{cand("a", 1, 0, 1, 0), cand("b", 1, 1, 2, -5), cand("c", 2, 0, 3, 2)},
			want:  []fairness.Allocation{{Key: "c", Quota: 2}},
		},
		{
			name:  "priority beats scheduled time",
			k:     2,
			cands: []fairness.Candidate{cand("late", 1, 100, 9, u), cand("early", 2, 0, 1, u)},
			want:  []fairness.Allocation{{Key: "late", Quota: 2}},
		},
		{
			name:  "id then key break ties",
			k:     3,
			cands: []fairness.Candidate{cand("z", 2, 0, 5, 1), cand("y", 2, 0, 5, 1), cand("x", 2, 0, 4, 1)},
			want:  []fairness.Allocation{{Key: "x", Quota: 1}, {Key: "y", Quota: 1}, {Key: "z", Quota: 1}},
		},
		{
			name:  "fair splits evenly with ceiling",
			k:     5,
			fair:  true,
			cands: []fairness.Candidate{cand("a", 2, 0, 1, u), cand("b", 2, 9, 2, u)},
			want:  []fairness.Allocation{{Key: "a", Quota: 3}, {Key: "b", Quota: 2}},
		},
		{
			name:   "fair serves least recently served first",
			k:      1,
			fair:   true,
			served: []string{"b", "a"},
			cands:  []fairness.Candidate{cand("a", 2, 0, 1, u), cand("b", 2, 9, 2, u)},
			want:   []fairness.Allocation{{Key: "b", Quota: 1}},
		},
		{
			name:   "never served key goes first",
			k:      1,
			fair:   true,
			served: []string{"a"},
			cands:  []fairness.Candidate{cand("a", 2, 0, 1, u), cand("small", 2, 50, 99, u)},
			want:   []fairness.Allocation{{Key: "small", Quota: 1}},
		},
		{
			name:  "fair applies within the highest priority level",
			k:     4,
			fair:  true,
			cands: []fairness.Candidate{cand("low", 3, 0, 1, u), cand("a", 1, 5, 2, u), cand("b", 1, 6, 3, u)},
			want:  []fairness.Allocation{{Key: "a", Quota: 2}, {Key: "b", Quota: 2}},
		},
		{
			name:  "headroom-limited level hands capacity to the next level",
			k:     6,
			fair:  true,
			cands: []fairness.Candidate{cand("a", 1, 0, 1, 1), cand("b", 1, 1, 2, 1), cand("c", 2, 0, 3, u), cand("d", 2, 0, 4, u)},
			want: []fairness.Allocation{
				{Key: "a", Quota: 1}, {Key: "b", Quota: 1}, {Key: "c", Quota: 2}, {Key: "d", Quota: 2},
			},
		},
		{
			name:  "fill pass after fair shares",
			k:     10,
			fair:  true,
			cands: []fairness.Candidate{cand("a", 2, 0, 1, u), cand("b", 2, 1, 2, 2), cand("c", 2, 2, 3, 1)},
			want:  []fairness.Allocation{{Key: "a", Quota: 7}, {Key: "b", Quota: 2}, {Key: "c", Quota: 1}},
		},
		{
			name:  "no-key bucket is a key like any other",
			k:     4,
			fair:  true,
			cands: []fairness.Candidate{cand("", 2, 0, 1, u), cand("t1", 2, 3, 2, u)},
			want:  []fairness.Allocation{{Key: "", Quota: 2}, {Key: "t1", Quota: 2}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := fairness.NewTracker()
			for _, k := range tc.served {
				tr.Served(k)
			}
			got := fairness.Allocate(tc.k, tc.cands, tc.fair, tr)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTracker(t *testing.T) {
	t.Parallel()
	tr := fairness.NewTracker()
	assert.Equal(t, uint64(0), tr.LastServed("a"))
	tr.Served("a")
	tr.Served("b")
	tr.Served("a")
	assert.Equal(t, uint64(3), tr.LastServed("a"))
	assert.Equal(t, uint64(2), tr.LastServed("b"))
	assert.Equal(t, 2, tr.Len())
	tr.Retain(map[string]struct{}{"b": {}})
	assert.Equal(t, 1, tr.Len())
	assert.Equal(t, uint64(0), tr.LastServed("a"))
	assert.Equal(t, uint64(2), tr.LastServed("b"))
}

// Property: quotas never exceed headroom or capacity, and capacity is fully
// handed out whenever the candidates have enough headroom; with fairness every
// key of the best priority level with headroom gets a share.
func TestAllocate_Property_Bounds(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		k := rapid.IntRange(0, 200).Draw(rt, "k")
		n := rapid.IntRange(0, 30).Draw(rt, "n")
		fair := rapid.Bool().Draw(rt, "fair")
		cands := make([]fairness.Candidate, n)
		head := map[string]int{}
		total := 0
		for i := range cands {
			hr := rapid.IntRange(-1, 20).Draw(rt, "headroom")
			key := string(rune('a' + i))
			cands[i] = cand(key, rapid.IntRange(1, 4).Draw(rt, "prio"), rapid.IntRange(0, 100).Draw(rt, "off"), int64(i), hr)
			head[key] = hr
			if hr == fairness.Unlimited {
				total += k
			} else {
				total += hr
			}
		}
		tr := fairness.NewTracker()
		for i := rapid.IntRange(0, 20).Draw(rt, "served"); i > 0; i-- {
			tr.Served(string(rune('a' + rapid.IntRange(0, 29).Draw(rt, "key"))))
		}
		allocs := fairness.Allocate(k, cands, fair, tr)
		sum := 0
		seen := map[string]bool{}
		for _, a := range allocs {
			if a.Quota <= 0 || seen[a.Key] {
				rt.Fatalf("bad allocation %+v", a)
			}
			seen[a.Key] = true
			if hr := head[a.Key]; hr != fairness.Unlimited && a.Quota > hr {
				rt.Fatalf("key %s quota %d above headroom %d", a.Key, a.Quota, hr)
			}
			sum += a.Quota
		}
		if sum > k {
			rt.Fatalf("allocated %d > capacity %d", sum, k)
		}
		if want := min(k, total); sum != want {
			rt.Fatalf("allocated %d, want %d", sum, want)
		}
		if !fair || k == 0 {
			return
		}
		best := 99
		for _, c := range cands {
			if c.Headroom != 0 && c.Priority < best {
				best = c.Priority
			}
		}
		var level []string
		for _, c := range cands {
			if c.Priority == best && c.Headroom != 0 {
				level = append(level, c.Key)
			}
		}
		if len(level) > 0 && len(level) <= k {
			for _, key := range level {
				if !seen[key] {
					rt.Fatalf("fair mode starved key %s of the best level", key)
				}
			}
		}
	})
}

// simQueue is a discrete-time model of one queue with per-key FIFOs.
type simQueue struct {
	jobs map[string][]int // key -> arrival ticks
}

func (q *simQueue) candidates() []fairness.Candidate {
	var out []fairness.Candidate
	for k, js := range q.jobs {
		if len(js) > 0 {
			out = append(out, fairness.Candidate{
				Key: k, Priority: 2, ScheduledAt: t0.Add(time.Duration(js[0]) * time.Millisecond),
				ID: int64(js[0]), Headroom: fairness.Unlimited,
			})
		}
	}
	return out
}

// simulate runs `ticks` steps of a queue whose workers each finish one job per
// tick (capacity jobs per tick). Tenant A starts with a standing backlog and
// arrives at 0.8 x capacity; tenant B arrives at one job every bEvery ticks.
// It returns B's wait times in ticks.
func simulate(fair bool, capacity, backlog, ticks, bEvery int) []int {
	q := &simQueue{jobs: map[string][]int{"A": {}, "B": {}}}
	for i := 0; i < backlog; i++ {
		q.jobs["A"] = append(q.jobs["A"], 0)
	}
	tr := fairness.NewTracker()
	var waits []int
	aCarry := 0.0
	for tick := 1; tick <= ticks; tick++ {
		aCarry += 0.8 * float64(capacity)
		for ; aCarry >= 1; aCarry-- {
			q.jobs["A"] = append(q.jobs["A"], tick)
		}
		if tick%bEvery == 0 {
			q.jobs["B"] = append(q.jobs["B"], tick)
		}
		for _, a := range fairness.Allocate(capacity, q.candidates(), fair, tr) {
			n := min(a.Quota, len(q.jobs[a.Key]))
			for _, arrived := range q.jobs[a.Key][:n] {
				tr.Served(a.Key)
				if a.Key == "B" {
					waits = append(waits, tick-arrived)
				}
			}
			q.jobs[a.Key] = q.jobs[a.Key][n:]
		}
	}
	return waits
}

func p95(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]int(nil), xs...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return sorted[(len(sorted)*95+99)/100-1]
}

// Fairness simulation: skewed arrival rates with a standing
// backlog. With fairness the small tenant is served in the tick it arrives;
// without it, it waits behind the whole backlog.
func TestAllocate_Simulation_SkewedArrivalsWithBacklog(t *testing.T) {
	t.Parallel()
	// Unfair mode is FIFO: a B job arriving at tick t starts once the 2000
	// backlog jobs and A's 16 jobs/tick ahead of it are done, at tick
	// 100 + 0.8t, so it waits 100 - 0.2t ticks (≈ 25..100 over 375 ticks).
	const capacity, backlog, ticks, bEvery = 20, 2000, 400, 7
	fairWaits := simulate(true, capacity, backlog, ticks, bEvery)
	unfairWaits := simulate(false, capacity, backlog, ticks, bEvery)
	assert.Len(t, fairWaits, ticks/bEvery, "fair: every B job ran")
	assert.Equal(t, 0, p95(fairWaits), "fair: B is served the tick it arrives")
	assert.GreaterOrEqual(t, p95(unfairWaits), 50, "unfair: B waits behind the backlog")
}
