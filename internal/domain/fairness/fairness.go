// SPDX-License-Identifier: Apache-2.0

package fairness

import (
	"cmp"
	"slices"
	"time"
)

// Unlimited marks a candidate whose running jobs are not capped.
const Unlimited = -1

// Candidate is a concurrency key with available jobs in a queue, described by
// its head job (the first job in fetch order) and its headroom under the
// per-key limit. Key "" is the bucket of jobs without a concurrency key.
type Candidate struct {
	Key         string
	Priority    int
	ScheduledAt time.Time
	ID          int64
	// Headroom is how many more jobs of this key may run, or Unlimited.
	Headroom int
}

// Allocation is the maximum number of jobs to fetch for one key.
type Allocation struct {
	Key   string
	Quota int
}

// Allocate splits a fetch of up to k jobs between candidate keys.
//
// Without fairness, capacity goes to keys in fetch order (priority, scheduled
// time, id of their head job), each limited by its headroom.
//
// With fairness, priority levels are served in order. Within a level, keys
// with headroom are ordered by least recently served (tracker) and each gets at
// most ceil(remaining / distinct_keys) jobs (floor shares plus one extra job
// for the least recently served keys); capacity still left after every
// level is then filled in fetch order, still limited by headroom.
//
// Quotas are upper bounds: a key may have fewer available jobs than its quota.
// The result lists keys with a positive quota in fetch order of their head job.
func Allocate(k int, cands []Candidate, fair bool, tr *Tracker) []Allocation {
	if k <= 0 || len(cands) == 0 {
		return nil
	}
	order := make([]int, len(cands))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return compareHead(cands[a], cands[b]) })

	left := make([]int, len(cands))
	for i, c := range cands {
		left[i] = c.Headroom
		if c.Headroom == Unlimited || c.Headroom > k {
			left[i] = k
		}
		left[i] = max(left[i], 0)
	}
	quota := make([]int, len(cands))
	remaining := k
	if fair {
		remaining = fairPass(cands, order, left, quota, remaining, tr)
	}
	for _, i := range order {
		if remaining == 0 {
			break
		}
		q := min(left[i], remaining)
		quota[i] += q
		left[i] -= q
		remaining -= q
	}
	out := make([]Allocation, 0, len(cands))
	for _, i := range order {
		if quota[i] > 0 {
			out = append(out, Allocation{Key: cands[i].Key, Quota: quota[i]})
		}
	}
	return out
}

// fairPass hands out one round of fair shares per priority level and returns
// the capacity that remains.
func fairPass(cands []Candidate, order, left, quota []int, remaining int, tr *Tracker) int {
	for start := 0; start < len(order) && remaining > 0; {
		var elig []int
		elig, start = levelWithHeadroom(cands, order, left, start)
		if len(elig) == 0 {
			continue
		}
		// Stable sort keeps fetch order among keys served equally recently.
		slices.SortStableFunc(elig, func(a, b int) int {
			return cmp.Compare(tr.LastServed(cands[a].Key), tr.LastServed(cands[b].Key))
		})
		remaining = share(elig, left, quota, remaining)
	}
	return remaining
}

// levelWithHeadroom returns the candidates with headroom in the priority
// level starting at order[start], and the start of the next level.
func levelWithHeadroom(cands []Candidate, order, left []int, start int) (elig []int, next int) {
	level := cands[order[start]].Priority
	next = start
	for next < len(order) && cands[order[next]].Priority == level {
		if left[order[next]] > 0 {
			elig = append(elig, order[next])
		}
		next++
	}
	return elig, next
}

// share gives each of elig floor(remaining/d) jobs, plus one for the first
// remaining%d keys (the least recently served): never more than
// ceil(remaining/d) per key, and every key gets a share when d <= remaining.
func share(elig, left, quota []int, remaining int) int {
	base, extra := remaining/len(elig), remaining%len(elig)
	for n, i := range elig {
		if remaining == 0 {
			break
		}
		s := base
		if n < extra {
			s++
		}
		q := min(s, left[i], remaining)
		quota[i] += q
		left[i] -= q
		remaining -= q
	}
	return remaining
}

func compareHead(a, b Candidate) int {
	switch {
	case a.Priority != b.Priority:
		return a.Priority - b.Priority
	case !a.ScheduledAt.Equal(b.ScheduledAt):
		return a.ScheduledAt.Compare(b.ScheduledAt)
	case a.ID != b.ID:
		if a.ID < b.ID {
			return -1
		}
		return 1
	default:
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		}
		return 0
	}
}

// Tracker remembers when each key was last served by one producer. It is
// kept in memory per producer (not a hot row) and is not safe for concurrent
// use; the owning producer serialises access.
type Tracker struct {
	seq  uint64
	last map[string]uint64
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker { return &Tracker{last: map[string]uint64{}} }

// Served records that one job of key was fetched.
func (t *Tracker) Served(key string) {
	t.seq++
	t.last[key] = t.seq
}

// LastServed returns the sequence number of the last service of key, 0 if never.
func (t *Tracker) LastServed(key string) uint64 { return t.last[key] }

// Len returns the number of keys tracked.
func (t *Tracker) Len() int { return len(t.last) }

// Retain forgets every key not in keep, bounding memory to active keys.
func (t *Tracker) Retain(keep map[string]struct{}) {
	for k := range t.last {
		if _, ok := keep[k]; !ok {
			delete(t.last, k)
		}
	}
}
