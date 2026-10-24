// SPDX-License-Identifier: Apache-2.0

package fairness

import (
	"cmp"
	"slices"
	"time"
)

const Unlimited = -1

type Candidate struct {
	Key         string
	Priority    int
	ScheduledAt time.Time
	ID          int64
	Headroom    int
}

type Allocation struct {
	Key   string
	Quota int
}

// Allocate splits a fetch of k jobs between the candidate keys.
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

func fairPass(cands []Candidate, order, left, quota []int, remaining int, tr *Tracker) int {
	for start := 0; start < len(order) && remaining > 0; {
		var elig []int
		elig, start = levelWithHeadroom(cands, order, left, start)
		if len(elig) == 0 {
			continue
		}
		slices.SortStableFunc(elig, func(a, b int) int {
			return cmp.Compare(tr.LastServed(cands[a].Key), tr.LastServed(cands[b].Key))
		})
		remaining = share(elig, left, quota, remaining)
	}
	return remaining
}

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

// Tracker remembers when each key was last served.
type Tracker struct {
	seq  uint64
	last map[string]uint64
}

func NewTracker() *Tracker { return &Tracker{last: map[string]uint64{}} }

func (t *Tracker) Served(key string) {
	t.seq++
	t.last[key] = t.seq
}

func (t *Tracker) LastServed(key string) uint64 { return t.last[key] }

func (t *Tracker) Len() int { return len(t.last) }

func (t *Tracker) Retain(keep map[string]struct{}) {
	for k := range t.last {
		if _, ok := keep[k]; !ok {
			delete(t.last, k)
		}
	}
}
