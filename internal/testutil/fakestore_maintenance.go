// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

func (s *FakeStore) PromoteDue(_ context.Context, now time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return 0, ErrInjected
	}
	n := 0
	for _, r := range s.sorted() {
		if n == limit {
			break
		}
		if (r.State == job.StateScheduled || r.State == job.StateRetryable) && !r.ScheduledAt.After(now) {
			r.State = job.StateAvailable
			n++
		}
	}
	return n, nil
}

func (s *FakeStore) sorted() []*job.Row {
	out := make([]*job.Row, 0, len(s.jobs))
	for _, r := range s.jobs {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b *job.Row) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func (s *FakeStore) StuckJobs(_ context.Context, cutoff time.Time, limit int) ([]*job.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	var out []*job.Row
	for _, r := range s.sorted() {
		if len(out) == limit {
			break
		}
		if r.State == job.StateRunning && r.HeartbeatAt != nil && r.HeartbeatAt.Before(cutoff) {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *FakeStore) Rescue(_ context.Context, cutoff time.Time, cs []job.Completion) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return 0, ErrInjected
	}
	n := 0
	for _, c := range cs {
		r, ok := s.jobs[c.ID]
		if !ok || r.State != job.StateRunning || r.Attempt != c.Attempt || r.HeartbeatAt == nil ||
			!r.HeartbeatAt.Before(cutoff) {
			continue
		}
		apply(r, c)
		n++
	}
	return n, nil
}

func (s *FakeStore) DeleteFinalized(_ context.Context, state job.State, cutoff time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return 0, ErrInjected
	}
	n := 0
	for _, r := range s.sorted() {
		if n == limit {
			break
		}
		if r.State == state && r.FinalizedAt != nil && r.FinalizedAt.Before(cutoff) {
			delete(s.jobs, r.ID)
			n++
		}
	}
	return n, nil
}

func (s *FakeStore) Reindex(context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return false, ErrInjected
	}
	return true, nil
}

func (s *FakeStore) QueueDepths(context.Context) ([]job.QueueDepth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	counts := map[[2]string]int64{}
	for _, r := range s.jobs {
		counts[[2]string{r.Queue, string(r.State)}]++
	}
	out := make([]job.QueueDepth, 0, len(counts))
	for k, n := range counts {
		out = append(out, job.QueueDepth{Queue: k[0], State: job.State(k[1]), Count: n})
	}
	slices.SortFunc(out, func(a, b job.QueueDepth) int {
		return cmp.Or(cmp.Compare(a.Queue, b.Queue), cmp.Compare(a.State, b.State))
	})
	return out, nil
}

func (s *FakeStore) LastTicks(context.Context) (map[string]time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	out := map[string]time.Time{}
	for name, ticks := range s.ticks {
		for t := range ticks {
			if t.After(out[name]) {
				out[name] = t
			}
		}
	}
	return out, nil
}

func (s *FakeStore) EnqueueTick(_ context.Context, name string, tick time.Time, p job.InsertParams) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return false, ErrInjected
	}
	if s.ticks[name] == nil {
		s.ticks[name] = map[time.Time]bool{}
	}
	if s.ticks[name][tick] {
		return false, nil
	}
	s.ticks[name][tick] = true
	s.insertLocked(p)
	return true, nil
}

func (s *FakeStore) MarkPeriodicStart(_ context.Context, name string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return ErrInjected
	}
	if s.ticks[name] == nil {
		s.ticks[name] = map[time.Time]bool{}
	}
	s.ticks[name][now] = true
	if s.markers == nil {
		s.markers = map[string]map[time.Time]bool{}
	}
	if s.markers[name] == nil {
		s.markers[name] = map[time.Time]bool{}
	}
	s.markers[name][now] = true
	return nil
}

func (s *FakeStore) PruneTicks(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return 0, ErrInjected
	}
	n := 0
	for _, ticks := range s.ticks {
		var latest time.Time
		for t := range ticks {
			if t.After(latest) {
				latest = t
			}
		}
		for t := range ticks {
			if t.Before(cutoff) && t.Before(latest) {
				delete(ticks, t)
				n++
			}
		}
	}
	return n, nil
}

func (s *FakeStore) Ticks(name string) []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, 0, len(s.ticks[name]))
	for t := range s.ticks[name] {
		if !s.markers[name][t] {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return out
}
