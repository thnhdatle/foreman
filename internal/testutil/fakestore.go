// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// ErrInjected is returned by FakeStore when a failure is injected.
var ErrInjected = errors.New("fakestore: injected failure")

// FakeStore is an in-memory implementation of the job store ports with the
// same semantics as the Postgres adapter: fetch order, per-key limits, the
// completion guard, leases and periodic ticks. One mutex makes every
// operation atomic, which models row locks and advisory locks.
type FakeStore struct {
	mu        sync.Mutex // protects every field below
	nextID    int64
	jobs      map[int64]*job.Row
	paused    map[string]bool
	lease     *fakeLease
	ticks     map[string]map[time.Time]bool
	markers   map[string]map[time.Time]bool
	failNext  int
	FetchHook func(rows []*job.Row) // called with fetched rows while the lock is held
}

type fakeLease struct {
	id      string
	expires time.Time
}

// NewFakeStore returns an empty store.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		jobs: map[int64]*job.Row{}, paused: map[string]bool{}, ticks: map[string]map[time.Time]bool{},
	}
}

// FailNext makes the next n operations fail with ErrInjected.
func (s *FakeStore) FailNext(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = n
}

func (s *FakeStore) failed() bool {
	if s.failNext > 0 {
		s.failNext--
		return true
	}
	return false
}

// Insert adds a job and returns its id.
func (s *FakeStore) Insert(p job.InsertParams) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.insertLocked(p)
}

func (s *FakeStore) insertLocked(p job.InsertParams) int64 {
	s.nextID++
	st := p.State
	if st == "" {
		st = job.StateAvailable
	}
	s.jobs[s.nextID] = &job.Row{
		ID: s.nextID, Kind: p.Kind, Queue: p.Queue, Args: p.Args, State: st, Priority: p.Priority,
		MaxAttempts: p.MaxAttempts, ScheduledAt: p.ScheduledAt, CreatedAt: p.CreatedAt, Tags: p.Tags,
		Metadata: p.Metadata, ConcurrencyKey: p.ConcurrencyKey, Timeout: p.Timeout,
	}
	return s.nextID
}

// Job returns a copy of a job.
func (s *FakeStore) Job(id int64) (job.Row, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.jobs[id]
	if !ok {
		return job.Row{}, false
	}
	return *r, true
}

// Jobs returns copies of all jobs ordered by id.
func (s *FakeStore) Jobs() []job.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]job.Row, 0, len(s.jobs))
	for _, r := range s.jobs {
		out = append(out, *r)
	}
	slices.SortFunc(out, func(a, b job.Row) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Update mutates a job under the store lock.
func (s *FakeStore) Update(id int64, fn func(*job.Row)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.jobs[id]; ok {
		fn(r)
	}
}

// SetPaused pauses or resumes a queue.
func (s *FakeStore) SetPaused(queue string, paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused[queue] = paused
}

// RunningByKey counts running jobs per (queue, key).
func (s *FakeStore) RunningByKey(queue string) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, r := range s.jobs {
		if r.Queue == queue && r.State == job.StateRunning {
			out[r.ConcurrencyKey]++
		}
	}
	return out
}

func fetchOrder(a, b *job.Row) int {
	switch {
	case a.Priority != b.Priority:
		return cmp.Compare(a.Priority, b.Priority)
	case !a.ScheduledAt.Equal(b.ScheduledAt):
		return a.ScheduledAt.Compare(b.ScheduledAt)
	default:
		return cmp.Compare(a.ID, b.ID)
	}
}

func (s *FakeStore) available(queue string, key *string) []*job.Row {
	var out []*job.Row
	for _, r := range s.jobs {
		if r.Queue == queue && r.State == job.StateAvailable && (key == nil || r.ConcurrencyKey == *key) {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, fetchOrder)
	return out
}

// Fetch implements job.Fetcher.
func (s *FakeStore) Fetch(_ context.Context, p job.FetchParams) ([]*job.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	if s.paused[p.Queue] {
		return nil, nil
	}
	var picked []*job.Row
	if !p.Keyed {
		avail := s.available(p.Queue, nil)
		picked = avail[:min(p.Limit, len(avail))]
	} else {
		running := map[string]int{}
		for _, r := range s.jobs {
			if r.Queue == p.Queue && r.State == job.StateRunning {
				running[r.ConcurrencyKey]++
			}
		}
		for _, q := range p.Quotas {
			n := q.Quota
			if p.PerKeyLimit > 0 && q.Key != "" {
				n = min(n, max(0, p.PerKeyLimit-running[q.Key]))
			}
			avail := s.available(p.Queue, &q.Key)
			picked = append(picked, avail[:min(n, len(avail))]...)
		}
		slices.SortFunc(picked, fetchOrder)
	}
	out := make([]*job.Row, 0, len(picked))
	for _, r := range picked {
		now := p.Now
		r.State = job.StateRunning
		r.Attempt++
		r.AttemptedAt = &now
		r.HeartbeatAt = &now
		r.AttemptedBy = append(r.AttemptedBy, p.ClientID)
		cp := *r
		out = append(out, &cp)
	}
	if s.FetchHook != nil {
		s.FetchHook(out)
	}
	return out, nil
}

// KeyStats implements job.Fetcher.
func (s *FakeStore) KeyStats(_ context.Context, queue string, limit int, _ bool) ([]job.KeyStat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	heads := map[string]*job.Row{}
	running := map[string]int{}
	for _, r := range s.jobs {
		if r.Queue != queue {
			continue
		}
		switch r.State {
		case job.StateRunning:
			running[r.ConcurrencyKey]++
		case job.StateAvailable:
			if h, ok := heads[r.ConcurrencyKey]; !ok || fetchOrder(r, h) < 0 {
				heads[r.ConcurrencyKey] = r
			}
		default:
		}
	}
	out := make([]job.KeyStat, 0, len(heads))
	for k, h := range heads {
		out = append(out, job.KeyStat{
			Key: k, Priority: h.Priority, ScheduledAt: h.ScheduledAt, ID: h.ID, Running: running[k],
		})
	}
	slices.SortFunc(out, func(a, b job.KeyStat) int { return cmp.Compare(a.Key, b.Key) })
	return out[:min(limit, len(out))], nil
}

// QueueHeartbeat implements job.Fetcher.
func (s *FakeStore) QueueHeartbeat(_ context.Context, queue string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return false, ErrInjected
	}
	return s.paused[queue], nil
}

func (s *FakeStore) guard(r *job.Row, clientID string, attempt int) bool {
	return r.State == job.StateRunning && r.Attempt == attempt && len(r.AttemptedBy) > 0 &&
		r.AttemptedBy[len(r.AttemptedBy)-1] == clientID
}

func apply(r *job.Row, c job.Completion) {
	r.State = c.State
	finalized := c.FinalizedAt
	if r.CancelRequestedAt != nil && c.State != job.StateCancelled {
		// Mirrors the SQL CASE: a requested cancel wins whatever Work returned.
		r.State = job.StateCancelled
		finalized = c.At
	}
	r.Attempt += c.AttemptDelta
	if !c.ScheduledAt.IsZero() {
		r.ScheduledAt = c.ScheduledAt
	}
	if r.State.IsFinal() {
		r.FinalizedAt = &finalized
	}
	if c.Error != nil {
		r.Errors = append(r.Errors, *c.Error)
	}
}

// Complete implements job.CompletionWriter.
func (s *FakeStore) Complete(_ context.Context, clientID string, cs []job.Completion) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	var applied []int64
	for _, c := range cs {
		r, ok := s.jobs[c.ID]
		if !ok || !s.guard(r, clientID, c.Attempt) {
			continue
		}
		apply(r, c)
		applied = append(applied, c.ID)
	}
	return applied, nil
}

// Heartbeat implements job.HeartbeatWriter.
func (s *FakeStore) Heartbeat(_ context.Context, clientID string, now time.Time, refs []job.Ref) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return 0, ErrInjected
	}
	n := 0
	for _, ref := range refs {
		if r, ok := s.jobs[ref.ID]; ok && s.guard(r, clientID, ref.Attempt) {
			t := now
			r.HeartbeatAt = &t
			n++
		}
	}
	return n, nil
}

// CancelRequested implements job.CancelPoller.
func (s *FakeStore) CancelRequested(_ context.Context, ids []int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return nil, ErrInjected
	}
	var out []int64
	for _, id := range ids {
		if r, ok := s.jobs[id]; ok && r.State == job.StateRunning && r.CancelRequestedAt != nil {
			out = append(out, id)
		}
	}
	return out, nil
}

// AcquireLease implements job.LeaseStore.
func (s *FakeStore) AcquireLease(_ context.Context, _, id string, now time.Time, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return false, ErrInjected
	}
	if s.lease == nil || s.lease.expires.Before(now) || s.lease.id == id {
		s.lease = &fakeLease{id: id, expires: now.Add(ttl)}
		return true, nil
	}
	return false, nil
}

// RenewLease implements job.LeaseStore.
func (s *FakeStore) RenewLease(_ context.Context, _, id string, now time.Time, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return false, ErrInjected
	}
	if s.lease == nil || s.lease.id != id {
		return false, nil
	}
	s.lease.expires = now.Add(ttl)
	return true, nil
}

// ReleaseLease implements job.LeaseStore.
func (s *FakeStore) ReleaseLease(_ context.Context, _, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed() {
		return ErrInjected
	}
	if s.lease != nil && s.lease.id == id {
		s.lease = nil
	}
	return nil
}

// LeaseHolder returns the current lease holder id and expiry.
func (s *FakeStore) LeaseHolder() (string, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil {
		return "", time.Time{}
	}
	return s.lease.id, s.lease.expires
}
