// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
)

// Pagination limits of JobList.
const (
	DefaultListLimit = 50
	MaxListLimit     = 500
)

// JobGet returns one job, or ErrNotFound.
func (c *Client) JobGet(ctx context.Context, id int64) (*JobRow, error) {
	r, err := c.store.JobGet(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("foreman: get job %d: %w", id, err)
	}
	return toJobRow(r), nil
}

// JobListParams filters JobList.
type JobListParams struct {
	States []State
	Kinds  []string
	Queues []string
	// Limit defaults to 50 and is at most 500.
	Limit  int
	Cursor string
}

// JobListResult is one page of jobs, newest first.
type JobListResult struct {
	Jobs       []*JobRow
	NextCursor string
}

// JobList lists jobs newest first with cursor pagination.
func (c *Client) JobList(ctx context.Context, p *JobListParams) (*JobListResult, error) {
	if p == nil {
		p = &JobListParams{}
	}
	limit := p.Limit
	switch {
	case limit <= 0:
		limit = DefaultListLimit
	case limit > MaxListLimit:
		limit = MaxListLimit
	}
	before, err := decodeCursor(p.Cursor)
	if err != nil {
		return nil, err
	}
	f := pgstore.Filter{Kinds: p.Kinds, Queues: p.Queues, States: toDomainStates(p.States), BeforeID: before}
	rows, err := c.store.JobList(ctx, f, limit+1)
	if err != nil {
		return nil, fmt.Errorf("foreman: list jobs: %w", err)
	}
	res := &JobListResult{}
	if len(rows) > limit {
		rows = rows[:limit]
		res.NextCursor = encodeCursor(rows[limit-1].ID)
	}
	res.Jobs = toJobRows(rows)
	return res, nil
}

func encodeCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("id:" + strconv.FormatInt(id, 10)))
}

func decodeCursor(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil && len(b) > 3 && string(b[:3]) == "id:" {
		if id, perr := strconv.ParseInt(string(b[3:]), 10, 64); perr == nil && id > 0 {
			return id, nil
		}
	}
	return 0, fmt.Errorf("%w: malformed cursor", ErrInvalidArgument)
}

func toDomainStates(states []State) []job.State {
	out := make([]job.State, len(states))
	for i, s := range states {
		out[i] = job.State(s)
	}
	return out
}

// JobCancel cancels a job.
func (c *Client) JobCancel(ctx context.Context, id int64) (*JobRow, error) {
	return c.jobUpdate(ctx, pgstore.OpCancel, id, job.StateCancelled)
}

// JobRetry moves a finished job back to available.
func (c *Client) JobRetry(ctx context.Context, id int64) (*JobRow, error) {
	return c.jobUpdate(ctx, pgstore.OpRetry, id, job.StateAvailable)
}

// JobDiscard moves a job to discarded.
func (c *Client) JobDiscard(ctx context.Context, id int64) (*JobRow, error) {
	return c.jobUpdate(ctx, pgstore.OpDiscard, id, job.StateDiscarded)
}

func (c *Client) jobUpdate(ctx context.Context, op string, id int64, target job.State) (*JobRow, error) {
	rows, err := c.store.JobUpdate(ctx, op, pgstore.Filter{IDs: []int64{id}}, c.clock.Now(), 1)
	if err != nil {
		return nil, fmt.Errorf("foreman: %s job %d: %w", op, id, err)
	}
	if len(rows) == 1 {
		cancelRequested := op == pgstore.OpCancel && rows[0].State == job.StateRunning
		if cancelRequested || op == pgstore.OpDiscard {
			c.cancelLocal(id)
		}
		return toJobRow(rows[0]), nil
	}
	cur, err := c.store.JobGet(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("foreman: %s job %d: %w", op, id, err)
	}
	if cur.State == target {
		return toJobRow(cur), nil
	}
	return nil, fmt.Errorf("foreman: %s job %d: %w", op, id,
		&job.InvalidTransitionError{From: cur.State, Event: job.Event("admin_" + op)})
}

func (c *Client) cancelLocal(id int64) {
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	if r != nil {
		r.cancelJob(id)
	}
}

// JobFilter selects jobs for bulk operations.
type JobFilter struct {
	Kinds         []string
	Queues        []string
	States        []State
	CreatedAfter  time.Time
	CreatedBefore time.Time
}

func (f *JobFilter) store() (pgstore.Filter, error) {
	if f == nil {
		return pgstore.Filter{}, fmt.Errorf("%w: nil filter", ErrInvalidArgument)
	}
	return pgstore.Filter{
		Kinds: f.Kinds, Queues: f.Queues, States: toDomainStates(f.States),
		CreatedAfter: f.CreatedAfter, CreatedBefore: f.CreatedBefore,
	}, nil
}

// JobRetryMany retries every final job matching f and returns how many.
func (c *Client) JobRetryMany(ctx context.Context, f *JobFilter) (int, error) {
	return c.updateMany(ctx, pgstore.OpRetry, f)
}

// JobDiscardMany discards every non-final job matching f and returns how many.
func (c *Client) JobDiscardMany(ctx context.Context, f *JobFilter) (int, error) {
	return c.updateMany(ctx, pgstore.OpDiscard, f)
}

func (c *Client) updateMany(ctx context.Context, op string, f *JobFilter) (int, error) {
	sf, err := f.store()
	if err != nil {
		return 0, err
	}
	n, err := c.store.JobUpdateAll(ctx, op, sf, c.clock.Now())
	if err != nil {
		return n, fmt.Errorf("foreman: %s jobs: %w", op, err)
	}
	return n, nil
}

// JobDeleteMany deletes matching jobs except running ones and returns how many.
func (c *Client) JobDeleteMany(ctx context.Context, f *JobFilter) (int, error) {
	sf, err := f.store()
	if err != nil {
		return 0, err
	}
	n, err := c.store.JobDeleteAll(ctx, sf)
	if err != nil {
		return n, fmt.Errorf("foreman: delete jobs: %w", err)
	}
	return n, nil
}

// QueuePause stops clients from fetching jobs of the queue.
func (c *Client) QueuePause(ctx context.Context, name string) error {
	return c.setPaused(ctx, name, true)
}

// QueueResume lets clients fetch jobs of the queue again.
func (c *Client) QueueResume(ctx context.Context, name string) error {
	return c.setPaused(ctx, name, false)
}

func (c *Client) setPaused(ctx context.Context, name string, paused bool) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%w: queue name %q", ErrInvalidArgument, name)
	}
	if err := c.store.QueueSetPaused(ctx, name, paused, c.clock.Now()); err != nil {
		return fmt.Errorf("foreman: set queue %q paused=%t: %w", name, paused, err)
	}
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	if r != nil {
		if p, ok := r.producers[name]; ok {
			p.SetPaused(paused)
		}
	}
	return nil
}

// QueueInfo describes a queue and its pending jobs.
type QueueInfo struct {
	Name       string
	Paused     bool
	PausedAt   *time.Time
	LastSeenAt *time.Time
	Available  int64
	Scheduled  int64
	Running    int64
	Retryable  int64
}

// QueueList lists the queues known to the database.
func (c *Client) QueueList(ctx context.Context) ([]*QueueInfo, error) {
	rows, err := c.store.QueueList(ctx)
	if err != nil {
		return nil, fmt.Errorf("foreman: list queues: %w", err)
	}
	out := make([]*QueueInfo, len(rows))
	for i, r := range rows {
		out[i] = &QueueInfo{
			Name: r.Name, Paused: r.PausedAt != nil, PausedAt: r.PausedAt, LastSeenAt: r.UpdatedAt,
			Available: r.Counts[job.StateAvailable], Scheduled: r.Counts[job.StateScheduled],
			Running: r.Counts[job.StateRunning], Retryable: r.Counts[job.StateRetryable],
		}
	}
	return out, nil
}

// LeaderInfo describes the current leader lease.
type LeaderInfo struct {
	LeaderID  string
	ElectedAt time.Time
	ExpiresAt time.Time
}

// LeaderGet returns the current lease, or ErrNotFound when no client holds one.
func (c *Client) LeaderGet(ctx context.Context) (*LeaderInfo, error) {
	l, err := c.store.LeaderGet(ctx, "default")
	if err != nil {
		return nil, fmt.Errorf("foreman: get leader: %w", err)
	}
	return &LeaderInfo{LeaderID: l.LeaderID, ElectedAt: l.ElectedAt, ExpiresAt: l.ExpiresAt}, nil
}

// PeriodicInfo summarises the ticks enqueued for one periodic job.
type PeriodicInfo struct {
	Name     string
	LastTick time.Time
	Ticks    int64
}

// PeriodicList lists periodic jobs that have enqueued at least one tick.
func (c *Client) PeriodicList(ctx context.Context) ([]*PeriodicInfo, error) {
	rows, err := c.store.PeriodicList(ctx)
	if err != nil {
		return nil, fmt.Errorf("foreman: list periodic jobs: %w", err)
	}
	out := make([]*PeriodicInfo, len(rows))
	for i, r := range rows {
		out[i] = &PeriodicInfo{Name: r.Name, LastTick: r.LastTick, Ticks: r.Ticks}
	}
	return out, nil
}
