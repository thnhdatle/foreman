// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// adminBatch bounds the rows changed per statement by bulk admin operations.
const adminBatch = 1000

var jobGet = `SELECT ` + cols() + ` FROM {{s}}.foreman_jobs j WHERE j.id = $1`

// JobGet returns one job or job.ErrNotFound.
func (s *Store) JobGet(ctx context.Context, id int64) (*job.Row, error) {
	r, err := scanRow(s.pool.QueryRow(ctx, s.sql(jobGet), id))
	if err != nil {
		return nil, translate("get job", err)
	}
	return r, nil
}

// Filter selects jobs for list and bulk operations; empty fields match all.
type Filter struct {
	IDs           []int64
	Kinds         []string
	Queues        []string
	States        []job.State
	CreatedAfter  time.Time
	CreatedBefore time.Time
	// BeforeID restricts to ids below it (list cursor); 0 = no restriction.
	BeforeID int64
}

const filterWhere = `
  (cardinality(@ids::bigint[]) = 0 OR j.id = ANY(@ids::bigint[]))
  AND (cardinality(@kinds::text[]) = 0 OR j.kind = ANY(@kinds::text[]))
  AND (cardinality(@queues::text[]) = 0 OR j.queue = ANY(@queues::text[]))
  AND (cardinality(@states::text[]) = 0 OR j.state::text = ANY(@states::text[]))
  AND (@created_after::timestamptz IS NULL OR j.created_at >= @created_after::timestamptz)
  AND (@created_before::timestamptz IS NULL OR j.created_at < @created_before::timestamptz)
  AND (@before_id::bigint = 0 OR j.id < @before_id::bigint)`

func (f Filter) args() pgx.NamedArgs {
	states := make([]string, len(f.States))
	for i, st := range f.States {
		states[i] = string(st)
	}
	var after, before *time.Time
	if !f.CreatedAfter.IsZero() {
		after = &f.CreatedAfter
	}
	if !f.CreatedBefore.IsZero() {
		before = &f.CreatedBefore
	}
	return pgx.NamedArgs{
		"ids": nonNil(f.IDs), "kinds": nonNil(f.Kinds), "queues": nonNil(f.Queues), "states": states,
		"created_after": after, "created_before": before, "before_id": f.BeforeID,
	}
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// JobList returns up to limit jobs matching f, newest first.
func (s *Store) JobList(ctx context.Context, f Filter, limit int) ([]*job.Row, error) {
	args := f.args()
	args["limit"] = limit
	q := `SELECT ` + cols() + ` FROM {{s}}.foreman_jobs j WHERE ` + filterWhere + ` ORDER BY j.id DESC LIMIT @limit`
	rows, err := s.query(ctx, s.pool, q, args)
	return rows, translate("list jobs", err)
}

// controlMessage is the JSON payload on the control channel.
type controlMessage struct {
	Action string `json:"action"`
	JobID  int64  `json:"job_id,omitempty"`
	Queue  string `json:"queue,omitempty"`
}

// cancelJobs cancels non-running jobs at once and requests cancellation of
// running ones, notifying the owning client.
var cancelJobs = `
WITH target AS (
  SELECT j.id, j.state FROM {{s}}.foreman_jobs j
  WHERE ` + filterWhere + ` AND j.state IN ('available', 'scheduled', 'retryable', 'running')
  ORDER BY j.id LIMIT @limit FOR UPDATE
),
upd AS (
  UPDATE {{s}}.foreman_jobs j SET
    state = CASE WHEN t.state = 'running' THEN j.state ELSE 'cancelled' END,
    finalized_at = CASE WHEN t.state = 'running' THEN j.finalized_at ELSE @now END,
    cancel_requested_at = CASE WHEN t.state = 'running' THEN coalesce(j.cancel_requested_at, @now)
                               ELSE j.cancel_requested_at END
  FROM target t WHERE j.id = t.id
  RETURNING ` + cols() + `
),
notified AS (
  SELECT pg_notify(@channel, json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)
  FROM upd WHERE state = 'running'
)
SELECT ` + plainCols("upd") + `, (SELECT count(*) FROM notified) FROM upd`

// discardJobs discards jobs in any non-final state; running jobs are also
// signalled so their worker stops (its completion is then rejected as stale).
var discardJobs = `
WITH target AS (
  SELECT j.id, j.state FROM {{s}}.foreman_jobs j
  WHERE ` + filterWhere + ` AND j.state IN ('available', 'scheduled', 'retryable', 'running')
  ORDER BY j.id LIMIT @limit FOR UPDATE
),
upd AS (
  UPDATE {{s}}.foreman_jobs j SET state = 'discarded', finalized_at = @now
  FROM target t WHERE j.id = t.id
  RETURNING ` + cols() + `, t.state::text AS previous
),
notified AS (
  SELECT pg_notify(@channel, json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)
  FROM upd WHERE previous = 'running'
)
SELECT ` + plainCols("upd") + `, (SELECT count(*) FROM notified) FROM upd`

// retryJobs moves final jobs back to available with attempt 0 and a
// metadata note, waking their queues.
var retryJobs = `
WITH target AS (
  SELECT j.id, j.state FROM {{s}}.foreman_jobs j
  WHERE ` + filterWhere + ` AND j.state IN ('completed', 'cancelled', 'discarded')
  ORDER BY j.id LIMIT @limit FOR UPDATE
),
upd AS (
  UPDATE {{s}}.foreman_jobs j SET
    state = 'available', attempt = 0, scheduled_at = @now, finalized_at = NULL, cancel_requested_at = NULL,
    metadata = j.metadata || jsonb_build_object('foreman_retried_at', @now::timestamptz,
      'foreman_retried_from', t.state::text,
      'foreman_retry_count', coalesce((j.metadata ->> 'foreman_retry_count')::int, 0) + 1)
  FROM target t WHERE j.id = t.id
  RETURNING ` + cols() + `
),
notified AS (
  SELECT pg_notify(@insert_channel, q.queue) FROM (SELECT DISTINCT queue FROM upd) q
)
SELECT ` + plainCols("upd") + `, (SELECT count(*) FROM notified) FROM upd`

var deleteJobs = `
DELETE FROM {{s}}.foreman_jobs WHERE id IN (
  SELECT j.id FROM {{s}}.foreman_jobs j
  WHERE ` + filterWhere + ` AND j.state <> 'running'
  ORDER BY j.id LIMIT @limit FOR UPDATE SKIP LOCKED
)`

// Admin operations on jobs.
const (
	OpCancel  = "cancel"
	OpDiscard = "discard"
	OpRetry   = "retry"
)

func (s *Store) adminSQL(op string) string {
	switch op {
	case OpCancel:
		return cancelJobs
	case OpDiscard:
		return discardJobs
	default:
		return retryJobs
	}
}

// JobUpdate applies op to the jobs matching f (at most limit of them) and
// returns the changed rows.
func (s *Store) JobUpdate(ctx context.Context, op string, f Filter, now time.Time, limit int) ([]*job.Row, error) {
	args := f.args()
	args["limit"], args["now"] = limit, now
	args["channel"], args["insert_channel"] = s.names.ControlChannel, s.names.InsertChannel
	r, err := s.pool.Query(ctx, s.sql(s.adminSQL(op)), args)
	if err != nil {
		return nil, translate(op+" jobs", err)
	}
	defer r.Close()
	var out []*job.Row
	for r.Next() {
		var notified int64
		row, err := scanRow(r, &notified)
		if err != nil {
			return nil, translate(op+" jobs", err)
		}
		out = append(out, row)
	}
	return out, translate(op+" jobs", r.Err())
}

// JobUpdateAll applies op in batches until no matching job is left and
// returns the number of jobs changed.
func (s *Store) JobUpdateAll(ctx context.Context, op string, f Filter, now time.Time) (int, error) {
	total := 0
	for {
		rows, err := s.JobUpdate(ctx, op, f, now, adminBatch)
		total += len(rows)
		if err != nil || len(rows) < adminBatch {
			return total, err
		}
	}
}

// JobDeleteAll deletes the non-running jobs matching f in batches.
func (s *Store) JobDeleteAll(ctx context.Context, f Filter) (int, error) {
	total := 0
	args := f.args()
	args["limit"] = adminBatch
	for {
		tag, err := s.pool.Exec(ctx, s.sql(deleteJobs), args)
		if err != nil {
			return total, translate("delete jobs", err)
		}
		n := int(tag.RowsAffected())
		total += n
		if n < adminBatch {
			return total, nil
		}
	}
}

const pauseQueue = `
WITH upsert AS (
  INSERT INTO {{s}}.foreman_queues AS q (name, paused_at, created_at, updated_at) VALUES ($1, $2, $3, $3)
  ON CONFLICT (name) DO UPDATE SET paused_at = EXCLUDED.paused_at
  RETURNING q.name
)
SELECT pg_notify($4, $5) FROM upsert`

// QueueSetPaused pauses (paused=true) or resumes a queue and notifies clients.
func (s *Store) QueueSetPaused(ctx context.Context, name string, paused bool, now time.Time) error {
	var pausedAt *time.Time
	action := "resume"
	if paused {
		pausedAt = &now
		action = "pause"
	}
	msg, err := json.Marshal(controlMessage{Action: action, Queue: name})
	if err != nil {
		return translate(action+" queue", err)
	}
	_, err = s.pool.Exec(ctx, s.sql(pauseQueue), name, pausedAt, now, s.names.ControlChannel, string(msg))
	return translate(action+" queue", err)
}

// QueueRow describes one queue for admin views.
type QueueRow struct {
	Name      string
	PausedAt  *time.Time
	UpdatedAt *time.Time
	Counts    map[job.State]int64
}

const queueList = `
WITH counts AS (
  SELECT queue, state::text AS state, count(*) AS n FROM {{s}}.foreman_jobs
  WHERE state IN ('available', 'scheduled', 'running', 'retryable') GROUP BY queue, state
),
names AS (SELECT name FROM {{s}}.foreman_queues UNION SELECT DISTINCT queue FROM counts)
SELECT n.name, q.paused_at, q.updated_at, c.state, c.n
FROM names n
LEFT JOIN {{s}}.foreman_queues q ON q.name = n.name
LEFT JOIN counts c ON c.queue = n.name
ORDER BY n.name, c.state`

// QueueList lists known queues with pending job counts per state.
func (s *Store) QueueList(ctx context.Context) ([]*QueueRow, error) {
	r, err := s.pool.Query(ctx, s.sql(queueList))
	if err != nil {
		return nil, translate("list queues", err)
	}
	defer r.Close()
	var out []*QueueRow
	for r.Next() {
		var name string
		var paused, updated *time.Time
		var state *string
		var n *int64
		if err := r.Scan(&name, &paused, &updated, &state, &n); err != nil {
			return nil, translate("list queues", err)
		}
		if len(out) == 0 || out[len(out)-1].Name != name {
			out = append(out, &QueueRow{Name: name, PausedAt: paused, UpdatedAt: updated, Counts: map[job.State]int64{}})
		}
		if state != nil && n != nil {
			out[len(out)-1].Counts[job.State(*state)] = *n
		}
	}
	return out, translate("list queues", r.Err())
}

// PeriodicRow summarises the ticks recorded for one periodic job.
type PeriodicRow struct {
	Name     string
	LastTick time.Time
	Ticks    int64
}

// PeriodicList lists periodic jobs that have enqueued at least one tick.
func (s *Store) PeriodicList(ctx context.Context) ([]*PeriodicRow, error) {
	// Start markers (ticks without a job) are not listed.
	r, err := s.pool.Query(ctx, s.sql(`SELECT name, max(tick_at) FILTER (WHERE job_id IS NOT NULL), count(job_id)
		FROM {{s}}.foreman_periodic_ticks GROUP BY name HAVING count(job_id) > 0 ORDER BY name`))
	if err != nil {
		return nil, translate("list periodic jobs", err)
	}
	defer r.Close()
	var out []*PeriodicRow
	for r.Next() {
		var p PeriodicRow
		if err := r.Scan(&p.Name, &p.LastTick, &p.Ticks); err != nil {
			return nil, translate("list periodic jobs", err)
		}
		out = append(out, &p)
	}
	return out, translate("list periodic jobs", r.Err())
}
