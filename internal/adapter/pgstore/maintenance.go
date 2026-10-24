// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

const promoteDue = `
WITH due AS (
  SELECT id FROM {{s}}.foreman_jobs
  WHERE state IN ('scheduled', 'retryable') AND scheduled_at <= $1
  ORDER BY scheduled_at LIMIT $2 FOR UPDATE SKIP LOCKED
),
upd AS (
  UPDATE {{s}}.foreman_jobs j SET state = 'available' FROM due WHERE j.id = due.id RETURNING j.queue
),
notified AS (
  SELECT pg_notify($3, q.queue) FROM (SELECT DISTINCT queue FROM upd) q
)
SELECT (SELECT count(*) FROM upd), (SELECT count(*) FROM notified)`

func (s *Store) PromoteDue(ctx context.Context, now time.Time, limit int) (int, error) {
	var n, queues int64
	err := s.pool.QueryRow(ctx, s.sql(promoteDue), now, limit, s.names.InsertChannel).Scan(&n, &queues)
	return int(n), translate("promote due jobs", err)
}

var stuckJobs = `
SELECT ` + cols() + ` FROM {{s}}.foreman_jobs j
WHERE j.state = 'running' AND j.heartbeat_at < $1 ORDER BY j.heartbeat_at LIMIT $2`

func (s *Store) StuckJobs(ctx context.Context, cutoff time.Time, limit int) ([]*job.Row, error) {
	r, err := s.pool.Query(ctx, s.sql(stuckJobs), cutoff, limit)
	if err != nil {
		return nil, translate("stuck jobs", err)
	}
	rows, err := collectRows(r)
	return rows, translate("stuck jobs", err)
}

func (s *Store) Rescue(ctx context.Context, cutoff time.Time, cs []job.Completion) (int, error) {
	if len(cs) == 0 {
		return 0, nil
	}
	a, err := toCompletionArrays(cs)
	if err != nil {
		return 0, translate("rescue", err)
	}
	args := a.args()
	args["cutoff"] = cutoff
	q := applyCompletions + `j.state = 'running' AND j.attempt = u.attempt AND j.heartbeat_at < @cutoff`
	tag, err := s.pool.Exec(ctx, s.sql(q), args)
	if err != nil {
		return 0, translate("rescue", err)
	}
	return int(tag.RowsAffected()), nil
}

const deleteFinalized = `
DELETE FROM {{s}}.foreman_jobs WHERE id IN (
  SELECT id FROM {{s}}.foreman_jobs
  WHERE state = $1::text::{{s}}.foreman_state AND finalized_at < $2
  LIMIT $3 FOR UPDATE SKIP LOCKED
)`

func (s *Store) DeleteFinalized(ctx context.Context, state job.State, cutoff time.Time, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, s.sql(deleteFinalized), string(state), cutoff, limit)
	if err != nil {
		return 0, translate("delete finalized jobs", err)
	}
	return int(tag.RowsAffected()), nil
}

var reindexedIndexes = []string{
	"foreman_jobs_fetch_idx", "foreman_jobs_fairness_idx", "foreman_jobs_running_key_idx",
	"foreman_jobs_scheduler_idx", "foreman_jobs_rescue_idx", "foreman_jobs_cleaner_idx",
}

func (s *Store) Reindex(ctx context.Context) (bool, error) {
	var busy bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index WHERE datid = (
		SELECT oid FROM pg_database WHERE datname = current_database()))`).Scan(&busy)
	if err != nil {
		return false, translate("reindex", err)
	}
	if busy {
		return false, nil
	}
	r, err := s.pool.Query(ctx, `SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT i.indisvalid AND c.relname LIKE 'foreman_jobs_%_ccnew%'`, s.names.Schema)
	if err != nil {
		return false, translate("reindex", err)
	}
	invalid, err := pgx.CollectRows(r, pgx.RowTo[string])
	if err != nil {
		return false, translate("reindex", err)
	}
	for _, name := range invalid {
		q := fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s.%s`, s.names.Quoted, pgx.Identifier{name}.Sanitize())
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return false, translate("reindex", err)
		}
	}
	for _, idx := range reindexedIndexes {
		q := fmt.Sprintf(`REINDEX INDEX CONCURRENTLY %s.%s`, s.names.Quoted, idx)
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return false, translate("reindex "+idx, err)
		}
	}
	return true, nil
}

const queueDepths = `
SELECT queue, state::text, count(*) FROM {{s}}.foreman_jobs
WHERE state IN ('available', 'scheduled', 'running', 'retryable')
GROUP BY queue, state ORDER BY queue, state`

func (s *Store) QueueDepths(ctx context.Context) ([]job.QueueDepth, error) {
	r, err := s.pool.Query(ctx, s.sql(queueDepths))
	if err != nil {
		return nil, translate("queue depths", err)
	}
	defer r.Close()
	var out []job.QueueDepth
	for r.Next() {
		var d job.QueueDepth
		var st string
		if err := r.Scan(&d.Queue, &st, &d.Count); err != nil {
			return nil, translate("queue depths", err)
		}
		d.State = job.State(st)
		out = append(out, d)
	}
	return out, translate("queue depths", r.Err())
}

func (s *Store) LastTicks(ctx context.Context) (map[string]time.Time, error) {
	r, err := s.pool.Query(ctx, s.sql(`SELECT name, max(tick_at) FROM {{s}}.foreman_periodic_ticks GROUP BY name`))
	if err != nil {
		return nil, translate("last ticks", err)
	}
	defer r.Close()
	out := map[string]time.Time{}
	for r.Next() {
		var name string
		var t time.Time
		if err := r.Scan(&name, &t); err != nil {
			return nil, translate("last ticks", err)
		}
		out[name] = t
	}
	return out, translate("last ticks", r.Err())
}

const insertTick = `
INSERT INTO {{s}}.foreman_periodic_ticks (name, tick_at, created_at) VALUES ($1, $2, $3)
ON CONFLICT (name, tick_at) DO NOTHING`

func (s *Store) EnqueueTick(ctx context.Context, name string, tick time.Time, p job.InsertParams) (bool, error) {
	inserted := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, s.sql(insertTick), name, tick, p.CreatedAt)
		if err != nil {
			return err //nolint:wrapcheck // wrapped below
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		res, err := s.Insert(ctx, tx, []job.InsertParams{p})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, s.sql(`UPDATE {{s}}.foreman_periodic_ticks SET job_id = $3 WHERE name = $1 AND tick_at = $2`),
			name, tick, res[0].Row.ID)
		inserted = err == nil
		return err //nolint:wrapcheck // wrapped below
	})
	return inserted, translate("enqueue periodic tick", err)
}

func (s *Store) MarkPeriodicStart(ctx context.Context, name string, now time.Time) error {
	_, err := s.pool.Exec(ctx, s.sql(insertTick), name, now, now)
	return translate("mark periodic start", err)
}

const pruneTicks = `
DELETE FROM {{s}}.foreman_periodic_ticks t
WHERE t.tick_at < $1
  AND t.tick_at < (SELECT max(tick_at) FROM {{s}}.foreman_periodic_ticks m WHERE m.name = t.name)`

func (s *Store) PruneTicks(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, s.sql(pruneTicks), cutoff)
	if err != nil {
		return 0, translate("prune ticks", err)
	}
	return int(tag.RowsAffected()), nil
}
