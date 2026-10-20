// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// maxAttemptedBy bounds the attempted_by history kept per job.
const maxAttemptedBy = 100

// markRunning is the shared tail of every fetch: it transitions the picked
// rows to running for client $cid at time $now.
const markRunning = `
UPDATE {{s}}.foreman_jobs j
SET state = 'running', attempt = j.attempt + 1, attempted_at = @now, heartbeat_at = @now,
    attempted_by = (j.attempted_by || @cid::text)[greatest(1, cardinality(j.attempted_by) + 2 - @keep):]
FROM picked WHERE j.id = picked.id
RETURNING `

// fetchPlain takes the next jobs of a queue in fetch order.
var fetchPlain = `
WITH picked AS (
  SELECT id FROM {{s}}.foreman_jobs
  WHERE queue = @queue AND state = 'available'
    AND NOT EXISTS (SELECT 1 FROM {{s}}.foreman_queues q WHERE q.name = @queue AND q.paused_at IS NOT NULL)
  ORDER BY priority, scheduled_at, id
  LIMIT @limit
  FOR UPDATE SKIP LOCKED
)` + markRunning + cols()

// fetchByID takes one specific available job (Client.JobWork).
var fetchByID = `
WITH picked AS (
  SELECT id FROM {{s}}.foreman_jobs WHERE id = @id AND state = 'available' FOR UPDATE SKIP LOCKED
)` + markRunning + cols()

// fetchKeyed takes up to a quota of jobs per concurrency key. With a per-key
// limit, the running count of each key is read in this statement, after the
// previous statement of the transaction took the key's advisory lock, so
// concurrent producers cannot exceed the limit.
var fetchKeyed = `
WITH quotas AS (
  SELECT * FROM unnest(@keys::text[], @quotas::int[]) AS q(key, quota) WHERE q.key <> ''
),
running AS (
  SELECT concurrency_key AS key, count(*) AS n FROM {{s}}.foreman_jobs
  WHERE @per_key_limit > 0 AND queue = @queue AND state = 'running' AND concurrency_key = ANY(@keys::text[])
  GROUP BY concurrency_key
),
paused AS (
  SELECT 1 FROM {{s}}.foreman_queues q WHERE q.name = @queue AND q.paused_at IS NOT NULL
),
picked AS (
  SELECT c.id FROM quotas q
  LEFT JOIN running r ON r.key = q.key
  CROSS JOIN LATERAL (
    SELECT id FROM {{s}}.foreman_jobs
    WHERE queue = @queue AND state = 'available' AND concurrency_key = q.key AND NOT EXISTS (SELECT 1 FROM paused)
    ORDER BY priority, scheduled_at, id
    LIMIT CASE WHEN @per_key_limit > 0 THEN least(q.quota, greatest(0, @per_key_limit - coalesce(r.n, 0)))
               ELSE q.quota END
    FOR UPDATE SKIP LOCKED
  ) c
  UNION ALL
  SELECT n.id FROM (
    SELECT id FROM {{s}}.foreman_jobs
    WHERE @no_key_quota > 0 AND queue = @queue AND state = 'available' AND concurrency_key IS NULL
      AND NOT EXISTS (SELECT 1 FROM paused)
    ORDER BY priority, scheduled_at, id
    LIMIT @no_key_quota
    FOR UPDATE SKIP LOCKED
  ) n
)` + markRunning + cols()

// lockKeys takes the per-key advisory locks without waiting; keys locked by
// another producer are skipped this round (they are being served). The lock
// key is hashtextextended(queue || ':' || key, 0), prefixed
// with "<schema>:" outside the public schema so installations sharing a
// database do not contend.
const lockKeys = `
SELECT k FROM unnest(@keys::text[]) AS k
WHERE pg_try_advisory_xact_lock(hashtextextended(@lock_prefix || @queue || ':' || k, 0))`

// keyStats enumerates keys with available jobs with a loose index scan over
// foreman_jobs_fairness_idx, plus the bucket of jobs without a key. Running
// counts come from one scan of foreman_jobs_running_key_idx when requested.
const keyStats = `
WITH RECURSIVE keys AS (
  (SELECT concurrency_key AS key, priority, scheduled_at, id FROM {{s}}.foreman_jobs
   WHERE queue = @queue AND state = 'available' AND concurrency_key IS NOT NULL
   ORDER BY concurrency_key, priority, scheduled_at, id LIMIT 1)
  UNION ALL
  SELECT n.* FROM keys k CROSS JOIN LATERAL (
    SELECT concurrency_key, priority, scheduled_at, id FROM {{s}}.foreman_jobs
    WHERE queue = @queue AND state = 'available' AND concurrency_key > k.key
    ORDER BY concurrency_key, priority, scheduled_at, id LIMIT 1
  ) n
),
running AS (
  SELECT concurrency_key AS key, count(*) AS n FROM {{s}}.foreman_jobs
  WHERE @with_running AND queue = @queue AND state = 'running' AND concurrency_key IS NOT NULL
  GROUP BY concurrency_key
),
limited AS (SELECT * FROM keys LIMIT @limit)
(SELECT l.key, l.priority, l.scheduled_at, l.id, coalesce(r.n, 0)
 FROM limited l LEFT JOIN running r ON r.key = l.key ORDER BY l.key)
UNION ALL
(SELECT '', priority, scheduled_at, id, 0 FROM {{s}}.foreman_jobs
 WHERE queue = @queue AND state = 'available' AND concurrency_key IS NULL
 ORDER BY priority, scheduled_at, id LIMIT 1)`

// Fetch implements job.Fetcher.
func (s *Store) Fetch(ctx context.Context, p job.FetchParams) ([]*job.Row, error) {
	var (
		rows []*job.Row
		err  error
	)
	switch {
	case !p.Keyed:
		if p.Limit <= 0 {
			return nil, nil
		}
		rows, err = s.query(ctx, s.pool, fetchPlain, pgx.NamedArgs{
			"queue": p.Queue, "limit": p.Limit, "now": p.Now, "cid": p.ClientID, "keep": maxAttemptedBy,
		})
	case p.PerKeyLimit > 0:
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var e error
			rows, e = s.fetchLocked(ctx, tx, p)
			return e
		})
	default:
		rows, err = s.fetchKeyed(ctx, s.pool, p, p.Quotas)
	}
	if err != nil {
		return nil, translate("fetch", err)
	}
	slices.SortFunc(rows, func(a, b *job.Row) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), a.ScheduledAt.Compare(b.ScheduledAt), cmp.Compare(a.ID, b.ID))
	})
	return rows, nil
}

// FetchByID marks one available job running for clientID. It returns
// job.ErrNotFound for unknown ids and an InvalidTransitionError when the job
// is not available (or is locked by another fetch).
func (s *Store) FetchByID(ctx context.Context, id int64, clientID string, now time.Time) (*job.Row, error) {
	args := pgx.NamedArgs{"id": id, "now": now, "cid": clientID, "keep": maxAttemptedBy}
	rows, err := s.query(ctx, s.pool, fetchByID, args)
	if err != nil {
		return nil, translate("fetch job", err)
	}
	if len(rows) == 1 {
		return rows[0], nil
	}
	cur, err := s.JobGet(ctx, id)
	if err != nil {
		return nil, err
	}
	return nil, translate("fetch job", &job.InvalidTransitionError{From: cur.State, Event: job.EventFetched})
}

func (s *Store) fetchLocked(ctx context.Context, tx pgx.Tx, p job.FetchParams) ([]*job.Row, error) {
	keys := make([]string, 0, len(p.Quotas))
	for _, q := range p.Quotas {
		if q.Key != "" {
			keys = append(keys, q.Key)
		}
	}
	locked := map[string]bool{}
	if len(keys) > 0 {
		r, err := tx.Query(ctx, lockKeys, pgx.NamedArgs{"keys": keys, "queue": p.Queue, "lock_prefix": s.names.LockPrefix})
		if err != nil {
			return nil, err //nolint:wrapcheck // wrapped by Fetch
		}
		got, err := pgx.CollectRows(r, pgx.RowTo[string])
		if err != nil {
			return nil, err //nolint:wrapcheck // wrapped by Fetch
		}
		for _, k := range got {
			locked[k] = true
		}
	}
	quotas := make([]job.KeyQuota, 0, len(p.Quotas))
	for _, q := range p.Quotas {
		if q.Key == "" || locked[q.Key] {
			quotas = append(quotas, q)
		}
	}
	if len(quotas) == 0 {
		return nil, nil
	}
	return s.fetchKeyed(ctx, tx, p, quotas)
}

func (s *Store) fetchKeyed(ctx context.Context, db DB, p job.FetchParams, quotas []job.KeyQuota) ([]*job.Row, error) {
	keys := make([]string, 0, len(quotas))
	counts := make([]int32, 0, len(quotas))
	noKey := 0
	for _, q := range quotas {
		if q.Key == "" {
			noKey += q.Quota
			continue
		}
		keys = append(keys, q.Key)
		counts = append(counts, int32(q.Quota)) //nolint:gosec // G115: quotas are bounded by MaxWorkers
	}
	return s.query(ctx, db, fetchKeyed, pgx.NamedArgs{
		"queue": p.Queue, "keys": keys, "quotas": counts, "per_key_limit": p.PerKeyLimit,
		"no_key_quota": noKey, "now": p.Now, "cid": p.ClientID, "keep": maxAttemptedBy,
	})
}

func (s *Store) query(ctx context.Context, db DB, q string, args pgx.NamedArgs) ([]*job.Row, error) {
	r, err := db.Query(ctx, s.sql(q), args)
	if err != nil {
		return nil, err //nolint:wrapcheck // callers wrap with the operation
	}
	return collectRows(r)
}

// KeyStats implements job.Fetcher.
func (s *Store) KeyStats(ctx context.Context, queue string, limit int, withRunning bool) ([]job.KeyStat, error) {
	args := pgx.NamedArgs{"queue": queue, "limit": limit, "with_running": withRunning}
	r, err := s.pool.Query(ctx, s.sql(keyStats), args)
	if err != nil {
		return nil, translate("key stats", err)
	}
	defer r.Close()
	var out []job.KeyStat
	for r.Next() {
		var k job.KeyStat
		var prio int16
		var running int64
		if err := r.Scan(&k.Key, &prio, &k.ScheduledAt, &k.ID, &running); err != nil {
			return nil, translate("key stats", err)
		}
		k.Priority, k.Running = int(prio), int(running)
		out = append(out, k)
	}
	return out, translate("key stats", r.Err())
}

const queueHeartbeat = `
INSERT INTO {{s}}.foreman_queues AS q (name, created_at, updated_at) VALUES ($1, $2, $2)
ON CONFLICT (name) DO UPDATE SET updated_at = greatest(q.updated_at, EXCLUDED.updated_at)
RETURNING q.paused_at IS NOT NULL`

// QueueHeartbeat implements job.Fetcher.
func (s *Store) QueueHeartbeat(ctx context.Context, queue string, now time.Time) (bool, error) {
	var paused bool
	err := s.pool.QueryRow(ctx, s.sql(queueHeartbeat), queue, now).Scan(&paused)
	return paused, translate("queue heartbeat", err)
}

const servedQueues = `SELECT name FROM {{s}}.foreman_queues WHERE updated_at >= $1`

// ServedQueues lists queues a producer reported since cutoff.
func (s *Store) ServedQueues(ctx context.Context, cutoff time.Time) ([]string, error) {
	r, err := s.pool.Query(ctx, s.sql(servedQueues), cutoff)
	if err != nil {
		return nil, translate("served queues", err)
	}
	names, err := pgx.CollectRows(r, pgx.RowTo[string])
	return names, translate("served queues", err)
}
