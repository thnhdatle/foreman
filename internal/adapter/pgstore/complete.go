// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

const guard = `j.state = 'running' AND j.attempt = u.attempt AND j.attempted_by[cardinality(j.attempted_by)] = @cid`

const applyCompletions = `
WITH u AS (
  SELECT * FROM unnest(@ids::bigint[], @attempts::int[], @states::text[], @scheduled::timestamptz[],
                       @finalized::timestamptz[], @errs::text[], @deltas::int[], @at::timestamptz[])
    AS u(id, attempt, state, scheduled_at, finalized_at, err, delta, at)
)
UPDATE {{s}}.foreman_jobs j SET
  state = CASE WHEN j.cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE u.state END::{{s}}.foreman_state,
  attempt = j.attempt + u.delta,
  scheduled_at = coalesce(u.scheduled_at, j.scheduled_at),
  finalized_at = CASE
    WHEN j.cancel_requested_at IS NOT NULL THEN coalesce(u.finalized_at, u.at)
    WHEN u.state IN ('completed', 'cancelled', 'discarded') THEN u.finalized_at
    ELSE NULL END,
  errors = CASE WHEN u.err IS NULL THEN j.errors ELSE array_append(j.errors, u.err::jsonb) END
FROM u
WHERE j.id = u.id AND `

type completionArrays struct {
	ids       []int64
	attempts  []int32
	states    []string
	scheduled []*time.Time
	finalized []*time.Time
	errs      []*string
	deltas    []int32
	at        []time.Time
}

func toCompletionArrays(cs []job.Completion) (completionArrays, error) {
	n := len(cs)
	a := completionArrays{
		ids: make([]int64, n), attempts: make([]int32, n), states: make([]string, n),
		scheduled: make([]*time.Time, n), finalized: make([]*time.Time, n), errs: make([]*string, n),
		deltas: make([]int32, n), at: make([]time.Time, n),
	}
	for i, c := range cs {
		a.ids[i] = c.ID
		a.attempts[i] = int32(c.Attempt) //nolint:gosec // G115: attempts fit in int4
		a.states[i] = string(c.State)
		if !c.ScheduledAt.IsZero() {
			t := c.ScheduledAt
			a.scheduled[i] = &t
		}
		if !c.FinalizedAt.IsZero() {
			t := c.FinalizedAt
			a.finalized[i] = &t
		}
		if c.Error != nil {
			b, err := json.Marshal(c.Error)
			if err != nil {
				return a, err //nolint:wrapcheck // wrapped by callers
			}
			e := string(b)
			a.errs[i] = &e
		}
		a.deltas[i] = int32(c.AttemptDelta) //nolint:gosec // G115: -1 or 0
		a.at[i] = c.At
	}
	return a, nil
}

func (a completionArrays) args() pgx.NamedArgs {
	return pgx.NamedArgs{
		"ids": a.ids, "attempts": a.attempts, "states": a.states, "scheduled": a.scheduled,
		"finalized": a.finalized, "errs": a.errs, "deltas": a.deltas, "at": a.at,
	}
}

// Complete writes the results of finished attempts.
func (s *Store) Complete(ctx context.Context, clientID string, cs []job.Completion) ([]int64, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	a, err := toCompletionArrays(cs)
	if err != nil {
		return nil, translate("complete", err)
	}
	args := a.args()
	args["cid"] = clientID
	r, err := s.pool.Query(ctx, s.sql(applyCompletions+guard+` RETURNING j.id`), args)
	if err != nil {
		return nil, translate("complete", err)
	}
	ids, err := pgx.CollectRows(r, pgx.RowTo[int64])
	return ids, translate("complete", err)
}

func (s *Store) CompleteTx(ctx context.Context, tx pgx.Tx, clientID string, ref job.Ref, now time.Time) error {
	c := job.Completion{ID: ref.ID, Attempt: ref.Attempt, State: job.StateCompleted, FinalizedAt: now, At: now}
	a, err := toCompletionArrays([]job.Completion{c})
	if err != nil {
		return translate("complete in transaction", err)
	}
	args := a.args()
	args["cid"] = clientID
	tag, err := tx.Exec(ctx, s.sql(applyCompletions+guard), args)
	if err != nil {
		return translate("complete in transaction", err)
	}
	if tag.RowsAffected() == 0 {
		return translate("complete in transaction", job.ErrStale)
	}
	return nil
}

const heartbeat = `
UPDATE {{s}}.foreman_jobs j SET heartbeat_at = @now
FROM unnest(@ids::bigint[], @attempts::int[]) AS u(id, attempt)
WHERE j.id = u.id AND ` + guard

func (s *Store) Heartbeat(ctx context.Context, clientID string, now time.Time, refs []job.Ref) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}
	ids, attempts := refsArrays(refs)
	args := pgx.NamedArgs{"now": now, "ids": ids, "attempts": attempts, "cid": clientID}
	tag, err := s.pool.Exec(ctx, s.sql(heartbeat), args)
	if err != nil {
		return 0, translate("heartbeat", err)
	}
	return int(tag.RowsAffected()), nil
}

const cancelRequested = `
SELECT id FROM {{s}}.foreman_jobs WHERE id = ANY($1) AND state = 'running' AND cancel_requested_at IS NOT NULL`

func (s *Store) CancelRequested(ctx context.Context, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	r, err := s.pool.Query(ctx, s.sql(cancelRequested), ids)
	if err != nil {
		return nil, translate("cancel poll", err)
	}
	out, err := pgx.CollectRows(r, pgx.RowTo[int64])
	return out, translate("cancel poll", err)
}

const acquireLease = `
INSERT INTO {{s}}.foreman_leader AS l (name, leader_id, elected_at, expires_at) VALUES (@name, @id, @now, @expires)
ON CONFLICT (name) DO UPDATE SET
  leader_id = EXCLUDED.leader_id,
  elected_at = CASE WHEN l.leader_id = EXCLUDED.leader_id THEN l.elected_at ELSE EXCLUDED.elected_at END,
  expires_at = EXCLUDED.expires_at
WHERE l.expires_at < @now OR l.leader_id = @id
RETURNING l.leader_id`

func (s *Store) AcquireLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error) {
	args := pgx.NamedArgs{"name": name, "id": id, "now": now, "expires": now.Add(ttl)}
	r, err := s.pool.Query(ctx, s.sql(acquireLease), args)
	if err != nil {
		return false, translate("acquire lease", err)
	}
	got, err := pgx.CollectRows(r, pgx.RowTo[string])
	return len(got) == 1, translate("acquire lease", err)
}

const renewLease = `UPDATE {{s}}.foreman_leader SET expires_at = $3 WHERE name = $1 AND leader_id = $2`

func (s *Store) RenewLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, s.sql(renewLease), name, id, now.Add(ttl))
	if err != nil {
		return false, translate("renew lease", err)
	}
	return tag.RowsAffected() == 1, nil
}

const releaseLease = `
WITH d AS (DELETE FROM {{s}}.foreman_leader WHERE name = $1 AND leader_id = $2 RETURNING name)
SELECT pg_notify($3, d.name) FROM d`

func (s *Store) ReleaseLease(ctx context.Context, name, id string) error {
	_, err := s.pool.Exec(ctx, s.sql(releaseLease), name, id, s.names.LeaderChannel)
	return translate("release lease", err)
}

type Leader struct {
	Name      string
	LeaderID  string
	ElectedAt time.Time
	ExpiresAt time.Time
}

func (s *Store) LeaderGet(ctx context.Context, name string) (*Leader, error) {
	var l Leader
	const q = `SELECT name, leader_id, elected_at, expires_at FROM {{s}}.foreman_leader WHERE name = $1`
	err := s.pool.QueryRow(ctx, s.sql(q), name).Scan(&l.Name, &l.LeaderID, &l.ElectedAt, &l.ExpiresAt)
	if err != nil {
		return nil, translate("leader", err)
	}
	return &l, nil
}
