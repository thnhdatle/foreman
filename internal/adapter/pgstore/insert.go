// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

const CopyThreshold = 10

const insertSourceUnnest = `SELECT * FROM unnest(
  $1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::smallint[], $7::int[], $8::timestamptz[],
  $9::timestamptz[], $10::text[], $11::text[], $12::text[], $13::text[], $14::text[], $15::int[]
) AS i(ord, kind, queue, args, state, priority, max_attempts, scheduled_at, created_at, tags, metadata,
       identity, unique_states, concurrency_key, timeout_ms)`

const insertSourceCopy = `SELECT * FROM foreman_insert_buffer`

var bufferColumns = []string{
	"ord", "kind", "queue", "args", "state", "priority", "max_attempts", "scheduled_at", "created_at",
	"tags", "metadata", "identity", "unique_states", "concurrency_key", "timeout_ms",
}

const createBuffer = `CREATE TEMP TABLE IF NOT EXISTS foreman_insert_buffer (
  ord int, kind text, queue text, args text, state text, priority smallint, max_attempts int,
  scheduled_at timestamptz, created_at timestamptz, tags text, metadata text, identity text,
  unique_states text, concurrency_key text, timeout_ms int
) ON COMMIT DELETE ROWS`

func insertTemplate(source string) string {
	return `WITH src AS (` + source + `),
input AS MATERIALIZED (
  SELECT nextval('{{s}}.foreman_jobs_id_seq') AS id, src.ord, src.kind, src.queue, src.args::jsonb AS args,
    src.state, src.priority, src.max_attempts, src.scheduled_at, src.created_at,
    ARRAY(SELECT jsonb_array_elements_text(src.tags::jsonb)) AS tags, src.metadata::jsonb AS metadata,
    CASE WHEN src.identity IS NULL THEN NULL
         ELSE sha256(convert_to({{s}}.foreman_canonical_json(src.identity::jsonb), 'UTF8')) END AS unique_key,
    src.unique_states, src.concurrency_key, src.timeout_ms
  FROM src
),
ins AS (
  INSERT INTO {{s}}.foreman_jobs AS j (id, kind, queue, args, state, priority, max_attempts, scheduled_at,
    created_at, tags, metadata, unique_key, unique_states, concurrency_key, timeout_ms)
  SELECT id, kind, queue, args, state::{{s}}.foreman_state, priority, max_attempts, scheduled_at, created_at,
    tags, metadata, unique_key, unique_states::bit(8), concurrency_key, timeout_ms
  FROM (
    SELECT * FROM input WHERE unique_key IS NULL
    UNION ALL
    (SELECT DISTINCT ON (unique_key) * FROM input WHERE unique_key IS NOT NULL ORDER BY unique_key, ord)
  ) x
  ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND {{s}}.foreman_state_in_mask(unique_states, state)
  DO UPDATE SET kind = EXCLUDED.kind
  RETURNING ` + cols() + `, (j.xmax::text <> '0') AS conflicted
)
SELECT ` + plainCols("ins") + `, input.ord, (ins.conflicted OR ins.id <> input.id)
FROM input JOIN ins ON ins.id = input.id WHERE input.unique_key IS NULL
UNION ALL
SELECT ` + plainCols("ins") + `, input.ord, (ins.conflicted OR ins.id <> input.id)
FROM input JOIN ins ON ins.unique_key = input.unique_key WHERE input.unique_key IS NOT NULL`
}

type insertArrays struct {
	ord          []int32
	kind, queue  []string
	args, state  []string
	priority     []int16
	maxAttempts  []int32
	scheduled    []time.Time
	created      []time.Time
	tags, meta   []string
	identity     []*string
	uniqueStates []*string
	key          []*string
	timeoutMS    []*int32
}

func toArrays(ps []job.InsertParams) (insertArrays, error) {
	n := len(ps)
	a := insertArrays{
		ord: make([]int32, n), kind: make([]string, n), queue: make([]string, n), args: make([]string, n),
		state: make([]string, n), priority: make([]int16, n), maxAttempts: make([]int32, n),
		scheduled: make([]time.Time, n), created: make([]time.Time, n), tags: make([]string, n),
		meta: make([]string, n), identity: make([]*string, n), uniqueStates: make([]*string, n),
		key: make([]*string, n), timeoutMS: make([]*int32, n),
	}
	for i, p := range ps {
		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}
		tagsJSON, err := json.Marshal(tags)
		if err != nil {
			return a, fmt.Errorf("encode tags: %w", err)
		}
		a.ord[i] = int32(i) //nolint:gosec // G115: batch sizes fit in int32
		a.kind[i], a.queue[i], a.args[i], a.state[i] = p.Kind, p.Queue, string(p.Args), string(p.State)
		a.priority[i] = int16(p.Priority)       //nolint:gosec // G115: validated 1..4
		a.maxAttempts[i] = int32(p.MaxAttempts) //nolint:gosec // G115: validated by the client
		a.scheduled[i], a.created[i] = p.ScheduledAt, p.CreatedAt
		a.tags[i] = string(tagsJSON)
		a.meta[i] = string(p.Metadata)
		if len(p.Metadata) == 0 {
			a.meta[i] = "{}"
		}
		if p.UniqueIdentity != nil {
			id, mask := string(p.UniqueIdentity), p.UniqueStates
			a.identity[i], a.uniqueStates[i] = &id, &mask
		}
		if p.ConcurrencyKey != "" {
			k := p.ConcurrencyKey
			a.key[i] = &k
		}
		if p.Timeout > 0 {
			ms := int32(p.Timeout / time.Millisecond) //nolint:gosec // G115: validated by the client
			a.timeoutMS[i] = &ms
		}
	}
	return a, nil
}

// Insert inserts jobs and returns one result per input.
func (s *Store) Insert(ctx context.Context, db DB, ps []job.InsertParams) ([]job.InsertResult, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	a, err := toArrays(ps)
	if err != nil {
		return nil, translate("insert", err)
	}
	if len(ps) < CopyThreshold {
		rows, qerr := db.Query(ctx, s.sql(insertTemplate(insertSourceUnnest)),
			a.ord, a.kind, a.queue, a.args, a.state, a.priority, a.maxAttempts, a.scheduled, a.created,
			a.tags, a.meta, a.identity, a.uniqueStates, a.key, a.timeoutMS)
		if qerr != nil {
			return nil, translate("insert", qerr)
		}
		res, cerr := collectInserted(rows, len(ps))
		return res, translate("insert", cerr)
	}
	if tx, ok := db.(pgx.Tx); ok {
		res, cerr := s.insertCopy(ctx, tx, a)
		return res, translate("insert", cerr)
	}
	var res []job.InsertResult
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var e error
		res, e = s.insertCopy(ctx, tx, a)
		return e
	})
	return res, translate("insert", err)
}

func (s *Store) insertCopy(ctx context.Context, tx pgx.Tx, a insertArrays) ([]job.InsertResult, error) {
	if _, err := tx.Exec(ctx, createBuffer); err != nil {
		return nil, err //nolint:wrapcheck // wrapped by Insert
	}
	n := len(a.ord)
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"foreman_insert_buffer"}, bufferColumns,
		pgx.CopyFromSlice(n, func(i int) ([]any, error) {
			return []any{
				a.ord[i], a.kind[i], a.queue[i], a.args[i], a.state[i], a.priority[i], a.maxAttempts[i],
				a.scheduled[i], a.created[i], a.tags[i], a.meta[i], a.identity[i], a.uniqueStates[i], a.key[i],
				a.timeoutMS[i],
			}, nil
		}))
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by Insert
	}
	rows, err := tx.Query(ctx, s.sql(insertTemplate(insertSourceCopy)))
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by Insert
	}
	res, err := collectInserted(rows, n)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `TRUNCATE foreman_insert_buffer`); err != nil {
		return nil, err //nolint:wrapcheck // wrapped by Insert
	}
	return res, nil
}

func collectInserted(rows pgx.Rows, n int) ([]job.InsertResult, error) {
	defer rows.Close()
	res := make([]job.InsertResult, n)
	seen := 0
	for rows.Next() {
		var ord int32
		var skipped bool
		r, err := scanRow(rows, &ord, &skipped)
		if err != nil {
			return nil, err
		}
		res[ord] = job.InsertResult{Row: r, Skipped: skipped}
		seen++
	}
	if err := rows.Err(); err != nil {
		return nil, err //nolint:wrapcheck // wrapped by Insert
	}
	if seen != n || slices.ContainsFunc(res, func(r job.InsertResult) bool { return r.Row == nil }) {
		return nil, fmt.Errorf("insert returned %d of %d rows", seen, n)
	}
	return res, nil
}
