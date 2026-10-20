// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

// DB is satisfied by *pgxpool.Pool and pgx.Tx: statements that may run in a
// caller's transaction (InsertTx, CompleteTx) accept either.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Store implements the job store ports on Postgres with pgx v5. Every SQL
// statement of Foreman lives in this package; every time value comes from
// the caller (client clock), never from now().
type Store struct {
	pool  *pgxpool.Pool
	names Names
}

// New returns a Store for schema (default public).
func New(pool *pgxpool.Pool, schema string) (*Store, error) {
	if pool == nil {
		return nil, errors.New("pgstore: nil pool")
	}
	names, err := NewNames(schema)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, names: names}, nil
}

// Names returns the schema-dependent identifiers.
func (s *Store) Names() Names { return s.names }

// Pool returns the underlying pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// sql renders a query template for the configured schema.
func (s *Store) sql(q string) string { return strings.ReplaceAll(q, "{{s}}", s.names.Quoted) }

// jobColumns are selected for every job row, in scanRow order.
var jobColumns = []string{
	"id", "kind", "queue", "args", "state::text", "priority", "attempt", "max_attempts", "scheduled_at",
	"attempted_at", "finalized_at", "created_at", "heartbeat_at", "cancel_requested_at", "attempted_by",
	"array_to_json(errors)", "tags", "metadata", "unique_key", "unique_states::text", "concurrency_key", "timeout_ms",
}

// cols returns the job columns of alias j as expressions named after their
// columns, so an outer query can select them by name.
func cols() string {
	const alias = "j"
	out := make([]string, len(jobColumns))
	for i, c := range jobColumns {
		switch c {
		case "state::text":
			out[i] = alias + ".state::text AS state"
		case "array_to_json(errors)":
			out[i] = "array_to_json(" + alias + ".errors) AS errors"
		case "unique_states::text":
			out[i] = alias + ".unique_states::text AS unique_states"
		default:
			out[i] = alias + "." + c
		}
	}
	return strings.Join(out, ", ")
}

// plainCols names the columns produced by cols(alias) through another alias.
func plainCols(alias string) string {
	names := []string{
		"id", "kind", "queue", "args", "state", "priority", "attempt", "max_attempts", "scheduled_at",
		"attempted_at", "finalized_at", "created_at", "heartbeat_at", "cancel_requested_at", "attempted_by",
		"errors", "tags", "metadata", "unique_key", "unique_states", "concurrency_key", "timeout_ms",
	}
	for i, n := range names {
		names[i] = alias + "." + n
	}
	return strings.Join(names, ", ")
}

// scanRow scans the columns of cols() (plus any extra destinations).
func scanRow(row pgx.Row, extra ...any) (*job.Row, error) {
	var (
		r         job.Row
		state     string
		priority  int16
		errs      []byte
		uniq      *string
		key       *string
		timeoutMS *int32
	)
	dest := make([]any, 0, 22+len(extra))
	dest = append(dest,
		&r.ID, &r.Kind, &r.Queue, &r.Args, &state, &priority, &r.Attempt, &r.MaxAttempts, &r.ScheduledAt,
		&r.AttemptedAt, &r.FinalizedAt, &r.CreatedAt, &r.HeartbeatAt, &r.CancelRequestedAt, &r.AttemptedBy,
		&errs, &r.Tags, &r.Metadata, &r.UniqueKey, &uniq, &key, &timeoutMS,
	)
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return nil, err //nolint:wrapcheck // callers wrap with the operation
	}
	r.State = job.State(state)
	r.Priority = int(priority)
	if len(errs) > 0 && string(errs) != "null" {
		if err := json.Unmarshal(errs, &r.Errors); err != nil {
			return nil, fmt.Errorf("decode errors of job %d: %w", r.ID, err)
		}
	}
	if uniq != nil {
		r.UniqueStates = unique.ParseMask(*uniq)
	}
	if key != nil {
		r.ConcurrencyKey = *key
	}
	if timeoutMS != nil {
		r.Timeout = time.Duration(*timeoutMS) * time.Millisecond
	}
	return &r, nil
}

func collectRows(rows pgx.Rows) ([]*job.Row, error) {
	defer rows.Close()
	var out []*job.Row
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err() //nolint:wrapcheck // callers wrap with the operation
}

// translate maps Postgres errors to domain errors and adds context.
func translate(op string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "22023" && strings.HasPrefix(pgErr.Message, unique.ErrArgsNotCanonical.Error()):
			return fmt.Errorf("pgstore: %s: %w: %s", op, unique.ErrArgsNotCanonical, pgErr.Message)
		case pgErr.Code == "23505" && pgErr.ConstraintName == "foreman_jobs_unique_idx":
			return fmt.Errorf("pgstore: %s: %w", op, job.ErrUniqueConflict)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pgstore: %s: %w", op, job.ErrNotFound)
	}
	return fmt.Errorf("pgstore: %s: %w", op, err)
}

func refsArrays(refs []job.Ref) (ids []int64, attempts []int32) {
	ids = make([]int64, len(refs))
	attempts = make([]int32, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
		attempts[i] = int32(r.Attempt) //nolint:gosec // G115: attempts are bounded by max_attempts (int4)
	}
	return ids, attempts
}
