// SPDX-License-Identifier: Apache-2.0

// Package accjobs holds the job types and workers shared by the acceptance
// tests and the acceptance worker process.
package accjobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman"
)

// NoopArgs is a configurable test job.
type NoopArgs struct {
	Seq int64 `json:"seq"`
	// SleepMS simulates work.
	SleepMS int `json:"sleep_ms,omitempty"`
	// FirstAttemptMS makes attempt 1 take this long; later attempts use SleepMS.
	FirstAttemptMS int `json:"first_attempt_ms,omitempty"`
}

// Kind implements foreman.JobArgs.
func (NoopArgs) Kind() string { return "acc_noop" }

// UniqueArgs identifies a unique job; Big carries int64 values above 2^53.
type UniqueArgs struct {
	Run int64 `json:"run"`
	Big int64 `json:"big"`
}

// Kind implements foreman.JobArgs.
func (UniqueArgs) Kind() string { return "acc_unique" }

// ExecutionsTable is created in the test schema to record executions.
const ExecutionsTable = "acc_executions"

// CreateExecutions creates the executions table in schema.
func CreateExecutions(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%s (
		job_id bigint NOT NULL, attempt int NOT NULL, client_id text NOT NULL,
		at timestamptz NOT NULL DEFAULT clock_timestamp())`,
		schema, ExecutionsTable))
	if err != nil {
		return fmt.Errorf("create executions table: %w", err)
	}
	return nil
}

// NoopWorker records every execution, then simulates work.
type NoopWorker struct {
	Pool   *pgxpool.Pool
	Schema string
	// Record disables recording when false.
	Record bool
}

// Work implements foreman.Worker.
func (w *NoopWorker) Work(ctx context.Context, j *foreman.Job[NoopArgs]) error {
	if w.Record {
		_, err := w.Pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q.%s (job_id, attempt, client_id) VALUES ($1, $2, $3)`,
			w.Schema, ExecutionsTable), j.ID, j.Attempt, foreman.ClientFromContext(ctx).ID())
		if err != nil {
			return fmt.Errorf("record execution: %w", err)
		}
	}
	sleep := j.Args.SleepMS
	if j.Attempt == 1 && j.Args.FirstAttemptMS > 0 {
		sleep = j.Args.FirstAttemptMS
	}
	if sleep > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(sleep) * time.Millisecond):
		}
	}
	// Success even if this attempt was rescued meanwhile: the completion
	// guard must then reject the result as stale.
	return nil
}

// UniqueWorker completes unique jobs.
type UniqueWorker struct{}

// Work implements foreman.Worker.
func (UniqueWorker) Work(context.Context, *foreman.Job[UniqueArgs]) error { return nil }

// BigBase is above 2^53; FMN-08 keys differ only in their last digits.
const BigBase = int64(9007199254740990)

// InsertUnique inserts one unique job through the Go client or, when viaSQL,
// through foreman_insert_job.
func InsertUnique(ctx context.Context, c *foreman.Client, pool *pgxpool.Pool, schema string, run, big int64,
	viaSQL bool,
) error {
	if !viaSQL {
		if _, err := c.Insert(ctx, UniqueArgs{Run: run, Big: big}, &foreman.InsertOpts{
			UniqueOpts: &foreman.UniqueOpts{ByArgs: true},
		}); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		return nil
	}
	q := fmt.Sprintf(`SELECT %q.foreman_insert_job('acc_unique', $1::text::jsonb, '{"unique":{"by_args":true}}')`, schema)
	if _, err := pool.Exec(ctx, q, fmt.Sprintf(`{"run":%d,"big":%d}`, run, big)); err != nil {
		return fmt.Errorf("foreman_insert_job: %w", err)
	}
	return nil
}
