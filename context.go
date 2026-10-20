// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/app/executor"
	"github.com/thnhdatle/foreman/internal/domain/job"
)

type clientKey struct{}

// ClientFromContext returns the client running the job, inside Work (or nil).
func ClientFromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey{}).(*Client)
	return c
}

// CompleteTx marks job completed inside tx, so the job's effects and its
// completion commit atomically. When Work then returns nil the
// client writes no further update; if Work returns an error after CompleteTx
// the normal retry path applies and the completion guard keeps the result
// consistent with whatever tx did. It returns ErrStaleJob if the attempt is
// no longer owned by this client.
func CompleteTx[T JobArgs](ctx context.Context, tx pgx.Tx, job *Job[T]) error {
	c := ClientFromContext(ctx)
	attempt := executor.AttemptFromContext(ctx)
	if c == nil || attempt == nil || job == nil || attempt.Row.ID != job.ID {
		return ErrNotInWorker
	}
	ref := jobRef(job.ID, job.Attempt)
	if err := c.store.CompleteTx(ctx, tx, c.cfg.ID, ref, c.clock.Now()); err != nil {
		return fmt.Errorf("foreman: complete in transaction: %w", err)
	}
	attempt.MarkCompletedInTx()
	return nil
}

// Snooze returns an error that makes the job run again after d without
// consuming an attempt.
func Snooze(d time.Duration) error { return &job.SnoozeError{Duration: d} }

// Discard returns an error that discards the job at once (dead letter),
// whatever attempts remain; err is recorded.
func Discard(err error) error { return &job.DiscardError{Err: err} }

// Cancel returns an error that cancels the job; err is recorded.
func Cancel(err error) error { return &job.CancelError{Err: err} }

// Heartbeat refreshes the heartbeat of the running job now, for long jobs
// between the automatic heartbeats. It is a no-op outside Work.
func Heartbeat(ctx context.Context) {
	c := ClientFromContext(ctx)
	attempt := executor.AttemptFromContext(ctx)
	if c == nil || attempt == nil {
		return
	}
	c.heartbeat(ctx, jobRef(attempt.Row.ID, attempt.Row.Attempt))
}

func jobRef(id int64, attempt int) job.Ref { return job.Ref{ID: id, Attempt: attempt} }
