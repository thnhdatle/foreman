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

// ClientFromContext returns the client running the job, or nil outside Work.
func ClientFromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey{}).(*Client)
	return c
}

// CompleteTx marks the job completed inside tx.
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

// Snooze returns an error that runs the job again after d without using an attempt.
func Snooze(d time.Duration) error { return &job.SnoozeError{Duration: d} }

// Discard returns an error that discards the job.
func Discard(err error) error { return &job.DiscardError{Err: err} }

// Cancel returns an error that cancels the job.
func Cancel(err error) error { return &job.CancelError{Err: err} }

// Heartbeat refreshes the heartbeat of the running job.
func Heartbeat(ctx context.Context) {
	c := ClientFromContext(ctx)
	attempt := executor.AttemptFromContext(ctx)
	if c == nil || attempt == nil {
		return
	}
	c.heartbeat(ctx, jobRef(attempt.Row.ID, attempt.Row.Attempt))
}

func jobRef(id int64, attempt int) job.Ref { return job.Ref{ID: id, Attempt: attempt} }
