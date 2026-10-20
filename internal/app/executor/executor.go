// SPDX-License-Identifier: Apache-2.0

package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// WorkFunc runs one attempt of a job.
type WorkFunc func(ctx context.Context, row *job.Row) error

// RetryFunc returns the next run time of a job that failed at now.
type RetryFunc func(row *job.Row, now time.Time) time.Time

// Handler is the per-kind configuration resolved from registered workers.
type Handler struct {
	Work    WorkFunc
	Timeout time.Duration // 0 uses the client default
	Retry   RetryFunc     // nil uses the client default
}

// Config configures an Executor.
type Config struct {
	// Lookup returns the handler for a job kind.
	Lookup func(kind string) (Handler, bool)
	// DefaultTimeout applies when neither the job nor its kind sets one; 0 = none.
	DefaultTimeout time.Duration
	// DefaultRetry computes retry times when the kind has no policy.
	DefaultRetry RetryFunc
	// HandleError and HandlePanic may override the outcome of errors and panics.
	HandleError func(ctx context.Context, row *job.Row, err error) *job.Override
	HandlePanic func(ctx context.Context, row *job.Row, value any, trace string) *job.Override
	Clock       clock.Clock
	Logger      *slog.Logger
	// Observe is called after every attempt with the outcome kind and duration.
	Observe func(row *job.Row, outcome string, d time.Duration)
}

// Executor runs Work with a timeout and panic recovery and maps the result
// to a completion.
type Executor struct {
	cfg Config
}

// ErrNoWorker is the error recorded for jobs whose kind has no worker.
var ErrNoWorker = job.ErrNoWorker

// New returns an Executor; Lookup, DefaultRetry and Clock are required.
func New(cfg Config) (*Executor, error) {
	if cfg.Lookup == nil || cfg.DefaultRetry == nil || cfg.Clock == nil {
		return nil, errors.New("executor: Lookup, DefaultRetry and Clock are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Executor{cfg: cfg}, nil
}

// Attempt is the per-attempt state reachable from the job context.
type Attempt struct {
	Row           *job.Row
	completedInTx atomic.Bool
}

// MarkCompletedInTx records that Work completed the job in its own
// transaction (CompleteTx); the executor then skips its own completion when
// Work returns nil.
func (a *Attempt) MarkCompletedInTx() { a.completedInTx.Store(true) }

// CompletedInTx reports whether MarkCompletedInTx was called.
func (a *Attempt) CompletedInTx() bool { return a.completedInTx.Load() }

type attemptKey struct{}

// AttemptFromContext returns the attempt of the job running in ctx, or nil.
func AttemptFromContext(ctx context.Context) *Attempt {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	return a
}

// Execute runs one attempt of row. ctx is the job context created by the
// producer; it is cancelled with cause job.ErrCancelRequested when an operator
// cancels the job. skip is true when the job was completed by CompleteTx and
// no completion must be written.
func (e *Executor) Execute(ctx context.Context, row *job.Row) (c job.Completion, skip bool) {
	attempt := &Attempt{Row: row}
	ctx = context.WithValue(ctx, attemptKey{}, attempt)
	start := e.cfg.Clock.Now()

	h, ok := e.cfg.Lookup(row.Kind)
	var o job.Outcome
	if !ok {
		o.Err = fmt.Errorf("%w %q", ErrNoWorker, row.Kind)
	} else {
		o = e.run(ctx, row, h)
	}
	o.CancelRequested = errors.Is(context.Cause(ctx), job.ErrCancelRequested)

	if o.IsHandlerCandidate() {
		o.Override = e.override(ctx, row, o)
	}
	now := e.cfg.Clock.Now()
	if e.cfg.Observe != nil {
		e.cfg.Observe(row, o.Kind(), now.Sub(start))
	}
	if o.Err == nil && !o.Panicked && !o.CancelRequested && attempt.CompletedInTx() {
		return job.Completion{}, true
	}
	retry := e.cfg.DefaultRetry
	if h.Retry != nil {
		retry = h.Retry
	}
	c = job.MapResult(row, o, now, func() time.Time { return retry(row, now) })
	if o.Panicked {
		e.cfg.Logger.Error("job panicked", "job_id", row.ID, "operation", row.Kind, "queue", row.Queue,
			"attempt", row.Attempt, "error", fmt.Sprint(o.PanicValue))
	}
	return c, false
}

func (e *Executor) run(ctx context.Context, row *job.Row, h Handler) (o job.Outcome) {
	timeout := e.cfg.DefaultTimeout
	if h.Timeout > 0 {
		timeout = h.Timeout
	}
	if row.Timeout > 0 {
		timeout = row.Timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer func() {
		if v := recover(); v != nil {
			o = job.Outcome{Panicked: true, PanicValue: v, Trace: string(debug.Stack())}
		}
	}()
	return job.Outcome{Err: h.Work(ctx, row)}
}

func (e *Executor) override(ctx context.Context, row *job.Row, o job.Outcome) (ov *job.Override) {
	// A misbehaving handler must not crash the worker; its panic is ignored.
	defer func() {
		if v := recover(); v != nil {
			e.cfg.Logger.Error("error handler panicked", "job_id", row.ID, "error", fmt.Sprint(v))
			ov = nil
		}
	}()
	switch {
	case o.Panicked && e.cfg.HandlePanic != nil:
		return e.cfg.HandlePanic(ctx, row, o.PanicValue, o.Trace)
	case !o.Panicked && e.cfg.HandleError != nil:
		return e.cfg.HandleError(ctx, row, o.Err)
	default:
		return nil
	}
}
