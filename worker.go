// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Worker handles jobs of one kind.
type Worker[T JobArgs] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// WorkFunc adapts a function to the Worker interface.
type WorkFunc[T JobArgs] func(ctx context.Context, job *Job[T]) error

// Work calls f.
func (f WorkFunc[T]) Work(ctx context.Context, job *Job[T]) error { return f(ctx, job) }

// Workers is the registry of workers by kind.
type Workers struct {
	byKind map[string]*workerEntry
}

type workerEntry struct {
	kind        string
	timeout     time.Duration
	retry       RetryPolicy
	maxAttempts int
	work        func(ctx context.Context, row *JobRow) error
}

// NewWorkers returns an empty registry.
func NewWorkers() *Workers { return &Workers{byKind: map[string]*workerEntry{}} }

// Kinds returns the registered kinds, sorted.
func (w *Workers) Kinds() []string { return slices.Sorted(maps.Keys(w.byKind)) }

// WorkerOption configures the jobs of one kind.
type WorkerOption func(*workerEntry)

// WithTimeout sets the default timeout of the kind.
func WithTimeout(d time.Duration) WorkerOption { return func(e *workerEntry) { e.timeout = d } }

// WithRetryPolicy sets the retry policy of the kind (default: client policy).
func WithRetryPolicy(p RetryPolicy) WorkerOption { return func(e *workerEntry) { e.retry = p } }

// WithMaxAttempts sets the default max attempts of the kind.
func WithMaxAttempts(n int) WorkerOption { return func(e *workerEntry) { e.maxAttempts = n } }

// AddWorker registers worker for the kind of T.
func AddWorker[T JobArgs](w *Workers, worker Worker[T], opts ...WorkerOption) {
	var zero T
	kind := zero.Kind()
	if kind == "" {
		panic("foreman: AddWorker: Kind() returned an empty string")
	}
	if _, dup := w.byKind[kind]; dup {
		panic(fmt.Sprintf("foreman: AddWorker: a worker for kind %q is already registered", kind))
	}
	e := &workerEntry{kind: kind}
	for _, o := range opts {
		o(e)
	}
	e.work = func(ctx context.Context, row *JobRow) error {
		j, err := newJob[T](row)
		if err != nil {
			return err
		}
		return worker.Work(ctx, j)
	}
	w.byKind[kind] = e
}

func newJob[T JobArgs](row *JobRow) (*Job[T], error) {
	var args T
	if err := json.Unmarshal(row.Args, &args); err != nil {
		return nil, fmt.Errorf("foreman: decode args of %s job %d: %w", row.Kind, row.ID, err)
	}
	return &Job[T]{
		ID: row.ID, Args: args, Attempt: row.Attempt, MaxAttempts: row.MaxAttempts, Queue: row.Queue,
		Priority: row.Priority, Tags: row.Tags, Metadata: row.Metadata, CreatedAt: row.CreatedAt,
		ScheduledAt: row.ScheduledAt, Errors: row.Errors,
	}, nil
}
