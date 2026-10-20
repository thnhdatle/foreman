// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/app/executor"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/metrics"
	"github.com/thnhdatle/foreman/internal/platform/tracing"
	"github.com/thnhdatle/foreman/migrate"
)

// Client inserts jobs, runs worker pools for its queues and, when elected
// leader, the maintenance services. It is safe for concurrent use.
type Client struct {
	cfg      Config
	store    *pgstore.Store
	clock    Clock
	logger   *slog.Logger
	metrics  *metrics.Metrics
	tracer   *tracing.Tracer
	executor *executor.Executor

	servedMu sync.Mutex // protects served and servedAt
	served   map[string]struct{}
	servedAt time.Time

	mu  sync.Mutex // protects run
	run *runState
}

// NewClient validates cfg and returns a client using pool. It starts no
// goroutines; call Start to run workers. A client with no queues is
// producer-only and never needs Start.
func NewClient(pool *pgxpool.Pool, cfg Config) (*Client, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidArgument)
	}
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	store, err := pgstore.New(pool, cfg.Schema)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	m, err := metrics.New(cfg.Registerer)
	if err != nil {
		return nil, fmt.Errorf("%w: metrics: %w", ErrInvalidConfig, err)
	}
	c := &Client{
		cfg: cfg, store: store, clock: cfg.Clock, logger: cfg.Logger.With("component", "foreman"), metrics: m,
		tracer: tracing.New(cfg.TracerProvider),
	}
	c.executor, err = executor.New(executor.Config{
		Lookup:         c.lookup,
		DefaultTimeout: cfg.JobTimeout,
		DefaultRetry:   c.retryWith(cfg.RetryPolicy),
		HandleError:    c.handleError,
		HandlePanic:    c.handlePanic,
		Clock:          cfg.Clock,
		Logger:         c.logger,
		Observe:        c.observeAttempt,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return c, nil
}

// ID returns the client id used in attempted_by and the leader lease.
func (c *Client) ID() string { return c.cfg.ID }

// Schema returns the schema holding the Foreman tables.
func (c *Client) Schema() string { return c.cfg.Schema }

// Pool returns the pool the client uses.
func (c *Client) Pool() *pgxpool.Pool { return c.store.Pool() }

// IsLeader reports whether this client currently runs the leader services.
func (c *Client) IsLeader() bool {
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	return r != nil && r.elector != nil && r.elector.IsLeader()
}

// Start runs the worker pools of the configured queues, the heartbeats, the
// LISTEN connection and leader election. It returns once everything is
// running. The schema must be migrated (package migrate). Cancelling ctx
// later has the effect of StopAndCancel with no grace period.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run != nil {
		return ErrAlreadyStarted
	}
	v, err := migrate.Version(ctx, c.store.Pool(), migrate.WithSchema(c.cfg.Schema))
	if err != nil {
		return fmt.Errorf("foreman: check schema: %w", err)
	}
	if v < migrate.Latest() {
		return fmt.Errorf("%w: schema %q is at version %d, need %d", ErrSchemaNotMigrated, c.cfg.Schema, v, migrate.Latest())
	}
	c.checkClockSkew(ctx)
	r, err := c.newRunState()
	if err != nil {
		return err
	}
	r.start(ctx)
	c.run = r
	return nil
}

// Stop shuts down gracefully: it stops fetching, waits for running
// jobs until ctx is done, then cancels the rest, flushes completions and
// resigns leadership. It returns ctx.Err() if jobs had to be cancelled.
func (c *Client) Stop(ctx context.Context) error { return c.stop(ctx, false) }

// StopAndCancel cancels the contexts of running jobs at once, waits for them
// (bounded by ctx), flushes completions and resigns leadership.
func (c *Client) StopAndCancel(ctx context.Context) error { return c.stop(ctx, true) }

func (c *Client) stop(ctx context.Context, cancelJobs bool) error {
	c.mu.Lock()
	r := c.run
	c.run = nil
	c.mu.Unlock()
	if r == nil {
		return ErrNotStarted
	}
	return r.shutdown(ctx, cancelJobs)
}

func (c *Client) checkClockSkew(ctx context.Context) {
	var dbNow time.Time
	if err := c.store.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return
	}
	skew := c.clock.Now().Sub(dbNow)
	if skew < 0 {
		skew = -skew
	}
	if skew > leaderTTL/3 {
		c.logger.Warn("client clock differs from the database clock by more than TTL/3; leader leases and "+
			"rescue timing assume less skew (see failure-modes.md)", "duration_ms", skew.Milliseconds())
	}
}

func (c *Client) lookup(kind string) (executor.Handler, bool) {
	w, ok := c.cfg.Workers.byKind[kind]
	if !ok {
		return executor.Handler{}, false
	}
	h := executor.Handler{Timeout: w.timeout, Work: c.workFunc(w)}
	if w.retry != nil {
		h.Retry = c.retryWith(w.retry)
	}
	return h, true
}

// workFunc wraps a worker in the tracing span and the middleware chain.
func (c *Client) workFunc(w *workerEntry) executor.WorkFunc {
	return func(ctx context.Context, row *job.Row) (err error) {
		jr := toJobRow(row)
		ctx, span := c.tracer.StartWork(ctx, row.ID, row.Kind, row.Queue, row.Attempt)
		defer func() { tracing.End(span, err) }()
		next := func(ctx context.Context) error { return w.work(ctx, jr) }
		for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
			mw, inner := c.cfg.Middleware[i], next
			next = func(ctx context.Context) error { return mw.Work(ctx, jr, inner) }
		}
		return next(ctx)
	}
}

func (c *Client) retryWith(p RetryPolicy) executor.RetryFunc {
	return func(row *job.Row, now time.Time) time.Time { return p.NextRetry(*toJobRow(row), now) }
}

func (c *Client) handleError(ctx context.Context, row *job.Row, err error) *job.Override {
	if c.cfg.ErrorHandler == nil {
		return nil
	}
	return toOverride(c.cfg.ErrorHandler.HandleError(ctx, toJobRow(row), err))
}

func (c *Client) handlePanic(ctx context.Context, row *job.Row, v any, trace string) *job.Override {
	if c.cfg.ErrorHandler == nil {
		return nil
	}
	return toOverride(c.cfg.ErrorHandler.HandlePanic(ctx, toJobRow(row), v, trace))
}

func toOverride(r *ErrorHandlerResult) *job.Override {
	if r == nil {
		return nil
	}
	return &job.Override{SetCancelled: r.SetCancelled, SetDiscarded: r.SetDiscarded, SnoozeFor: r.SnoozeFor}
}

func (c *Client) observeAttempt(row *job.Row, outcome string, d time.Duration) {
	q, k := c.metrics.Queue(row.Queue), c.metrics.Kind(row.Kind)
	c.metrics.Completed.WithLabelValues(q, k, outcome).Inc()
	c.metrics.Duration.WithLabelValues(q, k).Observe(d.Seconds())
}

// execute runs one fetched job; called by producers on the job goroutine.
func (c *Client) execute(ctx context.Context, r *runState, row *job.Row) {
	q := c.metrics.Queue(row.Queue)
	if row.AttemptedAt != nil {
		c.metrics.QueueLatency.WithLabelValues(q).Observe(row.AttemptedAt.Sub(row.ScheduledAt).Seconds())
	}
	c.metrics.Running.WithLabelValues(q).Inc()
	defer c.metrics.Running.WithLabelValues(q).Dec()
	ctx = context.WithValue(ctx, clientKey{}, c)
	comp, skip := c.executor.Execute(ctx, row)
	if !skip {
		r.completer.Add(comp)
	}
}

func (c *Client) heartbeat(ctx context.Context, ref job.Ref) {
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	if r == nil || r.heartbeater == nil {
		return
	}
	if err := r.heartbeater.Beat(ctx, ref); err != nil && ctx.Err() == nil {
		c.logger.Warn("manual heartbeat failed", "job_id", ref.ID, "error", err.Error())
	}
}

// JobWork runs one available job synchronously on this client (for tests
// and operator tooling): it marks the job running for this client, runs it
// through the middleware, executor and result mapping, writes the result and
// returns the updated row. The kind must have a worker on this client.
func (c *Client) JobWork(ctx context.Context, id int64) (*JobRow, error) {
	row, err := c.store.FetchByID(ctx, id, c.cfg.ID, c.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("foreman: work job %d: %w", id, err)
	}
	if _, ok := c.cfg.Workers.byKind[row.Kind]; !ok {
		return nil, fmt.Errorf("%w %q", ErrNoWorker, row.Kind)
	}
	jctx, cancel := context.WithCancelCause(context.WithValue(ctx, clientKey{}, c))
	defer cancel(nil)
	comp, skip := c.executor.Execute(jctx, row)
	if !skip {
		applied, err := c.store.Complete(ctx, c.cfg.ID, []job.Completion{comp})
		if err != nil {
			return nil, fmt.Errorf("foreman: work job %d: %w", id, err)
		}
		if len(applied) == 0 {
			c.metrics.Stale.Inc()
		}
	}
	return c.JobGet(ctx, id)
}
