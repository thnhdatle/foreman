// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
	"github.com/thnhdatle/foreman/internal/platform/tracing"
)

const (
	maxTagLength            = 255
	maxConcurrencyKeyLength = 255
	maxTimeout              = 24 * 24 * time.Hour // fits timeout_ms (int4)
)

// Insert inserts one job. It is committed immediately.
func (c *Client) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	res, err := c.insert(ctx, c.store.Pool(), []InsertParams{{Args: args, InsertOpts: opts}})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// InsertTx inserts one job in tx: the job exists only if tx commits, and
// workers are notified on commit.
func (c *Client) InsertTx(ctx context.Context, tx pgx.Tx, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("%w: nil transaction", ErrInvalidArgument)
	}
	res, err := c.insert(ctx, tx, []InsertParams{{Args: args, InsertOpts: opts}})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// InsertMany inserts a batch in one transaction; batches of 10 or more go
// through COPY. Results follow the order of params.
func (c *Client) InsertMany(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
	return c.insert(ctx, c.store.Pool(), params)
}

// InsertManyTx inserts a batch in tx.
func (c *Client) InsertManyTx(ctx context.Context, tx pgx.Tx, params []InsertParams) ([]*InsertResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("%w: nil transaction", ErrInvalidArgument)
	}
	return c.insert(ctx, tx, params)
}

func (c *Client) insert(ctx context.Context, db pgstore.DB, params []InsertParams) ([]*InsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	final := func(ctx context.Context) ([]*InsertResult, error) {
		return c.insertNow(ctx, db, params)
	}
	next := final
	for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
		mw, inner := c.cfg.Middleware[i], next
		next = func(ctx context.Context) ([]*InsertResult, error) { return mw.InsertMany(ctx, params, inner) }
	}
	return next(ctx)
}

func (c *Client) insertNow(ctx context.Context, db pgstore.DB, params []InsertParams) (res []*InsertResult, err error) {
	now := c.clock.Now()
	ps := make([]job.InsertParams, len(params))
	for i, p := range params {
		ps[i], err = c.buildInsert(p.Args, p.InsertOpts, now)
		if err != nil {
			if len(params) > 1 {
				err = fmt.Errorf("params[%d]: %w", i, err)
			}
			return nil, err
		}
	}
	kind := ps[0].Kind
	if len(ps) > 1 {
		kind = "batch"
	}
	ctx, span := c.tracer.StartInsert(ctx, kind, len(ps))
	defer func() { tracing.End(span, err) }()
	rows, err := c.store.Insert(ctx, db, ps)
	if err != nil {
		return nil, fmt.Errorf("foreman: insert: %w", err)
	}
	res = make([]*InsertResult, len(rows))
	for i, r := range rows {
		res[i] = &InsertResult{Job: toJobRow(r.Row), UniqueSkippedAsDuplicate: r.Skipped}
		if !r.Skipped {
			c.metrics.Inserted.WithLabelValues(c.metrics.Queue(r.Row.Queue), c.metrics.Kind(r.Row.Kind)).Inc()
			c.countUnserved(ctx, r.Row.Queue)
		}
	}
	return res, nil
}

// buildInsert resolves options (explicit > args defaults > worker > client
// defaults) and validates the job.
func (c *Client) buildInsert(args JobArgs, explicit *InsertOpts, now time.Time) (job.InsertParams, error) {
	if args == nil {
		return job.InsertParams{}, fmt.Errorf("%w: nil args", ErrInvalidArgument)
	}
	kind := args.Kind()
	o := c.resolveOpts(args, explicit)
	encoded, err := json.Marshal(args)
	if err != nil {
		return job.InsertParams{}, fmt.Errorf("foreman: encode %s args: %w", kind, err)
	}
	if len(encoded) > c.cfg.MaxArgsBytes {
		return job.InsertParams{}, fmt.Errorf("%w: %s args are %d bytes, limit %d", ErrArgsTooLarge, kind,
			len(encoded), c.cfg.MaxArgsBytes)
	}
	if verr := validateOpts(kind, o); verr != nil {
		return job.InsertParams{}, verr
	}
	scheduled := o.ScheduledAt
	if scheduled.IsZero() || scheduled.Before(now) {
		scheduled = now
	}
	p := job.InsertParams{
		Kind: kind, Queue: o.Queue, Args: encoded, State: job.InitialState(scheduled, now), Priority: o.Priority,
		MaxAttempts: o.MaxAttempts, ScheduledAt: scheduled, CreatedAt: now, Tags: o.Tags, Metadata: o.Metadata,
		ConcurrencyKey: o.ConcurrencyKey, Timeout: o.Timeout,
	}
	if o.UniqueOpts != nil {
		uo := toUniqueOptions(o.UniqueOpts)
		if verr := uo.Validate(); verr != nil {
			return job.InsertParams{}, fmt.Errorf("%w: %w", ErrInvalidInsertOpts, verr)
		}
		p.UniqueIdentity, err = unique.Identity(kind, o.Queue, encoded, uo, now)
		if err != nil {
			return job.InsertParams{}, fmt.Errorf("%w: %w", ErrInvalidInsertOpts, err)
		}
		p.UniqueStates = unique.Mask(uo.States())
	}
	return p, nil
}

func (c *Client) resolveOpts(args JobArgs, explicit *InsertOpts) InsertOpts {
	var o InsertOpts
	if explicit != nil {
		o = *explicit
	}
	var d InsertOpts
	if withOpts, ok := args.(JobArgsWithInsertOpts); ok {
		d = withOpts.InsertOpts()
	}
	o.Queue = firstNonZero(o.Queue, d.Queue, DefaultQueue)
	o.Priority = firstNonZero(o.Priority, d.Priority, DefaultPriority)
	maxAttempts := 0
	if w, ok := c.cfg.Workers.byKind[args.Kind()]; ok {
		maxAttempts = w.maxAttempts
	}
	o.MaxAttempts = firstNonZero(o.MaxAttempts, d.MaxAttempts, maxAttempts, DefaultMaxAttempts)
	if o.ScheduledAt.IsZero() {
		o.ScheduledAt = d.ScheduledAt
	}
	if o.Tags == nil {
		o.Tags = d.Tags
	}
	if o.Metadata == nil {
		o.Metadata = d.Metadata
	}
	o.ConcurrencyKey = firstNonZero(o.ConcurrencyKey, d.ConcurrencyKey)
	if o.UniqueOpts == nil {
		o.UniqueOpts = d.UniqueOpts
	}
	o.Timeout = firstNonZero(o.Timeout, d.Timeout)
	return o
}

func firstNonZero[T comparable](vals ...T) T {
	var zero T
	for _, v := range vals {
		if v != zero {
			return v
		}
	}
	return zero
}

func validateOpts(kind string, o InsertOpts) error {
	errs := validateBasics(kind, o)
	for _, t := range o.Tags {
		if t == "" || len(t) > maxTagLength {
			errs = append(errs, fmt.Errorf("tag %q must be 1..%d bytes", t, maxTagLength))
		}
	}
	if len(o.Metadata) > 0 {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(o.Metadata, &m); err != nil || m == nil {
			errs = append(errs, errors.New("metadata must be a JSON object"))
		}
	}
	if len(o.ConcurrencyKey) > maxConcurrencyKeyLength {
		errs = append(errs, fmt.Errorf("concurrency key must be at most %d bytes", maxConcurrencyKeyLength))
	}
	if o.Timeout < 0 || o.Timeout > maxTimeout {
		errs = append(errs, fmt.Errorf("timeout %s must be between 0 and %s", o.Timeout, maxTimeout))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidInsertOpts, errors.Join(errs...))
}

func validateBasics(kind string, o InsertOpts) []error {
	var errs []error
	if !nameRe.MatchString(kind) {
		errs = append(errs, fmt.Errorf("kind %q must match %s", kind, nameRe))
	}
	if !nameRe.MatchString(o.Queue) {
		errs = append(errs, fmt.Errorf("queue %q must match %s", o.Queue, nameRe))
	}
	if o.Priority < 1 || o.Priority > 4 {
		errs = append(errs, fmt.Errorf("priority %d must be 1..4", o.Priority))
	}
	if o.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("max attempts %d must be at least 1", o.MaxAttempts))
	}
	return errs
}

func toUniqueOptions(u *UniqueOpts) unique.Options {
	states := make([]job.State, len(u.ByState))
	for i, s := range u.ByState {
		states[i] = job.State(s)
	}
	return unique.Options{
		ByArgs: u.ByArgs, ByArgFields: u.ByArgFields, ByPeriod: u.ByPeriod, ByQueue: u.ByQueue, ByState: states,
	}
}

// servedRefresh is how often the list of queues served by running clients
// is re-read for the unserved-queue metric.
const servedRefresh = 30 * time.Second

// countUnserved increments foreman_jobs_unserved_queue_total when no running
// client reported serving queue in the last minute.
func (c *Client) countUnserved(ctx context.Context, queue string) {
	if _, ok := c.cfg.Queues[queue]; ok {
		return
	}
	c.servedMu.Lock()
	defer c.servedMu.Unlock()
	now := c.clock.Now()
	if now.Sub(c.servedAt) > servedRefresh || c.served == nil {
		names, err := c.store.ServedQueues(ctx, now.Add(-time.Minute))
		if err == nil {
			c.served = make(map[string]struct{}, len(names))
			for _, n := range names {
				c.served[n] = struct{}{}
			}
		}
		c.servedAt = now // also after an error: do not query on every insert
	}
	if _, ok := c.served[queue]; !ok {
		c.metrics.Unserved.WithLabelValues(c.metrics.Queue(queue)).Inc()
	}
}
